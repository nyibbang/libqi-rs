use super::capabilities;
use crate::{
    auth::{self, Authenticator},
    error::{HandlerError, NoHandlerError},
    messaging,
    value::{ActionId, FormatInto, IntoFormat, KeyDynValueMap, ObjectId, ServiceId},
    Error,
};
use bytes::Bytes;
use futures::{
    future::{err, ready},
    FutureExt, TryFutureExt,
};
use messaging::message;
use std::{future::Future, sync::Arc};
use tokio::sync::watch;

const SERVICE_ID: ServiceId = ServiceId(0);
const OBJECT_ID: ObjectId = ObjectId(0);
const AUTHENTICATE_ACTION_ID: ActionId = ActionId(8);

fn is_control_address(address: message::Address) -> bool {
    address.service() == SERVICE_ID && address.object() == OBJECT_ID
}

pub(crate) const AUTHENTICATE_ADDRESS: message::Address =
    message::Address(SERVICE_ID, OBJECT_ID, AUTHENTICATE_ACTION_ID);

#[derive(Clone)]
pub(super) struct Controller {
    authenticator: Option<Arc<dyn Authenticator + Send + Sync>>,
    capabilities: watch::Sender<Option<KeyDynValueMap>>,
    remote_authorized: watch::Sender<bool>,
}

impl Controller {
    fn authenticate(
        &self,
        request: KeyDynValueMap,
    ) -> Result<KeyDynValueMap, AuthenticateClientError> {
        let shared_capabilities = capabilities::shared_with_local(&request);
        capabilities::check_required(&shared_capabilities)?;
        if let Some(authenticator) = &self.authenticator {
            authenticator
                .verify(request)
                .map_err(AuthenticateClientError::AuthenticationVerification)?;
        }
        self.capabilities
            .send_replace(Some(shared_capabilities.clone()));
        self.remote_authorized.send_replace(true);
        Ok(auth::state_done_map(shared_capabilities))
    }

    pub(super) async fn authenticate_to_server(
        &self,
        client: &mut messaging::Client,
        parameters: KeyDynValueMap,
    ) -> Result<(), Error> {
        // Reset the current capabilities
        self.capabilities.send_replace(None);
        let mut request = capabilities::local_map().clone();
        request.extend(parameters);
        let mut shared_capabilities = client
            .call(AUTHENTICATE_ADDRESS, request.into_format_args()?)
            .await?
            .into_reflect_return_value()?;
        auth::extract_state_result(&mut shared_capabilities)
            .map_err(AuthenticateToServerError::ResultState)?;
        capabilities::check_required(&shared_capabilities)
            .map_err(AuthenticateToServerError::UnexpectedServerCapabilityValue)?;
        self.capabilities.send_replace(Some(shared_capabilities));
        Ok(())
    }
}

impl std::fmt::Debug for Controller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Control")
            .field("capabilities", &self.capabilities)
            .field("remote_authorized", &self.remote_authorized)
            .finish()
    }
}

pub(super) struct Control<H> {
    pub(super) controller: Controller,
    pub(super) capabilities: watch::Receiver<Option<KeyDynValueMap>>,
    pub(super) remote_authorized: watch::Receiver<bool>,
    pub(super) handler: ControlledHandler<H>,
}

// TODO: Split this between server and client
pub(super) fn create<Handler>(
    handler: Handler,
    authenticator: Option<Arc<dyn Authenticator + Send + Sync>>, // meaningless for a client
    remote_authorized: bool,                                     // meaningless for a client
) -> Control<Handler> {
    let (capabilities_sender, capabilities_receiver) = watch::channel(Default::default());
    let (remote_authorized_sender, remote_authorized_receiver) = watch::channel(remote_authorized);
    let controller = Controller {
        authenticator,
        capabilities: capabilities_sender,
        remote_authorized: remote_authorized_sender,
    };
    let controlled_handler = ControlledHandler {
        inner: handler,
        controller: controller.clone(),
    };
    Control {
        controller,
        capabilities: capabilities_receiver,
        remote_authorized: remote_authorized_receiver,
        handler: controlled_handler,
    }
}

pub(super) struct ControlledHandler<H> {
    inner: H,
    controller: Controller,
}

impl<Handler> messaging::CallHandler for ControlledHandler<Handler>
where
    Handler: messaging::CallHandler + Sync,
    Handler::Error: Into<HandlerError> + 'static,
{
    type Error = HandlerError;

    fn handle_call(
        &mut self,
        address: message::Address,
        args: Bytes,
    ) -> impl Future<Output = Result<Bytes, Self::Error>> + 'static + Send + 'static {
        if is_control_address(address) {
            let authenticate = || {
                self.controller
                    .authenticate(args.to_reflect_args()?)
                    // All authentication errors are fatal
                    .map_err(HandlerError::fatal)?
                    .into_format_return_value()
                    .map_err(Into::into)
            };
            ready(authenticate()).left_future()
        } else if *self.controller.remote_authorized.borrow() {
            self.inner
                .handle_call(address, args)
                .map_err(Into::into)
                .right_future()
        } else {
            err(HandlerError::non_fatal(NoHandlerError(
                message::Type::Call,
                address,
            )))
            .left_future()
        }
    }
}

impl<Handler> messaging::EventHandler for ControlledHandler<Handler>
where
    Handler: messaging::EventHandler + Sync,
{
    fn handle_event(&mut self, address: message::Address, value: Bytes) {
        if !is_control_address(address) && *self.controller.remote_authorized.borrow() {
            self.inner.handle_event(address, value)
        }
    }
}

impl<Handler> messaging::PostHandler for ControlledHandler<Handler>
where
    Handler: messaging::PostHandler + Sync,
{
    fn handle_post(&mut self, address: message::Address, args: Bytes) {
        if !is_control_address(address) && *self.controller.remote_authorized.borrow() {
            self.inner.handle_post(address, args)
        }
    }
}

impl<Handler> messaging::CapabilitiesHandler for ControlledHandler<Handler> {
    fn handle_capabilities(&mut self, _address: message::Address, _map: KeyDynValueMap) {
        // nothing, unhandled at the moment
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum AuthenticateClientError {
    #[error("unexpected capability value")]
    UnexpectedclientCapabilityValue(#[from] capabilities::KeyValueExpectError),

    #[error("failure to verify authentication request")]
    AuthenticationVerification(#[source] auth::Error),
}

impl From<AuthenticateClientError> for Error {
    fn from(err: AuthenticateClientError) -> Self {
        Error::Other(err.into())
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum AuthenticateToServerError {
    #[error("the authentication state sent back by the server is invalid")]
    ResultState(#[from] auth::StateError),

    #[error("the server sent an unexpected capability value")]
    UnexpectedServerCapabilityValue(#[from] capabilities::KeyValueExpectError),
}

impl From<AuthenticateToServerError> for Error {
    fn from(err: AuthenticateToServerError) -> Self {
        Error::Other(err.into())
    }
}
