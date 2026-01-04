mod capabilities;
mod control;
mod store;
mod target;

pub(crate) use self::{store::Store, target::Target};
use crate::{
    auth::Authenticator,
    error::{Error, HandlerError},
    messaging::{self, message},
    value::{self, FormatInto, IntoFormat, KeyDynValueMap, Value},
};
use control::Control;
use futures::{stream::FusedStream, Sink, StreamExt, TryStream};
use qi_messaging::Address;
use std::{net::SocketAddr, pin::pin, sync::Arc};
use tokio::{select, sync::watch, task, time};
use tokio_util::task::AbortOnDropHandle;

pub(crate) struct Session {
    capabilities: watch::Receiver<Option<KeyDynValueMap>>,
    client: messaging::Client,
}

impl Session {
    pub(crate) async fn connect<MsgStream, MsgSink, Handler>(
        messages_in: MsgStream,
        messages_out: MsgSink,
        credentials: KeyDynValueMap,
        handler: Handler,
    ) -> Result<Self, Error>
    where
        MsgStream: TryStream<Ok = messaging::Message> + Send + 'static,
        MsgStream::Error: Send,
        MsgSink: Sink<messaging::Message> + Send + 'static,
        MsgSink::Error: Send,
        Handler: messaging::CallHandler
            + messaging::EventHandler
            + messaging::PostHandler
            + Send
            + Sync
            + 'static,
        Handler::Error: Into<HandlerError> + 'static,
    {
        let Control {
            controller,
            capabilities,
            handler,
            ..
        } = control::create(handler, None, true);
        let (mut client, connection) =
            messaging::endpoint::start(messages_in, messages_out, handler);
        task::spawn(connection);
        controller
            .authenticate_to_server(&mut client, credentials)
            .await?;
        Ok(Session {
            capabilities,
            client,
        })
    }

    pub(crate) async fn call(
        &self,
        address: message::Address,
        args: Value<'_>,
        return_type: Option<&value::Type>,
    ) -> Result<Value<'static>, Error> {
        self.client
            .call(address, args.into_format_args()?)
            .await?
            .into_return_value(return_type)
    }

    pub(crate) async fn post(
        &self,
        address: message::Address,
        args: Value<'_>,
    ) -> Result<(), Error> {
        self.client
            .post(address, args.into_format_args()?)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn send_event(
        &self,
        address: message::Address,
        value: Value<'_>,
    ) -> Result<(), Error> {
        self.client
            .send_event(address, value.into_format_args()?)
            .await?;
        Ok(())
    }

    pub(crate) fn downgrade(&self) -> WeakSession {
        WeakSession {
            capabilities: self.capabilities.clone(),
            client: self.client.downgrade(),
        }
    }
}

impl Clone for Session {
    fn clone(&self) -> Self {
        Self {
            capabilities: self.capabilities.clone(),
            client: self.client.clone(),
        }
    }
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("capabilities", &self.capabilities)
            .field("client", &self.client)
            .finish()
    }
}

pub(crate) struct WeakSession {
    capabilities: watch::Receiver<Option<KeyDynValueMap>>,
    client: messaging::WeakClient,
}

impl WeakSession {
    pub(crate) fn upgrade(&self) -> Option<Session> {
        self.client.upgrade().map(|client| Session {
            capabilities: self.capabilities.clone(),
            client,
        })
    }
}

impl Clone for WeakSession {
    fn clone(&self) -> Self {
        Self {
            capabilities: self.capabilities.clone(),
            client: self.client.clone(),
        }
    }
}

impl std::fmt::Debug for WeakSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WeakSession")
            .field("capabilities", &self.capabilities)
            .field("client", &self.client)
            .finish()
    }
}

/// Binds a server of sessions to an address.
///
/// Spawn a server task that:
///   1) spawns a session server side with the given authenticator and messaging handler each
///      time a client connects to the server.
///   2) updates a list of endpoints for this session. The list of endpoints changes if the
///      address targets multiple interfaces and interfaces availability changes on the system.
///
/// The future terminates when the server is bound and clients can connect. The return value is a
/// watch receiver of a pair of:
///   - a local address that the server is bound to.
///   - a list of endpoints that clients can connect to.
///
/// The receiver is severed from its sender when the server is stopped.
pub(crate) async fn server<Handler>(
    address: messaging::Address,
    authenticator: Option<Arc<dyn Authenticator + Send + Sync>>,
    handler: Handler,
) -> Result<(Server, ServerEndpointsWatcher), std::io::Error>
where
    Handler: messaging::CallHandler
        + messaging::EventHandler
        + messaging::PostHandler
        + Send
        + Sync
        + Clone
        + 'static,
    Handler::Error: Into<HandlerError>,
{
    let (clients, local_address) = messaging::channel::serve(address).await?;
    let (mut endpoints_sender, endpoints_receiver) = watch::channel((local_address, Vec::new()));
    let task = task::spawn(async move {
        let mut clients = pin!(clients.fuse());
        let mut update_endpoints = pin!(update_address_endpoints(
            local_address,
            &mut endpoints_sender
        ));
        // Use a join set so that when this task is dropped, all spawned client session tasks are aborted.
        let mut client_tasks = task::JoinSet::new();
        loop {
            select! {
                Some((messages_stream, messages_sink, _address)) = clients.next(), if !clients.is_terminated() => {
                    client_tasks.spawn(serve_client(
                        messages_stream,
                        messages_sink,
                        authenticator.clone(),
                        handler.clone(),
                    ));
                }
                () = &mut update_endpoints => {
                    // nothing, if this future terminates it means that the address was not an
                    // "ANY" IP address. The endpoints sender must not be dropped.
                }
                else => {
                    break;
                }
            }
        }
    });
    Ok((Server(AbortOnDropHandle::new(task)), endpoints_receiver))
}

pub(crate) async fn serve_client<MsgStream, MsgSink, Handler>(
    messages_stream: MsgStream,
    messages_sink: MsgSink,
    authenticator: Option<Arc<dyn Authenticator + Send + Sync>>,
    handler: Handler,
) where
    MsgStream: TryStream<Ok = messaging::Message> + Send + 'static,
    MsgStream::Error: Send,
    MsgSink: Sink<messaging::Message> + Send + 'static,
    Handler: messaging::CallHandler
        + messaging::EventHandler
        + messaging::PostHandler
        + Send
        + Sync
        + 'static,
    Handler::Error: Into<HandlerError>,
{
    let Control {
        capabilities,
        mut remote_authorized,
        handler,
        ..
    } = control::create(handler, authenticator, false);
    let (client, connection) = messaging::endpoint::start(messages_stream, messages_sink, handler);
    let mut _session = None;
    task::spawn(async move {
        let _res = connection.await;
    });

    while let Ok(()) = remote_authorized.changed().await {
        if *remote_authorized.borrow_and_update() {
            _session = Some(Session {
                capabilities: capabilities.clone(),
                client: client.clone(),
            })
        } else {
            _session = None;
        }
    }
}

#[derive(Debug)]
pub(crate) struct Server(#[allow(dead_code)] AbortOnDropHandle<()>);

pub(crate) type ServerEndpointsWatcher = watch::Receiver<(Address, Vec<Address>)>;

const NETWORK_INTERFACES_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Returns a future that will update endpoints associated to a local address into the sender.
///
/// A local address can be bound to an "ANY" IP address, meaning that it is bound to all network
/// interfaces of the host system. This means that when the set of interfaces changes, so do local
/// endpoints. This future checks if the address is an "ANY" IP address and then continuously tracks
/// changes to the network interfaces to update the list of endpoints.
///
/// If the local address is not an "ANY" IP address, then the endpoints are updated immediately with
/// the local address and only that address and the future terminates.
///
/// In the endpoints tuple value, only the list of endpoints (the second element) is updated. The
/// first value (the local address) is never set by this function.
async fn update_address_endpoints(
    local_address: Address,
    endpoints_sender: &mut watch::Sender<(Address, Vec<Address>)>,
) {
    match local_address {
        // An "ANY" address, aka "unspecified".
        Address::Tcp {
            address: local_socket_address,
            ssl,
        } if local_socket_address.ip().is_unspecified() => {
            // Watch network interfaces changes to list all IP addresses of the host.
            let mut networks = sysinfo::Networks::new();
            loop {
                networks.refresh(true);
                let new_endpoints: Vec<_> = networks
                    .values()
                    .flat_map(|net| net.ip_networks())
                    .map(|ip_net| Address::Tcp {
                        address: SocketAddr::new(ip_net.addr, local_socket_address.port()),
                        ssl,
                    })
                    .collect();
                endpoints_sender.send_if_modified(move |(_, endpoints)| {
                    if endpoints != &new_endpoints {
                        *endpoints = new_endpoints;
                        true
                    } else {
                        false
                    }
                });
                time::sleep(NETWORK_INTERFACES_REFRESH_INTERVAL).await;
            }
        }
        // Not an any address, update endpoints and terminate.
        _ => endpoints_sender.send_modify(|(_, endpoints)| *endpoints = vec![local_address]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{auth, messaging::Message};
    use assert_matches::assert_matches;
    use bytes::Bytes;
    use futures::{channel::mpsc, SinkExt, StreamExt};
    use std::{convert::Infallible, future::Future};
    use tokio::spawn;

    #[derive(Clone, Copy)]
    struct DummyHandler;

    impl messaging::CallHandler for DummyHandler {
        type Error = HandlerError;

        #[allow(clippy::manual_async_fn)]
        fn handle_call(
            &mut self,
            _address: message::Address,
            args: Bytes,
        ) -> impl Future<Output = Result<Bytes, Self::Error>> + Send + 'static {
            async move { Ok(args) }
        }
    }

    impl messaging::EventHandler for DummyHandler {
        fn handle_event(&mut self, _address: message::Address, _args: Bytes) {}
    }
    impl messaging::PostHandler for DummyHandler {
        fn handle_post(&mut self, _address: message::Address, _args: Bytes) {}
    }
    impl messaging::CapabilitiesHandler for DummyHandler {
        fn handle_capabilities(&mut self, _address: message::Address, _map: KeyDynValueMap) {}
    }

    /// The server session receives an authentication request with incompatible capabilities.
    ///
    /// It is expected that:
    ///   1. the server replies to the request with an error.
    ///   2. the connection is closed.
    #[tokio::test]
    async fn server_sends_back_error_on_client_bad_capabilities() {
        // 0.1: start the server session
        let (mut send_to_server, server_recv) = mpsc::unbounded();
        let (server_send, mut recv_from_server) = mpsc::unbounded();
        let task = spawn(serve_client(
            server_recv.map(Ok::<_, Infallible>),
            server_send.sink_map_err(qi_messaging::Error::link_lost),
            None,
            DummyHandler,
        ));

        // 0.2: start the request
        send_to_server
            .send(Message::Call {
                id: message::Id(0),
                address: control::AUTHENTICATE_ADDRESS,
                payload: {
                    let mut map = KeyDynValueMap::new();
                    map.set("RemoteCancelableCalls", true);
                    map.set("ObjectPtrUID", true);
                    map.set("RelativeEndpointURI", false); // A required capabilities is set to false.
                    map
                }
                .into_format()
                .unwrap(),
            })
            .await
            .unwrap();

        // 1.
        let response = recv_from_server.next().await.unwrap();
        assert_matches!(
            response,
            Message::Error {
                address: control::AUTHENTICATE_ADDRESS,
                error,
                ..
            } => {
                assert!(error.contains("unexpected capability value"), "error is not an unexpected capability value: {error}")
            }
        );

        // 2.
        let () = task.await.unwrap();
    }

    /// The client session receives an authentication response with incompatible capabilities.
    ///
    /// It is expected that:
    ///   1. the connection is closed.
    ///   2. the error is reported back to the client user.
    #[tokio::test]
    async fn client_receives_bad_capabilities() {
        // 0.1: start the client session
        let (mut send_to_client, client_recv) = mpsc::unbounded();
        let (client_send, mut recv_from_client) = mpsc::unbounded();
        let task = spawn(Session::connect(
            client_recv.map(Ok::<_, Infallible>),
            client_send.sink_map_err(qi_messaging::Error::link_lost),
            Default::default(),
            DummyHandler,
        ));

        // 0.2: receive the request
        let request = recv_from_client.next().await.unwrap();
        assert_matches!(
            request,
            Message::Call {
                id: message::Id(1),
                address: control::AUTHENTICATE_ADDRESS,
                ..
            }
        );

        // 1: send the reply containing the capabilities
        send_to_client
            .send(Message::Reply {
                id: message::Id(1),
                address: control::AUTHENTICATE_ADDRESS,
                payload: {
                    let mut map = KeyDynValueMap::new();
                    map.set("RemoteCancelableCalls", true);
                    map.set("ObjectPtrUID", true);
                    map.set("RelativeEndpointURI", false); // A required capabilities is set to false.
                    map
                }
                .into_format()
                .unwrap(),
            })
            .await
            .unwrap();

        // 1. task terminates succesfully with a result in error.
        assert!(task.await.unwrap().is_err());
    }

    /// The server expects authentication parameters, the client sends correct ones.
    ///
    /// It is expected that:
    ///   1. the authentication succeeds.
    #[tokio::test]
    async fn client_sends_good_auth_parameters() {
        let auth = auth::UserTokenAuthenticator::new("myuser".to_owned(), "mytoken".to_owned());

        // 0.1: start the server session
        let (mut send_to_server, server_recv) = mpsc::unbounded();
        let (server_send, mut recv_from_server) = mpsc::unbounded();
        spawn(serve_client(
            server_recv.map(Ok::<_, Infallible>),
            server_send.sink_map_err(qi_messaging::Error::link_lost),
            Some(Arc::new(auth)),
            DummyHandler,
        ));

        // 0.2: start the request
        send_to_server
            .send(Message::Call {
                id: message::Id(0),
                address: control::AUTHENTICATE_ADDRESS,
                payload: {
                    let mut map = KeyDynValueMap::new();
                    map.set("RemoteCancelableCalls", true);
                    map.set("ObjectPtrUID", true);
                    map.set("RelativeEndpointURI", true);
                    map.set(auth::USER_KEY, "myuser");
                    map.set(auth::TOKEN_KEY, "mytoken");
                    map
                }
                .into_format()
                .unwrap(),
            })
            .await
            .unwrap();

        // 1.
        let response = recv_from_server.next().await.unwrap();
        let body = assert_matches!(
            response,
            Message::Reply {
                address: control::AUTHENTICATE_ADDRESS,
                id: message::Id(0),
                payload: body
            } => body
        );

        let mut map: KeyDynValueMap = body.to_reflect_value().unwrap();
        let state: u32 = map
            .remove(auth::STATE_KEY)
            .unwrap_or_else(|| panic!("missing state key in map {map:?}"))
            .cast_into()
            .expect("state value is not a u32");
        assert_eq!(state, auth::STATE_DONE);
    }

    /// The client sends bad authentication parameters.
    ///
    /// It is expected that:
    ///   1. the server replies with an error.
    ///   3. the error is reported back to the user of the client.
    ///   2. the connection is closed.
    #[tokio::test]
    async fn client_send_bad_auth_parameters() {
        let auth = auth::UserTokenAuthenticator::new("myuser".to_owned(), "mytoken".to_owned());

        // 0.1: start the server session
        let (mut send_to_server, server_recv) = mpsc::unbounded();
        let (server_send, mut recv_from_server) = mpsc::unbounded();
        let task = spawn(serve_client(
            server_recv.map(Ok::<_, Infallible>),
            server_send.sink_map_err(qi_messaging::Error::link_lost),
            Some(Arc::new(auth)),
            DummyHandler,
        ));

        // 0.2: start the request
        send_to_server
            .send(Message::Call {
                id: message::Id(0),
                address: control::AUTHENTICATE_ADDRESS,
                payload: {
                    let mut map = KeyDynValueMap::new();
                    map.set("RemoteCancelableCalls", true);
                    map.set("ObjectPtrUID", true);
                    map.set("RelativeEndpointURI", true);
                    map.set(auth::USER_KEY, "myuser");
                    map.set(auth::TOKEN_KEY, "badtoken"); // token is not correct
                    map
                }
                .into_format()
                .unwrap(),
            })
            .await
            .unwrap();

        // 1.
        let response = recv_from_server.next().await.unwrap();
        let error = assert_matches!(
            response,
            Message::Error {
                address: control::AUTHENTICATE_ADDRESS,
                error,
                ..
            } => error
        );
        assert!(
            error.contains("failure to verify authentication request"),
            "error is not an authentication failure: {error}"
        );

        // 2.
        let () = task.await.unwrap();
    }
}
