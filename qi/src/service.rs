use crate::{
    messaging::{self, message},
    node, object, session,
    value::{self, os, FormatInto, IntoFormat},
    ArcObject, Error, HandlerError, NoHandlerError, Object, Result,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{FutureExt, TryFutureExt};
use qi_value::RuntimeReflect;
use std::{collections::HashMap, future::Future, sync::Arc};
use tokio::{
    sync::{Mutex, OwnedMutexGuard},
    task,
};
use tracing::info;
pub use value::ServiceId as Id;

pub(super) const MAIN_OBJECT_ID: object::Id = object::Id(1);
const UNSPECIFIED_ID: Id = Id(0);

#[derive(Default, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, qi_macros::Valuable)]
#[qi(value(crate = "crate::value", case = "camelCase"))]
pub struct Info {
    pub(super) name: String,
    #[qi(value(name = "serviceId"))]
    pub(super) id: Id,
    pub(super) machine_id: os::MachineId,
    pub(super) process_id: u32,
    pub(super) endpoints: Vec<session::Target>,
    #[qi(value(name = "sessionId"))]
    pub(super) node_uid: node::Uid,
    /// Object uid in service info are represented as strings containing pure binary data for
    /// compatibility reasons. They are therefore NOT UTF-8 valid strings or even contain printable
    /// characters.
    pub(super) object_uid: ObjectUidAsStr,
}

impl Info {
    pub(super) fn unregistered(
        name: String,
        endpoints: Vec<session::Target>,
        node_uid: node::Uid,
        object_uid: object::Uid,
    ) -> Self {
        Self::process_local(name, UNSPECIFIED_ID, endpoints, node_uid, object_uid)
    }

    pub(super) fn process_local(
        name: String,
        id: Id,
        endpoints: Vec<session::Target>,
        node_uid: node::Uid,
        object_uid: object::Uid,
    ) -> Self {
        Self {
            name,
            id,
            machine_id: os::MachineId::local(),
            process_id: std::process::id(),
            endpoints,
            node_uid,
            object_uid: ObjectUidAsStr(object_uid),
        }
    }

    pub fn id(&self) -> Id {
        self.id
    }

    pub fn machine_id(&self) -> os::MachineId {
        self.machine_id
    }

    pub fn process_id(&self) -> u32 {
        self.process_id
    }

    pub fn endpoints(&self) -> &[session::Target] {
        &self.endpoints
    }

    pub fn node_uid(&self) -> node::Uid {
        self.node_uid.clone()
    }

    pub fn object_uid(&self) -> object::Uid {
        self.object_uid.0
    }
}

impl std::fmt::Display for Info {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Info {
            name,
            id: service_id,
            machine_id,
            process_id,
            endpoints,
            node_uid,
            object_uid,
        } = self;
        write!(
            f,
            "{name}({service_id}, machine={machine_id}, \
                process={process_id}, \
                endpoints=["
        )?;
        for endpoint in endpoints {
            endpoint.fmt(f)?;
        }
        write!(
            f,
            "], node={node_uid}, \
                object={object_uid})"
        )
    }
}

#[derive(
    Default,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    derive_more::Display,
    derive_more::From,
    derive_more::Into,
)]
pub(super) struct ObjectUidAsStr(pub object::Uid);

impl value::Reflect for ObjectUidAsStr {
    fn ty() -> Option<value::Type> {
        Some(value::Type::String)
    }
}

impl value::RuntimeReflect for ObjectUidAsStr {
    fn ty(&self) -> value::Type {
        value::Type::String
    }
}

impl value::ToValue for ObjectUidAsStr {
    fn to_value(&self) -> value::Value<'_> {
        value::String::from_maybe_utf8(self.0.bytes()).into()
    }
}

impl<'a> value::IntoValue<'a> for ObjectUidAsStr {
    fn into_value(self) -> value::Value<'a> {
        value::String::from_maybe_utf8_owned(self.0.bytes().to_vec()).into()
    }
}

impl<'a> value::FromValue<'a> for ObjectUidAsStr {
    fn from_value(value: value::Value<'a>) -> std::result::Result<Self, value::FromValueError> {
        let value_type = value.ty();
        let value_str = value
            .into_string()
            .ok_or_else(|| value::FromValueError::TypeMismatch {
                expected: "an Object UID".to_owned(),
                actual: value_type.to_string(),
            })?;
        let bytes = <[u8; 20]>::try_from(value_str.as_bytes())
            .map_err(|err| value::FromValueError::Other(err.into()))?;
        Ok(Self(bytes.into()))
    }
}

#[derive(Default)]
struct Service {
    info: Info,
    bound_objects: HashMap<object::Id, ArcObject>,
}

impl Service {
    pub(super) fn new(info: Info, main_object: ArcObject) -> Self {
        Self {
            info,
            bound_objects: [(MAIN_OBJECT_ID, main_object)].into_iter().collect(),
        }
    }
}

impl std::fmt::Debug for Service {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Service")
            .field("info", &self.info)
            .field("bound_objects", &self.bound_objects.keys())
            .finish()
    }
}

#[derive(Debug, Default)]
pub(super) struct Services(HashMap<Id, Service>);

impl Services {
    fn insert_handler(&mut self, info: Info, service_object: ArcObject) {
        self.0.insert(info.id(), Service::new(info, service_object));
    }

    pub(super) fn info_mut(&mut self) -> impl Iterator<Item = &mut Info> {
        self.0.values_mut().map(|data| &mut data.info)
    }

    async fn route_call(&self, address: message::Address, args: Bytes) -> Result<Bytes> {
        let (object, ident) = self
            .get_request_handler(address)
            .ok_or(NoHandlerError(message::Type::Call, address))?;
        object.handle_meta_call(ident, args).await
    }

    async fn route_post(&self, address: message::Address, args: Bytes) {
        let (object, action) = match self.get_request_handler(address) {
            Some(handler) => handler,
            None => {
                info!(%address, "post request discarded: no handler");
                return;
            }
        };
        object.handle_meta_post(action, args).await
    }

    async fn route_event(&self, address: message::Address, args: Bytes) {
        let (object, action) = match self.get_request_handler(address) {
            Some(handler) => handler,
            None => {
                info!(%address, "event request discarded: no handler");
                return;
            }
        };
        object.handle_meta_event(action, args).await
    }

    fn get_request_handler(
        &self,
        address: message::Address,
    ) -> Option<(&ArcObject, object::ActionId)> {
        let message::Address(service_id, object_id, action_id) = address;
        let object = self
            .0
            .get(&service_id)
            .and_then(|service| service.bound_objects.get(&object_id))?;
        Some((object, action_id))
    }
}

/// A messaging handler-like interface for objects.
///
/// Messaging handlers take messaging address as parameter, while this interface only takes action
/// identifiers (so without the service and object identifiers in messaging addresses).
#[async_trait]
trait HandlerObject: Object {
    async fn handle_meta_call(&self, id: object::ActionId, args: Bytes) -> Result<Bytes> {
        // Get the target method so that we can get the expected parameters type and know what type
        // of value we're supposed to deserialize.
        let action_name_or_id = object::ActionNameOrId::Id(id);
        let method = self
            .meta()
            .method(&action_name_or_id)
            .ok_or_else(|| Error::MethodNotFound(action_name_or_id.clone()))?;
        self.meta_call(
            action_name_or_id,
            args.to_args(method.parameters_signature.as_type())?,
        )
        .await?
        .into_format_return_value()
    }

    async fn handle_meta_post<'a>(&'a self, id: object::ActionId, args: Bytes) {
        // Same as for "call", we need to know the type of parameters to know what to deserialize.
        let action_name_or_id = object::ActionNameOrId::Id(id);
        let action = match object::PostAction::get(self.meta(), &action_name_or_id) {
            Some(action) => action,
            None => {
                info!(
                    action = %action_name_or_id,
                    "post request discarded: action not found"
                );
                return;
            }
        };
        match args.to_args(action.parameters_signature().as_type()) {
            Ok(args) => self.meta_post(action_name_or_id, args).await,
            Err(err) => info!(
                error = &err as &dyn std::error::Error,
                "post request discarded: failed to deserialize arguments"
            ),
        };
    }

    async fn handle_meta_event<'a>(&'a self, action: object::ActionId, args: Bytes) {
        let action_name_or_id = object::ActionNameOrId::Id(action);
        let signal = match self.meta().signal(&action_name_or_id) {
            Some(signal) => signal,
            None => {
                info!(
                    signal = %action_name_or_id,
                    "event request discarded: signal not found"
                );
                return;
            }
        };
        match args.to_args(signal.signature.as_type()) {
            Ok(args) => self.meta_event(action_name_or_id, args).await,
            Err(err) => info!(
                error = &err as &dyn std::error::Error,
                "event request discarded: failed to deserialize arguments"
            ),
        };
    }
}

impl<O> HandlerObject for O where O: Object + Sync + ?Sized {}

/// A messaging handler that routes requests to services.
#[derive(Default, Clone, Debug)]
pub(super) struct SharedServices {
    services: Arc<Mutex<Services>>,
}

impl SharedServices {
    pub(super) async fn add(&self, info: Info, object: ArcObject) {
        self.services.lock().await.insert_handler(info, object)
    }

    pub(super) async fn lock(&self) -> OwnedMutexGuard<Services> {
        Arc::clone(&self.services).lock_owned().await
    }
}

impl messaging::CallHandler for SharedServices {
    type Error = HandlerError;

    fn handle_call(
        &mut self,
        address: message::Address,
        value: Bytes,
    ) -> impl Future<Output = std::result::Result<Bytes, Self::Error>> + Send + 'static {
        let handlers = Arc::clone(&self.services);
        task::spawn(async move {
            handlers
                .lock_owned()
                .await
                .route_call(address, value)
                .await
                .map_err(Into::into)
        })
        .map_err(Into::into)
        .map(|res| res.flatten())
    }
}

impl messaging::EventHandler for SharedServices {
    fn handle_event(&mut self, address: message::Address, args: Bytes) {
        let handlers = Arc::clone(&self.services);
        task::spawn(async move { handlers.lock_owned().await.route_event(address, args).await });
    }
}

impl messaging::PostHandler for SharedServices {
    fn handle_post(&mut self, address: message::Address, args: Bytes) {
        let handlers = Arc::clone(&self.services);
        task::spawn(async move { handlers.lock_owned().await.route_post(address, args).await });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messaging;
    use messaging::Address;
    use std::net::{Ipv4Addr, SocketAddr};

    #[test]
    fn service_info_from_format_value() {
        let value_in = &[
            0x0a, 0x00, 0x00, 0x00, 0x43, 0x61, 0x6c, 0x63, 0x75, 0x6c, 0x61, 0x74, 0x6f, 0x72,
            0x02, 0x00, 0x00, 0x00, 0x24, 0x00, 0x00, 0x00, 0x39, 0x61, 0x36, 0x35, 0x62, 0x35,
            0x36, 0x65, 0x2d, 0x63, 0x33, 0x64, 0x33, 0x2d, 0x34, 0x34, 0x38, 0x35, 0x2d, 0x38,
            0x39, 0x32, 0x34, 0x2d, 0x36, 0x36, 0x31, 0x62, 0x30, 0x33, 0x36, 0x32, 0x30, 0x32,
            0x62, 0x33, 0x46, 0x31, 0x34, 0x00, 0x02, 0x00, 0x00, 0x00, 0x0d, 0x00, 0x00, 0x00,
            0x71, 0x69, 0x3a, 0x43, 0x61, 0x6c, 0x63, 0x75, 0x6c, 0x61, 0x74, 0x6f, 0x72, 0x15,
            0x00, 0x00, 0x00, 0x74, 0x63, 0x70, 0x3a, 0x2f, 0x2f, 0x31, 0x32, 0x37, 0x2e, 0x30,
            0x2e, 0x30, 0x2e, 0x31, 0x3a, 0x34, 0x31, 0x36, 0x38, 0x31, 0x24, 0x00, 0x00, 0x00,
            0x33, 0x36, 0x31, 0x65, 0x63, 0x65, 0x63, 0x34, 0x2d, 0x30, 0x30, 0x66, 0x37, 0x2d,
            0x34, 0x63, 0x39, 0x34, 0x2d, 0x61, 0x36, 0x65, 0x32, 0x2d, 0x64, 0x39, 0x31, 0x65,
            0x32, 0x38, 0x63, 0x35, 0x61, 0x30, 0x36, 0x63, 0x14, 0x00, 0x00, 0x00, 0xfd, 0xeb,
            0xc1, 0x2e, 0xcb, 0xea, 0x6b, 0x58, 0xcc, 0x42, 0x20, 0xb7, 0x33, 0x3d, 0xc4, 0xe1,
            0x0d, 0x8a, 0xd6, 0x16,
        ][..];
        let service_info: Info = value_in.to_reflect_value().unwrap();
        assert_eq!(
            service_info,
            Info {
                name: "Calculator".to_owned(),
                id: Id(2),
                machine_id: "9a65b56e-c3d3-4485-8924-661b036202b3".parse().unwrap(),
                process_id: 3420486,
                endpoints: vec![
                    session::Target::service("Calculator"),
                    session::Target::from(Address::Tcp {
                        address: SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 41681),
                        ssl: None
                    })
                ],
                node_uid: node::Uid::from_string("361ecec4-00f7-4c94-a6e2-d91e28c5a06c".to_owned()),
                object_uid: ObjectUidAsStr(object::Uid::from([
                    0xfd, 0xeb, 0xc1, 0x2e, 0xcb, 0xea, 0x6b, 0x58, 0xcc, 0x42, 0x20, 0xb7, 0x33,
                    0x3d, 0xc4, 0xe1, 0x0d, 0x8a, 0xd6, 0x16
                ]))
            }
        )
    }

    #[test]
    fn object_uid_from_to_format() {
        let value_in = &[
            0x14, 0x00, 0x00, 0x00, 0xfd, 0xeb, 0xc1, 0x2e, 0xcb, 0xea, 0x6b, 0x58, 0xcc, 0x42,
            0x20, 0xb7, 0x33, 0x3d, 0xc4, 0xe1, 0x0d, 0x8a, 0xd6, 0x16,
        ][..];
        let object_uid: ObjectUidAsStr = value_in.to_reflect_value().unwrap();
        assert_eq!(
            object_uid.0,
            [
                0xfd, 0xeb, 0xc1, 0x2e, 0xcb, 0xea, 0x6b, 0x58, 0xcc, 0x42, 0x20, 0xb7, 0x33, 0x3d,
                0xc4, 0xe1, 0x0d, 0x8a, 0xd6, 0x16
            ]
        );
        let value_out = object_uid.into_format().unwrap();
        assert_eq!(value_out, value_in);
    }
}
