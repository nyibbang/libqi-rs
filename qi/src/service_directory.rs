use crate::{
    object::{self, Object, ObjectExt},
    service, session,
    signal::SignalConnection,
    value::{
        object::{ActionId, MetaMethod, MetaObject},
        os, Reflect, Value,
    },
};
use async_trait::async_trait;
use once_cell::sync::Lazy;
use qi_value::object::MetaSignal;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use tokio::sync::broadcast;

pub(super) const SD_SERVICE_NAME: &str = "ServiceDirectory";
const SD_SERVICE_ID: service::Id = service::Id(1);

#[qi_macros::object]
#[async_trait]
pub trait ServiceDirectory {
    type Error;

    #[qi::method]
    async fn services(&self) -> Result<Vec<service::Info>, Self::Error>;

    #[qi::method]
    async fn service(&self, name: &str) -> Result<service::Info, Self::Error>;

    #[qi::method(name = "registerService")]
    async fn register(&self, info: &service::Info) -> Result<service::Id, Self::Error>;

    #[qi::method(name = "unregisterService")]
    async fn unregister(&self, id: service::Id) -> Result<(), Self::Error>;

    #[qi::method(name = "serviceReady")]
    async fn set_ready(&self, id: service::Id) -> Result<(), Self::Error>;

    #[qi::method(name = "updateServiceInfo")]
    async fn update(&self, info: &service::Info) -> Result<(), Self::Error>;

    #[qi::signal(name = "serviceAdded", type = "(service::Id, String)")]
    fn connect_service_added(&self) -> SignalConnection<(service::Id, String)>;

    #[qi::signal(name = "serviceRemoved", type = "(service::Id, String)")]
    fn connect_service_removed(&self) -> SignalConnection<(service::Id, String)>;

    #[qi::method(name = "machineId")]
    async fn machine_id(&self) -> Result<os::MachineId, Self::Error>;
}

#[derive(Debug)]
pub struct ServiceInfoMap {
    pending_services: HashMap<service::Id, service::Info>,
    services: HashMap<service::Id, service::Info>,
    service_id_iter: ServiceIdIterator,
    added: broadcast::Sender<(service::Id, String)>,
    removed: broadcast::Sender<(service::Id, String)>,
}

impl Default for ServiceInfoMap {
    fn default() -> Self {
        let (added, _) = broadcast::channel(16);
        let (removed, _) = broadcast::channel(16);
        Self {
            pending_services: HashMap::default(),
            services: HashMap::default(),
            service_id_iter: ServiceIdIterator::default(),
            added,
            removed,
        }
    }
}

impl ServiceInfoMap {
    fn has_service(&self, name: &str) -> bool {
        self.pending_services
            .values()
            .chain(self.pending_services.values())
            .any(|info| info.name == name)
    }
}

#[async_trait]
impl ServiceDirectory for Arc<RwLock<ServiceInfoMap>> {
    type Error = Error;

    async fn services(&self) -> Result<Vec<service::Info>, Self::Error> {
        Ok(read(self).services.values().cloned().collect())
    }

    async fn service(&self, name: &str) -> Result<service::Info, Self::Error> {
        read(self)
            .services
            .values()
            .find(|info| info.name == name)
            .cloned()
            .ok_or_else(|| Error::ServiceNotFound(name.to_owned()))
    }

    async fn register(&self, info: &service::Info) -> Result<service::Id, Self::Error> {
        let mut this = write(self);
        if this.has_service(&info.name) {
            return Err(Error::ServiceAlreadyExists(info.name.clone()));
        }
        let id = this.service_id_iter.next().ok_or(Error::MaxIdReached)?;

        this.pending_services
            .insert(id, service::Info { id, ..info.clone() });
        Ok(id)
    }

    async fn unregister(&self, id: service::Id) -> Result<(), Self::Error> {
        write(self).services.remove(&id);
        Ok(())
    }

    async fn set_ready(&self, id: service::Id) -> Result<(), Self::Error> {
        let mut this = write(self);
        let service = this.pending_services.remove(&id);
        match service {
            Some(service) => {
                this.services.insert(id, service);
                Ok(())
            }
            None => Err(Error::PendingServiceNotFound(id)),
        }
    }

    async fn update(&self, info: &service::Info) -> Result<(), Self::Error> {
        let mut this = write(self);
        match this.services.get_mut(&info.id) {
            Some(service_info) => *service_info = info.clone(),
            None => match this.pending_services.get_mut(&info.id) {
                Some(service_info) => *service_info = info.clone(),
                None => {
                    return Err(Error::ServiceWithIdNotFound(info.id));
                }
            },
        };
        Ok(())
    }

    fn connect_service_added(&self) -> SignalConnection<(service::Id, String)> {
        todo!()
    }

    fn connect_service_removed(&self) -> SignalConnection<(service::Id, String)> {
        todo!()
    }

    async fn machine_id(&self) -> Result<os::MachineId, Self::Error> {
        Ok(os::MachineId::local())
    }
}

fn read(this: &RwLock<ServiceInfoMap>) -> std::sync::RwLockReadGuard<'_, ServiceInfoMap> {
    this.read().unwrap_or_else(|err| {
        this.clear_poison();
        err.into_inner()
    })
}

fn write(this: &RwLock<ServiceInfoMap>) -> std::sync::RwLockWriteGuard<'_, ServiceInfoMap> {
    this.write().unwrap_or_else(|err| {
        this.clear_poison();
        err.into_inner()
    })
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("service \"{0}\" not found")]
    ServiceNotFound(String),

    #[error("service with id \"{0}\" not found")]
    ServiceWithIdNotFound(service::Id),

    #[error("service \"{0}\" already exists")]
    ServiceAlreadyExists(String),

    #[error("maximum service id has been reached, cannot register any more service")]
    MaxIdReached,

    #[error("there is no pending service with id {0}")]
    PendingServiceNotFound(service::Id),
}

impl From<Error> for crate::Error {
    fn from(error: Error) -> Self {
        Self::Other(error.into())
    }
}

pub struct Client(object::ObjectClient);

impl Client {
    pub(super) fn new(session: session::Session) -> Self {
        Self(object::ObjectClient::new(
            SD_SERVICE_ID,
            service::MAIN_OBJECT_ID,
            object::Uid::default(),
            Meta::get().object.clone(),
            session,
        ))
    }
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Client").field(&self.0).finish()
    }
}

#[async_trait]
impl Object for Client {
    fn meta(&self) -> &MetaObject {
        self.0.meta()
    }

    async fn meta_call(
        &self,
        ident: object::ActionNameOrId,
        args: Value<'_>,
    ) -> Result<Value<'static>, crate::Error> {
        self.0.meta_call(ident, args).await
    }

    async fn meta_post(&self, ident: object::ActionNameOrId, value: Value<'_>) {
        self.0.meta_post(ident, value).await
    }

    async fn meta_event(&self, ident: object::ActionNameOrId, value: Value<'_>) {
        self.0.meta_event(ident, value).await
    }

    fn uid(&self) -> object::Uid {
        self.0.uid()
    }
}

#[async_trait]
impl ServiceDirectory for Client {
    type Error = crate::Error;

    async fn services(&self) -> Result<Vec<service::Info>, Self::Error> {
        self.0.call(Meta::get().services, ()).await
    }

    async fn service(&self, name: &str) -> Result<service::Info, Self::Error> {
        self.0.call(Meta::get().service, name).await
    }

    async fn register(&self, info: &service::Info) -> Result<service::Id, Self::Error> {
        self.0.call(Meta::get().register_service, info).await
    }

    async fn unregister(&self, id: service::Id) -> Result<(), Self::Error> {
        self.0.call(Meta::get().unregister_service, id).await
    }

    async fn set_ready(&self, id: service::Id) -> Result<(), Self::Error> {
        self.0.call(Meta::get().service_ready, id).await
    }

    async fn update(&self, info: &service::Info) -> Result<(), Self::Error> {
        self.0.call(Meta::get().update_service_info, info).await
    }

    fn connect_service_added(&self) -> SignalConnection<(service::Id, String)> {
        todo!()
    }

    fn connect_service_removed(&self) -> SignalConnection<(service::Id, String)> {
        todo!()
    }

    async fn machine_id(&self) -> Result<os::MachineId, Self::Error> {
        self.0.call(Meta::get().machine_id, ()).await
    }
}

#[derive(Debug)]
struct Meta {
    object: MetaObject,
    service: ActionId,
    services: ActionId,
    register_service: ActionId,
    unregister_service: ActionId,
    service_ready: ActionId,
    update_service_info: ActionId,
    service_added: ActionId,
    service_removed: ActionId,
    machine_id: ActionId,
}

impl Meta {
    fn get() -> &'static Self {
        static META: Lazy<Meta> = Lazy::new(|| {
            let service;
            let services;
            let register_service;
            let unregister_service;
            let service_ready;
            let update_service_info;
            let service_added;
            let service_removed;
            let machine_id;
            let mut action_id = object::ACTION_START_ID;
            let mut builder = MetaObject::builder();
            // Method: service
            builder.add_method({
                service = action_id.next().unwrap();
                let mut builder = MetaMethod::builder(service);
                builder.set_name("service");
                builder.parameter(0).set_type(<&str>::ty());
                builder.return_value().set_type(service::Info::ty());
                builder.build()
            });
            // Method: services
            builder.add_method({
                services = action_id.next().unwrap();
                let mut builder = MetaMethod::builder(services);
                builder.set_name("services");
                builder.return_value().set_type(Vec::<service::Info>::ty());
                builder.build()
            });
            // Method: register_service
            builder.add_method({
                register_service = action_id.next().unwrap();
                let mut builder = MetaMethod::builder(register_service);
                builder.set_name("registerService");
                builder.parameter(0).set_type(service::Info::ty());
                builder.return_value().set_type(service::Id::ty());
                builder.build()
            });
            // Method: unregister_service
            builder.add_method({
                unregister_service = action_id.next().unwrap();
                let mut builder = MetaMethod::builder(unregister_service);
                builder.set_name("unregisterService");
                builder.parameter(0).set_type(service::Id::ty());
                builder.build()
            });
            // Method: service_ready
            builder.add_method({
                service_ready = action_id.next().unwrap();
                let mut builder = MetaMethod::builder(service_ready);
                builder.set_name("serviceReady");
                builder.parameter(0).set_type(service::Id::ty());
                builder.build()
            });
            // Method: update_service_info
            builder.add_method({
                update_service_info = action_id.next().unwrap();
                let mut builder = MetaMethod::builder(update_service_info);
                builder.set_name("updateServiceInfo");
                builder.parameter(0).set_type(service::Info::ty());
                builder.build()
            });
            // Signal: service_added
            builder.add_signal({
                service_added = action_id.next().unwrap();
                MetaSignal {
                    uid: service_added,
                    name: "serviceAdded".to_owned(),
                    signature: <(service::Id, String)>::ty().into(),
                }
            });
            // Signal: service_removed
            builder.add_signal({
                service_removed = action_id.next().unwrap();
                MetaSignal {
                    uid: service_removed,
                    name: "serviceRemoved".to_owned(),
                    signature: <(service::Id, String)>::ty().into(),
                }
            });
            // Method: machine_id
            builder.add_method({
                machine_id = action_id.next().unwrap();
                let mut builder = MetaMethod::builder(machine_id);
                builder.set_name("machineId");
                builder.return_value().set_type(os::MachineId::ty());
                builder.build()
            });
            let object = builder.build();
            Meta {
                object,
                service,
                services,
                register_service,
                unregister_service,
                service_ready,
                update_service_info,
                service_added,
                service_removed,
                machine_id,
            }
            // service = { id = 100, text = "get a service (method: service)" },
            // services = { id = 101, text = "get all services (method: services)" },
            // register_service = { id = 102, text = "register a service (method: registerService)" },
            // unregister_service = { id = 103, text = "unregister a service (method: unregisterService)" },
            // service_ready = { id = 104, text = "a service is ready (method: serviceReady)" },
            // update_service_info = { id = 105, text = "update information of a service (method: updateServiceInfo)"},
            // service_added = { id = 106, text = "a service has been added (signal: serviceAdded)" },
            // service_removed = { id = 107, text = "a service has been removed (signal: serviceRemoved)" },
            // machine_id = { id = 108, text = "get the machine id (method: machineId)" },
        });
        &META
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ServiceIdIterator {
    current: u32,
}

impl Default for ServiceIdIterator {
    fn default() -> Self {
        Self {
            current: SD_SERVICE_ID.0 + 1,
        }
    }
}

impl Iterator for ServiceIdIterator {
    type Item = service::Id;

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.current;
        self.current = self.current.checked_add(1)?;
        Some(service::Id(current))
    }
}
