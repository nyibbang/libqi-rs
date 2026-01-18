mod server;

use crate::{
    auth::Authenticator,
    service::{self, Info},
    service_directory::{self, ServiceDirectory},
    session,
    value::os::MachineId,
    Address, ArcObject, Error, Object, ObjectClient, Result,
};
use async_trait::async_trait;
use futures::{stream, StreamExt, TryStreamExt};
use qi_value::KeyDynValueMap;
use serde_with::serde_as;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use tokio::task;
use tracing::warn;

pub fn init() -> InitializingNode<NotSet> {
    InitializingNode::default()
}

#[derive(Default)]
pub struct InitializingNode<Method> {
    uid: Uid,
    authenticator: Option<Arc<dyn Authenticator + Send + Sync>>,
    bind_addresses: Vec<Address>,
    pending_services: HashMap<String, ArcObject>,
    services: service::SharedServices,
    method: Method,
}

impl<Method> InitializingNode<Method> {
    pub fn with_authenticator(
        &mut self,
        authenticator: Arc<dyn Authenticator + Send + Sync>,
    ) -> &mut Self {
        self.authenticator = Some(authenticator);
        self
    }

    pub fn add_service<Name>(
        &mut self,
        name: Name,
        object: Arc<dyn Object + Send + Sync>,
    ) -> &mut Self
    where
        Name: Into<String>,
    {
        self.pending_services.insert(name.into(), object.into());
        self
    }

    /// Binds the node to an address so that it may accept incoming connections on an endpoint at
    /// that address.
    pub fn bind(&mut self, address: Address) -> &mut Self {
        self.bind_addresses.push(address);
        self
    }

    /// Attaches the node to the space hosted at the given address.
    pub fn connect_to_space(
        self,
        address: Address,
        credentials: Option<KeyDynValueMap>,
    ) -> InitializingNode<ConnectToSpace> {
        InitializingNode {
            uid: self.uid,
            authenticator: self.authenticator,
            services: self.services,
            bind_addresses: self.bind_addresses,
            pending_services: self.pending_services,
            method: ConnectToSpace {
                address,
                credentials,
            },
        }
    }

    /// Host a new space on this node.
    pub fn host_space<A>(self) -> InitializingNode<HostSpace> {
        InitializingNode {
            uid: self.uid,
            authenticator: self.authenticator,
            services: self.services,
            bind_addresses: self.bind_addresses,
            pending_services: self.pending_services,
            method: HostSpace,
        }
    }
}

impl<M> InitializingNode<M>
where
    M: Method,
    M::ServiceDirectory: Clone + Send + Sync + 'static,
    <M::ServiceDirectory as ServiceDirectory>::Error: std::error::Error,
    Error: From<<M::ServiceDirectory as ServiceDirectory>::Error>,
{
    pub async fn start(self) -> Result<Node<M::ServiceDirectory>> {
        let services = self.services;
        let (server_set, mut endpoints_watcher) =
            server::start_servers(services.clone(), self.authenticator, self.bind_addresses)
                .await?;
        let session_store = session::Store::new(services.clone());
        let service_directory = self.method.create_service_directory(&session_store).await?;

        // Register each service to the directory, and mark them as ready.
        let server_endpoints = endpoints_to_client_targets(&endpoints_watcher.borrow_and_update());
        stream::iter(self.pending_services)
            .map(Ok)
            .try_for_each_concurrent(None, |(service_name, service_object)| {
                Self::register_pending_service(
                    self.uid.clone(),
                    &services,
                    &service_directory,
                    service_name,
                    service_object,
                    server_endpoints.clone(),
                )
            })
            .await?;

        // Update services info to the service directory whenever the server endpoints change.

        task::spawn({
            let service_directory = service_directory.clone();
            async move {
                while let Ok(()) = endpoints_watcher.changed().await {
                    let server_endpoints =
                        endpoints_to_client_targets(&endpoints_watcher.borrow_and_update());
                    stream::iter(services.lock().await.info_mut())
                        .for_each_concurrent(None, |service_info| async {
                            service_info.endpoints = server_endpoints.clone();
                            if let Err(err) = service_directory.update(service_info).await {
                                warn!(
                                    error = &err as &dyn std::error::Error,
                                    "could not update service info to service directory"
                                )
                            }
                        })
                        .await;
                }
            }
        });

        Ok(Node {
            uid: self.uid,
            session_store,
            service_directory,
            server_set,
        })
    }

    async fn register_pending_service<SD>(
        uid: Uid,
        services: &service::SharedServices,
        service_directory: &SD,
        name: String,
        object: ArcObject,
        endpoints: Vec<session::Target>,
    ) -> Result<()>
    where
        SD: ServiceDirectory,
        Error: From<SD::Error>,
    {
        let mut info = service::Info::unregistered(name, endpoints, uid, object.uid());
        // Registering the service to the directory gets us a service ID, that we can use to
        // update the local service info. With it, we can also index the service to the
        // messaging handler so that it can start treating requests for that service.
        // Consequently, we can notify the service directory of the readiness of the service.
        let service_id = service_directory.register(&info).await?;
        info.id = service_id;
        services.add(info, object).await;
        service_directory.set_ready(service_id).await?;
        Ok::<_, Error>(())
    }
}

impl<Method> std::fmt::Debug for InitializingNode<Method>
where
    Method: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Builder")
            .field("uid", &self.uid)
            .field("bind_addresses", &self.bind_addresses)
            .field("pending_services", &self.pending_services)
            .field("method", &self.method)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct Node<SD> {
    uid: Uid,
    session_store: session::Store,
    service_directory: SD,
    server_set: server::ServerSet,
}

impl<SD> Node<SD>
where
    SD: ServiceDirectory,
    Error: From<SD::Error>,
{
    pub async fn service(&self, name: &str) -> Result<impl Object + Clone> {
        let service = self.service_directory.service(name).await?;
        let session = self
            .session_store
            .get_or_create(
                name,
                sort_service_endpoints(&service),
                // Connecting to service nodes of a space should not require credentials.
                Default::default(),
            )
            .await?;
        let object = ObjectClient::connect(
            service.id(),
            service::MAIN_OBJECT_ID,
            service.object_uid(),
            session,
        )
        .await?;
        Ok(object)
    }

    pub fn service_directory(&self) -> &SD {
        &self.service_directory
    }
}

fn sort_service_endpoints(service: &Info) -> Vec<session::Target> {
    let service_is_local = service.machine_id() == MachineId::local();
    let mut endpoints = service.endpoints().to_vec();
    endpoints.sort_by_cached_key(|endpoint| {
        (
            endpoint.is_service_relative(),
            service_is_local && endpoint.is_machine_local(),
        )
    });
    endpoints
}

fn endpoints_to_client_targets(endpoints: &HashMap<Address, Vec<Address>>) -> Vec<session::Target> {
    let mut targets: Vec<_> = endpoints
        .values()
        .flatten()
        .copied()
        .map(session::Target::from)
        .collect();
    targets.sort();
    targets.dedup();
    targets
}

#[derive(
    Default,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Debug,
    qi_macros::Valuable,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
)]
#[serde_as]
#[qi(value(crate = "crate::value", transparent))]
pub struct Uid(String);

impl Uid {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    pub fn from_string(id: String) -> Self {
        Self(id)
    }
}

impl std::fmt::Display for Uid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for Uid {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self::from_string(s.to_owned()))
    }
}

#[async_trait]
pub trait Method {
    type ServiceDirectory: ServiceDirectory;

    async fn create_service_directory(
        self,
        store: &session::Store,
    ) -> Result<Self::ServiceDirectory>;
}

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct NotSet;

#[derive(Debug, PartialEq, Eq)]
pub struct ConnectToSpace {
    address: Address,
    credentials: Option<KeyDynValueMap>,
}

#[async_trait]
impl Method for ConnectToSpace {
    type ServiceDirectory = service_directory::Client;

    async fn create_service_directory(
        self,
        store: &session::Store,
    ) -> Result<Self::ServiceDirectory> {
        let session = store
            .get_or_create(
                service_directory::SD_SERVICE_NAME,
                [self.address.into()],
                self.credentials.unwrap_or_default(),
            )
            .await?;
        Ok(service_directory::Client::new(session))
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HostSpace;

#[async_trait]
impl Method for HostSpace {
    type ServiceDirectory = Arc<RwLock<service_directory::ServiceInfoMap>>;

    async fn create_service_directory(
        self,
        _store: &session::Store,
    ) -> Result<Self::ServiceDirectory> {
        Ok(Arc::default())
    }
}
