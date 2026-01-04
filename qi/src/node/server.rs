use crate::{
    auth::Authenticator,
    error::HandlerError,
    messaging::{self, Address},
    session,
};
use std::{collections::HashMap, sync::Arc};
use tokio::{sync::watch, task};

/// A set of servers with their aggregated endpoints.
///
/// Drops all server tasks when the set is dropped.
#[derive(Default, Debug)]
pub(super) struct ServerSet {
    _servers: Vec<session::Server>,
    _update_endpoints_tasks: task::JoinSet<()>,
}

pub(super) type EndpointsWatcher = watch::Receiver<HashMap<Address, Vec<Address>>>;

/// Instantiates a set of servers for a list of addresses and aggregates their endpoints.
///
/// For each server created, a task is spawned that will track changes to its endpoints.
///
/// If any server fails to bind to its address, then the future terminates with an error and all
/// created servers are stopped.
pub(super) async fn start_servers<Handler>(
    handler: Handler,
    authenticator: Option<Arc<dyn Authenticator + Send + Sync>>,
    addresses: impl IntoIterator<Item = Address>,
) -> Result<(ServerSet, EndpointsWatcher), std::io::Error>
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
    let (endpoints_sender, endpoints_receiver) = watch::channel(Default::default());
    let mut servers = Vec::new();
    let mut update_endpoints_tasks = task::JoinSet::new();
    for address in addresses {
        let (server, mut server_endpoints) =
            session::server(address, authenticator.clone(), handler.clone()).await?;
        servers.push(server);
        let endpoints_sender = endpoints_sender.clone();
        update_endpoints_tasks.spawn(async move {
            while let Ok(()) = server_endpoints.changed().await {
                endpoints_sender.send_modify(|endpoints: &mut HashMap<_, _>| {
                    let server_endpoints = server_endpoints.borrow_and_update();
                    endpoints.insert(server_endpoints.0, server_endpoints.1.clone());
                });
            }
        });
    }
    Ok((
        ServerSet {
            _servers: servers,
            _update_endpoints_tasks: update_endpoints_tasks,
        },
        endpoints_receiver,
    ))
}
