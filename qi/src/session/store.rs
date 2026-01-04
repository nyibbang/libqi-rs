use crate::{
    messaging::{self, Address},
    service,
    session::{self, target::Kind, Session, WeakSession},
    Error,
};
use qi_value::KeyDynValueMap;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

/// A session store that handles session targets and keeps track of existing
/// sessions to a space services.
///
/// It creates new sessions that are associated with services and register them
/// for further retrieval, enabling usage of service session targets.
#[derive(Debug)]
pub struct Store {
    services: service::SharedServices,

    /// The list of existing sessions with the associated service name.
    sessions: Arc<Mutex<HashMap<String, WeakSession>>>,
}

impl Store {
    pub(crate) fn new(services: service::SharedServices) -> Self {
        let sessions = Default::default();
        Self {
            services,
            sessions: Arc::clone(&sessions),
        }
    }

    async fn get(&self, name: &str) -> Option<Session> {
        let mut sessions = self.sessions.lock().await;
        match sessions.get(name) {
            Some(weak) => {
                let session = weak.upgrade();
                if session.is_none() {
                    sessions.remove(name);
                }
                session
            }
            None => None,
        }
    }

    /// Gets a session to the given targets, using the service name to store
    /// any created session for further retrieval.
    pub(crate) async fn get_or_create<Targets>(
        &self,
        service_name: &str,
        targets: Targets,
        credentials: KeyDynValueMap,
    ) -> Result<Session, Error>
    where
        for<'t> &'t Targets: IntoIterator<Item = &'t session::Target>,
    {
        let targets = targets.into_iter();
        let mut connection_errors = Vec::with_capacity({
            let (lower_bound, upper_bound) = targets.size_hint();
            upper_bound.unwrap_or(lower_bound)
        });
        for target in targets {
            match target.kind() {
                Kind::Service(service) => {
                    if let Some(session) = self.get(service).await {
                        return Ok(session);
                    }
                }
                Kind::Endpoint(address) => {
                    match self
                        .create(service_name, *address, credentials.clone())
                        .await
                    {
                        Err(err) => connection_errors.push(ConnectionError {
                            address: *address,
                            source: err.into(),
                        }),
                        Ok(session) => return Ok(session),
                    }
                }
            }
        }
        Err(UnreachableServiceError(service_name.to_owned(), connection_errors).into())
    }

    /// Opens a new channel to the address and connects a session client over
    /// it. The service name is used to register the session for this service,
    /// so that it may be reused for future service relative session targets.
    async fn create(
        &self,
        service_name: &str,
        address: messaging::Address,
        credentials: KeyDynValueMap,
    ) -> Result<Session, Error> {
        let (messages_in, messages_out) = messaging::channel::connect(address).await?;
        let session = Session::connect(
            messages_in,
            messages_out,
            credentials,
            self.services.clone(),
        )
        .await?;
        let service_name = service_name.to_owned();
        self.sessions
            .lock()
            .await
            .insert(service_name.clone(), session.downgrade());
        Ok(session)
    }
}

#[derive(Debug)]
struct UnreachableServiceError(String, Vec<ConnectionError>);

impl std::fmt::Display for UnreachableServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "could not reach service \"{}\", ", self.0)?;
        if self.0.is_empty() {
            f.write_str("no connection was tried")
        } else {
            f.write_str("tried the following connections: [")?;
            for error in &self.1 {
                write!(f, "{} => {}", error.address, error.source)?;
            }
            f.write_str("]")
        }
    }
}

impl std::error::Error for UnreachableServiceError {}

impl From<UnreachableServiceError> for Error {
    fn from(err: UnreachableServiceError) -> Self {
        Self::Other(err.into())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("could not connect to address \"{address}\"")]
pub(super) struct ConnectionError {
    address: Address,
    source: Box<dyn std::error::Error + Send + Sync>,
}
