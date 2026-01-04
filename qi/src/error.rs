use crate::{
    format,
    messaging::{self, message},
    value,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("the call request has been canceled")]
    CallCanceled,

    #[error("there is no object method with identifier {0}")]
    MethodNotFound(value::object::ActionNameOrId),

    #[error(transparent)]
    Other(#[from] BoxError),
}

impl From<messaging::Error> for Error {
    fn from(err: messaging::Error) -> Self {
        match err {
            messaging::Error::LinkLost(error) => Self::Other(error),
            messaging::Error::CallError(error) => Self::Other(error.into()),
            messaging::Error::CallCanceled => Self::CallCanceled,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::Other(err.into())
    }
}

impl From<String> for Error {
    fn from(message: String) -> Self {
        Self::Other(message.into())
    }
}

impl From<&str> for Error {
    fn from(message: &str) -> Self {
        Self::Other(message.into())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ValueConversionError {
    #[error("the conversion of the object method return value has failed")]
    MethodReturnValue(#[source] value::FromValueError),

    #[error("the conversion of the request arguments has failed")]
    Arguments(#[source] value::FromValueError),
}

impl From<ValueConversionError> for Error {
    fn from(err: ValueConversionError) -> Self {
        Error::Other(err.into())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FormatError {
    #[error("the serialization of the request arguments has failed")]
    ArgumentsSerialization(#[source] format::Error),

    #[error("the deserialization of the request arguments has failed")]
    ArgumentsDeserialization(#[source] format::Error),

    #[error("the serialization of the method return value has failed")]
    MethodReturnValueSerialization(#[source] format::Error),

    #[error("the deserialization of the method return value has failed")]
    MethodReturnValueDeserialization(#[source] format::Error),
}

impl From<FormatError> for crate::Error {
    fn from(err: FormatError) -> Self {
        Error::Other(err.into())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("there is no handler for message of type {0} to address {1}")]
pub(crate) struct NoHandlerError(pub(crate) message::Type, pub(crate) message::Address);

impl From<NoHandlerError> for Error {
    fn from(err: NoHandlerError) -> Self {
        Self::Other(err.into())
    }
}

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum HandlerError {
    #[error("the call request has been canceled")]
    CallCanceled,

    #[error("{message}")]
    Custom { message: String, is_fatal: bool },
}

impl HandlerError {
    pub(crate) fn non_fatal<E>(err: E) -> Self
    where
        E: std::string::ToString,
    {
        Self::Custom {
            message: err.to_string(),
            is_fatal: false,
        }
    }

    pub(crate) fn fatal<E>(err: E) -> Self
    where
        E: std::string::ToString,
    {
        Self::Custom {
            message: err.to_string(),
            is_fatal: true,
        }
    }
}

impl messaging::handler::CallError for HandlerError {
    fn is_canceled(&self) -> bool {
        matches!(self, Self::CallCanceled)
    }

    fn is_fatal(&self) -> bool {
        matches!(self, Self::Custom { is_fatal: true, .. })
    }
}

impl From<Error> for HandlerError {
    fn from(err: Error) -> Self {
        if let Error::CallCanceled = err {
            return Self::CallCanceled;
        }
        Self::Custom {
            message: err.to_string(),
            is_fatal: true,
        }
    }
}

impl From<tokio::task::JoinError> for HandlerError {
    fn from(err: tokio::task::JoinError) -> Self {
        if err.is_cancelled() {
            return Self::CallCanceled;
        }
        Self::Custom {
            message: err.to_string(),
            is_fatal: false,
        }
    }
}
