use std::borrow::Cow;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{}", s)]
    Message { s: Cow<'static, str> },

    #[error("Unexpected error running postgres command")]
    PgError(#[from] tokio_postgres::Error),

    #[error("Interacting with kafka: {0}")]
    KafkaError(#[from] rdkafka::error::KafkaError),

    #[error("Sending kafka message")]
    FuturesCancelled(#[from] futures_channel::oneshot::Canceled),

    #[error("Waiting for futures to complete")]
    JoinError(#[from] tokio::task::JoinError),

    #[error("Unable to parse schema json")]
    JsonError(#[from] serde_json::error::Error),
}

impl From<String> for Error {
    fn from(s: String) -> Error {
        Error::Message { s: Cow::from(s) }
    }
}

impl From<&'static str> for Error {
    fn from(s: &'static str) -> Error {
        Error::Message { s: Cow::from(s) }
    }
}
