//! Error types for the HA client.

use thiserror::Error;

/// Result type for HA operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Error type for HA operations.
#[derive(Error, Debug)]
pub enum Error {
    /// gRPC transport error
    #[error("gRPC transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    /// gRPC status error
    #[error("gRPC error: {0}")]
    Status(#[from] tonic::Status),

    /// SQLite error
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// URL parse error
    #[error("URL parse error: {0}")]
    UrlParse(#[from] url::ParseError),

    /// NATS error
    #[error("NATS error: {0}")]
    Nats(String),

    /// Query error from server
    #[error("Query error: {0}")]
    Query(String),

    /// Connection closed
    #[error("Connection is closed")]
    ConnectionClosed,

    /// Invalid parameter
    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),

    /// Timeout
    #[error("Operation timed out")]
    Timeout,

    /// Type conversion error
    #[error("Type conversion error: {0}")]
    TypeConversion(String),
}

impl From<async_nats::Error> for Error {
    fn from(e: async_nats::Error) -> Self {
        Error::Nats(e.to_string())
    }
}

impl From<async_nats::ConnectError> for Error {
    fn from(e: async_nats::ConnectError) -> Self {
        Error::Nats(e.to_string())
    }
}
