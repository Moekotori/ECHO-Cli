use thiserror::Error;

pub type Result<T> = std::result::Result<T, EchoError>;

#[derive(Debug, Error)]
pub enum EchoError {
    #[error("configuration path is unavailable on this system")]
    ConfigPathUnavailable,

    #[error("metadata read failed for {path}: {message}")]
    Metadata { path: String, message: String },

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Db(#[from] rusqlite::Error),

    #[error(transparent)]
    WalkDir(#[from] walkdir::Error),
}
