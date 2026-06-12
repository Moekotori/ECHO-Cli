use thiserror::Error;

pub type Result<T> = std::result::Result<T, EchoError>;

#[derive(Debug, Error)]
pub enum EchoError {
    #[error("configuration path is unavailable on this system")]
    ConfigPathUnavailable,

    #[error("metadata read failed for {path}: {message}")]
    Metadata { path: String, message: String },

    #[error("audio output failed: {0}")]
    Audio(String),

    #[error("decode failed for {path}: {message}")]
    Decode { path: String, message: String },

    #[error("playback failed: {0}")]
    Playback(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Db(#[from] rusqlite::Error),

    #[error(transparent)]
    WalkDir(#[from] walkdir::Error),
}
