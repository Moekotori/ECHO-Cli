#![allow(dead_code)]

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlaybackCommand {
    Play(String),
    Pause,
    Resume,
    Stop,
    Next,
    Previous,
    SeekMillis(u64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlaybackEvent {
    Idle,
    Loading(String),
    Playing(String),
    Paused(String),
    Finished(String),
    Error { path: String, message: String },
}
