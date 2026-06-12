use crate::audio_backend::{OutputStreamInfo, SharedOutput};
use crate::decoder::{self, DecodeStreamInfo};
use crate::error::{EchoError, Result};
use crate::library::Track;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

const PLAYBACK_BUFFER_CHUNKS: usize = 12;
const PREBUFFER_TIMEOUT: Duration = Duration::from_millis(300);

#[allow(dead_code)]
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
    Loading {
        title: String,
        path: String,
    },
    Playing {
        title: String,
        stream: DecodeStreamInfo,
        output: OutputStreamInfo,
    },
    Warning(String),
    Finished {
        title: String,
        elapsed_ms: u128,
    },
    Error {
        path: String,
        message: String,
    },
}

pub struct PlaybackEngine;

impl PlaybackEngine {
    pub fn new() -> Self {
        Self
    }

    pub fn play_blocking<F>(&self, track: &Track, mut on_event: F) -> Result<()>
    where
        F: FnMut(PlaybackEvent),
    {
        let path = Path::new(&track.path).to_path_buf();
        let started = Instant::now();
        on_event(PlaybackEvent::Loading {
            title: track.title.clone(),
            path: track.path.clone(),
        });

        let stream_info = decoder::probe_stream(&path)?;
        let (sample_tx, sample_rx) = mpsc::sync_channel(PLAYBACK_BUFFER_CHUNKS);
        let queued_samples = Arc::new(AtomicI64::new(0));
        let decoder_done = Arc::new(AtomicBool::new(false));
        let decoder_error = Arc::new(Mutex::new(None::<String>));

        let output = SharedOutput::open(&stream_info, sample_rx, queued_samples.clone())?;
        for warning in output.info().warnings.clone() {
            on_event(PlaybackEvent::Warning(warning));
        }

        let decoder_path = path.clone();
        let decoder_done_for_thread = decoder_done.clone();
        let decoder_error_for_thread = decoder_error.clone();
        let queued_for_thread = queued_samples.clone();
        let output_sample_rate = output.info().sample_rate;
        let decoder_handle = thread::Builder::new()
            .name("echo-cli-decoder".to_string())
            .spawn(move || {
                if let Err(error) = decoder::decode_to_channel(
                    &decoder_path,
                    sample_tx,
                    queued_for_thread,
                    output_sample_rate,
                ) && let Ok(mut slot) = decoder_error_for_thread.lock()
                {
                    *slot = Some(error.to_string());
                }
                decoder_done_for_thread.store(true, Ordering::Release);
            })
            .map_err(|error| EchoError::Playback(error.to_string()))?;

        wait_for_prebuffer(&queued_samples, &decoder_done);
        output.play()?;
        on_event(PlaybackEvent::Playing {
            title: track.title.clone(),
            stream: stream_info,
            output: output.info().clone(),
        });

        while !decoder_done.load(Ordering::Acquire) || queued_samples.load(Ordering::Acquire) > 0 {
            thread::sleep(Duration::from_millis(30));
        }

        thread::sleep(Duration::from_millis(150));

        decoder_handle
            .join()
            .map_err(|_| EchoError::Playback("decoder thread panicked".to_string()))?;

        if let Some(message) = decoder_error.lock().ok().and_then(|mut slot| slot.take()) {
            on_event(PlaybackEvent::Error {
                path: track.path.clone(),
                message: message.clone(),
            });
            return Err(EchoError::Playback(message));
        }

        on_event(PlaybackEvent::Finished {
            title: track.title.clone(),
            elapsed_ms: started.elapsed().as_millis(),
        });
        Ok(())
    }
}

fn wait_for_prebuffer(queued_samples: &AtomicI64, decoder_done: &AtomicBool) {
    let started = Instant::now();
    while queued_samples.load(Ordering::Acquire) <= 0
        && !decoder_done.load(Ordering::Acquire)
        && started.elapsed() < PREBUFFER_TIMEOUT
    {
        thread::sleep(Duration::from_millis(10));
    }
}

impl Default for PlaybackEngine {
    fn default() -> Self {
        Self::new()
    }
}
