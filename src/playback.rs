use crate::audio_backend::{OutputStreamInfo, SharedOutput};
use crate::decoder::{self, DecodeStreamInfo};
use crate::error::{EchoError, Result};
use crate::library::Track;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{
    Arc, Mutex,
    mpsc::{self, Receiver},
};
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
    Paused {
        title: String,
    },
    Resumed {
        title: String,
    },
    Stopped {
        title: String,
        elapsed_ms: u128,
    },
    Finished {
        title: String,
        elapsed_ms: u128,
    },
    Error {
        path: String,
        message: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaybackControl {
    Pause,
    Resume,
    Stop,
}

pub struct PlaybackEngine;

impl PlaybackEngine {
    pub fn new() -> Self {
        Self
    }

    pub fn play_blocking<F>(&self, track: &Track, on_event: F) -> Result<()>
    where
        F: FnMut(PlaybackEvent),
    {
        let (_control_tx, control_rx) = mpsc::channel();
        self.play_controlled(track, control_rx, on_event)
    }

    pub fn play_controlled<F>(
        &self,
        track: &Track,
        control_rx: Receiver<PlaybackControl>,
        mut on_event: F,
    ) -> Result<()>
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
        let stop_requested = Arc::new(AtomicBool::new(false));

        let output = SharedOutput::open(&stream_info, sample_rx, queued_samples.clone())?;
        for warning in output.info().warnings.clone() {
            on_event(PlaybackEvent::Warning(warning));
        }

        let decoder_path = path.clone();
        let decoder_done_for_thread = decoder_done.clone();
        let decoder_error_for_thread = decoder_error.clone();
        let queued_for_thread = queued_samples.clone();
        let stop_for_thread = stop_requested.clone();
        let output_sample_rate = output.info().sample_rate;
        let decoder_handle = thread::Builder::new()
            .name("echo-cli-decoder".to_string())
            .spawn(move || {
                if let Err(error) = decoder::decode_to_channel(
                    &decoder_path,
                    sample_tx,
                    queued_for_thread,
                    output_sample_rate,
                    stop_for_thread,
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

        let mut paused = false;
        let mut stopped = false;
        while !decoder_done.load(Ordering::Acquire) || queued_samples.load(Ordering::Acquire) > 0 {
            while let Ok(control) = control_rx.try_recv() {
                match control {
                    PlaybackControl::Pause if !paused => {
                        output.pause()?;
                        paused = true;
                        on_event(PlaybackEvent::Paused {
                            title: track.title.clone(),
                        });
                    }
                    PlaybackControl::Resume if paused => {
                        output.play()?;
                        paused = false;
                        on_event(PlaybackEvent::Resumed {
                            title: track.title.clone(),
                        });
                    }
                    PlaybackControl::Stop => {
                        stop_requested.store(true, Ordering::Release);
                        stopped = true;
                        break;
                    }
                    _ => {}
                }
            }

            if stopped {
                break;
            }

            thread::sleep(Duration::from_millis(30));
        }

        thread::sleep(Duration::from_millis(150));

        decoder_handle
            .join()
            .map_err(|_| EchoError::Playback("decoder thread panicked".to_string()))?;

        if stopped {
            on_event(PlaybackEvent::Stopped {
                title: track.title.clone(),
                elapsed_ms: started.elapsed().as_millis(),
            });
            return Ok(());
        }

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
