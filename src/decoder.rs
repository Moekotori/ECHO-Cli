use crate::error::{EchoError, Result};
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{
    Arc,
    mpsc::{Receiver, Sender, SyncSender, TryRecvError, TrySendError},
};
use symphonia::core::audio::SampleBuffer;
use symphonia::core::codecs::{CODEC_TYPE_NULL, DecoderOptions};
use symphonia::core::errors::Error as SymphoniaError;
use symphonia::core::formats::{FormatOptions, SeekMode, SeekTo};
use symphonia::core::io::MediaSourceStream;
use symphonia::core::meta::MetadataOptions;
use symphonia::core::probe::Hint;
use symphonia::core::units::Time;
use symphonia::default::{get_codecs, get_probe};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeStreamInfo {
    pub sample_rate: u32,
    pub channel_count: u16,
    pub bit_depth: Option<u32>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SampleChunk {
    pub generation: u64,
    pub samples: Vec<f32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecoderCommand {
    SeekToMillis { position_ms: u64, generation: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecoderEvent {
    Seeked { position_ms: u64, generation: u64 },
}

pub fn probe_stream(path: &Path) -> Result<DecodeStreamInfo> {
    let source = File::open(path)?;
    let media_source = MediaSourceStream::new(Box::new(source), Default::default());
    let mut hint = Hint::new();
    if let Some(extension) = path.extension().and_then(|value| value.to_str()) {
        hint.with_extension(extension);
    }

    let probed = get_probe()
        .format(
            &hint,
            media_source,
            &FormatOptions::default(),
            &MetadataOptions::default(),
        )
        .map_err(|error| decode_error(path, error))?;

    let track = probed
        .format
        .tracks()
        .iter()
        .find(|track| track.codec_params.codec != CODEC_TYPE_NULL)
        .ok_or_else(|| EchoError::Decode {
            path: path.to_string_lossy().to_string(),
            message: "no supported audio track found".to_string(),
        })?;

    stream_info_from_params(path, track.codec_params.clone())
}

pub fn decode_to_channel(
    path: &Path,
    sample_tx: SyncSender<SampleChunk>,
    queued_samples: Arc<AtomicI64>,
    target_sample_rate: u32,
    stop_requested: Arc<AtomicBool>,
    requested_generation: Arc<AtomicU64>,
    command_rx: Receiver<DecoderCommand>,
    event_tx: Sender<DecoderEvent>,
) -> Result<()> {
    let source = File::open(path)?;
    let media_source = MediaSourceStream::new(Box::new(source), Default::default());
    let mut hint = Hint::new();
    if let Some(extension) = path.extension().and_then(|value| value.to_str()) {
        hint.with_extension(extension);
    }

    let probed = get_probe()
        .format(
            &hint,
            media_source,
            &FormatOptions::default(),
            &MetadataOptions::default(),
        )
        .map_err(|error| decode_error(path, error))?;

    let mut format = probed.format;
    let track = format
        .tracks()
        .iter()
        .find(|track| track.codec_params.codec != CODEC_TYPE_NULL)
        .ok_or_else(|| EchoError::Decode {
            path: path.to_string_lossy().to_string(),
            message: "no supported audio track found".to_string(),
        })?;

    let track_id = track.id;
    let codec_params = track.codec_params.clone();
    let time_base = codec_params.time_base;
    let source_sample_rate = codec_params.sample_rate.ok_or_else(|| EchoError::Decode {
        path: path.to_string_lossy().to_string(),
        message: "missing sample rate".to_string(),
    })?;
    let source_channels = codec_params
        .channels
        .map(|channels| channels.count())
        .filter(|channels| *channels > 0)
        .ok_or_else(|| EchoError::Decode {
            path: path.to_string_lossy().to_string(),
            message: "missing channel layout".to_string(),
        })?;
    let mut sample_rate_adapter =
        SampleRateAdapter::new(source_sample_rate, target_sample_rate, source_channels);
    let mut decoder = get_codecs()
        .make(&codec_params, &DecoderOptions::default())
        .map_err(|error| decode_error(path, error))?;
    let mut generation = 0;

    loop {
        if stop_requested.load(Ordering::Acquire) {
            break;
        }

        while let Some(command) = next_decoder_command(&command_rx)? {
            match command {
                DecoderCommand::SeekToMillis {
                    position_ms,
                    generation: next_generation,
                } => {
                    let seeked = format
                        .seek(
                            SeekMode::Accurate,
                            SeekTo::Time {
                                time: time_from_millis(position_ms),
                                track_id: Some(track_id),
                            },
                        )
                        .map_err(|error| decode_error(path, error))?;
                    decoder.reset();
                    sample_rate_adapter = SampleRateAdapter::new(
                        source_sample_rate,
                        target_sample_rate,
                        source_channels,
                    );
                    generation = next_generation;
                    let actual_position_ms = time_base
                        .map(|time_base| millis_from_time(time_base.calc_time(seeked.actual_ts)))
                        .unwrap_or(position_ms);
                    let _ = event_tx.send(DecoderEvent::Seeked {
                        position_ms: actual_position_ms,
                        generation,
                    });
                }
            }
        }

        let packet = match format.next_packet() {
            Ok(packet) => packet,
            Err(SymphoniaError::IoError(error))
                if error.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                break;
            }
            Err(SymphoniaError::ResetRequired) => {
                return Err(EchoError::Decode {
                    path: path.to_string_lossy().to_string(),
                    message: "decoder reset is required and is not supported yet".to_string(),
                });
            }
            Err(error) => return Err(decode_error(path, error)),
        };

        if packet.track_id() != track_id {
            continue;
        }

        let decoded = match decoder.decode(&packet) {
            Ok(decoded) => decoded,
            Err(SymphoniaError::DecodeError(_)) => continue,
            Err(SymphoniaError::IoError(error))
                if error.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                break;
            }
            Err(error) => return Err(decode_error(path, error)),
        };

        let mut sample_buffer =
            SampleBuffer::<f32>::new(decoded.capacity() as u64, *decoded.spec());
        sample_buffer.copy_interleaved_ref(decoded);
        let samples = sample_rate_adapter.process(sample_buffer.samples());
        if samples.is_empty() {
            continue;
        }

        send_samples(
            SampleChunk {
                generation,
                samples,
            },
            &sample_tx,
            &queued_samples,
            &stop_requested,
            &requested_generation,
        )?;
    }

    Ok(())
}

fn next_decoder_command(command_rx: &Receiver<DecoderCommand>) -> Result<Option<DecoderCommand>> {
    match command_rx.try_recv() {
        Ok(command) => Ok(Some(command)),
        Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => Ok(None),
    }
}

fn send_samples(
    chunk: SampleChunk,
    sample_tx: &SyncSender<SampleChunk>,
    queued_samples: &AtomicI64,
    stop_requested: &AtomicBool,
    requested_generation: &AtomicU64,
) -> Result<()> {
    let sample_count = chunk.samples.len() as i64;
    queued_samples.fetch_add(sample_count, Ordering::Release);
    let mut pending = chunk;

    loop {
        if stop_requested.load(Ordering::Acquire) {
            queued_samples.fetch_sub(sample_count, Ordering::Release);
            return Ok(());
        }
        if requested_generation.load(Ordering::Acquire) != pending.generation {
            queued_samples.fetch_sub(sample_count, Ordering::Release);
            return Ok(());
        }

        match sample_tx.try_send(pending) {
            Ok(()) => return Ok(()),
            Err(TrySendError::Full(samples)) => {
                pending = samples;
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Err(TrySendError::Disconnected(_)) => {
                queued_samples.fetch_sub(sample_count, Ordering::Release);
                return Ok(());
            }
        }
    }
}

fn time_from_millis(position_ms: u64) -> Time {
    Time::new(position_ms / 1000, (position_ms % 1000) as f64 / 1000.0)
}

fn millis_from_time(time: Time) -> u64 {
    time.seconds
        .saturating_mul(1000)
        .saturating_add((time.frac.clamp(0.0, 0.999_999) * 1000.0).round() as u64)
}

fn stream_info_from_params(
    path: &Path,
    params: symphonia::core::codecs::CodecParameters,
) -> Result<DecodeStreamInfo> {
    let sample_rate = params.sample_rate.ok_or_else(|| EchoError::Decode {
        path: path.to_string_lossy().to_string(),
        message: "missing sample rate".to_string(),
    })?;
    let channel_count = params
        .channels
        .map(|channels| channels.count() as u16)
        .filter(|channels| *channels > 0)
        .ok_or_else(|| EchoError::Decode {
            path: path.to_string_lossy().to_string(),
            message: "missing channel layout".to_string(),
        })?;
    let duration_ms = params
        .n_frames
        .map(|frames| frames * 1000 / sample_rate as u64);

    Ok(DecodeStreamInfo {
        sample_rate,
        channel_count,
        bit_depth: params.bits_per_sample,
        duration_ms,
    })
}

fn decode_error(path: &Path, error: SymphoniaError) -> EchoError {
    EchoError::Decode {
        path: path.to_string_lossy().to_string(),
        message: error.to_string(),
    }
}

struct SampleRateAdapter {
    source_sample_rate: u32,
    target_sample_rate: u32,
    channels: usize,
    position: f64,
    pending: Vec<f32>,
}

impl SampleRateAdapter {
    fn new(source_sample_rate: u32, target_sample_rate: u32, channels: usize) -> Self {
        Self {
            source_sample_rate,
            target_sample_rate,
            channels,
            position: 0.0,
            pending: Vec::new(),
        }
    }

    fn process(&mut self, input: &[f32]) -> Vec<f32> {
        if self.source_sample_rate == self.target_sample_rate || input.is_empty() {
            return input.to_vec();
        }

        self.pending.extend_from_slice(input);
        let available_frames = self.pending.len() / self.channels;
        if available_frames < 2 {
            return Vec::new();
        }

        let ratio = self.source_sample_rate as f64 / self.target_sample_rate as f64;
        let mut output = Vec::with_capacity(
            ((available_frames as f64 / ratio).ceil() as usize).saturating_mul(self.channels),
        );

        while self.position + 1.0 < available_frames as f64 {
            let frame_index = self.position.floor() as usize;
            let fraction = (self.position - frame_index as f64) as f32;
            for channel in 0..self.channels {
                let left = self.pending[frame_index * self.channels + channel];
                let right = self.pending[(frame_index + 1) * self.channels + channel];
                output.push(left + (right - left) * fraction);
            }
            self.position += ratio;
        }

        let frames_to_drop = (self.position.floor() as usize).saturating_sub(1);
        if frames_to_drop > 0 {
            let samples_to_drop = frames_to_drop * self.channels;
            self.pending.drain(0..samples_to_drop);
            self.position -= frames_to_drop as f64;
        }

        output
    }
}

#[cfg(test)]
mod tests {
    use super::{SampleChunk, SampleRateAdapter, send_samples};
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

    #[test]
    fn sample_rate_adapter_expands_44100_to_48000() {
        let mut adapter = SampleRateAdapter::new(44_100, 48_000, 1);
        let input = vec![0.0; 4_410];
        let output = adapter.process(&input);

        assert!(output.len() > input.len());
        assert!(output.len() < 4_900);
    }

    #[test]
    fn send_samples_aborts_when_generation_is_stale() {
        let (sample_tx, _sample_rx) = std::sync::mpsc::sync_channel(0);
        let queued_samples = AtomicI64::new(0);
        let stop_requested = AtomicBool::new(false);
        let requested_generation = AtomicU64::new(1);

        send_samples(
            SampleChunk {
                generation: 0,
                samples: vec![0.5, 0.5],
            },
            &sample_tx,
            &queued_samples,
            &stop_requested,
            &requested_generation,
        )
        .unwrap();

        assert_eq!(queued_samples.load(Ordering::Acquire), 0);
    }
}
