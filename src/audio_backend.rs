use crate::decoder::{DecodeStreamInfo, SampleChunk};
use crate::device;
use crate::error::{EchoError, Result};
use cpal::traits::{DeviceTrait, StreamTrait};
use cpal::{SampleFormat, SampleRate, Stream, StreamConfig, SupportedStreamConfig};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, TryRecvError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputStreamInfo {
    pub device_name: String,
    pub mode: &'static str,
    pub sample_rate: u32,
    pub channel_count: u16,
    pub sample_format: String,
    pub buffer_size: String,
    pub warnings: Vec<String>,
}

pub struct SharedOutput {
    stream: Stream,
    info: OutputStreamInfo,
    volume_percent: Arc<AtomicU32>,
    current_generation: Arc<AtomicU64>,
}

impl SharedOutput {
    pub fn open_with_volume_on_device(
        stream_info: &DecodeStreamInfo,
        sample_rx: Receiver<SampleChunk>,
        queued_samples: Arc<AtomicI64>,
        initial_volume_percent: u8,
        output_device_selector: Option<&str>,
    ) -> Result<Self> {
        let selected_device = device::selected_output_device(output_device_selector)?;
        let device_name = selected_device.info.name.clone();
        let supported = select_output_config(&selected_device.device, stream_info)?;
        let config = supported.config();
        let sample_format = supported.sample_format();
        let mut warnings = Vec::new();

        if config.sample_rate.0 != stream_info.sample_rate {
            warnings.push(format!(
                "sample-rate conversion {} Hz -> {} Hz",
                stream_info.sample_rate, config.sample_rate.0
            ));
        }
        if config.channels != stream_info.channel_count {
            warnings.push(format!(
                "channel mapping {}ch -> {}ch",
                stream_info.channel_count, config.channels
            ));
        }

        let output_info = OutputStreamInfo {
            device_name,
            mode: "shared",
            sample_rate: config.sample_rate.0,
            channel_count: config.channels,
            sample_format: format!("{sample_format:?}"),
            buffer_size: format!("{:?}", config.buffer_size),
            warnings,
        };

        let volume_percent = Arc::new(AtomicU32::new(initial_volume_percent.min(100) as u32));
        let current_generation = Arc::new(AtomicU64::new(0));
        let stream = build_stream(
            &selected_device.device,
            &config,
            sample_format,
            stream_info.channel_count,
            sample_rx,
            queued_samples,
            volume_percent.clone(),
            current_generation.clone(),
        )?;

        Ok(Self {
            stream,
            info: output_info,
            volume_percent,
            current_generation,
        })
    }

    pub fn play(&self) -> Result<()> {
        self.stream
            .play()
            .map_err(|error| EchoError::Audio(error.to_string()))
    }

    pub fn pause(&self) -> Result<()> {
        self.stream
            .pause()
            .map_err(|error| EchoError::Audio(error.to_string()))
    }

    pub fn set_volume_percent(&self, percent: u8) {
        self.volume_percent
            .store(percent.min(100) as u32, Ordering::Relaxed);
    }

    pub fn set_generation(&self, generation: u64) {
        self.current_generation.store(generation, Ordering::Release);
    }

    pub fn info(&self) -> &OutputStreamInfo {
        &self.info
    }
}

pub fn backend_status_line() -> &'static str {
    if cfg!(windows) {
        "CPAL shared output over Windows audio; WASAPI exclusive planned"
    } else {
        "CPAL shared output; Windows is the primary target"
    }
}

fn select_output_config(
    device: &cpal::Device,
    stream_info: &DecodeStreamInfo,
) -> Result<SupportedStreamConfig> {
    let mut ranges = device
        .supported_output_configs()
        .map_err(|error| EchoError::Audio(error.to_string()))?
        .collect::<Vec<_>>();

    ranges.retain(|range| supported_output_sample_format(range.sample_format()));
    ranges.sort_by_key(|range| {
        let same_channels = range.channels() == stream_info.channel_count;
        let f32_format = range.sample_format() == SampleFormat::F32;
        (!same_channels, !f32_format, range.channels())
    });

    for range in ranges {
        if range.min_sample_rate().0 <= stream_info.sample_rate
            && range.max_sample_rate().0 >= stream_info.sample_rate
        {
            return Ok(range.with_sample_rate(SampleRate(stream_info.sample_rate)));
        }
    }

    let default_config = device
        .default_output_config()
        .map_err(|error| EchoError::Audio(error.to_string()))?;
    if supported_output_sample_format(default_config.sample_format()) {
        Ok(default_config)
    } else {
        Err(EchoError::Audio(format!(
            "unsupported default output sample format: {:?}",
            default_config.sample_format()
        )))
    }
}

fn supported_output_sample_format(sample_format: SampleFormat) -> bool {
    matches!(
        sample_format,
        SampleFormat::F32 | SampleFormat::I16 | SampleFormat::U16
    )
}

fn build_stream(
    device: &cpal::Device,
    config: &StreamConfig,
    sample_format: SampleFormat,
    source_channels: u16,
    sample_rx: Receiver<SampleChunk>,
    queued_samples: Arc<AtomicI64>,
    volume_percent: Arc<AtomicU32>,
    current_generation: Arc<AtomicU64>,
) -> Result<Stream> {
    match sample_format {
        SampleFormat::F32 => build_typed_stream::<f32>(
            device,
            config,
            source_channels,
            sample_rx,
            queued_samples,
            volume_percent,
            current_generation,
            write_f32,
        ),
        SampleFormat::I16 => build_typed_stream::<i16>(
            device,
            config,
            source_channels,
            sample_rx,
            queued_samples,
            volume_percent,
            current_generation,
            write_i16,
        ),
        SampleFormat::U16 => build_typed_stream::<u16>(
            device,
            config,
            source_channels,
            sample_rx,
            queued_samples,
            volume_percent,
            current_generation,
            write_u16,
        ),
        other => Err(EchoError::Audio(format!(
            "unsupported output sample format: {other:?}"
        ))),
    }
}

fn build_typed_stream<T>(
    device: &cpal::Device,
    config: &StreamConfig,
    source_channels: u16,
    sample_rx: Receiver<SampleChunk>,
    queued_samples: Arc<AtomicI64>,
    volume_percent: Arc<AtomicU32>,
    current_generation: Arc<AtomicU64>,
    write_sample: fn(&mut T, f32),
) -> Result<Stream>
where
    T: cpal::SizedSample + 'static,
{
    let output_channels = config.channels;
    let mut mapper = ChannelMapper::new(
        source_channels,
        output_channels,
        sample_rx,
        queued_samples,
        volume_percent,
        current_generation,
    );
    device
        .build_output_stream(
            config,
            move |data: &mut [T], _| mapper.write_output(data, write_sample),
            move |error| tracing::warn!("audio output stream error: {error}"),
            None,
        )
        .map_err(|error| EchoError::Audio(error.to_string()))
}

struct ChannelMapper {
    source_channels: u16,
    output_channels: u16,
    sample_rx: Receiver<SampleChunk>,
    scratch: VecDeque<f32>,
    frame: Vec<f32>,
    queued_samples: Arc<AtomicI64>,
    volume_percent: Arc<AtomicU32>,
    current_generation: Arc<AtomicU64>,
    generation: u64,
}

impl ChannelMapper {
    fn new(
        source_channels: u16,
        output_channels: u16,
        sample_rx: Receiver<SampleChunk>,
        queued_samples: Arc<AtomicI64>,
        volume_percent: Arc<AtomicU32>,
        current_generation: Arc<AtomicU64>,
    ) -> Self {
        Self {
            source_channels,
            output_channels,
            sample_rx,
            scratch: VecDeque::new(),
            frame: Vec::with_capacity(source_channels as usize),
            queued_samples,
            volume_percent,
            current_generation,
            generation: 0,
        }
    }

    fn write_output<T>(&mut self, output: &mut [T], write_sample: fn(&mut T, f32)) {
        self.sync_generation();
        let volume = volume_factor(self.volume_percent.load(Ordering::Relaxed));
        for output_frame in output.chunks_mut(self.output_channels as usize) {
            self.frame.clear();
            for _ in 0..self.source_channels {
                let sample = self.pop_sample().unwrap_or(0.0);
                self.frame.push(sample);
            }

            let mapped = map_frame(&self.frame, self.output_channels);
            for (sample, value) in output_frame.iter_mut().zip(mapped) {
                write_sample(sample, value * volume);
            }
        }
    }

    fn pop_sample(&mut self) -> Option<f32> {
        while self.scratch.is_empty() {
            match self.sample_rx.try_recv() {
                Ok(chunk) if chunk.generation == self.generation => {
                    self.scratch.extend(chunk.samples)
                }
                Ok(chunk) => {
                    self.queued_samples
                        .fetch_sub(chunk.samples.len() as i64, Ordering::Release);
                }
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => return None,
            }
        }

        let sample = self.scratch.pop_front();
        if sample.is_some() {
            self.queued_samples.fetch_sub(1, Ordering::Release);
        }
        sample
    }

    fn sync_generation(&mut self) {
        let current_generation = self.current_generation.load(Ordering::Acquire);
        if current_generation == self.generation {
            return;
        }

        if !self.scratch.is_empty() {
            self.queued_samples
                .fetch_sub(self.scratch.len() as i64, Ordering::Release);
            self.scratch.clear();
        }
        self.generation = current_generation;
        self.discard_stale_chunks_until_current();
    }

    fn discard_stale_chunks_until_current(&mut self) {
        loop {
            match self.sample_rx.try_recv() {
                Ok(chunk) if chunk.generation == self.generation => {
                    self.scratch.extend(chunk.samples);
                    return;
                }
                Ok(chunk) => {
                    self.queued_samples
                        .fetch_sub(chunk.samples.len() as i64, Ordering::Release);
                }
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => return,
            }
        }
    }
}

fn map_frame(frame: &[f32], output_channels: u16) -> Vec<f32> {
    let output_channels = output_channels as usize;
    if output_channels == 0 {
        return Vec::new();
    }

    if frame.is_empty() {
        return vec![0.0; output_channels];
    }

    if output_channels == 1 && frame.len() > 1 {
        let left = frame.first().copied().unwrap_or(0.0);
        let right = frame.get(1).copied().unwrap_or(left);
        return vec![(left + right) * 0.5];
    }

    if frame.len() == 1 {
        return vec![frame[0]; output_channels];
    }

    (0..output_channels)
        .map(|index| frame.get(index).copied().unwrap_or(0.0))
        .collect()
}

fn volume_factor(percent: u32) -> f32 {
    percent.min(100) as f32 / 100.0
}

fn write_f32(output: &mut f32, sample: f32) {
    *output = sample.clamp(-1.0, 1.0);
}

fn write_i16(output: &mut i16, sample: f32) {
    *output = (sample.clamp(-1.0, 1.0) * i16::MAX as f32) as i16;
}

fn write_u16(output: &mut u16, sample: f32) {
    *output = ((sample.clamp(-1.0, 1.0) * 0.5 + 0.5) * u16::MAX as f32) as u16;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mono_maps_to_stereo_by_duplication() {
        assert_eq!(map_frame(&[0.25], 2), vec![0.25, 0.25]);
    }

    #[test]
    fn stereo_maps_to_mono_by_average() {
        assert_eq!(map_frame(&[0.25, 0.75], 1), vec![0.5]);
    }

    #[test]
    fn extra_output_channels_are_silent() {
        assert_eq!(map_frame(&[0.1, 0.2], 4), vec![0.1, 0.2, 0.0, 0.0]);
    }

    #[test]
    fn volume_factor_clamps_to_percent_range() {
        assert_eq!(volume_factor(0), 0.0);
        assert_eq!(volume_factor(50), 0.5);
        assert_eq!(volume_factor(100), 1.0);
        assert_eq!(volume_factor(150), 1.0);
    }

    #[test]
    fn mapper_discards_stale_generation_and_keeps_volume() {
        let (sample_tx, sample_rx) = std::sync::mpsc::channel();
        let queued_samples = Arc::new(AtomicI64::new(4));
        let volume_percent = Arc::new(AtomicU32::new(50));
        let current_generation = Arc::new(AtomicU64::new(1));
        sample_tx
            .send(SampleChunk {
                generation: 0,
                samples: vec![1.0, 1.0],
            })
            .unwrap();
        sample_tx
            .send(SampleChunk {
                generation: 1,
                samples: vec![0.4, 0.8],
            })
            .unwrap();

        let mut mapper = ChannelMapper::new(
            1,
            1,
            sample_rx,
            queued_samples.clone(),
            volume_percent,
            current_generation,
        );
        let mut output = [0.0_f32; 2];
        mapper.write_output(&mut output, write_f32);

        assert_eq!(output, [0.2, 0.4]);
        assert_eq!(queued_samples.load(Ordering::Acquire), 0);
    }

    #[test]
    fn generation_switch_releases_stale_queued_chunks_without_dropping_current() {
        let (sample_tx, sample_rx) = std::sync::mpsc::channel();
        let queued_samples = Arc::new(AtomicI64::new(6));
        let volume_percent = Arc::new(AtomicU32::new(100));
        let current_generation = Arc::new(AtomicU64::new(1));
        sample_tx
            .send(SampleChunk {
                generation: 0,
                samples: vec![1.0, 1.0],
            })
            .unwrap();
        sample_tx
            .send(SampleChunk {
                generation: 0,
                samples: vec![0.5, 0.5],
            })
            .unwrap();
        sample_tx
            .send(SampleChunk {
                generation: 1,
                samples: vec![0.25, 0.75],
            })
            .unwrap();

        let mut mapper = ChannelMapper::new(
            1,
            1,
            sample_rx,
            queued_samples.clone(),
            volume_percent,
            current_generation,
        );

        mapper.write_output(&mut [], write_f32);

        assert_eq!(queued_samples.load(Ordering::Acquire), 2);
        assert_eq!(mapper.scratch.len(), 2);

        let mut output = [0.0_f32; 2];
        mapper.write_output(&mut output, write_f32);

        assert_eq!(output, [0.25, 0.75]);
        assert_eq!(queued_samples.load(Ordering::Acquire), 0);
    }
}
