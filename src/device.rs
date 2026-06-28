use crate::error::{EchoError, Result};
use cpal::traits::{DeviceTrait, HostTrait};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AudioDevice {
    pub id: String,
    pub name: String,
    pub is_default: bool,
}

pub struct SelectedAudioDevice {
    pub device: cpal::Device,
    pub info: AudioDevice,
}

pub fn list_devices() -> Vec<AudioDevice> {
    let host = cpal::default_host();
    let default_name = host
        .default_output_device()
        .and_then(|device| device.name().ok());
    let Ok(devices) = host.output_devices() else {
        return fallback_devices(default_name);
    };

    let mut output = Vec::new();
    for (index, device) in devices.enumerate() {
        let name = device
            .name()
            .unwrap_or_else(|_| format!("unknown output device {index}"));
        let is_default = default_name.as_deref() == Some(name.as_str());
        output.push(AudioDevice {
            id: format!("cpal-output-{index}"),
            name,
            is_default,
        });
    }

    if output.is_empty() {
        fallback_devices(default_name)
    } else {
        output
    }
}

pub fn selected_output_device(selector: Option<&str>) -> Result<SelectedAudioDevice> {
    let host = cpal::default_host();
    let selector = selector.map(str::trim).filter(|value| !value.is_empty());
    let default_name = host
        .default_output_device()
        .and_then(|device| device.name().ok());

    if selector.is_none_or(is_default_output_selector) {
        let device = host
            .default_output_device()
            .ok_or_else(|| EchoError::Audio("no default output device found".to_string()))?;
        let name = device
            .name()
            .unwrap_or_else(|_| "unknown output device".to_string());
        return Ok(SelectedAudioDevice {
            device,
            info: AudioDevice {
                id: "cpal-output-default".to_string(),
                is_default: true,
                name,
            },
        });
    }

    let selector = selector.unwrap_or_default();
    let devices = output_devices_with_handles(&host, default_name.as_deref())?;
    select_from_devices(devices, selector)
}

pub fn default_device_name() -> String {
    cpal::default_host()
        .default_output_device()
        .and_then(|device| device.name().ok())
        .unwrap_or_else(|| "no default output device found".to_string())
}

pub fn is_default_output_selector(selector: &str) -> bool {
    matches!(
        normalize_selector(selector).as_str(),
        "default" | "system" | "auto" | "clear" | "reset" | "默认" | "系统默认"
    )
}

fn fallback_devices(default_name: Option<String>) -> Vec<AudioDevice> {
    vec![AudioDevice {
        id: "cpal-output-default".to_string(),
        name: default_name.unwrap_or_else(|| "system default".to_string()),
        is_default: true,
    }]
}

fn output_devices_with_handles(
    host: &cpal::Host,
    default_name: Option<&str>,
) -> Result<Vec<(AudioDevice, cpal::Device)>> {
    let devices = host
        .output_devices()
        .map_err(|error| EchoError::Audio(error.to_string()))?;

    Ok(devices
        .enumerate()
        .map(|(index, device)| {
            let name = device
                .name()
                .unwrap_or_else(|_| format!("unknown output device {index}"));
            let is_default = default_name == Some(name.as_str());
            (
                AudioDevice {
                    id: format!("cpal-output-{index}"),
                    name,
                    is_default,
                },
                device,
            )
        })
        .collect())
}

fn select_from_devices(
    devices: Vec<(AudioDevice, cpal::Device)>,
    selector: &str,
) -> Result<SelectedAudioDevice> {
    if devices.is_empty() {
        return Err(EchoError::Audio("no output devices found".to_string()));
    }

    let normalized_selector = normalize_selector(selector);
    if let Some((info, device)) = devices
        .iter()
        .find(|(info, _)| {
            normalize_selector(&info.id) == normalized_selector
                || normalize_selector(&info.name) == normalized_selector
        })
        .map(|(info, device)| (info.clone(), device.clone()))
    {
        return Ok(SelectedAudioDevice { device, info });
    }

    let partial_matches = devices
        .iter()
        .filter(|(info, _)| normalize_selector(&info.name).contains(&normalized_selector))
        .map(|(info, device)| (info.clone(), device.clone()))
        .collect::<Vec<_>>();

    match partial_matches.len() {
        1 => {
            let (info, device) = partial_matches.into_iter().next().unwrap();
            Ok(SelectedAudioDevice { device, info })
        }
        0 => Err(EchoError::Audio(format!(
            "no output device matches '{selector}'"
        ))),
        _ => Err(EchoError::Audio(format!(
            "output device selector '{selector}' is ambiguous"
        ))),
    }
}

fn normalize_selector(value: &str) -> String {
    value.trim().to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_output_selector_accepts_clear_words() {
        assert!(is_default_output_selector("default"));
        assert!(is_default_output_selector(" system "));
        assert!(is_default_output_selector("clear"));
        assert!(is_default_output_selector("默认"));
        assert!(!is_default_output_selector("Speakers"));
    }
}
