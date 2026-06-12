use cpal::traits::{DeviceTrait, HostTrait};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AudioDevice {
    pub id: String,
    pub name: String,
    pub is_default: bool,
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

pub fn default_device_name() -> String {
    cpal::default_host()
        .default_output_device()
        .and_then(|device| device.name().ok())
        .unwrap_or_else(|| "no default output device found".to_string())
}

fn fallback_devices(default_name: Option<String>) -> Vec<AudioDevice> {
    vec![AudioDevice {
        id: "cpal-output-default".to_string(),
        name: default_name.unwrap_or_else(|| "system default".to_string()),
        is_default: true,
    }]
}
