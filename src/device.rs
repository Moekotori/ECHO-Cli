#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AudioDevice {
    pub id: String,
    pub name: String,
    pub is_default: bool,
}

pub fn list_devices() -> Vec<AudioDevice> {
    vec![AudioDevice {
        id: "phase4-wasapi-default".to_string(),
        name: default_device_name(),
        is_default: true,
    }]
}

pub fn default_device_name() -> String {
    "system default (WASAPI enumeration arrives in Phase 4)".to_string()
}
