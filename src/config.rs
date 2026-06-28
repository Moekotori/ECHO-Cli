use crate::error::{EchoError, Result};
use directories::ProjectDirs;
use std::fs;
use std::path::{Path, PathBuf};

const OUTPUT_DEVICE_FILE: &str = "output_device.txt";

#[derive(Debug, Clone)]
pub struct AppPaths {
    pub config_dir: PathBuf,
    pub cache_dir: PathBuf,
    pub database_path: PathBuf,
}

impl AppPaths {
    pub fn load() -> Result<Self> {
        let project_dirs =
            ProjectDirs::from("dev", "Echo", "ECHO CLI").ok_or(EchoError::ConfigPathUnavailable)?;

        let config_dir = project_dirs.config_dir().to_path_buf();
        let cache_dir = project_dirs.cache_dir().to_path_buf();
        fs::create_dir_all(&config_dir)?;
        fs::create_dir_all(&cache_dir)?;

        Ok(Self {
            database_path: cache_dir.join("library.sqlite3"),
            config_dir,
            cache_dir,
        })
    }

    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub fn output_device_preference_path(&self) -> PathBuf {
        self.config_dir.join(OUTPUT_DEVICE_FILE)
    }

    pub fn load_output_device_preference(&self) -> Result<Option<String>> {
        let path = self.output_device_preference_path();
        match fs::read_to_string(&path) {
            Ok(value) => {
                let value = value.trim();
                if value.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(value.to_string()))
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub fn save_output_device_preference(&self, preference: Option<&str>) -> Result<()> {
        fs::create_dir_all(&self.config_dir)?;
        let path = self.output_device_preference_path();
        if let Some(preference) = preference.map(str::trim).filter(|value| !value.is_empty()) {
            fs::write(path, preference)?;
        } else {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn output_device_preference_round_trips_and_clears() {
        let root = std::env::temp_dir().join(format!(
            "echo-cli-config-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let paths = AppPaths {
            config_dir: root.join("config"),
            cache_dir: root.join("cache"),
            database_path: root.join("cache").join("library.sqlite3"),
        };

        assert_eq!(paths.load_output_device_preference().unwrap(), None);

        paths
            .save_output_device_preference(Some("  Speakers  "))
            .unwrap();
        assert_eq!(
            paths.load_output_device_preference().unwrap(),
            Some("Speakers".to_string())
        );

        paths.save_output_device_preference(None).unwrap();
        assert_eq!(paths.load_output_device_preference().unwrap(), None);

        let _ = fs::remove_dir_all(root);
    }
}
