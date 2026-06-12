use crate::error::{EchoError, Result};
use directories::ProjectDirs;
use std::fs;
use std::path::{Path, PathBuf};

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
}
