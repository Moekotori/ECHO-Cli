use crate::app::is_supported_audio_path;
use crate::db::Database;
use crate::error::Result;
use crate::library::{FileFingerprint, Track};
use crate::metadata;
use jwalk::WalkDir;
use rayon::prelude::*;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScanSummary {
    pub scanned_files: usize,
    pub indexed_tracks: usize,
    pub skipped_unchanged: usize,
    pub failed_files: usize,
    pub removed_missing: usize,
    pub elapsed_ms: u128,
}

pub fn scan_folder(database: &mut Database, folder: &Path) -> Result<ScanSummary> {
    let started = Instant::now();
    let root = folder.canonicalize()?;
    let known = database.fingerprints()?;
    let mut seen_paths = HashSet::new();
    let mut candidates = Vec::new();
    let mut skipped_unchanged = 0;

    for entry in WalkDir::new(&root)
        .into_iter()
        .filter_map(std::result::Result::ok)
    {
        if !entry.file_type().is_file() {
            continue;
        }

        let path = entry.path();
        if !is_supported_audio_path(&path) {
            continue;
        }

        let metadata = match std::fs::metadata(&path) {
            Ok(metadata) => metadata,
            Err(_) => continue,
        };
        let path_string = path.to_string_lossy().to_string();
        let fingerprint = fingerprint_from_metadata(&metadata);
        seen_paths.insert(path_string.clone());

        if known.get(&path_string).copied() == Some(fingerprint) {
            skipped_unchanged += 1;
            continue;
        }

        candidates.push(path);
    }

    let scanned_files = candidates.len() + skipped_unchanged;
    let scan_results: Vec<ScanItem> = candidates.par_iter().map(read_scan_item).collect();

    let mut tracks = Vec::new();
    let mut errors = Vec::new();
    for item in scan_results {
        match item {
            ScanItem::Track(track) => tracks.push(track),
            ScanItem::Error { path, error } => errors.push((path, error)),
        }
    }

    database.upsert_tracks(&tracks)?;
    database.record_scan_errors(&errors)?;
    let removed_missing = database.remove_missing_under(&root, &seen_paths)?;

    Ok(ScanSummary {
        scanned_files,
        indexed_tracks: tracks.len(),
        skipped_unchanged,
        failed_files: errors.len(),
        removed_missing,
        elapsed_ms: started.elapsed().as_millis(),
    })
}

enum ScanItem {
    Track(Track),
    Error { path: String, error: String },
}

fn read_scan_item(path: &PathBuf) -> ScanItem {
    let file_metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) => {
            return ScanItem::Error {
                path: path.to_string_lossy().to_string(),
                error: error.to_string(),
            };
        }
    };

    match metadata::read_track(path, &file_metadata) {
        Ok(track) => ScanItem::Track(track),
        Err(error) => match metadata::fallback_track(path, &file_metadata) {
            Ok(track) => ScanItem::Track(track),
            Err(_) => ScanItem::Error {
                path: path.to_string_lossy().to_string(),
                error: error.to_string(),
            },
        },
    }
}

fn fingerprint_from_metadata(metadata: &std::fs::Metadata) -> FileFingerprint {
    let modified_unix = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default();

    FileFingerprint {
        modified_unix,
        size_bytes: metadata.len(),
    }
}

#[cfg(test)]
mod tests {
    use crate::app::is_supported_audio_path;
    use std::path::Path;

    #[test]
    fn scanner_filter_includes_only_supported_audio_extensions() {
        assert!(is_supported_audio_path(Path::new("a.mp3")));
        assert!(is_supported_audio_path(Path::new("a.FLAC")));
        assert!(!is_supported_audio_path(Path::new("a.aiff")));
        assert!(!is_supported_audio_path(Path::new("folder")));
    }
}
