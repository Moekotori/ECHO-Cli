use crate::app::fallback_title_from_path;
use crate::error::{EchoError, Result};
use crate::library::Track;
use lofty::file::{AudioFile, TaggedFileExt};
use lofty::prelude::Accessor;
use lofty::probe::Probe;
use lofty::tag::ItemKey;
use std::fs::Metadata;
use std::path::Path;
use std::time::UNIX_EPOCH;

pub fn read_track(path: &Path, file_metadata: &Metadata) -> Result<Track> {
    let tagged_file = Probe::open(path)
        .and_then(|probe| probe.read())
        .map_err(|error| EchoError::Metadata {
            path: path.to_string_lossy().to_string(),
            message: error.to_string(),
        })?;

    let tag = tagged_file
        .primary_tag()
        .or_else(|| tagged_file.first_tag());
    let properties = tagged_file.properties();

    let title = tag
        .and_then(|tag| tag.title())
        .and_then(clean_optional)
        .unwrap_or_else(|| fallback_title_from_path(path));

    let modified_unix = file_metadata
        .modified()?
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    Ok(Track {
        id: None,
        title,
        artist: tag.and_then(|tag| tag.artist()).and_then(clean_optional),
        album: tag.and_then(|tag| tag.album()).and_then(clean_optional),
        album_artist: tag
            .and_then(|tag| tag.get_string(&ItemKey::AlbumArtist))
            .and_then(clean_optional),
        track_number: tag.and_then(|tag| tag.track()),
        disc_number: tag.and_then(|tag| tag.disk()),
        duration_ms: Some(properties.duration().as_millis() as u64).filter(|value| *value > 0),
        sample_rate: properties.sample_rate(),
        channel_count: properties.channels().map(u32::from),
        bit_depth: properties.bit_depth().map(u32::from),
        path: path.to_string_lossy().to_string(),
        modified_unix,
        size_bytes: file_metadata.len(),
    })
}

fn clean_optional(value: impl AsRef<str>) -> Option<String> {
    let value = value.as_ref().trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

pub fn fallback_track(path: &Path, file_metadata: &Metadata) -> Result<Track> {
    let modified_unix = file_metadata
        .modified()?
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    Ok(Track {
        id: None,
        title: fallback_title_from_path(path),
        artist: None,
        album: None,
        album_artist: None,
        track_number: None,
        disc_number: None,
        duration_ms: None,
        sample_rate: None,
        channel_count: None,
        bit_depth: None,
        path: path.to_string_lossy().to_string(),
        modified_unix,
        size_bytes: file_metadata.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn fallback_track_uses_file_name() {
        let path =
            std::env::temp_dir().join(format!("echo-cli-fallback-{}.flac", std::process::id()));
        fs::write(&path, b"not-a-real-audio-file").unwrap();
        let metadata = fs::metadata(&path).unwrap();
        let track = fallback_track(&path, &metadata).unwrap();
        fs::remove_file(&path).unwrap();

        assert!(track.title.starts_with("echo-cli-fallback-"));
        assert_eq!(track.size_bytes, 21);
    }
}
