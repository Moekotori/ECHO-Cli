use std::path::Path;

pub const APP_NAME: &str = "ECHO CLI";
pub const SUPPORTED_EXTENSIONS: &[&str] = &["mp3", "flac", "wav", "mp4", "m4a"];

pub fn is_supported_audio_path(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| {
            SUPPORTED_EXTENSIONS
                .iter()
                .any(|supported| extension.eq_ignore_ascii_case(supported))
        })
        .unwrap_or(false)
}

pub fn fallback_title_from_path(path: &Path) -> String {
    path.file_stem()
        .or_else(|| path.file_name())
        .and_then(|name| name.to_str())
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .unwrap_or("Untitled")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supported_extension_match_is_case_insensitive() {
        assert!(is_supported_audio_path(Path::new("Track.FLAC")));
        assert!(is_supported_audio_path(Path::new("mix.m4a")));
        assert!(!is_supported_audio_path(Path::new("cover.jpg")));
    }

    #[test]
    fn fallback_title_uses_filename() {
        assert_eq!(
            fallback_title_from_path(Path::new("C:/Music/Blue.flac")),
            "Blue"
        );
    }
}
