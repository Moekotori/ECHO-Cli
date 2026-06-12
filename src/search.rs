use crate::db::Database;
use crate::error::Result;
use crate::library::Track;
use std::cmp::Ordering;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchResult {
    pub track: Track,
    pub score: u8,
}

pub fn search(database: &Database, query: &str, limit: usize) -> Result<Vec<SearchResult>> {
    let mut results: Vec<SearchResult> = database
        .search_candidates(query, limit.saturating_mul(8).max(limit))
        .map(|tracks| {
            tracks
                .into_iter()
                .map(|track| {
                    let score = rank_track(&track, query);
                    SearchResult { track, score }
                })
                .collect()
        })?;

    results.sort_by(compare_results);
    results.truncate(limit);
    Ok(results)
}

fn compare_results(left: &SearchResult, right: &SearchResult) -> Ordering {
    right.score.cmp(&left.score).then_with(|| {
        left.track
            .title
            .to_lowercase()
            .cmp(&right.track.title.to_lowercase())
    })
}

pub fn rank_track(track: &Track, query: &str) -> u8 {
    let query = query.trim().to_lowercase();
    if query.is_empty() {
        return 1;
    }

    let title = track.title.to_lowercase();
    let artist = track.artist.as_deref().unwrap_or("").to_lowercase();
    let album = track.album.as_deref().unwrap_or("").to_lowercase();
    let filename = Path::new(&track.path)
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("")
        .to_lowercase();
    let path = track.path.to_lowercase();

    if title == query || artist == query {
        100
    } else if title.starts_with(&query) || artist.starts_with(&query) {
        80
    } else if title.contains(&query) || artist.contains(&query) || album.contains(&query) {
        60
    } else if filename.contains(&query) {
        40
    } else if path.contains(&query) {
        20
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn track(title: &str, artist: Option<&str>, path: &str) -> Track {
        Track {
            id: None,
            title: title.to_string(),
            artist: artist.map(str::to_string),
            album: None,
            album_artist: None,
            track_number: None,
            disc_number: None,
            duration_ms: None,
            sample_rate: None,
            channel_count: None,
            bit_depth: None,
            path: path.to_string(),
            modified_unix: 1,
            size_bytes: 1,
        }
    }

    #[test]
    fn ranking_prioritizes_exact_then_prefix_then_contains_then_path() {
        let exact = track("Moon", None, "C:/x/a.flac");
        let prefix = track("Moonrise", None, "C:/x/b.flac");
        let contains = track("Blue Moon", None, "C:/x/c.flac");
        let path_only = track("Other", None, "C:/Moon/d.flac");

        assert!(rank_track(&exact, "moon") > rank_track(&prefix, "moon"));
        assert!(rank_track(&prefix, "moon") > rank_track(&contains, "moon"));
        assert!(rank_track(&contains, "moon") > rank_track(&path_only, "moon"));
    }
}
