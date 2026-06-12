use crate::library::{FileFingerprint, Track};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::Result;

pub struct Database {
    connection: Connection,
}

impl Database {
    pub fn open(path: &Path) -> Result<Self> {
        let connection = Connection::open(path)?;
        let database = Self { connection };
        database.ensure_schema()?;
        Ok(database)
    }

    #[cfg(test)]
    pub fn open_memory() -> Result<Self> {
        let connection = Connection::open_in_memory()?;
        let database = Self { connection };
        database.ensure_schema()?;
        Ok(database)
    }

    pub fn ensure_schema(&self) -> Result<()> {
        self.connection.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;

            CREATE TABLE IF NOT EXISTS tracks (
                id INTEGER PRIMARY KEY,
                path TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL,
                artist TEXT,
                album TEXT,
                album_artist TEXT,
                track_number INTEGER,
                disc_number INTEGER,
                duration_ms INTEGER,
                sample_rate INTEGER,
                channel_count INTEGER,
                bit_depth INTEGER,
                modified_unix INTEGER NOT NULL,
                size_bytes INTEGER NOT NULL,
                indexed_unix INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_tracks_title ON tracks(title);
            CREATE INDEX IF NOT EXISTS idx_tracks_artist ON tracks(artist);
            CREATE INDEX IF NOT EXISTS idx_tracks_album ON tracks(album);
            CREATE INDEX IF NOT EXISTS idx_tracks_path ON tracks(path);

            CREATE TABLE IF NOT EXISTS scan_errors (
                id INTEGER PRIMARY KEY,
                path TEXT NOT NULL,
                error TEXT NOT NULL,
                created_unix INTEGER NOT NULL
            );
            "#,
        )?;
        Ok(())
    }

    pub fn upsert_tracks(&mut self, tracks: &[Track]) -> Result<()> {
        let transaction = self.connection.transaction()?;
        {
            let mut statement = transaction.prepare(
                r#"
                INSERT INTO tracks (
                    path, title, artist, album, album_artist, track_number, disc_number,
                    duration_ms, sample_rate, channel_count, bit_depth,
                    modified_unix, size_bytes, indexed_unix
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
                ON CONFLICT(path) DO UPDATE SET
                    title = excluded.title,
                    artist = excluded.artist,
                    album = excluded.album,
                    album_artist = excluded.album_artist,
                    track_number = excluded.track_number,
                    disc_number = excluded.disc_number,
                    duration_ms = excluded.duration_ms,
                    sample_rate = excluded.sample_rate,
                    channel_count = excluded.channel_count,
                    bit_depth = excluded.bit_depth,
                    modified_unix = excluded.modified_unix,
                    size_bytes = excluded.size_bytes,
                    indexed_unix = excluded.indexed_unix
                "#,
            )?;

            let now = now_unix();
            for track in tracks {
                statement.execute(params![
                    track.path,
                    track.title,
                    track.artist,
                    track.album,
                    track.album_artist,
                    track.track_number.map(i64::from),
                    track.disc_number.map(i64::from),
                    track.duration_ms.map(|value| value as i64),
                    track.sample_rate.map(i64::from),
                    track.channel_count.map(i64::from),
                    track.bit_depth.map(i64::from),
                    track.modified_unix,
                    track.size_bytes as i64,
                    now,
                ])?;
            }
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn record_scan_errors(&mut self, errors: &[(String, String)]) -> Result<()> {
        if errors.is_empty() {
            return Ok(());
        }

        let transaction = self.connection.transaction()?;
        {
            let mut statement = transaction.prepare(
                "INSERT INTO scan_errors (path, error, created_unix) VALUES (?1, ?2, ?3)",
            )?;
            let now = now_unix();
            for (path, error) in errors {
                statement.execute(params![path, error, now])?;
            }
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn fingerprints(&self) -> Result<HashMap<String, FileFingerprint>> {
        let mut statement = self
            .connection
            .prepare("SELECT path, modified_unix, size_bytes FROM tracks")?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                FileFingerprint {
                    modified_unix: row.get(1)?,
                    size_bytes: row.get::<_, i64>(2)? as u64,
                },
            ))
        })?;

        let mut fingerprints = HashMap::new();
        for row in rows {
            let (path, fingerprint) = row?;
            fingerprints.insert(path, fingerprint);
        }
        Ok(fingerprints)
    }

    pub fn remove_missing_under(
        &mut self,
        root: &Path,
        seen_paths: &HashSet<String>,
    ) -> Result<usize> {
        let root = root.to_string_lossy();
        let paths = self.all_paths()?;
        let missing: Vec<String> = paths
            .into_iter()
            .filter(|path| path.starts_with(root.as_ref()) && !seen_paths.contains(path))
            .collect();

        let transaction = self.connection.transaction()?;
        let removed = {
            let mut statement = transaction.prepare("DELETE FROM tracks WHERE path = ?1")?;
            let mut removed = 0;
            for path in &missing {
                removed += statement.execute(params![path])?;
            }
            removed
        };
        transaction.commit()?;
        Ok(removed)
    }

    pub fn search_candidates(&self, query: &str, limit: usize) -> Result<Vec<Track>> {
        let normalized = format!("%{}%", query.to_lowercase());
        let sql = if query.trim().is_empty() {
            "SELECT * FROM tracks ORDER BY title COLLATE NOCASE LIMIT ?1".to_string()
        } else {
            r#"
            SELECT * FROM tracks
            WHERE lower(title) LIKE ?1
               OR lower(coalesce(artist, '')) LIKE ?1
               OR lower(coalesce(album, '')) LIKE ?1
               OR lower(path) LIKE ?1
            LIMIT ?2
            "#
            .to_string()
        };

        let mut statement = self.connection.prepare(&sql)?;
        if query.trim().is_empty() {
            let rows = statement.query_map(params![limit as i64], row_to_track)?;
            collect_tracks(rows).map_err(Into::into)
        } else {
            let rows = statement.query_map(params![normalized, limit as i64], row_to_track)?;
            collect_tracks(rows).map_err(Into::into)
        }
    }

    pub fn track_count(&self) -> Result<u64> {
        self.connection
            .query_row("SELECT COUNT(*) FROM tracks", [], |row| {
                row.get::<_, i64>(0)
            })
            .map(|value| value as u64)
            .map_err(Into::into)
    }

    pub fn recent_scan_errors(&self, limit: usize) -> Result<Vec<(String, String)>> {
        let mut statement = self.connection.prepare(
            "SELECT path, error FROM scan_errors ORDER BY created_unix DESC, id DESC LIMIT ?1",
        )?;
        let rows = statement.query_map(params![limit as i64], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;

        let mut errors = Vec::new();
        for row in rows {
            errors.push(row?);
        }
        Ok(errors)
    }

    pub fn find_exact_path(&self, path: &Path) -> Result<Option<Track>> {
        self.connection
            .query_row(
                "SELECT * FROM tracks WHERE path = ?1",
                params![path.to_string_lossy()],
                row_to_track,
            )
            .optional()
            .map_err(Into::into)
    }

    fn all_paths(&self) -> Result<Vec<String>> {
        let mut statement = self.connection.prepare("SELECT path FROM tracks")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut paths = Vec::new();
        for row in rows {
            paths.push(row?);
        }
        Ok(paths)
    }
}

fn row_to_track(row: &rusqlite::Row<'_>) -> rusqlite::Result<Track> {
    Ok(Track {
        id: row.get("id")?,
        path: row.get("path")?,
        title: row.get("title")?,
        artist: row.get("artist")?,
        album: row.get("album")?,
        album_artist: row.get("album_artist")?,
        track_number: row
            .get::<_, Option<i64>>("track_number")?
            .map(|value| value as u32),
        disc_number: row
            .get::<_, Option<i64>>("disc_number")?
            .map(|value| value as u32),
        duration_ms: row
            .get::<_, Option<i64>>("duration_ms")?
            .map(|value| value as u64),
        sample_rate: row
            .get::<_, Option<i64>>("sample_rate")?
            .map(|value| value as u32),
        channel_count: row
            .get::<_, Option<i64>>("channel_count")?
            .map(|value| value as u32),
        bit_depth: row
            .get::<_, Option<i64>>("bit_depth")?
            .map(|value| value as u32),
        modified_unix: row.get("modified_unix")?,
        size_bytes: row.get::<_, i64>("size_bytes")? as u64,
    })
}

fn collect_tracks<F>(rows: rusqlite::MappedRows<'_, F>) -> rusqlite::Result<Vec<Track>>
where
    F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<Track>,
{
    let mut tracks = Vec::new();
    for row in rows {
        tracks.push(row?);
    }
    Ok(tracks)
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_track(path: &str, title: &str, modified_unix: i64, size_bytes: u64) -> Track {
        Track {
            id: None,
            title: title.to_string(),
            artist: Some("ECHO".to_string()),
            album: Some("Night".to_string()),
            album_artist: None,
            track_number: Some(1),
            disc_number: None,
            duration_ms: Some(1000),
            sample_rate: Some(44100),
            channel_count: Some(2),
            bit_depth: Some(16),
            path: path.to_string(),
            modified_unix,
            size_bytes,
        }
    }

    #[test]
    fn upsert_replaces_changed_track() {
        let mut database = Database::open_memory().unwrap();
        database
            .upsert_tracks(&[sample_track("C:/Music/a.flac", "A", 1, 10)])
            .unwrap();
        database
            .upsert_tracks(&[sample_track("C:/Music/a.flac", "B", 2, 20)])
            .unwrap();

        let tracks = database.search_candidates("B", 10).unwrap();
        assert_eq!(tracks.len(), 1);
        assert_eq!(tracks[0].title, "B");
        assert_eq!(database.track_count().unwrap(), 1);
    }

    #[test]
    fn remove_missing_only_touches_scanned_root() {
        let mut database = Database::open_memory().unwrap();
        database
            .upsert_tracks(&[
                sample_track("C:/Music/a.flac", "A", 1, 10),
                sample_track("D:/Other/b.flac", "B", 1, 10),
            ])
            .unwrap();

        let removed = database
            .remove_missing_under(Path::new("C:/Music"), &HashSet::new())
            .unwrap();

        assert_eq!(removed, 1);
        assert_eq!(database.track_count().unwrap(), 1);
    }
}
