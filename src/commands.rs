use crate::app::{APP_NAME, SUPPORTED_EXTENSIONS};
use crate::audio_backend;
use crate::audio_backend_wasapi;
use crate::config::AppPaths;
use crate::db::Database;
use crate::device;
use crate::error::Result;
use crate::metadata;
use crate::playback::{PlaybackEngine, PlaybackEvent};
use crate::scanner;
use crate::search;
use crate::tui;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "echo-cli")]
#[command(version)]
#[command(about = "A fast, small, Claude Code-inspired terminal music player.")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Open the terminal UI.
    Tui,
    /// Scan a music folder into the local library database.
    Scan { folder: PathBuf },
    /// Search the indexed library.
    Search { query: String },
    /// Play a path or indexed search result.
    Play { query_or_path: String },
    /// List audio devices. Real WASAPI enumeration arrives in Phase 4.
    Devices,
    /// Save the preferred output device. Real switching arrives in Phase 4.
    UseDevice { device_id_or_name: String },
    /// Print runtime, database, and audio backend diagnostics.
    Doctor,
    /// Print version information.
    Version,
}

pub fn run(cli: Cli) -> Result<()> {
    match cli.command.unwrap_or(Command::Tui) {
        Command::Tui => {
            let paths = AppPaths::load()?;
            tui::run(&paths)
        }
        Command::Scan { folder } => run_scan(folder),
        Command::Search { query } => run_search(&query),
        Command::Play { query_or_path } => run_play(&query_or_path),
        Command::Devices => run_devices(),
        Command::UseDevice { device_id_or_name } => run_use_device(&device_id_or_name),
        Command::Doctor => run_doctor(),
        Command::Version => {
            println!("{} {}", APP_NAME, env!("CARGO_PKG_VERSION"));
            Ok(())
        }
    }
}

fn run_scan(folder: PathBuf) -> Result<()> {
    let paths = AppPaths::load()?;
    let mut database = Database::open(paths.database_path())?;

    println!("ECHO CLI  scan");
    println!("  folder  {}", folder.display());
    println!("  db      {}", paths.database_path().display());

    let summary = scanner::scan_folder(&mut database, &folder)?;

    println!();
    println!("indexed   {}", summary.indexed_tracks);
    println!("scanned   {}", summary.scanned_files);
    println!("skipped   {}", summary.skipped_unchanged);
    println!("failed    {}", summary.failed_files);
    println!("removed   {}", summary.removed_missing);
    println!("elapsed   {} ms", summary.elapsed_ms);
    Ok(())
}

fn run_search(query: &str) -> Result<()> {
    let paths = AppPaths::load()?;
    let database = Database::open(paths.database_path())?;
    let results = search::search(&database, query, 20)?;

    println!("ECHO CLI  search  /{query}");
    if results.is_empty() {
        println!("no matches");
        return Ok(());
    }

    for (index, result) in results.iter().enumerate() {
        let artist = result.track.artist.as_deref().unwrap_or("unknown artist");
        println!(
            "{:>2}. {:<42}  {:<28}  {}",
            index + 1,
            truncate(&result.track.title, 42),
            truncate(artist, 28),
            result.track.path
        );
    }
    Ok(())
}

fn run_play(query_or_path: &str) -> Result<()> {
    let paths = AppPaths::load()?;
    let database = Database::open(paths.database_path())?;
    let track = resolve_play_target(&database, query_or_path)?;
    let engine = PlaybackEngine::new();

    engine.play_blocking(&track, |event| print_playback_event(&event))
}

fn run_devices() -> Result<()> {
    let devices = device::list_devices();
    println!("ECHO CLI  devices");
    for device in devices {
        println!(
            "- {} [{}] {}",
            device.name,
            device.id,
            if device.is_default { "default" } else { "" }
        );
    }
    Ok(())
}

fn run_use_device(device_id_or_name: &str) -> Result<()> {
    println!("device preference is a Phase 4 feature: {device_id_or_name}");
    println!("WASAPI device switching is intentionally not wired in Phase 3.");
    Ok(())
}

fn run_doctor() -> Result<()> {
    let paths = AppPaths::load()?;
    let database = Database::open(paths.database_path())?;
    let recent_errors = database.recent_scan_errors(5)?;

    println!("ECHO CLI  doctor");
    println!(
        "  os                     {} {}",
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    println!(
        "  audio backend          {}",
        audio_backend::backend_status_line()
    );
    println!(
        "  wasapi exclusive       {}",
        audio_backend_wasapi::exclusive_status_line()
    );
    println!("  default output device  {}", device::default_device_name());
    println!("  available devices");
    for audio_device in device::list_devices() {
        println!(
            "    - {}{}",
            audio_device.name,
            if audio_device.is_default {
                " (default)"
            } else {
                ""
            }
        );
    }
    println!(
        "  database path          {}",
        paths.database_path().display()
    );
    println!("  config path            {}", paths.config_dir.display());
    println!("  cache path             {}", paths.cache_dir.display());
    println!("  library tracks         {}", database.track_count()?);
    println!(
        "  supported formats      {}",
        SUPPORTED_EXTENSIONS.join(", ")
    );

    if recent_errors.is_empty() {
        println!("  recent scan errors     none");
    } else {
        println!("  recent scan errors");
        for (path, error) in recent_errors {
            println!("    - {} :: {}", truncate(&path, 72), truncate(&error, 72));
        }
    }

    Ok(())
}

fn resolve_play_target(database: &Database, query_or_path: &str) -> Result<crate::library::Track> {
    let as_path = PathBuf::from(query_or_path);
    if as_path.exists() {
        let metadata = std::fs::metadata(&as_path)?;
        return metadata::read_track(&as_path, &metadata)
            .or_else(|_| metadata::fallback_track(&as_path, &metadata));
    }

    if let Some(track) = database.find_exact_path(&as_path)? {
        return Ok(track);
    }

    let results = search::search(database, query_or_path, 1)?;
    results
        .into_iter()
        .next()
        .map(|result| result.track)
        .ok_or_else(|| {
            crate::error::EchoError::Playback(format!("no playable match for: {query_or_path}"))
        })
}

fn print_playback_event(event: &PlaybackEvent) {
    match event {
        PlaybackEvent::Loading { title, path } => {
            println!("ECHO  playing");
            println!("  track    {title}");
            println!("  source   {path}");
            println!("  status   decoding...");
        }
        PlaybackEvent::Playing { stream, output, .. } => {
            println!("  output   {} / {}", output.device_name, output.mode);
            println!(
                "  format   {} Hz / {}ch / {}",
                output.sample_rate, output.channel_count, output.sample_format
            );
            println!("  buffer   {}", output.buffer_size);
            println!(
                "  source   {} Hz / {}ch{}",
                stream.sample_rate,
                stream.channel_count,
                stream
                    .bit_depth
                    .map(|bits| format!(" / {bits}-bit"))
                    .unwrap_or_default()
            );
            if let Some(duration_ms) = stream.duration_ms {
                println!("  length   {}", format_duration(duration_ms));
            }
            println!("  status   playing");
        }
        PlaybackEvent::Warning(message) => {
            println!("  warning  {message}");
        }
        PlaybackEvent::Finished { elapsed_ms, .. } => {
            println!("  status   finished in {} ms", elapsed_ms);
        }
        PlaybackEvent::Error { message, .. } => {
            println!("  status   failed");
            println!("  error    {message}");
        }
    }
}

fn format_duration(duration_ms: u64) -> String {
    let total_seconds = duration_ms / 1000;
    let minutes = total_seconds / 60;
    let seconds = total_seconds % 60;
    format!("{minutes}:{seconds:02}")
}

fn truncate(value: &str, width: usize) -> String {
    let char_count = value.chars().count();
    if char_count <= width {
        return value.to_string();
    }

    if width <= 3 {
        return ".".repeat(width);
    }

    let prefix: String = value.chars().take(width - 3).collect();
    format!("{prefix}...")
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn command_parser_accepts_required_phase1_commands() {
        Cli::command().debug_assert();
        Cli::try_parse_from(["echo-cli", "scan", "C:/Music"]).unwrap();
        Cli::try_parse_from(["echo-cli", "search", "moon"]).unwrap();
        Cli::try_parse_from(["echo-cli", "doctor"]).unwrap();
        Cli::try_parse_from(["echo-cli", "devices"]).unwrap();
    }
}
