# ECHO CLI

ECHO CLI is a fast, small, Windows-first terminal music player in Rust.

The direction is Claude Code-inspired without copying its branding or exact UI:
minimal panels, keyboard-first flow, sharp status lines, and a premium developer
tool feeling. ECHO stays focused on local music and serious playback.

## Current status

Implemented:

- Rust project architecture
- config/cache path setup
- SQLite library database
- high-performance folder scanning with parallel metadata reads
- Lofty metadata extraction without album art usage
- incremental scan by path, modified time, and size
- search ranking for title, artist, album, filename, and path
- `scan`, `search`, `doctor`, `devices`, `play`, `tui`, and `version` commands
- Symphonia-based streaming decode for local playback
- CPAL shared-mode output on the system default output device
- bounded decoder-to-output buffer
- typed playback events feeding CLI and TUI status updates
- Ratatui terminal UI with library browsing, instant search, command input,
  output-device panel, now-playing state, and background playback events
- focused tests for scanner filtering, metadata fallback, search ranking,
  database updates, command parsing, channel mapping, and TUI helpers

Not implemented yet:

- pause/resume controls in the TUI and CLI
- WASAPI device switching and exclusive mode

## Commands

```powershell
cargo run -- scan "D:\Music"
cargo run -- search "moon"
cargo run -- doctor
cargo run -- devices
cargo run -- play "D:\Music\song.flac"
cargo run -- tui
```

Launching `echo-cli` without a subcommand opens the Ratatui terminal UI.

## Design rules

- UI and playback stay separated.
- No blocking scan or decode work belongs on the UI thread.
- No FFmpeg, mpv, Electron, webview, or GUI framework.
- Do not scan, decode, or display album covers.
- Prefer small dependencies and human-readable errors.
- Keep each phase small, working, and testable.

## Verification

```powershell
cargo fmt
cargo test
cargo clippy --all-targets --all-features
cargo build --release
```
