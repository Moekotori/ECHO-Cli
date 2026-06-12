# ECHO CLI

ECHO CLI is a fast, small, Windows-first terminal music player in Rust.

The direction is Claude Code-inspired without copying its branding or exact UI:
minimal panels, keyboard-first flow, sharp status lines, and a premium developer
tool feeling. ECHO stays focused on local music and serious playback.

## Phase 1 status

Implemented:

- Rust project architecture
- config/cache path setup
- SQLite library database
- high-performance folder scanning with parallel metadata reads
- Lofty metadata extraction without album art usage
- incremental scan by path, modified time, and size
- search ranking for title, artist, album, filename, and path
- `scan`, `search`, `doctor`, `devices`, `play`, `tui`, and `version` commands
- focused tests for scanner filtering, metadata fallback, search ranking,
  database updates, and command parsing

Not implemented yet:

- real Ratatui interface
- playback and decoder pipeline
- WASAPI device enumeration, switching, and exclusive mode

## Commands

```powershell
cargo run -- scan "D:\Music"
cargo run -- search "moon"
cargo run -- doctor
cargo run -- devices
cargo run -- tui
```

Launching `echo-cli` without a subcommand opens the Phase 1 terminal preview.

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
