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
- `shell`, `scan`, `search`, `doctor`, `devices`, `play`, and `version` commands
- Symphonia-based streaming decode for local playback
- CPAL shared-mode output on the system default output device
- bounded decoder-to-output buffer
- typed playback events feeding CLI and shell status updates
- Shell-style REPL with a stateful prompt, live command suggestions,
  result-aware `play` and `search` completion, typo recovery, next-step tips,
  history, Tab/Enter completion, search results, playback controls, and
  folder-picker scanning
- session playback queue, automatic next-track advance, and `seek` controls
- focused tests for scanner filtering, metadata fallback, search ranking,
  database updates, command parsing, channel mapping, and shell helpers

Not implemented yet:

- WASAPI device switching and exclusive mode

## Quick start

```powershell
cargo run --
```

Launching `echo-cli` without a subcommand opens the ECHO shell. The shell is the
main experience: it keeps a normal terminal scrollback, shows command
suggestions as you type, and keeps accepting commands while music is playing.

First run:

1. Type `scan` or `add` to choose a music folder with the Windows folder picker.
2. After tracks appear, type `1`, `play`, `play 1`, or `shuffle`.
3. During playback, type `pause`, `resume`, `stop`, `seek +10`, `queue`, `next`, `prev`, `now`, or `quit`.
4. Press Enter on an empty prompt, or type `tips`, whenever you are unsure what to do next.
5. Press `Tab` on an empty prompt to accept the first suggested command.

Chinese guide:

```text
/language zh
扫描
播放 1
暂停
继续
停止
下一首
```

`/language`, `/language zh`, `/language en`, `/language status`, and
`/language list` work inside the shell. Chinese and English command names can be
mixed freely.

## Commands

```powershell
cargo run -- scan "D:\Music"
cargo run -- search "moon"
cargo run -- doctor
cargo run -- devices
cargo run -- play "D:\Music\song.flac"
cargo run -- shell
```

Launching `echo-cli` without a subcommand does the same thing as `echo-cli
shell`. `tui` remains a compatibility alias for the interactive ECHO shell, not
a full-screen panel UI.

Inside the shell:

- Press `?` or type `help` / `commands` for help.
- Slash commands also work: `/help`, `/home`, `/scan`, `/search`, `/play`, `/pause`, `/status`, and `/quit`.
- Type `/volume 65`, `volume +5`, or `volume mute` to control ECHO playback volume from 0 to 100.
- Type `meter`, `tokens`, `now`, or `status` during playback to see elapsed/remaining time, queue position, queue time remaining, seek undo position, up-next track, and simulated token/cost tracking. This is local UI only and never billed.
- Type `seek 1:30`, `seek 50%`, `seek +10%`, `seek undo`, `seek start`, `seek end`, `seek +10`, or `seek -10` to jump within the current track or return to the position before the last seek.
- Type `queue`, `queue all`, `queue undo`, `queue shuffle`, `queue reverse`, `queue dedupe`, `queue next 5`, `queue add 5`, `queue move 5 2`, `queue remove 4`, `queue 5`, `up next`, or `queue clear` to inspect known queued duration, expand, undo edits, shuffle/reverse upcoming tracks, remove duplicates, play next, append to, reorder, remove from, jump within, or trim the session playback queue.
- Type `/language`, `/language zh`, or `/language en` to switch the shell guide language; ECHO redraws the guide immediately. Use `/language status` or `/language list` to inspect it.
- Chinese command aliases are available too: `帮助`, `扫描`, `搜索 moon`, `播放 1`, `暂停`, `继续`, `停止`, `退出`.
- Type `aliases` to see alternate command names.
- Type `scan` or `add` to open a Windows folder picker and scan a music folder.
- Type `scan D:\Music` to scan a folder path directly.
- If scanning reports failed files, type `errors` to inspect the recent failures.
- After scanning, ECHO lists numbered tracks; type `1`, `play 1`, or just `play`.
- Type `next` or `prev` to move through the current search/list results.
- Playing from visible results makes that result list the active session queue; a natural track finish advances to the next queued song.
- Type `play #3`, `play next`, or `play prev` if that feels more natural.
- Type `shuffle`, `surprise`, or `play random` to play a random track from the active queue or current results.
- Type `list`, `recent`, `songs`, or `tracks` to show the current library list again.
- Type `results` to print the current search/list results again without resetting them.
- Type `more` to expand the current search/list results beyond the first 20.
- Result lists show numbered rows, column labels, visible counts, and the next action.
- Type `search moon` or `find moon` to search and then `play 1` to play a numbered result.
- Type `info 1` to inspect a numbered result before playing it.
- Or just type `moon` / `moon halo`; bare text searches the library.
- Type a prefix like `p` to list matching commands such as `play` and `pause`.
- Type `play ` after a search or library listing to pick from the current results by number or title.
- Type `open ` to reveal a current result in Explorer.
- Type `reveal`, `folder`, or `where` if you want to find the current track on disk.
- Type `copy ` to copy a current result path to the Windows clipboard.
- If a search has no matches, ECHO suggests simpler keywords, `library`, or `scan`.
- Type `search ` after a listing to complete from visible track titles.
- Use `Up/Down` to select a suggestion, `Tab` to complete it, or `Enter` to accept/run it.
- Complete commands such as `play`, `scan`, `open`, `copy`, and `info` run immediately on Enter.
- Suggestion lists show the selection controls and how many more matches are hidden.
- Use `Up/Down` on an empty prompt to browse saved command history across sessions.
- Type `history` to show saved commands, or `!7` to replay entry #7.
- Use `Left/Right`, `Home/End`, `Delete`, and `Backspace` to edit the prompt.
- Use `Ctrl+Left/Right`, `Ctrl+W`, `Ctrl+K`, and `Ctrl+L` for faster shell editing.
- Type `shortcuts` or `keys` to show the keyboard shortcuts again.
- The prompt changes from `echo ready>` to `echo playing>` while music is active.
- Type `now`, `current`, or `playing` to show the current track.
- Press Enter on an empty prompt, or type `tips`, whenever you are unsure what to do next.
- Type `home` to show the welcome screen and current library view again.
- Type `help play`, `help search`, or `help scan` for focused command help.
- Type `again` to repeat the last command.
- During playback, the prompt stays usable and shows elapsed time, queue position when useful, and simulated token cost; type `pause`, `resume`, `stop`, `volume 65`, `volume +5`, `seek +10`, `queue`, `meter`, `shuffle`, `play next`, `play prev`, or `quit`.
- Type `q` or `exit` if your fingers prefer shorter exits.
- If a command is mistyped, ECHO suggests the closest commands instead of leaving you stuck.
- Type `status`, `health` / `doctor`, `devices` / `outputs`, `errors`, or `open-db` for shell diagnostics.

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
