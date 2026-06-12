use crate::config::AppPaths;
use crate::db::Database;
use crate::device::{self, AudioDevice};
use crate::error::{EchoError, Result};
use crate::library::Track;
use crate::playback::{PlaybackEngine, PlaybackEvent};
use crate::{scanner, search};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Gauge, List, ListItem, ListState, Paragraph, Wrap};
use std::io::{self, Stdout};
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

const LIBRARY_LIMIT: usize = 500;
const SEARCH_LIMIT: usize = 200;

pub fn run(paths: &AppPaths) -> Result<()> {
    let mut terminal = TerminalSession::start()?;
    let result = run_app(&mut terminal.terminal, paths);
    terminal.stop()?;
    result
}

fn run_app(terminal: &mut Terminal<CrosstermBackend<Stdout>>, paths: &AppPaths) -> Result<()> {
    let database = Database::open(paths.database_path())?;
    let devices = device::list_devices();
    let (playback_tx, playback_rx) = mpsc::channel();
    let (scan_tx, scan_rx) = mpsc::channel();
    let mut app = TuiApp::load(database, devices, playback_tx, scan_tx)?;

    while !app.should_quit {
        app.drain_background_events(&playback_rx, &scan_rx)?;
        terminal.draw(|frame| draw(frame, &mut app))?;

        if event::poll(Duration::from_millis(50))?
            && let Event::Key(key) = event::read()?
        {
            app.handle_key(key)?;
        }
    }

    Ok(())
}

struct TerminalSession {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl TerminalSession {
    fn start() -> Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend)?;
        Ok(Self { terminal })
    }

    fn stop(&mut self) -> Result<()> {
        disable_raw_mode()?;
        execute!(self.terminal.backend_mut(), LeaveAlternateScreen)?;
        self.terminal.show_cursor()?;
        Ok(())
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
        let _ = self.terminal.show_cursor();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputMode {
    Browse,
    Search,
    Command,
}

struct NowPlaying {
    title: String,
    path: String,
    status: String,
    output: String,
    source_format: String,
    started: Option<Instant>,
    duration_ms: Option<u64>,
    progress_ms: Option<u64>,
}

impl NowPlaying {
    fn empty() -> Self {
        Self {
            title: "No track".to_string(),
            path: String::new(),
            status: "idle".to_string(),
            output: "system default".to_string(),
            source_format: "unknown".to_string(),
            started: None,
            duration_ms: None,
            progress_ms: None,
        }
    }

    fn estimated_progress(&self) -> Option<u64> {
        self.started
            .map(|started| started.elapsed().as_millis() as u64)
            .or(self.progress_ms)
    }
}

struct TuiApp {
    database: Database,
    tracks: Vec<Track>,
    selected: usize,
    list_state: ListState,
    devices: Vec<AudioDevice>,
    device_panel_visible: bool,
    input_mode: InputMode,
    input: String,
    status: String,
    command_history: Vec<String>,
    now_playing: NowPlaying,
    playback_tx: Sender<PlaybackEvent>,
    playback_busy: bool,
    scan_tx: Sender<ScanMessage>,
    scan_busy: bool,
    library_count: u64,
    should_quit: bool,
}

impl TuiApp {
    fn load(
        database: Database,
        devices: Vec<AudioDevice>,
        playback_tx: Sender<PlaybackEvent>,
        scan_tx: Sender<ScanMessage>,
    ) -> Result<Self> {
        let library_count = database.track_count()?;
        let tracks = search::search(&database, "", LIBRARY_LIMIT)?
            .into_iter()
            .map(|result| result.track)
            .collect::<Vec<_>>();
        let mut app = Self {
            database,
            tracks,
            selected: 0,
            list_state: ListState::default(),
            devices,
            device_panel_visible: true,
            input_mode: InputMode::Browse,
            input: String::new(),
            status: "ready".to_string(),
            command_history: Vec::new(),
            now_playing: NowPlaying::empty(),
            playback_tx,
            playback_busy: false,
            scan_tx,
            scan_busy: false,
            library_count,
            should_quit: false,
        };
        app.sync_selection();
        Ok(app)
    }

    fn handle_key(&mut self, key: KeyEvent) -> Result<()> {
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            self.should_quit();
            return Ok(());
        }

        match self.input_mode {
            InputMode::Browse => self.handle_browse_key(key),
            InputMode::Search => self.handle_search_key(key),
            InputMode::Command => self.handle_command_key(key),
        }
    }

    fn handle_browse_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Char('q') => self.should_quit(),
            KeyCode::Char('/') => {
                self.input_mode = InputMode::Search;
                self.input.clear();
                self.status = "search".to_string();
            }
            KeyCode::Char(':') => {
                self.input_mode = InputMode::Command;
                self.input.clear();
                self.status = "command".to_string();
            }
            KeyCode::Down | KeyCode::Char('j') => self.move_selection(1),
            KeyCode::Up | KeyCode::Char('k') => self.move_selection(-1),
            KeyCode::Enter => self.play_selected(),
            KeyCode::Char('d') => {
                self.device_panel_visible = !self.device_panel_visible;
            }
            KeyCode::Char('x') => {
                self.status = "exclusive mode lands in Phase 4".to_string();
            }
            KeyCode::Char(' ') => {
                self.status = "pause/resume lands after nonblocking controls".to_string();
            }
            KeyCode::Char('n') => {
                self.move_selection(1);
                self.play_selected();
            }
            KeyCode::Char('p') => {
                self.move_selection(-1);
                self.play_selected();
            }
            _ => {}
        }
        Ok(())
    }

    fn handle_search_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Esc => {
                self.input_mode = InputMode::Browse;
                self.input.clear();
                self.refresh_library()?;
            }
            KeyCode::Enter => {
                self.input_mode = InputMode::Browse;
                self.play_selected();
            }
            KeyCode::Backspace => {
                self.input.pop();
                self.apply_search()?;
            }
            KeyCode::Char(character) => {
                self.input.push(character);
                self.apply_search()?;
            }
            KeyCode::Down => self.move_selection(1),
            KeyCode::Up => self.move_selection(-1),
            _ => {}
        }
        Ok(())
    }

    fn handle_command_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Esc => {
                self.input_mode = InputMode::Browse;
                self.input.clear();
                self.status = "ready".to_string();
            }
            KeyCode::Enter => {
                let command = self.input.trim().to_string();
                self.input.clear();
                self.input_mode = InputMode::Browse;
                self.run_command(&command)?;
            }
            KeyCode::Backspace => {
                self.input.pop();
            }
            KeyCode::Char(character) => self.input.push(character),
            _ => {}
        }
        Ok(())
    }

    fn run_command(&mut self, command: &str) -> Result<()> {
        if command.is_empty() {
            self.status = "ready".to_string();
            return Ok(());
        }

        self.command_history.push(command.to_string());
        let mut parts = command.splitn(2, char::is_whitespace);
        let name = parts.next().unwrap_or_default();
        let argument = parts.next().unwrap_or_default().trim();

        match name {
            "q" | "quit" => self.should_quit(),
            "scan" => self.start_scan(argument)?,
            "devices" => {
                self.devices = device::list_devices();
                self.device_panel_visible = true;
                self.status = format!("{} output devices", self.devices.len());
            }
            "doctor" => {
                self.library_count = self.database.track_count()?;
                self.status = format!(
                    "{} tracks / {} devices / {}",
                    self.library_count,
                    self.devices.len(),
                    device::default_device_name()
                );
            }
            "clear" => {
                self.command_history.clear();
                self.status = "cleared".to_string();
            }
            _ => {
                self.status = format!("unknown command: {name}");
            }
        }

        Ok(())
    }

    fn apply_search(&mut self) -> Result<()> {
        self.tracks = search::search(&self.database, &self.input, SEARCH_LIMIT)?
            .into_iter()
            .map(|result| result.track)
            .collect();
        self.selected = 0;
        self.sync_selection();
        self.status = format!("{} matches", self.tracks.len());
        Ok(())
    }

    fn refresh_library(&mut self) -> Result<()> {
        self.library_count = self.database.track_count()?;
        self.tracks = search::search(&self.database, "", LIBRARY_LIMIT)?
            .into_iter()
            .map(|result| result.track)
            .collect();
        self.selected = self.selected.min(self.tracks.len().saturating_sub(1));
        self.sync_selection();
        self.status = format!("{} tracks indexed", self.library_count);
        Ok(())
    }

    fn move_selection(&mut self, delta: isize) {
        if self.tracks.is_empty() {
            self.selected = 0;
        } else if delta.is_negative() {
            self.selected = self.selected.saturating_sub(delta.unsigned_abs());
        } else {
            self.selected = (self.selected + delta as usize).min(self.tracks.len() - 1);
        }
        self.sync_selection();
    }

    fn sync_selection(&mut self) {
        if self.tracks.is_empty() {
            self.list_state.select(None);
        } else {
            self.list_state.select(Some(self.selected));
        }
    }

    fn play_selected(&mut self) {
        if self.playback_busy {
            self.status = "playback is already active".to_string();
            return;
        }

        let Some(track) = self.tracks.get(self.selected).cloned() else {
            self.status = "library is empty".to_string();
            return;
        };

        self.playback_busy = true;
        self.status = format!("loading {}", track.title);
        let tx = self.playback_tx.clone();
        thread::Builder::new()
            .name("echo-cli-tui-playback".to_string())
            .spawn(move || {
                let engine = PlaybackEngine::new();
                if let Err(error) = engine.play_blocking(&track, |event| {
                    let _ = tx.send(event);
                }) {
                    let _ = tx.send(PlaybackEvent::Error {
                        path: track.path,
                        message: error.to_string(),
                    });
                }
            })
            .map(|_| ())
            .unwrap_or_else(|error| {
                self.playback_busy = false;
                self.status = format!("playback thread failed: {error}");
            });
    }

    fn start_scan(&mut self, folder: &str) -> Result<()> {
        if self.scan_busy {
            self.status = "scan is already active".to_string();
            return Ok(());
        }

        if folder.is_empty() {
            self.status = "scan needs a folder".to_string();
            return Ok(());
        }

        let folder = PathBuf::from(folder);
        if !folder.exists() {
            self.status = format!("folder not found: {}", folder.display());
            return Ok(());
        }

        self.scan_busy = true;
        self.status = format!("scanning {}", folder.display());
        let tx = self.scan_tx.clone();
        thread::Builder::new()
            .name("echo-cli-tui-scan".to_string())
            .spawn(move || {
                let message = match AppPaths::load()
                    .and_then(|paths| Database::open(paths.database_path()))
                    .and_then(|mut database| scanner::scan_folder(&mut database, &folder))
                {
                    Ok(summary) => ScanMessage::Finished(format!(
                        "scan indexed {} / skipped {} / failed {}",
                        summary.indexed_tracks, summary.skipped_unchanged, summary.failed_files
                    )),
                    Err(error) => ScanMessage::Failed(error.to_string()),
                };
                let _ = tx.send(message);
            })
            .map(|_| ())
            .map_err(|error| EchoError::Playback(error.to_string()))
    }

    fn drain_background_events(
        &mut self,
        playback_rx: &Receiver<PlaybackEvent>,
        scan_rx: &Receiver<ScanMessage>,
    ) -> Result<()> {
        while let Ok(event) = playback_rx.try_recv() {
            self.apply_playback_event(event);
        }

        while let Ok(message) = scan_rx.try_recv() {
            self.scan_busy = false;
            match message {
                ScanMessage::Finished(status) => {
                    self.status = status;
                    self.database = Database::open(AppPaths::load()?.database_path())?;
                    self.refresh_library()?;
                }
                ScanMessage::Failed(error) => {
                    self.status = format!("scan failed: {error}");
                }
            }
        }

        Ok(())
    }

    fn apply_playback_event(&mut self, event: PlaybackEvent) {
        match event {
            PlaybackEvent::Loading { title, path } => {
                self.now_playing.title = title;
                self.now_playing.path = path;
                self.now_playing.status = "decoding".to_string();
                self.now_playing.started = None;
                self.now_playing.duration_ms = None;
                self.status = "decoding".to_string();
            }
            PlaybackEvent::Playing {
                title,
                stream,
                output,
            } => {
                self.now_playing.title = title;
                self.now_playing.status = "playing".to_string();
                self.now_playing.output = format!(
                    "{} / {} / {} Hz",
                    output.device_name, output.mode, output.sample_rate
                );
                self.now_playing.source_format = format!(
                    "{} Hz / {}ch{}",
                    stream.sample_rate,
                    stream.channel_count,
                    stream
                        .bit_depth
                        .map(|bits| format!(" / {bits}-bit"))
                        .unwrap_or_default()
                );
                self.now_playing.duration_ms = stream.duration_ms;
                self.now_playing.started = Some(Instant::now());
                self.status = "playing".to_string();
            }
            PlaybackEvent::Warning(message) => {
                self.status = format!("warning: {message}");
            }
            PlaybackEvent::Finished { elapsed_ms, .. } => {
                self.playback_busy = false;
                self.now_playing.status = "finished".to_string();
                self.now_playing.progress_ms = self.now_playing.duration_ms;
                self.now_playing.started = None;
                self.status = format!("finished in {elapsed_ms} ms");
            }
            PlaybackEvent::Error { message, .. } => {
                self.playback_busy = false;
                self.now_playing.status = "error".to_string();
                self.now_playing.started = None;
                self.status = message;
            }
        }
    }

    fn should_quit(&mut self) {
        self.should_quit = true;
    }

    fn prompt(&self) -> String {
        match self.input_mode {
            InputMode::Browse => self.status.clone(),
            InputMode::Search => format!("/{}", self.input),
            InputMode::Command => format!(":{}", self.input),
        }
    }

    fn title_line(&self) -> String {
        let mode = match self.input_mode {
            InputMode::Browse => "browse",
            InputMode::Search => "search",
            InputMode::Command => "command",
        };
        format!("ECHO CLI  {mode}  {} tracks", self.library_count)
    }
}

enum ScanMessage {
    Finished(String),
    Failed(String),
}

fn draw(frame: &mut ratatui::Frame<'_>, app: &mut TuiApp) {
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(8),
            Constraint::Length(3),
        ])
        .split(frame.area());

    draw_header(frame, root[0], app);
    draw_body(frame, root[1], app);
    draw_prompt(frame, root[2], app);
}

fn draw_header(frame: &mut ratatui::Frame<'_>, area: Rect, app: &TuiApp) {
    let line = Line::from(vec![
        Span::styled(
            " ECHO CLI ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(app.title_line()),
    ]);
    frame.render_widget(Paragraph::new(line), area);
}

fn draw_body(frame: &mut ratatui::Frame<'_>, area: Rect, app: &mut TuiApp) {
    let horizontal = if app.device_panel_visible {
        Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(34),
                Constraint::Percentage(42),
                Constraint::Percentage(24),
            ])
            .split(area)
    } else {
        Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(38), Constraint::Percentage(62)])
            .split(area)
    };

    draw_library(frame, horizontal[0], app);
    draw_now(frame, horizontal[1], app);
    if app.device_panel_visible && horizontal.len() > 2 {
        draw_devices(frame, horizontal[2], app);
    }
}

fn draw_library(frame: &mut ratatui::Frame<'_>, area: Rect, app: &mut TuiApp) {
    let items = app
        .tracks
        .iter()
        .map(|track| {
            let artist = track.artist.as_deref().unwrap_or("unknown artist");
            ListItem::new(Line::from(vec![
                Span::styled(
                    compact(&track.title, 34),
                    Style::default()
                        .fg(Color::White)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw("  "),
                Span::styled(compact(artist, 22), Style::default().fg(Color::DarkGray)),
            ]))
        })
        .collect::<Vec<_>>();

    let title = if app.input_mode == InputMode::Search {
        format!(" Library / {}", app.input)
    } else {
        " Library ".to_string()
    };

    let list = List::new(items)
        .block(Block::default().title(title).borders(Borders::ALL))
        .highlight_style(Style::default().fg(Color::Black).bg(Color::Cyan))
        .highlight_symbol("> ");
    frame.render_stateful_widget(list, area, &mut app.list_state);
}

fn draw_now(frame: &mut ratatui::Frame<'_>, area: Rect, app: &TuiApp) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(8),
            Constraint::Length(3),
            Constraint::Min(4),
        ])
        .split(area);

    let now = vec![
        Line::from(vec![
            Span::styled("track   ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                compact(&app.now_playing.title, 56),
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled("state   ", Style::default().fg(Color::DarkGray)),
            Span::raw(&app.now_playing.status),
        ]),
        Line::from(vec![
            Span::styled("output  ", Style::default().fg(Color::DarkGray)),
            Span::raw(compact(&app.now_playing.output, 58)),
        ]),
        Line::from(vec![
            Span::styled("source  ", Style::default().fg(Color::DarkGray)),
            Span::raw(&app.now_playing.source_format),
        ]),
        Line::from(vec![
            Span::styled("path    ", Style::default().fg(Color::DarkGray)),
            Span::raw(compact(&app.now_playing.path, 58)),
        ]),
    ];

    frame.render_widget(
        Paragraph::new(now)
            .block(Block::default().title(" Now ").borders(Borders::ALL))
            .wrap(Wrap { trim: true }),
        chunks[0],
    );

    let progress = progress_ratio(app);
    let label = progress_label(app);
    frame.render_widget(
        Gauge::default()
            .block(Block::default().title(" Progress ").borders(Borders::ALL))
            .gauge_style(Style::default().fg(Color::Cyan))
            .ratio(progress)
            .label(label),
        chunks[1],
    );

    let history = app
        .command_history
        .iter()
        .rev()
        .take(5)
        .map(|command| Line::raw(format!(":{}", compact(command, 64))))
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(history)
            .block(Block::default().title(" Console ").borders(Borders::ALL))
            .wrap(Wrap { trim: true }),
        chunks[2],
    );
}

fn draw_devices(frame: &mut ratatui::Frame<'_>, area: Rect, app: &TuiApp) {
    let lines = app
        .devices
        .iter()
        .map(|device| {
            let marker = if device.is_default { "* " } else { "  " };
            Line::from(vec![
                Span::styled(marker, Style::default().fg(Color::Cyan)),
                Span::raw(compact(&device.name, 32)),
            ])
        })
        .collect::<Vec<_>>();

    frame.render_widget(
        Paragraph::new(lines)
            .block(Block::default().title(" Output ").borders(Borders::ALL))
            .wrap(Wrap { trim: true }),
        area,
    );
}

fn draw_prompt(frame: &mut ratatui::Frame<'_>, area: Rect, app: &TuiApp) {
    let block = Block::default().title(" Command ").borders(Borders::ALL);
    let paragraph = Paragraph::new(app.prompt()).block(block);
    frame.render_widget(paragraph, area);

    if matches!(app.input_mode, InputMode::Search | InputMode::Command) {
        let x = area.x + 1 + app.prompt().chars().count() as u16;
        let y = area.y + 1;
        if x < area.right() {
            frame.set_cursor_position((x, y));
        }
    }

    if app.scan_busy || app.playback_busy {
        let overlay = centered_rect(38, 3, frame.area());
        let text = if app.scan_busy { "scanning" } else { "playing" };
        frame.render_widget(Clear, overlay);
        frame.render_widget(
            Paragraph::new(text)
                .style(Style::default().fg(Color::Cyan))
                .block(Block::default().borders(Borders::ALL)),
            overlay,
        );
    }
}

fn progress_ratio(app: &TuiApp) -> f64 {
    let Some(duration_ms) = app.now_playing.duration_ms else {
        return 0.0;
    };
    if duration_ms == 0 {
        return 0.0;
    }

    let progress_ms = app.now_playing.estimated_progress().unwrap_or(0);
    (progress_ms.min(duration_ms) as f64 / duration_ms as f64).clamp(0.0, 1.0)
}

fn progress_label(app: &TuiApp) -> String {
    let progress = app.now_playing.estimated_progress().unwrap_or(0);
    let duration = app.now_playing.duration_ms.unwrap_or(0);
    format!(
        "{} / {}",
        format_duration(progress),
        format_duration(duration)
    )
}

fn format_duration(duration_ms: u64) -> String {
    let total_seconds = duration_ms / 1000;
    let minutes = total_seconds / 60;
    let seconds = total_seconds % 60;
    format!("{minutes}:{seconds:02}")
}

fn compact(value: &str, width: usize) -> String {
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

fn centered_rect(percent_x: u16, height: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(0),
            Constraint::Length(height),
            Constraint::Min(0),
        ])
        .split(area);
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vertical[1]);
    horizontal[1]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_shortens_long_text_to_width() {
        assert_eq!(compact("abcdef", 4), "a...");
        assert_eq!(compact("abc", 4), "abc");
    }

    #[test]
    fn progress_ratio_is_clamped() {
        let mut app = minimal_app();
        app.now_playing.duration_ms = Some(1000);
        app.now_playing.progress_ms = Some(1500);

        assert_eq!(progress_ratio(&app), 1.0);
    }

    fn minimal_app() -> TuiApp {
        let database = Database::open_memory().unwrap();
        let (playback_tx, _) = mpsc::channel();
        let (scan_tx, _) = mpsc::channel();
        TuiApp::load(database, Vec::new(), playback_tx, scan_tx).unwrap()
    }
}
