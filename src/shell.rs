use crate::app::{APP_NAME, SUPPORTED_EXTENSIONS};
use crate::audio_backend;
use crate::audio_backend_wasapi;
use crate::config::AppPaths;
use crate::db::Database;
use crate::device;
use crate::error::{EchoError, Result};
use crate::library::Track;
use crate::metadata;
use crate::playback::{PlaybackControl, PlaybackEngine, PlaybackEvent};
use crate::{scanner, search};
use crossterm::cursor;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use crossterm::queue;
use crossterm::style::{
    Attribute, Color, Print, ResetColor, SetAttribute, SetForegroundColor, Stylize,
};
use crossterm::terminal::{self, Clear, ClearType};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const RESULT_LIMIT: usize = 20;
const RESULT_PAGE_SIZE: usize = 20;
const HISTORY_LIMIT: usize = 200;
const LANGUAGE_FILE: &str = "language.txt";

pub fn run(paths: &AppPaths) -> Result<()> {
    let database = Database::open(paths.database_path())?;
    let mut shell = EchoShell::new(paths.clone(), database)?;
    shell.run()
}

struct EchoShell {
    paths: AppPaths,
    database: Database,
    results: Vec<Track>,
    result_query: String,
    result_label: String,
    result_limit: usize,
    has_more_results: bool,
    current_track: Option<Track>,
    playback: Option<PlaybackSession>,
    last_command: Option<String>,
    language: ShellLanguage,
}

struct PlaybackSession {
    title: String,
    control_tx: Sender<PlaybackControl>,
    event_rx: Receiver<PlaybackEvent>,
    done_rx: Receiver<()>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShellLanguage {
    English,
    Chinese,
}

impl ShellLanguage {
    fn label(self) -> &'static str {
        match self {
            Self::English => "English",
            Self::Chinese => "中文",
        }
    }

    fn code(self) -> &'static str {
        match self {
            Self::English => "en",
            Self::Chinese => "zh",
        }
    }

    fn toggled(self) -> Self {
        match self {
            Self::English => Self::Chinese,
            Self::Chinese => Self::English,
        }
    }
}

impl EchoShell {
    fn new(paths: AppPaths, database: Database) -> Result<Self> {
        let (results, has_more_results) = load_result_window(&database, "", RESULT_LIMIT)?;
        let language = load_language(&paths);
        Ok(Self {
            paths,
            database,
            results,
            result_query: String::new(),
            result_label: "library".to_string(),
            result_limit: RESULT_LIMIT,
            has_more_results,
            current_track: None,
            playback: None,
            last_command: None,
            language,
        })
    }

    fn run(&mut self) -> Result<()> {
        self.print_welcome()?;
        let history_path = self.paths.config_dir.join("history.txt");
        let mut reader = ShellReader::load(history_path)?;

        loop {
            print_lines(self.drain_playback_lines()?);
            let prompt = self.prompt();
            reader.set_language(self.language);
            let suggestion_context = ShellSuggestionContext::new(
                &self.results,
                self.language,
                self.playback.is_some(),
                self.database.track_count()?,
            );
            match reader.readline(
                &prompt,
                |input| suggestion_context.suggestions(input),
                || self.drain_playback_lines(),
            )? {
                Some(line) => {
                    print_lines(self.drain_playback_lines()?);
                    let command = line.trim();
                    if command.is_empty() {
                        self.print_next_steps()?;
                        continue;
                    }
                    if reader.run_history_command(command)? {
                        continue;
                    }
                    if let Some(replayed) = reader.replay_history_command(command) {
                        println!("{command} -> {replayed}");
                        if is_history_worthy(&replayed) && reader.add_history(&replayed) {
                            reader.save_history_warning();
                        }
                        let keep_running = self.run_command(&replayed)?;
                        if keep_running && !is_repeat_command(&replayed) {
                            self.last_command = Some(replayed);
                        }
                        if !keep_running {
                            break;
                        }
                        continue;
                    }
                    if is_history_worthy(command) && reader.add_history(command) {
                        reader.save_history_warning();
                    }
                    let keep_running = self.run_command(command)?;
                    if keep_running && !is_repeat_command(command) {
                        self.last_command = Some(command.to_string());
                    }
                    if !keep_running {
                        break;
                    }
                }
                None => {
                    println!("{}", self.text("bye", "再见"));
                    break;
                }
            }
        }

        Ok(())
    }

    fn prompt(&self) -> String {
        prompt_for_playback(self.playback.is_some()).to_string()
    }

    fn text(&self, english: &'static str, chinese: &'static str) -> &'static str {
        match self.language {
            ShellLanguage::English => english,
            ShellLanguage::Chinese => chinese,
        }
    }

    fn print_welcome(&mut self) -> Result<()> {
        let track_count = self.database.track_count()?;
        print_welcome_card_lines(&welcome_card_lines(
            track_count,
            &device::default_device_name(),
            self.language,
            terminal_width_for_cards(),
        ));
        if self.results.is_empty() {
            println!(
                "{}",
                self.text(
                    "No tracks yet. Run scan to choose a folder.",
                    "还没有歌曲。输入 扫描 来选择音乐文件夹。"
                )
            );
        } else {
            self.print_results("library");
            println!(
                "{}",
                self.text(
                    "next: play 1, shuffle, next, prev, search <query>, or library",
                    "下一步: 播放 1、随机、下一首、上一首、搜索 <关键词>，或 曲库"
                )
            );
        }
        println!();
        Ok(())
    }

    fn run_command(&mut self, command: &str) -> Result<bool> {
        let command = command.trim_start_matches([':', '/']).trim();
        if let Some(index) = parse_result_index_input(command) {
            self.run_play(&index.to_string())?;
            return Ok(true);
        }

        let mut parts = command.splitn(2, char::is_whitespace);
        let name = parts.next().unwrap_or_default().to_ascii_lowercase();
        let argument = parts.next().unwrap_or_default().trim();

        match name.as_str() {
            "q" | "quit" | "exit" | "退出" => {
                self.stop_playback();
                return Ok(false);
            }
            "help" | "h" | "?" | "commands" | "帮助" | "命令" => self.print_help(argument),
            "again" | "repeat" | "!!" => return self.run_again(),
            "next" | "下一首" => self.run_relative_playback(1, "next")?,
            "prev" | "previous" | "上一首" => self.run_relative_playback(-1, "prev")?,
            "shuffle" | "random" | "surprise" | "随机" | "随便" => {
                self.run_shuffle_playback(name.as_str())?
            }
            "home" | "首页" => self.print_welcome()?,
            "tips" | "提示" | "下一步" => self.print_next_steps()?,
            "shortcuts" | "keys" | "快捷键" => self.print_shortcuts(),
            "aliases" | "alias" | "别名" => self.print_aliases(),
            "language" | "lang" | "语言" => self.run_language(argument)?,
            "pause" | "暂停" => self.pause_playback(),
            "resume" | "继续" => self.resume_playback(),
            "stop" | "停止" => self.stop_playback(),
            "scan" | "扫描" => self.run_scan(argument)?,
            "add" | "添加" => self.run_scan("add")?,
            "search" | "find" | "搜索" | "找" => self.run_search(argument)?,
            "library" | "list" | "recent" | "ls" | "songs" | "tracks" | "曲库" | "列表"
            | "歌曲" => self.run_library()?,
            "results" | "r" | "结果" => self.run_results(),
            "more" | "更多" => self.run_more_results()?,
            "play" | "播放" => self.run_play(argument)?,
            "now" | "current" | "playing" | "当前" | "正在播放" => self.run_now(),
            "info" | "i" | "信息" | "详情" => self.run_info(argument)?,
            "status" | "状态" => self.run_status()?,
            "devices" | "device" | "output" | "outputs" | "设备" | "输出" => self.run_devices(),
            "doctor" | "diagnose" | "diagnostics" | "health" | "check" | "诊断" | "检查" => {
                self.run_doctor()?
            }
            "errors" | "错误" => self.run_errors()?,
            "open" | "reveal" | "folder" | "where" | "打开" | "位置" => {
                self.run_open(argument)?
            }
            "copy" | "复制" => self.run_copy(argument)?,
            "open-db" => self.open_database_folder()?,
            "clear" | "cls" | "清屏" => {
                print!("\x1b[2J\x1b[H");
                self.print_welcome()?;
            }
            _ => {
                self.handle_unknown_input(command)?;
            }
        }

        Ok(true)
    }

    fn run_again(&mut self) -> Result<bool> {
        let Some(command) = self.last_command.clone() else {
            println!("nothing to repeat yet");
            println!("try: search <query>, play, or scan");
            return Ok(true);
        };

        println!("again {command}");
        self.run_command(&command)
    }

    fn print_help(&self, topic: &str) {
        print_lines(localized_help_lines(topic, self.language));
    }

    fn print_search_usage(&self) {
        print_lines(search_usage_lines(self.language));
    }

    fn print_no_results_yet(&self) {
        print_lines(no_results_yet_lines(self.language));
    }

    fn print_no_result_index(&self, index: usize) {
        print_lines(no_result_index_lines(
            index,
            self.results.len(),
            self.language,
        ));
    }

    fn print_next_steps(&self) -> Result<()> {
        println!("{}", self.text("next:", "下一步:"));
        if let Some(session) = &self.playback {
            match self.language {
                ShellLanguage::English => {
                    println!("  now playing       {}", compact(&session.title, 54));
                    println!("  pause             pause playback");
                    println!("  stop              stop playback");
                    println!("  next              play next visible result");
                    println!("  prev              play previous visible result");
                    println!("  shuffle           play a random visible result");
                    println!("  surprise          pick something for me");
                    println!("  now               show track details");
                    println!("  info              show track details");
                    println!("  results           print current results again");
                    println!("  open              show current track in Explorer");
                    println!("  copy              copy current track path");
                }
                ShellLanguage::Chinese => {
                    println!("  正在播放          {}", compact(&session.title, 54));
                    println!("  暂停              暂停播放");
                    println!("  继续              继续播放");
                    println!("  停止              停止播放");
                    println!("  下一首            播放当前结果里的下一首");
                    println!("  上一首            播放当前结果里的上一首");
                    println!("  随机              随机播放当前可见结果");
                    println!("  当前              查看正在播放的歌曲");
                    println!("  结果              重新显示当前列表");
                    println!("  打开              在 Explorer 里定位当前歌曲");
                    println!("  复制              复制当前歌曲路径");
                }
            }
            return Ok(());
        }

        if self.database.track_count()? == 0 {
            match self.language {
                ShellLanguage::English => {
                    println!("  scan              choose a music folder");
                    println!("  scan D:\\Music     scan a folder path");
                    println!("  devices           check output devices");
                }
                ShellLanguage::Chinese => {
                    println!("  扫描              选择音乐文件夹");
                    println!("  扫描 D:\\Music     直接扫描一个路径");
                    println!("  设备              检查输出设备");
                    println!("  帮助              查看所有命令");
                }
            }
            return Ok(());
        }

        if self.results.is_empty() {
            match self.language {
                ShellLanguage::English => {
                    println!("  library           show indexed tracks");
                    println!("  list              same as library");
                    println!("  search <query>    find a track");
                    println!("  more              show more current results");
                }
                ShellLanguage::Chinese => {
                    println!("  曲库              显示已入库歌曲");
                    println!("  搜索 <关键词>      找一首歌");
                    println!("  更多              显示更多当前结果");
                    println!("  扫描              再添加一个音乐文件夹");
                }
            }
            return Ok(());
        }

        match self.language {
            ShellLanguage::English => {
                println!("  play              play result #1");
                println!("  1                 play result #1");
                println!("  play <pick>       pick from current results");
                println!("  info <pick>       show track details");
                println!("  next              play next visible result");
                println!("  prev              play previous visible result");
                println!("  shuffle           play a random visible result");
                println!("  surprise          pick something for me");
                println!("  open <pick>       show a result in Explorer");
                println!("  copy <pick>       copy a result path");
                println!("  <keywords>        search without typing search");
                println!("  search <pick>     pick a result title to search");
                println!("  search <query>    narrow the list");
                println!("  results           print current results again");
                println!("  more              show more current results");
                println!("  again             repeat the last command");
                println!("  library           reset to recent tracks");
                println!("  list              same as library");
                println!("  home              show the welcome screen");
            }
            ShellLanguage::Chinese => {
                println!("  播放              播放第 1 个结果");
                println!("  1                 直接播放第 1 个结果");
                println!("  播放 <编号/歌名>   从当前结果里选择");
                println!("  信息 <编号/歌名>   查看歌曲详情");
                println!("  下一首            播放当前结果里的下一首");
                println!("  上一首            播放当前结果里的上一首");
                println!("  随机              随机播放当前可见结果");
                println!("  打开 <编号/歌名>   在 Explorer 里定位");
                println!("  复制 <编号/歌名>   复制歌曲路径");
                println!("  <关键词>          直接搜索，不用先打 搜索");
                println!("  搜索 <关键词>      缩小列表");
                println!("  结果              重新显示当前结果");
                println!("  更多              显示更多结果");
                println!("  again             重复上一条命令");
                println!("  曲库              回到最近入库歌曲");
                println!("  首页              回到欢迎页");
            }
        }
        Ok(())
    }

    fn print_shortcuts(&self) {
        print_lines(shortcut_lines(self.language));
    }

    fn print_aliases(&self) {
        print_lines(alias_lines(self.language));
    }

    fn run_language(&mut self, argument: &str) -> Result<()> {
        let argument = argument.trim();
        if is_language_status_argument(argument) {
            self.print_language_status();
            return Ok(());
        }

        let next = if argument.is_empty() {
            self.language.toggled()
        } else if let Some(language) = parse_language(argument) {
            language
        } else {
            println!(
                "{}",
                self.text("usage: language [en|zh]", "用法: 语言 [en|zh]")
            );
            println!(
                "{}",
                self.text("try: language zh", "例如: 语言 zh 或 language en")
            );
            return Ok(());
        };

        self.language = next;
        save_language(&self.paths, self.language)?;
        println!(
            "{} {}",
            self.text("language:", "语言："),
            self.language.label()
        );
        println!();
        self.print_welcome()
    }

    fn print_language_status(&self) {
        match self.language {
            ShellLanguage::English => {
                println!("language: {}", self.language.label());
                println!("available: en English, zh 中文");
                println!("switch: language zh, language en, or /language");
            }
            ShellLanguage::Chinese => {
                println!("语言：{}", self.language.label());
                println!("可用：en English，zh 中文");
                println!("切换：语言 zh、language en，或 /language");
            }
        }
    }

    fn handle_unknown_input(&mut self, input: &str) -> Result<()> {
        let Some(first_token) = input.split_whitespace().next() else {
            return Ok(());
        };
        let suggestions = nearest_command_suggestions(first_token);
        if suggestions.is_empty() {
            println!("{}", searching_line(input, self.language));
            self.run_search(input)?;
            return Ok(());
        }

        print_lines(unknown_command_lines(
            first_token,
            input,
            suggestions,
            self.language,
        ));
        Ok(())
    }

    fn run_scan(&mut self, argument: &str) -> Result<()> {
        let folder = if argument.is_empty() || argument.eq_ignore_ascii_case("add") {
            match pick_scan_folder()? {
                Some(folder) => folder,
                None => {
                    println!("{}", scan_canceled_line(self.language));
                    return Ok(());
                }
            }
        } else {
            PathBuf::from(argument)
        };

        println!("{}", scan_started_line(&folder, self.language));
        let summary = scanner::scan_folder(&mut self.database, &folder)?;
        println!("{}", scan_summary_line(&summary, self.language));
        if let Some(hint) = scan_failure_hint(summary.failed_files, self.language) {
            println!("{hint}");
        }
        self.refresh_results()?;
        self.print_results(&self.result_label);
        if self.results.is_empty() {
            println!("{}", scan_empty_next_steps(self.language));
        } else {
            println!("{}", scan_next_steps(self.language));
        }
        Ok(())
    }

    fn run_search(&mut self, query: &str) -> Result<()> {
        if query.is_empty() {
            self.print_search_usage();
            return Ok(());
        }

        self.load_results(query, query, RESULT_LIMIT)?;
        self.print_search_results(query);
        self.print_result_next_steps();
        Ok(())
    }

    fn run_library(&mut self) -> Result<()> {
        self.refresh_results()?;
        self.print_results(&self.result_label);
        self.print_result_next_steps();
        Ok(())
    }

    fn run_results(&self) {
        self.print_results(&self.result_label);
        self.print_result_next_steps();
    }

    fn run_more_results(&mut self) -> Result<()> {
        let old_count = self.results.len();
        let next_limit = self.result_limit.saturating_add(RESULT_PAGE_SIZE);
        let query = self.result_query.clone();
        let label = self.result_label.clone();
        self.load_results(&query, &label, next_limit)?;
        self.print_results(&self.result_label);
        if self.results.len() == old_count && !self.has_more_results {
            println!("no more matches in this view");
        }
        self.print_result_next_steps();
        Ok(())
    }

    fn run_play(&mut self, argument: &str) -> Result<()> {
        if argument.is_empty() {
            let Some(track) = self.results.first().cloned() else {
                self.print_no_results_yet();
                return Ok(());
            };
            return self.replace_playback(track);
        }

        let track = if argument.eq_ignore_ascii_case("random")
            || argument.eq_ignore_ascii_case("shuffle")
            || argument.eq_ignore_ascii_case("surprise")
            || matches!(argument, "随机" | "随便" | "惊喜")
        {
            return self.run_shuffle_playback(argument);
        } else if argument.eq_ignore_ascii_case("next") || argument == "下一首" {
            return self.run_relative_playback(1, "play next");
        } else if argument.eq_ignore_ascii_case("prev")
            || argument.eq_ignore_ascii_case("previous")
            || argument == "上一首"
        {
            return self.run_relative_playback(-1, "play prev");
        } else if argument.eq_ignore_ascii_case("last")
            || argument == "最后"
            || argument == "最后一首"
        {
            let Some(track) = self.results.last().cloned() else {
                self.print_no_results_yet();
                return Ok(());
            };
            track
        } else if argument.eq_ignore_ascii_case("best")
            || argument.eq_ignore_ascii_case("first")
            || matches!(argument, "第一首" | "第一" | "第一个")
        {
            let Some(track) = self.results.first().cloned() else {
                self.print_no_results_yet();
                return Ok(());
            };
            track
        } else if let Some(index) = parse_result_index_input(argument) {
            if index == 0 || index > self.results.len() {
                self.print_no_result_index(index);
                return Ok(());
            }
            self.results[index - 1].clone()
        } else {
            self.resolve_play_target(argument)?
        };

        self.replace_playback(track)
    }

    fn run_shuffle_playback(&mut self, label: &str) -> Result<()> {
        let Some(index) =
            shuffle_result_index(&self.results, self.current_track.as_ref(), random_seed())
        else {
            self.print_no_results_yet();
            return Ok(());
        };

        let track = self.results[index].clone();
        println!("{label} {}. {}", index + 1, compact(&track.title, 56));
        self.replace_playback(track)
    }

    fn run_relative_playback(&mut self, step: isize, label: &str) -> Result<()> {
        let Some(index) = relative_result_index(&self.results, self.current_track.as_ref(), step)
        else {
            self.print_no_results_yet();
            return Ok(());
        };

        let track = self.results[index].clone();
        println!("{label} {}. {}", index + 1, compact(&track.title, 56));
        self.replace_playback(track)
    }

    fn start_playback(&mut self, track: Track) -> Result<()> {
        print_lines(self.drain_playback_lines()?);
        if let Some(session) = &self.playback {
            print_lines(already_playing_lines(&session.title, self.language));
            return Ok(());
        }

        let (control_tx, control_rx) = mpsc::channel();
        let (event_tx, event_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let playback_track = track.clone();
        let title = track.title.clone();
        thread::Builder::new()
            .name("echo-cli-playback".to_string())
            .spawn(move || {
                let mut reported_error = false;
                let event_tx_for_callback = event_tx.clone();
                let result =
                    PlaybackEngine::new().play_controlled(&playback_track, control_rx, |event| {
                        if matches!(event, PlaybackEvent::Error { .. }) {
                            reported_error = true;
                        }
                        let _ = event_tx_for_callback.send(event);
                    });
                if let Err(error) = result
                    && !reported_error
                {
                    let _ = event_tx.send(PlaybackEvent::Error {
                        path: playback_track.path.clone(),
                        message: error.to_string(),
                    });
                }
                let _ = done_tx.send(());
            })
            .map_err(|error| EchoError::Playback(error.to_string()))?;

        self.current_track = Some(track);
        self.playback = Some(PlaybackSession {
            title: title.clone(),
            control_tx,
            event_rx,
            done_rx,
        });
        print_lines(started_playback_lines(&title, self.language));
        Ok(())
    }

    fn replace_playback(&mut self, track: Track) -> Result<()> {
        self.stop_playback_for_switch()?;
        self.start_playback(track)
    }

    fn stop_playback_for_switch(&mut self) -> Result<()> {
        print_lines(self.drain_playback_lines()?);
        let Some(session) = self.playback.take() else {
            return Ok(());
        };

        let _ = session.control_tx.send(PlaybackControl::Stop);
        match session.done_rx.recv_timeout(Duration::from_millis(1200)) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => {}
            Err(RecvTimeoutError::Timeout) => {
                println!("{}", stopping_timeout_line(self.language));
            }
        }
        Ok(())
    }

    fn pause_playback(&mut self) {
        if let Some(session) = &self.playback {
            let _ = session.control_tx.send(PlaybackControl::Pause);
        } else {
            println!("{}", nothing_playing_line(self.language));
        }
    }

    fn resume_playback(&mut self) {
        if let Some(session) = &self.playback {
            let _ = session.control_tx.send(PlaybackControl::Resume);
        } else {
            println!("{}", nothing_paused_line(self.language));
        }
    }

    fn stop_playback(&mut self) {
        if let Some(session) = self.playback.take() {
            let _ = session.control_tx.send(PlaybackControl::Stop);
            println!("{}", stopping_line(&session.title, self.language));
        }
    }

    fn drain_playback_lines(&mut self) -> Result<Vec<String>> {
        let mut lines = Vec::new();
        if let Some(session) = &self.playback {
            while let Ok(event) = session.event_rx.try_recv() {
                lines.extend(playback_event_lines(&event, self.language));
            }
        }

        let should_clear = match self.playback.as_ref() {
            Some(session) => match session.done_rx.try_recv() {
                Ok(()) | Err(TryRecvError::Disconnected) => true,
                Err(TryRecvError::Empty) => false,
            },
            None => false,
        };

        if should_clear {
            self.playback = None;
        }

        Ok(lines)
    }

    fn resolve_play_target(&mut self, query_or_path: &str) -> Result<Track> {
        let as_path = PathBuf::from(query_or_path);
        if as_path.exists() {
            let metadata = std::fs::metadata(&as_path)?;
            return metadata::read_track(&as_path, &metadata)
                .or_else(|_| metadata::fallback_track(&as_path, &metadata));
        }

        if let Some(track) = self.database.find_exact_path(&as_path)? {
            return Ok(track);
        }

        let results = search::search(&self.database, query_or_path, 1)?;
        results
            .into_iter()
            .next()
            .map(|result| result.track)
            .ok_or_else(|| EchoError::Playback(format!("no playable match for: {query_or_path}")))
    }

    fn run_devices(&self) {
        for audio_device in device::list_devices() {
            println!(
                "{} [{}]{}",
                audio_device.name,
                audio_device.id,
                if audio_device.is_default {
                    " default"
                } else {
                    ""
                }
            );
        }
    }

    fn run_now(&self) {
        let Some(track) = &self.current_track else {
            println!("now: idle");
            return;
        };

        print_track_info("now", track);
    }

    fn run_info(&mut self, argument: &str) -> Result<()> {
        let track = self.resolve_current_or_result_target(argument)?;

        let Some(track) = track else {
            print_lines(nothing_to_inspect_lines(self.language));
            return Ok(());
        };

        print_track_info("info", &track);
        Ok(())
    }

    fn run_open(&mut self, argument: &str) -> Result<()> {
        let track = self.resolve_current_or_result_target(argument)?;

        let Some(track) = track else {
            print_lines(nothing_to_open_lines(self.language));
            return Ok(());
        };

        open_track_in_explorer(&track)?;
        println!("opened {}", compact(&track.title, 56));
        Ok(())
    }

    fn run_copy(&mut self, argument: &str) -> Result<()> {
        let track = self.resolve_current_or_result_target(argument)?;

        let Some(track) = track else {
            print_lines(nothing_to_copy_lines(self.language));
            return Ok(());
        };

        let path = explorer_select_path(&track.path);
        copy_text_to_clipboard(&path)?;
        println!("copied path for {}", compact(&track.title, 56));
        Ok(())
    }

    fn resolve_current_or_result_target(&mut self, argument: &str) -> Result<Option<Track>> {
        if argument.is_empty()
            || argument.eq_ignore_ascii_case("current")
            || argument.eq_ignore_ascii_case("now")
        {
            return Ok(self
                .current_track
                .clone()
                .or_else(|| self.results.first().cloned()));
        }

        if let Ok(index) = argument.parse::<usize>() {
            return Ok(self.results.get(index.saturating_sub(1)).cloned());
        }

        self.resolve_play_target(argument).map(Some)
    }

    fn run_status(&self) -> Result<()> {
        let default_device = device::default_device_name();
        let database_path = self.paths.database_path().display().to_string();
        print_lines(status_lines(StatusSnapshot {
            track_count: self.database.track_count()?,
            result_count: self.results.len(),
            result_label: &self.result_label,
            result_query: &self.result_query,
            has_more_results: self.has_more_results,
            default_device: &default_device,
            playback_title: self.playback.as_ref().map(|session| session.title.as_str()),
            current_title: self
                .current_track
                .as_ref()
                .map(|track| track.title.as_str()),
            current_result: current_result_label(&self.results, self.current_track.as_ref()),
            database_path: &database_path,
            language: self.language,
        }));
        Ok(())
    }

    fn run_doctor(&self) -> Result<()> {
        println!(
            "os                  {} {}",
            std::env::consts::OS,
            std::env::consts::ARCH
        );
        println!(
            "audio backend       {}",
            audio_backend::backend_status_line()
        );
        println!(
            "wasapi exclusive    {}",
            audio_backend_wasapi::exclusive_status_line()
        );
        println!("default device      {}", device::default_device_name());
        println!(
            "database            {}",
            self.paths.database_path().display()
        );
        println!("tracks              {}", self.database.track_count()?);
        println!("formats             {}", SUPPORTED_EXTENSIONS.join(", "));
        Ok(())
    }

    fn run_errors(&self) -> Result<()> {
        let errors = self.database.recent_scan_errors(10)?;
        if errors.is_empty() {
            println!("no recent scan errors");
        } else {
            for (path, error) in errors {
                println!("{} :: {}", compact_path(&path, 64), compact(&error, 72));
            }
        }
        Ok(())
    }

    fn open_database_folder(&self) -> Result<()> {
        let Some(folder) = self.paths.database_path().parent() else {
            println!("database folder unavailable");
            return Ok(());
        };

        Command::new("explorer").arg(folder).spawn()?;
        println!("opened {}", folder.display());
        Ok(())
    }

    fn refresh_results(&mut self) -> Result<()> {
        self.database = Database::open(self.paths.database_path())?;
        self.load_results("", "library", RESULT_LIMIT)
    }

    fn load_results(&mut self, query: &str, label: &str, limit: usize) -> Result<()> {
        let (results, has_more_results) = load_result_window(&self.database, query, limit)?;
        self.results = results;
        self.result_query = query.to_string();
        self.result_label = label.to_string();
        self.result_limit = limit;
        self.has_more_results = has_more_results;
        Ok(())
    }

    fn print_results(&self, label: &str) {
        if self.results.is_empty() {
            println!("{label}: no tracks");
            return;
        }

        let terminal_width = terminal::size().map(|(width, _)| width).unwrap_or(120);
        println!(
            "{}",
            result_header(label, self.results.len())
                .as_str()
                .with(Color::Cyan)
                .bold()
        );
        println!(
            "{}",
            result_table_header_for_width(terminal_width)
                .as_str()
                .with(Color::DarkGrey)
        );
        for (index, track) in self.results.iter().enumerate() {
            println!(
                "{}",
                result_line_for_width(
                    index + 1,
                    track,
                    self.current_track.as_ref(),
                    terminal_width
                )
            );
        }
        if self.has_more_results {
            println!(
                "{}",
                format!(
                    "showing first {}; type more to expand, or a narrower keyword to filter",
                    self.results.len()
                )
                .as_str()
                .with(Color::DarkGrey)
            );
        }
    }

    fn print_search_results(&self, query: &str) {
        if self.results.is_empty() {
            print_lines(search_no_matches_lines(query, self.language));
            return;
        }

        self.print_results(query);
    }

    fn print_result_next_steps(&self) {
        println!("{}", result_next_steps(self.results.len(), self.language));
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ShellSuggestion {
    completion: String,
    description: String,
}

impl From<CommandSuggestion> for ShellSuggestion {
    fn from(suggestion: CommandSuggestion) -> Self {
        suggestion.to_shell_suggestion(ShellLanguage::English)
    }
}

fn shell_suggestion(
    completion: impl Into<String>,
    description: impl Into<String>,
) -> ShellSuggestion {
    ShellSuggestion {
        completion: completion.into(),
        description: description.into(),
    }
}

#[derive(Debug, Clone)]
struct ShellSuggestionContext {
    results: Vec<ResultSuggestion>,
    language: ShellLanguage,
    playback_active: bool,
    track_count: u64,
}

#[derive(Debug, Clone)]
struct ResultSuggestion {
    index: usize,
    title: String,
    artist: String,
}

impl ShellSuggestionContext {
    #[cfg(test)]
    fn from_tracks(tracks: &[Track]) -> Self {
        Self::from_tracks_with_language(tracks, ShellLanguage::English)
    }

    #[cfg(test)]
    fn from_tracks_with_language(tracks: &[Track], language: ShellLanguage) -> Self {
        Self::new(tracks, language, false, tracks.len() as u64)
    }

    fn new(
        tracks: &[Track],
        language: ShellLanguage,
        playback_active: bool,
        track_count: u64,
    ) -> Self {
        let results = tracks
            .iter()
            .enumerate()
            .map(|(index, track)| ResultSuggestion {
                index: index + 1,
                title: track.title.clone(),
                artist: track
                    .artist
                    .clone()
                    .unwrap_or_else(|| "unknown artist".to_string()),
            })
            .collect();
        Self {
            results,
            language,
            playback_active,
            track_count,
        }
    }

    fn suggestions(&self, input: &str) -> Vec<ShellSuggestion> {
        let normalized = normalized_suggestion_input(input);
        if normalized.is_empty() {
            return self.idle_suggestions();
        }

        let (command_input, completion_prefix) = slash_command_view(&normalized);
        if is_result_index_prefix(&normalized) {
            let index_suggestions = self.result_index_suggestions(&normalized);
            if !index_suggestions.is_empty() {
                return index_suggestions;
            }
        }

        if command_input.starts_with("play ") {
            let result_suggestions =
                self.result_command_suggestions(command_input, "play", completion_prefix);
            if !result_suggestions.is_empty() {
                return result_suggestions;
            }
        }
        if command_input.starts_with("播放 ") {
            let result_suggestions =
                self.result_command_suggestions(command_input, "播放", completion_prefix);
            if !result_suggestions.is_empty() {
                return result_suggestions;
            }
        }
        if command_input.starts_with("open ") {
            let result_suggestions =
                self.result_command_suggestions(command_input, "open", completion_prefix);
            if !result_suggestions.is_empty() {
                return result_suggestions;
            }
        }
        if command_input.starts_with("打开 ") {
            let result_suggestions =
                self.result_command_suggestions(command_input, "打开", completion_prefix);
            if !result_suggestions.is_empty() {
                return result_suggestions;
            }
        }
        if command_input.starts_with("copy ") {
            let result_suggestions =
                self.result_command_suggestions(command_input, "copy", completion_prefix);
            if !result_suggestions.is_empty() {
                return result_suggestions;
            }
        }
        if command_input.starts_with("复制 ") {
            let result_suggestions =
                self.result_command_suggestions(command_input, "复制", completion_prefix);
            if !result_suggestions.is_empty() {
                return result_suggestions;
            }
        }
        if command_input.starts_with("info ") {
            let result_suggestions =
                self.result_command_suggestions(command_input, "info", completion_prefix);
            if !result_suggestions.is_empty() {
                return result_suggestions;
            }
        }
        if command_input.starts_with("信息 ") {
            let result_suggestions =
                self.result_command_suggestions(command_input, "信息", completion_prefix);
            if !result_suggestions.is_empty() {
                return result_suggestions;
            }
        }
        if command_input.starts_with("详情 ") {
            let result_suggestions =
                self.result_command_suggestions(command_input, "详情", completion_prefix);
            if !result_suggestions.is_empty() {
                return result_suggestions;
            }
        }
        if command_input.starts_with("search ") {
            let result_suggestions =
                self.search_result_suggestions(command_input, completion_prefix);
            if !result_suggestions.is_empty() {
                return result_suggestions;
            }
        }
        if command_input.starts_with("搜索 ") {
            let result_suggestions = self.search_result_suggestions_for_command(
                command_input,
                completion_prefix,
                "搜索",
            );
            if !result_suggestions.is_empty() {
                return result_suggestions;
            }
        }
        if command_input.starts_with("找 ") {
            let result_suggestions =
                self.search_result_suggestions_for_command(command_input, completion_prefix, "找");
            if !result_suggestions.is_empty() {
                return result_suggestions;
            }
        }
        if command_input.starts_with("help ") || command_input.starts_with("帮助 ") {
            let help_suggestions = help_topic_suggestions(&normalized);
            if !help_suggestions.is_empty() {
                return help_suggestions;
            }
        }
        if !normalized.is_empty() && command_suggestions(input).is_empty() {
            let bare_suggestions = self.bare_search_suggestions(&normalized);
            if !bare_suggestions.is_empty() {
                return bare_suggestions;
            }
        }

        command_suggestions(input)
            .into_iter()
            .map(|suggestion| suggestion.to_shell_suggestion(self.language))
            .collect()
    }

    fn idle_suggestions(&self) -> Vec<ShellSuggestion> {
        match self.language {
            ShellLanguage::Chinese => self.chinese_idle_suggestions(),
            ShellLanguage::English => self.english_idle_suggestions(),
        }
    }

    fn english_idle_suggestions(&self) -> Vec<ShellSuggestion> {
        if self.playback_active {
            return vec![
                shell_suggestion("pause", "pause playback"),
                shell_suggestion("stop", "stop playback"),
                shell_suggestion("next", "play next visible result"),
                shell_suggestion("prev", "play previous visible result"),
                shell_suggestion("now", "show current track"),
                shell_suggestion("search ", "search indexed tracks"),
                shell_suggestion("tips", "show suggested next steps"),
                shell_suggestion("help", "show commands"),
            ];
        }

        if self.track_count == 0 {
            return vec![
                shell_suggestion("scan", "choose a music folder"),
                shell_suggestion("scan add", "choose a music folder"),
                shell_suggestion("scan D:\\Music", "scan a folder path"),
                shell_suggestion("devices", "check output devices"),
                shell_suggestion("language", "switch English / 中文"),
                shell_suggestion("help", "show commands"),
            ];
        }

        if self.results.is_empty() {
            return vec![
                shell_suggestion("library", "show indexed tracks"),
                shell_suggestion("search ", "search indexed tracks"),
                shell_suggestion("scan", "add another folder"),
                shell_suggestion("devices", "check output devices"),
                shell_suggestion("tips", "show suggested next steps"),
                shell_suggestion("help", "show commands"),
            ];
        }

        vec![
            shell_suggestion("play", "play result #1"),
            shell_suggestion("1", "play result #1"),
            shell_suggestion("shuffle", "play a random visible result"),
            shell_suggestion("next", "play next visible result"),
            shell_suggestion("info 1", "show result #1 details"),
            shell_suggestion("open 1", "show result #1 in Explorer"),
            shell_suggestion("search ", "search indexed tracks"),
            shell_suggestion("more", "show more current results"),
            shell_suggestion("tips", "show suggested next steps"),
        ]
    }

    fn chinese_idle_suggestions(&self) -> Vec<ShellSuggestion> {
        if self.playback_active {
            return vec![
                shell_suggestion("暂停", "暂停播放"),
                shell_suggestion("停止", "停止播放"),
                shell_suggestion("下一首", "播放当前结果里的下一首"),
                shell_suggestion("上一首", "播放当前结果里的上一首"),
                shell_suggestion("当前", "查看正在播放的歌曲"),
                shell_suggestion("搜索 ", "搜索已入库歌曲"),
                shell_suggestion("下一步", "显示建议动作"),
                shell_suggestion("帮助", "显示命令"),
            ];
        }

        if self.track_count == 0 {
            return vec![
                shell_suggestion("扫描", "选择音乐文件夹"),
                shell_suggestion("扫描 D:\\Music", "直接扫描一个路径"),
                shell_suggestion("设备", "检查输出设备"),
                shell_suggestion("语言", "切换 English / 中文"),
                shell_suggestion("帮助", "显示命令"),
            ];
        }

        if self.results.is_empty() {
            return vec![
                shell_suggestion("曲库", "显示已入库歌曲"),
                shell_suggestion("搜索 ", "搜索已入库歌曲"),
                shell_suggestion("扫描", "再添加一个音乐文件夹"),
                shell_suggestion("设备", "检查输出设备"),
                shell_suggestion("下一步", "显示建议动作"),
                shell_suggestion("帮助", "显示命令"),
            ];
        }

        vec![
            shell_suggestion("播放", "播放第 1 个结果"),
            shell_suggestion("1", "直接播放第 1 个结果"),
            shell_suggestion("随机", "随机播放当前可见结果"),
            shell_suggestion("下一首", "播放当前结果里的下一首"),
            shell_suggestion("信息 1", "查看第 1 个结果"),
            shell_suggestion("打开 1", "在 Explorer 里定位"),
            shell_suggestion("搜索 ", "搜索已入库歌曲"),
            shell_suggestion("更多", "显示更多当前结果"),
            shell_suggestion("下一步", "显示建议动作"),
        ]
    }

    fn result_command_suggestions(
        &self,
        normalized_input: &str,
        command: &str,
        completion_prefix: &str,
    ) -> Vec<ShellSuggestion> {
        let prefix = format!("{command} ");
        let query = normalized_input
            .strip_prefix(&prefix)
            .unwrap_or_default()
            .trim();
        self.results
            .iter()
            .filter(|result| result.matches(query))
            .take(12)
            .flat_map(|result| {
                [
                    ShellSuggestion {
                        completion: format!("{completion_prefix}{command} {}", result.index),
                        description: format!(
                            "{} - {}",
                            compact(&result.title, 42),
                            compact(&result.artist, 24)
                        ),
                    },
                    ShellSuggestion {
                        completion: format!("{completion_prefix}{command} {}", result.title),
                        description: self.result_title_completion_description(result),
                    },
                ]
            })
            .take(12)
            .collect()
    }

    fn search_result_suggestions(
        &self,
        normalized_input: &str,
        completion_prefix: &str,
    ) -> Vec<ShellSuggestion> {
        self.search_result_suggestions_for_command(normalized_input, completion_prefix, "search")
    }

    fn search_result_suggestions_for_command(
        &self,
        normalized_input: &str,
        completion_prefix: &str,
        command: &str,
    ) -> Vec<ShellSuggestion> {
        let prefix = format!("{command} ");
        let query = normalized_input
            .strip_prefix(&prefix)
            .unwrap_or_default()
            .trim();
        self.results
            .iter()
            .filter(|result| result.matches(query))
            .take(12)
            .map(|result| ShellSuggestion {
                completion: format!("{completion_prefix}{command} {}", result.title),
                description: self.result_title_completion_description(result),
            })
            .collect()
    }

    fn bare_search_suggestions(&self, normalized_input: &str) -> Vec<ShellSuggestion> {
        self.results
            .iter()
            .filter(|result| result.matches(normalized_input))
            .take(8)
            .map(|result| ShellSuggestion {
                completion: result.title.clone(),
                description: match self.language {
                    ShellLanguage::English => format!(
                        "search title | {} | result #{}",
                        compact(&result.artist, 22),
                        result.index
                    ),
                    ShellLanguage::Chinese => format!(
                        "搜索标题 | {} | 结果 #{}",
                        compact(&result.artist, 22),
                        result.index
                    ),
                },
            })
            .collect()
    }

    fn result_index_suggestions(&self, normalized_input: &str) -> Vec<ShellSuggestion> {
        let query = normalized_input.trim_start_matches('#');
        self.results
            .iter()
            .filter(|result| result.index.to_string().starts_with(query))
            .take(8)
            .map(|result| ShellSuggestion {
                completion: if normalized_input.starts_with('#') {
                    format!("#{}", result.index)
                } else {
                    result.index.to_string()
                },
                description: match self.language {
                    ShellLanguage::English => format!(
                        "play {} - {}",
                        compact(&result.title, 42),
                        compact(&result.artist, 24)
                    ),
                    ShellLanguage::Chinese => format!(
                        "播放 {} - {}",
                        compact(&result.title, 42),
                        compact(&result.artist, 24)
                    ),
                },
            })
            .collect()
    }

    fn result_title_completion_description(&self, result: &ResultSuggestion) -> String {
        match self.language {
            ShellLanguage::English => {
                format!("{} | result #{}", compact(&result.artist, 28), result.index)
            }
            ShellLanguage::Chinese => {
                format!("{} | 结果 #{}", compact(&result.artist, 28), result.index)
            }
        }
    }
}

impl ResultSuggestion {
    fn matches(&self, query: &str) -> bool {
        if query.is_empty() {
            return true;
        }

        let index = self.index.to_string();
        if index.starts_with(query) {
            return true;
        }

        let query = query.to_ascii_lowercase();
        self.title.to_ascii_lowercase().contains(&query)
            || self.artist.to_ascii_lowercase().contains(&query)
    }
}

struct ShellReader {
    history: Vec<String>,
    history_path: Option<PathBuf>,
    history_cursor: Option<usize>,
    suggestion_index: usize,
    last_char: Option<(char, KeyModifiers, Instant)>,
    language: ShellLanguage,
}

impl ShellReader {
    fn new() -> Self {
        Self {
            history: Vec::new(),
            history_path: None,
            history_cursor: None,
            suggestion_index: 0,
            last_char: None,
            language: ShellLanguage::English,
        }
    }

    fn load(history_path: PathBuf) -> Result<Self> {
        let mut reader = Self::new();
        reader.history = read_history_entries(&history_path)?;
        reader.history_path = Some(history_path);
        Ok(reader)
    }

    fn set_language(&mut self, language: ShellLanguage) {
        self.language = language;
    }

    fn add_history(&mut self, command: &str) -> bool {
        push_history_entry(&mut self.history, command)
    }

    fn save_history_warning(&self) {
        if let Err(error) = self.save_history() {
            println!("history warning {error}");
        }
    }

    fn save_history(&self) -> Result<()> {
        let Some(path) = &self.history_path else {
            return Ok(());
        };
        std::fs::write(path, serialize_history_entries(&self.history))?;
        Ok(())
    }

    fn run_history_command(&mut self, command: &str) -> Result<bool> {
        let command = command.trim_start_matches([':', '/']).trim();
        let mut parts = command.split_whitespace();
        let Some(name) = parts.next() else {
            return Ok(false);
        };
        if !name.eq_ignore_ascii_case("history") {
            return Ok(false);
        }

        let argument = parts.next().unwrap_or_default();
        if argument.eq_ignore_ascii_case("clear") {
            self.history.clear();
            self.save_history_warning();
            println!("history cleared");
            return Ok(true);
        }

        let count = argument
            .parse::<usize>()
            .ok()
            .filter(|count| *count > 0)
            .unwrap_or(20);
        print_history_entries(&self.history, count);
        Ok(true)
    }

    fn replay_history_command(&self, command: &str) -> Option<String> {
        history_replay_index(command)
            .and_then(|index| self.history.get(index.saturating_sub(1)))
            .cloned()
    }

    fn readline<S, O>(
        &mut self,
        prompt: &str,
        mut suggestions: S,
        mut on_output: O,
    ) -> Result<Option<String>>
    where
        S: FnMut(&str) -> Vec<ShellSuggestion>,
        O: FnMut() -> Result<Vec<String>>,
    {
        let mut raw_mode = RawModeGuard::enable()?;
        self.history_cursor = None;
        self.suggestion_index = 0;
        self.last_char = None;

        let mut input = String::new();
        let mut cursor_index = 0_usize;
        self.render(prompt, &input, cursor_index, &mut suggestions)?;

        loop {
            if !event::poll(Duration::from_millis(250)).map_err(terminal_error)? {
                self.flush_external_output(
                    prompt,
                    &input,
                    cursor_index,
                    &mut raw_mode,
                    &mut suggestions,
                    &mut on_output,
                )?;
                continue;
            }

            let Event::Key(key) = event::read().map_err(terminal_error)? else {
                continue;
            };
            if key.kind != KeyEventKind::Press {
                continue;
            }

            match self.handle_key(prompt, &mut input, &mut cursor_index, key, &mut suggestions)? {
                ReadAction::Continue => {}
                ReadAction::Submit => {
                    self.finish_line(prompt, &input)?;
                    raw_mode.disable()?;
                    return Ok(Some(input));
                }
                ReadAction::Cancel => {
                    self.finish_interrupt(prompt, &input)?;
                    raw_mode.disable()?;
                    return Ok(None);
                }
            }
        }
    }

    fn handle_key(
        &mut self,
        prompt: &str,
        input: &mut String,
        cursor_index: &mut usize,
        key: KeyEvent,
        suggestions: &mut impl FnMut(&str) -> Vec<ShellSuggestion>,
    ) -> Result<ReadAction> {
        match key.code {
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                Ok(ReadAction::Cancel)
            }
            KeyCode::Char('a') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                *cursor_index = 0;
                self.render(prompt, input, *cursor_index, suggestions)?;
                Ok(ReadAction::Continue)
            }
            KeyCode::Char('e') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                *cursor_index = input.len();
                self.render(prompt, input, *cursor_index, suggestions)?;
                Ok(ReadAction::Continue)
            }
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                input.drain(..*cursor_index);
                *cursor_index = 0;
                self.history_cursor = None;
                self.suggestion_index = 0;
                self.render(prompt, input, *cursor_index, suggestions)?;
                Ok(ReadAction::Continue)
            }
            KeyCode::Char('k') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                if remove_after_cursor(input, cursor_index) {
                    self.history_cursor = None;
                    self.suggestion_index = 0;
                    self.render(prompt, input, *cursor_index, suggestions)?;
                }
                Ok(ReadAction::Continue)
            }
            KeyCode::Char('l') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                let mut stdout = io::stdout();
                queue!(stdout, Clear(ClearType::All), cursor::MoveTo(0, 0))
                    .map_err(terminal_error)?;
                stdout.flush().map_err(terminal_error)?;
                self.render(prompt, input, *cursor_index, suggestions)?;
                Ok(ReadAction::Continue)
            }
            KeyCode::Char('w') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                if remove_word_before_cursor(input, cursor_index) {
                    self.history_cursor = None;
                    self.suggestion_index = 0;
                    self.render(prompt, input, *cursor_index, suggestions)?;
                }
                Ok(ReadAction::Continue)
            }
            KeyCode::Char('d')
                if key.modifiers.contains(KeyModifiers::CONTROL) && input.is_empty() =>
            {
                Ok(ReadAction::Cancel)
            }
            KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                if remove_char_at_cursor(input, cursor_index) {
                    self.history_cursor = None;
                    self.suggestion_index = 0;
                    self.render(prompt, input, *cursor_index, suggestions)?;
                }
                Ok(ReadAction::Continue)
            }
            KeyCode::Enter => {
                if self.has_visible_suggestions(input, suggestions)
                    && let Some(suggestion) = self.selected_suggestion(input, suggestions)
                    && should_accept_suggestion_on_enter(input, &suggestion)
                {
                    let needs_more_input = accepted_suggestion_needs_more_input(&suggestion);
                    *input = suggestion.completion;
                    *cursor_index = input.len();
                    self.history_cursor = None;
                    self.suggestion_index = 0;
                    if needs_more_input {
                        self.render(prompt, input, *cursor_index, suggestions)?;
                        return Ok(ReadAction::Continue);
                    }
                }
                Ok(ReadAction::Submit)
            }
            KeyCode::Esc => {
                input.clear();
                *cursor_index = 0;
                self.history_cursor = None;
                self.suggestion_index = 0;
                self.render(prompt, input, *cursor_index, suggestions)?;
                Ok(ReadAction::Continue)
            }
            KeyCode::Backspace => {
                let changed = if key.modifiers.contains(KeyModifiers::CONTROL) {
                    remove_word_before_cursor(input, cursor_index)
                } else {
                    remove_char_before_cursor(input, cursor_index)
                };
                if changed {
                    self.history_cursor = None;
                    self.suggestion_index = 0;
                    self.render(prompt, input, *cursor_index, suggestions)?;
                }
                Ok(ReadAction::Continue)
            }
            KeyCode::Delete => {
                let changed = if key.modifiers.contains(KeyModifiers::CONTROL) {
                    remove_after_cursor(input, cursor_index)
                } else {
                    remove_char_at_cursor(input, cursor_index)
                };
                if changed {
                    self.history_cursor = None;
                    self.suggestion_index = 0;
                    self.render(prompt, input, *cursor_index, suggestions)?;
                }
                Ok(ReadAction::Continue)
            }
            KeyCode::Left => {
                *cursor_index = if key.modifiers.contains(KeyModifiers::CONTROL) {
                    previous_word_boundary(input, *cursor_index)
                } else {
                    previous_char_boundary(input, *cursor_index)
                };
                self.render(prompt, input, *cursor_index, suggestions)?;
                Ok(ReadAction::Continue)
            }
            KeyCode::Right => {
                *cursor_index = if key.modifiers.contains(KeyModifiers::CONTROL) {
                    next_word_boundary(input, *cursor_index)
                } else {
                    next_char_boundary(input, *cursor_index)
                };
                self.render(prompt, input, *cursor_index, suggestions)?;
                Ok(ReadAction::Continue)
            }
            KeyCode::Home => {
                *cursor_index = 0;
                self.render(prompt, input, *cursor_index, suggestions)?;
                Ok(ReadAction::Continue)
            }
            KeyCode::End => {
                *cursor_index = input.len();
                self.render(prompt, input, *cursor_index, suggestions)?;
                Ok(ReadAction::Continue)
            }
            KeyCode::Tab => {
                if let Some(suggestion) = self.selected_suggestion(input, suggestions) {
                    *input = suggestion.completion;
                    *cursor_index = input.len();
                    self.history_cursor = None;
                    self.suggestion_index = 0;
                    self.render(prompt, input, *cursor_index, suggestions)?;
                }
                Ok(ReadAction::Continue)
            }
            KeyCode::Up => {
                if self.has_visible_suggestions(input, suggestions) {
                    self.previous_suggestion(input, suggestions);
                } else {
                    self.previous_history(input, cursor_index);
                }
                self.render(prompt, input, *cursor_index, suggestions)?;
                Ok(ReadAction::Continue)
            }
            KeyCode::Down => {
                if self.has_visible_suggestions(input, suggestions) {
                    self.next_suggestion(input, suggestions);
                } else {
                    self.next_history(input, cursor_index);
                }
                self.render(prompt, input, *cursor_index, suggestions)?;
                Ok(ReadAction::Continue)
            }
            KeyCode::Char(character) => {
                if self.accept_char(character, key.modifiers) {
                    insert_char_at_cursor(input, cursor_index, character);
                    self.history_cursor = None;
                    self.suggestion_index = 0;
                    self.render(prompt, input, *cursor_index, suggestions)?;
                }
                Ok(ReadAction::Continue)
            }
            _ => Ok(ReadAction::Continue),
        }
    }

    fn has_visible_suggestions(
        &self,
        input: &str,
        suggestions: &mut impl FnMut(&str) -> Vec<ShellSuggestion>,
    ) -> bool {
        !input.trim().is_empty() && !suggestions(input).is_empty()
    }

    fn selected_suggestion(
        &self,
        input: &str,
        suggestions: &mut impl FnMut(&str) -> Vec<ShellSuggestion>,
    ) -> Option<ShellSuggestion> {
        let candidates = suggestions(input);
        candidates
            .get(
                self.suggestion_index
                    .min(candidates.len().saturating_sub(1)),
            )
            .cloned()
    }

    fn previous_suggestion(
        &mut self,
        input: &str,
        suggestions: &mut impl FnMut(&str) -> Vec<ShellSuggestion>,
    ) {
        let count = suggestions(input).len();
        if count == 0 {
            return;
        }
        self.suggestion_index = if self.suggestion_index == 0 {
            count - 1
        } else {
            self.suggestion_index - 1
        };
    }

    fn next_suggestion(
        &mut self,
        input: &str,
        suggestions: &mut impl FnMut(&str) -> Vec<ShellSuggestion>,
    ) {
        let count = suggestions(input).len();
        if count == 0 {
            return;
        }
        self.suggestion_index = (self.suggestion_index + 1) % count;
    }

    fn previous_history(&mut self, input: &mut String, cursor_index: &mut usize) {
        if self.history.is_empty() {
            return;
        }
        let next = match self.history_cursor {
            Some(index) if index > 0 => index - 1,
            Some(index) => index,
            None => self.history.len() - 1,
        };
        self.history_cursor = Some(next);
        *input = self.history[next].clone();
        *cursor_index = input.len();
    }

    fn next_history(&mut self, input: &mut String, cursor_index: &mut usize) {
        let Some(index) = self.history_cursor else {
            return;
        };
        if index + 1 >= self.history.len() {
            self.history_cursor = None;
            input.clear();
            *cursor_index = 0;
        } else {
            let next = index + 1;
            self.history_cursor = Some(next);
            *input = self.history[next].clone();
            *cursor_index = input.len();
        }
    }

    fn accept_char(&mut self, character: char, modifiers: KeyModifiers) -> bool {
        let now = Instant::now();
        if let Some((last_character, last_modifiers, last_at)) = self.last_char
            && last_character == character
            && last_modifiers == modifiers
            && now.duration_since(last_at) < Duration::from_millis(25)
        {
            return false;
        }
        self.last_char = Some((character, modifiers, now));
        true
    }

    fn render(
        &mut self,
        prompt: &str,
        input: &str,
        cursor_index: usize,
        suggestions: &mut impl FnMut(&str) -> Vec<ShellSuggestion>,
    ) -> Result<()> {
        let candidates = suggestions(input);
        if !candidates.is_empty() {
            self.suggestion_index = self.suggestion_index.min(candidates.len() - 1);
        } else {
            self.suggestion_index = 0;
        }

        let mut stdout = io::stdout();
        self.clear_rendered(&mut stdout).map_err(terminal_error)?;
        queue!(
            stdout,
            Print(prompt),
            Print(input),
            Clear(ClearType::UntilNewLine)
        )
        .map_err(terminal_error)?;

        let visible_count = candidates.len().min(8);
        for (index, suggestion) in candidates.iter().take(visible_count).enumerate() {
            queue!(stdout, Print("\r\n")).map_err(terminal_error)?;
            if index == self.suggestion_index {
                queue!(
                    stdout,
                    SetForegroundColor(Color::Cyan),
                    SetAttribute(Attribute::Bold),
                    Print("> "),
                    Print(padded(&suggestion.completion, 18)),
                    ResetColor,
                    SetAttribute(Attribute::Reset),
                    Print(&suggestion.description)
                )
                .map_err(terminal_error)?;
            } else {
                queue!(
                    stdout,
                    Print("  "),
                    SetForegroundColor(Color::DarkGrey),
                    Print(padded(&suggestion.completion, 18)),
                    ResetColor,
                    Print(&suggestion.description)
                )
                .map_err(terminal_error)?;
            }
            queue!(stdout, Clear(ClearType::UntilNewLine)).map_err(terminal_error)?;
        }

        let footer = suggestion_footer_line_for_input(
            candidates.len(),
            visible_count,
            self.language,
            input.trim().is_empty(),
        );
        if let Some(footer) = &footer {
            queue!(
                stdout,
                Print("\r\n"),
                SetForegroundColor(Color::DarkGrey),
                Print(footer),
                ResetColor,
                Clear(ClearType::UntilNewLine)
            )
            .map_err(terminal_error)?;
        }

        if visible_count > 0 {
            let rendered_lines = visible_count + usize::from(footer.is_some());
            queue!(stdout, cursor::MoveUp(rendered_lines as u16)).map_err(terminal_error)?;
        }
        queue!(
            stdout,
            cursor::MoveToColumn(display_width(prompt) + display_width(&input[..cursor_index]))
        )
        .map_err(terminal_error)?;
        stdout.flush().map_err(terminal_error)?;
        Ok(())
    }

    fn clear_rendered(&mut self, stdout: &mut io::Stdout) -> io::Result<()> {
        queue!(
            stdout,
            cursor::MoveToColumn(0),
            Clear(ClearType::FromCursorDown)
        )?;
        Ok(())
    }

    fn finish_line(&mut self, prompt: &str, input: &str) -> Result<()> {
        let mut stdout = io::stdout();
        self.clear_rendered(&mut stdout).map_err(terminal_error)?;
        queue!(
            stdout,
            Print(prompt),
            Print(input),
            Clear(ClearType::UntilNewLine),
            Print("\r\n")
        )
        .map_err(terminal_error)?;
        stdout.flush().map_err(terminal_error)?;
        Ok(())
    }

    fn finish_interrupt(&mut self, prompt: &str, input: &str) -> Result<()> {
        let mut stdout = io::stdout();
        self.clear_rendered(&mut stdout).map_err(terminal_error)?;
        queue!(
            stdout,
            Print(prompt),
            Print(input),
            Clear(ClearType::UntilNewLine),
            Print("^C\r\n")
        )
        .map_err(terminal_error)?;
        stdout.flush().map_err(terminal_error)?;
        Ok(())
    }

    fn flush_external_output<F>(
        &mut self,
        prompt: &str,
        input: &str,
        cursor_index: usize,
        raw_mode: &mut RawModeGuard,
        suggestions: &mut impl FnMut(&str) -> Vec<ShellSuggestion>,
        on_output: &mut F,
    ) -> Result<()>
    where
        F: FnMut() -> Result<Vec<String>>,
    {
        let lines = on_output()?;
        if lines.is_empty() {
            return Ok(());
        }

        let mut stdout = io::stdout();
        self.clear_rendered(&mut stdout).map_err(terminal_error)?;
        stdout.flush().map_err(terminal_error)?;
        raw_mode.disable()?;
        print_lines(lines);
        raw_mode.reenable()?;
        self.render(prompt, input, cursor_index, suggestions)?;
        Ok(())
    }
}

enum ReadAction {
    Continue,
    Submit,
    Cancel,
}

struct RawModeGuard {
    enabled: bool,
}

impl RawModeGuard {
    fn enable() -> Result<Self> {
        terminal::enable_raw_mode().map_err(terminal_error)?;
        Ok(Self { enabled: true })
    }

    fn reenable(&mut self) -> Result<()> {
        if !self.enabled {
            terminal::enable_raw_mode().map_err(terminal_error)?;
            self.enabled = true;
        }
        Ok(())
    }

    fn disable(&mut self) -> Result<()> {
        if self.enabled {
            terminal::disable_raw_mode().map_err(terminal_error)?;
            self.enabled = false;
        }
        Ok(())
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        if self.enabled {
            let _ = terminal::disable_raw_mode();
        }
    }
}

fn insert_char_at_cursor(input: &mut String, cursor_index: &mut usize, character: char) {
    input.insert(*cursor_index, character);
    *cursor_index += character.len_utf8();
}

fn remove_char_before_cursor(input: &mut String, cursor_index: &mut usize) -> bool {
    if *cursor_index == 0 {
        return false;
    }

    let previous = previous_char_boundary(input, *cursor_index);
    input.drain(previous..*cursor_index);
    *cursor_index = previous;
    true
}

fn remove_char_at_cursor(input: &mut String, cursor_index: &mut usize) -> bool {
    if *cursor_index >= input.len() {
        return false;
    }

    let next = next_char_boundary(input, *cursor_index);
    input.drain(*cursor_index..next);
    true
}

fn remove_word_before_cursor(input: &mut String, cursor_index: &mut usize) -> bool {
    if *cursor_index == 0 {
        return false;
    }

    let start = previous_word_boundary(input, *cursor_index);
    input.drain(start..*cursor_index);
    *cursor_index = start;
    true
}

fn remove_after_cursor(input: &mut String, cursor_index: &mut usize) -> bool {
    if *cursor_index >= input.len() {
        return false;
    }

    input.truncate(*cursor_index);
    true
}

fn previous_char_boundary(input: &str, cursor_index: usize) -> usize {
    let cursor_index = cursor_index.min(input.len());
    if cursor_index == 0 {
        return 0;
    }

    input[..cursor_index]
        .char_indices()
        .last()
        .map(|(index, _)| index)
        .unwrap_or(0)
}

fn next_char_boundary(input: &str, cursor_index: usize) -> usize {
    let cursor_index = cursor_index.min(input.len());
    if cursor_index >= input.len() {
        return input.len();
    }

    input[cursor_index..]
        .chars()
        .next()
        .map(|character| cursor_index + character.len_utf8())
        .unwrap_or(input.len())
}

fn previous_word_boundary(input: &str, cursor_index: usize) -> usize {
    let mut index = cursor_index.min(input.len());
    while index > 0 {
        let previous = previous_char_boundary(input, index);
        let Some(character) = input[previous..index].chars().next() else {
            return previous;
        };
        if !character.is_whitespace() {
            break;
        }
        index = previous;
    }

    while index > 0 {
        let previous = previous_char_boundary(input, index);
        let Some(character) = input[previous..index].chars().next() else {
            return previous;
        };
        if character.is_whitespace() {
            break;
        }
        index = previous;
    }

    index
}

fn next_word_boundary(input: &str, cursor_index: usize) -> usize {
    let mut index = cursor_index.min(input.len());
    while index < input.len() {
        let next = next_char_boundary(input, index);
        let Some(character) = input[index..next].chars().next() else {
            return next;
        };
        if character.is_whitespace() {
            break;
        }
        index = next;
    }

    while index < input.len() {
        let next = next_char_boundary(input, index);
        let Some(character) = input[index..next].chars().next() else {
            return next;
        };
        if !character.is_whitespace() {
            break;
        }
        index = next;
    }

    index
}

fn accepted_suggestion_needs_more_input(suggestion: &ShellSuggestion) -> bool {
    suggestion.completion.ends_with(' ')
}

fn should_accept_suggestion_on_enter(input: &str, suggestion: &ShellSuggestion) -> bool {
    input != suggestion.completion && input.trim_end() != suggestion.completion.trim_end()
}

#[cfg(test)]
fn suggestion_footer_line(
    total_count: usize,
    visible_count: usize,
    language: ShellLanguage,
) -> Option<String> {
    suggestion_footer_line_for_input(total_count, visible_count, language, false)
}

fn suggestion_footer_line_for_input(
    total_count: usize,
    visible_count: usize,
    language: ShellLanguage,
    input_is_empty: bool,
) -> Option<String> {
    if visible_count == 0 {
        return None;
    }

    let hidden_count = total_count.saturating_sub(visible_count);
    let mut line = match language {
        ShellLanguage::English if input_is_empty => {
            "Tab accepts first | Enter shows tips | Up/Down history".to_string()
        }
        ShellLanguage::Chinese if input_is_empty => {
            "Tab 接受第一条 | Enter 显示下一步 | 上/下 历史".to_string()
        }
        ShellLanguage::English => "Up/Down select | Tab accept | Enter accept/run".to_string(),
        ShellLanguage::Chinese => "上/下 选择 | Tab 补全 | Enter 接受/执行".to_string(),
    };
    if hidden_count > 0 {
        match language {
            ShellLanguage::English => line.push_str(&format!(" | +{hidden_count} more")),
            ShellLanguage::Chinese => line.push_str(&format!(" | 还有 {hidden_count} 个")),
        }
    }
    Some(line)
}

fn read_history_entries(path: &Path) -> Result<Vec<String>> {
    match std::fs::read_to_string(path) {
        Ok(contents) => Ok(parse_history_entries(&contents)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(error.into()),
    }
}

fn parse_history_entries(contents: &str) -> Vec<String> {
    let mut history = Vec::new();
    for line in contents.lines() {
        push_history_entry(&mut history, line);
    }
    history
}

fn push_history_entry(history: &mut Vec<String>, command: &str) -> bool {
    let command = command.trim();
    if command.is_empty() || history.last().is_some_and(|entry| entry == command) {
        return false;
    }

    history.push(command.to_string());
    if history.len() > HISTORY_LIMIT {
        let overflow = history.len() - HISTORY_LIMIT;
        history.drain(0..overflow);
    }
    true
}

fn serialize_history_entries(history: &[String]) -> String {
    if history.is_empty() {
        return String::new();
    }

    format!("{}\n", history.join("\n"))
}

fn print_history_entries(history: &[String], count: usize) {
    if history.is_empty() {
        println!("history is empty");
        println!("try: search <query>, scan, or play 1");
        return;
    }

    let start = history.len().saturating_sub(count);
    for (index, command) in history.iter().enumerate().skip(start) {
        println!("{:>3}  {}", index + 1, command);
    }
    println!("replay: !<number>");
}

fn history_replay_index(command: &str) -> Option<usize> {
    let command = command.trim();
    if command == "!!" {
        return None;
    }
    let digits = command.strip_prefix('!')?;
    if digits.is_empty() || !digits.chars().all(|character| character.is_ascii_digit()) {
        return None;
    }

    digits.parse::<usize>().ok().filter(|index| *index > 0)
}

fn load_result_window(
    database: &Database,
    query: &str,
    limit: usize,
) -> Result<(Vec<Track>, bool)> {
    let fetch_limit = limit.saturating_add(1);
    let mut results: Vec<Track> = search::search(database, query, fetch_limit)?
        .into_iter()
        .map(|result| result.track)
        .collect();
    let has_more = results.len() > limit;
    results.truncate(limit);
    Ok((results, has_more))
}

fn is_history_worthy(command: &str) -> bool {
    let command = command.trim_start_matches([':', '/']).trim();
    let first = command
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    !matches!(
        first.as_str(),
        "" | "q"
            | "quit"
            | "exit"
            | "退出"
            | "clear"
            | "cls"
            | "清屏"
            | "history"
            | "again"
            | "repeat"
            | "!!"
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CommandSuggestion {
    completion: &'static str,
    usage: &'static str,
    description: &'static str,
}

impl CommandSuggestion {
    fn to_shell_suggestion(self, language: ShellLanguage) -> ShellSuggestion {
        ShellSuggestion {
            completion: self.completion.to_string(),
            description: localized_command_description(self.description, language).to_string(),
        }
    }
}

fn command_suggestions(input: &str) -> Vec<CommandSuggestion> {
    let input = normalized_suggestion_input(input);
    let suggestions = [
        CommandSuggestion {
            completion: "scan",
            usage: "scan",
            description: "open folder picker and scan",
        },
        CommandSuggestion {
            completion: "add",
            usage: "add",
            description: "choose a music folder",
        },
        CommandSuggestion {
            completion: "scan add",
            usage: "scan add",
            description: "same as scan",
        },
        CommandSuggestion {
            completion: "scan ",
            usage: "scan <folder>",
            description: "scan folder path directly",
        },
        CommandSuggestion {
            completion: "扫描",
            usage: "扫描",
            description: "open folder picker and scan",
        },
        CommandSuggestion {
            completion: "添加",
            usage: "添加",
            description: "same as scan",
        },
        CommandSuggestion {
            completion: "search ",
            usage: "search <query>",
            description: "search indexed tracks",
        },
        CommandSuggestion {
            completion: "find ",
            usage: "find <query>",
            description: "same as search",
        },
        CommandSuggestion {
            completion: "搜索 ",
            usage: "搜索 <query>",
            description: "search indexed tracks",
        },
        CommandSuggestion {
            completion: "找 ",
            usage: "找 <query>",
            description: "same as search",
        },
        CommandSuggestion {
            completion: "play ",
            usage: "play <query|#>",
            description: "play by query or result number",
        },
        CommandSuggestion {
            completion: "播放 ",
            usage: "播放 <query|#>",
            description: "play by query or result number",
        },
        CommandSuggestion {
            completion: "播放 下一首",
            usage: "播放 下一首",
            description: "play next visible result",
        },
        CommandSuggestion {
            completion: "播放 上一首",
            usage: "播放 上一首",
            description: "play previous visible result",
        },
        CommandSuggestion {
            completion: "播放 随机",
            usage: "播放 随机",
            description: "play a random visible result",
        },
        CommandSuggestion {
            completion: "play last",
            usage: "play last",
            description: "play last listed result",
        },
        CommandSuggestion {
            completion: "play first",
            usage: "play first",
            description: "same as play best",
        },
        CommandSuggestion {
            completion: "play best",
            usage: "play best",
            description: "play first listed result",
        },
        CommandSuggestion {
            completion: "play next",
            usage: "play next",
            description: "play next visible result",
        },
        CommandSuggestion {
            completion: "play prev",
            usage: "play prev",
            description: "play previous visible result",
        },
        CommandSuggestion {
            completion: "play previous",
            usage: "play previous",
            description: "same as play prev",
        },
        CommandSuggestion {
            completion: "play random",
            usage: "play random",
            description: "play a random visible result",
        },
        CommandSuggestion {
            completion: "play surprise",
            usage: "play surprise",
            description: "pick something for me",
        },
        CommandSuggestion {
            completion: "shuffle",
            usage: "shuffle",
            description: "play a random visible result",
        },
        CommandSuggestion {
            completion: "surprise",
            usage: "surprise",
            description: "pick something for me",
        },
        CommandSuggestion {
            completion: "random",
            usage: "random",
            description: "same as shuffle",
        },
        CommandSuggestion {
            completion: "随机",
            usage: "随机",
            description: "same as shuffle",
        },
        CommandSuggestion {
            completion: "随便",
            usage: "随便",
            description: "pick something for me",
        },
        CommandSuggestion {
            completion: "results",
            usage: "results",
            description: "print current results again",
        },
        CommandSuggestion {
            completion: "more",
            usage: "more",
            description: "show more current results",
        },
        CommandSuggestion {
            completion: "结果",
            usage: "结果",
            description: "print current results again",
        },
        CommandSuggestion {
            completion: "更多",
            usage: "更多",
            description: "show more current results",
        },
        CommandSuggestion {
            completion: "r",
            usage: "r",
            description: "same as results",
        },
        CommandSuggestion {
            completion: "open ",
            usage: "open <query|#>",
            description: "show a track in Explorer",
        },
        CommandSuggestion {
            completion: "open current",
            usage: "open current",
            description: "show current track in Explorer",
        },
        CommandSuggestion {
            completion: "reveal ",
            usage: "reveal <query|#>",
            description: "same as open",
        },
        CommandSuggestion {
            completion: "folder",
            usage: "folder",
            description: "show current track in Explorer",
        },
        CommandSuggestion {
            completion: "where",
            usage: "where",
            description: "show current track in Explorer",
        },
        CommandSuggestion {
            completion: "打开 ",
            usage: "打开 <query|#>",
            description: "show a track in Explorer",
        },
        CommandSuggestion {
            completion: "位置",
            usage: "位置",
            description: "show current track in Explorer",
        },
        CommandSuggestion {
            completion: "copy ",
            usage: "copy <query|#>",
            description: "copy a track path",
        },
        CommandSuggestion {
            completion: "复制 ",
            usage: "复制 <query|#>",
            description: "copy a track path",
        },
        CommandSuggestion {
            completion: "copy current",
            usage: "copy current",
            description: "copy current track path",
        },
        CommandSuggestion {
            completion: "info ",
            usage: "info <query|#>",
            description: "show track details",
        },
        CommandSuggestion {
            completion: "info current",
            usage: "info current",
            description: "show current track details",
        },
        CommandSuggestion {
            completion: "信息 ",
            usage: "信息 <query|#>",
            description: "show track details",
        },
        CommandSuggestion {
            completion: "详情 ",
            usage: "详情 <query|#>",
            description: "show track details",
        },
        CommandSuggestion {
            completion: "history",
            usage: "history",
            description: "show recent commands",
        },
        CommandSuggestion {
            completion: "history clear",
            usage: "history clear",
            description: "clear saved command history",
        },
        CommandSuggestion {
            completion: "again",
            usage: "again",
            description: "repeat the last command",
        },
        CommandSuggestion {
            completion: "repeat",
            usage: "repeat",
            description: "same as again",
        },
        CommandSuggestion {
            completion: "next",
            usage: "next",
            description: "play next visible result",
        },
        CommandSuggestion {
            completion: "下一首",
            usage: "下一首",
            description: "play next visible result",
        },
        CommandSuggestion {
            completion: "prev",
            usage: "prev",
            description: "play previous visible result",
        },
        CommandSuggestion {
            completion: "previous",
            usage: "previous",
            description: "same as prev",
        },
        CommandSuggestion {
            completion: "上一首",
            usage: "上一首",
            description: "play previous visible result",
        },
        CommandSuggestion {
            completion: "tips",
            usage: "tips",
            description: "show suggested next steps",
        },
        CommandSuggestion {
            completion: "提示",
            usage: "提示",
            description: "show suggested next steps",
        },
        CommandSuggestion {
            completion: "home",
            usage: "home",
            description: "show the welcome screen",
        },
        CommandSuggestion {
            completion: "首页",
            usage: "首页",
            description: "show the welcome screen",
        },
        CommandSuggestion {
            completion: "shortcuts",
            usage: "shortcuts",
            description: "show keyboard shortcuts",
        },
        CommandSuggestion {
            completion: "keys",
            usage: "keys",
            description: "same as shortcuts",
        },
        CommandSuggestion {
            completion: "快捷键",
            usage: "快捷键",
            description: "same as shortcuts",
        },
        CommandSuggestion {
            completion: "aliases",
            usage: "aliases",
            description: "show alternate command names",
        },
        CommandSuggestion {
            completion: "alias",
            usage: "alias",
            description: "same as aliases",
        },
        CommandSuggestion {
            completion: "别名",
            usage: "别名",
            description: "same as aliases",
        },
        CommandSuggestion {
            completion: "language",
            usage: "language",
            description: "switch English / 中文",
        },
        CommandSuggestion {
            completion: "language zh",
            usage: "language zh",
            description: "switch to 中文",
        },
        CommandSuggestion {
            completion: "language en",
            usage: "language en",
            description: "switch to English",
        },
        CommandSuggestion {
            completion: "language status",
            usage: "language status",
            description: "show current language",
        },
        CommandSuggestion {
            completion: "language list",
            usage: "language list",
            description: "show available languages",
        },
        CommandSuggestion {
            completion: "语言",
            usage: "语言",
            description: "switch English / 中文",
        },
        CommandSuggestion {
            completion: "语言 zh",
            usage: "语言 zh",
            description: "切换到中文",
        },
        CommandSuggestion {
            completion: "语言 en",
            usage: "语言 en",
            description: "切换到 English",
        },
        CommandSuggestion {
            completion: "语言 状态",
            usage: "语言 状态",
            description: "show current language",
        },
        CommandSuggestion {
            completion: "语言 列表",
            usage: "语言 列表",
            description: "show available languages",
        },
        CommandSuggestion {
            completion: "pause",
            usage: "pause",
            description: "pause playback",
        },
        CommandSuggestion {
            completion: "暂停",
            usage: "暂停",
            description: "pause playback",
        },
        CommandSuggestion {
            completion: "resume",
            usage: "resume",
            description: "resume playback",
        },
        CommandSuggestion {
            completion: "继续",
            usage: "继续",
            description: "resume playback",
        },
        CommandSuggestion {
            completion: "stop",
            usage: "stop",
            description: "stop playback",
        },
        CommandSuggestion {
            completion: "停止",
            usage: "停止",
            description: "stop playback",
        },
        CommandSuggestion {
            completion: "quit",
            usage: "quit",
            description: "exit",
        },
        CommandSuggestion {
            completion: "exit",
            usage: "exit",
            description: "same as quit",
        },
        CommandSuggestion {
            completion: "q",
            usage: "q",
            description: "same as quit",
        },
        CommandSuggestion {
            completion: "退出",
            usage: "退出",
            description: "same as quit",
        },
        CommandSuggestion {
            completion: "/pause",
            usage: "/pause",
            description: "pause playback",
        },
        CommandSuggestion {
            completion: "/resume",
            usage: "/resume",
            description: "resume playback",
        },
        CommandSuggestion {
            completion: "/stop",
            usage: "/stop",
            description: "stop playback",
        },
        CommandSuggestion {
            completion: "/next",
            usage: "/next",
            description: "play next visible result",
        },
        CommandSuggestion {
            completion: "/prev",
            usage: "/prev",
            description: "play previous visible result",
        },
        CommandSuggestion {
            completion: "/play ",
            usage: "/play <query|#>",
            description: "play by query or result number",
        },
        CommandSuggestion {
            completion: "/play next",
            usage: "/play next",
            description: "play next visible result",
        },
        CommandSuggestion {
            completion: "/play prev",
            usage: "/play prev",
            description: "play previous visible result",
        },
        CommandSuggestion {
            completion: "/play random",
            usage: "/play random",
            description: "play a random visible result",
        },
        CommandSuggestion {
            completion: "/search ",
            usage: "/search <query>",
            description: "search indexed tracks",
        },
        CommandSuggestion {
            completion: "/find ",
            usage: "/find <query>",
            description: "same as search",
        },
        CommandSuggestion {
            completion: "/scan",
            usage: "/scan",
            description: "open folder picker and scan",
        },
        CommandSuggestion {
            completion: "/library",
            usage: "/library",
            description: "show indexed tracks",
        },
        CommandSuggestion {
            completion: "/songs",
            usage: "/songs",
            description: "same as library",
        },
        CommandSuggestion {
            completion: "/results",
            usage: "/results",
            description: "print current results again",
        },
        CommandSuggestion {
            completion: "/more",
            usage: "/more",
            description: "show more current results",
        },
        CommandSuggestion {
            completion: "/open ",
            usage: "/open <query|#>",
            description: "show a track in Explorer",
        },
        CommandSuggestion {
            completion: "/reveal ",
            usage: "/reveal <query|#>",
            description: "same as open",
        },
        CommandSuggestion {
            completion: "/shortcuts",
            usage: "/shortcuts",
            description: "show keyboard shortcuts",
        },
        CommandSuggestion {
            completion: "/aliases",
            usage: "/aliases",
            description: "show alternate command names",
        },
        CommandSuggestion {
            completion: "/language",
            usage: "/language",
            description: "switch English / 中文",
        },
        CommandSuggestion {
            completion: "/language zh",
            usage: "/language zh",
            description: "switch to 中文",
        },
        CommandSuggestion {
            completion: "/language en",
            usage: "/language en",
            description: "switch to English",
        },
        CommandSuggestion {
            completion: "/language status",
            usage: "/language status",
            description: "show current language",
        },
        CommandSuggestion {
            completion: "/language list",
            usage: "/language list",
            description: "show available languages",
        },
        CommandSuggestion {
            completion: "/quit",
            usage: "/quit",
            description: "stop playback and exit",
        },
        CommandSuggestion {
            completion: "/help",
            usage: "/help",
            description: "show commands",
        },
        CommandSuggestion {
            completion: "/home",
            usage: "/home",
            description: "show the welcome screen",
        },
        CommandSuggestion {
            completion: "/tips",
            usage: "/tips",
            description: "show suggested next steps",
        },
        CommandSuggestion {
            completion: "/now",
            usage: "/now",
            description: "show current track",
        },
        CommandSuggestion {
            completion: "/current",
            usage: "/current",
            description: "same as now",
        },
        CommandSuggestion {
            completion: "/status",
            usage: "/status",
            description: "show shell status",
        },
        CommandSuggestion {
            completion: "/devices",
            usage: "/devices",
            description: "list output devices",
        },
        CommandSuggestion {
            completion: "/health",
            usage: "/health",
            description: "same as doctor",
        },
        CommandSuggestion {
            completion: "library",
            usage: "library",
            description: "show indexed tracks",
        },
        CommandSuggestion {
            completion: "曲库",
            usage: "曲库",
            description: "show indexed tracks",
        },
        CommandSuggestion {
            completion: "list",
            usage: "list",
            description: "same as library",
        },
        CommandSuggestion {
            completion: "recent",
            usage: "recent",
            description: "show recent tracks",
        },
        CommandSuggestion {
            completion: "songs",
            usage: "songs",
            description: "same as library",
        },
        CommandSuggestion {
            completion: "tracks",
            usage: "tracks",
            description: "same as library",
        },
        CommandSuggestion {
            completion: "列表",
            usage: "列表",
            description: "same as library",
        },
        CommandSuggestion {
            completion: "歌曲",
            usage: "歌曲",
            description: "same as library",
        },
        CommandSuggestion {
            completion: "now",
            usage: "now",
            description: "show current track",
        },
        CommandSuggestion {
            completion: "当前",
            usage: "当前",
            description: "show current track",
        },
        CommandSuggestion {
            completion: "current",
            usage: "current",
            description: "same as now",
        },
        CommandSuggestion {
            completion: "playing",
            usage: "playing",
            description: "same as now",
        },
        CommandSuggestion {
            completion: "正在播放",
            usage: "正在播放",
            description: "same as now",
        },
        CommandSuggestion {
            completion: "status",
            usage: "status",
            description: "show shell status",
        },
        CommandSuggestion {
            completion: "状态",
            usage: "状态",
            description: "show shell status",
        },
        CommandSuggestion {
            completion: "devices",
            usage: "devices",
            description: "list output devices",
        },
        CommandSuggestion {
            completion: "设备",
            usage: "设备",
            description: "list output devices",
        },
        CommandSuggestion {
            completion: "device",
            usage: "device",
            description: "same as devices",
        },
        CommandSuggestion {
            completion: "outputs",
            usage: "outputs",
            description: "same as devices",
        },
        CommandSuggestion {
            completion: "output",
            usage: "output",
            description: "same as devices",
        },
        CommandSuggestion {
            completion: "输出",
            usage: "输出",
            description: "same as devices",
        },
        CommandSuggestion {
            completion: "doctor",
            usage: "doctor",
            description: "print diagnostics",
        },
        CommandSuggestion {
            completion: "诊断",
            usage: "诊断",
            description: "same as doctor",
        },
        CommandSuggestion {
            completion: "diagnose",
            usage: "diagnose",
            description: "same as doctor",
        },
        CommandSuggestion {
            completion: "diagnostics",
            usage: "diagnostics",
            description: "same as doctor",
        },
        CommandSuggestion {
            completion: "health",
            usage: "health",
            description: "same as doctor",
        },
        CommandSuggestion {
            completion: "check",
            usage: "check",
            description: "same as doctor",
        },
        CommandSuggestion {
            completion: "检查",
            usage: "检查",
            description: "same as doctor",
        },
        CommandSuggestion {
            completion: "errors",
            usage: "errors",
            description: "show recent scan errors",
        },
        CommandSuggestion {
            completion: "错误",
            usage: "错误",
            description: "show recent scan errors",
        },
        CommandSuggestion {
            completion: "open-db",
            usage: "open-db",
            description: "open database folder",
        },
        CommandSuggestion {
            completion: "help",
            usage: "help",
            description: "show commands",
        },
        CommandSuggestion {
            completion: "帮助",
            usage: "帮助",
            description: "show commands",
        },
        CommandSuggestion {
            completion: "commands",
            usage: "commands",
            description: "same as help",
        },
        CommandSuggestion {
            completion: "命令",
            usage: "命令",
            description: "same as help",
        },
        CommandSuggestion {
            completion: "?",
            usage: "?",
            description: "same as help",
        },
        CommandSuggestion {
            completion: "clear",
            usage: "clear",
            description: "clear screen",
        },
        CommandSuggestion {
            completion: "清屏",
            usage: "清屏",
            description: "same as clear",
        },
        CommandSuggestion {
            completion: "cls",
            usage: "cls",
            description: "same as clear",
        },
    ];

    if input.is_empty() {
        return suggestions.to_vec();
    }

    suggestions
        .into_iter()
        .filter(|suggestion| {
            suggestion.completion.starts_with(&input) || suggestion.usage.starts_with(&input)
        })
        .collect()
}

fn localized_command_description(
    description: &'static str,
    language: ShellLanguage,
) -> &'static str {
    if language == ShellLanguage::English {
        return description;
    }

    match description {
        "open folder picker and scan" => "打开文件夹选择框并扫描",
        "choose a music folder" => "选择音乐文件夹",
        "same as scan" => "同 扫描",
        "scan folder path directly" => "直接扫描文件夹路径",
        "search indexed tracks" => "搜索已入库歌曲",
        "same as search" => "同 搜索",
        "play by query or result number" => "按关键词或编号播放",
        "play next visible result" => "播放当前列表下一首",
        "play previous visible result" => "播放当前列表上一首",
        "play a random visible result" => "随机播放当前列表",
        "play last listed result" => "播放列表最后一首",
        "same as play best" => "同 play best",
        "play first listed result" => "播放列表第一首",
        "same as play prev" => "同 play prev",
        "pick something for me" => "帮我随便选一首",
        "same as shuffle" => "同 随机",
        "print current results again" => "重新显示当前结果",
        "show more current results" => "显示更多当前结果",
        "same as results" => "同 结果",
        "show a track in Explorer" => "在 Explorer 中定位歌曲",
        "show current track in Explorer" => "在 Explorer 中定位当前歌曲",
        "same as open" => "同 打开",
        "copy a track path" => "复制歌曲路径",
        "copy current track path" => "复制当前歌曲路径",
        "show track details" => "显示歌曲详情",
        "show current track details" => "显示当前歌曲详情",
        "show recent commands" => "显示最近命令",
        "clear saved command history" => "清空已保存历史",
        "repeat the last command" => "重复上一条命令",
        "same as again" => "同 again",
        "show suggested next steps" => "显示下一步建议",
        "show the welcome screen" => "显示欢迎页",
        "show keyboard shortcuts" => "显示快捷键",
        "same as shortcuts" => "同 快捷键",
        "show alternate command names" => "显示命令别名",
        "same as aliases" => "同 别名",
        "switch English / 中文" => "切换 English / 中文",
        "switch to 中文" => "切换到中文",
        "switch to English" => "切换到 English",
        "show current language" => "显示当前语言",
        "show available languages" => "显示可用语言",
        "pause playback" => "暂停播放",
        "resume playback" => "继续播放",
        "stop playback" => "停止播放",
        "exit" => "退出",
        "same as quit" => "同 退出",
        "stop playback and exit" => "停止播放并退出",
        "show indexed tracks" => "显示已入库歌曲",
        "show recent tracks" => "显示最近歌曲",
        "show current track" => "显示当前歌曲",
        "same as now" => "同 当前",
        "show shell status" => "显示 shell 状态",
        "list output devices" => "列出输出设备",
        "same as devices" => "同 设备",
        "print diagnostics" => "打印诊断信息",
        "same as doctor" => "同 诊断",
        "show recent scan errors" => "显示最近扫描错误",
        "open database folder" => "打开数据库文件夹",
        "show commands" => "显示命令",
        "same as help" => "同 帮助",
        "clear screen" => "清屏",
        "same as clear" => "同 清屏",
        _ => description,
    }
}

fn searching_line(input: &str, language: ShellLanguage) -> String {
    match language {
        ShellLanguage::English => format!("searching {input}"),
        ShellLanguage::Chinese => format!("正在搜索 {input}"),
    }
}

fn unknown_command_lines(
    first_token: &str,
    input: &str,
    suggestions: Vec<CommandSuggestion>,
    language: ShellLanguage,
) -> Vec<String> {
    let mut lines = match language {
        ShellLanguage::English => vec![
            format!("unknown command: {first_token}"),
            "did you mean:".to_string(),
        ],
        ShellLanguage::Chinese => vec![
            format!("未知命令：{first_token}"),
            "你是不是想用:".to_string(),
        ],
    };

    lines.extend(suggestions.into_iter().map(|suggestion| {
        format!(
            "  {:<16} {}",
            suggestion.completion,
            localized_command_description(suggestion.description, language)
        )
    }));

    match language {
        ShellLanguage::English => lines.push(format!("or type search {input}")),
        ShellLanguage::Chinese => lines.push(format!("或者输入 搜索 {input}")),
    }

    lines
}

fn normalized_suggestion_input(input: &str) -> String {
    input
        .trim_start()
        .trim_start_matches(':')
        .trim_start()
        .to_ascii_lowercase()
}

fn slash_command_view(normalized_input: &str) -> (&str, &str) {
    normalized_input
        .strip_prefix('/')
        .map(|input| (input, "/"))
        .unwrap_or((normalized_input, ""))
}

fn nearest_command_suggestions(input: &str) -> Vec<CommandSuggestion> {
    let normalized = input
        .trim_start()
        .trim_start_matches([':', '/'])
        .trim()
        .to_ascii_lowercase();
    if normalized.is_empty() {
        return Vec::new();
    }

    let mut ranked: Vec<_> = command_suggestions("")
        .into_iter()
        .map(|suggestion| {
            let token = suggestion
                .completion
                .trim_start_matches('/')
                .split_whitespace()
                .next()
                .unwrap_or(suggestion.completion);
            (edit_distance(&normalized, token), suggestion)
        })
        .filter(|(distance, _)| *distance <= 2)
        .collect();
    ranked.sort_by_key(|(distance, suggestion)| (*distance, suggestion.completion.len()));
    ranked
        .into_iter()
        .map(|(_, suggestion)| suggestion)
        .take(3)
        .collect()
}

fn edit_distance(left: &str, right: &str) -> usize {
    let right_chars: Vec<char> = right.chars().collect();
    let mut previous: Vec<usize> = (0..=right_chars.len()).collect();

    for (left_index, left_char) in left.chars().enumerate() {
        let mut current = vec![left_index + 1];
        for (right_index, right_char) in right_chars.iter().enumerate() {
            let insertion = current[right_index] + 1;
            let deletion = previous[right_index + 1] + 1;
            let substitution = previous[right_index] + usize::from(left_char != *right_char);
            current.push(insertion.min(deletion).min(substitution));
        }
        previous = current;
    }

    previous[right_chars.len()]
}

fn prompt_for_playback(is_playing: bool) -> &'static str {
    if is_playing {
        "echo playing> "
    } else {
        "echo ready> "
    }
}

fn terminal_width_for_cards() -> usize {
    terminal::size()
        .map(|(width, _)| usize::from(width).clamp(64, 96))
        .unwrap_or(82)
}

fn welcome_card_lines(
    track_count: u64,
    default_device: &str,
    language: ShellLanguage,
    width: usize,
) -> Vec<String> {
    let width = width.max(72);
    let inner_width = width.saturating_sub(4);
    let split_total = width.saturating_sub(7);
    let left_width = (split_total * 42 / 100).clamp(28, split_total.saturating_sub(28));
    let right_width = split_total.saturating_sub(left_width);
    let rule = format!("+{}+", "-".repeat(width.saturating_sub(2)));
    let mut lines = vec![rule.clone()];

    match language {
        ShellLanguage::English => {
            lines.push(card_row(
                &format!(
                    "{APP_NAME} {}  local music shell",
                    env!("CARGO_PKG_VERSION")
                ),
                inner_width,
            ));
            lines.push(card_split_row(
                "Welcome back",
                "Tips for getting started",
                left_width,
                right_width,
            ));
            lines.push(card_split_row(
                &format!(
                    "{} tracks / {}",
                    track_count,
                    if track_count == 0 { "empty" } else { "indexed" }
                ),
                "scan or add    choose a music folder",
                left_width,
                right_width,
            ));
            lines.push(card_split_row(
                &format!(
                    "output {}",
                    compact(default_device, left_width.saturating_sub(7))
                ),
                "play 1         play the first result",
                left_width,
                right_width,
            ));
            lines.push(card_split_row(
                if track_count == 0 {
                    "state ready to scan"
                } else {
                    "state ready to play"
                },
                "Tab            accept the top suggestion",
                left_width,
                right_width,
            ));
            lines.push(card_divider(left_width, right_width));
            lines.push(card_split_row(
                "Now",
                "What's next",
                left_width,
                right_width,
            ));
            lines.push(card_split_row(
                "echo ready> shell",
                if track_count == 0 {
                    "scan"
                } else {
                    "play 1 / shuffle"
                },
                left_width,
                right_width,
            ));
            lines.push(card_split_row(
                "history saved",
                "search moon / info 1 / open 1",
                left_width,
                right_width,
            ));
            lines.push(card_split_row(
                "normal scrollback",
                "pause / resume / stop / next",
                left_width,
                right_width,
            ));
            lines.push(card_blank_row(inner_width));
            lines.push(card_row(
                "Type a prefix to see commands. Empty Enter shows what to do next.",
                inner_width,
            ));
        }
        ShellLanguage::Chinese => {
            lines.push(card_row(
                &format!("{APP_NAME} {}  本地音乐 shell", env!("CARGO_PKG_VERSION")),
                inner_width,
            ));
            lines.push(card_split_row(
                "欢迎回来",
                "开始提示",
                left_width,
                right_width,
            ));
            lines.push(card_split_row(
                &format!(
                    "{} 首歌 / {}",
                    track_count,
                    if track_count == 0 {
                        "空曲库"
                    } else {
                        "已入库"
                    }
                ),
                "扫描 或 添加    选择音乐文件夹",
                left_width,
                right_width,
            ));
            lines.push(card_split_row(
                &format!(
                    "输出 {}",
                    compact(default_device, left_width.saturating_sub(7))
                ),
                "播放 1         播放第一个结果",
                left_width,
                right_width,
            ));
            lines.push(card_split_row(
                if track_count == 0 {
                    "状态 准备扫描"
                } else {
                    "状态 准备播放"
                },
                "Tab            接受第一条建议",
                left_width,
                right_width,
            ));
            lines.push(card_divider(left_width, right_width));
            lines.push(card_split_row("现在", "下一步", left_width, right_width));
            lines.push(card_split_row(
                "echo ready> shell",
                if track_count == 0 {
                    "扫描"
                } else {
                    "播放 1 / 随机"
                },
                left_width,
                right_width,
            ));
            lines.push(card_split_row(
                "历史会保存",
                "搜索 moon / 信息 1 / 打开 1",
                left_width,
                right_width,
            ));
            lines.push(card_split_row(
                "普通滚动历史",
                "暂停 / 继续 / 停止 / 下一首",
                left_width,
                right_width,
            ));
            lines.push(card_blank_row(inner_width));
            lines.push(card_row(
                "输入前缀就会显示候选；空 Enter 会告诉你下一步。",
                inner_width,
            ));
        }
    }

    lines.push(rule);
    lines
}

fn print_welcome_card_lines(lines: &[String]) {
    for (index, line) in lines.iter().enumerate() {
        let is_rule = index == 0 || index + 1 == lines.len();
        if is_rule {
            println!("{}", line.as_str().with(Color::DarkGrey));
        } else if line.contains(APP_NAME) {
            println!("{}", line.as_str().with(Color::Cyan).bold());
        } else if line.contains("-+-") {
            println!("{}", line.as_str().with(Color::DarkGrey));
        } else if line.contains("Welcome back")
            || line.contains("Tips for getting started")
            || line.contains("What's next")
            || line.contains("欢迎回来")
            || line.contains("开始提示")
            || line.contains("下一步")
        {
            println!("{}", line.as_str().with(Color::DarkYellow).bold());
        } else {
            println!("{line}");
        }
    }
}

fn card_row(content: &str, inner_width: usize) -> String {
    let content = fit_line_to_width(content, inner_width);
    let padding = inner_width.saturating_sub(display_width(&content) as usize);
    format!("| {content}{} |", " ".repeat(padding))
}

fn card_blank_row(inner_width: usize) -> String {
    format!("| {} |", " ".repeat(inner_width))
}

fn card_split_row(left: &str, right: &str, left_width: usize, right_width: usize) -> String {
    let left = fit_line_to_width(left, left_width);
    let right = fit_line_to_width(right, right_width);
    let left_padding = left_width.saturating_sub(display_width(&left) as usize);
    let right_padding = right_width.saturating_sub(display_width(&right) as usize);
    format!(
        "| {left}{} | {right}{} |",
        " ".repeat(left_padding),
        " ".repeat(right_padding)
    )
}

fn card_divider(left_width: usize, right_width: usize) -> String {
    format!(
        "|{}+{}|",
        "-".repeat(left_width + 2),
        "-".repeat(right_width + 2)
    )
}

fn result_header(label: &str, count: usize) -> String {
    let noun = if count == 1 { "track" } else { "tracks" };
    format!("{label}: {count} {noun}")
}

struct StatusSnapshot<'a> {
    track_count: u64,
    result_count: usize,
    result_label: &'a str,
    result_query: &'a str,
    has_more_results: bool,
    default_device: &'a str,
    playback_title: Option<&'a str>,
    current_title: Option<&'a str>,
    current_result: Option<String>,
    database_path: &'a str,
    language: ShellLanguage,
}

fn status_lines(snapshot: StatusSnapshot<'_>) -> Vec<String> {
    match snapshot.language {
        ShellLanguage::English => english_status_lines(snapshot),
        ShellLanguage::Chinese => chinese_status_lines(snapshot),
    }
}

fn started_playback_lines(title: &str, language: ShellLanguage) -> Vec<String> {
    match language {
        ShellLanguage::English => vec![
            format!("started {title}"),
            "controls: pause resume stop next prev quit".to_string(),
        ],
        ShellLanguage::Chinese => vec![
            format!("开始播放 {title}"),
            "控制: 暂停 继续 停止 下一首 上一首 退出".to_string(),
        ],
    }
}

fn already_playing_lines(title: &str, language: ShellLanguage) -> Vec<String> {
    match language {
        ShellLanguage::English => vec![
            format!("already playing {title}"),
            "use stop before starting another track".to_string(),
        ],
        ShellLanguage::Chinese => vec![
            format!("正在播放 {title}"),
            "先输入 停止，再开始另一首。也可以用 播放 下一首 切歌。".to_string(),
        ],
    }
}

fn stopping_timeout_line(language: ShellLanguage) -> &'static str {
    match language {
        ShellLanguage::English => "previous track is still stopping; trying the next one anyway",
        ShellLanguage::Chinese => "上一首还在停止中；会继续尝试切到下一首",
    }
}

fn nothing_playing_line(language: ShellLanguage) -> &'static str {
    match language {
        ShellLanguage::English => "nothing is playing",
        ShellLanguage::Chinese => "现在没有在播放",
    }
}

fn nothing_paused_line(language: ShellLanguage) -> &'static str {
    match language {
        ShellLanguage::English => "nothing is paused",
        ShellLanguage::Chinese => "现在没有暂停的播放",
    }
}

fn stopping_line(title: &str, language: ShellLanguage) -> String {
    match language {
        ShellLanguage::English => format!("stopping {title}"),
        ShellLanguage::Chinese => format!("正在停止 {title}"),
    }
}

fn english_status_lines(snapshot: StatusSnapshot<'_>) -> Vec<String> {
    let mut lines = vec![
        format!("tracks       {}", snapshot.track_count),
        format!("results      {}", snapshot.result_count),
        format!(
            "view         {}",
            result_view_label(snapshot.result_label, snapshot.result_query)
        ),
        format!(
            "window       {}",
            result_window_label(snapshot.result_count, snapshot.has_more_results)
        ),
    ];
    if !snapshot.result_query.trim().is_empty() {
        lines.push(format!("query        {}", snapshot.result_query));
    }
    lines.extend([
        format!("device       {}", snapshot.default_device),
        format!("playback     {}", snapshot.playback_title.unwrap_or("idle")),
        format!("current      {}", snapshot.current_title.unwrap_or("idle")),
        format!(
            "result       {}",
            snapshot
                .current_result
                .unwrap_or_else(|| "not in current results".to_string())
        ),
        format!("database     {}", snapshot.database_path),
    ]);
    lines
}

fn chinese_status_lines(snapshot: StatusSnapshot<'_>) -> Vec<String> {
    let mut lines = vec![
        format!("歌曲        {}", snapshot.track_count),
        format!("结果        {}", snapshot.result_count),
        format!(
            "视图        {}",
            localized_result_view_label(
                snapshot.result_label,
                snapshot.result_query,
                snapshot.language
            )
        ),
        format!(
            "窗口        {}",
            localized_result_window_label(
                snapshot.result_count,
                snapshot.has_more_results,
                snapshot.language
            )
        ),
    ];
    if !snapshot.result_query.trim().is_empty() {
        lines.push(format!("关键词      {}", snapshot.result_query));
    }
    lines.extend([
        format!("设备        {}", snapshot.default_device),
        format!("播放        {}", snapshot.playback_title.unwrap_or("空闲")),
        format!("当前        {}", snapshot.current_title.unwrap_or("空闲")),
        format!(
            "结果位置    {}",
            snapshot
                .current_result
                .unwrap_or_else(|| "不在当前结果中".to_string())
        ),
        format!("数据库      {}", snapshot.database_path),
    ]);
    lines
}

fn result_view_label(label: &str, query: &str) -> String {
    if query.trim().is_empty() {
        label.to_string()
    } else {
        format!("search {query}")
    }
}

fn localized_result_view_label(label: &str, query: &str, language: ShellLanguage) -> String {
    match language {
        ShellLanguage::English => result_view_label(label, query),
        ShellLanguage::Chinese if !query.trim().is_empty() => format!("搜索 {query}"),
        ShellLanguage::Chinese => match label {
            "library" => "曲库".to_string(),
            "results" => "结果".to_string(),
            other => other.to_string(),
        },
    }
}

fn result_window_label(count: usize, has_more: bool) -> String {
    if has_more {
        format!("{count}+ visible")
    } else {
        format!("{count} visible")
    }
}

fn localized_result_window_label(count: usize, has_more: bool, language: ShellLanguage) -> String {
    match language {
        ShellLanguage::English => result_window_label(count, has_more),
        ShellLanguage::Chinese if has_more => format!("{count}+ 可见"),
        ShellLanguage::Chinese => format!("{count} 可见"),
    }
}

fn scan_started_line(folder: &Path, language: ShellLanguage) -> String {
    match language {
        ShellLanguage::English => format!("scan {}", folder.display()),
        ShellLanguage::Chinese => format!("扫描 {}", folder.display()),
    }
}

fn scan_canceled_line(language: ShellLanguage) -> &'static str {
    match language {
        ShellLanguage::English => "scan canceled",
        ShellLanguage::Chinese => "已取消扫描",
    }
}

fn scan_summary_line(summary: &scanner::ScanSummary, language: ShellLanguage) -> String {
    scan_summary_line_parts(
        summary.indexed_tracks,
        summary.scanned_files,
        summary.skipped_unchanged,
        summary.failed_files,
        summary.removed_missing,
        summary.elapsed_ms,
        language,
    )
}

fn scan_summary_line_parts(
    indexed_tracks: usize,
    scanned_files: usize,
    skipped_unchanged: usize,
    failed_files: usize,
    removed_missing: usize,
    elapsed_ms: u128,
    language: ShellLanguage,
) -> String {
    match language {
        ShellLanguage::English => format!(
            "indexed {indexed_tracks} | scanned {scanned_files} | skipped {skipped_unchanged} | failed {failed_files} | removed {removed_missing} | {elapsed_ms} ms"
        ),
        ShellLanguage::Chinese => format!(
            "已入库 {indexed_tracks} | 已扫描 {scanned_files} | 已跳过 {skipped_unchanged} | 失败 {failed_files} | 已移除 {removed_missing} | {elapsed_ms} ms"
        ),
    }
}

fn scan_failure_hint(failed_files: usize, language: ShellLanguage) -> Option<String> {
    match (failed_files, language) {
        (0, _) => None,
        (1, ShellLanguage::English) => Some("1 file failed; type errors to inspect it".to_string()),
        (count, ShellLanguage::English) => {
            Some(format!("{count} files failed; type errors to inspect them"))
        }
        (1, ShellLanguage::Chinese) => Some("1 个文件失败；输入 错误 查看".to_string()),
        (count, ShellLanguage::Chinese) => Some(format!("{count} 个文件失败；输入 错误 查看")),
    }
}

fn scan_empty_next_steps(language: ShellLanguage) -> &'static str {
    match language {
        ShellLanguage::English => "next: scan another folder, or type help",
        ShellLanguage::Chinese => "下一步: 再扫描一个文件夹，或输入 帮助",
    }
}

fn scan_next_steps(language: ShellLanguage) -> &'static str {
    match language {
        ShellLanguage::English => "next: play 1, shuffle, next, prev, play best, or search <query>",
        ShellLanguage::Chinese => "下一步: 播放 1、随机、下一首、上一首，或 搜索 <关键词>",
    }
}

#[cfg(test)]
fn result_line(index: usize, track: &Track, current_track: Option<&Track>) -> String {
    result_line_for_width(index, track, current_track, 132)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResultLineLayout {
    total_width: usize,
    title_width: usize,
    artist_width: usize,
    path_width: Option<usize>,
}

fn result_line_layout(terminal_width: u16, prefix_width: usize) -> ResultLineLayout {
    let total_width = usize::from(terminal_width.max(24));
    if total_width < 72 {
        let separator_width = 2;
        let content_width = total_width
            .saturating_sub(prefix_width + separator_width)
            .max(8);
        let title_width = (content_width * 2 / 3).clamp(8, content_width.saturating_sub(1));
        let artist_width = content_width.saturating_sub(title_width);
        return ResultLineLayout {
            total_width,
            title_width,
            artist_width,
            path_width: None,
        };
    }

    let separator_width = 4;
    let content_width = total_width
        .saturating_sub(prefix_width + separator_width)
        .max(24);
    let path_width = (content_width * 38 / 100).clamp(18, 56);
    let artist_width = (content_width * 20 / 100).clamp(12, 26);
    let title_width = content_width
        .saturating_sub(path_width + artist_width)
        .max(12);

    ResultLineLayout {
        total_width,
        title_width,
        artist_width,
        path_width: Some(path_width),
    }
}

fn result_table_header_for_width(terminal_width: u16) -> String {
    let prefix = " #   ";
    let layout = result_line_layout(terminal_width, usize::from(display_width(prefix)));
    let title = padded("title", layout.title_width as u16);
    let artist = compact("artist", layout.artist_width);
    let line = if let Some(path_width) = layout.path_width {
        format!(
            "{prefix}{title}  {}  {}",
            padded(&artist, layout.artist_width as u16),
            compact("path", path_width)
        )
    } else {
        format!("{prefix}{title}  {artist}")
    };
    fit_line_to_width(&line, layout.total_width)
}

fn result_line_for_width(
    index: usize,
    track: &Track,
    current_track: Option<&Track>,
    terminal_width: u16,
) -> String {
    let marker = if is_current_track(track, current_track) {
        ">"
    } else {
        " "
    };
    let prefix = format!("{marker}{index:>2}. ");
    let artist = track.artist.as_deref().unwrap_or("unknown artist");
    let layout = result_line_layout(terminal_width, usize::from(display_width(&prefix)));
    let title = padded(
        &compact(&track.title, layout.title_width),
        layout.title_width as u16,
    );

    let line = if let Some(path_width) = layout.path_width {
        let artist = padded(
            &compact(artist, layout.artist_width),
            layout.artist_width as u16,
        );
        format!(
            "{prefix}{title}  {artist}  {}",
            compact_path(&track.path, path_width)
        )
    } else {
        format!("{prefix}{title}  {}", compact(artist, layout.artist_width))
    };
    fit_line_to_width(&line, layout.total_width)
}

fn current_result_label(results: &[Track], current_track: Option<&Track>) -> Option<String> {
    let index = current_result_index(results, current_track)?;
    Some(format!("#{}", index + 1))
}

fn current_result_index(results: &[Track], current_track: Option<&Track>) -> Option<usize> {
    current_track.and_then(|current| {
        results
            .iter()
            .position(|track| is_current_track(track, Some(current)))
    })
}

fn is_current_track(track: &Track, current_track: Option<&Track>) -> bool {
    current_track.is_some_and(|current| current.path == track.path)
}

fn search_usage_lines(language: ShellLanguage) -> Vec<String> {
    match language {
        ShellLanguage::English => vec![
            "usage: search <query>".to_string(),
            "tip: you can also type keywords directly, like moon halo".to_string(),
            "try: library, scan, or help search".to_string(),
        ],
        ShellLanguage::Chinese => vec![
            "用法: 搜索 <关键词>".to_string(),
            "提示: 也可以直接输入关键词，比如 moon halo".to_string(),
            "可以试试: 曲库、扫描，或 帮助 搜索".to_string(),
        ],
    }
}

fn no_results_yet_lines(language: ShellLanguage) -> Vec<String> {
    match language {
        ShellLanguage::English => vec![
            "no visible results yet".to_string(),
            "try: library, search <query>, or scan".to_string(),
            "tip: after results appear, use play 1, 1, shuffle, next, or prev".to_string(),
        ],
        ShellLanguage::Chinese => vec![
            "现在还没有可用结果".to_string(),
            "可以试试: 曲库、搜索 <关键词>，或 扫描".to_string(),
            "有结果后可以直接输入: 播放 1、1、随机、下一首、上一首".to_string(),
        ],
    }
}

fn no_result_index_lines(index: usize, count: usize, language: ShellLanguage) -> Vec<String> {
    match language {
        ShellLanguage::English => {
            let mut lines = vec![format!("no result #{index}")];
            if count == 0 {
                lines.extend(no_results_yet_lines(language));
            } else {
                lines.push(format!("visible results are 1..{count}"));
                lines.push("try: play 1, results, more, or search <query>".to_string());
            }
            lines
        }
        ShellLanguage::Chinese => {
            let mut lines = vec![format!("没有第 {index} 个结果")];
            if count == 0 {
                lines.extend(no_results_yet_lines(language));
            } else {
                lines.push(format!("当前可见结果是 1..{count}"));
                lines.push("可以试试: 播放 1、结果、更多，或 搜索 <关键词>".to_string());
            }
            lines
        }
    }
}

fn nothing_to_inspect_lines(language: ShellLanguage) -> Vec<String> {
    match language {
        ShellLanguage::English => vec![
            "nothing to inspect yet".to_string(),
            "try: results, info 1, search <query>, or play".to_string(),
        ],
        ShellLanguage::Chinese => vec![
            "现在还没有可以查看的歌曲".to_string(),
            "可以试试: 结果、信息 1、搜索 <关键词>，或 播放".to_string(),
        ],
    }
}

fn nothing_to_open_lines(language: ShellLanguage) -> Vec<String> {
    match language {
        ShellLanguage::English => vec![
            "nothing to open yet".to_string(),
            "try: library, open 1, search <query>, or play".to_string(),
        ],
        ShellLanguage::Chinese => vec![
            "现在还没有可以打开的歌曲".to_string(),
            "可以试试: 曲库、打开 1、搜索 <关键词>，或 播放".to_string(),
        ],
    }
}

fn nothing_to_copy_lines(language: ShellLanguage) -> Vec<String> {
    match language {
        ShellLanguage::English => vec![
            "nothing to copy yet".to_string(),
            "try: library, copy 1, search <query>, or play".to_string(),
        ],
        ShellLanguage::Chinese => vec![
            "现在还没有可以复制的歌曲路径".to_string(),
            "可以试试: 曲库、复制 1、搜索 <关键词>，或 播放".to_string(),
        ],
    }
}

fn search_no_matches_lines(query: &str, language: ShellLanguage) -> Vec<String> {
    match language {
        ShellLanguage::English => vec![
            format!("{query}: no matches"),
            "try: fewer words, library, scan, or another keyword".to_string(),
        ],
        ShellLanguage::Chinese => vec![
            format!("{query}: 没有匹配"),
            "可以试试: 更少的关键词、曲库、扫描，或换个关键词".to_string(),
        ],
    }
}

fn result_next_steps(count: usize, language: ShellLanguage) -> String {
    match language {
        ShellLanguage::English if count == 0 => {
            "next: type another keyword, list, results, or scan".to_string()
        }
        ShellLanguage::English => format!(
            "next: 1, play, play 1..{count}, shuffle, info, results, more, next, prev, open, copy, or search"
        ),
        ShellLanguage::Chinese if count == 0 => {
            "下一步: 输入另一个关键词、曲库、结果，或 扫描".to_string()
        }
        ShellLanguage::Chinese => format!(
            "下一步: 1、播放、播放 1..{count}、随机、信息、结果、更多、下一首、上一首、打开、复制，或 搜索"
        ),
    }
}

fn relative_result_index(
    results: &[Track],
    current_track: Option<&Track>,
    step: isize,
) -> Option<usize> {
    if results.is_empty() {
        return None;
    }

    let current_index = current_result_index(results, current_track);
    let len = results.len() as isize;
    let index = match current_index {
        Some(index) => (index as isize + step).rem_euclid(len),
        None if step < 0 => len - 1,
        None => 0,
    };
    Some(index as usize)
}

fn shuffle_result_index(
    results: &[Track],
    current_track: Option<&Track>,
    seed: u64,
) -> Option<usize> {
    if results.is_empty() {
        return None;
    }

    let mut index = (seed as usize) % results.len();
    if results.len() > 1 && Some(index) == current_result_index(results, current_track) {
        index = (index + 1) % results.len();
    }
    Some(index)
}

fn random_seed() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or_default()
}

fn is_repeat_command(command: &str) -> bool {
    matches!(
        command
            .trim_start_matches([':', '/'])
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "again" | "repeat" | "!!"
    )
}

fn is_result_index_prefix(input: &str) -> bool {
    let value = input.trim();
    if value.is_empty() {
        return false;
    }

    let digits = value.strip_prefix('#').unwrap_or(value);
    !digits.is_empty() && digits.chars().all(|character| character.is_ascii_digit())
}

fn parse_result_index_input(input: &str) -> Option<usize> {
    let value = input.trim();
    let digits = value.strip_prefix('#').unwrap_or(value);
    if digits.is_empty() || !digits.chars().all(|character| character.is_ascii_digit()) {
        return None;
    }

    digits.parse::<usize>().ok().filter(|index| *index > 0)
}

fn open_track_in_explorer(track: &Track) -> Result<()> {
    let path = explorer_select_path(&track.path);
    Command::new("explorer")
        .arg(format!("/select,{path}"))
        .spawn()?;
    Ok(())
}

fn copy_text_to_clipboard(value: &str) -> Result<()> {
    let mut child = Command::new("powershell")
        .args([
            "-NoProfile",
            "-Command",
            "[Console]::InputEncoding=[System.Text.Encoding]::UTF8; Set-Clipboard -Value ([Console]::In.ReadToEnd())",
        ])
        .stdin(Stdio::piped())
        .spawn()?;

    if let Some(stdin) = child.stdin.as_mut() {
        stdin.write_all(value.as_bytes())?;
    }

    let status = child.wait()?;
    if !status.success() {
        return Err(EchoError::Playback(
            "failed to copy path to clipboard".to_string(),
        ));
    }

    Ok(())
}

fn explorer_select_path(path: &str) -> String {
    path.strip_prefix(r"\\?\").unwrap_or(path).to_string()
}

fn display_width(value: &str) -> u16 {
    value.chars().map(display_char_width).sum::<u16>()
}

fn padded(value: &str, width: u16) -> String {
    let value_width = display_width(value);
    if value_width >= width {
        value.to_string()
    } else {
        format!("{}{}", value, " ".repeat((width - value_width) as usize))
    }
}

fn terminal_error(error: io::Error) -> EchoError {
    EchoError::Playback(error.to_string())
}

fn load_language(paths: &AppPaths) -> ShellLanguage {
    let path = paths.config_dir.join(LANGUAGE_FILE);
    let Ok(value) = std::fs::read_to_string(path) else {
        return ShellLanguage::English;
    };
    parse_language(value.trim()).unwrap_or(ShellLanguage::English)
}

fn save_language(paths: &AppPaths, language: ShellLanguage) -> Result<()> {
    std::fs::create_dir_all(&paths.config_dir)?;
    std::fs::write(paths.config_dir.join(LANGUAGE_FILE), language.code())?;
    Ok(())
}

fn parse_language(value: &str) -> Option<ShellLanguage> {
    match value.trim().to_ascii_lowercase().as_str() {
        "en" | "eng" | "english" | "英文" | "英语" => Some(ShellLanguage::English),
        "zh" | "cn" | "chi" | "chinese" | "中文" | "简体中文" | "汉语" => {
            Some(ShellLanguage::Chinese)
        }
        _ => None,
    }
}

fn is_language_status_argument(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "list" | "status" | "current" | "show" | "?" | "列表" | "状态" | "当前" | "查看"
    )
}

fn pick_scan_folder() -> Result<Option<PathBuf>> {
    let script = r#"
Add-Type -AssemblyName System.Windows.Forms
$dialog = New-Object System.Windows.Forms.FolderBrowserDialog
$dialog.Description = 'Choose a music folder for ECHO CLI'
$dialog.ShowNewFolderButton = $false
if ($dialog.ShowDialog() -eq [System.Windows.Forms.DialogResult]::OK) {
    [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
    Write-Output $dialog.SelectedPath
}
"#;

    let output = Command::new("powershell")
        .args(["-NoProfile", "-STA", "-Command", script])
        .output()?;

    if !output.status.success() {
        return Err(EchoError::Playback(
            String::from_utf8_lossy(&output.stderr).trim().to_string(),
        ));
    }

    let selected = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if selected.is_empty() {
        Ok(None)
    } else {
        Ok(Some(PathBuf::from(selected)))
    }
}

fn print_lines(lines: Vec<String>) {
    for line in lines {
        println!("{line}");
    }
}

fn print_track_info(label: &str, track: &Track) {
    println!("{label}:");
    println!("  title    {}", track.title);
    println!(
        "  artist   {}",
        track.artist.as_deref().unwrap_or("unknown artist")
    );
    println!(
        "  album    {}",
        track.album.as_deref().unwrap_or("unknown album")
    );
    if let Some(album_artist) = &track.album_artist {
        println!("  album by {}", album_artist);
    }
    if track.track_number.is_some() || track.disc_number.is_some() {
        println!(
            "  number   {}",
            format_track_number(track.disc_number, track.track_number)
        );
    }
    println!(
        "  length   {}",
        track
            .duration_ms
            .map(format_duration)
            .unwrap_or_else(|| "unknown".to_string())
    );
    println!("  format   {}", format_track_technical_summary(track));
    println!("  size     {}", format_size(track.size_bytes));
    println!("  path     {}", explorer_select_path(&track.path));
}

fn playback_event_lines(event: &PlaybackEvent, language: ShellLanguage) -> Vec<String> {
    match language {
        ShellLanguage::English => match event {
            PlaybackEvent::Loading { title, path } => vec![
                format!("playing {title}"),
                format!("source  {path}"),
                "status  decoding".to_string(),
            ],
            PlaybackEvent::Playing { stream, output, .. } => vec![
                format!("output  {} / {}", output.device_name, output.mode),
                format!(
                    "format  {} Hz / {}ch / {}",
                    output.sample_rate, output.channel_count, output.sample_format
                ),
                format!(
                    "source  {} Hz / {}ch",
                    stream.sample_rate, stream.channel_count
                ),
                "status  playing".to_string(),
                "next: pause, stop, shuffle, next, prev, now, or search <query>".to_string(),
            ],
            PlaybackEvent::Warning(message) => vec![format!("warning {message}")],
            PlaybackEvent::Paused { title } => vec![
                format!("paused {title}"),
                "next: resume, stop, shuffle, next, prev, or quit".to_string(),
            ],
            PlaybackEvent::Resumed { title } => vec![format!("resumed {title}")],
            PlaybackEvent::Stopped { title, elapsed_ms } => {
                vec![format!("stopped {title} after {elapsed_ms} ms")]
            }
            PlaybackEvent::Finished { elapsed_ms, .. } => vec![
                format!("finished in {elapsed_ms} ms"),
                "next: play, search <query>, or library".to_string(),
            ],
            PlaybackEvent::Error { message, .. } => {
                vec![
                    format!("error {message}"),
                    "next: doctor or devices".to_string(),
                ]
            }
        },
        ShellLanguage::Chinese => match event {
            PlaybackEvent::Loading { title, path } => vec![
                format!("正在播放 {title}"),
                format!("来源  {path}"),
                "状态  解码中".to_string(),
            ],
            PlaybackEvent::Playing { stream, output, .. } => vec![
                format!("输出  {} / {}", output.device_name, output.mode),
                format!(
                    "格式  {} Hz / {}ch / {}",
                    output.sample_rate, output.channel_count, output.sample_format
                ),
                format!(
                    "来源  {} Hz / {}ch",
                    stream.sample_rate, stream.channel_count
                ),
                "状态  播放中".to_string(),
                "下一步: 暂停、停止、随机、下一首、上一首、当前，或 搜索 <关键词>".to_string(),
            ],
            PlaybackEvent::Warning(message) => vec![format!("警告 {message}")],
            PlaybackEvent::Paused { title } => vec![
                format!("已暂停 {title}"),
                "下一步: 继续、停止、随机、下一首、上一首，或 退出".to_string(),
            ],
            PlaybackEvent::Resumed { title } => vec![format!("已继续 {title}")],
            PlaybackEvent::Stopped { title, elapsed_ms } => {
                vec![format!("已停止 {title}，用时 {elapsed_ms} ms")]
            }
            PlaybackEvent::Finished { elapsed_ms, .. } => vec![
                format!("播放结束，用时 {elapsed_ms} ms"),
                "下一步: 播放、搜索 <关键词>，或 曲库".to_string(),
            ],
            PlaybackEvent::Error { message, .. } => {
                vec![
                    format!("错误 {message}"),
                    "下一步: 诊断 或 设备".to_string(),
                ]
            }
        },
    }
}

fn shortcut_lines(language: ShellLanguage) -> Vec<String> {
    if language == ShellLanguage::Chinese {
        return vec![
            "快捷键:".to_string(),
            "  上/下            选择候选；空输入时浏览历史".to_string(),
            "  Tab              补全当前候选".to_string(),
            "  Enter            执行完整命令或接受候选".to_string(),
            "  左/右            移动光标".to_string(),
            "  Ctrl+左/右       按词跳转".to_string(),
            "  Ctrl+W           删除前一个词".to_string(),
            "  Ctrl+K           删除到行尾".to_string(),
            "  Ctrl+U           清空光标前内容".to_string(),
            "  Ctrl+L           清屏".to_string(),
            "  Ctrl+C           退出 shell".to_string(),
            String::new(),
            "播放控制: 暂停、继续、停止、下一首、上一首、随机、当前".to_string(),
        ];
    }

    vec![
        "shortcuts:".to_string(),
        "  Up/Down          select suggestions; browse history on an empty prompt".to_string(),
        "  Tab              complete the selected suggestion".to_string(),
        "  Enter            run complete commands or accept selected suggestions".to_string(),
        "  Left/Right       move the cursor".to_string(),
        "  Ctrl+Left/Right  jump by word".to_string(),
        "  Ctrl+W           delete the previous word".to_string(),
        "  Ctrl+K           delete to the end of the line".to_string(),
        "  Ctrl+U           clear before the cursor".to_string(),
        "  Ctrl+L           clear the screen".to_string(),
        "  Ctrl+C           exit the shell".to_string(),
        String::new(),
        "playback: pause, resume, stop, next, prev, shuffle, now".to_string(),
    ]
}

fn alias_lines(language: ShellLanguage) -> Vec<String> {
    if language == ShellLanguage::Chinese {
        return vec![
            "别名:".to_string(),
            "  帮助        help, h, ?, commands, /help".to_string(),
            "  语言        language, lang, /language".to_string(),
            "  曲库        library, list, recent, ls, songs, tracks".to_string(),
            "  搜索        search, find, 也可以直接输入关键词".to_string(),
            "  播放        play, 播放 1, 播放 下一首, 播放 随机".to_string(),
            "  下一首      next, play next".to_string(),
            "  上一首      prev, previous, play prev".to_string(),
            "  随机        shuffle, random, surprise".to_string(),
            "  当前        now, current, playing".to_string(),
            "  打开        open, reveal, folder, where".to_string(),
            "  设备        devices, output, outputs".to_string(),
            "  诊断        doctor, health, check".to_string(),
            "  快捷键      shortcuts, keys".to_string(),
            "  退出        q, quit, exit, /quit".to_string(),
            String::new(),
            "中英文命令可以混用，照你顺手的来。".to_string(),
        ];
    }

    vec![
        "aliases:".to_string(),
        "  help        h, ?, commands, /help".to_string(),
        "  帮助        help, commands, ?".to_string(),
        "  library     list, recent, ls, songs, tracks".to_string(),
        "  曲库        library, list, songs, tracks".to_string(),
        "  search      find, bare keywords".to_string(),
        "  搜索        search, find".to_string(),
        "  play        1, #1, play best, play first, play last".to_string(),
        "  播放        play, 播放 1, 播放 下一首, 播放 随机".to_string(),
        "  play next   next, play prev, prev, previous".to_string(),
        "  shuffle     random, surprise, play random, play surprise".to_string(),
        "  随机        shuffle, random, surprise".to_string(),
        "  now         current, playing".to_string(),
        "  当前        now, current, playing".to_string(),
        "  open        reveal, folder, where".to_string(),
        "  打开        open, reveal, folder, where".to_string(),
        "  devices     device, output, outputs".to_string(),
        "  设备        devices, outputs".to_string(),
        "  doctor      health, diagnose, diagnostics, check".to_string(),
        "  诊断        doctor, health, check".to_string(),
        "  again       repeat, !!".to_string(),
        "  clear       cls".to_string(),
        "  quit        q, exit, /quit".to_string(),
        "  shortcuts   keys, /shortcuts".to_string(),
        String::new(),
        "Type any alias exactly like a normal command.".to_string(),
    ]
}

fn localized_help_lines(topic: &str, language: ShellLanguage) -> Vec<String> {
    match language {
        ShellLanguage::English => help_lines(topic),
        ShellLanguage::Chinese => chinese_help_lines(topic),
    }
}

fn help_lines(topic: &str) -> Vec<String> {
    let topic = topic.trim().trim_start_matches('/').to_ascii_lowercase();
    match topic.as_str() {
        "" => vec![
            "commands:".to_string(),
            "  scan              open a Windows folder picker and scan".to_string(),
            "  add               same as scan".to_string(),
            "  scan add          same as scan".to_string(),
            "  scan <folder>     scan a folder path directly".to_string(),
            "  search <query>    search title, artist, album, filename, path".to_string(),
            "  library           show recent indexed tracks".to_string(),
            "  list              same as library".to_string(),
            "  results           print current search/list results again".to_string(),
            "  more              show more current search/list results".to_string(),
            "  play <n>          play numbered result".to_string(),
            "  1                 play result #1 directly".to_string(),
            "  play              play result #1".to_string(),
            "  shuffle           play a random visible result".to_string(),
            "  surprise          pick something for me".to_string(),
            "  info <n>          show details for numbered result".to_string(),
            "  open              show current/result #1 in Explorer".to_string(),
            "  copy              copy current/result #1 path".to_string(),
            "  again             repeat the last command".to_string(),
            "  pause             pause current playback".to_string(),
            "  resume            resume current playback".to_string(),
            "  stop              stop current playback".to_string(),
            "  next              play next visible result".to_string(),
            "  prev              play previous visible result".to_string(),
            "  tips              show what to do next".to_string(),
            "  home              show the welcome screen".to_string(),
            "  shortcuts         show keyboard shortcuts".to_string(),
            "  aliases           show alternate command names".to_string(),
            "  history           show recent commands".to_string(),
            "  now               show current track".to_string(),
            "  status            show shell status".to_string(),
            "  devices           list output devices".to_string(),
            "  doctor            print diagnostics".to_string(),
            "  errors            show recent scan failures".to_string(),
            "  open-db           open the database folder".to_string(),
            "  clear             clear the screen".to_string(),
            "  /help             slash commands also work".to_string(),
            "  help <command>    explain one command".to_string(),
            "  commands          same as help".to_string(),
            "  ?                 same as help".to_string(),
            "  quit              exit".to_string(),
            String::new(),
            "Type a prefix to list matches. Up/Down selects. Left/Right edits. Ctrl+Left/Right jumps words.".to_string(),
            "History is saved between sessions; use Up/Down when the prompt is empty.".to_string(),
        ],
        "scan" | "add" => vec![
            "help: scan".to_string(),
            "  scan              choose a music folder with a Windows picker".to_string(),
            "  add               same as scan".to_string(),
            "  scan add          same as scan".to_string(),
            "  scan D:\\Music     scan a folder path directly".to_string(),
            "  errors            show recent files that failed during scanning".to_string(),
            String::new(),
            "After scanning, use play, shuffle, surprise, next, or prev to choose from results."
                .to_string(),
        ],
        "search" | "find" | "搜索" | "找" => vec![
            "help: search".to_string(),
            "  search moon       search indexed tracks".to_string(),
            "  find moon         same as search".to_string(),
            "  search            pick from visible track titles".to_string(),
            "  moon halo         bare text also searches the library".to_string(),
            "  results           print the current search results again".to_string(),
            "  more              show more current search results".to_string(),
            "  library           reset to recent indexed tracks".to_string(),
            "  list              same as library".to_string(),
            String::new(),
            "Tip: after search, type play, info, open, or copy and pick a visible result."
                .to_string(),
        ],
        "library" | "list" | "recent" | "ls" | "songs" | "tracks" | "曲库" | "列表"
        | "歌曲" | "results" | "r" | "结果" => vec![
            "help: library".to_string(),
            "  library           show recent indexed tracks".to_string(),
            "  list              same as library".to_string(),
            "  recent            same as library".to_string(),
            "  ls                same as library".to_string(),
            "  songs             same as library".to_string(),
            "  tracks            same as library".to_string(),
            "  results           print current results without resetting them".to_string(),
            "  r                 same as results".to_string(),
            "  more              show more current results".to_string(),
            String::new(),
            "After listing, type 1, play <pick>, shuffle, surprise, info <pick>, next, prev, open <pick>, or copy <pick>.".to_string(),
        ],
        "play" | "播放" | "shuffle" | "random" | "surprise" | "随机" | "随便" => vec![
            "help: play".to_string(),
            "  play              play result #1".to_string(),
            "  7                 play result #7 directly".to_string(),
            "  #7                same as 7".to_string(),
            "  play #7           same as play 7".to_string(),
            "  play <pick>       pick from current results".to_string(),
            "  play 7            play result #7".to_string(),
            "  play best         play first listed result".to_string(),
            "  play first        same as play best".to_string(),
            "  play last         play last listed result".to_string(),
            "  play next         play next visible result".to_string(),
            "  play prev         play previous visible result".to_string(),
            "  play random       play a random visible result".to_string(),
            "  play surprise     pick something for me".to_string(),
            "  shuffle           same as play random".to_string(),
            "  surprise          same as play surprise".to_string(),
            "  random            same as shuffle".to_string(),
            "  play <query>      search and play best match".to_string(),
            "  next              play next visible result".to_string(),
            "  prev              play previous visible result".to_string(),
            String::new(),
            "During playback, the prompt stays usable: pause, resume, stop, next, prev, quit."
                .to_string(),
        ],
        "open" | "打开" | "reveal" | "folder" | "where" | "位置" => vec![
            "help: open".to_string(),
            "  open              show current track, or result #1, in Explorer".to_string(),
            "  open current      show current track in Explorer".to_string(),
            "  open 7            show result #7 in Explorer".to_string(),
            "  open <query>      search and show best match in Explorer".to_string(),
            "  reveal <pick>     same as open".to_string(),
            "  folder            show current track in Explorer".to_string(),
            "  where             same as folder".to_string(),
        ],
        "copy" | "复制" => vec![
            "help: copy".to_string(),
            "  copy              copy current track, or result #1, to clipboard".to_string(),
            "  copy current      copy current track path".to_string(),
            "  copy 7            copy result #7 path".to_string(),
            "  copy <query>      search and copy best match path".to_string(),
        ],
        "info" | "i" | "信息" | "详情" | "now" | "current" | "playing" | "当前"
        | "正在播放" => vec![
            "help: info".to_string(),
            "  now               show current track details".to_string(),
            "  current           same as now".to_string(),
            "  playing           same as now".to_string(),
            "  info              show current track, or result #1, details".to_string(),
            "  info current      show current track details".to_string(),
            "  info 7            show result #7 details".to_string(),
            "  info <query>      search and show best match details".to_string(),
        ],
        "again" | "repeat" | "!!" => vec![
            "help: again".to_string(),
            "  again             repeat the last non-again command".to_string(),
            "  repeat            same as again".to_string(),
            "  !!                same as again".to_string(),
        ],
        "history" => vec![
            "help: history".to_string(),
            "  history           show recent saved commands".to_string(),
            "  history 50        show the last 50 saved commands".to_string(),
            "  !7                replay history entry #7".to_string(),
            "  history clear     clear saved command history".to_string(),
        ],
        "language" | "lang" | "语言" => vec![
            "help: language".to_string(),
            "  language          toggle English / 中文".to_string(),
            "  language zh       switch to 中文".to_string(),
            "  language en       switch to English".to_string(),
            "  language status   show current language".to_string(),
            "  language list     show available languages".to_string(),
            "  /language         slash form also works".to_string(),
        ],
        "errors" => vec![
            "help: errors".to_string(),
            "  errors            show recent files that failed during scanning".to_string(),
            String::new(),
            "Use this after scan reports failed files.".to_string(),
        ],
        "pause" | "暂停" | "resume" | "继续" | "stop" | "停止" | "playback" => vec![
            "help: playback".to_string(),
            "  pause             pause current playback".to_string(),
            "  resume            resume playback".to_string(),
            "  stop              stop playback".to_string(),
            "  next              stop current track and play next visible result".to_string(),
            "  prev              stop current track and play previous visible result".to_string(),
            "  shuffle           stop current track and play a random visible result".to_string(),
            "  surprise          stop current track and pick something for me".to_string(),
            "  now               show current track details".to_string(),
            String::new(),
            "The prompt changes to echo playing> while music is active.".to_string(),
        ],
        "devices" | "device" | "output" | "outputs" | "doctor" | "diagnose" | "diagnostics"
        | "health" | "check" | "status" | "open-db" | "设备" | "输出" | "诊断" | "检查"
        | "状态" => vec![
            "help: diagnostics".to_string(),
            "  status            show library, results, device, and playback state".to_string(),
            "  devices           list output devices".to_string(),
            "  device            same as devices".to_string(),
            "  outputs           same as devices".to_string(),
            "  doctor            print runtime and audio backend diagnostics".to_string(),
            "  diagnose          same as doctor".to_string(),
            "  health            same as doctor".to_string(),
            "  open-db           open the database folder in Explorer".to_string(),
        ],
        "clear" | "cls" => vec![
            "help: clear".to_string(),
            "  clear             clear the terminal and show the welcome screen".to_string(),
            "  cls               same as clear".to_string(),
        ],
        "quit" | "q" | "exit" => vec![
            "help: quit".to_string(),
            "  quit              stop playback and exit".to_string(),
            "  q                 same as quit".to_string(),
            "  exit              same as quit".to_string(),
            "  /quit             slash form also works".to_string(),
        ],
        "next" | "下一首" | "prev" | "previous" | "上一首" => vec![
            "help: next".to_string(),
            "  next              play next visible result".to_string(),
            "  prev              play previous visible result".to_string(),
            "  previous          same as prev".to_string(),
            String::new(),
            "If nothing is playing, next starts result #1 and prev starts the last result."
                .to_string(),
        ],
        "tips" | "提示" | "下一步" => vec![
            "help: tips".to_string(),
            "  tips              show the most useful commands for the current state".to_string(),
            String::new(),
            "Empty Enter does the same thing, so the shell never leaves you stranded.".to_string(),
        ],
        "home" | "首页" => vec![
            "help: home".to_string(),
            "  home              show the welcome screen and current library view".to_string(),
            "  clear             clear the terminal and then show the welcome screen".to_string(),
            String::new(),
            "Use this when you feel lost and want the shell to re-orient you.".to_string(),
        ],
        "shortcuts" | "keys" | "快捷键" => shortcut_lines(ShellLanguage::English),
        "aliases" | "alias" | "别名" => alias_lines(ShellLanguage::English),
        _ => {
            let suggestions = nearest_command_suggestions(&topic);
            if suggestions.is_empty() {
                vec![
                    format!("no help topic for: {topic}"),
                    "try: help play, help search, help scan, or help devices".to_string(),
                ]
            } else {
                let mut lines = vec![
                    format!("no help topic for: {topic}"),
                    "did you mean:".to_string(),
                ];
                lines.extend(
                    suggestions
                        .into_iter()
                        .map(|suggestion| format!("  help {}", suggestion.completion.trim())),
                );
                lines
            }
        }
    }
}

fn chinese_help_lines(topic: &str) -> Vec<String> {
    let topic = topic.trim().trim_start_matches('/').to_ascii_lowercase();
    match topic.as_str() {
        "" => vec![
            "命令:".to_string(),
            "  扫描              选择音乐文件夹并扫描".to_string(),
            "  扫描 D:\\Music     直接扫描文件夹路径".to_string(),
            "  搜索 <关键词>      搜索曲名、艺人、专辑、文件名、路径".to_string(),
            "  曲库              显示最近入库歌曲".to_string(),
            "  播放              播放第 1 个结果".to_string(),
            "  播放 7            播放第 7 个结果".to_string(),
            "  播放 #7           同 play #7".to_string(),
            "  播放 下一首       播放当前列表下一首".to_string(),
            "  播放 上一首       播放当前列表上一首".to_string(),
            "  随机              随机播放当前可见结果".to_string(),
            "  暂停 / 继续 / 停止 控制当前播放".to_string(),
            "  当前              查看正在播放".to_string(),
            "  打开 1            在 Explorer 中定位结果".to_string(),
            "  复制 1            复制结果路径".to_string(),
            "  更多              展开更多结果".to_string(),
            "  状态              查看 shell 状态".to_string(),
            "  设备              查看输出设备".to_string(),
            "  诊断              查看运行诊断".to_string(),
            "  语言              切换 English / 中文".to_string(),
            "  快捷键            查看键盘操作".to_string(),
            "  别名              查看中英文别名".to_string(),
            "  退出              离开 shell".to_string(),
            String::new(),
            "输入前缀会显示候选；上下键选择，Tab 补全，Enter 执行。".to_string(),
        ],
        "language" | "lang" | "语言" => vec![
            "帮助: 语言".to_string(),
            "  语言              在 English / 中文 之间切换".to_string(),
            "  语言 zh           切换到中文".to_string(),
            "  language en       switch to English".to_string(),
            "  语言 状态         查看当前语言".to_string(),
            "  语言 列表         查看可用语言".to_string(),
            "  /language         slash 命令也可以用".to_string(),
        ],
        "play" | "播放" | "shuffle" | "random" | "surprise" | "随机" | "随便" => vec![
            "帮助: 播放".to_string(),
            "  播放              播放第 1 个结果".to_string(),
            "  播放 7            播放第 7 个结果".to_string(),
            "  播放 #7           同 play #7".to_string(),
            "  播放 下一首       播放当前列表下一首".to_string(),
            "  播放 上一首       播放当前列表上一首".to_string(),
            "  播放 随机         随机播放当前可见结果".to_string(),
            "  随机              同 play random".to_string(),
            "  播放 <关键词>     搜索并播放最佳匹配".to_string(),
        ],
        "search" | "find" | "搜索" | "找" => vec![
            "帮助: 搜索".to_string(),
            "  搜索 moon         搜索已入库歌曲".to_string(),
            "  找 moon           同 搜索".to_string(),
            "  moon halo         直接输入关键词也会搜索".to_string(),
            "  结果              重新显示当前结果".to_string(),
            "  更多              显示更多结果".to_string(),
        ],
        "scan" | "扫描" | "add" | "添加" => vec![
            "帮助: 扫描".to_string(),
            "  扫描              打开 Windows 文件夹选择框".to_string(),
            "  添加              同 扫描".to_string(),
            "  扫描 D:\\Music     直接扫描路径".to_string(),
            "  错误              查看扫描失败文件".to_string(),
        ],
        "shortcuts" | "keys" | "快捷键" => shortcut_lines(ShellLanguage::Chinese),
        "aliases" | "alias" | "别名" => alias_lines(ShellLanguage::Chinese),
        _ => help_lines(&topic),
    }
}

fn help_topic_suggestions(normalized_input: &str) -> Vec<ShellSuggestion> {
    let (completion_prefix, query) = if let Some(query) = normalized_input.strip_prefix("/help ") {
        ("/help ", query.trim())
    } else if let Some(query) = normalized_input.strip_prefix("帮助 ") {
        ("帮助 ", query.trim())
    } else {
        (
            "help ",
            normalized_input
                .strip_prefix("help ")
                .unwrap_or_default()
                .trim(),
        )
    };
    let topics = [
        ("play", "play tracks and choose current results"),
        ("播放", "播放歌曲和当前结果"),
        ("shuffle", "play a random visible result"),
        ("随机", "随机播放当前结果"),
        ("surprise", "pick something for me"),
        ("open", "show tracks in Explorer"),
        ("打开", "在 Explorer 中定位歌曲"),
        ("reveal", "same as open"),
        ("folder", "same as open"),
        ("where", "same as open"),
        ("copy", "copy track paths to clipboard"),
        ("复制", "复制歌曲路径"),
        ("again", "repeat the last command"),
        ("search", "find tracks and narrow visible results"),
        ("搜索", "搜索曲库"),
        ("find", "same as search"),
        ("scan", "add music folders to the library"),
        ("扫描", "添加音乐文件夹"),
        ("library", "show recent indexed tracks"),
        ("曲库", "显示最近歌曲"),
        ("list", "same as library"),
        ("songs", "same as library"),
        ("tracks", "same as library"),
        ("results", "print current results again"),
        ("more", "show more current results"),
        ("info", "show track details"),
        ("now", "show current track details"),
        ("current", "same as now"),
        ("history", "show and replay saved commands"),
        ("errors", "show recent scan failures"),
        ("playback", "pause, resume, stop, and inspect playback"),
        ("devices", "list output devices and diagnostics"),
        ("设备", "查看输出设备"),
        ("device", "same as devices"),
        ("outputs", "same as devices"),
        ("status", "show shell and database state"),
        ("状态", "查看 shell 状态"),
        ("doctor", "print diagnostics"),
        ("诊断", "查看诊断"),
        ("health", "same as doctor"),
        ("open-db", "open the database folder"),
        ("next", "play next visible result"),
        ("下一首", "播放下一首"),
        ("prev", "play previous visible result"),
        ("上一首", "播放上一首"),
        ("tips", "show suggested next steps"),
        ("提示", "显示下一步建议"),
        ("home", "show the welcome screen"),
        ("首页", "显示欢迎页"),
        ("clear", "clear the terminal"),
        ("shortcuts", "show keyboard shortcuts"),
        ("快捷键", "查看键盘操作"),
        ("aliases", "show alternate command names"),
        ("别名", "查看中英文别名"),
        ("alias", "same as aliases"),
        ("language", "switch English / 中文"),
        ("语言", "切换 English / 中文"),
        ("quit", "exit the shell"),
        ("退出", "退出 shell"),
        ("exit", "same as quit"),
    ];

    topics
        .into_iter()
        .filter(|(topic, _)| query.is_empty() || topic.starts_with(query))
        .map(|(topic, description)| ShellSuggestion {
            completion: format!("{completion_prefix}{topic}"),
            description: description.to_string(),
        })
        .collect()
}

fn compact(value: &str, width: usize) -> String {
    if display_width(value) as usize <= width {
        return value.to_string();
    }

    if width <= 3 {
        return ".".repeat(width);
    }

    let mut used_width = 0_usize;
    let mut prefix = String::new();
    for character in value.chars() {
        let character_width = display_char_width(character) as usize;
        if used_width + character_width > width - 3 {
            break;
        }
        prefix.push(character);
        used_width += character_width;
    }
    format!("{prefix}...")
}

fn compact_path(value: &str, width: usize) -> String {
    if display_width(value) as usize <= width {
        return value.to_string();
    }

    if width <= 3 {
        return ".".repeat(width);
    }

    let mut used_width = 0_usize;
    let mut tail = Vec::new();
    for character in value.chars().rev() {
        let character_width = display_char_width(character) as usize;
        if used_width + character_width > width - 3 {
            break;
        }
        tail.push(character);
        used_width += character_width;
    }
    tail.reverse();
    let tail: String = tail.into_iter().collect();
    format!("...{tail}")
}

fn fit_line_to_width(value: &str, width: usize) -> String {
    if display_width(value) as usize <= width {
        value.to_string()
    } else {
        compact(value, width)
    }
}

fn display_char_width(character: char) -> u16 {
    if character.is_ascii() { 1 } else { 2 }
}

fn format_duration(duration_ms: u64) -> String {
    let total_seconds = duration_ms / 1000;
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;
    if hours > 0 {
        format!("{hours}:{minutes:02}:{seconds:02}")
    } else {
        format!("{minutes}:{seconds:02}")
    }
}

fn format_track_number(disc_number: Option<u32>, track_number: Option<u32>) -> String {
    match (disc_number, track_number) {
        (Some(disc), Some(track)) => format!("{disc}.{track}"),
        (Some(disc), None) => format!("disc {disc}"),
        (None, Some(track)) => track.to_string(),
        (None, None) => "unknown".to_string(),
    }
}

fn format_track_technical_summary(track: &Track) -> String {
    let sample_rate = track
        .sample_rate
        .map(|value| format!("{value} Hz"))
        .unwrap_or_else(|| "unknown Hz".to_string());
    let channels = track
        .channel_count
        .map(|value| format!("{value}ch"))
        .unwrap_or_else(|| "unknown ch".to_string());
    let bit_depth = track
        .bit_depth
        .map(|value| format!("{value}-bit"))
        .unwrap_or_else(|| "unknown bit".to_string());
    format!("{sample_rate} / {channels} / {bit_depth}")
}

fn format_size(size_bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let size = size_bytes as f64;
    if size >= GIB {
        format!("{:.1} GiB", size / GIB)
    } else if size >= MIB {
        format!("{:.1} MiB", size / MIB)
    } else if size >= KIB {
        format!("{:.1} KiB", size / KIB)
    } else {
        format!("{size_bytes} B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_suggestions_filter_prefix() {
        let suggestions = command_suggestions("sc");

        assert_eq!(suggestions[0].completion, "scan");
        assert!(
            suggestions
                .iter()
                .any(|suggestion| suggestion.usage == "scan <folder>")
        );
    }

    #[test]
    fn command_suggestions_include_add_shortcut() {
        let suggestions = command_suggestions("a");

        assert!(
            suggestions
                .iter()
                .any(|suggestion| suggestion.completion == "add")
        );
    }

    #[test]
    fn command_suggestions_include_list_aliases() {
        let list_suggestions = command_suggestions("li");
        let recent_suggestions = command_suggestions("r");
        let song_suggestions = command_suggestions("so");
        let track_suggestions = command_suggestions("tr");

        assert!(
            list_suggestions
                .iter()
                .any(|suggestion| suggestion.completion == "list")
        );
        assert!(
            recent_suggestions
                .iter()
                .any(|suggestion| suggestion.completion == "recent")
        );
        assert!(
            song_suggestions
                .iter()
                .any(|suggestion| suggestion.completion == "songs")
        );
        assert!(
            track_suggestions
                .iter()
                .any(|suggestion| suggestion.completion == "tracks")
        );
    }

    #[test]
    fn command_suggestions_include_find_alias() {
        let suggestions = command_suggestions("fi");

        assert!(
            suggestions
                .iter()
                .any(|suggestion| suggestion.completion == "find ")
        );
    }

    #[test]
    fn command_suggestions_show_multiple_playback_matches() {
        let completions: Vec<_> = command_suggestions("p")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();

        assert!(completions.contains(&"play "));
        assert!(completions.contains(&"play last"));
        assert!(completions.contains(&"play first"));
        assert!(completions.contains(&"play next"));
        assert!(completions.contains(&"play prev"));
        assert!(completions.contains(&"pause"));
        assert!(completions.contains(&"playing"));
        assert!(completions.contains(&"play random"));
    }

    #[test]
    fn command_suggestions_show_multiple_uppercase_playback_matches() {
        let completions: Vec<_> = command_suggestions("P")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();

        assert!(completions.contains(&"play "));
        assert!(completions.contains(&"play surprise"));
        assert!(completions.contains(&"pause"));
        assert!(completions.contains(&"prev"));
    }

    #[test]
    fn command_suggestions_include_shuffle() {
        let completions: Vec<_> = command_suggestions("s")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();

        assert!(completions.contains(&"shuffle"));
        assert!(completions.contains(&"surprise"));
        assert!(completions.contains(&"search "));
    }

    #[test]
    fn command_suggestions_keep_slash_controls() {
        let suggestions = command_suggestions("/p");
        let completions: Vec<_> = suggestions
            .iter()
            .map(|suggestion| suggestion.completion)
            .collect();

        assert_eq!(suggestions[0].completion, "/pause");
        assert!(completions.contains(&"/play "));
        assert!(completions.contains(&"/play next"));
        assert!(completions.contains(&"/play prev"));
        assert!(completions.contains(&"/play random"));
    }

    #[test]
    fn command_suggestions_include_slash_command_center() {
        let home_completions: Vec<_> = command_suggestions("/h")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();
        let status_completions: Vec<_> = command_suggestions("/s")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();

        assert!(home_completions.contains(&"/help"));
        assert!(home_completions.contains(&"/home"));
        assert!(
            command_suggestions("/c")
                .iter()
                .any(|suggestion| suggestion.completion == "/current")
        );
        assert!(status_completions.contains(&"/status"));
        assert!(status_completions.contains(&"/stop"));
        assert!(status_completions.contains(&"/scan"));
        assert!(status_completions.contains(&"/search "));
        assert!(
            command_suggestions("/f")
                .iter()
                .any(|suggestion| suggestion.completion == "/find ")
        );
        assert!(
            command_suggestions("/l")
                .iter()
                .any(|suggestion| suggestion.completion == "/library")
        );
        assert!(
            command_suggestions("/so")
                .iter()
                .any(|suggestion| suggestion.completion == "/songs")
        );
        assert!(
            command_suggestions("/r")
                .iter()
                .any(|suggestion| suggestion.completion == "/results")
        );
        assert!(
            command_suggestions("/o")
                .iter()
                .any(|suggestion| suggestion.completion == "/open ")
        );
        assert!(
            command_suggestions("/re")
                .iter()
                .any(|suggestion| suggestion.completion == "/reveal ")
        );
        assert!(
            command_suggestions("/sh")
                .iter()
                .any(|suggestion| suggestion.completion == "/shortcuts")
        );
        assert!(
            command_suggestions("/he")
                .iter()
                .any(|suggestion| suggestion.completion == "/health")
        );
    }

    #[test]
    fn command_suggestions_include_next_step_help() {
        let next_completions: Vec<_> = command_suggestions("n")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();
        let prev_completions: Vec<_> = command_suggestions("pr")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();

        assert!(next_completions.contains(&"next"));
        assert!(prev_completions.contains(&"prev"));
        assert!(prev_completions.contains(&"previous"));
    }

    #[test]
    fn command_suggestions_include_shortcuts() {
        let shortcut_completions: Vec<_> = command_suggestions("sh")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();
        let key_completions: Vec<_> = command_suggestions("k")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();

        assert!(shortcut_completions.contains(&"shortcuts"));
        assert!(key_completions.contains(&"keys"));
    }

    #[test]
    fn command_suggestions_include_aliases() {
        let alias_completions: Vec<_> = command_suggestions("al")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();
        let slash_completions: Vec<_> = command_suggestions("/al")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();

        assert!(alias_completions.contains(&"aliases"));
        assert!(alias_completions.contains(&"alias"));
        assert!(slash_completions.contains(&"/aliases"));
    }

    #[test]
    fn command_suggestions_include_language_switching() {
        let english: Vec<_> = command_suggestions("lang")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();
        let slash: Vec<_> = command_suggestions("/lang")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();
        let chinese: Vec<_> = command_suggestions("语")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();

        assert!(english.contains(&"language"));
        assert!(english.contains(&"language zh"));
        assert!(english.contains(&"language status"));
        assert!(english.contains(&"language list"));
        assert!(slash.contains(&"/language"));
        assert!(slash.contains(&"/language zh"));
        assert!(slash.contains(&"/language status"));
        assert!(slash.contains(&"/language list"));
        assert!(chinese.contains(&"语言"));
        assert!(chinese.contains(&"语言 zh"));
        assert!(chinese.contains(&"语言 状态"));
        assert!(chinese.contains(&"语言 列表"));
    }

    #[test]
    fn command_suggestions_include_chinese_aliases() {
        let play: Vec<_> = command_suggestions("播")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();
        let search: Vec<_> = command_suggestions("搜")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();
        let help: Vec<_> = command_suggestions("帮")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();

        assert!(play.contains(&"播放 "));
        assert!(play.contains(&"播放 下一首"));
        assert!(search.contains(&"搜索 "));
        assert!(help.contains(&"帮助"));
    }

    #[test]
    fn command_suggestions_localize_descriptions() {
        let context =
            ShellSuggestionContext::from_tracks_with_language(&[], ShellLanguage::Chinese);
        let suggestions = context.suggestions("播");

        assert!(
            suggestions
                .iter()
                .any(|suggestion| suggestion.completion == "播放 "
                    && suggestion.description.contains("编号播放"))
        );
    }

    #[test]
    fn unknown_command_lines_are_localized() {
        let suggestions = nearest_command_suggestions("ply");
        let english = unknown_command_lines(
            "ply",
            "ply moon",
            suggestions.clone(),
            ShellLanguage::English,
        );
        let chinese = unknown_command_lines("ply", "ply moon", suggestions, ShellLanguage::Chinese);

        assert!(english.iter().any(|line| line.contains("unknown command")));
        assert!(english.iter().any(|line| line.contains("did you mean")));
        assert!(
            english
                .iter()
                .any(|line| line.contains("or type search ply moon"))
        );
        assert!(chinese.iter().any(|line| line.contains("未知命令")));
        assert!(chinese.iter().any(|line| line.contains("你是不是想用")));
        assert!(
            chinese
                .iter()
                .any(|line| line.contains("或者输入 搜索 ply moon"))
        );
    }

    #[test]
    fn searching_line_is_localized() {
        assert_eq!(
            searching_line("moon", ShellLanguage::English),
            "searching moon"
        );
        assert_eq!(
            searching_line("moon", ShellLanguage::Chinese),
            "正在搜索 moon"
        );
    }

    #[test]
    fn command_suggestions_include_diagnostic_aliases() {
        let device_completions: Vec<_> = command_suggestions("dev")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();
        let output_completions: Vec<_> = command_suggestions("out")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();
        let health_completions: Vec<_> = command_suggestions("he")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();
        let diagnose_completions: Vec<_> = command_suggestions("diag")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();

        assert!(device_completions.contains(&"devices"));
        assert!(device_completions.contains(&"device"));
        assert!(output_completions.contains(&"outputs"));
        assert!(output_completions.contains(&"output"));
        assert!(health_completions.contains(&"health"));
        assert!(diagnose_completions.contains(&"diagnose"));
        assert!(diagnose_completions.contains(&"diagnostics"));
    }

    #[test]
    fn command_suggestions_include_open_and_again() {
        let open_completions: Vec<_> = command_suggestions("o")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();
        let reveal_completions: Vec<_> = command_suggestions("re")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();
        let folder_completions: Vec<_> = command_suggestions("fo")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();
        let where_completions: Vec<_> = command_suggestions("wh")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();
        let again_completions: Vec<_> = command_suggestions("a")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();

        assert!(open_completions.contains(&"open "));
        assert!(reveal_completions.contains(&"reveal "));
        assert!(folder_completions.contains(&"folder"));
        assert!(where_completions.contains(&"where"));
        assert!(again_completions.contains(&"again"));
    }

    #[test]
    fn command_suggestions_include_copy_path() {
        let completions: Vec<_> = command_suggestions("c")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();

        assert!(completions.contains(&"copy "));
        assert!(completions.contains(&"copy current"));
        assert!(completions.contains(&"clear"));
        assert!(completions.contains(&"commands"));
        assert!(completions.contains(&"current"));
    }

    #[test]
    fn command_suggestions_include_results_and_info() {
        let result_completions: Vec<_> = command_suggestions("r")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();
        let info_completions: Vec<_> = command_suggestions("i")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();

        assert!(result_completions.contains(&"results"));
        assert!(result_completions.contains(&"r"));
        assert!(info_completions.contains(&"info "));
        assert!(info_completions.contains(&"info current"));
    }

    #[test]
    fn command_suggestions_include_more() {
        let completions: Vec<_> = command_suggestions("m")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();

        assert!(completions.contains(&"more"));
    }

    #[test]
    fn command_suggestions_include_history() {
        let completions: Vec<_> = command_suggestions("h")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();

        assert!(completions.contains(&"history"));
        assert!(completions.contains(&"history clear"));
        assert!(completions.contains(&"help"));
        assert!(completions.contains(&"home"));
    }

    #[test]
    fn command_suggestions_include_shell_control_aliases() {
        let quit_completions: Vec<_> = command_suggestions("q")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();
        let exit_completions: Vec<_> = command_suggestions("ex")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();
        let clear_completions: Vec<_> = command_suggestions("cl")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();
        let repeat_completions: Vec<_> = command_suggestions("re")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();

        assert!(quit_completions.contains(&"quit"));
        assert!(quit_completions.contains(&"q"));
        assert!(exit_completions.contains(&"exit"));
        assert!(clear_completions.contains(&"clear"));
        assert!(clear_completions.contains(&"cls"));
        assert!(repeat_completions.contains(&"repeat"));
    }

    #[test]
    fn command_suggestions_include_question_mark_help() {
        let suggestions = command_suggestions("?");

        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].completion, "?");
        assert_eq!(suggestions[0].description, "same as help");
    }

    #[test]
    fn history_entries_are_deduped_trimmed_and_limited() {
        let mut history =
            parse_history_entries(" scan D:\\Music \nscan D:\\Music\n\nsearch moon\n");

        assert_eq!(history, vec!["scan D:\\Music", "search moon"]);
        assert!(!push_history_entry(&mut history, "search moon"));
        assert!(push_history_entry(&mut history, "play 1"));
        assert_eq!(history.last().map(String::as_str), Some("play 1"));

        for index in 0..(HISTORY_LIMIT + 5) {
            push_history_entry(&mut history, &format!("search {index}"));
        }
        assert_eq!(history.len(), HISTORY_LIMIT);
        assert_eq!(
            serialize_history_entries(&history).lines().count(),
            HISTORY_LIMIT
        );
    }

    #[test]
    fn history_skips_transient_control_commands() {
        assert!(!is_history_worthy("quit"));
        assert!(!is_history_worthy("/exit"));
        assert!(!is_history_worthy(":clear"));
        assert!(!is_history_worthy("history"));
        assert!(!is_history_worthy("history clear"));
        assert!(!is_history_worthy("again"));
        assert!(is_history_worthy("search moon"));
        assert!(is_history_worthy("play 1"));
    }

    #[test]
    fn history_replay_index_accepts_bang_numbers() {
        assert_eq!(history_replay_index("!1"), Some(1));
        assert_eq!(history_replay_index("!20"), Some(20));
        assert_eq!(history_replay_index("!!"), None);
        assert_eq!(history_replay_index("!"), None);
        assert_eq!(history_replay_index("history"), None);
    }

    #[test]
    fn result_window_tracks_whether_more_rows_exist() {
        let mut database = Database::open_memory().unwrap();
        database
            .upsert_tracks(&[
                test_track("A", "Artist"),
                test_track("B", "Artist"),
                test_track("C", "Artist"),
            ])
            .unwrap();

        let (two_tracks, has_more) = load_result_window(&database, "", 2).unwrap();
        assert_eq!(two_tracks.len(), 2);
        assert!(has_more);

        let (three_tracks, has_more) = load_result_window(&database, "", 3).unwrap();
        assert_eq!(three_tracks.len(), 3);
        assert!(!has_more);
    }

    #[test]
    fn shell_reader_replays_numbered_history_entries() {
        let mut reader = ShellReader::new();
        assert!(reader.add_history("scan D:\\Music"));
        assert!(reader.add_history("search moon"));

        assert_eq!(
            reader.replay_history_command("!1"),
            Some("scan D:\\Music".to_string())
        );
        assert_eq!(
            reader.replay_history_command("!2"),
            Some("search moon".to_string())
        );
        assert_eq!(reader.replay_history_command("!3"), None);
    }

    #[test]
    fn accepted_prefix_suggestions_wait_for_more_input() {
        let play_prefix = ShellSuggestion {
            completion: "play ".to_string(),
            description: "play by query or result number".to_string(),
        };
        let pause_command = ShellSuggestion {
            completion: "pause".to_string(),
            description: "pause playback".to_string(),
        };

        assert!(accepted_suggestion_needs_more_input(&play_prefix));
        assert!(!accepted_suggestion_needs_more_input(&pause_command));
    }

    #[test]
    fn enter_runs_complete_command_instead_of_adding_trailing_space() {
        let play_prefix = ShellSuggestion {
            completion: "play ".to_string(),
            description: "play by query or result number".to_string(),
        };

        assert!(should_accept_suggestion_on_enter("pla", &play_prefix));
        assert!(!should_accept_suggestion_on_enter("play", &play_prefix));
        assert!(!should_accept_suggestion_on_enter("play ", &play_prefix));
    }

    #[test]
    fn suggestion_footer_explains_selection_and_hidden_matches() {
        assert_eq!(suggestion_footer_line(0, 0, ShellLanguage::English), None);
        assert_eq!(
            suggestion_footer_line(3, 3, ShellLanguage::English),
            Some("Up/Down select | Tab accept | Enter accept/run".to_string())
        );
        assert_eq!(
            suggestion_footer_line(11, 8, ShellLanguage::English),
            Some("Up/Down select | Tab accept | Enter accept/run | +3 more".to_string())
        );
        assert_eq!(
            suggestion_footer_line(11, 8, ShellLanguage::Chinese),
            Some("上/下 选择 | Tab 补全 | Enter 接受/执行 | 还有 3 个".to_string())
        );
    }

    #[test]
    fn empty_prompt_suggestions_follow_shell_state() {
        let no_tracks = ShellSuggestionContext::new(&[], ShellLanguage::English, false, 0);
        let no_track_completions: Vec<_> = no_tracks
            .suggestions("")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();
        assert_eq!(
            no_track_completions.first().map(String::as_str),
            Some("scan")
        );
        assert!(no_track_completions.contains(&"devices".to_string()));

        let tracks = [test_track("Moon Halo", "Mili")];
        let ready = ShellSuggestionContext::new(&tracks, ShellLanguage::English, false, 1);
        let ready_completions: Vec<_> = ready
            .suggestions("")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();
        assert_eq!(ready_completions.first().map(String::as_str), Some("play"));
        assert!(ready_completions.contains(&"1".to_string()));

        let playing = ShellSuggestionContext::new(&tracks, ShellLanguage::Chinese, true, 1);
        let playing_completions: Vec<_> = playing
            .suggestions("")
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();
        assert_eq!(
            playing_completions.first().map(String::as_str),
            Some("暂停")
        );
        assert!(playing_completions.contains(&"停止".to_string()));
        assert!(playing_completions.contains(&"下一首".to_string()));
    }

    #[test]
    fn empty_prompt_footer_keeps_history_hint() {
        assert_eq!(
            suggestion_footer_line_for_input(4, 4, ShellLanguage::English, true),
            Some("Tab accepts first | Enter shows tips | Up/Down history".to_string())
        );
        assert_eq!(
            suggestion_footer_line_for_input(4, 4, ShellLanguage::Chinese, true),
            Some("Tab 接受第一条 | Enter 显示下一步 | 上/下 历史".to_string())
        );
    }

    #[test]
    fn cursor_boundaries_are_utf8_safe() {
        let input = "a你b";

        assert_eq!(next_char_boundary(input, 0), 1);
        assert_eq!(next_char_boundary(input, 1), 4);
        assert_eq!(next_char_boundary(input, 4), 5);
        assert_eq!(previous_char_boundary(input, 5), 4);
        assert_eq!(previous_char_boundary(input, 4), 1);
        assert_eq!(previous_char_boundary(input, 1), 0);
    }

    #[test]
    fn cursor_editing_inserts_and_deletes_wide_characters() {
        let mut input = "ab".to_string();
        let mut cursor_index = 1;

        insert_char_at_cursor(&mut input, &mut cursor_index, '你');
        assert_eq!(input, "a你b");
        assert_eq!(cursor_index, 4);
        assert_eq!(display_width(&input[..cursor_index]), 3);

        assert!(remove_char_before_cursor(&mut input, &mut cursor_index));
        assert_eq!(input, "ab");
        assert_eq!(cursor_index, 1);

        assert!(remove_char_at_cursor(&mut input, &mut cursor_index));
        assert_eq!(input, "a");
        assert_eq!(cursor_index, 1);
        assert!(!remove_char_at_cursor(&mut input, &mut cursor_index));
    }

    #[test]
    fn word_boundaries_skip_whitespace_and_respect_utf8() {
        let input = "search 月光 halo";
        let end = input.len();
        let halo_start = "search 月光 ".len();
        let moon_start = "search ".len();

        assert_eq!(previous_word_boundary(input, end), halo_start);
        assert_eq!(previous_word_boundary(input, halo_start), moon_start);
        assert_eq!(next_word_boundary(input, 0), moon_start);
        assert_eq!(next_word_boundary(input, moon_start), halo_start);
    }

    #[test]
    fn word_editing_deletes_previous_word_and_tail() {
        let mut input = "search 月光 halo".to_string();
        let mut cursor_index = input.len();

        assert!(remove_word_before_cursor(&mut input, &mut cursor_index));
        assert_eq!(input, "search 月光 ");
        assert_eq!(cursor_index, "search 月光 ".len());

        assert!(remove_word_before_cursor(&mut input, &mut cursor_index));
        assert_eq!(input, "search ");
        assert_eq!(cursor_index, "search ".len());

        cursor_index = 0;
        assert!(remove_after_cursor(&mut input, &mut cursor_index));
        assert_eq!(input, "");
        assert_eq!(cursor_index, 0);
        assert!(!remove_after_cursor(&mut input, &mut cursor_index));
    }

    #[test]
    fn nearest_command_suggestions_help_with_typos() {
        let suggestions = nearest_command_suggestions("ply");

        assert!(
            suggestions
                .iter()
                .any(|suggestion| suggestion.completion == "play ")
        );
    }

    #[test]
    fn playback_event_lines_include_next_step() {
        let lines = playback_event_lines(
            &PlaybackEvent::Paused {
                title: "Song".to_string(),
            },
            ShellLanguage::English,
        );

        assert!(lines.iter().any(|line| line.contains("resume")));
        assert!(lines.iter().any(|line| line.contains("prev")));
    }

    #[test]
    fn playback_event_lines_are_localized() {
        let lines = playback_event_lines(
            &PlaybackEvent::Paused {
                title: "Song".to_string(),
            },
            ShellLanguage::Chinese,
        );

        assert!(lines.iter().any(|line| line.contains("已暂停 Song")));
        assert!(lines.iter().any(|line| line.contains("继续")));
        assert!(lines.iter().any(|line| line.contains("上一首")));
    }

    #[test]
    fn play_prefix_suggests_current_results() {
        let context = ShellSuggestionContext::from_tracks(&[
            test_track("Moon Halo", "Mili"),
            test_track("A Lonely Night", "The Weeknd"),
        ]);
        let suggestions = context.suggestions("play ");

        assert_eq!(suggestions[0].completion, "play 1");
        assert!(suggestions[0].description.contains("Moon Halo"));
        assert_eq!(suggestions[1].completion, "play Moon Halo");
        assert_eq!(suggestions[2].completion, "play 2");
    }

    #[test]
    fn slash_play_prefix_suggests_current_results() {
        let context = ShellSuggestionContext::from_tracks(&[
            test_track("Moon Halo", "Mili"),
            test_track("A Lonely Night", "The Weeknd"),
        ]);
        let suggestions = context.suggestions("/play ");

        assert_eq!(suggestions[0].completion, "/play 1");
        assert!(suggestions[0].description.contains("Moon Halo"));
        assert_eq!(suggestions[1].completion, "/play Moon Halo");
    }

    #[test]
    fn chinese_play_prefix_suggests_current_results() {
        let context = ShellSuggestionContext::from_tracks(&[
            test_track("Moon Halo", "Mili"),
            test_track("A Lonely Night", "The Weeknd"),
        ]);
        let suggestions = context.suggestions("播放 ");

        assert_eq!(suggestions[0].completion, "播放 1");
        assert!(suggestions[0].description.contains("Moon Halo"));
        assert_eq!(suggestions[1].completion, "播放 Moon Halo");
    }

    #[test]
    fn chinese_play_prefix_localizes_result_descriptions() {
        let context = ShellSuggestionContext::from_tracks_with_language(
            &[
                test_track("Moon Halo", "Mili"),
                test_track("A Lonely Night", "The Weeknd"),
            ],
            ShellLanguage::Chinese,
        );
        let suggestions = context.suggestions("播放 moon");

        assert_eq!(suggestions[0].completion, "播放 1");
        assert_eq!(suggestions[1].completion, "播放 Moon Halo");
        assert!(suggestions[1].description.contains("结果 #1"));
    }

    #[test]
    fn play_prefix_filters_result_titles() {
        let context = ShellSuggestionContext::from_tracks(&[
            test_track("Moon Halo", "Mili"),
            test_track("A Lonely Night", "The Weeknd"),
        ]);
        let suggestions = context.suggestions("play lonely");

        assert_eq!(suggestions.len(), 2);
        assert_eq!(suggestions[0].completion, "play 2");
        assert_eq!(suggestions[1].completion, "play A Lonely Night");
    }

    #[test]
    fn open_prefix_suggests_current_results() {
        let context = ShellSuggestionContext::from_tracks(&[
            test_track("Moon Halo", "Mili"),
            test_track("A Lonely Night", "The Weeknd"),
        ]);
        let suggestions = context.suggestions("open ");

        assert_eq!(suggestions[0].completion, "open 1");
        assert!(suggestions[0].description.contains("Moon Halo"));
        assert_eq!(suggestions[1].completion, "open Moon Halo");
    }

    #[test]
    fn slash_result_commands_keep_slash_completion() {
        let context = ShellSuggestionContext::from_tracks(&[
            test_track("Moon Halo", "Mili"),
            test_track("A Lonely Night", "The Weeknd"),
        ]);

        assert_eq!(context.suggestions("/open ")[0].completion, "/open 1");
        assert_eq!(context.suggestions("/copy ")[0].completion, "/copy 1");
        assert_eq!(context.suggestions("/info ")[0].completion, "/info 1");
    }

    #[test]
    fn copy_prefix_suggests_current_results() {
        let context = ShellSuggestionContext::from_tracks(&[
            test_track("Moon Halo", "Mili"),
            test_track("A Lonely Night", "The Weeknd"),
        ]);
        let suggestions = context.suggestions("copy ");

        assert_eq!(suggestions[0].completion, "copy 1");
        assert!(suggestions[0].description.contains("Moon Halo"));
        assert_eq!(suggestions[1].completion, "copy Moon Halo");
    }

    #[test]
    fn info_prefix_suggests_current_results() {
        let context = ShellSuggestionContext::from_tracks(&[
            test_track("Moon Halo", "Mili"),
            test_track("A Lonely Night", "The Weeknd"),
        ]);
        let suggestions = context.suggestions("info ");

        assert_eq!(suggestions[0].completion, "info 1");
        assert!(suggestions[0].description.contains("Moon Halo"));
        assert_eq!(suggestions[1].completion, "info Moon Halo");
    }

    #[test]
    fn result_command_suggestions_include_title_completion() {
        let context = ShellSuggestionContext::from_tracks(&[
            test_track("Moon Halo", "Mili"),
            test_track("A Lonely Night", "The Weeknd"),
        ]);
        let suggestions = context.suggestions("play moon");

        assert_eq!(suggestions[0].completion, "play 1");
        assert_eq!(suggestions[1].completion, "play Moon Halo");
    }

    #[test]
    fn numeric_prefix_suggests_direct_play_result() {
        let context = ShellSuggestionContext::from_tracks(&[
            test_track("Moon Halo", "Mili"),
            test_track("A Lonely Night", "The Weeknd"),
        ]);
        let suggestions = context.suggestions("1");

        assert_eq!(suggestions[0].completion, "1");
        assert!(suggestions[0].description.contains("play Moon Halo"));
    }

    #[test]
    fn hash_numeric_prefix_suggests_direct_play_result() {
        let context = ShellSuggestionContext::from_tracks(&[
            test_track("Moon Halo", "Mili"),
            test_track("A Lonely Night", "The Weeknd"),
        ]);
        let suggestions = context.suggestions("#2");

        assert_eq!(suggestions[0].completion, "#2");
        assert!(suggestions[0].description.contains("A Lonely Night"));
    }

    #[test]
    fn search_prefix_suggests_current_result_titles() {
        let context = ShellSuggestionContext::from_tracks(&[
            test_track("Moon Halo", "Mili"),
            test_track("A Lonely Night", "The Weeknd"),
        ]);
        let suggestions = context.suggestions("search moon");

        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].completion, "search Moon Halo");
        assert!(suggestions[0].description.contains("result #1"));
    }

    #[test]
    fn slash_search_prefix_suggests_current_result_titles() {
        let context = ShellSuggestionContext::from_tracks(&[
            test_track("Moon Halo", "Mili"),
            test_track("A Lonely Night", "The Weeknd"),
        ]);
        let suggestions = context.suggestions("/search moon");

        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].completion, "/search Moon Halo");
    }

    #[test]
    fn chinese_search_prefix_suggests_current_result_titles() {
        let context = ShellSuggestionContext::from_tracks(&[
            test_track("Moon Halo", "Mili"),
            test_track("A Lonely Night", "The Weeknd"),
        ]);
        let suggestions = context.suggestions("搜索 moon");

        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].completion, "搜索 Moon Halo");
    }

    #[test]
    fn bare_text_suggests_matching_result_titles() {
        let context = ShellSuggestionContext::from_tracks(&[
            test_track("Moon Halo", "Mili"),
            test_track("A Lonely Night", "The Weeknd"),
        ]);
        let suggestions = context.suggestions("moon");

        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].completion, "Moon Halo");
        assert!(suggestions[0].description.contains("search title"));
    }

    #[test]
    fn bare_text_does_not_hide_command_typos() {
        let context = ShellSuggestionContext::from_tracks(&[test_track("Playground", "Artist")]);
        let suggestions = context.suggestions("pla");

        assert!(
            suggestions
                .iter()
                .any(|suggestion| suggestion.completion == "play ")
        );
    }

    #[test]
    fn shell_prompt_reflects_playback_state() {
        assert_eq!(prompt_for_playback(false), "echo ready> ");
        assert_eq!(prompt_for_playback(true), "echo playing> ");
    }

    #[test]
    fn welcome_card_keeps_stable_width_and_useful_commands() {
        let english = welcome_card_lines(12, "Speakers", ShellLanguage::English, 72);
        assert!(english.iter().all(|line| display_width(line) == 72));
        assert!(english.iter().any(|line| line.contains("Welcome back")));
        assert!(english.iter().any(|line| line.contains("scan or add")));
        assert!(english.iter().any(|line| line.contains("play 1")));
        assert!(english.iter().any(|line| line.contains("Empty Enter")));

        let chinese = welcome_card_lines(0, "Mi Monitor", ShellLanguage::Chinese, 72);
        assert!(chinese.iter().all(|line| display_width(line) == 72));
        assert!(chinese.iter().any(|line| line.contains("欢迎回来")));
        assert!(chinese.iter().any(|line| line.contains("扫描 或 添加")));
        assert!(chinese.iter().any(|line| line.contains("播放 1")));
        assert!(chinese.iter().any(|line| line.contains("空 Enter")));
    }

    #[test]
    fn result_header_and_next_steps_are_specific() {
        assert_eq!(result_header("library", 1), "library: 1 track");
        assert_eq!(result_header("library", 2), "library: 2 tracks");
        assert!(result_next_steps(0, ShellLanguage::English).contains("list"));
        assert!(result_next_steps(3, ShellLanguage::English).contains("play 1..3"));
        assert!(result_next_steps(3, ShellLanguage::English).contains("info"));
        assert!(result_next_steps(3, ShellLanguage::English).contains("results"));
        assert!(result_next_steps(3, ShellLanguage::English).contains("more"));
        assert!(result_next_steps(3, ShellLanguage::English).contains("next"));
        assert!(result_next_steps(3, ShellLanguage::English).contains("prev"));
        assert!(result_next_steps(3, ShellLanguage::English).contains("copy"));
        assert!(result_next_steps(0, ShellLanguage::Chinese).contains("扫描"));
        assert!(result_next_steps(3, ShellLanguage::Chinese).contains("播放 1..3"));
    }

    #[test]
    fn dead_end_messages_suggest_next_actions() {
        let search_usage = search_usage_lines(ShellLanguage::English);
        assert!(
            search_usage
                .iter()
                .any(|line| line.contains("search <query>"))
        );
        assert!(
            search_usage
                .iter()
                .any(|line| line.contains("keywords directly"))
        );

        let no_results = no_results_yet_lines(ShellLanguage::Chinese);
        assert!(no_results.iter().any(|line| line.contains("曲库")));
        assert!(no_results.iter().any(|line| line.contains("播放 1")));

        let no_index = no_result_index_lines(9, 3, ShellLanguage::Chinese);
        assert!(no_index.iter().any(|line| line.contains("1..3")));
        assert!(no_index.iter().any(|line| line.contains("更多")));

        let no_matches = search_no_matches_lines("moon", ShellLanguage::Chinese);
        assert!(no_matches.iter().any(|line| line.contains("没有匹配")));
        assert!(no_matches.iter().any(|line| line.contains("换个关键词")));

        assert!(
            nothing_to_open_lines(ShellLanguage::Chinese)
                .iter()
                .any(|line| line.contains("打开 1"))
        );
        assert!(
            nothing_to_copy_lines(ShellLanguage::Chinese)
                .iter()
                .any(|line| line.contains("复制 1"))
        );
        assert!(
            nothing_to_inspect_lines(ShellLanguage::Chinese)
                .iter()
                .any(|line| line.contains("信息 1"))
        );
    }

    #[test]
    fn result_status_labels_explain_view_and_window() {
        assert_eq!(result_view_label("library", ""), "library");
        assert_eq!(result_view_label("moon", "moon"), "search moon");
        assert_eq!(result_window_label(20, true), "20+ visible");
        assert_eq!(result_window_label(17, false), "17 visible");
        assert_eq!(
            localized_result_view_label("library", "", ShellLanguage::Chinese),
            "曲库"
        );
        assert_eq!(
            localized_result_view_label("library", "moon", ShellLanguage::Chinese),
            "搜索 moon"
        );
        assert_eq!(
            localized_result_window_label(20, true, ShellLanguage::Chinese),
            "20+ 可见"
        );
    }

    #[test]
    fn status_lines_are_localized() {
        let english = status_lines(StatusSnapshot {
            track_count: 12,
            result_count: 3,
            result_label: "library",
            result_query: "",
            has_more_results: true,
            default_device: "Speakers",
            playback_title: None,
            current_title: Some("Moon Halo"),
            current_result: Some("#1".to_string()),
            database_path: "G:\\ECHOCli\\echo.db",
            language: ShellLanguage::English,
        });
        let chinese = status_lines(StatusSnapshot {
            track_count: 12,
            result_count: 3,
            result_label: "library",
            result_query: "",
            has_more_results: true,
            default_device: "Speakers",
            playback_title: None,
            current_title: Some("Moon Halo"),
            current_result: Some("#1".to_string()),
            database_path: "G:\\ECHOCli\\echo.db",
            language: ShellLanguage::Chinese,
        });

        assert!(english.iter().any(|line| line == "tracks       12"));
        assert!(english.iter().any(|line| line == "playback     idle"));
        assert!(chinese.iter().any(|line| line == "歌曲        12"));
        assert!(chinese.iter().any(|line| line == "播放        空闲"));
        assert!(chinese.iter().any(|line| line == "视图        曲库"));
    }

    #[test]
    fn playback_control_lines_are_localized() {
        let started_en = started_playback_lines("Moon Halo", ShellLanguage::English);
        let started_zh = started_playback_lines("Moon Halo", ShellLanguage::Chinese);
        let busy_zh = already_playing_lines("Moon Halo", ShellLanguage::Chinese);

        assert!(started_en.iter().any(|line| line == "started Moon Halo"));
        assert!(started_zh.iter().any(|line| line == "开始播放 Moon Halo"));
        assert!(
            started_zh
                .iter()
                .any(|line| line.contains("暂停 继续 停止"))
        );
        assert!(busy_zh.iter().any(|line| line == "正在播放 Moon Halo"));
        assert_eq!(
            nothing_playing_line(ShellLanguage::Chinese),
            "现在没有在播放"
        );
        assert_eq!(
            nothing_paused_line(ShellLanguage::Chinese),
            "现在没有暂停的播放"
        );
        assert_eq!(
            stopping_line("Moon Halo", ShellLanguage::Chinese),
            "正在停止 Moon Halo"
        );
        assert!(stopping_timeout_line(ShellLanguage::Chinese).contains("上一首"));
    }

    #[test]
    fn scan_failure_hint_points_to_errors_command() {
        assert_eq!(scan_failure_hint(0, ShellLanguage::English), None);
        assert_eq!(
            scan_failure_hint(1, ShellLanguage::English),
            Some("1 file failed; type errors to inspect it".to_string())
        );
        assert_eq!(
            scan_failure_hint(3, ShellLanguage::English),
            Some("3 files failed; type errors to inspect them".to_string())
        );
    }

    #[test]
    fn scan_lines_are_localized() {
        let folder = PathBuf::from(r"D:\MusicRin");
        assert_eq!(
            scan_started_line(&folder, ShellLanguage::English),
            r"scan D:\MusicRin"
        );
        assert_eq!(
            scan_started_line(&folder, ShellLanguage::Chinese),
            r"扫描 D:\MusicRin"
        );
        assert_eq!(scan_canceled_line(ShellLanguage::Chinese), "已取消扫描");
        assert_eq!(
            scan_summary_line_parts(138, 140, 2, 1, 0, 65, ShellLanguage::Chinese),
            "已入库 138 | 已扫描 140 | 已跳过 2 | 失败 1 | 已移除 0 | 65 ms"
        );
        assert_eq!(
            scan_failure_hint(3, ShellLanguage::Chinese),
            Some("3 个文件失败；输入 错误 查看".to_string())
        );
        assert!(scan_next_steps(ShellLanguage::Chinese).contains("播放 1"));
        assert!(scan_empty_next_steps(ShellLanguage::Chinese).contains("帮助"));
    }

    #[test]
    fn result_line_marks_current_track() {
        let current = test_track("Moon Halo", "Mili");
        let line = result_line(1, &current, Some(&current));

        assert!(line.starts_with("> 1."));
        assert!(line.contains("Moon Halo"));
        assert!(line.contains("Mili"));
    }

    #[test]
    fn result_line_keeps_plain_marker_for_other_tracks() {
        let current = test_track("Moon Halo", "Mili");
        let other = test_track("A Lonely Night", "The Weeknd");
        let line = result_line(2, &other, Some(&current));

        assert!(line.starts_with("  2."));
        assert!(line.contains("A Lonely Night"));
    }

    #[test]
    fn result_line_fits_terminal_width() {
        let track = Track {
            id: None,
            title: "A Very Very Long Song Title That Used To Wrap In The Shell".to_string(),
            artist: Some("A Very Long Artist Name".to_string()),
            album: None,
            album_artist: None,
            track_number: None,
            disc_number: None,
            duration_ms: None,
            sample_rate: None,
            channel_count: None,
            bit_depth: None,
            path: r"\\?\D:\MusicRin\A Very Long Folder Name\A Very Long File Name.flac".to_string(),
            modified_unix: 0,
            size_bytes: 0,
        };

        let narrow = result_line_for_width(12, &track, None, 48);
        let wide = result_line_for_width(12, &track, None, 120);

        assert!(usize::from(display_width(&narrow)) <= 48);
        assert!(usize::from(display_width(&wide)) <= 120);
        assert!(wide.contains("..."));
        assert!(wide.contains("flac"));
    }

    #[test]
    fn result_table_header_matches_terminal_width() {
        let narrow = result_table_header_for_width(48);
        let wide = result_table_header_for_width(120);

        assert!(usize::from(display_width(&narrow)) <= 48);
        assert!(usize::from(display_width(&wide)) <= 120);
        assert!(narrow.contains("title"));
        assert!(narrow.contains("artist"));
        assert!(!narrow.contains("path"));
        assert!(wide.contains("path"));
    }

    #[test]
    fn current_result_label_reports_visible_position() {
        let tracks = vec![
            test_track("Moon Halo", "Mili"),
            test_track("A Lonely Night", "The Weeknd"),
        ];
        let outside = test_track("Outside", "Artist");

        assert_eq!(
            current_result_label(&tracks, Some(&tracks[1])),
            Some("#2".to_string())
        );
        assert_eq!(current_result_label(&tracks, Some(&outside)), None);
        assert_eq!(current_result_label(&tracks, None), None);
    }

    #[test]
    fn relative_result_index_moves_and_wraps_visible_results() {
        let tracks = vec![
            test_track("Moon Halo", "Mili"),
            test_track("A Lonely Night", "The Weeknd"),
            test_track("Minecraft", "C418"),
        ];

        assert_eq!(relative_result_index(&tracks, Some(&tracks[0]), 1), Some(1));
        assert_eq!(relative_result_index(&tracks, Some(&tracks[2]), 1), Some(0));
        assert_eq!(
            relative_result_index(&tracks, Some(&tracks[0]), -1),
            Some(2)
        );
    }

    #[test]
    fn relative_result_index_starts_from_edge_without_current_track() {
        let tracks = vec![
            test_track("Moon Halo", "Mili"),
            test_track("A Lonely Night", "The Weeknd"),
        ];
        let outside = test_track("Outside", "Artist");

        assert_eq!(relative_result_index(&tracks, None, 1), Some(0));
        assert_eq!(relative_result_index(&tracks, None, -1), Some(1));
        assert_eq!(relative_result_index(&tracks, Some(&outside), 1), Some(0));
        assert_eq!(relative_result_index(&tracks, Some(&outside), -1), Some(1));
        assert_eq!(relative_result_index(&[], None, 1), None);
    }

    #[test]
    fn shuffle_result_index_uses_seed_and_avoids_current_when_possible() {
        let tracks = vec![
            test_track("Moon Halo", "Mili"),
            test_track("A Lonely Night", "The Weeknd"),
            test_track("Minecraft", "C418"),
        ];

        assert_eq!(shuffle_result_index(&tracks, None, 4), Some(1));
        assert_eq!(shuffle_result_index(&tracks, Some(&tracks[1]), 4), Some(2));
        assert_eq!(
            shuffle_result_index(&tracks[..1], Some(&tracks[0]), 4),
            Some(0)
        );
        assert_eq!(shuffle_result_index(&[], None, 4), None);
    }

    #[test]
    fn help_topic_explains_play_command() {
        let lines = help_lines("play");

        assert!(lines.iter().any(|line| line.contains("play <pick>")));
        assert!(lines.iter().any(|line| line.contains("play #7")));
        assert!(lines.iter().any(|line| line.contains("play first")));
        assert!(lines.iter().any(|line| line.contains("play next")));
        assert!(lines.iter().any(|line| line.contains("play prev")));
        assert!(lines.iter().any(|line| line.contains("shuffle")));
        assert!(lines.iter().any(|line| line.contains("surprise")));
        assert!(lines.iter().any(|line| line.contains("pause")));
    }

    #[test]
    fn help_topic_explains_bare_search() {
        let lines = help_lines("search");

        assert!(lines.iter().any(|line| line.contains("bare text")));
        assert!(lines.iter().any(|line| line.contains("find moon")));
    }

    #[test]
    fn help_topic_explains_scan_shortcut() {
        let lines = help_lines("scan");

        assert!(lines.iter().any(|line| line.contains("  scan")));
        assert!(lines.iter().any(|line| line.contains("same as scan")));
    }

    #[test]
    fn help_topic_explains_library_aliases() {
        let lines = help_lines("list");

        assert!(lines.iter().any(|line| line.contains("recent")));
        assert!(lines.iter().any(|line| line.contains("songs")));
        assert!(lines.iter().any(|line| line.contains("tracks")));
        assert!(lines.iter().any(|line| line.contains("play <pick>")));
        assert!(lines.iter().any(|line| line.contains("results")));
        assert!(lines.iter().any(|line| line.contains("more")));
    }

    #[test]
    fn help_topic_suggestions_filter_prefix() {
        let suggestions = help_topic_suggestions("help p");
        let completions: Vec<_> = suggestions
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();

        assert!(completions.contains(&"help play".to_string()));
        assert!(completions.contains(&"help playback".to_string()));
    }

    #[test]
    fn chinese_help_topic_suggestions_filter_prefix() {
        let suggestions = help_topic_suggestions("帮助 播");
        let completions: Vec<_> = suggestions
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();

        assert!(completions.contains(&"帮助 播放".to_string()));
    }

    #[test]
    fn slash_help_topic_suggestions_keep_slash_completion() {
        let suggestions = help_topic_suggestions("/help p");
        let completions: Vec<_> = suggestions
            .into_iter()
            .map(|suggestion| suggestion.completion)
            .collect();

        assert!(completions.contains(&"/help play".to_string()));
        assert!(completions.contains(&"/help playback".to_string()));
    }

    #[test]
    fn localized_help_lines_support_chinese() {
        let root = localized_help_lines("", ShellLanguage::Chinese);
        let play = localized_help_lines("播放", ShellLanguage::Chinese);
        let language = localized_help_lines("语言", ShellLanguage::Chinese);
        let shortcuts = shortcut_lines(ShellLanguage::Chinese);
        let aliases = alias_lines(ShellLanguage::Chinese);

        assert!(root.iter().any(|line| line.contains("命令")));
        assert!(play.iter().any(|line| line.contains("播放 下一首")));
        assert!(language.iter().any(|line| line.contains("语言 zh")));
        assert!(language.iter().any(|line| line.contains("语言 状态")));
        assert!(language.iter().any(|line| line.contains("语言 列表")));
        assert!(shortcuts.iter().any(|line| line.contains("Ctrl+W")));
        assert!(aliases.iter().any(|line| line.contains("中英文命令")));
    }

    #[test]
    fn help_topic_explains_language_command() {
        let lines = help_lines("language");

        assert!(lines.iter().any(|line| line.contains("language zh")));
        assert!(lines.iter().any(|line| line.contains("language status")));
        assert!(lines.iter().any(|line| line.contains("language list")));
    }

    #[test]
    fn parse_language_accepts_supported_names() {
        assert_eq!(parse_language("zh"), Some(ShellLanguage::Chinese));
        assert_eq!(parse_language("中文"), Some(ShellLanguage::Chinese));
        assert_eq!(parse_language("en"), Some(ShellLanguage::English));
        assert_eq!(parse_language("English"), Some(ShellLanguage::English));
        assert_eq!(parse_language("jp"), None);
    }

    #[test]
    fn language_status_argument_accepts_status_words() {
        assert!(is_language_status_argument("status"));
        assert!(is_language_status_argument("list"));
        assert!(is_language_status_argument("current"));
        assert!(is_language_status_argument("状态"));
        assert!(is_language_status_argument("列表"));
        assert!(!is_language_status_argument("zh"));
    }

    #[test]
    fn help_topic_explains_next_and_tips_separately() {
        let next_lines = help_lines("next");
        let tips_lines = help_lines("tips");
        let home_lines = help_lines("home");
        let shortcut_lines = help_lines("shortcuts");

        assert!(next_lines.iter().any(|line| line.contains("prev")));
        assert!(next_lines.iter().any(|line| line.contains("result #1")));
        assert!(
            tips_lines
                .iter()
                .any(|line| line.contains("most useful commands"))
        );
        assert!(
            home_lines
                .iter()
                .any(|line| line.contains("welcome screen"))
        );
        assert!(shortcut_lines.iter().any(|line| line.contains("Ctrl+W")));
        assert!(shortcut_lines.iter().any(|line| line.contains("Tab")));
    }

    #[test]
    fn help_topic_explains_open_and_again() {
        let open_lines = help_lines("open");
        let again_lines = help_lines("again");

        assert!(open_lines.iter().any(|line| line.contains("open current")));
        assert!(open_lines.iter().any(|line| line.contains("reveal")));
        assert!(open_lines.iter().any(|line| line.contains("where")));
        assert!(
            again_lines
                .iter()
                .any(|line| line.contains("last non-again"))
        );
    }

    #[test]
    fn help_topic_explains_copy_command() {
        let lines = help_lines("copy");

        assert!(lines.iter().any(|line| line.contains("copy current")));
        assert!(lines.iter().any(|line| line.contains("clipboard")));
    }

    #[test]
    fn help_topic_explains_info_command() {
        let lines = help_lines("info");
        let current_lines = help_lines("current");

        assert!(lines.iter().any(|line| line.contains("info current")));
        assert!(lines.iter().any(|line| line.contains("info 7")));
        assert!(current_lines.iter().any(|line| line.contains("playing")));
    }

    #[test]
    fn help_topic_explains_diagnostic_aliases() {
        let lines = help_lines("health");

        assert!(lines.iter().any(|line| line.contains("device")));
        assert!(lines.iter().any(|line| line.contains("outputs")));
        assert!(lines.iter().any(|line| line.contains("diagnose")));
        assert!(lines.iter().any(|line| line.contains("health")));
    }

    #[test]
    fn help_topic_explains_aliases() {
        let lines = help_lines("aliases");

        assert!(lines.iter().any(|line| line.contains("library")));
        assert!(lines.iter().any(|line| line.contains("doctor")));
        assert!(lines.iter().any(|line| line.contains("shortcuts")));
        assert!(lines.iter().any(|line| line.contains("bare keywords")));
    }

    #[test]
    fn help_topic_explains_shell_control_commands() {
        let errors = help_lines("errors");
        let clear = help_lines("clear");
        let quit = help_lines("quit");
        let open_db = help_lines("open-db");

        assert!(errors.iter().any(|line| line.contains("failed")));
        assert!(clear.iter().any(|line| line.contains("cls")));
        assert!(quit.iter().any(|line| line.contains("exit")));
        assert!(open_db.iter().any(|line| line.contains("database folder")));
    }

    #[test]
    fn repeat_command_is_not_recorded_as_last_command() {
        assert!(is_repeat_command("again"));
        assert!(is_repeat_command("/repeat"));
        assert!(is_repeat_command("!!"));
        assert!(!is_repeat_command("play 1"));
    }

    #[test]
    fn parse_result_index_accepts_plain_and_hash_numbers() {
        assert_eq!(parse_result_index_input("1"), Some(1));
        assert_eq!(parse_result_index_input("#12"), Some(12));
        assert_eq!(parse_result_index_input("0"), None);
        assert_eq!(parse_result_index_input("play 1"), None);
    }

    #[test]
    fn explorer_select_path_removes_verbatim_prefix() {
        assert_eq!(
            explorer_select_path(r"\\?\D:\Music\Song.flac"),
            r"D:\Music\Song.flac"
        );
    }

    #[test]
    fn compact_path_keeps_tail() {
        assert_eq!(compact_path("C:/Music/Album/Song.flac", 12), "...Song.flac");
    }

    #[test]
    fn compact_respects_wide_character_display_width() {
        let value = compact("インターネット最高: NSO by ItsMeUltimate", 18);

        assert!(display_width(&value) <= 18);
        assert!(value.ends_with("..."));
    }

    #[test]
    fn compact_path_respects_wide_character_display_width() {
        let value = compact_path(
            r"D:\MusicRin\油兔凡云视听曲目\AViVA - BLAME IT ON THE KIDS.flac",
            24,
        );

        assert!(display_width(&value) <= 24);
        assert!(value.ends_with("KIDS.flac"));
    }

    #[test]
    fn track_detail_formatters_are_human_readable() {
        let mut track = test_track("Moon Halo", "Mili");
        track.duration_ms = Some(65_000);
        track.sample_rate = Some(44_100);
        track.channel_count = Some(2);
        track.bit_depth = Some(16);
        track.size_bytes = 5 * 1024 * 1024;

        assert_eq!(format_duration(track.duration_ms.unwrap()), "1:05");
        assert_eq!(format_track_number(Some(1), Some(7)), "1.7");
        assert_eq!(
            format_track_technical_summary(&track),
            "44100 Hz / 2ch / 16-bit"
        );
        assert_eq!(format_size(track.size_bytes), "5.0 MiB");
    }

    fn test_track(title: &str, artist: &str) -> Track {
        Track {
            id: None,
            title: title.to_string(),
            artist: Some(artist.to_string()),
            album: None,
            album_artist: None,
            track_number: None,
            disc_number: None,
            duration_ms: None,
            sample_rate: None,
            channel_count: None,
            bit_depth: None,
            path: format!("C:/Music/{title}.flac"),
            modified_unix: 0,
            size_bytes: 0,
        }
    }
}
