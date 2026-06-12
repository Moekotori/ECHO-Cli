mod app;
mod audio_backend;
mod audio_backend_wasapi;
mod commands;
mod config;
mod db;
mod decoder;
mod device;
mod error;
mod library;
mod metadata;
mod playback;
mod scanner;
mod search;
mod shell;

use clap::Parser;
use commands::Cli;
use error::Result;
use tracing_subscriber::EnvFilter;

fn main() {
    init_logging();

    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    commands::run(cli)
}

fn init_logging() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .without_time()
        .init();
}
