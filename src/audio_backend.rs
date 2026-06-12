pub fn backend_status_line() -> &'static str {
    if cfg!(windows) {
        "WASAPI planned; playback backend is not linked in Phase 1"
    } else {
        "Windows WASAPI target; this platform is for development only"
    }
}
