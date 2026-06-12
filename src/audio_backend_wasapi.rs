pub fn exclusive_status_line() -> &'static str {
    if cfg!(windows) {
        "planned, not enabled in Phase 1"
    } else {
        "unavailable on this platform"
    }
}
