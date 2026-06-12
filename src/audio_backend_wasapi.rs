pub fn exclusive_status_line() -> &'static str {
    if cfg!(windows) {
        "planned for Phase 4; Phase 2 uses shared output"
    } else {
        "unavailable on this platform"
    }
}
