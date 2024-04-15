use std::time::{Duration, UNIX_EPOCH};

fn timestamp() -> Duration {
    std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
}

pub fn timestamp_ms() -> u64 {
    timestamp().as_millis() as u64
}
