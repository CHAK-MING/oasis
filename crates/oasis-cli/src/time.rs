use chrono::Local;

/// 将 UTC 秒级时间戳格式化为本地时区时间字符串。
/// 返回格式：YYYY-MM-DD HH:MM:SS；当时间戳非法时返回 "-"。
pub fn format_local_ts(timestamp_secs: i64) -> String {
    if let Some(utc_dt) = chrono::DateTime::from_timestamp(timestamp_secs, 0) {
        utc_dt
            .with_timezone(&Local)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string()
    } else {
        "-".to_string()
    }
}

/// 返回当前本地时间字符串（用于 CLI 页眉/统计等）。
pub fn now_local_string() -> String {
    Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
}
