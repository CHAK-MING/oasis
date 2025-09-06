use console::style;
use std::io::Write;
use std::time::Duration;
use tokio::time::sleep;

/// Docker 风格符号
pub struct Symbols;
impl Symbols {
    pub const SUCCESS: &'static str = "[+]";
    pub const ERROR: &'static str = "[!]";
    pub const WARNING: &'static str = "[*]";
    pub const INFO: &'static str = "[i]";
    pub const ARROW: &'static str = "=>";
}

/// Docker 风格颜色主题
pub struct Theme {
    pub success: console::Style,
    pub error: console::Style,
    pub warning: console::Style,
    pub info: console::Style,
    pub dim: console::Style,
}

pub fn get_theme() -> Theme {
    Theme {
        success: console::Style::new().green(),
        error: console::Style::new().red(),
        warning: console::Style::new().yellow(),
        info: console::Style::new().blue(),
        dim: console::Style::new().dim(),
    }
}

/// 成功/失败状态输出
pub fn print_status(message: &str, success: bool) {
    let (symbol, color) = if success {
        (Symbols::SUCCESS, get_theme().success)
    } else {
        (Symbols::ERROR, get_theme().error)
    };
    println!(" {} {}", color.apply_to(symbol), message);
}

/// 信息提示
pub fn print_info(message: &str) {
    println!(" {} {}", get_theme().info.apply_to(Symbols::INFO), message);
}

/// 警告信息
pub fn print_warning(message: &str) {
    println!(
        " {} {}",
        get_theme().warning.apply_to(Symbols::WARNING),
        message
    );
}

pub fn print_error(message: &str) {
    println!(
        " {} {}",
        get_theme().error.apply_to(Symbols::ERROR),
        message
    );
}

/// 重要信息或下一步提示
pub fn print_next_step(message: &str) {
    println!(" {} {}", get_theme().info.apply_to(Symbols::ARROW), message);
}

/// 操作标题
pub fn print_header(title: &str) {
    println!("{}", style(title).bold());
}

/// 进度条显示
pub fn print_progress_bar(current: usize, total: usize, description: &str) {
    let percentage = (current as f64 / total as f64 * 100.0) as usize;
    let width = 25;
    let filled = (percentage as f64 / 100.0 * width as f64) as usize;
    let empty = width - filled;

    println!(
        " {} [{}{}] {}/{} ({}%) {}",
        get_theme().info.apply_to(Symbols::ARROW),
        style("█".repeat(filled)).green(),
        "░".repeat(empty),
        current,
        total,
        percentage,
        style(description).dim()
    );
}

/// 简单进度提示
pub fn print_progress(current: usize, total: usize, description: &str) {
    println!(
        " {} {}/{} - {}",
        get_theme().info.apply_to(Symbols::ARROW),
        current,
        total,
        style(description).dim()
    );
}

/// 确认操作
pub fn confirm_action(message: &str, danger: bool) -> bool {
    let (symbol, color) = if danger {
        (Symbols::WARNING, get_theme().warning)
    } else {
        (Symbols::INFO, get_theme().info)
    };

    println!(" {} {}", color.apply_to(symbol), message);
    print!(
        " {} 是否继续? [y/N]: ",
        get_theme().dim.apply_to(Symbols::ARROW)
    );
    std::io::stdout().flush().expect("Failed to flush stdout");

    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap_or_else(|e| {
        eprintln!("Failed to read user input: {}", e);
        0
    });
    matches!(input.trim().to_lowercase().as_str(), "y" | "yes" | "是")
}

/// 带详细信息的日志
pub fn log_detail(key: &str, value: &str) {
    println!("   {}: {}", style(key).dim(), value);
}

/// 操作日志（带缩进层次）
pub fn log_operation(operation: &str, details: Option<&[(&str, &str)]>) {
    println!(
        " {} {}",
        get_theme().info.apply_to(Symbols::ARROW),
        operation
    );

    if let Some(details) = details {
        for (key, value) in details {
            log_detail(key, value);
        }
    }
}

/// 等待和进度动画
pub async fn show_operation_with_dots(message: &str, duration: Duration) {
    let dots = ["", ".", "..", "..."];
    let start = std::time::Instant::now();

    while start.elapsed() < duration {
        for dot in &dots {
            print!(
                "\r {} {}{}",
                get_theme().info.apply_to(Symbols::ARROW),
                message,
                dot
            );
            std::io::stdout().flush().expect("Failed to flush stdout");
            sleep(Duration::from_millis(400)).await;

            if start.elapsed() >= duration {
                break;
            }
        }
    }

    println!(
        "\r {} {} 完成",
        get_theme().success.apply_to(Symbols::SUCCESS),
        message
    );
}
