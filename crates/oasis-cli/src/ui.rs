use console::{Emoji, Term, style};
use indicatif::{ProgressBar, ProgressStyle};
use std::io::Write;
use std::time::Duration;

pub struct Symbols;
impl Symbols {
    pub const SUCCESS: Emoji<'static, 'static> = Emoji("✔", "+");
    pub const ERROR: Emoji<'static, 'static> = Emoji("✖", "x");
    pub const WARNING: Emoji<'static, 'static> = Emoji("⚠", "!");
    pub const INFO: Emoji<'static, 'static> = Emoji("ℹ", "i");
    pub const ARROW: Emoji<'static, 'static> = Emoji("➜", ">");
    pub const STEP: Emoji<'static, 'static> = Emoji("⚡", "*");
}

pub fn print_header(title: &str) {
    println!();
    println!("{} {}", Symbols::STEP, style(title).bold().underlined());
    println!();
}

pub fn print_status(message: &str, success: bool) {
    if success {
        println!("  {} {}", style(Symbols::SUCCESS).green(), message);
    } else {
        println!("  {} {}", style(Symbols::ERROR).red(), message);
    }
}

pub fn print_success(message: &str) {
    println!("  {} {}", style(Symbols::SUCCESS).green(), message);
}

pub fn print_error(message: &str) {
    println!("  {} {}", style(Symbols::ERROR).red(), message);
}

pub fn print_warning(message: &str) {
    println!("  {} {}", style(Symbols::WARNING).yellow(), message);
}

pub fn print_info(message: &str) {
    println!("  {} {}", style(Symbols::INFO).blue(), message);
}

pub fn print_next_step(message: &str) {
    println!("  {} {}", style(Symbols::ARROW).cyan(), message);
}

#[allow(dead_code)]
pub fn log_detail(key: &str, value: &str) {
    println!("    {}: {}", style(key).dim(), value);
}

pub fn log_operation(operation: &str, details: Option<&[(&str, &str)]>) {
    println!("  {} {}", style(Symbols::ARROW).cyan(), operation);
    if let Some(details) = details {
        for (k, v) in details {
            println!("    {}: {}", style(k).dim(), v);
        }
    }
}

pub fn confirm_action(message: &str, danger: bool) -> bool {
    println!();
    if danger {
        println!(
            "  {} {}",
            style(Symbols::WARNING).yellow(),
            style(message).red().bold()
        );
    } else {
        println!("  {} {}", style("?").blue(), message);
    }

    let term = Term::stdout();
    print!("  {} (y/N): ", style(Symbols::ARROW).cyan());
    let _ = std::io::stdout().flush();

    let input = term.read_line().unwrap_or_default();
    matches!(input.trim().to_lowercase().as_str(), "y" | "yes" | "true")
}

pub fn create_spinner(message: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ ")
            .template("{spinner:.blue} {msg}")
            .unwrap(),
    );
    pb.set_message(message.to_string());
    pb.enable_steady_tick(Duration::from_millis(80));
    pb
}

pub fn create_progress_bar(total: u64, message: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta}) {msg}")
            .unwrap()
            .progress_chars("=>-"),
    );
    pb.set_message(message.to_string());
    pb
}
