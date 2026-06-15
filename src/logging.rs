use anyhow::{Context, Result};
use std::fmt;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use colored::Colorize;

pub struct Logger {
    pub output_mode: OutputMode,
    pub log_lock: Arc<Mutex<()>>,
    pub base_dir: String,
}

impl fmt::Debug for Logger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Logger")
            .field("output_mode", &self.output_mode)
            .field("base_dir", &self.base_dir)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    Terminal,
    Json,
    Junit,
    Html,
}

impl Logger {
    pub fn new(base_dir: &str, output_mode: OutputMode) -> Result<Self> {
        let logs_dir = Path::new(base_dir).join(".ci").join("logs");
        std::fs::create_dir_all(&logs_dir)
            .with_context(|| format!("Failed to create logs dir: {:?}", logs_dir))?;
        Ok(Self {
            output_mode,
            log_lock: Arc::new(Mutex::new(())),
            base_dir: base_dir.to_string(),
        })
    }

    pub fn get_log_path(&self, job_name: &str) -> std::path::PathBuf {
        let safe_name = job_name.replace('/', "_").replace(' ', "_");
        Path::new(&self.base_dir)
            .join(".ci")
            .join("logs")
            .join(format!("{}.log", safe_name))
    }

    pub fn log_line(&self, line: &str) {
        if matches!(self.output_mode, OutputMode::Terminal) {
            println!("{}", line);
        }
    }

    pub fn emit_event(&self, event: LogEvent) {
        match self.output_mode {
            OutputMode::Json => {
                let json = serde_json::to_string(&event).unwrap();
                println!("{}", json);
            }
            OutputMode::Terminal => {}
            _ => {}
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogEvent {
    pub timestamp: String,
    pub event_type: String,
    pub job_name: String,
    pub status: String,
    pub message: String,
}

impl LogEvent {
    pub fn now(event_type: &str, job_name: &str, status: &str, message: &str) -> Self {
        Self {
            timestamp: chrono::Local::now().to_rfc3339(),
            event_type: event_type.to_string(),
            job_name: job_name.to_string(),
            status: status.to_string(),
            message: message.to_string(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StepOutput {
    pub name: String,
    pub output: String,
    pub exit_code: i32,
    pub duration_ms: u64,
    pub outputs: std::collections::HashMap<String, String>,
}

const COLORS: &[&str] = &[
    "cyan",
    "magenta",
    "yellow",
    "green",
    "blue",
    "bright_cyan",
    "bright_magenta",
    "bright_yellow",
    "bright_green",
];

fn pick_color(job_name: &str) -> &'static str {
    let mut hash = 0u64;
    for b in job_name.bytes() {
        hash = hash.wrapping_mul(31).wrapping_add(b as u64);
    }
    COLORS[(hash % COLORS.len() as u64) as usize]
}

fn colorize(text: &str, color: &str) -> String {
    match color {
        "cyan" => text.cyan().to_string(),
        "magenta" => text.magenta().to_string(),
        "yellow" => text.yellow().to_string(),
        "green" => text.green().to_string(),
        "blue" => text.blue().to_string(),
        "bright_cyan" => text.bright_cyan().to_string(),
        "bright_magenta" => text.bright_magenta().to_string(),
        "bright_yellow" => text.bright_yellow().to_string(),
        "bright_green" => text.bright_green().to_string(),
        _ => text.to_string(),
    }
}

pub struct JobLogger {
    job_name: String,
    prefix: String,
    log_path: std::path::PathBuf,
    file_lock: Arc<Mutex<std::fs::File>>,
}

impl JobLogger {
    pub fn new(job_name: &str, logger: &Logger) -> Result<Self> {
        let log_path = logger.get_log_path(job_name);
        if let Some(parent) = log_path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .with_context(|| format!("Failed to open log file: {:?}", log_path))?;
        let color = pick_color(job_name);
        let prefix = format!("[{}]", job_name);
        let prefix = colorize(&prefix, color);
        Ok(Self {
            job_name: job_name.to_string(),
            prefix,
            log_path,
            file_lock: Arc::new(Mutex::new(file)),
        })
    }

    pub async fn log_raw(&self, line: &str, output_mode: OutputMode, global_lock: &Arc<Mutex<()>>) {
        {
            let mut f = self.file_lock.lock().await;
            let _ = writeln!(f, "{}", line);
        }
        if matches!(output_mode, OutputMode::Terminal) {
            let _g = global_lock.lock().await;
            println!("{} {}", self.prefix, line);
        }
    }

    pub async fn log_stdout(&self, line: &str, output_mode: OutputMode, global_lock: &Arc<Mutex<()>>) {
        {
            let mut f = self.file_lock.lock().await;
            let _ = writeln!(f, "[stdout] {}", line);
        }
        if matches!(output_mode, OutputMode::Terminal) {
            let _g = global_lock.lock().await;
            println!("{} {}", self.prefix, line);
        }
    }

    pub async fn log_stderr(&self, line: &str, output_mode: OutputMode, global_lock: &Arc<Mutex<()>>) {
        {
            let mut f = self.file_lock.lock().await;
            let _ = writeln!(f, "[stderr] {}", line);
        }
        if matches!(output_mode, OutputMode::Terminal) {
            let _g = global_lock.lock().await;
            println!("{} {}", self.prefix, line.red().to_string());
        }
    }
}
