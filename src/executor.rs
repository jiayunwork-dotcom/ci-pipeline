use crate::logging::*;
use crate::models::*;
use crate::variables::*;
use anyhow::{anyhow, Context, Result};
use colored::Colorize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::time::{Duration, timeout};

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub working_dir: PathBuf,
    pub default_timeout: u64,
    pub default_retry: u32,
    pub output_mode: OutputMode,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            working_dir: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            default_timeout: 3600,
            default_retry: 0,
            output_mode: OutputMode::Terminal,
        }
    }
}

pub struct Executor {
    pub config: ExecutorConfig,
    pub logger: Arc<Logger>,
}

impl Executor {
    pub fn new(config: ExecutorConfig, logger: Arc<Logger>) -> Self {
        Self { config, logger }
    }

    pub async fn execute_job(
        &self,
        job: &Job,
        resolver: &VariableResolver,
        global_lock: &Arc<Mutex<()>>,
        service_manager: &Arc<Mutex<crate::services::ServiceManager>>,
        history_avg: &HashMap<String, u64>,
        logger: &Arc<crate::logging::Logger>,
    ) -> JobResult {
        let started_at = chrono::Local::now();
        let timeout_secs = job.timeout.unwrap_or(self.config.default_timeout);
        let max_retries = job.retry.unwrap_or(self.config.default_retry);

        let job_logger = match JobLogger::new(&job.name, &self.logger) {
            Ok(l) => Arc::new(l),
            Err(e) => {
                return JobResult {
                    job_name: job.name.clone(),
                    status: JobStatus::Failed,
                    duration_ms: 0,
                    retry_count: 0,
                    message: Some(format!("Failed to create job logger: {}", e)),
                    outputs: HashMap::new(),
                    started_at: Some(started_at),
                    finished_at: Some(chrono::Local::now()),
                };
            }
        };

        job_logger
            .log_raw(
                &format!(
                    "=== Job '{}' started at {} ===",
                    job.name,
                    started_at.format("%Y-%m-%d %H:%M:%S")
                ),
                self.config.output_mode,
                global_lock,
            )
            .await;

        let mut last_error = None;
        let mut retry_count = 0;
        let mut outputs: HashMap<String, String> = HashMap::new();

        let is_container = matches!(job.isolation, IsolationMode::Container);
        let mut container_started = false;
        if is_container {
            let image = job.image.clone().unwrap_or_else(|| "alpine:latest".to_string());
            job_logger
                .log_raw(
                    &format!("=== Starting container with image '{}' ===", image),
                    self.config.output_mode,
                    global_lock,
                )
                .await;
            let merged_env = crate::variables::build_merged_env(&resolver.global_vars, &job.env, &HashMap::new());
            let mut final_env: HashMap<String, String> = HashMap::new();
            for (k, v) in merged_env {
                let resolved = resolver.try_resolve_value(&v, &job.env, &HashMap::new());
                final_env.insert(k, resolved);
            }
            let mut sm = service_manager.lock().await;
            match sm.start_job_container(&job.name, &image, &self.config.working_dir, &final_env).await {
                Ok(_) => {
                    container_started = true;
                }
                Err(e) => {
                    let finished_at = chrono::Local::now();
                    job_logger
                        .log_raw(
                            &format!("=== Failed to start container: {} ===", e),
                            self.config.output_mode,
                            global_lock,
                        )
                        .await;
                    return JobResult {
                        job_name: job.name.clone(),
                        status: JobStatus::Failed,
                        duration_ms: 0,
                        retry_count: 0,
                        message: Some(format!("Failed to start container: {}", e)),
                        outputs: HashMap::new(),
                        started_at: Some(started_at),
                        finished_at: Some(finished_at),
                    };
                }
            }
        }

        let history_avg_for_job = history_avg.get(&job.name).copied();
        let slow_warn_threshold = history_avg_for_job.map(|avg| avg * 3);
        let job_name_clone = job.name.clone();
        let logger_clone = logger.clone();
        let output_mode = self.config.output_mode;
        let global_lock_clone = global_lock.clone();
        let job_logger_clone = job_logger.clone();
        let warn_handle = if let Some(threshold) = slow_warn_threshold {
            Some(tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(threshold)).await;
                let msg = format!(
                    "!!! WARNING: Job '{}' is running slower than 3x historical average ({}ms) !!!",
                    job_name_clone, threshold
                );
                if matches!(output_mode, OutputMode::Terminal) {
                    let _g = global_lock_clone.lock().await;
                    eprintln!("{}", msg.yellow());
                }
                job_logger_clone
                    .log_raw(&msg, output_mode, &global_lock_clone)
                    .await;
                logger_clone.emit_event(LogEvent::now(
                    "job_slow_warning",
                    &job_name_clone,
                    "running",
                    &msg,
                ));
            }))
        } else {
            None
        };

        loop {
            match self
                .execute_job_attempt(job, resolver, global_lock, &job_logger, timeout_secs, &mut outputs, service_manager)
                .await
            {
                Ok(final_outputs) => {
                    outputs = final_outputs;
                    let finished_at = chrono::Local::now();
                    let duration_ms = (finished_at - started_at).num_milliseconds() as u64;
                    if let Some(handle) = warn_handle {
                        handle.abort();
                    }
                    job_logger
                        .log_raw(
                            &format!(
                                "=== Job '{}' succeeded in {}ms (retries: {}) ===",
                                job.name, duration_ms, retry_count
                            ),
                            self.config.output_mode,
                            global_lock,
                        )
                        .await;
                    if is_container && container_started {
                        service_manager.lock().await.stop_job_container(&job.name).await;
                    }
                    return JobResult {
                        job_name: job.name.clone(),
                        status: JobStatus::Success,
                        duration_ms,
                        retry_count,
                        message: None,
                        outputs,
                        started_at: Some(started_at),
                        finished_at: Some(finished_at),
                    };
                }
                Err(e) => {
                    last_error = Some(e);
                    if retry_count < max_retries {
                        retry_count += 1;
                        let backoff = 2u64.pow(retry_count);
                        job_logger
                            .log_raw(
                                &format!(
                                    "!!! Job '{}' attempt {} failed: {}. Retrying in {}s...",
                                    job.name, retry_count, last_error.as_ref().unwrap(), backoff
                                ),
                                self.config.output_mode,
                                global_lock,
                            )
                            .await;
                        tokio::time::sleep(Duration::from_secs(backoff)).await;
                        continue;
                    }
                    break;
                }
            }
        }

        if let Some(handle) = warn_handle {
            handle.abort();
        }
        if is_container && container_started {
            service_manager.lock().await.stop_job_container(&job.name).await;
        }

        let finished_at = chrono::Local::now();
        let duration_ms = (finished_at - started_at).num_milliseconds() as u64;
        job_logger
            .log_raw(
                &format!(
                    "=== Job '{}' FAILED in {}ms (retries: {}) - reason: {} ===",
                    job.name,
                    duration_ms,
                    retry_count,
                    last_error.as_ref().unwrap()
                ),
                self.config.output_mode,
                global_lock,
            )
            .await;

        JobResult {
            job_name: job.name.clone(),
            status: JobStatus::Failed,
            duration_ms,
            retry_count,
            message: last_error.map(|e| e.to_string()),
            outputs,
            started_at: Some(started_at),
            finished_at: Some(finished_at),
        }
    }

    async fn execute_job_attempt(
        &self,
        job: &Job,
        resolver: &VariableResolver,
        global_lock: &Arc<Mutex<()>>,
        job_logger: &Arc<JobLogger>,
        timeout_secs: u64,
        outputs: &mut HashMap<String, String>,
        service_manager: &Arc<Mutex<crate::services::ServiceManager>>,
    ) -> Result<HashMap<String, String>> {
        let result = timeout(
            Duration::from_secs(timeout_secs),
            self.execute_job_inner(job, resolver, global_lock, job_logger, outputs, service_manager),
        )
        .await;

        match result {
            Ok(inner) => inner,
            Err(_) => {
                return Err(anyhow!(
                    "Job exceeded timeout of {} seconds",
                    timeout_secs
                ));
            }
        }
    }

    async fn execute_job_inner(
        &self,
        job: &Job,
        resolver: &VariableResolver,
        global_lock: &Arc<Mutex<()>>,
        job_logger: &Arc<JobLogger>,
        _outputs: &mut HashMap<String, String>,
        service_manager: &Arc<Mutex<crate::services::ServiceManager>>,
    ) -> Result<HashMap<String, String>> {
        let mut step_outputs: HashMap<String, String> = HashMap::new();

        for (i, step) in job.steps.iter().enumerate() {
            let step_name = step
                .name
                .clone()
                .unwrap_or_else(|| format!("step-{}", i + 1));

            job_logger
                .log_raw(
                    &format!("--- Step {}: {} ---", i + 1, step_name),
                    self.config.output_mode,
                    global_lock,
                )
                .await;

            match self
                .execute_step(job, step, resolver, global_lock, job_logger, &mut step_outputs, service_manager)
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    if step.allow_failure {
                        job_logger
                            .log_raw(
                                &format!("Step '{}' failed but allow_failure=true: {}", step_name, e),
                                self.config.output_mode,
                                global_lock,
                            )
                            .await;
                        continue;
                    }
                    return Err(anyhow!("Step '{}' failed: {}", step_name, e));
                }
            }
        }

        Ok(step_outputs)
    }

    async fn execute_step(
        &self,
        job: &Job,
        step: &Step,
        resolver: &VariableResolver,
        global_lock: &Arc<Mutex<()>>,
        job_logger: &Arc<JobLogger>,
        step_outputs: &mut HashMap<String, String>,
        service_manager: &Arc<Mutex<crate::services::ServiceManager>>,
    ) -> Result<()> {
        let run_cmd = resolver.resolve_value(&step.run, &job.env, &step.env).unwrap_or_else(|_| step.run.clone());

        let merged_env = build_merged_env(&resolver.global_vars, &job.env, &step.env);
        let mut final_env: HashMap<String, String> = HashMap::new();
        for (k, v) in merged_env {
            let resolved = resolver.try_resolve_value(&v, &job.env, &step.env);
            final_env.insert(k, resolved);
        }
        for (k, v) in step_outputs.iter() {
            final_env.insert(k.clone(), v.clone());
        }

        let is_container = matches!(job.isolation, IsolationMode::Container);

        let step_outputs_arc = Arc::new(Mutex::new(step_outputs.clone()));
        let step_outputs_for_stdout = step_outputs_arc.clone();
        let job_logger_clone = job_logger.clone();
        let global_lock_clone = global_lock.clone();
        let output_mode = self.config.output_mode;
        let job_logger_clone2 = job_logger.clone();
        let global_lock_clone2 = global_lock.clone();
        let output_mode2 = self.config.output_mode;

        let status: std::process::ExitStatus = if is_container {
            let sm = service_manager.lock().await;
            let out = sm.exec_in_job_container(&job.name, &run_cmd, &final_env).await?;
            let stdout_bytes = out.stdout.clone();
            let stderr_bytes = out.stderr.clone();

            let stdout_task: tokio::task::JoinHandle<()> = tokio::spawn(async move {
                let stdout_str = String::from_utf8_lossy(&stdout_bytes);
                for line in stdout_str.lines() {
                    if let Some((key, value)) = parse_set_output(&line) {
                        let mut map = step_outputs_for_stdout.lock().await;
                        map.insert(key, value);
                    }
                    job_logger_clone
                        .log_stdout(&line, output_mode, &global_lock_clone)
                        .await;
                }
            });

            let stderr_task: tokio::task::JoinHandle<()> = tokio::spawn(async move {
                let stderr_str = String::from_utf8_lossy(&stderr_bytes);
                for line in stderr_str.lines() {
                    job_logger_clone2
                        .log_stderr(&line, output_mode2, &global_lock_clone2)
                        .await;
                }
            });

            let _ = stdout_task.await;
            let _ = stderr_task.await;
            out.status
        } else {
            let is_windows = cfg!(target_os = "windows");
            let shell = if is_windows { "cmd" } else { "bash" };
            let shell_arg = if is_windows { "/C" } else { "-c" };

            let mut cmd = Command::new(shell);
            cmd.arg(shell_arg);
            cmd.arg(&run_cmd);
            cmd.current_dir(&self.config.working_dir);
            for (k, v) in &final_env {
                cmd.env(k, v);
            }
            cmd.stdout(Stdio::piped());
            cmd.stderr(Stdio::piped());
            cmd.stdin(Stdio::null());

            let mut child = cmd.spawn().with_context(|| {
                format!(
                    "Failed to spawn shell command for step: {}",
                    step.name.as_deref().unwrap_or("unnamed")
                )
            })?;

            let stdout = child.stdout.take().ok_or_else(|| anyhow!("Failed to get stdout"))?;
            let stderr = child.stderr.take().ok_or_else(|| anyhow!("Failed to get stderr"))?;

            let stdout_task = tokio::spawn(async move {
                let mut reader = BufReader::new(stdout).lines();
                while let Some(line) = reader.next_line().await.unwrap_or(None) {
                    if let Some((key, value)) = parse_set_output(&line) {
                        let mut map = step_outputs_for_stdout.lock().await;
                        map.insert(key, value);
                    }
                    job_logger_clone
                        .log_stdout(&line, output_mode, &global_lock_clone)
                        .await;
                }
            });

            let stderr_task = tokio::spawn(async move {
                let mut reader = BufReader::new(stderr).lines();
                while let Some(line) = reader.next_line().await.unwrap_or(None) {
                    job_logger_clone2
                        .log_stderr(&line, output_mode2, &global_lock_clone2)
                        .await;
                }
            });

            let status = child.wait().await.with_context(|| "Failed to wait for process")?;

            let _ = stdout_task.await;
            let _ = stderr_task.await;
            status
        };

        {
            let map = step_outputs_arc.lock().await;
            for (k, v) in map.iter() {
                step_outputs.insert(k.clone(), v.clone());
            }
        }

        if status.success() {
            Ok(())
        } else {
            let code = status.code().unwrap_or(-1);
            Err(anyhow!("Process exited with non-zero status code: {}", code))
        }
    }
}

fn parse_set_output(line: &str) -> Option<(String, String)> {
    let prefix = "::set-output name=";
    if let Some(rest) = line.strip_prefix(prefix) {
        if let Some(eq_pos) = rest.find("::") {
            let name = rest[..eq_pos].to_string();
            let value = rest[eq_pos + 2..].to_string();
            return Some((name, value));
        }
    }
    None
}
