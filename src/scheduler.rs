use crate::artifacts::*;
use crate::dag::*;
use crate::executor::*;
use crate::logging::*;
use crate::models::*;
use crate::reporting::*;
use crate::services::*;
use crate::state::*;
use crate::variables::*;
use anyhow::{anyhow, Result};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    pub file: String,
    pub parallel: usize,
    pub output_mode: OutputMode,
    pub resume: bool,
    pub dry_run: bool,
    pub filter: Option<String>,
    pub working_dir: PathBuf,
    pub cache_ttl_days: i64,
    pub default_timeout: u64,
    pub default_retry: u32,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            file: "pipeline.yml".to_string(),
            parallel: 4,
            output_mode: OutputMode::Terminal,
            resume: false,
            dry_run: false,
            filter: None,
            working_dir: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            cache_ttl_days: 7,
            default_timeout: 3600,
            default_retry: 0,
        }
    }
}

pub struct Scheduler {
    pub config: SchedulerConfig,
}

impl Scheduler {
    pub fn new(config: SchedulerConfig) -> Self {
        Self { config }
    }

    pub async fn run(&self) -> Result<Vec<JobResult>> {
        let working_dir = &self.config.working_dir.clone();
        let ci_dir = working_dir.join(".ci");
        std::fs::create_dir_all(&ci_dir).ok();

        let pipeline_content = std::fs::read_to_string(&self.config.file)
            .map_err(|e| anyhow!("Failed to read pipeline file {}: {}", self.config.file, e))?;

        let mut pipeline = crate::parser::parse_pipeline_from_str(&pipeline_content)?;

        let errors = crate::validator::validate_pipeline(&pipeline)?;
        if !errors.is_empty() {
            eprintln!("Validation errors:");
            for e in &errors {
                eprintln!("  - {}", e);
            }
            return Err(anyhow!("Pipeline validation failed with {} errors", errors.len()));
        }

        pipeline.jobs = crate::matrix::expand_matrix_jobs(pipeline.jobs);

        let mut dag = Dag::build(&pipeline.jobs)?;
        dag.topological_order()?;

        let execution_order: Vec<String> = if let Some(filter) = &self.config.filter {
            let pattern = glob::Pattern::new(filter)
                .map_err(|e| anyhow!("Invalid filter pattern: {}", e))?;
            dag.filter_by_pattern(&pattern)?
        } else {
            dag.topological_order()?
        };

        if execution_order.is_empty() {
            eprintln!("No jobs match the filter pattern.");
            return Ok(Vec::new());
        }

        let job_map: HashMap<String, Job> = pipeline.jobs
            .iter()
            .filter(|j| execution_order.contains(&j.name))
            .cloned()
            .map(|j| (j.name.clone(), j))
            .collect();

        let state_manager = StateManager::new(&working_dir);
        let pipeline_hash = state_manager.compute_pipeline_hash(&pipeline_content);
        let previous_results: HashMap<String, JobResult> = if self.config.resume {
            state_manager
                .load_state(&pipeline_hash)
                .map(|s| s.job_results)
                .unwrap_or_default()
        } else {
            HashMap::new()
        };

        self.print_plan(&job_map, &execution_order, &dag, &previous_results);

        if self.config.dry_run {
            println!("\n--dry-run mode: no jobs executed.");
            return Ok(Vec::new());
        }

        let logger = Arc::new(Logger::new(
            working_dir.to_string_lossy().as_ref(),
            self.config.output_mode,
        )?);
        let artifact_manager = Arc::new(ArtifactManager::new(&working_dir));
        let cache_manager = Arc::new(CacheManager::new(&working_dir, self.config.cache_ttl_days));
        let service_manager = Arc::new(Mutex::new(ServiceManager::new()));

        let exec_config = ExecutorConfig {
            working_dir: working_dir.clone(),
            default_timeout: self.config.default_timeout,
            default_retry: self.config.default_retry,
            output_mode: self.config.output_mode,
        };
        let executor = Arc::new(Executor::new(exec_config, logger.clone()));

        let semaphore = Arc::new(Semaphore::new(self.config.parallel));
        let resolver = Arc::new(Mutex::new(VariableResolver::new(&pipeline)));
        let completed: Arc<Mutex<HashMap<String, JobResult>>> = Arc::new(Mutex::new(previous_results.clone()));
        let job_statuses: Arc<Mutex<HashMap<String, JobStatus>>> = Arc::new(Mutex::new(HashMap::new()));

        for (name, result) in &previous_results {
            if matches!(result.status, JobStatus::Success) {
                job_statuses.lock().await.insert(name.clone(), JobStatus::Success);
            }
        }

        let global_lock = Arc::new(Mutex::new(()));

        let cancelled_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let cancel_flag_ref = cancelled_flag.clone();
        let signal_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        eprintln!("\nReceived shutdown signal. Gracefully stopping...");
                        cancel_flag_ref.store(true, std::sync::atomic::Ordering::SeqCst);
                        break;
                    }
                }
            }
        });

        let start_time = chrono::Local::now();
        let total_jobs = execution_order.len();

        if matches!(self.config.output_mode, OutputMode::Terminal) {
            println!("\nStarting pipeline execution...");
            println!("  Working directory: {}", working_dir.display());
            println!("  Parallel jobs: {}", self.config.parallel);
            println!("  Total jobs to run: {}\n", total_jobs);
        }

        let (event_tx, _event_rx) = mpsc::channel::<()>(100);

        let mut handles: HashMap<String, tokio::task::JoinHandle<JobResult>> = HashMap::new();
        let pending: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(execution_order.iter().cloned().collect()));
        let running: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

        let progress_handle = if matches!(self.config.output_mode, OutputMode::Terminal) {
            let pending_ref = pending.clone();
            let running_ref = running.clone();
            let completed_ref = completed.clone();
            Some(tokio::spawn(async move {
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                    let pending = pending_ref.lock().await;
                    let running = running_ref.lock().await;
                    let completed = completed_ref.lock().await;
                    let running_list: Vec<String> = running.iter().cloned().collect();
                    let pending_list: Vec<String> = pending.iter().cloned().collect();
                    print_progress_update(&running_list, &*completed, &pending_list, total_jobs, start_time);
                }
            }))
        } else {
            None
        };

        loop {
            {
                if cancelled_flag.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
            }

            let mut to_dispatch: Vec<String> = Vec::new();
            let mut to_skip: Vec<(String, JobResult)> = Vec::new();
            let mut to_running: Vec<String> = Vec::new();
            let mut to_remove_pending: Vec<String> = Vec::new();

            let completed_status: HashMap<String, JobStatus> = {
                let completed_guard = completed.lock().await;
                completed_guard
                    .iter()
                    .map(|(k, v)| (k.clone(), v.status.clone()))
                    .collect()
            };
            {
                let running_guard = running.lock().await;
                let pending_guard = pending.lock().await;

                let mut sorted_pending: Vec<&String> = pending_guard.iter().collect();
                sorted_pending.sort_by(|a, b| {
                    let ia = execution_order.iter().position(|x| x == *a).unwrap_or(usize::MAX);
                    let ib = execution_order.iter().position(|x| x == *b).unwrap_or(usize::MAX);
                    ia.cmp(&ib)
                });

                for job_name in sorted_pending {
                    let job = &job_map[job_name];
                    if previous_results.get(job_name)
                        .map(|r| matches!(r.status, JobStatus::Success))
                        .unwrap_or(false)
                    {
                        to_remove_pending.push(job_name.clone());
                        continue;
                    }

                    if running_guard.contains(job_name) {
                        continue;
                    }

                    if !dag.has_dependencies_met(job_name, &completed_status) {
                        continue;
                    }

                    let any_failed = dag.any_dep_failed(job_name, &completed_status);
                    if any_failed {
                        let should_skip = match &job.condition {
                            Some(cond) => {
                                let resolver_guard = resolver.lock().await;
                                let eval = ConditionEvaluator::new(completed_status.clone());
                                match eval.evaluate(cond, &resolver_guard, &job.env, &HashMap::new()) {
                                    Ok(true) => false,
                                    _ => true,
                                }
                            }
                            None => true,
                        };
                        if should_skip {
                            to_remove_pending.push(job_name.clone());
                            let skipped = JobResult {
                                job_name: job_name.clone(),
                                status: JobStatus::Skipped,
                                duration_ms: 0,
                                retry_count: 0,
                                message: Some("Dependency failed".to_string()),
                                outputs: HashMap::new(),
                                started_at: Some(chrono::Local::now()),
                                finished_at: Some(chrono::Local::now()),
                            };
                            to_skip.push((job_name.clone(), skipped));
                            logger.emit_event(LogEvent::now(
                                "job_skipped",
                                job_name,
                                "skipped",
                                "Dependency failed",
                            ));
                            continue;
                        } else {
                            if running_guard.len() + to_running.len() >= self.config.parallel {
                                continue;
                            }
                            to_dispatch.push(job_name.clone());
                            to_running.push(job_name.clone());
                            continue;
                        }
                    }

                    let condition_ok = match &job.condition {
                        Some(cond) => {
                            let resolver_guard = resolver.lock().await;
                            let eval = ConditionEvaluator::new(completed_status.clone());
                            match eval.evaluate(cond, &resolver_guard, &job.env, &HashMap::new()) {
                                Ok(v) => v,
                                Err(e) => {
                                    to_remove_pending.push(job_name.clone());
                                    let skipped = JobResult {
                                        job_name: job_name.clone(),
                                        status: JobStatus::Skipped,
                                        duration_ms: 0,
                                        retry_count: 0,
                                        message: Some(format!("Condition evaluation error: {}", e)),
                                        outputs: HashMap::new(),
                                        started_at: Some(chrono::Local::now()),
                                        finished_at: Some(chrono::Local::now()),
                                    };
                                    to_skip.push((job_name.clone(), skipped));
                                    false
                                }
                            }
                        }
                        None => true,
                    };

                    if !condition_ok {
                        to_remove_pending.push(job_name.clone());
                        let skipped = JobResult {
                            job_name: job_name.clone(),
                            status: JobStatus::Skipped,
                            duration_ms: 0,
                            retry_count: 0,
                            message: Some("Condition not met".to_string()),
                            outputs: HashMap::new(),
                            started_at: Some(chrono::Local::now()),
                            finished_at: Some(chrono::Local::now()),
                        };
                        to_skip.push((job_name.clone(), skipped));
                        logger.emit_event(LogEvent::now(
                            "job_skipped",
                            job_name,
                            "skipped",
                            "Condition not met",
                        ));
                        continue;
                    }

                    if running_guard.len() + to_running.len() >= self.config.parallel {
                        continue;
                    }

                    to_dispatch.push(job_name.clone());
                    to_running.push(job_name.clone());
                }
            }

            {
                let mut c = completed.lock().await;
                for (name, result) in &to_skip {
                    c.insert(name.clone(), result.clone());
                }
            }
            {
                let mut js = job_statuses.lock().await;
                for (name, result) in &to_skip {
                    js.insert(name.clone(), result.status.clone());
                }
                for name in &to_running {
                    js.insert(name.clone(), JobStatus::Running);
                }
            }
            {
                let mut rg = running.lock().await;
                for name in &to_running {
                    rg.insert(name.clone());
                }
            }
            {
                let mut pg = pending.lock().await;
                for name in &to_remove_pending {
                    pg.remove(name);
                }
                for name in &to_running {
                    pg.remove(name);
                }
                for (name, _) in &to_skip {
                    pg.remove(name);
                }
            }

            for job_name in &to_dispatch {
                let job = job_map[job_name].clone();
                let permit = semaphore.clone().acquire_owned().await.map_err(|_| anyhow!("Semaphore closed"))?;
                let executor = executor.clone();
                let resolver = resolver.clone();
                let completed = completed.clone();
                let job_statuses = job_statuses.clone();
                let artifact_manager = artifact_manager.clone();
                let cache_manager = cache_manager.clone();
                let service_manager = service_manager.clone();
                let global_lock = global_lock.clone();
                let cancelled_flag = cancelled_flag.clone();
                let working_dir = working_dir.clone();
                let logger = logger.clone();
                let _event_tx = event_tx.clone();
                let running = running.clone();
                let pending = pending.clone();

                {
                    let mut js = job_statuses.lock().await;
                    js.insert(job_name.clone(), JobStatus::Running);
                }

                logger.emit_event(LogEvent::now("job_started", job_name, "running", ""));

                let handle = tokio::spawn(async move {
                    let _permit = permit;

                    for dep in &job.needs_artifacts {
                        let _restored = artifact_manager
                            .restore_artifacts(dep, &working_dir)
                            .unwrap_or(false);
                    }

                    if !job.cache.is_empty() {
                        let cache_key = cache_manager
                            .compute_cache_key(&job.cache, &working_dir)
                            .unwrap_or_default();
                        if !cache_key.is_empty() {
                            let _restored = cache_manager
                                .restore_cache(&cache_key, &working_dir)
                                .unwrap_or(false);
                        }
                    }

                    let _service_hosts = service_manager
                        .lock()
                        .await
                        .start_services(&job.name, &job.services)
                        .await
                        .unwrap_or_default();

                    let resolver_guard = resolver.lock().await;
                    let resolver_for_exec = (*resolver_guard).clone();
                    drop(resolver_guard);

                    let is_cancelled = cancelled_flag.load(std::sync::atomic::Ordering::SeqCst);

                    let result = if is_cancelled {
                        JobResult {
                            job_name: job.name.clone(),
                            status: JobStatus::Cancelled,
                            duration_ms: 0,
                            retry_count: 0,
                            message: Some("Cancelled before start".to_string()),
                            outputs: HashMap::new(),
                            started_at: Some(chrono::Local::now()),
                            finished_at: Some(chrono::Local::now()),
                        }
                    } else {
                        executor
                            .execute_job(&job, &resolver_for_exec, &global_lock)
                            .await
                    };

                    if matches!(result.status, JobStatus::Success) {
                        for (k, v) in &result.outputs {
                            resolver
                                .lock()
                                .await
                                .set_job_output(&job.name, k, v);
                        }

                        if !job.artifacts.is_empty() {
                            let _packaged = artifact_manager
                                .package_artifacts(&job, &working_dir)
                                .unwrap_or(None);
                        }

                        if !job.cache.is_empty() {
                            if let Ok(cache_key) = cache_manager.compute_cache_key(&job.cache, &working_dir) {
                                if !cache_key.is_empty() {
                                    let _saved = cache_manager
                                        .save_cache(&job.cache, &cache_key, &working_dir)
                                        .ok();
                                }
                            }
                        }
                    }

                    service_manager.lock().await.stop_job_services(&job.name).await;

                    {
                        let mut c = completed.lock().await;
                        c.insert(job.name.clone(), result.clone());
                    }
                    {
                        let mut js = job_statuses.lock().await;
                        js.insert(job.name.clone(), result.status.clone());
                    }
                    {
                        let mut rg = running.lock().await;
                        rg.remove(&job.name);
                    }
                    {
                        let mut pg = pending.lock().await;
                        pg.remove(&job.name);
                    }

                    let status_str = result.status.to_string();
                    let msg = result.message.clone().unwrap_or_default();
                    logger.emit_event(LogEvent::now(
                        "job_finished",
                        &job.name,
                        &status_str,
                        &msg,
                    ));

                    result
                });

                handles.insert(job_name.clone(), handle);
            }

            {
                let pending_guard = pending.lock().await;
                let running_guard = running.lock().await;
                if pending_guard.is_empty() && running_guard.is_empty() {
                    break;
                }
            }

            {
                if cancelled_flag.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        {
            if cancelled_flag.load(std::sync::atomic::Ordering::SeqCst) {
                let pending_guard = pending.lock().await;
                let pending_names: Vec<String> = pending_guard.iter().cloned().collect();
                drop(pending_guard);
                for job_name in pending_names {
                    let has = {
                        let completed_guard = completed.lock().await;
                        completed_guard.contains_key(&job_name)
                    };
                    if !has {
                        let cancelled = JobResult {
                            job_name: job_name.clone(),
                            status: JobStatus::Cancelled,
                            duration_ms: 0,
                            retry_count: 0,
                            message: Some("Cancelled by user signal".to_string()),
                            outputs: HashMap::new(),
                            started_at: Some(chrono::Local::now()),
                            finished_at: Some(chrono::Local::now()),
                        };
                        completed.lock().await.insert(job_name.clone(), cancelled);
                        job_statuses.lock().await.insert(job_name.clone(), JobStatus::Cancelled);
                    }
                }
                service_manager.lock().await.stop_all().await;
            }
        }

        if let Some(handle) = progress_handle {
            handle.abort();
        }

        for (_name, handle) in handles {
            let _ = handle.await;
        }

        signal_handle.abort();
        let _ = signal_handle.await;

        let final_results_map = completed.lock().await.clone();
        let mut final_results: Vec<JobResult> = Vec::new();
        for name in &execution_order {
            if let Some(r) = final_results_map.get(name) {
                final_results.push(r.clone());
            }
        }

        let _ = state_manager.save_state(&pipeline_hash, &final_results_map);

        if matches!(self.config.output_mode, OutputMode::Terminal) {
            eprint!("\r");
            eprint!("{}", " ".repeat(150));
            eprint!("\r");
            print_summary(&final_results);
        }

        if matches!(self.config.output_mode, OutputMode::Junit) {
            let xml = generate_junit_xml(&final_results);
            let junit_path = ci_dir.join("junit.xml");
            std::fs::write(&junit_path, &xml).ok();
            println!("{}", xml);
        }

        if matches!(self.config.output_mode, OutputMode::Html) {
            generate_html_report(&final_results, working_dir.to_string_lossy().as_ref())?;
        }

        let _ = cache_manager.cleanup_expired();

        Ok(final_results)
    }

    fn print_plan(
        &self,
        job_map: &HashMap<String, Job>,
        order: &[String],
        dag: &Dag,
        previous: &HashMap<String, JobResult>,
    ) {
        if !matches!(self.config.output_mode, OutputMode::Terminal) {
            return;
        }
        println!();
        println!("Pipeline Execution Plan");
        println!("{}", "═".repeat(80));
        for (i, name) in order.iter().enumerate() {
            let job = &job_map[name];
            let deps = dag.dependencies_of(name);
            let dep_str = if deps.is_empty() {
                "none".to_string()
            } else {
                deps.join(", ")
            };
            let resume_status = previous.get(name).map(|r| match r.status {
                JobStatus::Success => " [resume: skip]".to_string(),
                JobStatus::Failed => " [resume: re-run]".to_string(),
                _ => String::new(),
            }).unwrap_or_default();
            println!(
                "  {:<3} {} {:<35} stage={:<10} deps={}{}",
                i + 1,
                format_status_emoji(&JobStatus::Pending),
                truncate_for_plan(name, 33),
                job.stage.clone().unwrap_or_else(|| "-".to_string()),
                dep_str,
                resume_status
            );
        }
        println!("{}", "═".repeat(80));
    }
}

fn truncate_for_plan(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut t: String = s.chars().take(max - 3).collect();
        t.push_str("...");
        t
    }
}
