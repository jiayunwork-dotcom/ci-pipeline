use crate::artifacts::*;
use crate::dag::*;
use crate::executor::*;
use crate::logging::*;
use crate::models::*;
use crate::remote_cache::*;
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
    pub changed_files: Vec<String>,
    pub remote_cache: RemoteCacheConfig,
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
            changed_files: Vec::new(),
            remote_cache: RemoteCacheConfig::default(),
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

        let remote_cache_config = pipeline.remote_cache.clone();

        pipeline.jobs = crate::matrix::expand_matrix_jobs(pipeline.jobs);

        let changed_files = self.config.changed_files.clone();
        let trigger = pipeline.trigger.clone();
        let skipped_by_trigger: HashSet<String> = if let Some(t) = &trigger {
            compute_trigger_skipped_jobs(&pipeline, t, &changed_files)
        } else {
            HashSet::new()
        };

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

        let (history, history_avg) = load_history(&working_dir);

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

        let default_namespace = get_default_namespace();
        let remote_cache_client = Arc::new(RemoteCacheClient::new(
            remote_cache_config.clone(),
            &default_namespace,
        ));

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

                    if skipped_by_trigger.contains(job_name) {
                        to_remove_pending.push(job_name.clone());
                        let skipped = JobResult {
                            job_name: job_name.clone(),
                            status: JobStatus::Skipped,
                            duration_ms: 0,
                            retry_count: 0,
                            message: Some("Trigger paths not matched".to_string()),
                            outputs: HashMap::new(),
                            started_at: Some(chrono::Local::now()),
                            finished_at: Some(chrono::Local::now()),
                        };
                        to_skip.push((job_name.clone(), skipped));
                        logger.emit_event(LogEvent::now(
                            "job_skipped",
                            job_name,
                            "skipped",
                            "Trigger paths not matched",
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
                let history_avg_clone = history_avg.clone();
                let remote_cache_client = remote_cache_client.clone();

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

                    let stable_cache_key = if !job.cache.is_empty() {
                        Some(cache_manager.compute_stable_cache_key(&job.name, &job.cache))
                    } else {
                        None
                    };

                    let mut cache_restored = false;

                    if !job.cache.is_empty() {
                        if let Some(ref cache_key) = stable_cache_key {
                            let mut restored = cache_manager
                                .restore_cache(&cache_key, &working_dir)
                                .unwrap_or(false);

                            if !restored && remote_cache_client.is_enabled() {
                                let remote_path = cache_manager.cache_entry_path(&cache_key);
                                if let Ok(true) = remote_cache_client
                                    .download_cache(&cache_key, &remote_path)
                                    .await
                                {
                                    let _ = cache_manager
                                        .restore_cache(&cache_key, &working_dir);
                                    restored = true;
                                }
                            }

                            cache_restored = restored;
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
                    } else if cache_restored {
                        JobResult {
                            job_name: job.name.clone(),
                            status: JobStatus::Success,
                            duration_ms: 0,
                            retry_count: 0,
                            message: Some("Skipped (cache hit)".to_string()),
                            outputs: HashMap::new(),
                            started_at: Some(chrono::Local::now()),
                            finished_at: Some(chrono::Local::now()),
                        }
                    } else {
                        executor
                            .execute_job(&job, &resolver_for_exec, &global_lock, &service_manager, &history_avg_clone, &logger)
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
                            if let Some(artifact_path) = artifact_manager
                                .package_artifacts(&job, &working_dir)
                                .unwrap_or(None)
                            {
                                if remote_cache_client.is_enabled() {
                                    let artifact_key = format!(
                                        "artifact-{}-{}",
                                        job.name,
                                        compute_file_hash(&artifact_path).unwrap_or_default()
                                    );
                                    let _ = remote_cache_client
                                        .upload_cache(&artifact_key, &artifact_path)
                                        .await;
                                }
                            }
                        }

                        if !job.cache.is_empty() && !cache_restored {
                            if let Some(ref cache_key) = stable_cache_key {
                                if let Ok(cache_path) = cache_manager
                                    .save_cache(&job.cache, &cache_key, &working_dir)
                                {
                                    if remote_cache_client.is_enabled() {
                                        let _ = remote_cache_client
                                            .upload_cache(&cache_key, &cache_path)
                                            .await;
                                    }
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

        let mut new_history = history;
        let mut job_durations: HashMap<String, u64> = HashMap::new();
        for r in &final_results {
            job_durations.insert(r.job_name.clone(), r.duration_ms);
        }
        new_history.push(HistoryEntry {
            timestamp: chrono::Local::now(),
            job_durations,
        });
        while new_history.len() > 10 {
            new_history.remove(0);
        }
        let _ = save_history(&working_dir, &new_history);

        let slow_jobs = compute_slow_jobs(&final_results);
        let remote_cache_stats = remote_cache_client.get_local_stats().await;

        if matches!(self.config.output_mode, OutputMode::Terminal) {
            eprint!("\r");
            eprint!("{}", " ".repeat(150));
            eprint!("\r");
            print_summary(&final_results, &slow_jobs, &remote_cache_stats, remote_cache_client.is_enabled());
        }

        if matches!(self.config.output_mode, OutputMode::Json) {
            let pipeline_complete = serde_json::json!({
                "timestamp": chrono::Local::now().to_rfc3339(),
                "event_type": "pipeline_complete",
                "total_jobs": final_results.len(),
                "success_count": final_results.iter().filter(|r| matches!(r.status, JobStatus::Success)).count(),
                "failed_count": final_results.iter().filter(|r| matches!(r.status, JobStatus::Failed)).count(),
                "skipped_count": final_results.iter().filter(|r| matches!(r.status, JobStatus::Skipped)).count(),
                "cancelled_count": final_results.iter().filter(|r| matches!(r.status, JobStatus::Cancelled)).count(),
                "total_duration_ms": final_results.iter().map(|r| r.duration_ms).sum::<u64>(),
                "slow_jobs": slow_jobs,
                "job_results": final_results,
                "remote_cache_stats": remote_cache_stats,
            });
            println!("{}", serde_json::to_string_pretty(&pipeline_complete).unwrap());
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

fn compute_trigger_skipped_jobs(
    pipeline: &Pipeline,
    trigger: &TriggerConfig,
    changed_files: &[String],
) -> HashSet<String> {
    let mut skipped: HashSet<String> = HashSet::new();
    if changed_files.is_empty() {
        return skipped;
    }

    let filtered_changed: Vec<&String> = changed_files
        .iter()
        .filter(|f| {
            !trigger.paths_exclude.iter().any(|pat| {
                glob::Pattern::new(pat)
                    .map(|p| p.matches(f))
                    .unwrap_or(false)
            })
        })
        .collect();

    if filtered_changed.is_empty() {
        for job in &pipeline.jobs {
            skipped.insert(job.name.clone());
        }
        return skipped;
    }

    let include_patterns: Vec<glob::Pattern> = trigger
        .paths_include
        .iter()
        .filter_map(|pat| glob::Pattern::new(pat).ok())
        .collect();

    if include_patterns.is_empty() {
        return skipped;
    }

    let stage_to_jobs: HashMap<String, Vec<String>> = {
        let mut map: HashMap<String, Vec<String>> = HashMap::new();
        for job in &pipeline.jobs {
            if let Some(stage) = &job.stage {
                map.entry(stage.clone()).or_default().push(job.name.clone());
            }
        }
        map
    };

    let mut matched_stages: HashSet<String> = HashSet::new();
    for stage in &pipeline.stages {
        if include_patterns.iter().any(|pat| {
            filtered_changed.iter().any(|f| pat.matches(f))
        }) {
            matched_stages.insert(stage.clone());
        }
    }

    let stage_patterns: HashMap<String, Vec<glob::Pattern>> = {
        let mut map = HashMap::new();
        for (i, stage) in pipeline.stages.iter().enumerate() {
            if i < trigger.paths_include.len() {
                if let Ok(pat) = glob::Pattern::new(&trigger.paths_include[i]) {
                    map.insert(stage.clone(), vec![pat]);
                }
            }
        }
        map
    };

    let _ = stage_patterns;

    for (stage, jobs) in &stage_to_jobs {
        let stage_has_match = include_patterns.iter().any(|pat| {
            filtered_changed.iter().any(|f| pat.matches(f))
        });
        if !stage_has_match {
            for job in jobs {
                skipped.insert(job.clone());
            }
        }
    }

    skipped
}

fn load_history(working_dir: &PathBuf) -> (Vec<HistoryEntry>, HashMap<String, u64>) {
    let ci_dir = working_dir.join(".ci");
    let history_path = ci_dir.join("history.json");
    let entries: Vec<HistoryEntry> = if history_path.exists() {
        if let Ok(content) = std::fs::read_to_string(&history_path) {
            serde_json::from_str(&content).unwrap_or_default()
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    let mut sum_map: HashMap<String, (u64, usize)> = HashMap::new();
    for entry in &entries {
        for (job, dur) in &entry.job_durations {
            let e = sum_map.entry(job.clone()).or_insert((0, 0));
            e.0 += dur;
            e.1 += 1;
        }
    }

    let avg_map: HashMap<String, u64> = sum_map
        .into_iter()
        .map(|(k, (sum, cnt))| (k, if cnt > 0 { sum / cnt as u64 } else { 0 }))
        .collect();

    (entries, avg_map)
}

fn save_history(working_dir: &PathBuf, history: &[HistoryEntry]) -> Result<()> {
    let ci_dir = working_dir.join(".ci");
    std::fs::create_dir_all(&ci_dir).ok();
    let history_path = ci_dir.join("history.json");
    let content = serde_json::to_string_pretty(history)?;
    std::fs::write(&history_path, content)?;
    Ok(())
}

fn compute_slow_jobs(results: &[JobResult]) -> Vec<SlowJobInfo> {
    let total_ms: u64 = results.iter().map(|r| r.duration_ms).sum();
    let count = results.iter().filter(|r| r.duration_ms > 0).count() as u64;
    if count == 0 || total_ms == 0 {
        return Vec::new();
    }
    let avg_ms = total_ms / count;
    let threshold = avg_ms * 2;

    let mut slow: Vec<SlowJobInfo> = results
        .iter()
        .filter(|r| r.duration_ms > threshold)
        .map(|r| SlowJobInfo {
            job_name: r.job_name.clone(),
            duration_ms: r.duration_ms,
            percentage: if total_ms > 0 {
                (r.duration_ms as f64 / total_ms as f64) * 100.0
            } else {
                0.0
            },
        })
        .collect();

    slow.sort_by(|a, b| b.duration_ms.cmp(&a.duration_ms));
    slow
}

fn compute_file_hash(path: &std::path::Path) -> Result<String> {
    use sha2::{Digest, Sha256};
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 8192];
    use std::io::Read;
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }
    let result = hasher.finalize();
    Ok(format!("{:x}", result))
}
