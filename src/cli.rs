use crate::logging::OutputMode;
use crate::scheduler::{Scheduler, SchedulerConfig};
use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "ci-pipeline")]
#[command(about = "CI Pipeline DSL parser and DAG scheduler executor", version, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    Run(RunArgs),
    Validate(ValidateArgs),
    Graph(GraphArgs),
    Clean(CleanArgs),
    Cache(CacheArgs),
}

#[derive(Debug, Parser)]
pub struct CacheArgs {
    #[arg(long, short = 'f', default_value = "pipeline.yml")]
    pub file: String,

    #[command(subcommand)]
    pub command: CacheCommands,
}

#[derive(Debug, Subcommand)]
pub enum CacheCommands {
    List(CacheListArgs),
    Push(CachePushArgs),
    Pull(CachePullArgs),
    Gc,
    Stats,
}

#[derive(Debug, Parser)]
pub struct CacheListArgs {
    #[arg(long)]
    pub namespace: Option<String>,
}

#[derive(Debug, Parser)]
pub struct CachePushArgs {
    pub path: String,

    #[arg(long)]
    pub key: String,

    #[arg(long)]
    pub namespace: Option<String>,
}

#[derive(Debug, Parser)]
pub struct CachePullArgs {
    pub key: String,

    #[arg(long)]
    pub output: String,

    #[arg(long)]
    pub namespace: Option<String>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    Terminal,
    Json,
    Junit,
    Html,
}

impl From<OutputFormat> for OutputMode {
    fn from(f: OutputFormat) -> Self {
        match f {
            OutputFormat::Terminal => OutputMode::Terminal,
            OutputFormat::Json => OutputMode::Json,
            OutputFormat::Junit => OutputMode::Junit,
            OutputFormat::Html => OutputMode::Html,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum GraphFormat {
    Dot,
    Ascii,
}

#[derive(Debug, Parser)]
pub struct RunArgs {
    #[arg(long, short = 'f', default_value = "pipeline.yml")]
    pub file: String,

    #[arg(long, short = 'p', default_value_t = 4)]
    pub parallel: usize,

    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,

    #[arg(long, default_value_t = false)]
    pub resume: bool,

    #[arg(long, default_value_t = false)]
    pub dry_run: bool,

    #[arg(long)]
    pub filter: Option<String>,

    #[arg(long, short = 'C')]
    pub working_dir: Option<PathBuf>,

    #[arg(long, default_value_t = 3600)]
    pub timeout: u64,

    #[arg(long, default_value_t = 0)]
    pub retry: u32,

    #[arg(long)]
    pub changed_files: Option<String>,
}

#[derive(Debug, Parser)]
pub struct ValidateArgs {
    #[arg(long, short = 'f', default_value = "pipeline.yml")]
    pub file: String,

    #[arg(long, default_value_t = false)]
    pub lint: bool,

    #[arg(long, default_value_t = false)]
    pub strict: bool,
}

#[derive(Debug, Parser)]
pub struct GraphArgs {
    #[arg(long, short = 'f', default_value = "pipeline.yml")]
    pub file: String,

    #[arg(long, value_enum, default_value_t = GraphFormat::Ascii)]
    pub format: GraphFormat,
}

#[derive(Debug, Parser)]
pub struct CleanArgs {
    #[arg(long, default_value_t = false)]
    pub all: bool,
}

pub fn parse_args() -> Cli {
    Cli::parse()
}

pub async fn handle_run(args: RunArgs) -> Result<()> {
    let working_dir = args
        .working_dir
        .clone()
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));

    let changed_files = parse_changed_files(&args.changed_files);

    let config = SchedulerConfig {
        file: args.file.clone(),
        parallel: args.parallel,
        output_mode: args.output.into(),
        resume: args.resume,
        dry_run: args.dry_run,
        filter: args.filter.clone(),
        working_dir: working_dir.clone(),
        cache_ttl_days: 7,
        default_timeout: args.timeout,
        default_retry: args.retry,
        changed_files,
        remote_cache: crate::models::RemoteCacheConfig::default(),
    };

    let scheduler = Scheduler::new(config);
    let results = scheduler.run().await?;

    let has_failed = results.iter().any(|r| matches!(r.status, crate::models::JobStatus::Failed));
    let has_cancelled = results.iter().any(|r| matches!(r.status, crate::models::JobStatus::Cancelled));

    if has_failed {
        std::process::exit(1);
    }
    if has_cancelled && !results.iter().all(|r| matches!(r.status, crate::models::JobStatus::Success | crate::models::JobStatus::Skipped | crate::models::JobStatus::Cancelled)) {
        std::process::exit(130);
    }
    Ok(())
}

fn parse_changed_files(arg: &Option<String>) -> Vec<String> {
    match arg {
        Some(s) if s == "-" => {
            let mut input = String::new();
            std::io::Read::read_to_string(&mut std::io::stdin(), &mut input).unwrap_or_default();
            input
                .split(|c| c == ',' || c == '\n' || c == ' ')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        }
        Some(s) => {
            s.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        }
        None => Vec::new(),
    }
}

pub fn handle_validate(args: ValidateArgs) -> Result<()> {
    let content = std::fs::read_to_string(&args.file)
        .map_err(|e| anyhow!("Failed to read pipeline file {}: {}", args.file, e))?;

    let pipeline = crate::parser::parse_pipeline_from_str(&content)?;

    println!("✓ YAML parsing successful");

    let errors = crate::validator::validate_pipeline(&pipeline)?;
    if !errors.is_empty() {
        println!("✗ Validation failed with {} error(s):", errors.len());
        for (i, e) in errors.iter().enumerate() {
            println!("  {}. [{}] {}", i + 1, e.location, e.message);
        }
        std::process::exit(1);
    }
    println!("✓ Validation passed");

    let jobs_expanded = crate::matrix::expand_matrix_jobs(pipeline.jobs.clone());
    println!("  - Stages: {}", pipeline.stages.len());
    println!("  - Jobs defined: {}", pipeline.jobs.len());
    println!("  - Jobs after matrix expansion: {}", jobs_expanded.len());
    println!("  - Variables: {}", pipeline.variables.len());

    let dag = crate::dag::Dag::build(&jobs_expanded)?;
    let _order = dag.topological_order()?;
    println!("✓ DAG cycle check passed (no cycles detected)");

    let mut lint_warnings: Vec<crate::validator::LintWarning> = Vec::new();
    if args.lint {
        lint_warnings = crate::validator::lint_pipeline(&pipeline, &jobs_expanded)?;
        if lint_warnings.is_empty() {
            println!("✓ Lint passed (no warnings)");
        } else {
            println!("⚠ Lint found {} warning(s):", lint_warnings.len());
            for (i, w) in lint_warnings.iter().enumerate() {
                println!("  {}. [warning] [{}] {}", i + 1, w.location, w.message);
            }
        }
    }

    println!();
    if args.lint && !lint_warnings.is_empty() && args.strict {
        println!("Summary: Pipeline definition has validation errors (strict mode).");
        std::process::exit(1);
    } else {
        println!("Summary: Pipeline definition is valid.");
    }

    Ok(())
}

pub fn handle_graph(args: GraphArgs) -> Result<()> {
    let content = std::fs::read_to_string(&args.file)
        .map_err(|e| anyhow!("Failed to read pipeline file {}: {}", args.file, e))?;

    let pipeline = crate::parser::parse_pipeline_from_str(&content)?;
    let errors = crate::validator::validate_pipeline(&pipeline)?;
    if !errors.is_empty() {
        return Err(anyhow!(
            "Cannot generate graph - validation has errors: {:?}",
            errors
        ));
    }
    let jobs = crate::matrix::expand_matrix_jobs(pipeline.jobs.clone());
    let dag = crate::dag::Dag::build(&jobs)?;

    match args.format {
        GraphFormat::Dot => {
            println!("{}", dag.as_dot());
        }
        GraphFormat::Ascii => {
            crate::reporting::print_ascii_dag(&dag);
        }
    }
    Ok(())
}

pub fn handle_clean(args: CleanArgs) -> Result<()> {
    let working_dir = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let ci_dir = working_dir.join(".ci");

    if !ci_dir.exists() {
        println!(".ci directory not found, nothing to clean.");
        return Ok(());
    }

    let logs_dir = ci_dir.join("logs");
    let artifacts_dir = ci_dir.join("artifacts");
    let cache_dir = ci_dir.join("cache");
    let state_file = ci_dir.join("state.json");
    let report_file = ci_dir.join("report.html");
    let junit_file = ci_dir.join("junit.xml");

    let mut removed = 0;

    if logs_dir.exists() {
        match std::fs::remove_dir_all(&logs_dir) {
            Ok(_) => {
                println!("✓ Removed logs directory: {}", logs_dir.display());
                removed += 1;
            }
            Err(e) => println!("⚠ Failed to remove logs: {}", e),
        }
    }

    if artifacts_dir.exists() {
        match std::fs::remove_dir_all(&artifacts_dir) {
            Ok(_) => {
                println!("✓ Removed artifacts directory: {}", artifacts_dir.display());
                removed += 1;
            }
            Err(e) => println!("⚠ Failed to remove artifacts: {}", e),
        }
    }

    if args.all && cache_dir.exists() {
        match std::fs::remove_dir_all(&cache_dir) {
            Ok(_) => {
                println!("✓ Removed cache directory: {}", cache_dir.display());
                removed += 1;
            }
            Err(e) => println!("⚠ Failed to remove cache: {}", e),
        }
    } else if cache_dir.exists() {
        let cm = crate::artifacts::CacheManager::new(&working_dir, 7);
        match cm.cleanup_expired() {
            Ok(n) => {
                if n > 0 {
                    println!("✓ Cleaned {} expired cache entries", n);
                    removed += n;
                } else {
                    println!("ℹ No expired cache entries found");
                }
            }
            Err(e) => println!("⚠ Failed to cleanup cache: {}", e),
        }
    }

    for f in [state_file, report_file, junit_file] {
        if f.exists() {
            match std::fs::remove_file(&f) {
                Ok(_) => {
                    println!("✓ Removed: {}", f.display());
                    removed += 1;
                }
                Err(e) => println!("⚠ Failed to remove {}: {}", f.display(), e),
            }
        }
    }

    if removed == 0 {
        println!("Nothing to clean (run with --all to also remove cache).");
    } else {
        println!("\nCleanup complete. Removed {} item(s).", removed);
    }

    Ok(())
}

pub async fn handle_cache(args: CacheArgs) -> Result<()> {
    let pipeline_content = std::fs::read_to_string(&args.file)
        .map_err(|e| anyhow::anyhow!("Failed to read pipeline file {}: {}", args.file, e))?;

    let pipeline = crate::parser::parse_pipeline_from_str(&pipeline_content)?;

    if !pipeline.remote_cache.enabled {
        eprintln!("⚠  Remote cache is not enabled in pipeline.yml");
    }

    let default_namespace = crate::remote_cache::get_default_namespace();
    let client = crate::remote_cache::RemoteCacheClient::new(
        pipeline.remote_cache.clone(),
        &default_namespace,
    );

    if !client.is_enabled() {
        return Err(anyhow::anyhow!(
            "Remote cache is not configured. Please set remote_cache.url in pipeline.yml"
        ));
    }

    match args.command {
        CacheCommands::List(list_args) => {
            let entries = client.list_namespace(list_args.namespace.as_deref()).await?;
            let ns = list_args.namespace.unwrap_or_else(|| client.namespace.clone());

            println!("Remote Cache Entries (namespace: {})", ns);
            println!("{}", "─".repeat(80));

            if entries.is_empty() {
                println!("  (empty)");
            } else {
                for entry in &entries {
                    let size_str = format_size(entry.size_bytes);
                    println!(
                        "  {:<50} {:>10}  last accessed: {}",
                        truncate_for_list(&entry.key, 48),
                        size_str,
                        entry.last_accessed
                    );
                }
            }
            println!();
            println!("Total: {} entries", entries.len());
        }

        CacheCommands::Push(push_args) => {
            let path = std::path::Path::new(&push_args.path);
            if !path.exists() {
                return Err(anyhow::anyhow!("File not found: {}", push_args.path));
            }

            println!("Pushing to remote cache...");
            println!("  Key: {}", push_args.key);
            println!("  File: {}", push_args.path);
            if let Some(ns) = &push_args.namespace {
                println!("  Namespace: {}", ns);
            }

            let mut client = client;
            if let Some(ns) = push_args.namespace {
                client.namespace = ns;
            }

            client.upload_cache(&push_args.key, path).await?;
            println!("✓ Push successful");
        }

        CacheCommands::Pull(pull_args) => {
            let output = std::path::Path::new(&pull_args.output);
            if let Some(parent) = output.parent() {
                std::fs::create_dir_all(parent).ok();
            }

            println!("Pulling from remote cache...");
            println!("  Key: {}", pull_args.key);
            println!("  Output: {}", pull_args.output);
            if let Some(ns) = &pull_args.namespace {
                println!("  Namespace: {}", ns);
            }

            let mut client = client;
            if let Some(ns) = pull_args.namespace {
                client.namespace = ns;
            }

            let success = client.download_cache(&pull_args.key, output).await?;
            if success {
                println!("✓ Pull successful");
            } else {
                println!("✗ Cache key not found");
                std::process::exit(1);
            }
        }

        CacheCommands::Gc => {
            println!("Triggering garbage collection...");
            let result = client.trigger_gc().await?;
            println!("✓ GC complete");
            println!("  Removed: {} entries", result.removed);
            println!("  Freed: {}", format_size(result.freed_bytes));
        }

        CacheCommands::Stats => {
            let stats = client.get_stats().await?;

            println!("Remote Cache Statistics");
            println!("{}", "─".repeat(80));
            println!("  Total entries: {}", stats.total_entries);
            println!("  Total size: {}", format_size(stats.total_size_bytes));
            println!("  Hits (24h): {}", stats.hits_last_24h);
            println!("  Misses (24h): {}", stats.misses_last_24h);
            println!();
            println!("  Namespace distribution:");
            if stats.namespace_counts.is_empty() {
                println!("    (none)");
            } else {
                for (ns, count) in &stats.namespace_counts {
                    println!("    {}: {} entries", ns, count);
                }
            }
            println!();
        }
    }

    Ok(())
}

fn format_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

fn truncate_for_list(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut t: String = s.chars().take(max - 3).collect();
        t.push_str("...");
        t
    }
}
