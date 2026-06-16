use crate::models::*;
use anyhow::{Context, Result};
use colored::Colorize;
use std::collections::HashMap;
use std::path::PathBuf;

pub fn format_status_emoji(status: &JobStatus) -> String {
    match status {
        JobStatus::Pending => "⏳".to_string(),
        JobStatus::Running => "🔵".to_string(),
        JobStatus::Success => "✅".to_string(),
        JobStatus::Failed => "❌".to_string(),
        JobStatus::Skipped => "⏭".to_string(),
        JobStatus::Cancelled => "🚫".to_string(),
    }
}

pub fn format_status_colored(status: &JobStatus) -> String {
    let s = status.to_string();
    match status {
        JobStatus::Success => s.green().to_string(),
        JobStatus::Failed => s.red().to_string(),
        JobStatus::Skipped => s.yellow().to_string(),
        JobStatus::Running => s.cyan().to_string(),
        JobStatus::Cancelled => s.magenta().to_string(),
        JobStatus::Pending => s.dimmed().to_string(),
    }
}

pub fn format_duration(ms: u64) -> String {
    if ms < 1000 {
        format!("{}ms", ms)
    } else if ms < 60_000 {
        format!("{:.2}s", ms as f64 / 1000.0)
    } else {
        let mins = ms / 60_000;
        let secs = (ms % 60_000) / 1000;
        format!("{}m{}s", mins, secs)
    }
}

pub fn print_summary(
    results: &[JobResult],
    slow_jobs: &[crate::models::SlowJobInfo],
    remote_cache_stats: &crate::models::RemoteCacheStats,
    remote_cache_enabled: bool,
) {
    println!();
    println!("{}", "=".repeat(80).bold());
    println!("{}", "Pipeline Summary".bold());
    println!("{}", "=".repeat(80).bold());

    let header = format!(
        "{:<4} {:<40} {:<12} {:<12} {:<8} {}",
        "#", "Job", "Status", "Duration", "Retries", "Message"
    );
    println!("{}", header.bold());
    println!("{}", "-".repeat(80));

    let mut success = 0;
    let mut failed = 0;
    let mut skipped = 0;
    let mut cancelled = 0;
    let mut total_ms = 0u64;

    let slow_names: std::collections::HashSet<&str> = slow_jobs.iter().map(|s| s.job_name.as_str()).collect();

    for (i, r) in results.iter().enumerate() {
        let msg = r.message.clone().unwrap_or_default();
        let msg_truncated = if msg.chars().count() > 40 {
            let mut s: String = msg.chars().take(37).collect();
            s.push_str("...");
            s
        } else {
            msg
        };
        let job_name = truncate(&r.job_name, 38);
        let is_slow = slow_names.contains(r.job_name.as_str());
        let line = format!(
            "{:<4} {:<40} {:<12} {:<12} {:<8} {}",
            i + 1,
            job_name,
            format_status_colored(&r.status),
            format_duration(r.duration_ms),
            r.retry_count,
            msg_truncated
        );
        if is_slow {
            println!("{}", line.yellow().bold());
        } else {
            println!("{}", line);
        }
        match r.status {
            JobStatus::Success => success += 1,
            JobStatus::Failed => failed += 1,
            JobStatus::Skipped => skipped += 1,
            JobStatus::Cancelled => cancelled += 1,
            _ => {}
        }
        total_ms += r.duration_ms;
    }

    println!("{}", "-".repeat(80));
    println!(
        "Total: {} jobs, {} success, {} failed, {} skipped, {} cancelled",
        results.len(),
        success.to_string().green(),
        failed.to_string().red(),
        skipped.to_string().yellow(),
        cancelled.to_string().magenta()
    );
    println!("Total duration: {}", format_duration(total_ms));
    println!();

    if remote_cache_enabled {
        println!("{}", "Remote Cache".bold());
        println!("{}", "-".repeat(80));
        println!(
            "  Hits:    {}  |  Misses:  {}  |  Pushes:  {}",
            remote_cache_stats.hits.to_string().green(),
            remote_cache_stats.misses.to_string().yellow(),
            remote_cache_stats.pushes.to_string().cyan()
        );
        let total_requests = remote_cache_stats.hits + remote_cache_stats.misses;
        let hit_rate = if total_requests > 0 {
            (remote_cache_stats.hits as f64 / total_requests as f64) * 100.0
        } else {
            0.0
        };
        println!("  Hit rate: {:.1}%", hit_rate);
        println!();
    }

    if !slow_jobs.is_empty() {
        println!("{}", "Performance Insights".bold());
        println!("{}", "-".repeat(80));
        println!("⚠  Slow Jobs (duration > 2x average, marked yellow above):");
        for s in slow_jobs {
            println!(
                "   - {}: {} ({:.1}% of total)",
                s.job_name.yellow().bold(),
                format_duration(s.duration_ms),
                s.percentage
            );
        }
        println!();
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut truncated: String = s.chars().take(max - 3).collect();
        truncated.push_str("...");
        truncated
    }
}

pub fn print_progress_update(
    running: &[String],
    completed: &HashMap<String, JobResult>,
    pending: &[String],
    total: usize,
    start_time: chrono::DateTime<chrono::Local>,
) {
    let now = chrono::Local::now();
    let elapsed = (now - start_time).num_seconds();
    let done_count = completed.len();
    let progress_pct = if total == 0 { 0 } else { (done_count * 100) / total };
    let bar_len = 30;
    let filled = if total == 0 {
        0
    } else {
        (done_count * bar_len) / total
    };
    let bar: String = (0..filled).map(|_| '█').chain(
        (filled..bar_len).map(|_| '░')
    ).collect();

    let mut line = format!(
        "\r[{}] {}% | {}/{} done | {} running | {} queued | {}s elapsed",
        bar,
        progress_pct,
        done_count,
        total,
        running.len(),
        pending.len(),
        elapsed
    );
    if line.chars().count() < 100 {
        line.push_str(&" ".repeat(100 - line.chars().count()));
    }
    eprint!("{}", line);
}

pub fn generate_junit_xml(results: &[JobResult]) -> String {
    let mut test_cases = Vec::new();
    for r in results {
        let name = xml_escape(&r.job_name);
        let time_secs = r.duration_ms as f64 / 1000.0;
        let mut tc = format!(
            r#"    <testcase name="{}" time="{:.3}""#,
            name, time_secs
        );
        match r.status {
            JobStatus::Success => {
                tc.push_str("/>\n");
            }
            JobStatus::Failed => {
                let msg = xml_escape(&r.message.clone().unwrap_or_default());
                tc.push_str(">\n");
                tc.push_str(&format!(
                    r#"      <failure message="{}">{}</failure>"#,
                    msg, msg
                ));
                tc.push_str("\n    </testcase>\n");
            }
            JobStatus::Skipped => {
                tc.push_str(">\n");
                tc.push_str("      <skipped message=\"Job skipped\"/>\n");
                tc.push_str("    </testcase>\n");
            }
            JobStatus::Cancelled => {
                tc.push_str(">\n");
                tc.push_str("      <skipped message=\"Job cancelled\"/>\n");
                tc.push_str("    </testcase>\n");
            }
            _ => {
                tc.push_str("/>\n");
            }
        }
        test_cases.push(tc);
    }

    let tests = results.len();
    let failures = results.iter().filter(|r| matches!(r.status, JobStatus::Failed)).count();
    let skipped = results.iter().filter(|r| matches!(r.status, JobStatus::Skipped | JobStatus::Cancelled)).count();
    let errors = 0;
    let total_time: f64 = results.iter().map(|r| r.duration_ms as f64 / 1000.0).sum();

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<testsuites>
  <testsuite name="ci-pipeline" tests="{}" failures="{}" errors="{}" skipped="{}" time="{:.3}">
{}  </testsuite>
</testsuites>
"#,
        tests, failures, errors, skipped, total_time,
        test_cases.join("")
    )
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

pub fn generate_html_report(results: &[JobResult], base_dir: &str) -> Result<()> {
    let mut rows = String::new();
    for r in results {
        let status_class = match r.status {
            JobStatus::Success => "success",
            JobStatus::Failed => "failed",
            JobStatus::Skipped => "skipped",
            JobStatus::Cancelled => "cancelled",
            JobStatus::Running => "running",
            JobStatus::Pending => "pending",
        };
        let msg = r.message.clone().unwrap_or_default();
        let retry_cell = if r.retry_count > 0 {
            format!("<span class='retry'>🔄 {}</span>", r.retry_count)
        } else {
            "-".to_string()
        };
        let started = r.started_at
            .map(|d| d.format("%H:%M:%S").to_string())
            .unwrap_or_default();
        let finished = r.finished_at
            .map(|d| d.format("%H:%M:%S").to_string())
            .unwrap_or_default();
        rows.push_str(&format!(
            r#"    <tr class="{status_class}">
      <td>{job}</td>
      <td><span class="badge {status_class}">{emoji} {status}</span></td>
      <td>{duration}</td>
      <td>{retry}</td>
      <td>{started}</td>
      <td>{finished}</td>
      <td>{msg}</td>
    </tr>
"#,
            status_class = status_class,
            job = html_escape(&r.job_name),
            emoji = format_status_emoji(&r.status),
            status = r.status,
            duration = format_duration(r.duration_ms),
            retry = retry_cell,
            started = started,
            finished = finished,
            msg = html_escape(&msg)
        ));
    }

    let stats_success = results.iter().filter(|r| matches!(r.status, JobStatus::Success)).count();
    let stats_failed = results.iter().filter(|r| matches!(r.status, JobStatus::Failed)).count();
    let stats_skipped = results.iter().filter(|r| matches!(r.status, JobStatus::Skipped | JobStatus::Cancelled)).count();
    let total_duration: f64 = results.iter().map(|r| r.duration_ms as f64 / 1000.0).sum();
    let total_time = format_duration(results.iter().map(|r| r.duration_ms).sum());
    let generated_at = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();

    let html = format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>CI Pipeline Report</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: #f5f7fa;
      padding: 20px;
      color: #333;
    }}
    .container {{ max-width: 1200px; margin: 0 auto; }}
    .header {{
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      color: white;
      padding: 30px;
      border-radius: 12px;
      margin-bottom: 24px;
    }}
    .header h1 {{ font-size: 28px; margin-bottom: 8px; }}
    .header p {{ opacity: 0.9; }}
    .stats {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 16px;
      margin-bottom: 24px;
    }}
    .stat-card {{
      background: white;
      border-radius: 8px;
      padding: 20px;
      box-shadow: 0 2px 4px rgba(0,0,0,0.05);
      text-align: center;
    }}
    .stat-card .number {{ font-size: 32px; font-weight: bold; margin-bottom: 4px; }}
    .stat-card .label {{ font-size: 14px; color: #666; }}
    .stat-card.success .number {{ color: #10b981; }}
    .stat-card.failed .number {{ color: #ef4444; }}
    .stat-card.skipped .number {{ color: #f59e0b; }}
    .stat-card.total .number {{ color: #3b82f6; }}
    .table-container {{
      background: white;
      border-radius: 12px;
      overflow: hidden;
      box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{
      padding: 14px 16px;
      text-align: left;
      border-bottom: 1px solid #e5e7eb;
    }}
    th {{
      background: #f9fafb;
      font-weight: 600;
      color: #374151;
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }}
    tr.success td {{ background: rgba(16, 185, 129, 0.05); }}
    tr.failed td {{ background: rgba(239, 68, 68, 0.05); }}
    tr.skipped td, tr.cancelled td {{ background: rgba(245, 158, 11, 0.05); }}
    .badge {{
      display: inline-flex;
      align-items: center;
      gap: 4px;
      padding: 4px 10px;
      border-radius: 20px;
      font-size: 12px;
      font-weight: 600;
    }}
    .badge.success {{ background: #d1fae5; color: #065f46; }}
    .badge.failed {{ background: #fee2e2; color: #991b1b; }}
    .badge.skipped, .badge.cancelled {{ background: #fef3c7; color: #92400e; }}
    .badge.running {{ background: #dbeafe; color: #1e40af; }}
    .badge.pending {{ background: #e5e7eb; color: #374151; }}
    .retry {{ color: #92400e; font-weight: 600; }}
    .footer {{
      text-align: center;
      padding: 20px;
      color: #9ca3af;
      font-size: 13px;
      margin-top: 24px;
    }}
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>CI Pipeline Report</h1>
      <p>Generated at {generated_at} &middot; Total runtime: {total_time}</p>
    </div>

    <div class="stats">
      <div class="stat-card total">
        <div class="number">{total}</div>
        <div class="label">Total Jobs</div>
      </div>
      <div class="stat-card success">
        <div class="number">{success}</div>
        <div class="label">Success</div>
      </div>
      <div class="stat-card failed">
        <div class="number">{failed}</div>
        <div class="label">Failed</div>
      </div>
      <div class="stat-card skipped">
        <div class="number">{skipped}</div>
        <div class="label">Skipped</div>
      </div>
    </div>

    <div class="table-container">
      <table>
        <thead>
          <tr>
            <th>Job Name</th>
            <th>Status</th>
            <th>Duration</th>
            <th>Retries</th>
            <th>Started</th>
            <th>Finished</th>
            <th>Message</th>
          </tr>
        </thead>
        <tbody>
{rows}        </tbody>
      </table>
    </div>

    <div class="footer">
      ci-pipeline &middot; Generated {generated_at}
    </div>
  </div>
</body>
</html>
"#,
        generated_at = generated_at,
        total_time = total_time,
        total = results.len(),
        success = stats_success,
        failed = stats_failed,
        skipped = stats_skipped,
        rows = rows
    );

    let report_path = PathBuf::from(base_dir).join(".ci").join("report.html");
    if let Some(parent) = report_path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    std::fs::write(&report_path, html)
        .with_context(|| format!("Failed to write HTML report to {:?}", report_path))?;
    println!("HTML report written to: {}", report_path.display());
    Ok(())
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

pub fn print_ascii_dag(dag: &crate::dag::Dag) {
    let order = dag.topological_order().unwrap_or_default();
    let max_len = order.iter().map(|s| s.len()).max().unwrap_or(0) + 2;
    println!("\nDAG (Topological Order):");
    println!("{}", "─".repeat(max_len + 20));
    for (i, name) in order.iter().enumerate() {
        let deps = dag.dependencies_of(name);
        let dep_str = if deps.is_empty() {
            "─".to_string()
        } else {
            deps.join(", ")
        };
        let arrow = if deps.is_empty() {
            "◯".to_string()
        } else {
            "◉".to_string()
        };
        println!(
            "  {:<3} {} {:<width$} ← depends on: {}",
            i + 1,
            arrow,
            name,
            dep_str,
            width = max_len
        );
    }
    println!("{}", "─".repeat(max_len + 20));
    println!("  ◯ = root (no deps), ◉ = has dependencies\n");
}
