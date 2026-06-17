use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    #[serde(default)]
    pub variables: HashMap<String, String>,
    #[serde(default)]
    pub stages: Vec<String>,
    #[serde(default)]
    pub trigger: Option<TriggerConfig>,
    #[serde(default)]
    pub remote_cache: RemoteCacheConfig,
    pub jobs: Vec<Job>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RemoteCacheConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default)]
    pub namespace: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RemoteCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub pushes: u64,
    pub evictions: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TriggerConfig {
    #[serde(default)]
    pub paths_include: Vec<String>,
    #[serde(default)]
    pub paths_exclude: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum IsolationMode {
    None,
    Container,
}

impl Default for IsolationMode {
    fn default() -> Self {
        IsolationMode::None
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub timestamp: chrono::DateTime<chrono::Local>,
    pub job_durations: HashMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlowJobInfo {
    pub job_name: String,
    pub duration_ms: u64,
    pub percentage: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub name: String,
    #[serde(default)]
    pub stage: Option<String>,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub steps: Vec<Step>,
    #[serde(default)]
    pub condition: Option<String>,
    #[serde(default)]
    pub timeout: Option<u64>,
    #[serde(default)]
    pub retry: Option<u32>,
    #[serde(default)]
    pub artifacts: Vec<ArtifactConfig>,
    #[serde(default)]
    pub cache: Vec<String>,
    #[serde(default)]
    pub services: Vec<ServiceConfig>,
    #[serde(default)]
    pub matrix: Option<HashMap<String, Vec<String>>>,
    #[serde(default)]
    pub needs_artifacts: Vec<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub isolation: IsolationMode,
    #[serde(default)]
    pub image: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactConfig {
    #[serde(default)]
    pub paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceConfig {
    pub image: String,
    #[serde(default)]
    pub env: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Step {
    #[serde(default)]
    pub name: Option<String>,
    pub run: String,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub allow_failure: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobStatus {
    Pending,
    Running,
    Success,
    Failed,
    Skipped,
    Cancelled,
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobStatus::Pending => write!(f, "pending"),
            JobStatus::Running => write!(f, "running"),
            JobStatus::Success => write!(f, "success"),
            JobStatus::Failed => write!(f, "failed"),
            JobStatus::Skipped => write!(f, "skipped"),
            JobStatus::Cancelled => write!(f, "cancelled"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResult {
    pub job_name: String,
    pub status: JobStatus,
    pub duration_ms: u64,
    pub retry_count: u32,
    pub message: Option<String>,
    pub outputs: HashMap<String, String>,
    pub started_at: Option<chrono::DateTime<chrono::Local>>,
    pub finished_at: Option<chrono::DateTime<chrono::Local>>,
}

#[derive(Debug, Clone)]
pub struct RuntimeJob {
    pub job: Job,
    pub status: JobStatus,
    pub result: Option<JobResult>,
    pub outputs: HashMap<String, String>,
    pub matrix_params: Option<HashMap<String, String>>,
}

impl RuntimeJob {
    pub fn new(job: Job) -> Self {
        Self {
            job,
            status: JobStatus::Pending,
            result: None,
            outputs: HashMap::new(),
            matrix_params: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateFile {
    pub pipeline_hash: String,
    pub job_results: HashMap<String, JobResult>,
    pub timestamp: chrono::DateTime<chrono::Local>,
}
