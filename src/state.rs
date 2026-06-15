use crate::models::*;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use sha2::{Digest, Sha256};

pub struct StateManager {
    pub base_dir: PathBuf,
}

impl StateManager {
    pub fn new(base_dir: &Path) -> Self {
        let state_dir = base_dir.join(".ci");
        std::fs::create_dir_all(&state_dir).ok();
        Self {
            base_dir: base_dir.to_path_buf(),
        }
    }

    pub fn state_file_path(&self) -> PathBuf {
        self.base_dir.join(".ci").join("state.json")
    }

    pub fn compute_pipeline_hash(&self, pipeline_content: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(pipeline_content.as_bytes());
        let result = hasher.finalize();
        format!("{:x}", result)
    }

    pub fn load_state(&self, pipeline_hash: &str) -> Option<StateFile> {
        let path = self.state_file_path();
        if !path.exists() {
            return None;
        }
        let content = std::fs::read_to_string(&path).ok()?;
        let state: StateFile = serde_json::from_str(&content).ok()?;
        if state.pipeline_hash != pipeline_hash {
            return None;
        }
        Some(state)
    }

    pub fn save_state(
        &self,
        pipeline_hash: &str,
        job_results: &HashMap<String, JobResult>,
    ) -> Result<()> {
        let state = StateFile {
            pipeline_hash: pipeline_hash.to_string(),
            job_results: job_results.clone(),
            timestamp: chrono::Local::now(),
        };
        let content = serde_json::to_string_pretty(&state)
            .context("Failed to serialize state")?;
        std::fs::write(self.state_file_path(), content)
            .context("Failed to write state file")?;
        Ok(())
    }

    pub fn clear_state(&self) -> Result<()> {
        let path = self.state_file_path();
        if path.exists() {
            std::fs::remove_file(&path).ok();
        }
        Ok(())
    }
}
