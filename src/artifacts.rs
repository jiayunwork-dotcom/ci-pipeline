use crate::models::*;
use anyhow::{Context, Result};
use flate2::write::GzEncoder;
use flate2::Compression;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use tar::Builder;
use walkdir::WalkDir;

pub struct ArtifactManager {
    pub base_dir: PathBuf,
}

impl ArtifactManager {
    pub fn new(base_dir: &Path) -> Self {
        let artifacts_dir = base_dir.join(".ci").join("artifacts");
        std::fs::create_dir_all(&artifacts_dir).ok();
        Self {
            base_dir: base_dir.to_path_buf(),
        }
    }

    pub fn artifacts_dir(&self) -> PathBuf {
        self.base_dir.join(".ci").join("artifacts")
    }

    pub fn job_artifact_path(&self, job_name: &str) -> PathBuf {
        let safe_name = job_name.replace('/', "_").replace(' ', "_");
        self.artifacts_dir().join(format!("{}.tar.gz", safe_name))
    }

    pub fn job_artifact_dir(&self, job_name: &str) -> PathBuf {
        let safe_name = job_name.replace('/', "_").replace(' ', "_");
        self.artifacts_dir().join(safe_name)
    }

    pub fn collect_artifacts(
        &self,
        job: &Job,
        working_dir: &Path,
    ) -> Result<Vec<String>> {
        let mut collected = Vec::new();
        for artifact in &job.artifacts {
            for path_pattern in &artifact.paths {
                let resolved = working_dir.join(path_pattern);
                let matches = glob_pattern(resolved.to_string_lossy().as_ref())?;
                for m in matches {
                    collected.push(m);
                }
            }
        }
        Ok(collected)
    }

    pub fn package_artifacts(&self, job: &Job, working_dir: &Path) -> Result<Option<PathBuf>> {
        let paths = self.collect_artifacts(job, working_dir)?;
        if paths.is_empty() {
            return Ok(None);
        }

        let artifact_path = self.job_artifact_path(&job.name);
        if let Some(parent) = artifact_path.parent() {
            std::fs::create_dir_all(parent).ok();
        }

        let file = fs::File::create(&artifact_path)
            .with_context(|| format!("Failed to create artifact file: {:?}", artifact_path))?;
        let gz = GzEncoder::new(file, Compression::default());
        let mut tar = Builder::new(gz);

        for path in &paths {
            let path = Path::new(path);
            let rel = match path.strip_prefix(working_dir) {
                Ok(r) => r.to_path_buf(),
                Err(_) => path.file_name()
                    .map(|n| PathBuf::from(n))
                    .unwrap_or_else(|| path.to_path_buf()),
            };
            if path.is_dir() {
                tar.append_dir_all(&rel, path).ok();
            } else if path.exists() {
                tar.append_path_with_name(path, &rel).ok();
            }
        }

        tar.finish().ok();
        Ok(Some(artifact_path))
    }

    pub fn restore_artifacts(&self, job_name: &str, working_dir: &Path) -> Result<bool> {
        let artifact_path = self.job_artifact_path(job_name);
        if !artifact_path.exists() {
            return Ok(false);
        }

        std::fs::create_dir_all(working_dir).ok();
        let file = fs::File::open(&artifact_path)
            .with_context(|| format!("Failed to open artifact file: {:?}", artifact_path))?;
        let gz = flate2::read::GzDecoder::new(file);
        let mut archive = tar::Archive::new(gz);
        archive
            .unpack(working_dir)
            .with_context(|| format!("Failed to unpack artifact: {:?}", artifact_path))?;
        Ok(true)
    }
}

fn glob_pattern(pattern: &str) -> Result<Vec<String>> {
    let mut results = Vec::new();
    let pat = Path::new(pattern);
    if pat.exists() {
        results.push(pat.to_string_lossy().to_string());
        return Ok(results);
    }

    let (dir, pattern) = if let Some(parent) = pat.parent() {
        if parent.exists() {
            let file_pat = pat.file_name().map(|f| f.to_string_lossy().to_string()).unwrap_or_default();
            (Some(parent), file_pat)
        } else {
            (None, String::new())
        }
    } else {
        (None, String::new())
    };

    if let Some(dir) = dir {
        let re = glob_to_regex(&pattern);
        if let Ok(regex) = regex::Regex::new(&re) {
            for entry in WalkDir::new(dir).into_iter().filter_map(|e| e.ok()) {
                let file_name = entry.file_name().to_string_lossy().to_string();
                if regex.is_match(&file_name) {
                    results.push(entry.path().to_string_lossy().to_string());
                }
            }
        }
    }

    if results.is_empty() {
        if let Ok(entries) = glob::glob(&pattern) {
            for entry in entries.flatten() {
                results.push(entry.to_string_lossy().to_string());
            }
        }
    }

    Ok(results)
}

fn glob_to_regex(pat: &str) -> String {
    let mut result = String::from("^");
    for c in pat.chars() {
        match c {
            '*' => result.push_str(".*"),
            '?' => result.push('.'),
            '.' => result.push_str("\\."),
            '+' => result.push_str("\\+"),
            '(' => result.push_str("\\("),
            ')' => result.push_str("\\)"),
            '[' => result.push_str("\\["),
            ']' => result.push_str("\\]"),
            '|' => result.push_str("\\|"),
            '^' => result.push_str("\\^"),
            '$' => result.push_str("\\$"),
            '\\' => result.push_str("\\\\"),
            c => result.push(c),
        }
    }
    result.push('$');
    result
}

pub struct CacheManager {
    pub base_dir: PathBuf,
    pub ttl_days: i64,
}

impl CacheManager {
    pub fn new(base_dir: &Path, ttl_days: i64) -> Self {
        let cache_dir = base_dir.join(".ci").join("cache");
        std::fs::create_dir_all(&cache_dir).ok();
        Self {
            base_dir: base_dir.to_path_buf(),
            ttl_days,
        }
    }

    pub fn cache_dir(&self) -> PathBuf {
        self.base_dir.join(".ci").join("cache")
    }

    pub fn compute_cache_key(&self, paths: &[String], working_dir: &Path) -> Result<String> {
        let mut hasher = Sha256::new();
        let mut all_paths: Vec<PathBuf> = Vec::new();
        for p in paths {
            let full = working_dir.join(p);
            if full.exists() {
                all_paths.push(full);
            }
        }

        let mut sorted = all_paths;
        sorted.sort();
        for path in sorted {
            if path.is_dir() {
                for entry in WalkDir::new(&path).into_iter().filter_map(|e| e.ok()) {
                    if entry.file_type().is_file() {
                        hasher.update(entry.path().to_string_lossy().as_bytes());
                        if let Ok(content) = std::fs::read(entry.path()) {
                            hasher.update(&content);
                        }
                    }
                }
            } else if path.is_file() {
                hasher.update(path.to_string_lossy().as_bytes());
                if let Ok(content) = std::fs::read(&path) {
                    hasher.update(&content);
                }
            }
        }

        let result = hasher.finalize();
        Ok(format!("{:x}", result))
    }

    pub fn compute_stable_cache_key(&self, job_name: &str, paths: &[String]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(job_name.as_bytes());
        let mut sorted_paths = paths.to_vec();
        sorted_paths.sort();
        for p in sorted_paths {
            hasher.update(p.as_bytes());
        }
        let result = hasher.finalize();
        let hex_full = format!("{:x}", result);
        format!("cache-{}-{}", job_name.replace('/', "_").replace(' ', "_"), &hex_full[..16])
    }

    pub fn cache_entry_path(&self, cache_key: &str) -> PathBuf {
        self.cache_dir().join(format!("{}.tar.gz", cache_key))
    }

    pub fn cache_metadata_path(&self, cache_key: &str) -> PathBuf {
        self.cache_dir().join(format!("{}.json", cache_key))
    }

    pub fn save_cache(
        &self,
        paths: &[String],
        cache_key: &str,
        working_dir: &Path,
    ) -> Result<PathBuf> {
        let cache_path = self.cache_entry_path(cache_key);
        let metadata_path = self.cache_metadata_path(cache_key);

        if let Some(parent) = cache_path.parent() {
            std::fs::create_dir_all(parent).ok();
        }

        let file = fs::File::create(&cache_path)
            .with_context(|| format!("Failed to create cache file: {:?}", cache_path))?;
        let gz = GzEncoder::new(file, Compression::default());
        let mut tar = Builder::new(gz);

        for pattern in paths {
            let full = working_dir.join(pattern);
            let matches = glob_pattern(full.to_string_lossy().as_ref())?;
            for path in matches {
                let p = Path::new(&path);
                let rel = match p.strip_prefix(working_dir) {
                    Ok(r) => r.to_path_buf(),
                    Err(_) => p.file_name()
                        .map(|n| PathBuf::from(n))
                        .unwrap_or_else(|| p.to_path_buf()),
                };
                if p.is_dir() {
                    tar.append_dir_all(&rel, p).ok();
                } else if p.exists() {
                    tar.append_path_with_name(p, &rel).ok();
                }
            }
        }

        tar.finish().ok();

        let metadata = serde_json::json!({
            "key": cache_key,
            "created_at": chrono::Local::now().to_rfc3339(),
            "paths": paths,
        });
        std::fs::write(&metadata_path, metadata.to_string()).ok();

        Ok(cache_path)
    }

    pub fn restore_cache(
        &self,
        cache_key: &str,
        working_dir: &Path,
    ) -> Result<bool> {
        let cache_path = self.cache_entry_path(cache_key);
        if !cache_path.exists() {
            return Ok(false);
        }

        let metadata_path = self.cache_metadata_path(cache_key);
        if metadata_path.exists() {
            if let Ok(content) = std::fs::read_to_string(&metadata_path) {
                if let Ok(meta) = serde_json::from_str::<serde_json::Value>(&content) {
                    if let Some(created) = meta.get("created_at").and_then(|c| c.as_str()) {
                        if let Ok(created_dt) = chrono::DateTime::parse_from_rfc3339(created) {
                            let now = chrono::Local::now();
                            let age = now.signed_duration_since(created_dt.with_timezone(&chrono::Local));
                            if age.num_days() > self.ttl_days {
                                std::fs::remove_file(&cache_path).ok();
                                std::fs::remove_file(&metadata_path).ok();
                                return Ok(false);
                            }
                        }
                    }
                }
            }
        }

        let file = fs::File::open(&cache_path)
            .with_context(|| format!("Failed to open cache file: {:?}", cache_path))?;
        let gz = flate2::read::GzDecoder::new(file);
        let mut archive = tar::Archive::new(gz);
        archive
            .unpack(working_dir)
            .with_context(|| format!("Failed to unpack cache: {:?}", cache_path))?;
        Ok(true)
    }

    pub fn cleanup_expired(&self) -> Result<usize> {
        let mut removed = 0;
        let cache_dir = self.cache_dir();
        if !cache_dir.exists() {
            return Ok(0);
        }
        for entry in WalkDir::new(&cache_dir).into_iter().filter_map(|e| e.ok()) {
            if entry.file_type().is_file() {
                let name = entry.file_name().to_string_lossy();
                if name.ends_with(".json") {
                    if let Ok(content) = std::fs::read_to_string(entry.path()) {
                        if let Ok(meta) = serde_json::from_str::<serde_json::Value>(&content) {
                            if let Some(created) = meta.get("created_at").and_then(|c| c.as_str()) {
                                if let Ok(created_dt) = chrono::DateTime::parse_from_rfc3339(created) {
                                    let now = chrono::Local::now();
                                    let age = now.signed_duration_since(created_dt.with_timezone(&chrono::Local));
                                    if age.num_days() > self.ttl_days {
                                        if let Some(key) = meta.get("key").and_then(|k| k.as_str()) {
                                            let targz = self.cache_entry_path(key);
                                            std::fs::remove_file(&targz).ok();
                                        }
                                        std::fs::remove_file(entry.path()).ok();
                                        removed += 1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(removed)
    }
}
