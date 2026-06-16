use crate::models::*;
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;

const CHUNK_SIZE: u64 = 10 * 1024 * 1024; // 10MB
const LARGE_FILE_THRESHOLD: u64 = 100 * 1024 * 1024; // 100MB

#[derive(Debug, Clone)]
pub struct RemoteCacheClient {
    pub config: RemoteCacheConfig,
    pub namespace: String,
    pub stats: Arc<Mutex<RemoteCacheStats>>,
    client: reqwest::Client,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CacheEntryInfo {
    pub key: String,
    pub size_bytes: u64,
    pub last_accessed: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ServerStats {
    pub total_entries: usize,
    pub total_size_bytes: u64,
    pub hits_last_24h: u64,
    pub misses_last_24h: u64,
    pub namespace_counts: std::collections::HashMap<String, usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GcResult {
    pub removed: usize,
    pub freed_bytes: u64,
}

impl RemoteCacheClient {
    pub fn new(config: RemoteCacheConfig, default_namespace: &str) -> Self {
        let namespace = config
            .namespace
            .clone()
            .unwrap_or_else(|| default_namespace.to_string());

        Self {
            config,
            namespace,
            stats: Arc::new(Mutex::new(RemoteCacheStats::default())),
            client: reqwest::Client::new(),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enabled && self.config.url.is_some()
    }

    fn base_url(&self) -> Result<String> {
        self.config
            .url
            .clone()
            .ok_or_else(|| anyhow!("Remote cache URL not configured"))
    }

    fn auth_header(&self) -> Option<(String, String)> {
        self.config.token.as_ref().map(|token| {
            (
                "Authorization".to_string(),
                format!("Bearer {}", token),
            )
        })
    }

    pub async fn upload_cache(&self, key: &str, file_path: &Path) -> Result<bool> {
        if !self.is_enabled() {
            return Ok(false);
        }

        let base = self.base_url()?;
        let url = format!("{}/cache/{}/{}", base.trim_end_matches('/'), self.namespace, key);

        let file_size = fs::metadata(file_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0);

        let result = if file_size > LARGE_FILE_THRESHOLD {
            self.upload_chunked(&url, file_path, file_size).await
        } else {
            self.upload_single(&url, file_path).await
        };

        if result.is_ok() {
            let mut stats = self.stats.lock().await;
            stats.pushes += 1;
        }

        result
    }

    async fn upload_single(&self, url: &str, file_path: &Path) -> Result<bool> {
        let data = fs::read(file_path)
            .await
            .with_context(|| format!("Failed to read file: {:?}", file_path))?;

        let mut request = self.client.put(url).body(data);

        if let Some((key, value)) = self.auth_header() {
            request = request.header(key, value);
        }

        let response = request
            .send()
            .await
            .with_context(|| "Failed to send upload request")?;

        if response.status().is_success() {
            Ok(true)
        } else {
            Err(anyhow!(
                "Upload failed with status: {}",
                response.status()
            ))
        }
    }

    async fn upload_chunked(&self, url: &str, file_path: &Path, file_size: u64) -> Result<bool> {
        let mut file = fs::File::open(file_path)
            .await
            .with_context(|| format!("Failed to open file: {:?}", file_path))?;

        let mut offset: u64 = 0;
        let mut buffer = vec![0u8; CHUNK_SIZE as usize];

        while offset < file_size {
            let bytes_read = file
                .read(&mut buffer)
                .await
                .with_context(|| "Failed to read chunk")?;

            if bytes_read == 0 {
                break;
            }

            let chunk = &buffer[..bytes_read];
            let end = offset + bytes_read as u64 - 1;
            let content_range = format!("bytes {}-{}/{}", offset, end, file_size);

            let mut request = self
                .client
                .put(url)
                .body(chunk.to_vec())
                .header("Content-Range", content_range);

            if let Some((key, value)) = self.auth_header() {
                request = request.header(key, value);
            }

            let response = request
                .send()
                .await
                .with_context(|| format!("Failed to upload chunk at offset {}", offset))?;

            if !response.status().is_success() {
                return Err(anyhow!(
                    "Chunk upload failed at offset {} with status: {}",
                    offset,
                    response.status()
                ));
            }

            offset += bytes_read as u64;
        }

        Ok(true)
    }

    pub async fn download_cache(&self, key: &str, output_path: &Path) -> Result<bool> {
        if !self.is_enabled() {
            return Ok(false);
        }

        let base = self.base_url()?;
        let url = format!("{}/cache/{}/{}", base.trim_end_matches('/'), self.namespace, key);

        let result = self.try_download_with_resume(&url, output_path).await;

        {
            let mut stats = self.stats.lock().await;
            match result {
                Ok(true) => stats.hits += 1,
                Ok(false) => stats.misses += 1,
                Err(_) => stats.misses += 1,
            }
        }

        result
    }

    async fn try_download_with_resume(&self, url: &str, output_path: &Path) -> Result<bool> {
        let mut existing_size = if output_path.exists() {
            fs::metadata(output_path).await.map(|m| m.len()).unwrap_or(0)
        } else {
            0
        };

        if existing_size > 0 {
            let tmp_path = output_path.with_extension("part");
            if tmp_path.exists() {
                existing_size = fs::metadata(&tmp_path)
                    .await
                    .map(|m| m.len())
                    .unwrap_or(0);
            }
        }

        let tmp_path = output_path.with_extension("part");

        let mut request = self.client.get(url);

        if let Some((key, value)) = self.auth_header() {
            request = request.header(key, value);
        }

        if existing_size > 0 {
            request = request.header("Range", format!("bytes={}-", existing_size));
        }

        let response = request
            .send()
            .await
            .with_context(|| "Failed to send download request")?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            let _ = fs::remove_file(&tmp_path).await;
            return Ok(false);
        }

        if !response.status().is_success() && response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(anyhow!(
                "Download failed with status: {}",
                response.status()
            ));
        }

        let is_partial = response.status() == reqwest::StatusCode::PARTIAL_CONTENT;

        let content_length = response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        let total_size = if is_partial && existing_size > 0 {
            response
                .headers()
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| {
                    let parts: Vec<&str> = v.split('/').collect();
                    if parts.len() == 2 {
                        parts[1].parse::<u64>().ok()
                    } else {
                        None
                    }
                })
                .unwrap_or(content_length + existing_size)
        } else {
            content_length
        };

        let bytes = response
            .bytes()
            .await
            .with_context(|| "Failed to read response body")?;

        if is_partial && existing_size > 0 {
            let mut file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&tmp_path)
                .await
                .with_context(|| format!("Failed to open file for append: {:?}", tmp_path))?;

            use tokio::io::AsyncWriteExt;
            file.write_all(&bytes)
                .await
                .with_context(|| "Failed to append chunk")?;
            file.flush().await.ok();
            drop(file);

            let final_size = fs::metadata(&tmp_path).await.map(|m| m.len()).unwrap_or(0);
            if final_size >= total_size && total_size > 0 {
                fs::rename(&tmp_path, output_path)
                    .await
                    .with_context(|| "Failed to rename temp file")?;
            }
        } else {
            if let Some(parent) = output_path.parent() {
                fs::create_dir_all(parent).await.ok();
            }
            fs::write(&tmp_path, &bytes)
                .await
                .with_context(|| format!("Failed to write file: {:?}", tmp_path))?;

            if total_size > 0 && bytes.len() as u64 >= total_size {
                fs::rename(&tmp_path, output_path)
                    .await
                    .with_context(|| "Failed to rename temp file")?;
            } else if total_size == 0 {
                fs::rename(&tmp_path, output_path)
                    .await
                    .with_context(|| "Failed to rename temp file")?;
            }
        }

        let size_ok = if let Ok(meta) = fs::metadata(output_path).await {
            meta.len() > 0
        } else {
            false
        };

        Ok(size_ok)
    }

    pub async fn delete_cache(&self, key: &str) -> Result<bool> {
        if !self.is_enabled() {
            return Ok(false);
        }

        let base = self.base_url()?;
        let url = format!("{}/cache/{}/{}", base.trim_end_matches('/'), self.namespace, key);

        let mut request = self.client.delete(url);

        if let Some((key_h, value)) = self.auth_header() {
            request = request.header(key_h, value);
        }

        let response = request
            .send()
            .await
            .with_context(|| "Failed to send delete request")?;

        Ok(response.status().is_success())
    }

    pub async fn list_namespace(&self, namespace: Option<&str>) -> Result<Vec<CacheEntryInfo>> {
        if !self.is_enabled() {
            return Ok(Vec::new());
        }

        let ns = namespace.unwrap_or(&self.namespace);
        let base = self.base_url()?;
        let url = format!("{}/cache/{}", base.trim_end_matches('/'), ns);

        let mut request = self.client.get(&url);

        if let Some((key, value)) = self.auth_header() {
            request = request.header(key, value);
        }

        let response = request
            .send()
            .await
            .with_context(|| "Failed to send list request")?;

        if !response.status().is_success() {
            return Err(anyhow!("List failed with status: {}", response.status()));
        }

        let entries: Vec<CacheEntryInfo> = response
            .json()
            .await
            .with_context(|| "Failed to parse list response")?;

        Ok(entries)
    }

    pub async fn trigger_gc(&self) -> Result<GcResult> {
        if !self.is_enabled() {
            return Err(anyhow!("Remote cache not enabled"));
        }

        let base = self.base_url()?;
        let url = format!("{}/cache/gc", base.trim_end_matches('/'));

        let mut request = self.client.post(&url);

        if let Some((key, value)) = self.auth_header() {
            request = request.header(key, value);
        }

        let response = request
            .send()
            .await
            .with_context(|| "Failed to send GC request")?;

        if !response.status().is_success() {
            return Err(anyhow!("GC failed with status: {}", response.status()));
        }

        let result: GcResult = response
            .json()
            .await
            .with_context(|| "Failed to parse GC response")?;

        Ok(result)
    }

    pub async fn get_stats(&self) -> Result<ServerStats> {
        if !self.is_enabled() {
            return Err(anyhow!("Remote cache not enabled"));
        }

        let base = self.base_url()?;
        let url = format!("{}/stats", base.trim_end_matches('/'));

        let mut request = self.client.get(&url);

        if let Some((key, value)) = self.auth_header() {
            request = request.header(key, value);
        }

        let response = request
            .send()
            .await
            .with_context(|| "Failed to send stats request")?;

        if !response.status().is_success() {
            return Err(anyhow!("Stats failed with status: {}", response.status()));
        }

        let stats: ServerStats = response
            .json()
            .await
            .with_context(|| "Failed to parse stats response")?;

        Ok(stats)
    }

    pub async fn get_local_stats(&self) -> RemoteCacheStats {
        self.stats.lock().await.clone()
    }
}

pub fn detect_git_branch() -> Option<String> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .ok()?;

    if output.status.success() {
        let branch = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !branch.is_empty() {
            return Some(branch);
        }
    }

    None
}

pub fn get_default_namespace() -> String {
    detect_git_branch().unwrap_or_else(|| "default".to_string())
}
