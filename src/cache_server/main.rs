use anyhow::{anyhow, Result};
use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Json, Response},
    routing::{delete, get, post, put},
    Router,
};
use chrono::{DateTime, Duration, Utc};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::path::{Path as StdPath, PathBuf};
use std::sync::Arc;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;
use walkdir::WalkDir;

#[derive(Parser, Debug)]
#[command(name = "ci-cache-server", version, about = "Remote CI Cache Server")]
struct Cli {
    #[arg(short, long, default_value = "config.toml")]
    config: String,
}

#[derive(Debug, Deserialize, Clone)]
struct ServerConfig {
    #[serde(default = "default_listen_addr")]
    listen_addr: String,
    #[serde(default = "default_storage_dir")]
    storage_dir: String,
    #[serde(default = "default_max_size_mb")]
    max_size_mb: u64,
    #[serde(default = "default_ttl_days")]
    ttl_days: i64,
    #[serde(default)]
    auth_token: Option<String>,
    #[serde(default = "default_per_namespace_max_mb")]
    per_namespace_max_mb: u64,
    #[serde(default)]
    namespace_tokens: HashMap<String, String>,
}

fn default_listen_addr() -> String {
    "0.0.0.0:9876".to_string()
}

fn default_storage_dir() -> String {
    "./storage".to_string()
}

fn default_max_size_mb() -> u64 {
    500
}

fn default_ttl_days() -> i64 {
    7
}

fn default_per_namespace_max_mb() -> u64 {
    200
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: default_listen_addr(),
            storage_dir: default_storage_dir(),
            max_size_mb: default_max_size_mb(),
            ttl_days: default_ttl_days(),
            auth_token: None,
            per_namespace_max_mb: default_per_namespace_max_mb(),
            namespace_tokens: HashMap::new(),
        }
    }
}

impl ServerConfig {
    fn has_any_token(&self) -> bool {
        self.auth_token.is_some() || !self.namespace_tokens.is_empty()
    }
}

#[derive(Clone)]
struct AppState {
    config: ServerConfig,
    storage_dir: PathBuf,
    stats: Arc<Mutex<CacheStatsInner>>,
}

#[derive(Debug, Clone, Serialize)]
struct EvictionEvent {
    timestamp: DateTime<Utc>,
    keys: Vec<String>,
    freed_bytes: u64,
}

#[derive(Debug, Default, Clone)]
struct CacheStatsInner {
    total_entries: usize,
    total_size_bytes: u64,
    hits_last_24h: u64,
    misses_last_24h: u64,
    namespace_counts: HashMap<String, usize>,
    evictions: VecDeque<EvictionEvent>,
}

#[derive(Debug, Default, Clone, Serialize)]
struct CacheStats {
    total_entries: usize,
    total_size_bytes: u64,
    hits_last_24h: u64,
    misses_last_24h: u64,
    namespace_counts: HashMap<String, usize>,
    evictions_last_24h: u64,
}

impl From<CacheStatsInner> for CacheStats {
    fn from(inner: CacheStatsInner) -> Self {
        let cutoff = Utc::now() - Duration::hours(24);
        let evictions_last_24h = inner
            .evictions
            .iter()
            .filter(|e| e.timestamp >= cutoff)
            .count() as u64;
        Self {
            total_entries: inner.total_entries,
            total_size_bytes: inner.total_size_bytes,
            hits_last_24h: inner.hits_last_24h,
            misses_last_24h: inner.misses_last_24h,
            namespace_counts: inner.namespace_counts,
            evictions_last_24h,
        }
    }
}

#[derive(Debug, Serialize)]
struct CacheEntryInfo {
    key: String,
    size_bytes: u64,
    last_accessed: String,
    created_by: String,
    access_count: u64,
    last_accessed_by: String,
}

#[derive(Debug, Serialize)]
struct GcResult {
    removed: usize,
    freed_bytes: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = load_config(&cli.config)?;

    let storage_dir = std::path::Path::new(&config.storage_dir).canonicalize().unwrap_or_else(|_| {
        std::fs::create_dir_all(&config.storage_dir).ok();
        std::path::Path::new(&config.storage_dir).to_path_buf()
    });

    println!("Starting ci-cache-server");
    println!("  Listen address: {}", config.listen_addr);
    println!("  Storage directory: {}", storage_dir.display());
    println!("  Max cache size: {} MB", config.max_size_mb);
    println!("  Per-namespace max cache size: {} MB", config.per_namespace_max_mb);
    println!("  TTL: {} days", config.ttl_days);
    println!("  Global auth token: {}", if config.auth_token.is_some() { "enabled" } else { "disabled" });
    println!("  Namespace tokens: {} namespace(s)", config.namespace_tokens.len());

    let state = AppState {
        config: config.clone(),
        storage_dir: storage_dir.clone(),
        stats: Arc::new(Mutex::new(CacheStatsInner::default())),
    };

    refresh_stats(&state).await?;

    let app = Router::new()
        .route("/cache/:namespace/:key", put(put_cache))
        .route("/cache/:namespace/:key", get(get_cache))
        .route("/cache/:namespace/:key", delete(delete_cache))
        .route("/cache/:namespace", get(list_namespace))
        .route("/cache/gc", post(run_gc))
        .route("/stats", get(get_stats))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&config.listen_addr).await?;
    println!("\nServer listening on http://{}", config.listen_addr);

    axum::serve(listener, app).await?;

    Ok(())
}

fn load_config(path: &str) -> Result<ServerConfig> {
    let config_path = StdPath::new(path);
    if config_path.exists() {
        let content = std::fs::read_to_string(config_path)?;
        let config: ServerConfig = toml::from_str(&content)?;
        Ok(config)
    } else {
        println!("Config file not found at {}, using defaults", path);
        Ok(ServerConfig::default())
    }
}

fn check_auth(headers: &HeaderMap, config: &ServerConfig, namespace: Option<&str>) -> Result<(), Response> {
    if !config.has_any_token() {
        return Ok(());
    }

    let provided_token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(|s| s.to_string());

    let provided_token = match provided_token {
        Some(t) => t,
        None => return Err(forbidden_response()),
    };

    if let Some(global) = &config.auth_token {
        if &provided_token == global {
            return Ok(());
        }
    }

    if let Some(ns) = namespace {
        if let Some(ns_token) = config.namespace_tokens.get(ns) {
            if &provided_token == ns_token {
                return Ok(());
            }
        }
    }

    Err(forbidden_response())
}

fn forbidden_response() -> Response {
    (
        StatusCode::FORBIDDEN,
        "Forbidden: Invalid or missing authorization token",
    ).into_response()
}

fn get_client_id(headers: &HeaderMap) -> String {
    headers
        .get("X-Client-ID")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "anonymous".to_string())
}

async fn put_cache(
    State(state): State<AppState>,
    Path((namespace, key)): Path<(String, String)>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    if let Err(resp) = check_auth(&headers, &state.config, Some(&namespace)) {
        return resp;
    }

    let client_id = get_client_id(&headers);

    let namespace_dir = state.storage_dir.join(&namespace);
    if let Err(e) = fs::create_dir_all(&namespace_dir).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to create namespace dir: {}", e)).into_response();
    }

    let cache_path = state.storage_dir.join(&namespace).join(format!("{}.tar.gz", key));
    let meta_path = state.storage_dir.join(&namespace).join(format!("{}.meta.json", key));
    let tmp_path = state.storage_dir.join(&namespace).join(format!(".{}.tmp", key));

    let max_size_bytes = state.config.max_size_mb * 1024 * 1024;

    let content_range = headers.get("Content-Range").and_then(|v| v.to_str().ok());

    if let Some(range) = content_range {
        return handle_chunked_upload(
            &state,
            &namespace,
            &key,
            &cache_path,
            &meta_path,
            &tmp_path,
            range,
            body,
            max_size_bytes,
            &client_id,
        ).await;
    }

    let result: Result<(), anyhow::Error> = async {
        let mut file = File::create(&tmp_path).await
            .map_err(|e| anyhow!("Failed to create temp file: {}", e))?;

        let body_data = axum::body::to_bytes(body, max_size_bytes as usize + 1).await
            .map_err(|e| anyhow!("Failed to read body: {}", e))?;

        if body_data.len() as u64 > max_size_bytes {
            return Err(anyhow!("Cache file exceeds max size of {} MB", state.config.max_size_mb));
        }

        file.write_all(&body_data).await
            .map_err(|e| anyhow!("Failed to write temp file: {}", e))?;
        file.flush().await
            .map_err(|e| anyhow!("Failed to flush temp file: {}", e))?;
        drop(file);

        fs::rename(&tmp_path, &cache_path).await
            .map_err(|e| anyhow!("Failed to rename temp file: {}", e))?;

        let metadata = serde_json::json!({
            "key": key,
            "namespace": namespace,
            "created_at": Utc::now().to_rfc3339(),
            "last_accessed": Utc::now().to_rfc3339(),
            "size_bytes": body_data.len(),
            "created_by": client_id,
            "access_count": 0,
            "last_accessed_by": client_id,
        });
        fs::write(&meta_path, metadata.to_string()).await
            .map_err(|e| anyhow!("Failed to write metadata: {}", e))?;

        Ok(())
    }.await;

    match result {
        Ok(()) => {
            let _ = refresh_stats(&state).await;
            let _ = run_eviction_if_needed(&state, &namespace).await;
            (StatusCode::OK, Json(serde_json::json!({ "status": "ok", "key": key, "namespace": namespace }))).into_response()
        }
        Err(e) => {
            let _ = fs::remove_file(&tmp_path).await;
            (StatusCode::INTERNAL_SERVER_ERROR, format!("Upload failed: {}", e)).into_response()
        }
    }
}

async fn handle_chunked_upload(
    state: &AppState,
    namespace: &str,
    key: &str,
    cache_path: &StdPath,
    meta_path: &StdPath,
    tmp_path: &StdPath,
    range: &str,
    body: Body,
    max_size_bytes: u64,
    client_id: &str,
) -> Response {
    let (start, end, total) = match parse_content_range(range) {
        Some(v) => v,
        None => return (StatusCode::BAD_REQUEST, "Invalid Content-Range header").into_response(),
    };

    let result: Result<(u64, bool), anyhow::Error> = async {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(tmp_path).await
            .map_err(|e| anyhow!("Failed to open temp file: {}", e))?;

        file.seek(std::io::SeekFrom::Start(start)).await
            .map_err(|e| anyhow!("Failed to seek: {}", e))?;

        let body_data = axum::body::to_bytes(body, 100 * 1024 * 1024).await
            .map_err(|e| anyhow!("Failed to read body: {}", e))?;

        if body_data.len() as u64 != (end - start + 1) {
            return Err(anyhow!("Chunk size mismatch"));
        }

        file.write_all(&body_data).await
            .map_err(|e| anyhow!("Failed to write chunk: {}", e))?;
        file.flush().await
            .map_err(|e| anyhow!("Failed to flush: {}", e))?;
        drop(file);

        let is_complete = if let Some(total_size) = total {
            if end + 1 >= total_size {
                let file_size = std::fs::metadata(tmp_path)
                    .map(|m| m.len())
                    .unwrap_or(0);
                if file_size >= total_size {
                    if file_size > max_size_bytes {
                        return Err(anyhow!("Cache file exceeds max size of {} MB", state.config.max_size_mb));
                    }

                    fs::rename(tmp_path, cache_path).await
                        .map_err(|e| anyhow!("Failed to rename temp file: {}", e))?;

                    let metadata = serde_json::json!({
                        "key": key,
                        "namespace": namespace,
                        "created_at": Utc::now().to_rfc3339(),
                        "last_accessed": Utc::now().to_rfc3339(),
                        "size_bytes": file_size,
                        "created_by": client_id,
                        "access_count": 0,
                        "last_accessed_by": client_id,
                    });
                    fs::write(meta_path, metadata.to_string()).await
                        .map_err(|e| anyhow!("Failed to write metadata: {}", e))?;

                    true
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        Ok((body_data.len() as u64, is_complete))
    }.await;

    match result {
        Ok((chunk_size, is_complete)) => {
            if is_complete {
                let _ = refresh_stats(state).await;
                let _ = run_eviction_if_needed(state, namespace).await;
            }
            (StatusCode::OK, Json(serde_json::json!({
                "status": if is_complete { "completed" } else { "chunk_received" },
                "bytes_received": chunk_size,
                "start": start,
                "end": end,
                "total": total,
            }))).into_response()
        }
        Err(e) => {
            let _ = fs::remove_file(tmp_path).await;
            (StatusCode::INTERNAL_SERVER_ERROR, format!("Chunk upload failed: {}", e)).into_response()
        }
    }
}

fn parse_content_range(range: &str) -> Option<(u64, u64, Option<u64>)> {
    let rest = range.strip_prefix("bytes ")?;
    let parts: Vec<&str> = rest.split('/').collect();
    if parts.len() != 2 {
        return None;
    }
    let range_parts: Vec<&str> = parts[0].split('-').collect();
    if range_parts.len() != 2 {
        return None;
    }
    let start: u64 = range_parts[0].parse().ok()?;
    let end: u64 = range_parts[1].parse().ok()?;
    let total = if parts[1] == "*" {
        None
    } else {
        Some(parts[1].parse().ok()?)
    };
    Some((start, end, total))
}

async fn get_cache(
    State(state): State<AppState>,
    Path((namespace, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if let Err(resp) = check_auth(&headers, &state.config, Some(&namespace)) {
        return resp;
    }

    let client_id = get_client_id(&headers);

    let cache_path = state.storage_dir.join(&namespace).join(format!("{}.tar.gz", key));
    let meta_path = state.storage_dir.join(&namespace).join(format!("{}.meta.json", key));

    if !cache_path.exists() {
        let mut stats = state.stats.lock().await;
        stats.misses_last_24h += 1;
        return StatusCode::NOT_FOUND.into_response();
    }

    let mut stats = state.stats.lock().await;
    stats.hits_last_24h += 1;
    drop(stats);

    if meta_path.exists() {
        if let Ok(meta_str) = fs::read_to_string(&meta_path).await {
            if let Ok(mut meta) = serde_json::from_str::<serde_json::Value>(&meta_str) {
                meta["last_accessed"] = serde_json::Value::String(Utc::now().to_rfc3339());
                let current_count = meta.get("access_count")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                meta["access_count"] = serde_json::Value::Number(serde_json::Number::from(current_count + 1));
                meta["last_accessed_by"] = serde_json::Value::String(client_id.clone());
                let _ = fs::write(&meta_path, meta.to_string()).await;
            }
        }
    }

    let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok());

    if let Some(range) = range_header {
        return handle_range_request(&cache_path, range).await;
    }

    match fs::read(&cache_path).await {
        Ok(data) => {
            let size = data.len();
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/gzip")
                .header(header::CONTENT_LENGTH, size.to_string())
                .header("Accept-Ranges", "bytes")
                .body(Body::from(data))
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("Read failed: {}", e)).into_response()
        }
    }
}

async fn handle_range_request(path: &StdPath, range: &str) -> Response {
    let (start, end) = match parse_range_header(range) {
        Some(v) => v,
        None => return (StatusCode::BAD_REQUEST, "Invalid Range header").into_response(),
    };

    match fs::metadata(path).await {
        Ok(meta) => {
            let file_size = meta.len();
            let start = start.unwrap_or(0);
            let end = end.unwrap_or(file_size - 1);

            if start >= file_size || end < start || end >= file_size {
                return (
                    StatusCode::RANGE_NOT_SATISFIABLE,
                    format!("Requested range not satisfiable (file size: {})", file_size)
                ).into_response();
            }

            let length = end - start + 1;

            match File::open(path).await {
                Ok(mut file) => {
                    if let Err(e) = file.seek(std::io::SeekFrom::Start(start)).await {
                        return (StatusCode::INTERNAL_SERVER_ERROR, format!("Seek failed: {}", e)).into_response();
                    }

                    let mut reader = BufReader::with_capacity(8192, file.take(length));
                    let mut buffer = Vec::with_capacity(length as usize);

                    match reader.read_to_end(&mut buffer).await {
                        Ok(_) => {
                            Response::builder()
                                .status(StatusCode::PARTIAL_CONTENT)
                                .header(header::CONTENT_TYPE, "application/gzip")
                                .header(header::CONTENT_LENGTH, length.to_string())
                                .header(
                                    header::CONTENT_RANGE,
                                    format!("bytes {}-{}/{}", start, end, file_size),
                                )
                                .header("Accept-Ranges", "bytes")
                                .body(Body::from(buffer))
                                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
                        }
                        Err(e) => {
                            (StatusCode::INTERNAL_SERVER_ERROR, format!("Read failed: {}", e)).into_response()
                        }
                    }
                }
                Err(e) => {
                    (StatusCode::INTERNAL_SERVER_ERROR, format!("Open failed: {}", e)).into_response()
                }
            }
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("Metadata failed: {}", e)).into_response()
        }
    }
}

fn parse_range_header(range: &str) -> Option<(Option<u64>, Option<u64>)> {
    let rest = range.strip_prefix("bytes=")?;
    let parts: Vec<&str> = rest.split('-').collect();
    if parts.len() != 2 {
        return None;
    }

    let start = if parts[0].is_empty() {
        None
    } else {
        Some(parts[0].parse().ok()?)
    };

    let end = if parts[1].is_empty() {
        None
    } else {
        Some(parts[1].parse().ok()?)
    };

    Some((start, end))
}

async fn delete_cache(
    State(state): State<AppState>,
    Path((namespace, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if let Err(resp) = check_auth(&headers, &state.config, Some(&namespace)) {
        return resp;
    }

    let cache_path = state.storage_dir.join(&namespace).join(format!("{}.tar.gz", key));
    let meta_path = state.storage_dir.join(&namespace).join(format!("{}.meta.json", key));

    let mut removed = 0;

    if cache_path.exists() {
        if fs::remove_file(&cache_path).await.is_ok() {
            removed += 1;
        }
    }
    if meta_path.exists() {
        if fs::remove_file(&meta_path).await.is_ok() {
            removed += 1;
        }
    }

    let _ = refresh_stats(&state).await;

    (StatusCode::OK, Json(serde_json::json!({ "removed": removed }))).into_response()
}

async fn list_namespace(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(resp) = check_auth(&headers, &state.config, Some(&namespace)) {
        return resp;
    }

    let namespace_dir = state.storage_dir.join(&namespace);
    if !namespace_dir.exists() {
        return (StatusCode::OK, Json(serde_json::json!([] as [serde_json::Value; 0]))).into_response();
    }

    let mut entries: Vec<CacheEntryInfo> = Vec::new();

    if let Ok(read_dir) = fs::read_dir(&namespace_dir).await {
        let mut read_dir = read_dir;
        while let Ok(Some(entry)) = read_dir.next_entry().await {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("gz") {
                if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
                    let key = file_name
                        .strip_suffix(".tar.gz")
                        .or_else(|| file_name.strip_suffix(".gz"))
                        .unwrap_or(file_name)
                        .to_string();
                    let meta_path = namespace_dir.join(format!("{}.meta.json", key));

                    let size = if let Ok(meta) = fs::metadata(&path).await {
                        meta.len()
                    } else {
                        0
                    };

                    let (last_accessed, created_by, access_count, last_accessed_by) = if meta_path.exists() {
                        if let Ok(meta_str) = fs::read_to_string(&meta_path).await {
                            if let Ok(meta) = serde_json::from_str::<serde_json::Value>(&meta_str) {
                                let last = meta.get("last_accessed")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("unknown")
                                    .to_string();
                                let cb = meta.get("created_by")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("unknown")
                                    .to_string();
                                let ac = meta.get("access_count")
                                    .and_then(|v| v.as_u64())
                                    .unwrap_or(0);
                                let lab = meta.get("last_accessed_by")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("unknown")
                                    .to_string();
                                (last, cb, ac, lab)
                            } else {
                                ("unknown".to_string(), "unknown".to_string(), 0, "unknown".to_string())
                            }
                        } else {
                            ("unknown".to_string(), "unknown".to_string(), 0, "unknown".to_string())
                        }
                    } else {
                        ("unknown".to_string(), "unknown".to_string(), 0, "unknown".to_string())
                    };

                    entries.push(CacheEntryInfo {
                        key,
                        size_bytes: size,
                        last_accessed,
                        created_by,
                        access_count,
                        last_accessed_by,
                    });
                }
            }
        }
    }

    entries.sort_by(|a, b| a.key.cmp(&b.key));

    (StatusCode::OK, Json(entries)).into_response()
}

async fn run_gc(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    if let Err(resp) = check_auth(&headers, &state.config, None) {
        return resp;
    }

    let ttl = Duration::days(state.config.ttl_days);
    let now = Utc::now();
    let mut removed = 0;
    let mut freed_bytes: u64 = 0;

    let storage_dir = state.storage_dir.clone();

    for entry in WalkDir::new(&storage_dir).into_iter().filter_map(|e| e.ok()) {
        if entry.file_type().is_file() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("json") &&
               path.file_name().and_then(|n| n.to_str()).map_or(false, |n| n.ends_with(".meta.json"))
            {
                if let Ok(content) = std::fs::read_to_string(path) {
                    if let Ok(meta) = serde_json::from_str::<serde_json::Value>(&content) {
                        if let Some(created_str) = meta.get("created_at").and_then(|c| c.as_str()) {
                            if let Ok(created) = DateTime::parse_from_rfc3339(created_str) {
                                let age = now - created.with_timezone(&Utc);
                                if age > ttl {
                                    if let Some(key) = meta.get("key").and_then(|k| k.as_str()) {
                                        if let Some(ns) = meta.get("namespace").and_then(|n| n.as_str()) {
                                            let cache_file = storage_dir.join(ns).join(format!("{}.tar.gz", key));
                                            if let Ok(meta_fs) = std::fs::metadata(&cache_file) {
                                                freed_bytes += meta_fs.len();
                                            }
                                            std::fs::remove_file(&cache_file).ok();
                                            removed += 1;
                                        }
                                    }
                                    std::fs::remove_file(path).ok();
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let _ = refresh_stats(&state).await;

    (StatusCode::OK, Json(GcResult { removed, freed_bytes })).into_response()
}

async fn get_stats(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    if let Err(resp) = check_auth(&headers, &state.config, None) {
        return resp;
    }

    let inner = state.stats.lock().await.clone();
    let stats: CacheStats = inner.into();
    (StatusCode::OK, Json(stats)).into_response()
}

async fn refresh_stats(state: &AppState) -> Result<()> {
    let mut stats = state.stats.lock().await;
    stats.total_entries = 0;
    stats.total_size_bytes = 0;
    stats.namespace_counts.clear();

    let cutoff = Utc::now() - Duration::hours(24);
    stats.evictions.retain(|e| e.timestamp >= cutoff);

    if state.storage_dir.exists() {
        for entry in WalkDir::new(&state.storage_dir).into_iter().filter_map(|e| e.ok()) {
            if entry.file_type().is_file() {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) == Some("gz") {
                    stats.total_entries += 1;
                    if let Ok(meta) = std::fs::metadata(path) {
                        stats.total_size_bytes += meta.len();
                    }
                    if let Some(parent) = path.parent() {
                        if let Some(ns) = parent.file_name().and_then(|n| n.to_str()) {
                            *stats.namespace_counts.entry(ns.to_string()).or_insert(0) += 1;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct NamespaceEntryMeta {
    key: String,
    last_accessed: DateTime<Utc>,
    size_bytes: u64,
    cache_path: PathBuf,
    meta_path: PathBuf,
}

async fn run_eviction_if_needed(state: &AppState, namespace: &str) -> Result<()> {
    let max_bytes = state.config.per_namespace_max_mb * 1024 * 1024;
    let target_bytes = (max_bytes as f64 * 0.8) as u64;

    let namespace_dir = state.storage_dir.join(namespace);
    if !namespace_dir.exists() {
        return Ok(());
    }

    let mut entries: Vec<NamespaceEntryMeta> = Vec::new();
    let mut total_size: u64 = 0;

    if let Ok(read_dir) = fs::read_dir(&namespace_dir).await {
        let mut read_dir = read_dir;
        while let Ok(Some(entry)) = read_dir.next_entry().await {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("gz") {
                if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
                    let key = file_name
                        .strip_suffix(".tar.gz")
                        .or_else(|| file_name.strip_suffix(".gz"))
                        .unwrap_or(file_name)
                        .to_string();
                    let meta_path = namespace_dir.join(format!("{}.meta.json", key));

                    let size = if let Ok(meta) = fs::metadata(&path).await {
                        meta.len()
                    } else {
                        0
                    };
                    total_size += size;

                    let last_accessed = if meta_path.exists() {
                        if let Ok(meta_str) = fs::read_to_string(&meta_path).await {
                            if let Ok(meta) = serde_json::from_str::<serde_json::Value>(&meta_str) {
                                meta.get("last_accessed")
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                                    .map(|d| d.with_timezone(&Utc))
                                    .unwrap_or_else(|| Utc::now())
                            } else {
                                Utc::now()
                            }
                        } else {
                            Utc::now()
                        }
                    } else {
                        Utc::now()
                    };

                    entries.push(NamespaceEntryMeta {
                        key,
                        last_accessed,
                        size_bytes: size,
                        cache_path: path,
                        meta_path,
                    });
                }
            }
        }
    }

    if total_size <= max_bytes {
        return Ok(());
    }

    entries.sort_by(|a, b| a.last_accessed.cmp(&b.last_accessed));

    let mut evicted_keys: Vec<String> = Vec::new();
    let mut freed: u64 = 0;

    for entry in &entries {
        if total_size - freed <= target_bytes {
            break;
        }

        if fs::remove_file(&entry.cache_path).await.is_ok() {
            freed += entry.size_bytes;
            evicted_keys.push(entry.key.clone());
        }
        let _ = fs::remove_file(&entry.meta_path).await;
    }

    if !evicted_keys.is_empty() {
        println!(
            "[EVICTION] namespace={} evicted_keys={:?} freed_bytes={} ({} MB)",
            namespace,
            evicted_keys,
            freed,
            freed / (1024 * 1024)
        );

        let mut stats = state.stats.lock().await;
        stats.evictions.push_back(EvictionEvent {
            timestamp: Utc::now(),
            keys: evicted_keys,
            freed_bytes: freed,
        });
        let cutoff = Utc::now() - Duration::hours(24);
        stats.evictions.retain(|e| e.timestamp >= cutoff);
        drop(stats);

        let _ = refresh_stats(state).await;
    }

    Ok(())
}
