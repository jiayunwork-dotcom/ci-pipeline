use crate::models::*;
use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::process::Command;

pub struct ServiceManager {
    containers: HashMap<String, String>,
    networks: HashMap<String, String>,
    job_containers: HashMap<String, String>,
    enabled: bool,
}

impl ServiceManager {
    pub fn new() -> Self {
        Self {
            containers: HashMap::new(),
            networks: HashMap::new(),
            job_containers: HashMap::new(),
            enabled: true,
        }
    }

    async fn check_docker(&mut self) {
        let result = Command::new("docker")
            .arg("version")
            .output()
            .await;
        match result {
            Ok(out) if out.status.success() => {}
            _ => {
                self.enabled = false;
            }
        }
    }

    pub fn is_enabled(&mut self) -> bool {
        if self.enabled {
            true
        } else {
            false
        }
    }

    async fn ensure_network(&mut self, job_name: &str) -> Result<String> {
        let safe_job = sanitize_name(job_name);
        let network_name = format!("ci-net-{}", safe_job);
        if let Some(existing) = self.networks.get(&safe_job) {
            return Ok(existing.clone());
        }

        let _ = Command::new("docker")
            .args(["network", "rm", &network_name])
            .output()
            .await;

        let out = Command::new("docker")
            .args(["network", "create", &network_name])
            .output()
            .await
            .with_context(|| format!("Failed to create network for job {}", job_name))?;

        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            return Err(anyhow!("Failed to create network {}: {}", network_name, stderr));
        }

        let net_id = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let id = if net_id.is_empty() { network_name.clone() } else { net_id };
        self.networks.insert(safe_job, id.clone());
        Ok(id)
    }

    pub async fn start_services(
        &mut self,
        job_name: &str,
        services: &[ServiceConfig],
    ) -> Result<HashMap<String, String>> {
        let mut hosts = HashMap::new();
        if services.is_empty() {
            return Ok(hosts);
        }

        self.check_docker().await;
        if !self.enabled {
            eprintln!(
                "[warn] Docker not available, skipping services for job '{}'",
                job_name
            );
            return Ok(hosts);
        }

        let _ = self.ensure_network(job_name).await;

        for service in services {
            let service_name = sanitize_service_name(job_name, &service.image);
            let container_id = self
                .start_container(job_name, &service_name, service)
                .await?;
            hosts.insert(service_name.clone(), service_name.clone());
            self.containers.insert(
                format!("{}-{}", job_name, service_name),
                container_id,
            );
        }

        Ok(hosts)
    }

    async fn start_container(
        &self,
        job_name: &str,
        service_name: &str,
        service: &ServiceConfig,
    ) -> Result<String> {
        let container_name = format!(
            "ci-{}-{}",
            sanitize_name(job_name),
            service_name
        );
        let network_name = format!("ci-net-{}", sanitize_name(job_name));

        let _ = Command::new("docker")
            .args(["rm", "-f", &container_name])
            .output()
            .await;

        let mut args: Vec<String> = vec![
            "run".into(), "-d".into(),
            "--name".into(), container_name.clone(),
            "--network".into(), network_name,
            "--network-alias".into(), service_name.into(),
        ];
        for (k, v) in &service.env {
            args.push("-e".into());
            args.push(format!("{}={}", k, v));
        }
        args.push(service.image.clone());

        let out = Command::new("docker")
            .args(&args)
            .output()
            .await
            .with_context(|| format!("Failed to start service container for {}", service.image))?;

        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            return Err(anyhow!(
                "Failed to start service container {}: {}",
                service.image,
                stderr
            ));
        }

        let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
        Ok(if id.is_empty() { container_name } else { id })
    }

    pub async fn start_job_container(
        &mut self,
        job_name: &str,
        image: &str,
        working_dir: &PathBuf,
        env: &HashMap<String, String>,
    ) -> Result<String> {
        self.check_docker().await;
        if !self.enabled {
            return Err(anyhow!("Docker not available for container isolation"));
        }

        let _ = self.ensure_network(job_name).await;

        let safe_job = sanitize_name(job_name);
        let container_name = format!("ci-job-{}", safe_job);
        let network_name = format!("ci-net-{}", safe_job);
        let workspace = dunce::canonicalize(working_dir)
            .unwrap_or_else(|_| working_dir.clone())
            .to_string_lossy()
            .to_string();

        let _ = Command::new("docker")
            .args(["rm", "-f", &container_name])
            .output()
            .await;

        let mut args: Vec<String> = vec![
            "run".into(), "-d".into(), "-t".into(),
            "--name".into(), container_name.clone(),
            "--network".into(), network_name,
            "-v".into(), format!("{}:/workspace", workspace),
            "-w".into(), "/workspace".into(),
            "--entrypoint".into(), "sleep".into(),
        ];
        for (k, v) in env {
            args.push("-e".into());
            args.push(format!("{}={}", k, v));
        }
        args.push(image.into());
        args.push("infinity".into());

        let out = Command::new("docker")
            .args(&args)
            .output()
            .await
            .with_context(|| format!("Failed to start job container for {}", job_name))?;

        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            return Err(anyhow!(
                "Failed to start job container {}: {}",
                job_name, stderr
            ));
        }

        let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let cid = if id.is_empty() { container_name.clone() } else { id };
        self.job_containers.insert(job_name.to_string(), cid.clone());
        Ok(cid)
    }

    pub async fn exec_in_job_container(
        &self,
        job_name: &str,
        cmd: &str,
        env: &HashMap<String, String>,
    ) -> Result<std::process::Output> {
        let container_id = self.job_containers.get(job_name)
            .ok_or_else(|| anyhow!("Job container not started for {}", job_name))?;

        let mut args: Vec<String> = vec!["exec".into()];
        for (k, v) in env {
            args.push("-e".into());
            args.push(format!("{}={}", k, v));
        }
        args.extend(vec![
            container_id.clone(),
            "sh".into(), "-c".into(), cmd.into(),
        ]);

        let out = Command::new("docker")
            .args(&args)
            .output()
            .await
            .with_context(|| format!("Failed to exec in job container {}", job_name))?;

        Ok(out.into())
    }

    pub async fn stop_job_container(&mut self, job_name: &str) {
        if let Some(id) = self.job_containers.remove(job_name) {
            let _ = Command::new("docker")
                .args(["stop", "-t", "5", &id])
                .output()
                .await;
            let _ = Command::new("docker")
                .args(["rm", "-f", &id])
                .output()
                .await;
        }
    }

    pub async fn stop_job_services(&mut self, job_name: &str) {
        let to_stop: Vec<String> = self
            .containers
            .keys()
            .filter(|k| k.starts_with(&format!("{}-", job_name)))
            .cloned()
            .collect();
        for key in to_stop {
            if let Some(id) = self.containers.remove(&key) {
                let _ = Command::new("docker")
                    .args(["stop", "-t", "10", &id])
                    .output()
                    .await;
                let _ = Command::new("docker").args(["rm", "-f", &id]).output().await;
            }
        }
        self.stop_job_container(job_name).await;
        if let Some(net_id) = self.networks.remove(&sanitize_name(job_name)) {
            let _ = Command::new("docker")
                .args(["network", "rm", &net_id])
                .output()
                .await;
        }
    }

    pub async fn stop_all(&mut self) {
        let containers: Vec<String> = self.containers.values().cloned().collect();
        for id in containers {
            let _ = Command::new("docker")
                .args(["stop", "-t", "10", &id])
                .output()
                .await;
            let _ = Command::new("docker").args(["rm", "-f", &id]).output().await;
        }
        self.containers.clear();

        let job_cids: Vec<String> = self.job_containers.values().cloned().collect();
        for id in job_cids {
            let _ = Command::new("docker")
                .args(["stop", "-t", "5", &id])
                .output()
                .await;
            let _ = Command::new("docker").args(["rm", "-f", &id]).output().await;
        }
        self.job_containers.clear();

        let nets: Vec<String> = self.networks.values().cloned().collect();
        for id in nets {
            let _ = Command::new("docker")
                .args(["network", "rm", &id])
                .output()
                .await;
        }
        self.networks.clear();
    }
}

fn sanitize_name(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect()
}

fn sanitize_service_name(job_name: &str, image: &str) -> String {
    let base = image
        .split('/')
        .last()
        .unwrap_or(image)
        .split(':')
        .next()
        .unwrap_or(image);
    sanitize_name(base)
}
