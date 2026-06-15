use crate::models::*;
use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use tokio::process::Command;

pub struct ServiceManager {
    containers: HashMap<String, String>,
    enabled: bool,
}

impl ServiceManager {
    pub fn new() -> Self {
        Self {
            containers: HashMap::new(),
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

        for service in services {
            let service_name = sanitize_service_name(job_name, &service.image);
            let container_id = self
                .start_container(job_name, &service_name, service)
                .await?;
            hosts.insert(service_name.clone(), "127.0.0.1".to_string());
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

        let _ = Command::new("docker")
            .args(["rm", "-f", &container_name])
            .output()
            .await;

        let mut args: Vec<String> = vec!["run".into(), "-d".into(), "--name".into(), container_name.clone()];
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
        let _ = Command::new("docker")
            .args(["inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", &id])
            .output()
            .await;

        Ok(if id.is_empty() { container_name } else { id })
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
