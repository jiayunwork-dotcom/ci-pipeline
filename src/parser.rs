use crate::models::*;
use anyhow::{anyhow, Context, Result};
use std::fs;
use std::path::Path;

pub struct ParseContext {
    pub file_path: String,
}

impl ParseContext {
    pub fn new(file_path: &str) -> Self {
        Self {
            file_path: file_path.to_string(),
        }
    }
}

pub fn parse_pipeline(file_path: &str) -> Result<Pipeline> {
    let path = Path::new(file_path);
    if !path.exists() {
        return Err(anyhow!("Pipeline file not found: {}", file_path));
    }

    let content = fs::read_to_string(path)
        .with_context(|| format!("Failed to read pipeline file: {}", file_path))?;

    parse_pipeline_from_str(&content)
}

pub fn parse_pipeline_from_str(content: &str) -> Result<Pipeline> {
    let pipeline: Pipeline = serde_yaml::from_str(content)
        .map_err(|e| {
            let loc = e.location();
            let line = loc.as_ref().map(|l| l.line()).unwrap_or(0);
            let col = loc.as_ref().map(|l| l.column()).unwrap_or(0);
            anyhow!(
                "YAML parse error at line {}, column {}: {}",
                line,
                col,
                e.to_string().split('\n').next().unwrap_or("Unknown error")
            )
        })?;

    if pipeline.jobs.is_empty() {
        return Err(anyhow!("Pipeline must define at least one job"));
    }

    Ok(pipeline)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_pipeline() {
        let yaml = r#"
stages:
  - build
  - test

variables:
  VERSION: "1.0.0"
  GOOS: linux

jobs:
  - name: build
    stage: build
    steps:
      - name: compile
        run: go build -o app
  - name: test
    stage: test
    depends_on: [build]
    steps:
      - run: go test ./...
"#;
        let pipeline = parse_pipeline_from_str(yaml).unwrap();
        assert_eq!(pipeline.stages.len(), 2);
        assert_eq!(pipeline.jobs.len(), 2);
        assert_eq!(pipeline.jobs[0].name, "build");
        assert_eq!(pipeline.jobs[1].depends_on, vec!["build"]);
    }
}
