use crate::models::*;
use anyhow::{anyhow, Result};
use regex::Regex;
use std::collections::HashMap;
use std::env;

#[derive(Debug, Clone)]
pub struct VariableResolver {
    pub global_vars: HashMap<String, String>,
    job_outputs: HashMap<String, HashMap<String, String>>,
    system_env: HashMap<String, String>,
}

impl VariableResolver {
    pub fn new(pipeline: &Pipeline) -> Self {
        let mut system_env = HashMap::new();
        for (k, v) in env::vars() {
            system_env.insert(k, v);
        }
        Self {
            global_vars: pipeline.variables.clone(),
            job_outputs: HashMap::new(),
            system_env,
        }
    }

    pub fn get_job_outputs(&self) -> &HashMap<String, HashMap<String, String>> {
        &self.job_outputs
    }

    pub fn set_job_output(&mut self, job_name: &str, key: &str, value: &str) {
        self.job_outputs
            .entry(job_name.to_string())
            .or_default()
            .insert(key.to_string(), value.to_string());
    }

    pub fn set_job_outputs(&mut self, job_name: &str, outputs: HashMap<String, String>) {
        self.job_outputs.insert(job_name.to_string(), outputs);
    }

    pub fn resolve_value(
        &self,
        value: &str,
        job_env: &HashMap<String, String>,
        step_env: &HashMap<String, String>,
    ) -> Result<String> {
        self.resolve_with_max_depth(value, job_env, step_env, 0)
    }

    fn resolve_with_max_depth(
        &self,
        value: &str,
        job_env: &HashMap<String, String>,
        step_env: &HashMap<String, String>,
        depth: u32,
    ) -> Result<String> {
        if depth > 10 {
            return Err(anyhow!("Variable resolution exceeded max nesting depth"));
        }

        let re = Regex::new(r"\$\{\{\s*([^{}]+?)\s*\}\}")?;
        let mut result = value.to_string();

        for _ in 0..20 {
            let mut changed = false;
            let matches: Vec<(String, usize, usize)> = re
                .find_iter(&result)
                .map(|m| {
                    let full = m.as_str().to_string();
                    (full, m.start(), m.end())
                })
                .collect();

            if matches.is_empty() {
                break;
            }

            for (full, start, end) in matches.iter().rev() {
                let caps = re.captures(full).ok_or_else(|| anyhow!("Invalid capture"))?;
                let inner = caps.get(1).unwrap().as_str().trim();
                let resolved = self.resolve_single_var(inner, job_env, step_env)?;
                result.replace_range(*start..*end, &resolved);
                changed = true;
            }

            if !changed {
                break;
            }
        }

        if re.is_match(&result) {
            let new_result = self.resolve_with_max_depth(&result, job_env, step_env, depth + 1)?;
            return Ok(new_result);
        }

        Ok(result)
    }

    fn resolve_single_var(
        &self,
        name: &str,
        job_env: &HashMap<String, String>,
        step_env: &HashMap<String, String>,
    ) -> Result<String> {
        if name.starts_with("jobs.") {
            let parts: Vec<&str> = name.splitn(4, '.').collect();
            if parts.len() >= 4 && parts[2] == "outputs" {
                let job_name = parts[1];
                let output_key = parts[3];
                if let Some(outputs) = self.job_outputs.get(job_name) {
                    if let Some(val) = outputs.get(output_key) {
                        return Ok(val.clone());
                    }
                }
                return Err(anyhow!(
                    "Cannot resolve output: jobs.{}.outputs.{} - job output not found",
                    job_name,
                    output_key
                ));
            }
            return Err(anyhow!("Invalid job reference: {}", name));
        }

        if let Some(val) = step_env.get(name) {
            return Ok(val.clone());
        }
        if let Some(val) = job_env.get(name) {
            return Ok(val.clone());
        }
        if let Some(val) = self.global_vars.get(name) {
            return Ok(val.clone());
        }
        if let Some(val) = self.system_env.get(name) {
            return Ok(val.clone());
        }

        Err(anyhow!("Variable '{}' not found", name))
    }

    pub fn try_resolve_value(
        &self,
        value: &str,
        job_env: &HashMap<String, String>,
        step_env: &HashMap<String, String>,
    ) -> String {
        match self.resolve_value(value, job_env, step_env) {
            Ok(v) => v,
            Err(_) => value.to_string(),
        }
    }
}

pub struct ConditionEvaluator {
    job_statuses: HashMap<String, JobStatus>,
}

impl ConditionEvaluator {
    pub fn new(job_statuses: HashMap<String, JobStatus>) -> Self {
        Self { job_statuses }
    }

    pub fn evaluate(
        &self,
        expr: &str,
        resolver: &VariableResolver,
        job_env: &HashMap<String, String>,
        step_env: &HashMap<String, String>,
    ) -> Result<bool> {
        let resolved = resolver.try_resolve_value(expr, job_env, step_env);
        self.evaluate_inner(&resolved, &resolver, job_env, step_env)
    }

    fn evaluate_inner(
        &self,
        expr: &str,
        _resolver: &VariableResolver,
        _job_env: &HashMap<String, String>,
        _step_env: &HashMap<String, String>,
    ) -> Result<bool> {
        let expr = expr.trim();

        if expr == "success()" {
            return Ok(self.job_statuses
                .values()
                .all(|s| matches!(s, JobStatus::Success | JobStatus::Skipped)
                    || matches!(s, JobStatus::Pending)));
        }

        if expr == "failure()" {
            return Ok(self.job_statuses
                .values()
                .any(|s| matches!(s, JobStatus::Failed)));
        }

        if expr == "always()" {
            return Ok(true);
        }

        if expr == "true" {
            return Ok(true);
        }
        if expr == "false" {
            return Ok(false);
        }

        if expr.starts_with('(') && expr.ends_with(')') {
            let inner = &expr[1..expr.len() - 1];
            return self.evaluate_inner(inner, _resolver, _job_env, _step_env);
        }

        if expr.starts_with('!') {
            let inner = &expr[1..].trim();
            let val = self.evaluate_inner(inner, _resolver, _job_env, _step_env)?;
            return Ok(!val);
        }

        if let Some(idx) = find_operator(expr, "||") {
            let left = self.evaluate_inner(&expr[..idx], _resolver, _job_env, _step_env)?;
            if left {
                return Ok(true);
            }
            let right = self.evaluate_inner(&expr[idx + 2..], _resolver, _job_env, _step_env)?;
            return Ok(left || right);
        }

        if let Some(idx) = find_operator(expr, "&&") {
            let left = self.evaluate_inner(&expr[..idx], _resolver, _job_env, _step_env)?;
            if !left {
                return Ok(false);
            }
            let right = self.evaluate_inner(&expr[idx + 2..], _resolver, _job_env, _step_env)?;
            return Ok(left && right);
        }

        for op in ["==", "!=", ">=", "<=", ">", "<"] {
            if let Some(idx) = find_operator(expr, op) {
                let left = expr[..idx].trim();
                let right = expr[idx + op.len()..].trim();
                let left_val = unquote_string(left);
                let right_val = unquote_string(right);
                return match op {
                    "==" => Ok(left_val == right_val),
                    "!=" => Ok(left_val != right_val),
                    ">=" => Ok(left_val >= right_val),
                    "<=" => Ok(left_val <= right_val),
                    ">" => Ok(left_val > right_val),
                    "<" => Ok(left_val < right_val),
                    _ => Err(anyhow!("Unknown operator: {}", op)),
                };
            }
        }

        let v = unquote_string(expr);
        if v == "true" {
            return Ok(true);
        }
        if v == "false" {
            return Ok(false);
        }

        Ok(false)
    }
}

fn find_operator(expr: &str, op: &str) -> Option<usize> {
    let mut depth = 0;
    let mut in_str = false;
    let mut str_ch = ' ';
    let chars: Vec<char> = expr.chars().collect();
    let op_chars: Vec<char> = op.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if !in_str && (c == '"' || c == '\'') {
            in_str = true;
            str_ch = c;
        } else if in_str && c == str_ch {
            in_str = false;
        } else if !in_str {
            match c {
                '(' => depth += 1,
                ')' => depth -= 1,
                _ => {}
            }
            if depth == 0 && i + op_chars.len() <= chars.len() {
                let matches = op_chars.iter().enumerate().all(|(j, &oc)| chars[i + j] == oc);
                if matches {
                    return Some(i);
                }
            }
        }
        i += 1;
    }
    None
}

fn unquote_string(s: &str) -> String {
    let s = s.trim();
    if s.len() >= 2 {
        let first = s.chars().next().unwrap();
        let last = s.chars().last().unwrap();
        if (first == '"' && last == '"') || (first == '\'' && last == '\'') {
            return s[1..s.len() - 1].to_string();
        }
    }
    s.to_string()
}

pub fn build_merged_env(
    global_vars: &HashMap<String, String>,
    job_env: &HashMap<String, String>,
    step_env: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut merged: HashMap<String, String> = std::env::vars().collect();
    for (k, v) in global_vars {
        merged.insert(k.clone(), v.clone());
    }
    for (k, v) in job_env {
        merged.insert(k.clone(), v.clone());
    }
    for (k, v) in step_env {
        merged.insert(k.clone(), v.clone());
    }
    merged
}
