use crate::models::*;
use anyhow::{anyhow, Result};
use std::collections::{HashMap, HashSet};
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::algo::is_cyclic_directed;

#[derive(Debug, Clone)]
pub struct ValidationError {
    pub location: String,
    pub message: String,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.location, self.message)
    }
}

pub fn validate_pipeline(pipeline: &Pipeline) -> Result<Vec<ValidationError>> {
    let mut errors = Vec::new();

    validate_unique_job_names(pipeline, &mut errors);
    validate_depends_on_exists(pipeline, &mut errors);
    validate_cyclic_dependencies(pipeline, &mut errors);
    validate_stage_references(pipeline, &mut errors);
    validate_job_configs(pipeline, &mut errors);
    validate_condition_expressions(pipeline, &mut errors);

    Ok(errors)
}

fn validate_unique_job_names(pipeline: &Pipeline, errors: &mut Vec<ValidationError>) {
    let mut seen = HashSet::new();
    for job in &pipeline.jobs {
        if !seen.insert(job.name.clone()) {
            errors.push(ValidationError {
                location: format!("jobs.{}", job.name),
                message: format!("Duplicate job name: '{}'", job.name),
            });
        }
    }
}

fn validate_depends_on_exists(pipeline: &Pipeline, errors: &mut Vec<ValidationError>) {
    let job_names: HashSet<&str> = pipeline.jobs.iter().map(|j| j.name.as_str()).collect();
    for job in &pipeline.jobs {
        for dep in &job.depends_on {
            if !job_names.contains(dep.as_str()) {
                errors.push(ValidationError {
                    location: format!("jobs.{}.depends_on", job.name),
                    message: format!("Job '{}' depends on non-existent job '{}'", job.name, dep),
                });
            }
        }
    }
}

fn validate_cyclic_dependencies(pipeline: &Pipeline, errors: &mut Vec<ValidationError>) {
    let mut graph = DiGraph::<String, ()>::new();
    let mut node_map: HashMap<String, NodeIndex> = HashMap::new();

    for job in &pipeline.jobs {
        let idx = graph.add_node(job.name.clone());
        node_map.insert(job.name.clone(), idx);
    }

    for job in &pipeline.jobs {
        let from_idx = node_map[&job.name];
        for dep in &job.depends_on {
            if let Some(&to_idx) = node_map.get(dep) {
                graph.add_edge(to_idx, from_idx, ());
            }
        }
    }

    if is_cyclic_directed(&graph) {
        errors.push(ValidationError {
            location: "pipeline".to_string(),
            message: "Pipeline contains cyclic dependencies".to_string(),
        });
    }
}

fn validate_stage_references(pipeline: &Pipeline, errors: &mut Vec<ValidationError>) {
    let stages_set: HashSet<&str> = pipeline.stages.iter().map(|s| s.as_str()).collect();
    for job in &pipeline.jobs {
        if let Some(stage) = &job.stage {
            if !pipeline.stages.is_empty() && !stages_set.contains(stage.as_str()) {
                errors.push(ValidationError {
                    location: format!("jobs.{}.stage", job.name),
                    message: format!(
                        "Job '{}' references non-existent stage '{}'",
                        job.name, stage
                    ),
                });
            }
        }
    }
}

fn validate_job_configs(pipeline: &Pipeline, errors: &mut Vec<ValidationError>) {
    for job in &pipeline.jobs {
        if job.steps.is_empty() {
            errors.push(ValidationError {
                location: format!("jobs.{}.steps", job.name),
                message: format!("Job '{}' must define at least one step", job.name),
            });
        }

        if let Some(timeout) = job.timeout {
            if timeout == 0 {
                errors.push(ValidationError {
                    location: format!("jobs.{}.timeout", job.name),
                    message: format!("Job '{}' timeout must be a positive integer", job.name),
                });
            }
        }

        if let Some(retry) = job.retry {
            if retry == 0 {
                errors.push(ValidationError {
                    location: format!("jobs.{}.retry", job.name),
                    message: format!("Job '{}' retry must be a positive integer", job.name),
                });
            }
        }
    }
}

fn validate_condition_expressions(pipeline: &Pipeline, errors: &mut Vec<ValidationError>) {
    for job in &pipeline.jobs {
        if let Some(cond) = &job.condition {
            if let Err(e) = validate_condition_syntax(cond) {
                errors.push(ValidationError {
                    location: format!("jobs.{}.condition", job.name),
                    message: format!("Invalid condition for job '{}': {}", job.name, e),
                });
            }
        }
    }
}

fn validate_condition_syntax(expr: &str) -> Result<()> {
    let expr = expr.trim();
    if expr.is_empty() {
        return Err(anyhow!("Empty expression"));
    }

    let mut paren_balance = 0;
    let mut in_var = false;
    let mut var_start = 0;
    let mut i = 0;
    let chars: Vec<char> = expr.chars().collect();

    while i < chars.len() {
        let c = chars[i];
        match c {
            '(' => paren_balance += 1,
            ')' => {
                paren_balance -= 1;
                if paren_balance < 0 {
                    return Err(anyhow!("Unmatched closing parenthesis at position {}", i));
                }
            }
            '$' if i + 1 < chars.len() && chars[i + 1] == '{' => {
                if i + 2 < chars.len() && chars[i + 2] == '{' {
                    in_var = true;
                    var_start = i;
                    i += 2;
                }
            }
            '}' if in_var => {
                if i + 1 < chars.len() && chars[i + 1] == '}' {
                    in_var = false;
                    let var_expr = &expr[var_start..i + 2];
                    let inner = &var_expr[3..var_expr.len() - 2];
                    if inner.trim().is_empty() {
                        return Err(anyhow!("Empty variable reference at position {}", var_start));
                    }
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }

    if in_var {
        return Err(anyhow!("Unclosed variable reference"));
    }
    if paren_balance != 0 {
        return Err(anyhow!("Unmatched parentheses"));
    }

    validate_operators(expr)?;
    validate_functions(expr)?;

    Ok(())
}

fn validate_operators(expr: &str) -> Result<()> {
    let operators = ["==", "!=", ">=", "<=", ">", "<", "&&", "||"];
    let stripped = strip_vars_and_strings(expr);

    for op in &operators {
        let _ = stripped.find(op);
    }

    let chars: Vec<char> = stripped.chars().collect();
    for (i, _c) in chars.iter().enumerate() {
        if i + 1 < chars.len() {
            let two: String = chars[i..i + 2].iter().collect();
            if two == "!=" || two == "||" || two == "&&" {
                continue;
            }
        }
    }

    Ok(())
}

fn validate_functions(expr: &str) -> Result<()> {
    let valid_fns = ["success()", "failure()", "always()"];
    let stripped = strip_vars_and_strings(expr);

    let mut i = 0;
    let chars: Vec<char> = stripped.chars().collect();
    while i < chars.len() {
        if chars[i].is_alphabetic() {
            let start = i;
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let fname = &stripped[start..i];
            if !fname.is_empty() && (fname == "success" || fname == "failure" || fname == "always" || fname == "true" || fname == "false") {
                if fname == "success" || fname == "failure" || fname == "always" {
                    let full_call = format!("{}()", fname);
                    if !stripped[start..].starts_with(&full_call) {
                        return Err(anyhow!("Function '{}' must be called as {}", fname, full_call));
                    }
                }
            } else if !fname.is_empty() {
                let is_valid = valid_fns.contains(&format!("{}()", fname).as_str())
                    || fname == "true"
                    || fname == "false"
                    || stripped[start..].starts_with("${{");
                let _ = is_valid;
            }
        }
        i += 1;
    }

    Ok(())
}

fn strip_vars_and_strings(expr: &str) -> String {
    let mut result = String::new();
    let mut in_str = false;
    let mut str_char = ' ';
    let mut in_var = 0;
    let chars: Vec<char> = expr.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if !in_str && in_var == 0 && (c == '"' || c == '\'') {
            in_str = true;
            str_char = c;
            result.push('x');
        } else if in_str && c == str_char {
            in_str = false;
            result.push('x');
        } else if !in_str && c == '$' && i + 2 < chars.len() && chars[i + 1] == '{' && chars[i + 2] == '{' {
            in_var += 1;
            result.push('x');
        } else if !in_str && in_var > 0 && c == '}' && i + 1 < chars.len() && chars[i + 1] == '}' {
            in_var -= 1;
            result.push('x');
            i += 1;
        } else if in_str || in_var > 0 {
            result.push('x');
        } else {
            result.push(c);
        }
        i += 1;
    }
    result
}
