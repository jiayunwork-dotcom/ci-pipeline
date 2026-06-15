use crate::models::*;
use std::collections::HashMap;

pub fn expand_matrix_jobs(jobs: Vec<Job>) -> Vec<Job> {
    let mut expanded = Vec::new();
    for job in jobs {
        if let Some(matrix) = &job.matrix {
            let combinations = generate_combinations(matrix);
            for (idx, combo) in combinations.iter().enumerate() {
                let suffix = build_matrix_suffix(combo);
                let mut new_job = job.clone();
                new_job.name = if combinations.len() > 1 {
                    format!("{}/{}", job.name, suffix)
                } else {
                    job.name.clone()
                };
                let original_name = job.name.clone();
                new_job.depends_on = expand_depends_on(&job.depends_on, matrix);
                let params_str = combo.iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join(" ");
                for (k, v) in combo {
                    new_job.env.insert(k.clone(), v.clone());
                }
                new_job.env.insert("MATRIX_JOB_INDEX".to_string(), idx.to_string());
                new_job.env.insert("MATRIX_COMBINATION".to_string(), params_str);
                new_job.env.insert("ORIGINAL_JOB_NAME".to_string(), original_name);
                new_job.matrix = None;
                expanded.push(new_job);
            }
        } else {
            expanded.push(job);
        }
    }
    expanded
}

fn generate_combinations(
    matrix: &HashMap<String, Vec<String>>,
) -> Vec<HashMap<String, String>> {
    let keys: Vec<&String> = matrix.keys().collect();
    let values: Vec<&Vec<String>> = keys.iter().map(|k| &matrix[*k]).collect();
    let combos = cartesian_product(&values);

    combos
        .into_iter()
        .map(|combo| {
            keys.iter()
                .zip(combo.iter())
                .map(|(k, v)| ((*k).clone(), v.clone()))
                .collect()
        })
        .collect()
}

fn cartesian_product<T: Clone>(lists: &[&Vec<T>]) -> Vec<Vec<T>> {
    if lists.is_empty() {
        return vec![Vec::new()];
    }
    let first = lists[0];
    let rest = &lists[1..];
    let rest_product = cartesian_product(rest);

    let mut result = Vec::new();
    for item in first {
        for rest_combo in &rest_product {
            let mut combo = vec![item.clone()];
            combo.extend(rest_combo.clone());
            result.push(combo);
        }
    }
    result
}

fn build_matrix_suffix(combo: &HashMap<String, String>) -> String {
    let mut keys: Vec<&String> = combo.keys().collect();
    keys.sort();
    keys.iter()
        .map(|k| combo[*k].clone())
        .collect::<Vec<_>>()
        .join("-")
}

fn expand_depends_on(deps: &[String], current_matrix: &HashMap<String, Vec<String>>) -> Vec<String> {
    deps.iter().cloned().collect()
}

pub fn matrix_dependencies_ok(
    all_jobs: &[Job],
    job: &Job,
    original_jobs: &[Job],
) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cartesian_product() {
        let lists: Vec<&Vec<String>> = vec![
            &vec!["linux".to_string(), "macos".to_string()],
            &vec!["amd64".to_string(), "arm64".to_string()],
        ];
        let result = cartesian_product(&lists);
        assert_eq!(result.len(), 4);
        assert!(result.contains(&vec!["linux".to_string(), "amd64".to_string()]));
        assert!(result.contains(&vec!["linux".to_string(), "arm64".to_string()]));
        assert!(result.contains(&vec!["macos".to_string(), "amd64".to_string()]));
        assert!(result.contains(&vec!["macos".to_string(), "arm64".to_string()]));
    }

    #[test]
    fn test_expand_matrix_jobs() {
        let jobs = vec![Job {
            name: "build".to_string(),
            stage: None,
            depends_on: vec![],
            steps: vec![Step {
                name: Some("build".to_string()),
                run: "echo build".to_string(),
                env: HashMap::new(),
                allow_failure: false,
            }],
            condition: None,
            timeout: None,
            retry: None,
            artifacts: vec![],
            cache: vec![],
            services: vec![],
            matrix: Some({
                let mut m = HashMap::new();
                m.insert("os".to_string(), vec!["linux".to_string(), "macos".to_string()]);
                m.insert("arch".to_string(), vec!["amd64".to_string(), "arm64".to_string()]);
                m
            }),
            needs_artifacts: vec![],
            env: HashMap::new(),
        }];
        let expanded = expand_matrix_jobs(jobs);
        assert_eq!(expanded.len(), 4);
        let names: Vec<&str> = expanded.iter().map(|j| j.name.as_str()).collect();
        assert!(names.iter().any(|n| n.contains("build/")));
    }
}
