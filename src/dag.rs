use crate::models::*;
use anyhow::{anyhow, Result};
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::algo::toposort;
use std::collections::{HashMap, HashSet};

pub struct Dag {
    pub graph: DiGraph<String, ()>,
    pub node_map: HashMap<String, NodeIndex>,
    pub rev_map: HashMap<NodeIndex, String>,
    pub job_map: HashMap<String, Job>,
}

impl Dag {
    pub fn build(jobs: &[Job]) -> Result<Self> {
        let mut graph = DiGraph::<String, ()>::new();
        let mut node_map: HashMap<String, NodeIndex> = HashMap::new();
        let mut rev_map: HashMap<NodeIndex, String> = HashMap::new();
        let mut job_map: HashMap<String, Job> = HashMap::new();

        for job in jobs {
            let idx = graph.add_node(job.name.clone());
            node_map.insert(job.name.clone(), idx);
            rev_map.insert(idx, job.name.clone());
            job_map.insert(job.name.clone(), job.clone());
        }

        for job in jobs {
            let to_idx = node_map[&job.name];
            for dep in &job.depends_on {
                if let Some(&from_idx) = node_map.get(dep) {
                    graph.add_edge(from_idx, to_idx, ());
                }
            }
        }

        Ok(Self {
            graph,
            node_map,
            rev_map,
            job_map,
        })
    }

    pub fn topological_order(&self) -> Result<Vec<String>> {
        match toposort(&self.graph, None) {
            Ok(order) => Ok(order
                .iter()
                .map(|idx| self.rev_map[idx].clone())
                .collect()),
            Err(_) => Err(anyhow!("Cycle detected in DAG")),
        }
    }

    pub fn dependencies_of(&self, job_name: &str) -> Vec<String> {
        let mut result: Vec<String> = Vec::new();
        if let Some(&idx) = self.node_map.get(job_name) {
            return self
                .graph
                .neighbors_directed(idx, petgraph::Direction::Incoming)
                .map(|n| self.rev_map[&n].clone())
                .collect();
        }
        result
    }

    pub fn dependents_of(&self, job_name: &str) -> Vec<String> {
        if let Some(&idx) = self.node_map.get(job_name) {
            return self
                .graph
                .neighbors_directed(idx, petgraph::Direction::Outgoing)
                .map(|n| self.rev_map[&n].clone())
                .collect();
        }
        Vec::new()
    }

    pub fn has_dependencies_met(
        &self,
        job_name: &str,
        completed: &HashMap<String, JobStatus>,
    ) -> bool {
        let deps = self.dependencies_of(job_name);
        if deps.is_empty() {
            return true;
        }
        deps.iter().all(|dep| {
            completed.get(dep).map_or(false, |s| matches!(s, JobStatus::Success | JobStatus::Skipped))
        })
    }

    pub fn all_deps_success(
        &self,
        job_name: &str,
        completed: &HashMap<String, JobStatus>,
    ) -> bool {
        let deps = self.dependencies_of(job_name);
        if deps.is_empty() {
            return true;
        }
        deps.iter().all(|dep| {
            completed.get(dep).map_or(false, |s| matches!(s, JobStatus::Success))
        })
    }

    pub fn any_dep_failed(
        &self,
        job_name: &str,
        completed: &HashMap<String, JobStatus>,
    ) -> bool {
        let deps = self.dependencies_of(job_name);
        deps.iter().any(|dep| {
            completed.get(dep).map_or(false, |s| {
                matches!(s, JobStatus::Failed | JobStatus::Cancelled)
            })
        })
    }

    pub fn filter_by_pattern(&mut self, pattern: &glob::Pattern) -> Result<Vec<String>> {
        let all = self.topological_order()?;
        let matched: HashSet<String> = all
            .iter()
            .filter(|name| pattern.matches(name))
            .cloned()
            .collect();

        let mut keep: HashSet<String> = HashSet::new();
        for name in &matched {
            keep.insert(name.clone());
            let mut stack = vec![name.clone()];
            while let Some(current) = stack.pop() {
                for dep in self.dependencies_of(&current) {
                    if keep.insert(dep.clone()) {
                        stack.push(dep);
                    }
                }
            }
        }

        Ok(all.into_iter().filter(|n| keep.contains(n)).collect())
    }

    pub fn as_dot(&self) -> String {
        let mut out = String::new();
        let mut result = String::from("digraph pipeline {\n");
        result.push_str("  rankdir=LR;\n");
        result.push_str("  node [shape=box, style=filled, fillcolor=lightblue];\n");

        for job in self.job_map.values() {
            let safe_name = job.name.replace('/', "_").replace('-', "_");
            let label = job.name.replace('"', "\\\"");
            result.push_str(&format!("  \"{}\" [label=\"{}\"];\n", safe_name, label));
        }

        for job in self.job_map.values() {
            let to_name = job.name.replace('/', "_").replace('-', "_");
            for dep in &job.depends_on {
                if let Some(dep_job) = self.job_map.get(dep) {
                    let from_name = dep_job.name.replace('/', "_").replace('-', "_");
                    result.push_str(&format!("  \"{}\" -> \"{}\";\n", from_name, to_name));
                }
            }
        }

        result.push_str("}\n");
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_job(name: &str, deps: Vec<&str>) -> Job {
        Job {
            name: name.to_string(),
            stage: None,
            depends_on: deps.iter().map(|s| s.to_string()).collect(),
            steps: vec![Step {
                name: None,
                run: "echo hi".to_string(),
                env: std::collections::HashMap::new(),
                allow_failure: false,
            }],
            condition: None,
            timeout: None,
            retry: None,
            artifacts: vec![],
            cache: vec![],
            services: vec![],
            matrix: None,
            needs_artifacts: vec![],
            env: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn test_dag_build() {
        let jobs = vec![
            make_job("a", vec![]),
            make_job("b", vec!["a"]),
            make_job("c", vec!["a", "b"]),
        ];
        let dag = Dag::build(&jobs).unwrap();
        let order = dag.topological_order().unwrap();
        assert_eq!(order[0], "a");
        assert!(order.iter().position(|x| x == "b") < order.iter().position(|x| x == "c"));
    }
}
