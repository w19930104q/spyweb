use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::{fs, path::Path};

use crate::config;
use crate::config::validate;
use crate::lua::hooks::JobHooks;
use crate::scraper::control::JobControl;
use crate::services::db::Db;
use anyhow::Result;
use config::types::{Job, JobConfig, JobFile, Jobs};

/// Registry mapping a stable job id to its process-lifetime control.
///
/// Job ids survive hot reload, so a control created on first load is reused by
/// every subsequent rebuild of the job's config and Lua VM. This is what lets a
/// reload observed mid-cycle reach the still-running, old-config cycle.
pub type JobControls = Arc<RwLock<HashMap<String, Arc<JobControl>>>>;

/// Fresh, empty registry. The running engine shares one registry across every
/// reload; one-shot CLI paths (`debug`, `profile`, tests) create their own.
pub fn empty_controls() -> JobControls {
    Arc::new(RwLock::new(HashMap::new()))
}

pub fn load_all_jobs(
    file_path: &str,
    dir_path: &str,
    db: Arc<Db>,
    controls: JobControls,
) -> Result<Jobs> {
    let mut list = load_config_from_file(file_path)?;
    list.extend(load_config_from_dir(dir_path, db, controls)?);
    validate::validate_job_set(&list)?;
    Ok(Jobs { list })
}

pub fn load_dir_jobs(dir_path: &str, db: Arc<Db>, controls: JobControls) -> Result<Vec<Job>> {
    load_config_from_dir(dir_path, db, controls)
}

fn load_config_from_file(path: &str) -> Result<Vec<Job>> {
    let path = Path::new(path);
    if !path.exists() {
        return Ok(vec![]);
    }

    let content = fs::read_to_string(path)?;
    let file: JobFile = toml::from_str(&content)
        .map_err(|e| anyhow::anyhow!("Config error in '{}': {}", path.display(), e))?;

    let mut jobs = Vec::with_capacity(file.jobs.len());
    for config in file.jobs {
        validate::validate_job_config(&config)?;
        jobs.push(Job {
            config,
            hooks: None,
            has_hooks_file: false,
            dir: None,
        });
    }

    Ok(jobs)
}

fn load_config_from_dir(path: &str, db: Arc<Db>, controls: JobControls) -> Result<Vec<Job>> {
    let mut jobs: Vec<Job> = vec![];
    let base = Path::new(path);
    if !base.exists() {
        return Ok(jobs);
    }
    for entry in fs::read_dir(base)? {
        let dir = entry?.path();
        if !dir.is_dir() {
            continue;
        }

        let config_path = dir.join("config.toml");
        if !config_path.exists() {
            continue;
        }

        let content = fs::read_to_string(&config_path)?;
        let config: JobConfig = toml::from_str(&content)
            .map_err(|e| anyhow::anyhow!("Config error in '{}': {}", config_path.display(), e))?;
        validate::validate_job_config(&config)?;
        let hook_path = dir.join("hooks.lua");
        let has_hooks_file = hook_path.exists();

        // Reuse the same control across reloads; create one on first sight.
        let job_id = config.id();
        let control = {
            let mut registry = controls.write().unwrap_or_else(|e| e.into_inner());
            registry.entry(job_id.clone()).or_default().clone()
        };

        let hooks = if config.options.enabled && has_hooks_file {
            match JobHooks::load(&hook_path, Arc::clone(&db), &job_id, control) {
                Ok(h) => Some(h),
                Err(e) => {
                    crate::t_eprintln!("Failed to load hooks for '{}': {}", config.name, e);
                    None
                }
            }
        } else {
            None
        };

        jobs.push(Job {
            config,
            hooks,
            has_hooks_file,
            dir: Some(dir),
        });
    }

    Ok(jobs)
}
