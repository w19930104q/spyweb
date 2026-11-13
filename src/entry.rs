use anyhow::Result;
use smol::{Executor, Timer};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::config::loader::{self, JobControls};
use crate::config::types::Jobs;
use crate::scraper::control::JobControl;
use crate::scraper::pipeline;
use crate::scraper::runner::Runner;
use crate::services::db::Db;
use crate::services::server::{JobSummary, WebServer};
use crate::services::utils::{Phase, register_shutdown_hook, shutdown_system};
use crate::services::watcher;

/// Extra grace period given to a cancelled cycle after a reload/stop before
/// its task is dropped. Per-job `cycle_timeout_secs` is the primary bound;
/// this only covers the unwind after the token flips.
const FORCED_DRAIN_GRACE_SECS: u64 = 10;

pub fn start_app() -> Result<()> {
    start_app_with_port(None, false)
}

pub fn start_app_with_port(port_override: Option<u16>, disable_server: bool) -> Result<()> {
    if let Err(e) = ctrlc::set_handler(move || {
        crate::t_println!("\nShutting down gracefully...");
        shutdown_system();
        std::process::exit(0);
    }) {
        crate::t_warnln!("Warning: Could not set Ctrl-C handler: {}", e);
    }

    let db = Arc::new(Db::open("data")?);
    crate::lua::startup::run(Arc::clone(&db))?;
    let controls = loader::empty_controls();
    let jobs = loader::load_all_jobs("jobs.toml", "jobs", Arc::clone(&db), Arc::clone(&controls))?;

    register_shutdown_hook(Phase::Post, {
        let db = Arc::clone(&db);
        move || {
            if let Err(e) = db.close() {
                crate::t_eprintln!("database close failed: {}", e);
            }
        }
    });

    let (stop_tx, stop_rx) = smol::channel::unbounded::<()>();
    register_shutdown_hook(Phase::Pre, move || {
        let _ = stop_tx.send_blocking(()); // Err = job_manager already gone
    });

    let active_job_ids = Arc::new(std::sync::RwLock::new(
        jobs.list
            .iter()
            .map(|j| JobSummary {
                id: j.config.id(),
                name: j.config.name.clone(),
                enabled: j.config.options.enabled,
            })
            .collect::<Vec<JobSummary>>(),
    ));

    let ex = Arc::new(Executor::new());
    let (_s, r) = smol::channel::unbounded::<()>();

    let thread_count = std::env::var("SPYWEB_THREADS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(2)
        .clamp(1, 64);

    for _ in 0..thread_count {
        let ex = Arc::clone(&ex);
        let s = r.clone();
        thread::spawn(move || smol::block_on(ex.run(s.recv())));
    }

    let server_disabled = disable_server || std::env::var("SPYWEB_DISABLE_SERVER").is_ok();
    if !server_disabled {
        let server_db = Arc::clone(&db);
        let server_active_jobs = Arc::clone(&active_job_ids);
        thread::spawn(move || {
            let server = WebServer::new(server_db, server_active_jobs);
            let addr = crate::config::get_base_url_with_override(port_override);
            if let Err(e) = server.listen(&addr) {
                crate::t_eprintln!("Server error: {}", e);
            }
        });
    }

    let (reload_tx, reload_rx) = smol::channel::unbounded::<()>();
    thread::spawn(move || {
        if let Err(e) = watcher::watch_configs(reload_tx) {
            crate::t_eprintln!("Watcher error: {}", e);
        }
    });

    let runner = Arc::new(Runner::new());
    let ex_manager: Arc<Executor<'static>> = Arc::clone(&ex);
    let manager_active_jobs = Arc::clone(&active_job_ids);
    let manager_controls = Arc::clone(&controls);
    ex.spawn(async move {
        job_manager(
            ex_manager,
            runner,
            db,
            jobs,
            manager_controls,
            reload_rx,
            stop_rx,
            manager_active_jobs,
        )
        .await;
    })
    .detach();

    smol::block_on(smol::future::pending::<()>());
    Ok(())
}

struct RunningJob {
    handle: smol::Task<()>,
    control: Arc<JobControl>,
    /// Explicit per-cycle timeout configured when the task was spawned.
    cycle_timeout_secs: u32,
}

fn spawn_jobs(
    ex: &Arc<Executor<'static>>,
    runner: &Arc<Runner>,
    db: &Arc<Db>,
    jobs: Jobs,
    controls: &JobControls,
) -> Vec<RunningJob> {
    jobs.list
        .into_iter()
        .filter(|job| job.config.options.enabled)
        .map(|job| {
            let job_id = job.config.id();
            let cycle_timeout_secs = job.config.options.cycle_timeout_secs;
            let control = {
                let mut registry = controls.write().unwrap_or_else(|e| e.into_inner());
                let control = registry.entry(job_id.clone()).or_default().clone();
                // Fresh task: it must not inherit a reload meant for the
                // previous generation of this job's task.
                control.rearm();
                control
            };
            let runner = Arc::clone(runner);
            let db = Arc::clone(db);
            let task_ex = Arc::clone(ex);
            let job_control = Arc::clone(&control);
            // Two distinct Arcs: the receiver borrow (`ex.spawn`) and the
            // future's moved capture cannot share one binding at construction.
            let handle = ex.spawn(async move {
                pipeline::run_job_loop(job, db, runner, &task_ex, job_control).await;
            });
            RunningJob {
                handle,
                control,
                cycle_timeout_secs,
            }
        })
        .collect()
}

/// Maximum time the manager waits for old tasks to reach their cycle boundary
/// after a reload. A job with an explicit timeout is bounded by it; others get
/// a fixed grace window before their token is force-cancelled.
fn drain_deadline(running: &[RunningJob]) -> Duration {
    let explicit = running
        .iter()
        .map(|j| j.cycle_timeout_secs)
        .filter(|secs| *secs > 0)
        .max()
        .unwrap_or(0);
    let secs = (explicit as u64 + FORCED_DRAIN_GRACE_SECS).max(FORCED_DRAIN_GRACE_SECS);
    Duration::from_secs(secs)
}

// The shared control registry is the eighth argument; keeping it explicit
// beats bundling it into a context struct just to satisfy the arg-count lint.
#[allow(clippy::too_many_arguments)]
async fn job_manager(
    ex: Arc<Executor<'static>>,
    runner: Arc<Runner>,
    db: Arc<Db>,
    initial_jobs: Jobs,
    controls: JobControls,
    reload_rx: smol::channel::Receiver<()>,
    stop_rx: smol::channel::Receiver<()>,
    active_job_ids: Arc<std::sync::RwLock<Vec<JobSummary>>>,
) {
    enum Signal {
        Reload,
        Stop,
    }

    let mut running = spawn_jobs(&ex, &runner, &db, initial_jobs, &controls);

    loop {
        // Reload keeps the loop alive; Stop (or either channel closing)
        // falls out and drains every job task.
        let signal = smol::future::or(
            async { reload_rx.recv().await.ok().map(|_| Signal::Reload) },
            async { stop_rx.recv().await.ok().map(|_| Signal::Stop) },
        )
        .await;
        let stopping = matches!(signal, None | Some(Signal::Stop));
        if !stopping {
            // Coalesce burst save events (editors write in multiple steps).
            Timer::after(Duration::from_millis(200)).await;
            while reload_rx.try_recv().is_ok() {}
        }

        if stopping {
            // Cooperative shutdown: cancel tokens so every side-effect gate
            // closes, then give tasks a bounded moment to unwind before drop.
            for job in &running {
                job.control.cancel_current();
            }
            drain_jobs(&mut running, Duration::from_secs(FORCED_DRAIN_GRACE_SECS)).await;
            break;
        }

        crate::t_println!("Reloading config...");

        let new_jobs = match loader::load_all_jobs(
            "jobs.toml",
            "jobs",
            Arc::clone(&db),
            Arc::clone(&controls),
        ) {
            Ok(new_jobs) => new_jobs,
            Err(e) => {
                crate::t_eprintln!("Config reload failed: {}", e);
                continue;
            }
        };

        // Mark every known job: running cycles keep the OLD config; the new
        // config takes effect at the next cycle boundary.
        for job in &running {
            job.control.request_reload();
        }

        let deadline = drain_deadline(&running);
        drain_jobs(&mut running, deadline).await;
        // Browsers belong to the old Lua VMs; only tear them down once the
        // old tasks have unwound past their cycle boundary.
        crate::cdp::browser::shutdown_all();

        if let Ok(mut lock) = active_job_ids.write() {
            *lock = new_jobs
                .list
                .iter()
                .map(|j| JobSummary {
                    id: j.config.id(),
                    name: j.config.name.clone(),
                    enabled: j.config.options.enabled,
                })
                .collect();
        }

        running = spawn_jobs(&ex, &runner, &db, new_jobs, &controls);
        crate::t_println!(
            "{} {} jobs (new config applies at the next cycle)",
            crate::color::c_ok("Reloaded"),
            crate::color::c_info(&running.len().to_string())
        );
    }
}

/// Wait for job tasks to finish naturally at their cycle boundary; once the
/// deadline passes, force-cancel any straggler's active token and drop its
/// task. Dropping remains the backstop — the token is what makes the drop safe
/// (no late store/notification can land afterwards).
async fn drain_jobs(jobs: &mut Vec<RunningJob>, deadline: Duration) {
    if jobs.is_empty() {
        return;
    }

    let step = Duration::from_millis(100);
    let mut remaining = deadline;
    while remaining > Duration::ZERO && jobs.iter().any(|j| !j.handle.is_finished()) {
        Timer::after(step).await;
        remaining = remaining.saturating_sub(step);
    }

    let stragglers: Vec<_> = jobs
        .iter()
        .filter(|j| !j.handle.is_finished())
        .map(|j| Arc::clone(&j.control))
        .collect();
    if !stragglers.is_empty() {
        crate::t_warnln!(
            "{} job task(s) past the drain deadline; force-cancelling their cycles",
            stragglers.len()
        );
        for control in &stragglers {
            control.cancel_current();
        }
        // Short unwind window after the token flips.
        let mut grace = Duration::from_secs(2);
        while grace > Duration::ZERO && jobs.iter().any(|j| !j.handle.is_finished()) {
            Timer::after(step).await;
            grace = grace.saturating_sub(step);
        }
    }

    let old = std::mem::take(jobs);
    for job in old {
        if job.handle.is_finished() {
            job.handle.await;
        } else {
            // Final backstop: drop the still-running task. Its token is
            // cancelled, so anything it still touches is gated off.
            drop(job.handle);
        }
    }
}

// (module tests below)

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::types::{Job, JobConfig, Jobs};
    use tempfile::TempDir;

    fn make_job(name: &str, enabled: bool) -> Job {
        Job {
            config: JobConfig {
                name: name.to_string(),
                url: "http://example.com".to_string(),
                selector: "div.item".to_string(),
                fields: vec![],
                options: crate::config::types::JobOptions {
                    enabled,
                    interval: 60,
                    ..Default::default()
                },
            },
            hooks: None,
            has_hooks_file: false,
            dir: None,
        }
    }

    fn make_temp_db() -> (TempDir, Db) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data");
        let path_str = path.to_str().unwrap();
        let db = Db::open(path_str).unwrap();
        (dir, db)
    }

    #[test]
    fn test_job_manager_reload_then_stop() {
        let (_dir, db) = make_temp_db();
        let ex = Arc::new(Executor::new());
        let runner = Arc::new(Runner::new());
        let (reload_tx, reload_rx) = smol::channel::unbounded::<()>();
        let (stop_tx, stop_rx) = smol::channel::unbounded::<()>();
        let active_job_ids = Arc::new(std::sync::RwLock::new(Vec::new()));
        let jobs = Jobs { list: vec![] };
        let controls = loader::empty_controls();

        let mgr = job_manager(
            ex,
            runner,
            Arc::new(db),
            jobs,
            controls,
            reload_rx,
            stop_rx,
            active_job_ids,
        );

        smol::block_on(async {
            // Reload must not terminate the manager...
            reload_tx.send(()).await.unwrap();
            // ...but Stop must (either channel closing counts too).
            stop_tx.send(()).await.unwrap();

            smol::future::or(mgr, async {
                Timer::after(Duration::from_secs(5)).await;
                panic!("job_manager did not stop after stop signal");
            })
            .await;
        });
    }

    #[test]
    fn test_job_manager_stops_when_channels_close() {
        let (_dir, db) = make_temp_db();
        let ex = Arc::new(Executor::new());
        let runner = Arc::new(Runner::new());
        let (reload_tx, reload_rx) = smol::channel::unbounded::<()>();
        let (_stop_tx, stop_rx) = smol::channel::unbounded::<()>();
        let active_job_ids = Arc::new(std::sync::RwLock::new(Vec::new()));
        let jobs = Jobs { list: vec![] };
        let controls = loader::empty_controls();

        let mgr = job_manager(
            ex,
            runner,
            Arc::new(db),
            jobs,
            controls,
            reload_rx,
            stop_rx,
            active_job_ids,
        );
        drop(reload_tx); // dropped sender → recv errors → break (old behavior)

        smol::block_on(async {
            smol::future::or(mgr, async {
                Timer::after(Duration::from_secs(5)).await;
                panic!("job_manager did not stop after channel close");
            })
            .await;
        });
    }

    #[test]
    fn test_spawn_jobs_filters_disabled() {
        let (_dir, db) = make_temp_db();
        let ex = Arc::new(Executor::new());
        let runner = Arc::new(Runner::new());
        let db = Arc::new(db);
        let controls = loader::empty_controls();

        let jobs = Jobs {
            list: vec![
                make_job("enabled_job_1", true),
                make_job("disabled_job", false),
                make_job("enabled_job_2", true),
            ],
        };

        let running = spawn_jobs(&ex, &runner, &db, jobs, &controls);

        assert_eq!(running.len(), 2);
        drop(running);
    }

    #[test]
    fn test_spawn_jobs_all_disabled() {
        let (_dir, db) = make_temp_db();
        let ex = Arc::new(Executor::new());
        let runner = Arc::new(Runner::new());
        let db = Arc::new(db);
        let controls = loader::empty_controls();

        let jobs = Jobs {
            list: vec![
                make_job("disabled_job_1", false),
                make_job("disabled_job_2", false),
            ],
        };

        let running = spawn_jobs(&ex, &runner, &db, jobs, &controls);

        assert_eq!(running.len(), 0);
        drop(running);
    }

    #[test]
    fn test_spawn_jobs_all_enabled() {
        let (_dir, db) = make_temp_db();
        let ex = Arc::new(Executor::new());
        let runner = Arc::new(Runner::new());
        let db = Arc::new(db);
        let controls = loader::empty_controls();

        let jobs = Jobs {
            list: vec![
                make_job("enabled_job_1", true),
                make_job("enabled_job_2", true),
                make_job("enabled_job_3", true),
            ],
        };

        let running = spawn_jobs(&ex, &runner, &db, jobs, &controls);

        assert_eq!(running.len(), 3);
        drop(running);
    }

    #[test]
    fn test_spawn_jobs_registers_controls_by_stable_job_id() {
        let (_dir, db) = make_temp_db();
        let ex = Arc::new(Executor::new());
        let runner = Arc::new(Runner::new());
        let db = Arc::new(db);
        let controls = loader::empty_controls();

        let jobs = Jobs {
            list: vec![make_job("Stable Job", true)],
        };

        let running = spawn_jobs(&ex, &runner, &db, jobs, &controls);
        assert_eq!(running.len(), 1);

        // The registry must key on the normalized job id so a later reload
        // signals the same control the running task holds.
        let registered = controls
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get("stable_job")
            .map(Arc::clone);
        assert!(registered.is_some(), "control must be registered by job id");
        assert!(Arc::ptr_eq(
            registered.as_ref().unwrap(),
            &running[0].control
        ));
        drop(running);
    }

    #[test]
    fn test_drop_running_job_cancels_task() {
        let (_dir, db) = make_temp_db();
        let ex = Arc::new(Executor::new());
        let runner = Arc::new(Runner::new());
        let db = Arc::new(db);
        let controls = loader::empty_controls();

        let jobs = Jobs {
            list: vec![make_job("test_job", true)],
        };

        let running = spawn_jobs(&ex, &runner, &db, jobs, &controls);

        let job = running.into_iter().next().unwrap();
        drop(job);

        let jobs2 = Jobs { list: vec![] };
        let running2 = spawn_jobs(&ex, &runner, &db, jobs2, &controls);
        assert_eq!(running2.len(), 0);
    }
}
