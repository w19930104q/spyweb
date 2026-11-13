#[allow(clippy::duplicate_mod)]
#[path = "shared/mod.rs"]
mod helpers;

use helpers::{DbVariant, TestEnv};
use spyweb::scraper::control::JobControl;
use spyweb::scraper::pipeline::{CycleRunner, CycleStats};
use spyweb::scraper::runner::Runner;
use std::sync::Arc;
use std::time::Duration;

const FIXTURE: &str = "reload-recovery";
const JOB_ID: &str = "reload_recovery";

/// Background executor with one worker thread, kept alive for the test.
fn test_executor() -> (Arc<smol::Executor<'static>>, smol::channel::Sender<()>) {
    let ex = Arc::new(smol::Executor::new());
    let (tx, rx) = smol::channel::unbounded::<()>();
    let executor = Arc::clone(&ex);
    std::thread::spawn(move || smol::block_on(executor.run(rx.recv())));
    (ex, tx)
}

fn control_of(job: &spyweb::config::types::Job) -> Arc<JobControl> {
    job.hooks
        .as_ref()
        .expect("fixture must load hooks.lua")
        .control()
}

fn stored_titles(env: &TestEnv) -> Vec<String> {
    env.db
        .get_all_records(None)
        .expect("records readable")
        .remove(JOB_ID)
        .map(|records| {
            let mut titles: Vec<String> = records
                .iter()
                .filter_map(|r| r.fields.get("title").cloned())
                .collect();
            titles.sort();
            titles
        })
        .unwrap_or_default()
}

/// Seed batch size in the fixture (3 seed URLs + 1 queue_push).
const TOTAL_URLS: usize = 4;

// ---------------------------------------------------------------------------
// (a) Mid-cycle config change: the running cycle keeps its old config and is
// allowed to finish; the reload only takes effect at the next cycle boundary.
// ---------------------------------------------------------------------------

#[test]
fn reload_requested_mid_cycle_lets_running_cycle_complete() {
    let env = TestEnv::new(FIXTURE, DbVariant::Kv);
    let job = Arc::new(env.load_job(FIXTURE));
    let control = control_of(&job);
    let runner = Arc::new(Runner::new());
    let (ex, _keep_alive) = test_executor();

    // Park the first fetched URL in-flight so the reload lands while the
    // workers are definitely inside the running cycle.
    env.set_global_string(&job, "slow_url", "http://127.0.0.1:1/page/seed-1");
    env.set_global_u64(&job, "fetch_sleep_ms", 800);

    smol::block_on(async {
        let cycle = CycleRunner::new(&job, &env.db, &runner, &ex, job.config.worker_count())
            .with_control(Arc::clone(&control));

        // Signal branch: wait for the cycle token, then mark the reload. It
        // never completes, so `or` resolves with the real cycle's stats. Both
        // branches borrow stack data, which is fine under block_on (no spawn).
        let run = async { cycle.run_iteration().await };
        let signal = async {
            for _ in 0..50 {
                if control.current_token().is_some() {
                    break;
                }
                smol::Timer::after(Duration::from_millis(20)).await;
            }
            assert!(
                control.current_token().is_some(),
                "cycle must have installed its token before the reload"
            );
            control.request_reload();

            // The running cycle is NOT cancelled: side effects stay open.
            assert!(control.side_effects_open());
            smol::future::pending::<Arc<CycleStats>>().await
        };

        let stats = smol::future::or(run, signal).await;

        assert_eq!(stats.failed(), 0);
        assert_eq!(stats.processed(), TOTAL_URLS);
        // The epoch-based reload signal survives the completed cycle: the
        // manager would respawn the job from the new config at this boundary.
        assert!(control.reload_requested());
    });

    // Every URL reached the store exactly once despite the mid-cycle reload.
    let titles = stored_titles(&env);
    assert_eq!(
        titles,
        vec![
            "pushed-1".to_string(),
            "seed-1".to_string(),
            "seed-2".to_string(),
            "seed-3".to_string(),
        ]
    );
}

// ---------------------------------------------------------------------------
// (b) Explicit cycle timeout: the cycle is cancelled cooperatively, late
// results are neither stored nor notified, and unfinished URLs stay durable.
// ---------------------------------------------------------------------------

#[test]
fn cycle_timeout_cancels_cooperatively_and_keeps_urls_durable() {
    let env = TestEnv::new(FIXTURE, DbVariant::Kv);
    let job = Arc::new(env.load_job(FIXTURE));
    let control = control_of(&job);
    let runner = Arc::new(Runner::new());
    let (ex, _keep_alive) = test_executor();

    // First URL sleeps longer than the timeout; the second worker stalls too,
    // so at least one URL cannot complete before the limit fires.
    env.set_global_string(&job, "slow_url", "http://127.0.0.1:1/page/seed-1");
    env.set_global_u64(&job, "fetch_sleep_ms", 3000);

    let depth_before = env.db.queue_depth(JOB_ID).unwrap();
    assert_eq!(depth_before, 0, "no durable rows before the first cycle");

    let stats = smol::block_on(async {
        let cycle = CycleRunner::new(&job, &env.db, &runner, &ex, job.config.worker_count())
            .with_control(Arc::clone(&control));
        cycle
            .run_iteration_with_timeout(Duration::from_millis(400))
            .await
    });

    // The timeout fired: the active token is closed by the iteration itself.
    assert!(
        !control.side_effects_open(),
        "side-effect gates must be closed after the timeout"
    );

    // Nothing that was still in flight could be stored late.
    let titles = stored_titles(&env);
    let unfinished = env.db.queue_depth(JOB_ID).unwrap();
    assert!(unfinished > 0, "unfinished URLs must remain durable");
    assert!(
        unfinished <= TOTAL_URLS,
        "durable set cannot exceed the accepted URL set"
    );

    // Whatever was stored before cancellation is strictly fewer than all URLs.
    assert!(
        titles.len() < TOTAL_URLS,
        "at least one late result must have been refused, got {titles:?}"
    );
    assert!(stats.processed() + stats.failed() <= TOTAL_URLS);
}

// ---------------------------------------------------------------------------
// (c) Process restart: close and reopen the database, reload the job, and run
// again. Unfinished durable URLs are resumed; dedupe prevents duplicates.
// ---------------------------------------------------------------------------

#[test]
fn restart_resumes_unfinished_urls_without_duplicates() {
    let mut env = TestEnv::new(FIXTURE, DbVariant::Kv);
    let job = Arc::new(env.load_job(FIXTURE));
    let control = control_of(&job);
    let runner = Arc::new(Runner::new());
    let (ex, _keep_alive) = test_executor();

    env.set_global_string(&job, "slow_url", "http://127.0.0.1:1/page/seed-1");
    env.set_global_u64(&job, "fetch_sleep_ms", 3000);

    smol::block_on(async {
        let first_cycle = CycleRunner::new(&job, &env.db, &runner, &ex, job.config.worker_count())
            .with_control(Arc::clone(&control));
        first_cycle
            .run_iteration_with_timeout(Duration::from_millis(300))
            .await
    });

    let pending_after_cancel = env.db.queue_depth(JOB_ID).unwrap();
    assert!(
        pending_after_cancel > 0,
        "cancellation must leave recoverable URLs behind"
    );
    let titles_after_cancel = stored_titles(&env);

    // Simulate a process restart: drop the live DB handle and reopen the file.
    env.reopen_db();
    assert_eq!(
        env.db.queue_depth(JOB_ID).unwrap(),
        pending_after_cancel,
        "durable queue must survive the restart"
    );

    // Reload the job against the reopened database (fresh Lua VM + control).
    let restarted = Arc::new(env.load_job(FIXTURE));
    let restarted_control = control_of(&restarted);
    assert_eq!(
        restarted_control.generation(),
        0,
        "the new process starts its own generation sequence"
    );

    // No slow URL this time: the resumed cycle drains everything.
    let stats = smol::block_on(async {
        let second_cycle = CycleRunner::new(
            &restarted,
            &env.db,
            &runner,
            &ex,
            restarted.config.worker_count(),
        )
        .with_control(Arc::clone(&restarted_control));
        second_cycle.run_iteration().await
    });

    assert_eq!(stats.failed(), 0);
    assert_eq!(
        env.db.queue_depth(JOB_ID).unwrap(),
        0,
        "every accepted URL must be completed after the resume"
    );

    // Full set stored, with no duplicates from the rerun.
    let titles = stored_titles(&env);
    assert_eq!(
        titles,
        vec![
            "pushed-1".to_string(),
            "seed-1".to_string(),
            "seed-2".to_string(),
            "seed-3".to_string(),
        ]
    );
    assert!(titles_after_cancel.len() <= titles.len());
    assert_eq!(
        titles.len(),
        titles
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        "rerun must not produce duplicate records"
    );
}

// ---------------------------------------------------------------------------
// (d) Duplicate notifications:
//   1. The Lua `notify` global refuses calls once the cycle token is closed
//      (old defer.lua callbacks cannot notify after cancel/restart).
//   2. Re-running the same URLs through the pipeline stores nothing twice and
//      therefore cannot produce a second notification for the same item.
// ---------------------------------------------------------------------------

#[test]
fn late_notify_is_gated_and_reruns_do_not_duplicate() {
    let env = TestEnv::new(FIXTURE, DbVariant::Kv);
    let job = Arc::new(env.load_job(FIXTURE));
    let control = control_of(&job);
    let runner = Arc::new(Runner::new());
    let (ex, _keep_alive) = test_executor();

    // --- Part 1: the engine-level notify gate -----------------------------
    smol::block_on(async {
        let hooks = job.hooks.as_ref().unwrap();

        // Between cycles there is no active token: notify must be refused.
        assert!(!control.side_effects_open());
        let blocked = hooks
            .call_global::<_, ()>(
                "notify",
                ("blocked".to_string(), "blocked".to_string(), 1u32),
            )
            .await;
        assert!(
            blocked.is_err(),
            "notify must error while no cycle is active"
        );

        // Inside an open cycle the gate passes (headless no-op notification).
        let token = control.begin_cycle();
        assert!(control.side_effects_open());
        let allowed = hooks
            .call_global::<_, ()>("notify", ("title".to_string(), "body".to_string(), 1u32))
            .await;
        assert!(
            allowed.is_ok(),
            "notify must succeed while the cycle is open"
        );

        // Closing the token re-closes the gate immediately: a late callback
        // (e.g. from an old defer.lua) cannot notify after cancellation.
        token.cancel();
        assert!(!control.side_effects_open());
        let late = hooks
            .call_global::<_, ()>("notify", ("late".to_string(), "late".to_string(), 1u32))
            .await;
        assert!(
            late.is_err(),
            "a late notify after cancellation must be refused"
        );

        control.end_cycle();
    });

    // --- Part 2: rerun dedupe ---------------------------------------------
    let (first, second) = smol::block_on(async {
        let cycle = CycleRunner::new(&job, &env.db, &runner, &ex, job.config.worker_count())
            .with_control(Arc::clone(&control));
        let first = cycle.run_iteration().await;
        // The exact same job a second time: the durable queue emptied on
        // success, so the rerun starts from the same seed batch again.
        let second = cycle.run_iteration().await;
        (first, second)
    });

    assert_eq!(first.processed(), TOTAL_URLS);
    assert_eq!(first.failed(), 0);
    assert_eq!(second.failed(), 0);

    // The seen-table dedupe makes every second-run item a duplicate: no new
    // rows, hence nothing new to notify about.
    let titles = stored_titles(&env);
    assert_eq!(
        titles.len(),
        TOTAL_URLS,
        "store stays at the unique item count, got {titles:?}"
    );
    assert_eq!(
        titles.len(),
        titles
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        "rerun must not produce duplicate records"
    );
}
