use crate::scraper::control::{CycleToken, JobControl};
use crate::scraper::coordinator::{DurableQueue, Message, QueueState};
use crate::{
    color, config::types::Job, lua::hooks::TelemetryHandle, scraper::request::RequestConfig,
    scraper::runner::Runner, services::db::Db, services::notifier, services::webhook,
};
use anyhow::Result;
use smol::channel::{Receiver, Sender, unbounded};
use smol::{Executor, Timer};
use std::collections::{HashSet, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::ThreadId;
use std::time::Duration;

pub(crate) type ThreadProbe = Option<Arc<Mutex<HashSet<ThreadId>>>>;

const WORKER_STAGGER_MS: u64 = 200;

/// Error returned when a cycle's cooperative token is cancelled before a
/// side effect (store / notification / webhook) can run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CycleCancelled;

impl std::fmt::Display for CycleCancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("cycle cancelled before side effect could run")
    }
}

impl std::error::Error for CycleCancelled {}

pub struct CycleStats {
    processed: AtomicUsize,
    failed: AtomicUsize,
    pushed: AtomicUsize,
    duplicates: AtomicUsize,
}

impl CycleStats {
    pub fn new() -> Self {
        Self {
            processed: AtomicUsize::new(0),
            failed: AtomicUsize::new(0),
            pushed: AtomicUsize::new(0),
            duplicates: AtomicUsize::new(0),
        }
    }

    pub(crate) fn record(&self, ok: bool) {
        self.processed.fetch_add(1, Ordering::Relaxed);
        if !ok {
            self.failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_pushed(&self) {
        self.pushed.fetch_add(1, Ordering::Relaxed);
    }

    fn record_duplicate(&self) {
        self.duplicates.fetch_add(1, Ordering::Relaxed);
    }

    pub fn processed(&self) -> usize {
        self.processed.load(Ordering::Relaxed)
    }

    pub fn failed(&self) -> usize {
        self.failed.load(Ordering::Relaxed)
    }

    pub fn pushed(&self) -> usize {
        self.pushed.load(Ordering::Relaxed)
    }

    pub fn duplicates(&self) -> usize {
        self.duplicates.load(Ordering::Relaxed)
    }
}

impl Default for CycleStats {
    fn default() -> Self {
        Self::new()
    }
}

fn spawn_worker<F>(
    ex: &Arc<Executor<'static>>,
    future: F,
    thread_probe: &ThreadProbe,
) -> smol::Task<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let probe = thread_probe.clone();
    ex.spawn(async move {
        if let Some(p) = probe.as_ref() {
            p.lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(std::thread::current().id());
        }
        future.await;
    })
}

pub(crate) async fn compute_cycle_batch(job: &Job) -> Option<Vec<String>> {
    let seed = if job.config.has_urls() {
        job.config.options.urls.clone()
    } else {
        None
    };
    match job.hooks.as_ref() {
        Some(hooks) if hooks.has_on_start() => hooks.run_on_start(seed).await,
        _ => seed,
    }
}

pub struct JobCtx<'a> {
    job: &'a Job,
    db: &'a Arc<Db>,
    runner: &'a Arc<Runner>,
    token: Option<Arc<CycleToken>>,
}

impl<'a> JobCtx<'a> {
    pub fn new(job: &'a Job, db: &'a Arc<Db>, runner: &'a Arc<Runner>) -> Self {
        Self {
            job,
            db,
            runner,
            token: None,
        }
    }

    /// Attach the active cycle's cancellation token. Every side-effecting
    /// stage re-checks it immediately before running, so a result that became
    /// late while an await was in flight can neither be stored nor notified.
    pub(crate) fn with_token(mut self, token: Option<Arc<CycleToken>>) -> Self {
        self.token = token;
        self
    }

    fn ensure_open(&self) -> Result<()> {
        if self.token.as_ref().is_some_and(|t| t.is_cancelled()) {
            return Err(anyhow::Error::new(CycleCancelled));
        }
        Ok(())
    }

    // One request lifecycle: ctx → telemetry → stages → finalize → on_success/on_error/on_finally → cleanup.
    pub async fn run_once(&self, base_request: &RequestConfig, worker_id: usize) -> Result<()> {
        let job = self.job;
        let ctx = match job.hooks.as_ref() {
            Some(h) => Some(h.new_cycle_context(worker_id).await?),
            None => None,
        };
        let tel = TelemetryHandle::new(job.hooks.as_ref(), ctx.as_ref());
        let started = std::time::Instant::now();
        tel.init().await?;
        let result = self.stages(base_request, &tel).await;
        tel.finalize(started.elapsed()).await;

        if let (Some(h), Some(ctx)) = (job.hooks.as_ref(), ctx.as_ref()) {
            match &result {
                Ok(()) => h.run_on_success(ctx).await,
                Err(e) => h.run_on_error(ctx, e).await,
            }
            h.run_on_finally(ctx).await;
        }

        tel.cleanup().await;
        result
    }

    // Stage pipeline: prepare → fetch → extract → filter → store → emit.
    // Each `None` return == a stage skipping the rest of the cycle (`Ok(())`).
    pub(crate) async fn stages(
        &self,
        base_request: &RequestConfig,
        tel: &TelemetryHandle<'_>,
    ) -> Result<()> {
        // A worker parked or scheduled right as the cycle was cancelled must
        // not start a new fetch at all.
        self.ensure_open()?;

        let request = match tel.hooks() {
            Some(h) => {
                let Some(ctx) = tel.ctx() else { return Ok(()) };
                match h.before_fetch(base_request.clone(), ctx).await? {
                    None => return Ok(()),
                    Some(r) => r,
                }
            }
            None => base_request.clone(),
        };

        let fetch_attempt = match tel.hooks().filter(|h| h.has_override_fetch()) {
            Some(h) => {
                let Some(ctx) = tel.ctx() else { return Ok(()) };
                h.override_fetch(request.clone(), ctx).await?
            }
            None => {
                let token = tel.start().await;
                let attempt = smol::unblock({
                    let runner = Arc::clone(self.runner);
                    let config = self.job.config.clone();
                    let request = request.clone();
                    move || runner.fetch(&config, &request)
                })
                .await;
                if let (Some(h), Some(ctx)) = (tel.hooks(), tel.ctx()) {
                    h.set_last_fetch(ctx, &attempt).await?;
                }
                let (status, error) = match &attempt.result {
                    Ok(_) => ("success", None),
                    Err(err) => ("error", Some(err.clone())),
                };
                tel.record("fetch", token, status, error).await;
                attempt
            }
        };

        let response = match tel.hooks() {
            Some(h) => {
                let Some(ctx) = tel.ctx() else { return Ok(()) };
                match h.after_fetch(fetch_attempt, ctx).await? {
                    None => return Ok(()),
                    Some(r) => r,
                }
            }
            None => fetch_attempt.result.map_err(anyhow::Error::msg)?,
        };

        let status_code = response.status;

        if status_code >= 400 {
            crate::t_warnln!(
                "Job {} fetch returned status {}",
                color::c_job(&self.job.config.name),
                color::c_warn(&status_code.to_string())
            );
        }

        let extraction = match tel.hooks().filter(|h| h.has_override_extract()) {
            Some(h) => {
                let Some(ctx) = tel.ctx() else { return Ok(()) };
                let items = h.override_extract(&response, ctx).await?;
                let count = items.len();
                crate::scraper::extractor::ExtractionResult {
                    items,
                    selector_matches: count,
                }
            }
            None => {
                tel.stage(
                    "extract",
                    smol::unblock({
                        let runner = Arc::clone(self.runner);
                        let config = self.job.config.clone();
                        let dir = self.job.dir.clone();
                        move || runner.extract(&config, dir.as_deref(), &response)
                    }),
                )
                .await?
            }
        };

        if let (Some(h), Some(ctx)) = (tel.hooks(), tel.ctx()) {
            h.set_selector_matches(ctx, extraction.selector_matches)
                .await?;
        }

        let items = match tel.hooks() {
            Some(h) => {
                let Some(ctx) = tel.ctx() else { return Ok(()) };
                h.after_extract(extraction.items, ctx).await?
            }
            None => extraction.items,
        };

        if items.is_empty() {
            if status_code == 200 {
                if extraction.selector_matches == 0 {
                    if tel.hooks().is_some_and(|h| h.has_override_extract()) {
                        crate::t_println!(
                            "Job [{}]: override_extract returned 0 items",
                            color::c_job(&self.job.config.name),
                        );
                    } else {
                        crate::t_warnln!(
                            "Job [{}]: Selector '{}' matched 0 elements. The site may have changed or you have wrong selector",
                            color::c_job(&self.job.config.name),
                            self.job.config.selector
                        );
                    }
                } else {
                    crate::t_println!(
                        "Job [{}]: {} items found by selector, but none matched your keywords",
                        color::c_job(&self.job.config.name),
                        extraction.selector_matches
                    );
                }
            }
            return Ok(());
        }

        let token = tel.start().await;
        let items = match tel.hooks() {
            Some(h) if h.has_filter_item() => {
                let Some(ctx) = tel.ctx() else { return Ok(()) };
                let mut filtered = Vec::new();
                for item in items {
                    if let Some(mut kept) = h.filter_item(item, ctx).await? {
                        kept.matches = crate::scraper::extractor::matching_keywords(
                            &kept.fields,
                            self.job.config.options.keywords.as_deref(),
                            self.job.config.options.search_fields.as_deref(),
                        );
                        filtered.push(kept);
                    }
                }
                filtered
            }
            _ => self.runner.keyword_filter(&self.job.config, items),
        };
        let error = match (tel.hooks(), tel.ctx()) {
            (Some(h), Some(ctx)) => h.take_filter_error(ctx).await,
            _ => None,
        };
        let status = if error.is_some() { "error" } else { "success" };
        tel.record("filter", token, status, error).await;

        let items = match tel.hooks() {
            Some(h) => {
                let Some(ctx) = tel.ctx() else { return Ok(()) };
                match h.before_store(items, ctx).await? {
                    None => return Ok(()),
                    Some(i) => i,
                }
            }
            None => items,
        };

        // Late-result gate #1: a reload/timeout/shutdown that landed while
        // extraction was in flight must not persist anything. The durable
        // queue keeps the URL pending for the next cycle / restart.
        self.ensure_open()?;

        let new_items = tel
            .stage(
                "store",
                smol::unblock({
                    let db = Arc::clone(self.db);
                    let config = self.job.config.clone();
                    move || db.batch_check_and_insert(&config, items)
                }),
            )
            .await?;

        if new_items.is_empty() {
            if status_code == 200 {
                crate::t_println!(
                    "Job [{}] no new items found",
                    color::c_job(&self.job.config.name)
                );
            }

            return Ok(());
        }

        crate::t_println!(
            "Found {} for {}",
            color::c_ok(&format!("{} new items", new_items.len())),
            color::c_job(&self.job.config.name)
        );

        // Late-result gate #2: store and notify are separate awaits. A
        // cancellation landing in between must suppress the notification even
        // though the rows are already committed (dedupe makes a re-run a no-op
        // anyway, so no duplicate notification can ever be produced).
        self.ensure_open()?;

        let notify_items = match tel.hooks() {
            Some(h) => {
                let Some(ctx) = tel.ctx() else { return Ok(()) };
                h.before_notify(new_items.clone(), ctx).await?
            }
            None => Some(new_items.clone()),
        };

        if let Some(items) = notify_items {
            tel.stage(
                "notify",
                smol::unblock({
                    let config = self.job.config.clone();
                    let items = items.clone();
                    move || notifier::trigger_notification(&config, &items)
                }),
            )
            .await?;
        }

        let payload = webhook::build_default_payload(&self.job.config.name, &new_items);
        let payload = match tel.hooks() {
            Some(h) => {
                let Some(ctx) = tel.ctx() else { return Ok(()) };
                match h.before_webhook(payload, ctx).await? {
                    None => return Ok(()),
                    Some(p) => p,
                }
            }
            None => payload,
        };

        // Late-result gate #3: same protection for the outbound webhook.
        self.ensure_open()?;

        tel.stage(
            "webhook",
            webhook::trigger_webhook(&self.job.config, payload),
        )
        .await?;

        Ok(())
    }
}

// All scheduling decisions for one cycle, mode derived from batch presence (empty batch = zero work).
pub(crate) enum Work {
    Queue(Vec<String>),
    Multi,
    Inline(RequestConfig),
    Idle,
}

impl Work {
    pub(crate) async fn plan(job: &Job, worker_count: usize) -> Work {
        match compute_cycle_batch(job).await {
            Some(b) if !b.is_empty() => Work::Queue(b),
            Some(_) => Work::Idle,
            _ if worker_count > 1 => Work::Multi,
            None => Work::Inline(RequestConfig::from_job(&job.config)),
        }
    }
}

enum ErrStyle {
    Inline,
    Worker,
}

// The optional cycle token is threaded through so cooperative cancellation can
// gate side effects; it is the eighth argument of this per-URL driver.
#[allow(clippy::too_many_arguments)]
async fn run_recorded(
    job: &Job,
    db: &Arc<Db>,
    runner: &Arc<Runner>,
    request: RequestConfig,
    worker_id: usize,
    stats: &CycleStats,
    style: ErrStyle,
    token: Option<Arc<CycleToken>>,
) -> bool {
    let ctx = JobCtx::new(job, db, runner).with_token(token);
    match ctx.run_once(&request, worker_id).await {
        Ok(()) => {
            stats.record(true);
            true
        }
        Err(e) => {
            stats.record(false);
            if e.downcast_ref::<CycleCancelled>().is_some() {
                crate::t_println!(
                    "Job '{}' cycle cancelled; late result for {} discarded",
                    color::c_job(&job.config.name),
                    request.url
                );
            } else {
                match style {
                    ErrStyle::Inline => {
                        crate::t_eprintln!("Job '{}' error: {}", color::c_job(&job.config.name), e);
                    }
                    ErrStyle::Worker => {
                        crate::t_eprintln!("Job '{}' worker error: {}", job.config.name, e);
                    }
                }
            }
            false
        }
    }
}

// One cycle's dispatch mechanics: plan → workers → stats.
pub struct CycleRunner<'a> {
    job: &'a Arc<Job>,
    db: &'a Arc<Db>,
    runner: &'a Arc<Runner>,
    ex: &'a Arc<Executor<'static>>,
    worker_count: usize,
    thread_probe: ThreadProbe,
    control: Option<Arc<JobControl>>,
}

impl<'a> CycleRunner<'a> {
    pub fn new(
        job: &'a Arc<Job>,
        db: &'a Arc<Db>,
        runner: &'a Arc<Runner>,
        ex: &'a Arc<Executor<'static>>,
        worker_count: usize,
    ) -> Self {
        Self {
            job,
            db,
            runner,
            ex,
            worker_count,
            thread_probe: None,
            control: None,
        }
    }

    // Test seam: record which executor threads workers run on.
    #[cfg(test)]
    pub(crate) fn with_probe(mut self, probe: ThreadProbe) -> Self {
        self.thread_probe = probe;
        self
    }

    /// Install the per-job reload/cancellation control.
    pub fn with_control(mut self, control: Arc<JobControl>) -> Self {
        self.control = Some(control);
        self
    }

    /// Run exactly one job iteration: plan → dispatch → `on_finished`.
    pub async fn run_iteration(&self) -> Arc<CycleStats> {
        self.run_iteration_inner(None).await
    }

    /// Same as [`Self::run_iteration`], but cancels the cycle cooperatively
    /// once `timeout` elapses. Late side effects are refused; unfinished queue
    /// URLs remain durable and resume on the next cycle.
    pub async fn run_iteration_with_timeout(&self, timeout: Duration) -> Arc<CycleStats> {
        self.run_iteration_inner(Some(timeout)).await
    }

    async fn run_iteration_inner(&self, timeout: Option<Duration>) -> Arc<CycleStats> {
        // Every cycle starts with no active queue cycle — covers all modes
        // and makes a cancelled (dropped) previous cycle safe: its leftover
        // state is cleared before anything user-visible runs.
        if let Some(hooks) = self.job.hooks.as_ref() {
            hooks.queue.deactivate();
        }

        // Install this cycle's cancellation token before any work begins.
        let token = match &self.control {
            Some(control) => control.begin_cycle(),
            None => CycleToken::new(0),
        };

        let work = Work::plan(self.job, self.worker_count).await;
        let stats = Arc::new(CycleStats::new());

        let timed_out = match timeout {
            None => {
                self.dispatch_with_token(work, Arc::clone(&stats), Arc::clone(&token))
                    .await;
                false
            }
            Some(limit) => {
                let dispatch =
                    self.dispatch_with_token(work, Arc::clone(&stats), Arc::clone(&token));
                smol::future::or(
                    async {
                        dispatch.await;
                        false
                    },
                    async {
                        Timer::after(limit).await;
                        true
                    },
                )
                .await
            }
        };

        if timed_out {
            crate::t_warnln!(
                "Job [{}] cycle timeout ({:?}) reached; cancelling cooperatively",
                color::c_job(&self.job.config.name),
                timeout.unwrap()
            );
            token.cancel();
            if let Some(control) = &self.control {
                control.cancel_current();
            }
        }

        if let Some(hooks) = self.job.hooks.as_ref() {
            hooks.run_on_finished(&stats).await;
            let _ = hooks.lua.lock().await.gc_collect();
        }

        if let Some(control) = &self.control {
            control.end_cycle();
        }
        stats
    }

    // Total match on `Work`; stats always created here so `on_finished` fires (with zeros) on every path, including Idle.
    // Production cycles go through `dispatch_with_token`; this is the test seam.
    #[allow(dead_code)]
    pub(crate) async fn dispatch(&self, work: Work) -> Arc<CycleStats> {
        let token = match &self.control {
            Some(control) => control
                .current_token()
                .unwrap_or_else(|| CycleToken::new(0)),
            None => CycleToken::new(0),
        };
        let stats = Arc::new(CycleStats::new());
        self.dispatch_with_token(work, stats.clone(), token).await;
        stats
    }

    async fn dispatch_with_token(
        &self,
        work: Work,
        stats: Arc<CycleStats>,
        token: Arc<CycleToken>,
    ) {
        match work {
            Work::Idle => {}
            Work::Inline(request) => {
                run_recorded(
                    self.job,
                    self.db,
                    self.runner,
                    request,
                    1,
                    &stats,
                    ErrStyle::Inline,
                    Some(token),
                )
                .await;
            }
            Work::Queue(batch) => {
                let queue = self.job.hooks.as_ref().map(|h| Arc::clone(&h.queue));
                self.run_queue_cycle(batch, stats.clone(), queue, token)
                    .await;
            }
            Work::Multi => {
                self.spawn_multi_workers(&stats, token).await;
            }
        }
    }

    // Stagger → spawn → join for `Multi`: each worker runs the base request once.
    async fn spawn_multi_workers(&self, stats: &Arc<CycleStats>, token: Arc<CycleToken>) {
        let mut handles = Vec::with_capacity(self.worker_count);
        for i in 1..=self.worker_count {
            if i > 1 {
                Timer::after(Duration::from_millis(WORKER_STAGGER_MS)).await;
            }
            // Workers that have not started when the cycle is cancelled never do.
            if token.is_cancelled() {
                break;
            }
            let worker_id = i;
            let job = Arc::clone(self.job);
            let db = Arc::clone(self.db);
            let runner = Arc::clone(self.runner);
            let stats = Arc::clone(stats);
            let token = Arc::clone(&token);
            handles.push(spawn_worker(
                self.ex,
                async move {
                    let request = RequestConfig::from_job(&job.config);
                    run_recorded(
                        &job,
                        &db,
                        &runner,
                        request,
                        worker_id,
                        &stats,
                        ErrStyle::Worker,
                        Some(token),
                    )
                    .await;
                },
                &self.thread_probe,
            ));
        }

        for handle in handles {
            handle.await;
        }
    }

    // Queue-cycle engine: durable pending set, in-memory deque, visited set,
    // in-flight count, worker handles. Inline in the cycle future — dropping it
    // cancels workers; every URL is also persisted, so unfinished work resumes
    // on the next cycle or after a process restart.
    async fn run_queue_cycle(
        &self,
        batch: Vec<String>,
        stats: Arc<CycleStats>,
        queue: Option<Arc<QueueState>>,
        token: Arc<CycleToken>,
    ) {
        if self.worker_count == 0 {
            return;
        }

        let job_id = self.job.config.id();
        let durable = DurableQueue::new(Arc::clone(self.db), job_id.clone());

        // Recover URLs left pending by an earlier interrupted cycle/process,
        // then merge this cycle's seed. DB dedupe makes the union safe.
        let recovered = durable.reclaim();
        if !recovered.is_empty() {
            crate::t_println!(
                "Job [{}] resuming {} unfinished URL(s) from durable queue",
                color::c_job(&self.job.config.name),
                recovered.len()
            );
        }
        let mut combined: VecDeque<String> = recovered.into();
        for url in &batch {
            if !combined.iter().any(|u| u == url) {
                combined.push_back(url.clone());
            }
        }

        // Persist the whole batch before any worker starts.
        durable.enqueue(
            &combined.iter().cloned().collect::<Vec<_>>(),
            token.generation(),
        );

        let (event_tx, event_rx) = unbounded::<Message>();
        let mut pending: VecDeque<String> = combined;
        // Seed visited with the initial batch: re-pushing an initial URL is a
        // duplicate; duplicates *within* the initial batch keep processing
        // twice (today's behavior — the visited set gates pushes only).
        let mut visited: HashSet<String> = pending.iter().cloned().collect();
        if let Some(q) = &queue {
            q.begin_cycle(event_tx.clone(), visited.len(), Arc::clone(&token));
        }

        let mut replies: Vec<Sender<Reply>> = Vec::with_capacity(self.worker_count);
        let mut parked: VecDeque<usize> = VecDeque::new();
        let mut in_flight: usize = 0;
        let mut handles = Vec::with_capacity(self.worker_count);

        for i in 1..=self.worker_count {
            if i > 1 {
                Timer::after(Duration::from_millis(WORKER_STAGGER_MS)).await;
            }
            let (reply_tx, reply_rx) = unbounded::<Reply>();
            replies.push(reply_tx);
            handles.push(spawn_worker(
                self.ex,
                worker_loop(
                    Arc::clone(self.job),
                    Arc::clone(self.db),
                    Arc::clone(self.runner),
                    Arc::clone(&stats),
                    event_tx.clone(),
                    reply_rx,
                    i,
                    Arc::clone(&token),
                    Arc::clone(&durable),
                ),
                &self.thread_probe,
            ));
        }

        while let Ok(msg) = event_rx.recv().await {
            match msg {
                Message::RequestWork { worker_id } => {
                    // Never hand out new URLs once the cycle is cancelling.
                    if token.is_cancelled() {
                        parked.push_back(worker_id);
                        if in_flight == 0 {
                            break;
                        }
                        continue;
                    }
                    if let Some(url) = pending.pop_front() {
                        in_flight += 1;
                        let _ = replies[worker_id - 1].try_send(Reply::Work(url));
                    } else if in_flight == 0 {
                        // Quiescence: nothing pending, nothing running.
                        break;
                    } else {
                        // No work now, but requests are in flight — the
                        // worker waits; a Push will assign directly, or the
                        // last Completed re-checks quiescence.
                        parked.push_back(worker_id);
                    }
                }
                Message::Push { url } => {
                    if !visited.insert(url.clone()) {
                        stats.record_duplicate();
                        if let Some(q) = &queue {
                            q.release_reservation();
                        }
                        continue;
                    }
                    stats.record_pushed();
                    if token.is_cancelled() {
                        // Already durably enqueued by QueueState::push_url;
                        // leave it for the next cycle to pick up.
                        pending.push_back(url);
                        continue;
                    }
                    if let Some(worker_id) = parked.pop_front() {
                        in_flight += 1;
                        let _ = replies[worker_id - 1].try_send(Reply::Work(url));
                    } else {
                        pending.push_back(url);
                    }
                }
                Message::Completed | Message::Failed => {
                    in_flight -= 1;
                    if pending.is_empty() && in_flight == 0 {
                        break;
                    }
                }
            }
        }

        for tx in &replies {
            let _ = tx.try_send(Reply::Shutdown);
        }
        for handle in handles {
            handle.await;
        }
        if let Some(q) = &queue {
            q.deactivate();
        }
        // Anything still durably pending is intentionally retained: it is the
        // recovery set for the next cycle / a restart. No explicit clear here.
    }
}

// Queue-cycle→worker replies, one unbounded channel per worker.
enum Reply {
    Work(String),
    Shutdown,
}

#[allow(clippy::too_many_arguments)]
async fn worker_loop(
    job: Arc<Job>,
    db: Arc<Db>,
    runner: Arc<Runner>,
    stats: Arc<CycleStats>,
    event_tx: Sender<Message>,
    reply_rx: Receiver<Reply>,
    worker_id: usize,
    token: Arc<CycleToken>,
    durable: Arc<DurableQueue>,
) {
    loop {
        if event_tx
            .try_send(Message::RequestWork { worker_id })
            .is_err()
        {
            return;
        }
        match reply_rx.recv().await {
            Ok(Reply::Work(url)) => {
                let request = RequestConfig {
                    url: url.clone(),
                    ..RequestConfig::from_job(&job.config)
                };
                let ok = run_recorded(
                    &job,
                    &db,
                    &runner,
                    request,
                    worker_id,
                    &stats,
                    ErrStyle::Worker,
                    Some(Arc::clone(&token)),
                )
                .await;

                if ok {
                    // Only a fully completed URL leaves the durable queue.
                    // Cancellation errors keep the row for recovery.
                    durable.complete(&url);
                    if event_tx.try_send(Message::Completed).is_err() {
                        return;
                    }
                } else if token.is_cancelled() {
                    // Interrupted, not failed: the row stays pending for the
                    // next cycle/restart; tell the coordinator and exit.
                    let _ = event_tx.try_send(Message::Failed);
                    return;
                } else if event_tx.try_send(Message::Failed).is_err() {
                    return;
                }
            }
            Ok(Reply::Shutdown) | Err(_) => return,
        }
    }
}

pub async fn run_job_loop(
    job: Job,
    db: Arc<Db>,
    runner: Arc<Runner>,
    ex: &Arc<Executor<'static>>,
    control: Arc<JobControl>,
) {
    let interval = Duration::from_secs(job.config.options.interval as u64);
    let cycle_timeout = match job.config.options.cycle_timeout_secs {
        0 => None,
        secs => Some(Duration::from_secs(secs as u64)),
    };
    let worker_count = job.config.worker_count();
    let job = Arc::new(job);
    let cycle =
        CycleRunner::new(&job, &db, &runner, ex, worker_count).with_control(Arc::clone(&control));

    loop {
        crate::t_println!("Running job: {}", color::c_job(&job.config.name));
        let started = std::time::Instant::now();

        run_one_cycle(&cycle, &control, cycle_timeout).await;

        let pipeline_elapsed = started.elapsed();
        crate::t_println!(
            "Job [{}] finished in {}, sleeping for {}",
            color::c_job(&job.config.name),
            color::c_info(&crate::services::utils::format_duration(pipeline_elapsed)),
            color::c_info(&format!("{}s", job.config.options.interval))
        );

        // A reload observed during the cycle (or during sleep) ends this task
        // at the cycle boundary. The manager respawns the job from new config.
        if control.reload_requested() {
            control.acknowledge_reload();
            crate::t_println!(
                "Job [{}] handing over to reloaded config at cycle boundary",
                color::c_job(&job.config.name)
            );
            return;
        }

        if sleep_interruptible(interval, &control).await {
            control.acknowledge_reload();
            crate::t_println!(
                "Job [{}] reload requested during sleep; ending old task",
                color::c_job(&job.config.name)
            );
            return;
        }
    }
}

/// Run one cycle, enforcing the optional explicit timeout. On timeout the
/// cycle token is cancelled inside the iteration: workers unwind at their next
/// await boundary and every late side effect is refused.
async fn run_one_cycle(
    cycle: &CycleRunner<'_>,
    _control: &Arc<JobControl>,
    timeout: Option<Duration>,
) {
    match timeout {
        Some(limit) => {
            cycle.run_iteration_with_timeout(limit).await;
        }
        None => {
            cycle.run_iteration().await;
        }
    }
}

/// Wait up to `interval`, returning early (true) when a reload is requested.
async fn sleep_interruptible(interval: Duration, control: &Arc<JobControl>) -> bool {
    let step = Duration::from_millis(200);
    let mut remaining = interval;
    while remaining > Duration::ZERO {
        let chunk = remaining.min(step);
        Timer::after(chunk).await;
        if control.reload_requested() {
            return true;
        }
        remaining = remaining.saturating_sub(chunk);
    }
    false
}
