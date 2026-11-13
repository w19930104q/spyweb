use crate::config::types::MAX_BATCH;
use crate::scraper::control::CycleToken;
use crate::services::db::Db;
use smol::channel::Sender;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

// Worker→coordinator events. One shared unbounded stream: each send is an atomic append.
pub(crate) enum Message {
    RequestWork { worker_id: usize },
    Push { url: String },
    Completed,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PushError {
    Inactive,
    Full,
}

/// Durable backing for a queue cycle.
///
/// Every URL accepted by the cycle (seed batch and `queue_push` alike) is
/// upserted into the database before any worker can fetch it. A URL row is
/// deleted only when its worker finishes, so a cancellation, hot reload or
/// process restart leaves every unfinished URL recoverable via
/// [`Db::queue_reclaim`]. The in-memory coordinator on top stays the fast path
/// for scheduling; the database is the source of truth across restarts.
#[derive(Clone)]
pub(crate) struct DurableQueue {
    db: Arc<Db>,
    job_id: String,
}

impl DurableQueue {
    pub(crate) fn new(db: Arc<Db>, job_id: String) -> Arc<Self> {
        Arc::new(Self { db, job_id })
    }

    pub(crate) fn enqueue(&self, urls: &[String], generation: u64) {
        if let Err(e) = self.db.queue_enqueue(&self.job_id, urls, generation) {
            crate::t_eprintln!(
                "Job {} durable queue enqueue failed: {e}",
                crate::color::c_job(&self.job_id)
            );
        }
    }

    pub(crate) fn complete(&self, url: &str) {
        if let Err(e) = self.db.queue_complete(&self.job_id, url) {
            crate::t_eprintln!(
                "Job {} durable queue complete failed: {e}",
                crate::color::c_job(&self.job_id)
            );
        }
    }

    pub(crate) fn reclaim(&self) -> Vec<String> {
        self.db.queue_reclaim(&self.job_id).unwrap_or_default()
    }

    /// Drop every durable row for this job. Used by the `reset` CLI path and
    /// by tests; normal cycles never call it — unfinished rows are the
    /// recovery set and are removed one URL at a time via [`Self::complete`].
    #[allow(dead_code)]
    pub(crate) fn clear(&self) {
        if let Err(e) = self.db.queue_clear(&self.job_id) {
            crate::t_eprintln!(
                "Job {} durable queue clear failed: {e}",
                crate::color::c_job(&self.job_id)
            );
        }
    }
}

// Per-job queue-cycle state shared between the `queue_push` binding (sync,
// worker-side) and the coordinator (async, single consumer). The only
// worker-visible fields are the two atomics, everything else is owned by the
// coordinator task.
pub(crate) struct QueueState {
    slot: Mutex<Option<Sender<Message>>>,
    active: AtomicBool,
    reserved: AtomicUsize,
    durable: Option<Arc<DurableQueue>>,
    token: Mutex<Option<Arc<CycleToken>>>,
}

impl QueueState {
    /// Queue state without durable backing (tests, non-job Lua VMs).
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            slot: Mutex::new(None),
            active: AtomicBool::new(false),
            reserved: AtomicUsize::new(0),
            durable: None,
            token: Mutex::new(None),
        })
    }

    pub(crate) fn with_durable(durable: Arc<DurableQueue>) -> Arc<Self> {
        Arc::new(Self {
            slot: Mutex::new(None),
            active: AtomicBool::new(false),
            reserved: AtomicUsize::new(0),
            durable: Some(durable),
            token: Mutex::new(None),
        })
    }

    // Clear all cycle state. Called at every cycle start (`run_iteration`,
    // covers all modes) and when the coordinator ends — reset-at-start makes
    // cancellation (dropped cycle future) safe with no cleanup path.
    pub(crate) fn deactivate(&self) {
        self.active.store(false, Ordering::SeqCst);
        self.slot.lock().unwrap_or_else(|e| e.into_inner()).take();
        self.token.lock().unwrap_or_else(|e| e.into_inner()).take();
        self.reserved.store(0, Ordering::SeqCst);
    }

    pub(crate) fn begin_cycle(&self, tx: Sender<Message>, initial: usize, token: Arc<CycleToken>) {
        self.reserved.store(initial, Ordering::SeqCst);
        *self.slot.lock().unwrap_or_else(|e| e.into_inner()) = Some(tx);
        *self.token.lock().unwrap_or_else(|e| e.into_inner()) = Some(token);
        self.active.store(true, Ordering::SeqCst);
    }

    pub(crate) fn release_reservation(&self) {
        self.reserved.fetch_sub(1, Ordering::SeqCst);
    }

    // `queue_push` binding entry: sync gate + capacity reservation + durable
    // enqueue + send. Never blocks; all shared state touched here is atomic or
    // a short-lived mutex on a clone-only slot.
    pub(crate) fn push_url(&self, url: String) -> Result<(), PushError> {
        if !self.active.load(Ordering::SeqCst) {
            return Err(PushError::Inactive);
        }
        let prev = self.reserved.fetch_add(1, Ordering::SeqCst);
        if prev >= MAX_BATCH {
            self.reserved.fetch_sub(1, Ordering::SeqCst);
            return Err(PushError::Full);
        }

        // Durable enqueue first: once accepted, the URL survives even if the
        // in-memory send below races with a cycle cancellation.
        let generation = self
            .token
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|t| t.generation())
            .unwrap_or(0);
        if let Some(durable) = &self.durable {
            durable.enqueue(std::slice::from_ref(&url), generation);
        }

        let tx = self.slot.lock().unwrap_or_else(|e| e.into_inner()).clone();
        match tx {
            Some(tx) if tx.try_send(Message::Push { url }).is_ok() => Ok(()),
            _ => {
                self.reserved.fetch_sub(1, Ordering::SeqCst);
                Err(PushError::Inactive)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smol::channel::unbounded;

    #[test]
    fn push_url_rejected_outside_active_cycle() {
        let state = QueueState::new();
        assert_eq!(
            state.push_url("u".into()),
            Err(PushError::Inactive),
            "gate must reject before any cycle starts"
        );
    }

    #[test]
    fn push_url_rejected_after_deactivate() {
        let state = QueueState::new();
        let (tx, _rx) = unbounded::<Message>();
        let token = CycleToken::new(1);
        state.begin_cycle(tx, 1, token);
        assert_eq!(state.push_url("u".into()), Ok(()));
        state.deactivate();
        assert_eq!(state.push_url("u".into()), Err(PushError::Inactive));
    }

    #[test]
    fn push_url_rejects_over_cap_with_rollback() {
        let state = QueueState::new();
        let (tx, _rx) = unbounded::<Message>();
        let token = CycleToken::new(1);
        state.begin_cycle(tx, MAX_BATCH, token);
        assert_eq!(state.push_url("u".into()), Err(PushError::Full));
        // Reservation must roll back — a later accept must still fit.
        state.release_reservation();
        assert_eq!(state.push_url("u2".into()), Ok(()));
    }

    #[test]
    fn begin_cycle_resets_previous_state() {
        let state = QueueState::new();
        let (tx1, _rx1) = unbounded::<Message>();
        state.begin_cycle(tx1, 5, CycleToken::new(1));
        assert_eq!(state.push_url("u".into()), Ok(()));
        // Next cycle starts fresh: old pushes are gone, budget reset.
        let (tx2, rx2) = unbounded::<Message>();
        state.begin_cycle(tx2, 2, CycleToken::new(2));
        assert!(rx2.try_recv().is_err(), "old channel must be dropped");
        assert_eq!(state.reserved.load(Ordering::SeqCst), 2);
    }
}
