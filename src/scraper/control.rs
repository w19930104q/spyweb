use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// Cooperative cancellation boundary for one queue/worker cycle.
///
/// A token starts *open*; [`CycleToken::cancel`] closes it exactly once. Every
/// side-effecting step (store, notification, webhook, Lua `notify`) checks
/// [`CycleToken::is_open`] immediately before running, so results that arrive
/// after a reload, an explicit cycle timeout or a process shutdown never reach
/// the database or produce a notification.
#[derive(Debug)]
pub struct CycleToken {
    cancelled: AtomicU64,
    generation: u64,
}

impl CycleToken {
    pub fn new(generation: u64) -> Arc<Self> {
        Arc::new(Self {
            cancelled: AtomicU64::new(0),
            generation,
        })
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn cancel(&self) {
        self.cancelled.store(1, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst) != 0
    }

    pub fn is_open(&self) -> bool {
        !self.is_cancelled()
    }
}

/// Per-job control surface shared by the job loop, the reload manager and the
/// Lua bindings. It holds the token of the *currently running* cycle plus a
/// reload epoch consulted between cycles.
///
/// One control lives for the whole process lifetime of a job id: hot reload
/// rebuilds the Lua VM and the config, but the control stays in place, which is
/// what lets a reload signal reach the in-flight, old-config cycle.
#[derive(Debug)]
pub struct JobControl {
    generation: AtomicU64,
    /// Monotonic reload epoch. `request_reload` bumps it; the job loop records
    /// the epoch it last observed, so a reload landing between cycles (while no
    /// token is installed) is never silently consumed by `begin_cycle`.
    reload_epoch: AtomicU64,
    observed_epoch: AtomicU64,
    current: RwLock<Option<Arc<CycleToken>>>,
}

impl JobControl {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            generation: AtomicU64::new(0),
            reload_epoch: AtomicU64::new(0),
            observed_epoch: AtomicU64::new(0),
            current: RwLock::new(None),
        })
    }

    /// Begin a new cycle: bumps the generation and installs a fresh, open
    /// token. The reload epoch is intentionally left untouched — a reload
    /// observed while no cycle is running must still be visible afterwards.
    pub fn begin_cycle(&self) -> Arc<CycleToken> {
        let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
        let token = CycleToken::new(generation);
        *self.current.write().unwrap_or_else(|e| e.into_inner()) = Some(Arc::clone(&token));
        token
    }

    /// Remove the active token when a cycle ends. Late work then sees no open
    /// token and must refuse side effects.
    pub fn end_cycle(&self) {
        *self.current.write().unwrap_or_else(|e| e.into_inner()) = None;
    }

    /// The active cycle's token, if a cycle is currently running.
    pub fn current_token(&self) -> Option<Arc<CycleToken>> {
        self.current
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// True while a cycle is installed and has not yet been cancelled. Side
    /// effects from Lua/HTTP callbacks use this as their gate.
    pub fn side_effects_open(&self) -> bool {
        self.current_token().is_some_and(|token| token.is_open())
    }

    /// Signal that new config was observed. The running cycle keeps its old
    /// config; the job loop picks the new config up at the next cycle boundary.
    pub fn request_reload(&self) {
        self.reload_epoch.fetch_add(1, Ordering::SeqCst);
    }

    /// True when a reload was requested and not yet acknowledged by this job
    /// loop. Edge-safe across the between-cycles window: the epoch only moves
    /// forward, and acknowledgement is the loop's own record.
    pub fn reload_requested(&self) -> bool {
        self.reload_epoch.load(Ordering::SeqCst) != self.observed_epoch.load(Ordering::SeqCst)
    }

    /// Acknowledge the latest reload. The job loop calls this when it is about
    /// to hand over to the manager, so an acknowledgement can never consume a
    /// reload that arrived after the loop's last check.
    pub fn acknowledge_reload(&self) {
        self.observed_epoch
            .store(self.reload_epoch.load(Ordering::SeqCst), Ordering::SeqCst);
    }

    /// Reset the loop's reload view to the current epoch. The manager calls
    /// this when it (re)spawns a job task, so a fresh task never inherits a
    /// reload meant for the previous generation of the task.
    pub fn rearm(&self) {
        self.acknowledge_reload();
    }

    /// Force-close the running cycle (explicit timeout / shutdown / forced
    /// reload drain). No-op between cycles.
    pub fn cancel_current(&self) {
        if let Some(token) = self.current_token() {
            token.cancel();
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }
}

impl Default for JobControl {
    fn default() -> Self {
        // RwLock/atomics are cheap; Arc-wrapping happens in `new`.
        Self {
            generation: AtomicU64::new(0),
            reload_epoch: AtomicU64::new(0),
            observed_epoch: AtomicU64::new(0),
            current: RwLock::new(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_starts_open_and_cancel_is_sticky() {
        let token = CycleToken::new(1);
        assert!(token.is_open());
        assert_eq!(token.generation(), 1);
        token.cancel();
        assert!(token.is_cancelled());
        // A second cancel must not reopen anything.
        token.cancel();
        assert!(token.is_cancelled());
    }

    #[test]
    fn control_cycles_get_fresh_tokens() {
        let control = JobControl::new();
        let first = control.begin_cycle();
        assert_eq!(first.generation(), 1);
        assert!(control.side_effects_open());

        first.cancel();
        assert!(!control.side_effects_open());

        let second = control.begin_cycle();
        assert_eq!(second.generation(), 2);
        assert!(second.is_open());
        assert!(control.side_effects_open());
        assert_ne!(Arc::as_ptr(&first), Arc::as_ptr(&second));

        control.end_cycle();
        assert!(!control.side_effects_open());
    }

    #[test]
    fn reload_epoch_survives_cycle_boundaries_until_acknowledged() {
        let control = JobControl::new();
        control.begin_cycle();
        assert!(!control.reload_requested());
        control.request_reload();
        assert!(control.reload_requested());
        // Same cycle keeps going under old config.
        assert!(control.side_effects_open());
        // Beginning a new cycle must NOT consume a reload that landed while
        // no loop was paying attention.
        control.begin_cycle();
        assert!(control.reload_requested());
        // Only the loop's explicit acknowledgement clears it.
        control.acknowledge_reload();
        assert!(!control.reload_requested());
        // A second reload bumps the epoch again.
        control.request_reload();
        assert!(control.reload_requested());
        // A fresh task is re-armed to the current epoch.
        control.rearm();
        assert!(!control.reload_requested());
    }

    #[test]
    fn cancel_current_only_affects_running_cycle() {
        let control = JobControl::new();
        control.cancel_current(); // no cycle: must be a no-op
        let token = control.begin_cycle();
        control.cancel_current();
        assert!(token.is_cancelled());
    }
}
