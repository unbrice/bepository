// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use object_store::path::Path;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::{AcquisitionStatus, Epoch, Lock};

/// Why the lock was lost.
#[derive(Debug)]
pub enum LockLost {
    /// Ownership lost: preempted by a higher-priority writer, or another
    /// writer took ownership during renewal.
    Preempted,
    /// Renewal failed and 2/3 of the lease elapsed since last success.
    Expired {
        elapsed_secs: u64,
        threshold_secs: u64,
    },
    /// Clock regression detected during renewal.
    ClockRegression,
    /// Shutdown requested via cancellation token.
    Cancelled,
}

impl std::fmt::Display for LockLost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockLost::Preempted => write!(f, "ownership lost"),
            LockLost::Expired {
                elapsed_secs,
                threshold_secs,
            } => {
                write!(
                    f,
                    "lease expired: {elapsed_secs}s since last renewal (threshold {threshold_secs}s)"
                )
            }
            LockLost::ClockRegression => write!(f, "clock regression detected"),
            LockLost::Cancelled => write!(f, "cancelled"),
        }
    }
}

/// A held lock that auto-renews in the background.
///
/// Created via [`Lock::hold`]. The renewal task runs until the lock is lost,
/// cancelled, or [`LockGuard::release`] is called.
pub struct LockGuard {
    epoch: Epoch,
    lost_rx: oneshot::Receiver<LockLost>,
    /// Dedicated token for the renewal task. Cancelling this does NOT
    /// cancel the parent (Ctrl+C) token — it only stops the renewal loop.
    renew_cancel: CancellationToken,
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    holder: String,
    priority: u32,
    duration: u64,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        // Ensure the renewal task is stopped even if release() was never called
        // (e.g. early return with `?` between hold() and release()).
        self.renew_cancel.cancel();
    }
}

impl LockGuard {
    /// The epoch of the acquired lock.
    #[must_use]
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Returns when the lock is lost. Resolves with the reason.
    pub async fn lost(mut self) -> LockLost {
        (&mut self.lost_rx).await.unwrap_or(LockLost::Cancelled)
    }

    /// A future that resolves when the lock is lost.
    ///
    /// Unlike [`lost`], this borrows the guard so you can use it in `select!`.
    pub async fn lost_ref(&mut self) -> LockLost {
        (&mut self.lost_rx).await.unwrap_or(LockLost::Cancelled)
    }

    /// Cancels the renewal task, then best-effort releases the lock.
    ///
    /// Always attempts to clean up epoch files, even if the lock was
    /// already lost (the file may still exist after expiry or `Queued`).
    ///
    /// Returns `Ok(())` if the lock was still held, or `Err(LockLost)` if
    /// the lock was already lost before release.
    pub async fn release(mut self) -> Result<(), LockLost> {
        self.renew_cancel.cancel();

        // Always try to clean up our epoch file(s).
        let lock = Lock::new(
            &*self.store,
            self.prefix.clone(),
            self.holder.clone(),
            self.priority,
            self.duration,
        );
        if let Err(e) = lock.release().await {
            tracing::warn!("failed to release lock: {:?}", e);
        }

        // Report whether the lock was already lost.
        match self.lost_rx.try_recv() {
            Ok(reason) => {
                tracing::warn!(epoch = %self.epoch, %reason, "lock lost before release");
                Err(reason)
            }
            Err(oneshot::error::TryRecvError::Closed) => {
                tracing::info!(epoch = %self.epoch, "lock cancelled before release");
                Err(LockLost::Cancelled)
            }
            Err(oneshot::error::TryRecvError::Empty) => {
                tracing::info!(epoch = %self.epoch, "lock released cleanly");
                Ok(())
            }
        }
    }
}

/// Extension methods on [`Lock`] for long-lived lock holding.
impl<'a, T: ObjectStore + ?Sized + 'static> Lock<'a, T> {
    /// Acquire the lock and start a background renewal task.
    ///
    /// Blocks until the lock is acquired. Retries on transient failures.
    /// Returns `Err` on fatal errors (`QueuedBackwardClock`, store errors).
    ///
    /// The `cancel` token aborts acquisition and the renewal task.
    #[tracing::instrument(level = "info", skip(self, store, cancel), fields(holder = %self.holder, priority = self.priority))]
    pub async fn hold(
        &self,
        store: Arc<dyn ObjectStore>,
        cancel: &CancellationToken,
    ) -> Result<LockGuard, crate::Error> {
        // Acquisition loop
        let epoch = loop {
            if cancel.is_cancelled() {
                return Err(crate::Error::Cancelled);
            }
            let status = tokio::select! {
                res = self.acquire() => res?,
                _ = cancel.cancelled() => return Err(crate::Error::Cancelled),
            };
            match status {
                AcquisitionStatus::Owner(epoch) => break epoch,
                AcquisitionStatus::QueuedBackwardClock => {
                    return Err(crate::Error::ClockRegression);
                }
                AcquisitionStatus::Queued => {
                    tracing::info!("Queued — waiting for current owner's lease to expire");
                }
                AcquisitionStatus::Yielded => {
                    tracing::info!("Yielded to higher priority, retrying");
                }
                AcquisitionStatus::Failed | AcquisitionStatus::NotOwner => {
                    tracing::info!("Lock held by another device, waiting");
                }
            }
            let wait = Duration::from_secs((self.duration / 3).max(1));
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = cancel.cancelled() => return Err(crate::Error::Cancelled),
            }
        };

        tracing::info!(epoch = %epoch, "lock acquired");

        let (lost_tx, lost_rx) = oneshot::channel();
        // Child token: cancelled when parent (Ctrl+C) is cancelled, but
        // cancelling it does NOT cancel the parent.
        let renew_cancel = cancel.child_token();

        let renew_store = store.clone();
        let prefix = self.prefix.clone();
        let holder = self.holder.clone();
        let priority = self.priority;
        let duration = self.duration;
        let task_cancel = renew_cancel.clone();
        let acquired_at = std::time::Instant::now();

        use tracing::Instrument;
        tokio::spawn(
            async move {
                let reason = renewal_loop(
                    &renew_store,
                    &prefix,
                    &holder,
                    priority,
                    duration,
                    &task_cancel,
                    acquired_at,
                )
                .await;
                let _ = lost_tx.send(reason);
            }
            .in_current_span(),
        );

        Ok(LockGuard {
            epoch,
            lost_rx,
            renew_cancel,
            store,
            prefix: self.prefix.clone(),
            holder: self.holder.clone(),
            priority: self.priority,
            duration: self.duration,
        })
    }
}

/// Sleep `duration` using systime (CLOCK_BOOTTIME on Linux) if available,
/// falling back to tokio. Returns whether systime succeeded (caller should
/// keep using it on `true`, fall back on `false`).
async fn cancellable_sleep(
    duration: Duration,
    use_systime: bool,
    cancel: &CancellationToken,
) -> Result<bool, LockLost> {
    if use_systime {
        let clock = systime::tokio::ClockType::TrackSleep;
        if let Ok(s) = clock.sleep(duration) {
            tokio::select! {
                res = s => return Ok(res.is_ok()),
                _ = cancel.cancelled() => return Err(LockLost::Cancelled),
            }
        }
    }
    tokio::select! {
        _ = tokio::time::sleep(duration) => Ok(false),
        _ = cancel.cancelled() => Err(LockLost::Cancelled),
    }
}

async fn renewal_loop(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    holder: &str,
    priority: u32,
    lease: u64,
    cancel: &CancellationToken,
    acquired_at: std::time::Instant,
) -> LockLost {
    let renew_interval = Duration::from_secs(lease / 3);
    let retry_interval = Duration::from_secs(15);
    let give_up_after = lease * 2 / 3;
    let mut last_success = acquired_at;

    // Sleep lease/3 before the first renewal attempt.
    let mut use_systime = match cancellable_sleep(renew_interval, true, cancel).await {
        Ok(ok) => ok,
        Err(lost) => return lost,
    };

    loop {
        let elapsed = last_success.elapsed().as_secs();
        if elapsed >= give_up_after {
            return LockLost::Expired {
                elapsed_secs: elapsed,
                threshold_secs: give_up_after,
            };
        }

        let lock = Lock::new(
            &**store,
            prefix.clone(),
            holder.to_string(),
            priority,
            lease,
        );
        let sleep_dur = match lock.acquire().await {
            Ok(AcquisitionStatus::Owner(_)) => {
                last_success = std::time::Instant::now();
                renew_interval
            }
            Ok(AcquisitionStatus::Yielded) => return LockLost::Preempted,
            Ok(AcquisitionStatus::QueuedBackwardClock) => return LockLost::ClockRegression,
            Ok(_) => return LockLost::Preempted,
            Err(e) => {
                tracing::warn!("lock renewal failed: {:?}, retrying in 15s", e);
                retry_interval
            }
        };

        match cancellable_sleep(sleep_dur, use_systime, cancel).await {
            Ok(ok) => {
                use_systime = ok;
            }
            Err(lost) => return lost,
        }
    }
}
