// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::future::Future;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::error::{BepError, StorageError};

/// Controls how storage failures are retried and how terminal errors are mapped.
///
/// The policy owns both decisions: which errors are retryable and how to convert
/// a terminal `StorageError` into a `BepError`. This allows test code to use
/// zero-delay retries while production code uses exponential backoff.
pub trait RetryPolicy: Send + Sync + 'static {
    /// Called after the Nth failure (0-indexed).
    ///
    /// Receives the actual error so the policy decides what is retryable.
    /// Returns the delay before the next attempt, or `None` to give up.
    fn next_delay(&self, attempt: u32, error: &StorageError) -> Option<Duration>;

    /// Maps a terminal `StorageError` (after all retries are exhausted, or for
    /// non-retryable errors) to a `BepError`.
    fn map_error(&self, error: StorageError) -> BepError {
        standard_map_error(error)
    }
}

fn standard_map_error(error: StorageError) -> BepError {
    match error {
        StorageError::TransientIo(msg) => BepError::TransientIo(msg),
        StorageError::Corruption(msg) => BepError::Corruption(msg),
        StorageError::Standby(msg) => BepError::Standby(msg),
        StorageError::NotFound(msg) => BepError::Corruption(format!("not found: {msg}")),
        StorageError::InvalidInput(msg) => {
            BepError::PeerBadMessage(format!("invalid input: {msg}"))
        }
        StorageError::Internal(msg) => BepError::Internal(msg),
    }
}

/// Exponential backoff — for production use.
///
/// Only retries [`StorageError::TransientIo`]. Fatal errors (`Corruption`,
/// `Internal`, `Standby`) return `None` immediately. Lock expiry is handled
/// naturally: when the lease expires the next storage call returns `Standby`,
/// which is non-retryable and closes the connection.
pub struct ExponentialBackoff {
    /// Delay before the first retry.
    pub base: Duration,
    /// Multiplicative factor applied on each attempt.
    pub multiplier: f64,
    /// Maximum per-attempt delay cap.
    pub max_delay: Duration,
    /// Give up after this many retries.
    pub max_attempts: u32,
}

impl Default for ExponentialBackoff {
    fn default() -> Self {
        Self {
            base: Duration::from_secs(1),
            multiplier: 2.0,
            max_delay: Duration::from_secs(60),
            max_attempts: 10,
        }
    }
}

impl RetryPolicy for ExponentialBackoff {
    fn next_delay(&self, attempt: u32, error: &StorageError) -> Option<Duration> {
        if !matches!(error, StorageError::TransientIo(_)) {
            return None;
        }
        if attempt >= self.max_attempts {
            return None;
        }
        let base_secs = self.base.as_secs_f64() * self.multiplier.powf(f64::from(attempt));
        let capped = base_secs.min(self.max_delay.as_secs_f64());
        Some(Duration::from_secs_f64(capped))
    }
}

/// Zero-retry policy — for unit tests where storage never returns `TransientIo`
/// naturally, or where any error should be immediately fatal.
pub struct NoRetry;

impl RetryPolicy for NoRetry {
    fn next_delay(&self, _attempt: u32, _error: &StorageError) -> Option<Duration> {
        None
    }
}

/// Zero-delay retry policy — for integration tests that inject faults.
///
/// Retries [`StorageError::TransientIo`] up to `max_attempts` times with no
/// sleep, so tests run fast without wall-clock sensitivity.
pub struct ImmediateRetry {
    pub max_attempts: u32,
}

impl RetryPolicy for ImmediateRetry {
    fn next_delay(&self, attempt: u32, error: &StorageError) -> Option<Duration> {
        if !matches!(error, StorageError::TransientIo(_)) {
            return None;
        }
        if attempt < self.max_attempts {
            Some(Duration::ZERO)
        } else {
            None
        }
    }
}

/// Execute a fallible storage operation with retry according to `policy`.
///
/// - On success: return the value.
/// - On error: call `policy.next_delay(attempt, &error)`.
///   - `Some(Duration::ZERO)`: yield to the executor and retry.
///   - `Some(delay > 0)`: sleep (cancellable via `shutdown`), then retry.
///   - `None`: return `policy.map_error(error)`.
/// - If `shutdown` fires during a sleep: return `BepError::TransientIo` immediately.
pub async fn retry_storage_op<F, Fut, T>(
    policy: &dyn RetryPolicy,
    shutdown: &CancellationToken,
    op: &str,
    f: F,
) -> crate::error::Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = std::result::Result<T, StorageError>> + Send,
{
    let mut attempt = 0u32;
    loop {
        match f().await {
            Ok(val) => return Ok(val),
            Err(err) => match policy.next_delay(attempt, &err) {
                Some(delay) => {
                    tracing::warn!(
                        op = %op,
                        attempt = attempt,
                        error = %err,
                        "transient storage error, retrying"
                    );
                    if delay.is_zero() {
                        tokio::task::yield_now().await;
                    } else {
                        tokio::select! {
                            _ = tokio::time::sleep(delay) => {}
                            _ = shutdown.cancelled() => {
                                return Err(BepError::TransientIo(
                                    "shutdown requested during retry".into(),
                                ));
                            }
                        }
                    }
                    attempt += 1;
                }
                None => {
                    if attempt > 0 {
                        tracing::warn!(
                            op = %op,
                            attempt = attempt,
                            error = %err,
                            "storage operation failed after retries"
                        );
                    }
                    return Err(policy.map_error(err));
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::error::{BepError, StorageError};

    // --- ExponentialBackoff::next_delay unit tests ---

    #[test]
    fn backoff_doubles_each_attempt() {
        let policy = ExponentialBackoff {
            base: Duration::from_secs(1),
            multiplier: 2.0,
            max_delay: Duration::from_secs(3600),
            max_attempts: 10,
        };
        let err = StorageError::TransientIo("x".into());
        let d0 = policy.next_delay(0, &err).unwrap();
        let d1 = policy.next_delay(1, &err).unwrap();
        let d2 = policy.next_delay(2, &err).unwrap();
        assert_eq!(d0, Duration::from_secs(1));
        assert_eq!(d1, Duration::from_secs(2));
        assert_eq!(d2, Duration::from_secs(4));
    }

    #[test]
    fn backoff_caps_at_max_delay() {
        let policy = ExponentialBackoff {
            base: Duration::from_secs(1),
            multiplier: 2.0,
            max_delay: Duration::from_secs(5),
            max_attempts: 10,
        };
        let err = StorageError::TransientIo("x".into());
        // attempt 3 would be 8s without cap
        let d = policy.next_delay(3, &err).unwrap();
        assert_eq!(d, Duration::from_secs(5));
    }

    #[test]
    fn backoff_stops_at_max_attempts() {
        let policy = ExponentialBackoff {
            max_attempts: 3,
            ..ExponentialBackoff::default()
        };
        let err = StorageError::TransientIo("x".into());
        assert!(policy.next_delay(2, &err).is_some());
        assert!(policy.next_delay(3, &err).is_none());
    }

    #[test]
    fn backoff_does_not_retry_non_transient() {
        let policy = ExponentialBackoff::default();
        for err in [
            StorageError::Corruption("c".into()),
            StorageError::Standby("nm".into()),
            StorageError::NotFound("nf".into()),
            StorageError::InvalidInput("ii".into()),
            StorageError::Internal("i".into()),
        ] {
            assert!(
                policy.next_delay(0, &err).is_none(),
                "should not retry {err:?}"
            );
        }
    }

    // --- standard_map_error (via NoRetry.map_error) ---

    #[test]
    fn error_mapping_transient_io() {
        assert!(matches!(
            NoRetry.map_error(StorageError::TransientIo("x".into())),
            BepError::TransientIo(_)
        ));
    }

    #[test]
    fn error_mapping_corruption() {
        assert!(matches!(
            NoRetry.map_error(StorageError::Corruption("x".into())),
            BepError::Corruption(_)
        ));
    }

    #[test]
    fn error_mapping_standby() {
        assert!(matches!(
            NoRetry.map_error(StorageError::Standby("x".into())),
            BepError::Standby(_)
        ));
    }

    #[test]
    fn error_mapping_not_found_becomes_corruption() {
        assert!(matches!(
            NoRetry.map_error(StorageError::NotFound("x".into())),
            BepError::Corruption(_)
        ));
    }

    #[test]
    fn error_mapping_invalid_input_becomes_peer_bad_message() {
        assert!(matches!(
            NoRetry.map_error(StorageError::InvalidInput("x".into())),
            BepError::PeerBadMessage(_)
        ));
    }

    #[test]
    fn error_mapping_internal() {
        assert!(matches!(
            NoRetry.map_error(StorageError::Internal("x".into())),
            BepError::Internal(_)
        ));
    }

    // --- retry_storage_op behavioural tests ---

    #[tokio::test]
    async fn success_on_first_attempt() {
        let shutdown = CancellationToken::new();
        let result = retry_storage_op(&NoRetry, &shutdown, "op", || async {
            Ok::<u32, StorageError>(42)
        })
        .await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn non_retryable_error_fails_immediately() {
        let shutdown = CancellationToken::new();
        let calls = Arc::new(AtomicU32::new(0));
        let calls2 = calls.clone();
        let result = retry_storage_op(&NoRetry, &shutdown, "op", || {
            let c = calls2.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(StorageError::Corruption("bad".into()))
            }
        })
        .await;
        assert!(matches!(result, Err(BepError::Corruption(_))));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "should not retry non-transient errors"
        );
    }

    #[tokio::test]
    async fn transient_error_retried_until_success() {
        let policy = ImmediateRetry { max_attempts: 5 };
        let shutdown = CancellationToken::new();
        let calls = Arc::new(AtomicU32::new(0));
        let calls2 = calls.clone();
        let result = retry_storage_op(&policy, &shutdown, "op", || {
            let c = calls2.clone();
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst);
                if n < 3 {
                    Err(StorageError::TransientIo("blip".into()))
                } else {
                    Ok("done")
                }
            }
        })
        .await;
        assert_eq!(result.unwrap(), "done");
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn transient_error_gives_up_after_max_attempts() {
        let policy = ImmediateRetry { max_attempts: 3 };
        let shutdown = CancellationToken::new();
        let calls = Arc::new(AtomicU32::new(0));
        let calls2 = calls.clone();
        let result = retry_storage_op(&policy, &shutdown, "op", || {
            let c = calls2.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(StorageError::TransientIo("always fails".into()))
            }
        })
        .await;
        assert!(matches!(result, Err(BepError::TransientIo(_))));
        // 1 initial attempt + 3 retries
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn shutdown_interrupts_sleep() {
        // Pre-cancel the token so the sleep select! resolves immediately via
        // the cancelled() branch rather than waiting out the long delay.
        let shutdown = CancellationToken::new();
        shutdown.cancel();

        let policy = ExponentialBackoff {
            base: Duration::from_secs(3600),
            multiplier: 1.0,
            max_delay: Duration::from_secs(3600),
            max_attempts: 10,
        };
        let result = retry_storage_op(&policy, &shutdown, "op", || async {
            Err::<(), _>(StorageError::TransientIo("fail".into()))
        })
        .await;
        assert!(
            matches!(&result, Err(BepError::TransientIo(msg)) if msg.contains("shutdown")),
            "expected shutdown error, got {result:?}"
        );
    }

    #[tokio::test]
    async fn immediate_retry_does_not_retry_non_transient() {
        let policy = ImmediateRetry { max_attempts: 10 };
        let shutdown = CancellationToken::new();
        let calls = Arc::new(AtomicU32::new(0));
        let calls2 = calls.clone();
        let result = retry_storage_op(&policy, &shutdown, "op", || {
            let c = calls2.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(StorageError::Standby("lost lock".into()))
            }
        })
        .await;
        assert!(matches!(result, Err(BepError::Standby(_))));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
