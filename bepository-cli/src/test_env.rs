// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Shared guard for tests that mutate process env: concurrent `set_var` /
//! `env::vars` from parallel test threads is UB (Rust 2024), so every
//! env-mutating test in this binary serializes on this one mutex.

use std::sync::LazyLock;

static GUARD: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(|| tokio::sync::Mutex::new(()));

/// Holds the mutex and restores the given vars to their pre-lock values on
/// Drop: a failing assert must not leak mutations into other tests. Vars are
/// cleared on acquire so each test starts from a clean slate.
pub(crate) struct EnvGuard {
    _guard: tokio::sync::MutexGuard<'static, ()>,
    saved: Vec<(&'static str, Option<String>)>,
}

impl EnvGuard {
    /// For sync tests (panics inside an async context — use `lock_async`).
    pub(crate) fn lock(vars: &[&'static str]) -> Self {
        Self::acquire(GUARD.blocking_lock(), vars)
    }

    /// For async tests; the guard is held across `.await`s.
    #[cfg_attr(not(feature = "self-manage"), allow(dead_code))]
    pub(crate) async fn lock_async(vars: &[&'static str]) -> Self {
        Self::acquire(GUARD.lock().await, vars)
    }

    fn acquire(guard: tokio::sync::MutexGuard<'static, ()>, vars: &[&'static str]) -> Self {
        let saved = vars.iter().map(|k| (*k, std::env::var(k).ok())).collect();
        // Safety: serialized by the held mutex; restored on Drop.
        unsafe {
            for k in vars {
                std::env::remove_var(k);
            }
        }
        Self {
            _guard: guard,
            saved,
        }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        // Safety: still serialized by the held mutex.
        unsafe {
            for (k, orig) in &self.saved {
                match orig {
                    Some(v) => std::env::set_var(k, v),
                    None => std::env::remove_var(k),
                }
            }
        }
    }
}
