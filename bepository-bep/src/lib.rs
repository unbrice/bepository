// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

pub mod conflict;
pub mod connection;
pub mod device_id;
pub mod engine;
pub mod error;
pub mod events;
pub mod framing;
pub mod ids;
pub mod proto;
pub mod retry;
pub mod storage;

#[cfg(any(test, feature = "test-utils"))]
pub mod fault;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

// Re-export primary public types
#[cfg(any(test, feature = "test-utils"))]
pub use conflict::BackupResolver;
pub use conflict::{ConflictResolution, ConflictResolver, Ordering, compare};
pub use connection::{CloseReason, ConnectionHandle, ConnectionOptions};
pub use device_id::DeviceId;
pub use engine::BepEngine;
pub use error::{BepError, Result, StorageError};
pub use events::{EngineEvent, EventReceiver};
pub use framing::{CLIENT_NAME, CLIENT_VERSION, RawMessage};
pub use ids::{FolderId, FolderLabel};
pub use retry::{ExponentialBackoff, ImmediateRetry, NoRetry, RetryPolicy};
pub use storage::{Sequence, Storage, StorageFolder, UpdateResult};
