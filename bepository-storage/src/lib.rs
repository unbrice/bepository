// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

mod api;
mod compaction;
#[cfg(any(test, feature = "test-utils"))]
pub mod fault;
pub mod fsck;
pub mod meta;
pub mod proto;
pub mod snapshot;
mod snapshot_fs;
mod stats;
mod store;
pub mod store_keys;

pub use api::{CacheProvider, FolderStorageKey, FsckEvent, SlateFolder, SlateStorage};
pub use bepository_bep::ids::{FolderId, FolderLabel, FolderLabelRef};
pub use bepository_lock::Epoch;
pub use fsck::FsckLevel;
pub use meta::CheckpointSchedule;
pub use snapshot::{FsEntry, SnapshotError, SnapshotFs, SnapshotRef};
pub use snapshot_fs::SlateSnapshotRef;

#[cfg(any(test, feature = "test-utils"))]
pub use store::SeqAllocator;
