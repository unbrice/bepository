// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

use linear_map::LinearMap;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, Stream};
use parking_lot::RwLock;

use crate::conflict;
use crate::device_id::DeviceId;
use crate::error::StorageError;
use crate::ids::{FolderId, FolderLabel, FolderLabelRef};
use crate::proto::bep::{BlockInfo, Counter, FileInfo, Vector};
use crate::storage::{
    Sequence, Storage, StorageFolder, StorageInspectorForTests, UpdateResult, blocks_equal,
};

pub const LOCAL_DEV: DeviceId = DeviceId([1; 32]);
pub const REMOTE_DEV: DeviceId = DeviceId([0xFF; 32]);

pub use crate::conflict::{BackupResolver, resolve_conflict};

/// Create a [`DeviceId`] by repeating a single byte.
#[must_use]
pub fn make_device(byte: u8) -> DeviceId {
    DeviceId::from_bytes([byte; 32])
}

/// Build a [`FileInfo`] with a version vector and optional `deleted` flag.
#[must_use]
pub fn make_file(name: &str, counters: &[(u64, u64)], deleted: bool) -> FileInfo {
    FileInfo {
        name: name.into(),
        version: Some(Vector {
            counters: counters
                .iter()
                .map(|&(id, value)| Counter { id, value })
                .collect(),
        }),
        deleted,
        ..Default::default()
    }
}

/// Block size used by [`make_file_with_blocks`].
pub const TEST_BLOCK_SIZE: i32 = 1024;

/// Build a [`FileInfo`] with blocks derived from hash values.
///
/// Offsets and sizes are computed automatically: each block is
/// [`TEST_BLOCK_SIZE`] bytes at offset `index * TEST_BLOCK_SIZE`.
#[must_use]
pub fn make_file_with_blocks(
    name: &str,
    counters: &[(u64, u64)],
    block_hashes: &[[u8; 32]],
) -> FileInfo {
    FileInfo {
        name: name.into(),
        version: Some(Vector {
            counters: counters
                .iter()
                .map(|&(id, value)| Counter { id, value })
                .collect(),
        }),
        blocks: block_hashes
            .iter()
            .enumerate()
            .map(|(i, hash)| BlockInfo {
                offset: (i64::try_from(i).expect("block index overflow"))
                    * (i64::from(TEST_BLOCK_SIZE)),
                size: TEST_BLOCK_SIZE,
                hash: hash.to_vec(),
            })
            .collect(),
        block_size: TEST_BLOCK_SIZE,
        size: (i64::try_from(block_hashes.len()).expect("block count overflow"))
            * (i64::from(TEST_BLOCK_SIZE)),
        ..Default::default()
    }
}

/// In-memory Storage for testing protocol logic without I/O.
///
/// Cloning shares the same underlying state (all fields are `Arc`-wrapped).
#[derive(Clone)]
pub struct MemoryStorage {
    /// (folder, path) → FileInfo — committed index
    index: Arc<RwLock<HashMap<(FolderId, String), FileInfo>>>,
    /// (folder, path) → FileInfo — inbox (staged, awaiting block completion)
    inbox: Arc<RwLock<HashMap<(FolderId, String), FileInfo>>>,
    /// block hash → block data (content-addressed, like SlateStorage)
    blocks: Arc<RwLock<HashMap<[u8; 32], Bytes>>>,
    /// folder → sequence counter
    sequences: Arc<RwLock<LinearMap<FolderId, i64>>>,
    /// (folder, device) → last known remote sequence
    remote_states: Arc<RwLock<HashMap<(FolderId, String), crate::storage::RemoteIndexState>>>,
    /// folder → label
    labels: Arc<RwLock<LinearMap<FolderId, FolderLabel>>>,
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStorage {
    #[must_use]
    pub fn new() -> Self {
        Self {
            index: Arc::new(RwLock::new(HashMap::new())),
            inbox: Arc::new(RwLock::new(HashMap::new())),
            blocks: Arc::new(RwLock::new(HashMap::new())),
            sequences: Arc::new(RwLock::new(LinearMap::new())),
            remote_states: Arc::new(RwLock::new(HashMap::new())),
            labels: Arc::new(RwLock::new(LinearMap::new())),
        }
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    type Folder = MemoryFolder;
    type StorageKey = FolderId;

    async fn folder(&self, id: FolderId) -> Result<MemoryFolder, StorageError> {
        let label = self
            .labels
            .read()
            .get(&id)
            .cloned()
            .unwrap_or_else(|| FolderLabel::from(format!("LabelFor{id}")));
        Ok(MemoryFolder {
            folder: id,
            label,
            index: self.index.clone(),
            inbox: self.inbox.clone(),
            blocks: self.blocks.clone(),
            sequences: self.sequences.clone(),
            remote_states: self.remote_states.clone(),
        })
    }

    async fn list_folders(&self) -> Result<Vec<(FolderId, FolderLabel, FolderId)>, StorageError> {
        let labels = self.labels.read();
        let mut result: Vec<(FolderId, FolderLabel, FolderId)> = labels
            .iter()
            .map(|(id, label)| (*id, label.clone(), *id))
            .collect();
        result.sort_by_key(|id_label| id_label.0);
        Ok(result)
    }

    async fn ensure_folders(
        &self,
        folders: &[(FolderId, FolderLabel)],
    ) -> Result<Vec<bool>, StorageError> {
        let mut labels = self.labels.write();
        let mut created = Vec::with_capacity(folders.len());
        for (id, label) in folders {
            let is_new = !labels.contains_key(id);
            labels.insert(*id, label.clone());
            created.push(is_new);
        }
        Ok(created)
    }
}

/// Per-folder handle for [`MemoryStorage`].
///
/// Cheaply cloneable — shares the same underlying `Arc` state as the backend.
#[derive(Clone)]
pub struct MemoryFolder {
    folder: FolderId,
    label: FolderLabel,
    index: Arc<RwLock<HashMap<(FolderId, String), FileInfo>>>,
    inbox: Arc<RwLock<HashMap<(FolderId, String), FileInfo>>>,
    blocks: Arc<RwLock<HashMap<[u8; 32], Bytes>>>,
    sequences: Arc<RwLock<LinearMap<FolderId, i64>>>,
    remote_states: Arc<RwLock<HashMap<(FolderId, String), crate::storage::RemoteIndexState>>>,
}

#[async_trait]
impl StorageInspectorForTests for MemoryFolder {
    type Epoch = ();

    async fn insert_file(&self, file: FileInfo) {
        let mut idx = self.index.write();
        let mut seqs = self.sequences.write();
        let seq = seqs.entry(self.folder).or_insert(0);
        *seq += 1;
        let mut file = file;
        file.sequence = *seq;
        idx.insert((self.folder, file.name.clone()), file);
    }

    async fn insert_block(&self, name: &str, offset: i64, data: Bytes) {
        let hash = self.block_hash(name, offset);
        let mut blocks = self.blocks.write();
        blocks.insert(hash, data);
    }

    async fn get_file(&self, name: &str) -> Option<FileInfo> {
        let idx = self.index.read();
        idx.get(&(self.folder, name.to_string())).cloned()
    }

    async fn get_inbox_file(&self, _epoch: (), name: &str) -> Option<FileInfo> {
        let inbox = self.inbox.read();
        inbox.get(&(self.folder, name.to_string())).cloned()
    }

    async fn get_block(&self, name: &str, offset: i64) -> Option<Bytes> {
        let hash = self.block_hash(name, offset);
        let blocks = self.blocks.read();
        blocks.get(&hash).cloned()
    }
}

impl MemoryFolder {
    /// Resolve the 32-byte hash for a block at `(name, offset)` by scanning the
    /// committed index. Panics if not found — intended for test setup only.
    fn block_hash(&self, name: &str, offset: i64) -> [u8; 32] {
        let idx = self.index.read();
        let key = (self.folder, name.to_string());
        let file = idx.get(&key).unwrap_or_else(|| {
            panic!(
                "block_hash: file {name:?} not in index for folder {:?}",
                self.folder
            )
        });
        for block in &file.blocks {
            if block.offset == offset {
                return block.hash.as_slice().try_into().unwrap_or_else(|_| {
                    panic!("block_hash: hash for {name}@{offset} is not 32 bytes")
                });
            }
        }
        panic!("block_hash: no block at offset {offset} in file {name:?}");
    }
}

#[async_trait]
impl StorageFolder for MemoryFolder {
    fn id(&self) -> FolderId {
        self.folder
    }

    fn label(&self) -> &FolderLabelRef {
        &self.label
    }

    async fn index(
        &self,
        since: Sequence,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<FileInfo, StorageError>> + Send>>, StorageError>
    {
        let idx = self.index.read();
        let since = since.get();
        let folder = &self.folder;
        let files: Vec<FileInfo> = idx
            .iter()
            .filter(|((f, _), fi)| f == folder && fi.sequence > since)
            .map(|(_, fi)| fi.clone())
            .collect();
        Ok(Box::pin(stream::iter(files.into_iter().map(Ok))))
    }

    async fn read_block(
        &self,
        _name: &str,
        _offset: i64,
        _size: i32,
        hash: &[u8],
    ) -> Result<Bytes, StorageError> {
        let hash: [u8; 32] = hash
            .try_into()
            .map_err(|_| StorageError::InvalidInput("block hash must be 32 bytes".into()))?;
        let blocks = self.blocks.read();
        blocks
            .get(&hash)
            .cloned()
            .ok_or_else(|| StorageError::NotFound(format!("block not found: {:?}", &hash[..8])))
    }

    async fn apply_update(
        &self,
        remote_file: &FileInfo,
        _remote_device: &DeviceId,
    ) -> Result<UpdateResult, StorageError> {
        let idx = self.index.read();
        let key = (self.folder, remote_file.name.clone());

        let local_file = idx.get(&key);

        match local_file {
            None => {
                drop(idx);
                let mut inbox = self.inbox.write();
                inbox.insert(key, remote_file.clone());
                Ok(UpdateResult::NeedBlocks(remote_file.clone()))
            }
            Some(local) => {
                let local_ver = local.version.as_ref();
                let remote_ver = remote_file.version.as_ref();

                match (local_ver, remote_ver) {
                    (Some(lv), Some(rv)) => {
                        let ord = conflict::compare(lv, rv)
                            .map_err(|e| StorageError::Corruption(e.to_string()))?;
                        match ord {
                            conflict::Ordering::Less => {
                                let blocks_match = blocks_equal(local, remote_file);
                                if blocks_match {
                                    // Metadata-only update — commit directly.
                                    let local = local.clone();
                                    drop(idx);
                                    let mut idx = self.index.write();
                                    let mut seqs = self.sequences.write();
                                    let seq = seqs.entry(self.folder).or_insert(0);
                                    *seq += 1;
                                    let mut stored = remote_file.clone();
                                    stored.sequence = *seq;
                                    idx.insert(key, stored.clone());
                                    drop(local);
                                    Ok(UpdateResult::Applied(stored))
                                } else {
                                    // Need new blocks — stage in inbox.
                                    drop(idx);
                                    let mut inbox = self.inbox.write();
                                    inbox.insert(key, remote_file.clone());
                                    Ok(UpdateResult::NeedBlocks(remote_file.clone()))
                                }
                            }
                            conflict::Ordering::Greater | conflict::Ordering::Equal => {
                                Ok(UpdateResult::NoAction)
                            }
                            conflict::Ordering::Concurrent => Ok(UpdateResult::Concurrent {
                                local: local.clone(),
                                remote: remote_file.clone(),
                            }),
                        }
                    }
                    _ => {
                        drop(idx);
                        let mut inbox = self.inbox.write();
                        inbox.insert(key, remote_file.clone());
                        Ok(UpdateResult::NeedBlocks(remote_file.clone()))
                    }
                }
            }
        }
    }

    async fn complete_file(
        &self,
        name: &str,
        expected_version: Option<&crate::proto::bep::Vector>,
    ) -> Result<Option<FileInfo>, StorageError> {
        let key = (self.folder, name.to_string());
        let mut inbox = self.inbox.write();
        if let Some(file) = inbox.remove(&key) {
            // Check version to emulate the real backend behavior
            if file.version.as_ref() != expected_version {
                inbox.insert(key, file); // Put it back since we didn't complete it
                return Ok(None);
            }
            drop(inbox);
            let mut idx = self.index.write();
            let mut seqs = self.sequences.write();
            let seq = seqs.entry(self.folder).or_insert(0);
            *seq += 1;
            let mut stored = file;
            stored.sequence = *seq;
            idx.insert(key, stored.clone());
            return Ok(Some(stored));
        }
        Ok(None)
    }

    async fn local_sequence(&self) -> Result<Sequence, StorageError> {
        let seqs = self.sequences.read();
        Ok(Sequence(*seqs.get(&self.folder).unwrap_or(&0)))
    }

    async fn remote_state(
        &self,
        device: &DeviceId,
    ) -> Result<crate::storage::RemoteIndexState, StorageError> {
        let states = self.remote_states.read();
        Ok(states
            .get(&(self.folder, device.to_string()))
            .cloned()
            .unwrap_or_default())
    }

    async fn resolve_conflict(
        &self,
        winner: &FileInfo,
        loser: &FileInfo,
        loser_path: Option<&str>,
    ) -> Result<(), StorageError> {
        let mut idx = self.index.write();
        let mut seqs = self.sequences.write();
        let seq = seqs.entry(self.folder).or_insert(0);

        // Persist winner at its original path.
        *seq += 1;
        let mut stored_winner = winner.clone();
        stored_winner.sequence = *seq;
        idx.insert((self.folder, winner.name.clone()), stored_winner);

        // Persist loser at the conflict path, if a backup was requested.
        if let Some(path) = loser_path {
            *seq += 1;
            let mut stored_loser = loser.clone();
            stored_loser.name = path.to_string();
            stored_loser.sequence = *seq;
            idx.insert((self.folder, path.to_string()), stored_loser);
        }

        Ok(())
    }

    async fn store_block(
        &self,
        _name: &str,
        _offset: i64,
        hash: &[u8],
        data: Bytes,
    ) -> Result<(), StorageError> {
        let hash: [u8; 32] = hash
            .try_into()
            .map_err(|_| StorageError::InvalidInput("block hash must be 32 bytes".into()))?;
        let mut blocks = self.blocks.write();
        blocks.insert(hash, data);
        Ok(())
    }

    async fn reuse_block(
        &self,
        name: &str,
        offset: i64,
        hash: &[u8],
        size: i32,
    ) -> Result<bool, StorageError> {
        self.has_block(name, offset, hash, size).await
    }

    async fn has_block(
        &self,
        _file: &str,
        _offset: i64,
        hash: &[u8],
        _size: i32,
    ) -> Result<bool, StorageError> {
        let hash: [u8; 32] = match hash.try_into() {
            Ok(h) => h,
            Err(_) => return Ok(false),
        };
        let blocks = self.blocks.read();
        Ok(blocks.contains_key(&hash))
    }

    async fn set_remote_state(
        &self,
        device: &DeviceId,
        state: crate::storage::RemoteIndexState,
    ) -> Result<(), StorageError> {
        self.remote_states
            .write()
            .insert((self.folder, device.to_string()), state);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup_folder(id: &str) -> MemoryFolder {
        MemoryStorage::new()
            .folder(FolderId::from(id))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn apply_new_file() {
        let f = setup_folder("folder1").await;
        let file = make_file("hello.txt", &[(1, 1)], false);

        let result = f.apply_update(&file, &REMOTE_DEV).await.unwrap();
        assert!(matches!(result, UpdateResult::NeedBlocks(_)));

        // File should be in inbox, not committed index.
        assert!(f.get_file("hello.txt").await.is_none());

        // After completing, it should appear in the index.
        f.complete_file("hello.txt", file.version.as_ref())
            .await
            .unwrap();
        let stored = f.get_file("hello.txt").await.unwrap();
        assert_eq!(stored.name, "hello.txt");
        assert_eq!(stored.sequence, 1);
    }

    #[tokio::test]
    async fn apply_remote_dominates_same_blocks() {
        let f = setup_folder("folder1").await;

        // Insert local version (no blocks)
        let local = make_file("hello.txt", &[(1, 1)], false);
        f.insert_file(local).await;

        // Apply remote with higher version but same (empty) blocks
        let remote = make_file("hello.txt", &[(1, 2)], false);
        let result = f.apply_update(&remote, &REMOTE_DEV).await.unwrap();
        assert!(matches!(result, UpdateResult::Applied(_)));
    }

    #[tokio::test]
    async fn apply_remote_dominates_different_blocks() {
        use crate::proto::bep::BlockInfo;

        let f = setup_folder("folder1").await;

        let local = make_file("hello.txt", &[(1, 1)], false);
        f.insert_file(local).await;

        // Remote has different blocks
        let mut remote = make_file("hello.txt", &[(1, 2)], false);
        remote.blocks = vec![BlockInfo {
            hash: vec![0xAA; 32],
            offset: 0,
            size: 100,
        }];
        let result = f.apply_update(&remote, &REMOTE_DEV).await.unwrap();
        assert!(matches!(result, UpdateResult::NeedBlocks(_)));

        // Old version should still be in committed index until complete.
        let committed = f.get_file("hello.txt").await.unwrap();
        assert_eq!(committed.version.as_ref().unwrap().counters[0].value, 1);

        // After completing, new version is committed.
        f.complete_file("hello.txt", remote.version.as_ref())
            .await
            .unwrap();
        let committed = f.get_file("hello.txt").await.unwrap();
        assert_eq!(committed.version.as_ref().unwrap().counters[0].value, 2);
    }

    #[tokio::test]
    async fn apply_local_dominates() {
        let f = setup_folder("folder1").await;

        let local = make_file("hello.txt", &[(1, 5)], false);
        f.insert_file(local).await;

        let remote = make_file("hello.txt", &[(1, 3)], false);
        let result = f.apply_update(&remote, &REMOTE_DEV).await.unwrap();
        assert!(matches!(result, UpdateResult::NoAction));
    }

    #[tokio::test]
    async fn apply_concurrent_conflict() {
        let f = setup_folder("folder1").await;

        let local = make_file("hello.txt", &[(1, 5)], false);
        f.insert_file(local).await;

        // Concurrent: different counters
        let remote = make_file("hello.txt", &[(2, 3)], false);
        let result = f.apply_update(&remote, &REMOTE_DEV).await.unwrap();
        assert!(matches!(result, UpdateResult::Concurrent { .. }));
    }

    #[tokio::test]
    async fn index_full() {
        let backend = MemoryStorage::new();
        let f1 = backend.folder(FolderId::from("f1")).await.unwrap();
        let f2 = backend.folder(FolderId::from("f2")).await.unwrap();

        f1.insert_file(make_file("a.txt", &[(1, 1)], false)).await;
        f1.insert_file(make_file("b.txt", &[(1, 2)], false)).await;
        f2.insert_file(make_file("c.txt", &[(1, 1)], false)).await;

        use futures::StreamExt;
        let stream = f1.index(Sequence::ZERO).await.unwrap();
        let files: Vec<_> = stream.collect().await;
        assert_eq!(files.len(), 2);
    }

    #[tokio::test]
    async fn index_since() {
        let f = setup_folder("f1").await;
        f.insert_file(make_file("a.txt", &[(1, 1)], false)).await; // seq 1
        f.insert_file(make_file("b.txt", &[(1, 2)], false)).await; // seq 2

        use futures::StreamExt;
        let stream = f.index(Sequence(1)).await.unwrap();
        let files: Vec<_> = stream.collect().await;
        assert_eq!(files.len(), 1);
    }

    #[tokio::test]
    async fn read_block_round_trip() {
        let f = setup_folder("f1").await;
        let hash = [0xAB; 32];
        f.insert_file(make_file_with_blocks("test.txt", &[(1, 1)], &[hash]))
            .await;
        let data = Bytes::from_static(b"hello world");
        f.insert_block("test.txt", 0, data.clone()).await;

        let read = f.read_block("test.txt", 0, 11, &hash).await.unwrap();
        assert_eq!(read, data);
    }

    #[tokio::test]
    async fn read_block_not_found() {
        let f = setup_folder("f1").await;
        let missing_hash = [0xFF; 32];
        let result = f.read_block("missing.txt", 0, 10, &missing_hash).await;
        assert!(result.is_err());
    }

    #[test]
    fn conflict_larger_total_wins() {
        let a = make_file("test.txt", &[(1, 10)], false);
        let b = make_file("test.txt", &[(2, 5)], false);
        let dev_a = DeviceId::from_bytes([0; 32]);
        let dev_b = LOCAL_DEV;

        let (winner, _) = resolve_conflict(&a, &dev_a, &b, &dev_b).unwrap();
        assert_eq!(winner.version.as_ref().unwrap().counters[0].value, 10);
    }

    #[test]
    fn conflict_not_deleted_wins_on_tie() {
        let a = make_file("test.txt", &[(1, 5)], true);
        let b = make_file("test.txt", &[(2, 5)], false);
        let dev_a = DeviceId::from_bytes([0; 32]);
        let dev_b = LOCAL_DEV;

        let (winner, _) = resolve_conflict(&a, &dev_a, &b, &dev_b).unwrap();
        assert!(!winner.deleted);
    }

    #[test]
    fn conflict_device_id_tiebreak() {
        let a = make_file("test.txt", &[(1, 5)], false);
        let b = make_file("test.txt", &[(2, 5)], false);
        let dev_a = LOCAL_DEV; // larger
        let dev_b = DeviceId::from_bytes([0; 32]);

        let (_, loser) = resolve_conflict(&a, &dev_a, &b, &dev_b).unwrap();
        assert_eq!(loser.version.as_ref().unwrap().counters[0].id, 2);
    }
}
