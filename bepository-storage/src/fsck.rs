// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

use bepository_bep::error::StorageError;
use prost::Message;
use sha2::{Digest, Sha256};
use std::collections::HashSet;

use crate::proto::storage::{BlockRef, File, FolderIndexMeta, Inbox, RemoteIndexState};
use crate::store::FolderStore;
use crate::store::slate_err;
use crate::store_keys;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum FsckLevel {
    Quick,
    Structural,
    Full,
}

pub(crate) async fn check_folder_integrity(
    store: &FolderStore,
    level: FsckLevel,
) -> Result<Vec<String>, StorageError> {
    let mut errors = Vec::new();

    check_inbox(&store.db, &mut errors).await?;

    if level >= FsckLevel::Structural {
        let max_sequence = check_metadata(&store.db, &mut errors).await?;
        check_sequences(&store.db, max_sequence, level, &mut errors).await?;
        check_directory_blocks(&store.db, level, &mut errors).await?;
    }

    Ok(errors)
}

async fn check_inbox(db: &slatedb::Db, errors: &mut Vec<String>) -> Result<(), StorageError> {
    let mut iter = db
        .scan_prefix(store_keys::INBOX_PREFIX)
        .await
        .map_err(slate_err)?;
    while let Some(kv) = iter.next().await.map_err(slate_err)? {
        let key_display = store_keys::parse_inbox_key(&kv.key)
            .map(|(epoch, name)| format!("in/{}/{name}", epoch.as_base32()))
            .unwrap_or_else(|| format!("{kv:?}"));
        let Ok(inbox) = Inbox::decode(kv.value) else {
            errors.push(format!("Failed to decode Inbox for {key_display}"));
            continue;
        };
        if inbox.file_info.is_none() {
            errors.push(format!("Inbox entry {key_display} missing file_info"));
        }
    }
    Ok(())
}

async fn check_metadata(
    db: &slatedb::Db,
    errors: &mut Vec<String>,
) -> Result<Option<i64>, StorageError> {
    let mut max_sequence = None;
    if let Some(val) = db.get(store_keys::IX_KEY).await.map_err(slate_err)? {
        match FolderIndexMeta::decode(val) {
            Ok(meta) => max_sequence = Some(meta.max_sequence),
            Err(e) => errors.push(format!("Failed to decode FolderIndexMeta: {e}")),
        }
    }
    let mut iter = db
        .scan_prefix(store_keys::DEVICE_PREFIX)
        .await
        .map_err(slate_err)?;
    while let Some(kv) = iter.next().await.map_err(slate_err)? {
        if let Err(e) = RemoteIndexState::decode(kv.value) {
            errors.push(format!(
                "Failed to decode RemoteIndexState for key {:?}: {e}",
                kv.key
            ));
        }
    }
    Ok(max_sequence)
}

async fn check_sequences(
    db: &slatedb::Db,
    max_sequence: Option<i64>,
    level: FsckLevel,
    errors: &mut Vec<String>,
) -> Result<(), StorageError> {
    let mut max_observed_seq: u64 = 0;
    let mut max_observed_file_seq: i64 = 0;

    let mut iter = db
        .scan_prefix(store_keys::SEQ_PREFIX)
        .await
        .map_err(slate_err)?;
    while let Some(kv) = iter.next().await.map_err(slate_err)? {
        let Some(seq) = store_keys::parse_seq_key(&kv.key) else {
            errors.push(format!("Invalid sequence key: {:?}", kv.key));
            continue;
        };
        max_observed_seq = max_observed_seq.max(seq);

        let Ok(name) = std::str::from_utf8(&kv.value) else {
            errors.push(format!("Sequence {seq} name is not valid UTF-8"));
            continue;
        };

        let file_key = store_keys::file_key(name);
        let Some(file_val) = db.get(&file_key).await.map_err(slate_err)? else {
            errors.push(format!("Sequence {seq} points to missing file: {name}"));
            continue;
        };
        let Ok(file) = File::decode(file_val) else {
            errors.push(format!("Failed to decode File for sequence {seq}: {name}"));
            continue;
        };
        let Some(fi) = file.file_info else {
            errors.push(format!("File {name} missing file_info"));
            continue;
        };
        if u64::try_from(fi.sequence).ok() != Some(seq) {
            errors.push(format!(
                "Sequence mismatch for {name}: s/ says {seq}, n/ says {}",
                fi.sequence
            ));
        }
    }

    if level == FsckLevel::Full {
        let mut file_iter = db
            .scan_prefix(store_keys::FILE_PREFIX)
            .await
            .map_err(slate_err)?;
        while let Some(kv) = file_iter.next().await.map_err(slate_err)? {
            let Some(name) = store_keys::parse_file_key(&kv.key) else {
                continue;
            };
            let Ok(file) = File::decode(kv.value) else {
                continue;
            };
            let Some(fi) = file.file_info else { continue };
            max_observed_file_seq = max_observed_file_seq.max(fi.sequence);

            let seq_key = store_keys::seq_key(fi.sequence)?;
            if db.get(&seq_key).await.map_err(slate_err)?.is_none() {
                errors.push(format!(
                    "File {name} has sequence {} but no s/ entry exists",
                    fi.sequence
                ));
            }
        }
    }

    if let Some(stored_max) = max_sequence {
        if u64::try_from(stored_max)
            .map(|m| max_observed_seq > m)
            .unwrap_or(true)
        {
            errors.push(format!(
                "max_sequence {stored_max} in ix is less than highest s/ key {max_observed_seq}"
            ));
        }
        if level == FsckLevel::Full && max_observed_file_seq > stored_max {
            errors.push(format!(
                "max_sequence {stored_max} in ix is less than highest file sequence {max_observed_file_seq}"
            ));
        }
    }

    Ok(())
}

/// Groups a `DbIterator`'s entries by directory using a caller-supplied
/// key→dir extraction function. Maintains a one-entry lookahead so callers
/// can peek at the next directory and drain entries one directory at a time.
struct DirGroupIter<F: for<'a> Fn(&'a [u8]) -> Option<&'a str>> {
    iter: slatedb::DbIterator,
    pending: Option<slatedb::KeyValue>,
    dir_fn: F,
}

impl<F: for<'a> Fn(&'a [u8]) -> Option<&'a str>> DirGroupIter<F> {
    async fn new(mut iter: slatedb::DbIterator, dir_fn: F) -> Result<Self, StorageError> {
        let pending = iter.next().await.map_err(slate_err)?;
        Ok(Self {
            iter,
            pending,
            dir_fn,
        })
    }

    fn peek_dir(&self) -> Option<&str> {
        self.pending.as_ref().and_then(|kv| (self.dir_fn)(&kv.key))
    }

    async fn take_dir(&mut self, dir: &str) -> Result<Vec<slatedb::KeyValue>, StorageError> {
        let mut batch = Vec::new();
        while self.peek_dir() == Some(dir) {
            batch.push(self.pending.take().unwrap());
            self.pending = self.iter.next().await.map_err(slate_err)?;
        }
        Ok(batch)
    }
}

async fn collect_expected_blocks(
    db: &slatedb::Db,
    file_kvs: &[slatedb::KeyValue],
    errors: &mut Vec<String>,
) -> Result<HashSet<[u8; 32]>, StorageError> {
    let mut expected = HashSet::new();
    for kv in file_kvs {
        let Some(name) = store_keys::parse_file_key(&kv.key) else {
            errors.push(format!("Invalid file key: {:?}", kv.key));
            continue;
        };
        let Ok(file) = File::decode(kv.value.clone()) else {
            errors.push(format!("Failed to decode File for {name}"));
            continue;
        };
        let Some(file_info) = file.file_info else {
            errors.push(format!("File {name} missing file_info"));
            continue;
        };

        if file_info.deleted {
            continue;
        }

        for block in file_info.blocks {
            if block.hash.len() != 32 {
                errors.push(format!(
                    "Invalid hash length {} for file {name}",
                    block.hash.len()
                ));
                continue;
            }
            let hash: [u8; 32] = block.hash.as_slice().try_into().unwrap();
            expected.insert(hash);

            let rev_key = store_keys::block_reverse_key(&hash, &name);
            if db.get(&rev_key).await.map_err(slate_err)?.is_none() {
                errors.push(format!(
                    "Missing reverse reference br/ for block {} in file {name}",
                    hex::encode(hash)
                ));
            }
        }
    }
    Ok(expected)
}

async fn check_block_entries(
    db: &slatedb::Db,
    dir: &str,
    level: FsckLevel,
    expected: &mut HashSet<[u8; 32]>,
    block_kvs: &[slatedb::KeyValue],
    errors: &mut Vec<String>,
) -> Result<(), StorageError> {
    for kv in block_kvs {
        if let Some(hash) = store_keys::parse_block_ref_key(&kv.key) {
            let Ok(block_ref) = BlockRef::decode(kv.value.clone()) else {
                errors.push(format!(
                    "Failed to decode BlockRef for {} in dir {dir}",
                    hex::encode(hash)
                ));
                continue;
            };
            let target_key = store_keys::block_data_key(&block_ref.source_dir, &hash);
            if db.get(&target_key).await.map_err(slate_err)?.is_none() {
                errors.push(format!(
                    "BlockRef for {} in {dir} points to missing target in dir {}",
                    hex::encode(hash),
                    block_ref.source_dir
                ));
            }
            expected.remove(&hash);
        } else if let Some(hash) = store_keys::parse_block_data_key(&kv.key) {
            if level == FsckLevel::Full {
                let mut hasher = Sha256::new();
                hasher.update(&kv.value);
                let computed = hasher.finalize();
                if computed[..] != hash[..] {
                    errors.push(format!(
                        "Data checksum mismatch for block {} in dir {dir}",
                        hex::encode(hash)
                    ));
                }
            }
            expected.remove(&hash);
        }
    }
    Ok(())
}

async fn check_directory_blocks(
    db: &slatedb::Db,
    level: FsckLevel,
    errors: &mut Vec<String>,
) -> Result<(), StorageError> {
    let mut files = DirGroupIter::new(
        db.scan_prefix(store_keys::FILE_PREFIX)
            .await
            .map_err(slate_err)?,
        store_keys::file_key_dir,
    )
    .await?;

    let mut blocks = DirGroupIter::new(
        db.scan_prefix(store_keys::BLOCK_PREFIX)
            .await
            .map_err(slate_err)?,
        store_keys::block_key_dir,
    )
    .await?;

    loop {
        let dir = match (files.peek_dir(), blocks.peek_dir()) {
            (None, None) => break,
            (Some(f), None) => f.to_string(),
            (None, Some(b)) => b.to_string(),
            (Some(f), Some(b)) => std::cmp::min(f, b).to_string(),
        };

        let file_kvs = files.take_dir(&dir).await?;
        let block_kvs = blocks.take_dir(&dir).await?;

        let mut expected = collect_expected_blocks(db, &file_kvs, errors).await?;
        check_block_entries(db, &dir, level, &mut expected, &block_kvs, errors).await?;

        for missing in &expected {
            errors.push(format!(
                "Missing block {} in dir {dir}",
                hex::encode(missing)
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use slatedb::Db;
    use slatedb::config::Settings;
    use std::sync::Arc;

    async fn test_store() -> FolderStore {
        let object_store = Arc::new(InMemory::new());
        let db = Db::builder("test".to_string(), object_store)
            .with_settings(Settings {
                flush_interval: Some(std::time::Duration::from_millis(100)),
                max_unflushed_bytes: 4 * 1024 * 1024,
                manifest_poll_interval: std::time::Duration::from_millis(100),
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        FolderStore::new(db, Arc::new(crate::store::CompactionState::new()))
    }

    #[tokio::test]
    async fn test_check_folder_integrity_healthy() {
        let store = test_store().await;

        let file_info = crate::proto::storage::FileInfo {
            name: "dir1/file1".to_string(),
            sequence: 1,
            blocks: vec![crate::proto::storage::BlockInfo {
                offset: 0,
                size: 4,
                hash: vec![0u8; 32],
            }],
            ..Default::default()
        };

        let file = File {
            file_info: Some(file_info.clone()),
        };
        store
            .db
            .put(store_keys::file_key(&file_info.name), file.encode_to_vec())
            .await
            .unwrap();
        store
            .db
            .put(store_keys::seq_key(1).unwrap(), file_info.name.as_bytes())
            .await
            .unwrap();
        store
            .db
            .put(store_keys::block_data_key("dir1", &[0u8; 32]), b"data")
            .await
            .unwrap();
        store
            .db
            .put(
                store_keys::block_reverse_key(&[0u8; 32], &file_info.name),
                b"",
            )
            .await
            .unwrap();

        let errors = check_folder_integrity(&store, FsckLevel::Structural)
            .await
            .unwrap();
        assert!(errors.is_empty(), "Expected no errors, got: {errors:?}");
    }

    #[tokio::test]
    async fn test_check_folder_integrity_missing_block() {
        let store = test_store().await;

        let file_info = crate::proto::storage::FileInfo {
            name: "file1".to_string(),
            sequence: 1,
            blocks: vec![crate::proto::storage::BlockInfo {
                offset: 0,
                size: 4,
                hash: vec![1u8; 32],
            }],
            ..Default::default()
        };

        let file = File {
            file_info: Some(file_info.clone()),
        };
        store
            .db
            .put(store_keys::file_key(&file_info.name), file.encode_to_vec())
            .await
            .unwrap();
        store
            .db
            .put(store_keys::seq_key(1).unwrap(), file_info.name.as_bytes())
            .await
            .unwrap();
        let errors = check_folder_integrity(&store, FsckLevel::Structural)
            .await
            .unwrap();
        assert!(!errors.is_empty());
        assert!(errors.iter().any(|e| e.contains("Missing block")));
        assert!(
            errors
                .iter()
                .any(|e| e.contains("Missing reverse reference"))
        );
    }

    #[tokio::test]
    async fn test_check_folder_integrity_sequence_mismatch() {
        let store = test_store().await;

        let file_info = crate::proto::storage::FileInfo {
            name: "file1".to_string(),
            sequence: 1,
            ..Default::default()
        };

        let file = File {
            file_info: Some(file_info.clone()),
        };
        store
            .db
            .put(store_keys::file_key(&file_info.name), file.encode_to_vec())
            .await
            .unwrap();
        store
            .db
            .put(store_keys::seq_key(2).unwrap(), file_info.name.as_bytes())
            .await
            .unwrap();

        let errors = check_folder_integrity(&store, FsckLevel::Structural)
            .await
            .unwrap();
        assert!(!errors.is_empty());
        assert!(errors.iter().any(|e| e.contains("Sequence mismatch")));
    }

    #[tokio::test]
    async fn test_check_folder_integrity_data_corruption() {
        let store = test_store().await;

        let hash = [0u8; 32];
        let file_info = crate::proto::storage::FileInfo {
            name: "file1".to_string(),
            sequence: 1,
            blocks: vec![crate::proto::storage::BlockInfo {
                offset: 0,
                size: 4,
                hash: hash.to_vec(),
            }],
            ..Default::default()
        };

        let file = File {
            file_info: Some(file_info.clone()),
        };
        store
            .db
            .put(store_keys::file_key(&file_info.name), file.encode_to_vec())
            .await
            .unwrap();
        store
            .db
            .put(store_keys::seq_key(1).unwrap(), file_info.name.as_bytes())
            .await
            .unwrap();
        store
            .db
            .put(store_keys::block_data_key("", &hash), b"corrupt")
            .await
            .unwrap();
        store
            .db
            .put(store_keys::block_reverse_key(&hash, &file_info.name), b"")
            .await
            .unwrap();

        let errors_fs = check_folder_integrity(&store, FsckLevel::Structural)
            .await
            .unwrap();
        assert!(
            errors_fs.is_empty(),
            "Structural should pass: {errors_fs:?}"
        );

        let errors_data = check_folder_integrity(&store, FsckLevel::Full)
            .await
            .unwrap();
        assert!(!errors_data.is_empty());
        assert!(
            errors_data
                .iter()
                .any(|e| e.contains("Data checksum mismatch"))
        );
    }

    #[tokio::test]
    async fn test_check_folder_integrity_max_sequence_monotonicity() {
        let store = test_store().await;

        let meta = FolderIndexMeta { max_sequence: 10 };
        store
            .db
            .put(store_keys::IX_KEY, meta.encode_to_vec())
            .await
            .unwrap();

        let mut file_info = crate::proto::storage::FileInfo {
            name: "file1".to_string(),
            sequence: 5,
            ..Default::default()
        };
        let file = File {
            file_info: Some(file_info.clone()),
        };
        store
            .db
            .put(store_keys::file_key(&file_info.name), file.encode_to_vec())
            .await
            .unwrap();
        store
            .db
            .put(store_keys::seq_key(5).unwrap(), file_info.name.as_bytes())
            .await
            .unwrap();

        let errors = check_folder_integrity(&store, FsckLevel::Structural)
            .await
            .unwrap();
        assert!(errors.is_empty(), "Expected no errors, got: {errors:?}");

        let meta_invalid = FolderIndexMeta { max_sequence: 3 };
        store
            .db
            .put(store_keys::IX_KEY, meta_invalid.encode_to_vec())
            .await
            .unwrap();
        let errors = check_folder_integrity(&store, FsckLevel::Structural)
            .await
            .unwrap();
        assert!(
            errors
                .iter()
                .any(|e| e.contains("max_sequence 3 in ix is less than highest s/ key 5"))
        );

        let meta_valid_for_s = FolderIndexMeta { max_sequence: 5 };
        store
            .db
            .put(store_keys::IX_KEY, meta_valid_for_s.encode_to_vec())
            .await
            .unwrap();

        file_info.sequence = 6;
        let file_high_seq = File {
            file_info: Some(file_info.clone()),
        };
        store
            .db
            .put(
                store_keys::file_key(&file_info.name),
                file_high_seq.encode_to_vec(),
            )
            .await
            .unwrap();
        let errors = check_folder_integrity(&store, FsckLevel::Full)
            .await
            .unwrap();
        assert!(
            errors
                .iter()
                .any(|e| e.contains("max_sequence 5 in ix is less than highest file sequence 6"))
        );
    }

    #[tokio::test]
    async fn test_check_folder_integrity_deleted_file_with_blocks() {
        let store = test_store().await;

        let file_info = crate::proto::storage::FileInfo {
            name: "deleted_file".to_string(),
            sequence: 1,
            deleted: true,
            blocks: vec![crate::proto::storage::BlockInfo {
                offset: 0,
                size: 4,
                hash: vec![1u8; 32],
            }],
            ..Default::default()
        };

        let file = File {
            file_info: Some(file_info.clone()),
        };
        store
            .db
            .put(store_keys::file_key(&file_info.name), file.encode_to_vec())
            .await
            .unwrap();
        store
            .db
            .put(store_keys::seq_key(1).unwrap(), file_info.name.as_bytes())
            .await
            .unwrap();

        let errors = check_folder_integrity(&store, FsckLevel::Structural)
            .await
            .unwrap();
        assert!(
            errors.is_empty(),
            "Expected no errors for deleted file, got: {errors:?}"
        );
    }
}
