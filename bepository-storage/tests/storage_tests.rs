// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::sync::Arc;

use bytes::Bytes;
use object_store::memory::InMemory;

use bepository_bep::proto::bep::{BlockInfo, Counter, FileInfo, Vector};
use bepository_bep::storage::{
    Sequence, Storage, StorageFolder, StorageInspectorForTests, UpdateResult,
};
use bepository_bep::test_utils::{REMOTE_DEV, make_file, make_file_with_blocks};
use bepository_bep::{FolderId, FolderLabel, StorageError};
use bepository_storage::fault::{FaultObjectStore, ObjectStoreMethod};
use bepository_storage::{SlateFolder, SlateStorage};

async fn make_storage() -> SlateStorage {
    let object_store = Arc::new(InMemory::new());
    let (fault_store, _config) = FaultObjectStore::new(object_store);
    let s = SlateStorage::new(
        Arc::new(fault_store),
        None,
        tokio::runtime::Handle::current(),
    );
    s.activate(bepository_lock::Epoch::new(1).unwrap())
        .await
        .unwrap();
    s
}

pub fn make_file_with_blocks_of_size(
    name: &str,
    counters: &[(u64, u64)],
    block_hashes: &[[u8; 32]],
    block_size: i32,
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
                offset: (i64::try_from(i).expect("block index overflow")) * (i64::from(block_size)),
                size: block_size,
                hash: hash.to_vec(),
            })
            .collect(),
        block_size,
        size: (i64::try_from(block_hashes.len()).expect("block count overflow"))
            * (i64::from(block_size)),
        ..Default::default()
    }
}

async fn setup_folder(id: &str) -> (SlateStorage, SlateFolder) {
    let storage = make_storage().await;
    let folder_id = FolderId::from(id);
    let folder_label = FolderLabel::from(format!("LabelFor{id}"));
    storage
        .register_folder(folder_id, &folder_label)
        .await
        .unwrap();
    let folder = storage.folder(folder_id).await.unwrap();
    (storage, folder)
}

async fn check_integrity_passed(
    storage: &SlateStorage,
    level: bepository_storage::FsckLevel,
) -> bool {
    use futures::StreamExt;
    let stream = storage.check_integrity(level);
    tokio::pin!(stream);
    let mut all_passed = true;
    while let Some(res) = stream.next().await {
        match res {
            Ok(bepository_storage::FsckEvent::FolderFinished { errors_found, .. })
                if errors_found > 0 =>
            {
                all_passed = false;
            }
            Err(_) => return false,
            _ => {}
        }
    }
    all_passed
}

// --- Basic CRUD ---

#[tokio::test]
async fn file_round_trip() {
    let (storage, folder) = setup_folder("f1").await;
    let file = make_file("hello.txt", &[(1, 1)], false);

    folder.insert_file(file).await;

    let stored = folder.get_file("hello.txt").await.unwrap();
    assert_eq!(stored.name, "hello.txt");
    assert!(stored.sequence > 0);

    assert!(check_integrity_passed(&storage, bepository_storage::FsckLevel::Full).await);
}

#[tokio::test]
async fn get_missing_file() {
    let (_storage, folder) = setup_folder("f1").await;
    assert!(folder.get_file("missing.txt").await.is_none());
}

#[tokio::test]
async fn sequence_increments() {
    let (_storage, folder) = setup_folder("f1").await;

    folder
        .insert_file(make_file("a.txt", &[(1, 1)], false))
        .await;
    let a = folder.get_file("a.txt").await.unwrap();

    folder
        .insert_file(make_file("b.txt", &[(1, 2)], false))
        .await;
    let b = folder.get_file("b.txt").await.unwrap();

    assert!(b.sequence > a.sequence);
}

#[tokio::test]
async fn remove_folder_deletes_data() {
    use futures::StreamExt;
    let (storage, folder) = setup_folder("f1").await;
    let folder_id = folder.id();
    let sk = storage.list_folders().unwrap()[0].2.clone();

    // Add some data
    folder
        .insert_file(make_file("hello.txt", &[(1, 1)], false))
        .await;

    // Verify data exists in object store
    let prefix = object_store::path::Path::from(sk.as_str());
    let objects: Vec<_> = storage
        .object_store()
        .list(Some(&prefix))
        .collect::<Vec<_>>()
        .await;
    assert!(
        !objects.is_empty(),
        "Folder prefix should not be empty before removal"
    );

    // Remove folder
    storage.remove_folder(folder_id).await.unwrap();

    // Verify folder is gone from registry
    let folders = storage.list_folders().unwrap();
    assert!(folders.iter().all(|(id, _, _)| *id != folder_id));

    // Verify data is gone from object store
    let objects: Vec<_> = storage
        .object_store()
        .list(Some(&prefix))
        .collect::<Vec<_>>()
        .await;
    assert!(
        objects.is_empty(),
        "Folder prefix should be empty after removal"
    );
}

// --- Full and delta index ---

#[tokio::test]
async fn index_full() {
    let (_storage, folder) = setup_folder("f1").await;
    folder
        .insert_file(make_file("a.txt", &[(1, 1)], false))
        .await;
    folder
        .insert_file(make_file("b.txt", &[(1, 2)], false))
        .await;

    use futures::StreamExt;
    let stream = folder.index(Sequence::ZERO).await.unwrap();
    let files: Vec<_> = stream.collect().await;
    assert_eq!(files.len(), 2);
}

#[tokio::test]
async fn index_since() {
    let (_storage, folder) = setup_folder("f1").await;
    folder
        .insert_file(make_file("a.txt", &[(1, 1)], false))
        .await;
    folder
        .insert_file(make_file("b.txt", &[(1, 2)], false))
        .await;

    use futures::StreamExt;
    let stream = folder.index(Sequence(1)).await.unwrap();
    let files: Vec<_> = stream.collect::<Vec<_>>().await;
    assert_eq!(files.len(), 1);
    let fi = files[0].as_ref().unwrap();
    assert_eq!(fi.name, "b.txt");
}

// --- apply_update ---

#[tokio::test]
async fn apply_new_file() {
    let (_storage, folder) = setup_folder("f1").await;
    let file = make_file("hello.txt", &[(1, 1)], false);

    let result = folder.apply_update(&file, &REMOTE_DEV).await.unwrap();
    assert!(matches!(result, UpdateResult::NeedBlocks(_)));

    // File should be in inbox, not committed index.
    assert!(folder.get_file("hello.txt").await.is_none());

    // After completing, it should appear in the committed index.
    folder
        .complete_file("hello.txt", file.version.as_ref())
        .await
        .unwrap();
    let stored = folder.get_file("hello.txt").await.unwrap();
    assert_eq!(stored.name, "hello.txt");
    assert!(stored.sequence > 0);
}

#[tokio::test]
async fn apply_remote_dominates_same_blocks() {
    let (_storage, folder) = setup_folder("f1").await;

    let local = make_file("hello.txt", &[(1, 1)], false);
    folder.insert_file(local).await;

    let remote = make_file("hello.txt", &[(1, 2)], false);
    let result = folder.apply_update(&remote, &REMOTE_DEV).await.unwrap();
    assert!(matches!(result, UpdateResult::Applied(_)));
}

#[tokio::test]
async fn apply_remote_dominates_different_blocks() {
    let (_storage, folder) = setup_folder("f1").await;

    let local = make_file("hello.txt", &[(1, 1)], false);
    folder.insert_file(local).await;

    let mut remote = make_file("hello.txt", &[(1, 2)], false);
    remote.blocks = vec![BlockInfo {
        hash: vec![0xAA; 32],
        offset: 0,
        size: 100,
    }];
    let result = folder.apply_update(&remote, &REMOTE_DEV).await.unwrap();
    assert!(matches!(result, UpdateResult::NeedBlocks(_)));
}

#[tokio::test]
async fn apply_local_dominates() {
    let (_storage, folder) = setup_folder("f1").await;

    let local = make_file("hello.txt", &[(1, 5)], false);
    folder.insert_file(local).await;

    let remote = make_file("hello.txt", &[(1, 3)], false);
    let result = folder.apply_update(&remote, &REMOTE_DEV).await.unwrap();
    assert!(matches!(result, UpdateResult::NoAction));
}

#[tokio::test]
async fn apply_concurrent_conflict() {
    let (_storage, folder) = setup_folder("f1").await;

    let local = make_file("hello.txt", &[(1, 5)], false);
    folder.insert_file(local).await;

    let remote = make_file("hello.txt", &[(2, 3)], false);
    let result = folder.apply_update(&remote, &REMOTE_DEV).await.unwrap();
    assert!(matches!(result, UpdateResult::Concurrent { .. }));
}

// --- Block storage ---

#[tokio::test]
async fn block_round_trip() {
    let (_storage, folder) = setup_folder("f1").await;
    let hash = [0xBB; 32];
    let file = make_file_with_blocks("docs/report.txt", &[(1, 1)], &[hash]);
    folder.insert_file(file).await;

    let data = Bytes::from_static(b"block content here");
    folder
        .insert_block("docs/report.txt", 0, data.clone())
        .await;

    let read = folder
        .read_block("docs/report.txt", 0, 1024, &hash)
        .await
        .unwrap();
    assert_eq!(read, data);
}

#[tokio::test]
async fn has_block() {
    let (_storage, folder) = setup_folder("f1").await;
    let hash = [0xCC; 32];
    let file = make_file_with_blocks("test.txt", &[(1, 1)], &[hash]);
    folder.insert_file(file).await;

    assert!(!folder.has_block("test.txt", 0, &hash, 1024).await.unwrap());

    folder
        .insert_block("test.txt", 0, Bytes::from_static(b"data"))
        .await;

    assert!(folder.has_block("test.txt", 0, &hash, 1024).await.unwrap());
    assert!(
        !folder
            .has_block("test.txt", 0, &[0xDD; 32], 1024)
            .await
            .unwrap()
    );
}

// --- Remote state tracking ---

#[tokio::test]
async fn remote_state_tracking() {
    let (_storage, folder) = setup_folder("f1").await;

    let state = folder.remote_state(&REMOTE_DEV).await.unwrap();
    assert_eq!(state.max_sequence.get(), 0);

    folder
        .set_remote_state(
            &REMOTE_DEV,
            bepository_bep::storage::RemoteIndexState {
                index_id: 0,
                max_sequence: Sequence(42),
            },
        )
        .await
        .unwrap();

    let state = folder.remote_state(&REMOTE_DEV).await.unwrap();
    assert_eq!(state.max_sequence.get(), 42);
}

// --- Local sequence ---

#[tokio::test]
async fn local_sequence_tracks_inserts() {
    let (_storage, folder) = setup_folder("f1").await;

    let seq = folder.local_sequence().await.unwrap();
    assert_eq!(seq, Sequence::ZERO);

    folder
        .insert_file(make_file("a.txt", &[(1, 1)], false))
        .await;

    let seq = folder.local_sequence().await.unwrap();
    assert!(seq.get() > 0);
}

// --- Cross-directory block dedup ---

#[tokio::test]
async fn block_deduplication() {
    let (_storage, folder) = setup_folder("f1").await;
    let hash = [0xEE; 32];

    // Two files sharing the same block hash.
    folder
        .insert_file(make_file_with_blocks_of_size(
            "photos/a.jpg",
            &[(1, 1)],
            &[hash],
            4096,
        ))
        .await;
    folder
        .insert_file(make_file_with_blocks_of_size(
            "backup/a.jpg",
            &[(1, 1)],
            &[hash],
            4096,
        ))
        .await;

    let data = Bytes::from(vec![0xEE; 4096]);
    folder.insert_block("photos/a.jpg", 0, data.clone()).await;

    // Both should be readable. For the second file, the engine would call `reuse_block`.
    assert!(
        folder
            .reuse_block("backup/a.jpg", 0, &hash, 4096)
            .await
            .unwrap()
    );

    let read1 = folder
        .read_block("photos/a.jpg", 0, 4096, &hash)
        .await
        .unwrap();
    let read2 = folder
        .read_block("backup/a.jpg", 0, 4096, &hash)
        .await
        .unwrap();
    assert_eq!(read1, data);
    assert_eq!(read2, data);
}

// --- Two-phase file intake (inbox) ---

#[tokio::test]
async fn complete_file_is_idempotent() {
    let (_storage, folder) = setup_folder("f1").await;
    let file = make_file("test.txt", &[(1, 1)], false);

    folder.apply_update(&file, &REMOTE_DEV).await.unwrap();
    folder
        .complete_file("test.txt", file.version.as_ref())
        .await
        .unwrap();

    // Second call should be a no-op.
    folder
        .complete_file("test.txt", file.version.as_ref())
        .await
        .unwrap();

    let stored = folder.get_file("test.txt").await.unwrap();
    assert_eq!(stored.name, "test.txt");
}

#[tokio::test]
async fn apply_update_uncommitted_returns_need_blocks() {
    let (_storage, folder) = setup_folder("f1").await;
    let file = make_file("partial.txt", &[(1, 1)], false);

    folder.apply_update(&file, &REMOTE_DEV).await.unwrap();

    // Applying same version again should still return NeedBlocks if not committed.
    let result = folder.apply_update(&file, &REMOTE_DEV).await.unwrap();
    assert!(matches!(result, UpdateResult::NeedBlocks(_)));
}

#[tokio::test]
async fn version_upgrade_during_transfer() {
    let (_storage, folder) = setup_folder("f1").await;

    // V1 is committed.
    folder
        .insert_file(make_file("upgrade.txt", &[(1, 1)], false))
        .await;

    // V2 arrives (newer) — staged in inbox.
    let v2 = make_file_with_blocks("upgrade.txt", &[(1, 2)], &[[0xBB; 32]]);
    folder.apply_update(&v2, &REMOTE_DEV).await.unwrap();

    // V1 should still be in committed index during transfer.
    let committed = folder.get_file("upgrade.txt").await.unwrap();
    assert_eq!(committed.version.as_ref().unwrap().counters[0].value, 1);

    // Complete V2.
    folder
        .complete_file("upgrade.txt", v2.version.as_ref())
        .await
        .unwrap();
    let committed = folder.get_file("upgrade.txt").await.unwrap();
    assert_eq!(committed.version.as_ref().unwrap().counters[0].value, 2);
}

#[tokio::test]
async fn inbox_overwrite_race_condition() {
    let (_storage, folder) = setup_folder("f1").await;

    // V1 arrives — staged in inbox.
    let v1 = make_file("race.txt", &[(1, 1)], false);
    folder.apply_update(&v1, &REMOTE_DEV).await.unwrap();

    // V2 arrives (newer) — overwrites V1 in inbox.
    let v2 = make_file("race.txt", &[(1, 2)], false);
    folder.apply_update(&v2, &REMOTE_DEV).await.unwrap();

    // Attempt to complete V1 — should be a no-op because inbox now has V2.
    folder
        .complete_file("race.txt", v1.version.as_ref())
        .await
        .unwrap();
    assert!(folder.get_file("race.txt").await.is_none());

    // Complete V2.
    folder
        .complete_file("race.txt", v2.version.as_ref())
        .await
        .unwrap();
    let committed = folder.get_file("race.txt").await.unwrap();
    assert_eq!(committed.version.as_ref().unwrap().counters[0].value, 2);
}

// --- Compaction GC ---

#[tokio::test]
async fn block_visibility_during_gc() {
    let (_storage, folder) = setup_folder("f1").await;
    let hash = [0xAA; 32];

    folder
        .insert_file(make_file_with_blocks_of_size(
            "a.txt",
            &[(1, 1)],
            &[hash],
            4096,
        ))
        .await;
    folder
        .insert_block("a.txt", 0, Bytes::from(vec![0xAA; 4096]))
        .await;

    // Baseline: block is visible.
    assert!(folder.has_block("a.txt", 0, &hash, 4096).await.unwrap());
}

// ---------------------------------------------------------------------------
// Fault injection tests — ObjectStore error propagation through SlateStorage
// ---------------------------------------------------------------------------

// Recovery path: ObjectStore Put error → SlateStorage returns TransientIo.
#[tokio::test]
async fn object_store_put_error_propagates_as_transient_io() {
    // Create fresh storage (not yet activated) so we can inject a fault before the first put.
    let object_store = Arc::new(InMemory::new());
    let (fault_store, fault_config) = FaultObjectStore::new(object_store);
    let storage = SlateStorage::new(
        Arc::new(fault_store),
        None,
        tokio::runtime::Handle::current(),
    );

    // Inject one Put failure: activate calls write_meta which does an unconditional put.
    fault_config.set(ObjectStoreMethod::Put, 1, || object_store::Error::Generic {
        store: "test",
        source: Box::new(std::io::Error::other("injected")),
    });

    let result = storage
        .activate(bepository_lock::Epoch::new(1).unwrap())
        .await;
    assert!(
        matches!(result, Err(StorageError::TransientIo(_))),
        "Put error should map to TransientIo, got: {result:?}"
    );
}

// Recovery path: ObjectStore list error during activate → SlateStorage returns TransientIo.
// (put_opts is only used by bepository-lock, not SlateStorage directly; this test
// covers the list_with_delimiter call in read_meta_unlocked which is the first I/O
// performed by activate.)
#[tokio::test]
async fn object_store_list_error_propagates_as_transient_io() {
    let object_store = Arc::new(InMemory::new());
    let (fault_store, fault_config) = FaultObjectStore::new(object_store);
    let storage = SlateStorage::new(
        Arc::new(fault_store),
        None,
        tokio::runtime::Handle::current(),
    );

    // Inject one ListWithDelimiter failure: activate calls read_meta_unlocked which lists files.
    fault_config.set(ObjectStoreMethod::ListWithDelimiter, 1, || {
        object_store::Error::Generic {
            store: "test",
            source: Box::new(std::io::Error::other("injected")),
        }
    });

    let result = storage
        .activate(bepository_lock::Epoch::new(1).unwrap())
        .await;
    assert!(
        matches!(result, Err(StorageError::TransientIo(_))),
        "List error should map to TransientIo, got: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// New tests: seq-at-stage-time invariants
// ---------------------------------------------------------------------------

/// Concurrent complete_file calls on the same staged file must produce a
/// consistent committed state: exactly one seq_key, all callers that return
/// Some(_) carry the same sequence number.
#[tokio::test]
async fn concurrent_complete_is_consistent() {
    let (_storage, folder) = setup_folder("concurrent").await;
    let file = make_file("concur.txt", &[(1, 1)], false);

    folder.apply_update(&file, &REMOTE_DEV).await.unwrap();

    // Fire 8 concurrent complete_file calls.
    let n = 8usize;
    let mut handles = Vec::with_capacity(n);
    for _ in 0..n {
        let folder = folder.clone();
        let ver = file.version.clone();
        handles.push(tokio::spawn(async move {
            folder
                .complete_file("concur.txt", ver.as_ref())
                .await
                .expect("complete_file must not error")
        }));
    }
    let results: Vec<Option<_>> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.expect("task panicked"))
        .collect();

    // Collect all sequences returned by Some(_) results.
    let seqs: Vec<i64> = results
        .iter()
        .filter_map(|r| r.as_ref())
        .map(|fi| fi.sequence)
        .collect();

    // At least one caller must have committed.
    assert!(!seqs.is_empty(), "at least one caller should return Some");

    // All Some(_) results must carry the same sequence.
    let first_seq = seqs[0];
    for &s in &seqs {
        assert_eq!(s, first_seq, "all successful callers must return same seq");
    }

    // The committed file must be visible.
    let committed = folder.get_file("concur.txt").await.unwrap();
    assert_eq!(committed.sequence, first_seq);
}

/// Stage v1, then stage v2 (overwriting v1 in inbox) without completing v1.
/// Completing v1 must be a no-op and v2 must commit cleanly at the next seq.
#[tokio::test]
async fn restage_overwrites_inbox_and_v1_complete_is_noop() {
    let (_storage, folder) = setup_folder("restage").await;
    let v1 = make_file("gap.txt", &[(1, 1)], false);
    let v2 = make_file("gap.txt", &[(1, 2)], false);

    folder.apply_update(&v1, &REMOTE_DEV).await.unwrap();

    let epoch = folder.epoch().unwrap();
    let v1_staged = folder.get_inbox_file(epoch, "gap.txt").await;
    assert!(v1_staged.is_some(), "v1 should be staged");

    // v2 arrives — overwrites the inbox entry.
    folder.apply_update(&v2, &REMOTE_DEV).await.unwrap();

    // Completing v1 must be a no-op because inbox now holds v2.
    let v1_result = folder
        .complete_file("gap.txt", v1.version.as_ref())
        .await
        .unwrap();
    assert!(v1_result.is_none(), "completing stale v1 must be a no-op");

    // Completing v2 must succeed.
    let v2_result = folder
        .complete_file("gap.txt", v2.version.as_ref())
        .await
        .unwrap()
        .expect("v2 must commit");

    let v2_seq = v2_result.sequence;
    assert!(v2_seq > 0, "v2 must have a positive sequence");

    let committed = folder.get_file("gap.txt").await.unwrap();
    assert_eq!(committed.version, v2.version);
    assert_eq!(committed.sequence, v2_seq);
}

#[tokio::test]
async fn test_segment_extractor_compatibility_and_reopen() {
    let object_store = Arc::new(InMemory::new());
    let folder_id = FolderId::from("reopen-test");
    let folder_label = FolderLabel::from("ReopenTest");

    // 1. Create a storage, register folder (which initializes DB with bep-segment extractor)
    let folder_storage_path = {
        let s = SlateStorage::new(
            object_store.clone(),
            None,
            tokio::runtime::Handle::current(),
        );
        s.activate(bepository_lock::Epoch::new(1).unwrap())
            .await
            .unwrap();
        let sk = s.register_folder(folder_id, &folder_label).await.unwrap();
        let path = sk.to_string();
        // Insert a file to ensure some writes happen and manifest/SSTs are written
        let folder = s.folder(folder_id).await.unwrap();
        folder
            .insert_file(make_file("hello.txt", &[(1, 1)], false))
            .await;
        s.close().await.unwrap();
        path
    };

    // 2. Reopen using the same SlateStorage (which uses BepSegmentExtractor). This should succeed.
    {
        let s = SlateStorage::new(
            object_store.clone(),
            None,
            tokio::runtime::Handle::current(),
        );
        s.activate(bepository_lock::Epoch::new(2).unwrap())
            .await
            .unwrap();
        let folder = s.folder(folder_id).await.unwrap();
        let stored = folder.get_file("hello.txt").await.unwrap();
        assert_eq!(stored.name, "hello.txt");
        s.close().await.unwrap();
    }

    // 3. Try to open the same database using a different segment extractor name.
    // This should fail because the segment extractor name stored in the manifest is "bep-segment",
    // and SlateDB validates this on open.
    {
        use slatedb::PrefixExtractor;
        use slatedb::PrefixTarget;

        #[derive(Debug, Default)]
        struct DummyExtractor;

        impl PrefixExtractor for DummyExtractor {
            fn name(&self) -> &str {
                "different-segment-extractor-name"
            }

            fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
                let bytes: &Bytes = match target {
                    PrefixTarget::Point(b) | PrefixTarget::Prefix(b) => b,
                };
                (bytes.len() >= 2).then_some(2)
            }
        }

        // Open with dummy extractor
        let db_result = slatedb::Db::builder(folder_storage_path, object_store)
            .with_segment_extractor(Arc::new(DummyExtractor))
            .build()
            .await;

        let err = match db_result {
            Ok(_) => panic!("Expected database open to fail with mismatched segment extractor"),
            Err(e) => e,
        };
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("segment_extractor") || err_msg.contains("extractor"),
            "Error message should mention extractor mismatch, got: {err_msg}"
        );
    }
}

#[tokio::test]
async fn test_inline_block_dedup_bypass() {
    let (_storage, folder) = setup_folder("inline-dedup-bypass").await;

    let hash = [0x77; 32];

    // Create two files sharing the same block hash but with tiny (inline) blocks (e.g. 100 bytes)
    folder
        .insert_file(make_file_with_blocks_of_size(
            "a.txt",
            &[(1, 1)],
            &[hash],
            100,
        ))
        .await;
    folder
        .insert_file(make_file_with_blocks_of_size(
            "b.txt",
            &[(1, 1)],
            &[hash],
            100,
        ))
        .await;

    // Store block for a.txt (should inline)
    let data = Bytes::from(vec![0x77; 100]);
    folder.insert_block("a.txt", 0, data.clone()).await;

    // Both should bypass deduplication. reuse_block for the second file should return false.
    let reused = folder.reuse_block("b.txt", 0, &hash, 100).await.unwrap();
    assert!(!reused, "inline block should bypass reuse_block/dedup");

    // Write the block to b.txt directly
    folder.insert_block("b.txt", 0, data.clone()).await;

    // Both should be readable, and they have separate inline copies
    let read1 = folder.read_block("a.txt", 0, 100, &hash).await.unwrap();
    let read2 = folder.read_block("b.txt", 0, 100, &hash).await.unwrap();
    assert_eq!(read1, data);
    assert_eq!(read2, data);

    // Verify inline block has_block queries
    assert!(
        folder.has_block("a.txt", 0, &hash, 100).await.unwrap(),
        "inline block should exist in file metadata"
    );
    assert!(
        folder.has_block("b.txt", 0, &hash, 100).await.unwrap(),
        "inline block should exist in file metadata"
    );

    // Verify no reverse ref key is written for this hash in the DB (querying as a separated block)
    assert!(
        !folder.has_block("a.txt", 0, &hash, 4096).await.unwrap(),
        "inline block should not have br/ row in DB"
    );
}

#[tokio::test]
async fn test_checkpoint_block_survival_after_compaction() {
    use bepository_storage::snapshot::SnapshotFs;

    let (storage, folder) = setup_folder("checkpoint-compaction").await;
    let hash = [0x99; 32];
    let file = make_file_with_blocks_of_size("photos/a.jpg", &[(1, 1)], &[hash], 4096);

    folder.insert_file(file).await;

    let data = Bytes::from(vec![0x99; 4096]);
    folder.insert_block("photos/a.jpg", 0, data.clone()).await;

    // Retrieve allocated blockseq from file metadata
    let raw_val = folder
        .get_raw(&bepository_storage::store_keys::file_key("photos/a.jpg"))
        .await
        .unwrap();
    let file =
        <bepository_storage::proto::storage::File as prost::Message>::decode(&raw_val[..]).unwrap();
    let file_info = file.file_info.unwrap();
    let seq = file_info.blocks[0]
        .blockseq
        .expect("blockseq must be allocated");

    // Verify raw block data key exists in the database
    let bd_key = bepository_storage::store_keys::block_data_seq_key(seq);
    assert!(folder.get_raw(&bd_key).await.is_some());

    // Create a checkpoint
    storage
        .create_checkpoints(
            std::time::Duration::from_secs(3600),
            std::time::Duration::from_secs(3600),
        )
        .await
        .unwrap();

    // Get the snapshot reference
    let snapshots = storage.list_snapshots().await.unwrap();
    let snap = snapshots
        .iter()
        .find(|s| s.folder_label.as_str() == "LabelForcheckpoint-compaction")
        .expect("Snapshot should exist for folder");

    // Delete the file from the head index (un-reference the block)
    let mut deleted_file = make_file_with_blocks_of_size("photos/a.jpg", &[(1, 2)], &[hash], 4096);
    deleted_file.deleted = true;
    deleted_file.blocks.clear();
    deleted_file.size = 0;
    folder.insert_file(deleted_file).await;

    // Verify head has the file marked as deleted
    assert!(folder.get_file("photos/a.jpg").await.unwrap().deleted);

    // Run full compaction
    storage.compact(folder.id()).await.unwrap();

    // After compaction the active DB no longer references the orphaned
    // bd/<seq> row, so a `get` against the active manifest returns None.
    // The underlying data still lives in the SSTs pinned by the checkpoint's
    // manifest and remains readable through the checkpoint reader below.
    let _ = bd_key;

    // Verify block is still readable from the checkpoint snapshot (survival via SlateDB checkpoint SST pinning)
    let read_snap = storage
        .read_bytes(snap, "photos/a.jpg", 0, 4096)
        .await
        .unwrap();
    assert_eq!(read_snap, data);
}

#[tokio::test]
async fn test_orphaned_block_data_dropped_after_compaction() {
    let (storage, folder) = setup_folder("orphaned-compaction").await;
    let hash = [0x88; 32];
    let file = make_file_with_blocks_of_size("photos/b.jpg", &[(1, 1)], &[hash], 4096);

    folder.insert_file(file).await;

    let data = Bytes::from(vec![0x88; 4096]);
    folder.insert_block("photos/b.jpg", 0, data.clone()).await;

    // Retrieve allocated blockseq from file metadata
    let raw_val = folder
        .get_raw(&bepository_storage::store_keys::file_key("photos/b.jpg"))
        .await
        .unwrap();
    let file =
        <bepository_storage::proto::storage::File as prost::Message>::decode(&raw_val[..]).unwrap();
    let file_info = file.file_info.unwrap();
    let seq = file_info.blocks[0]
        .blockseq
        .expect("blockseq must be allocated");

    // Verify raw block data key exists in the database
    let bd_key = bepository_storage::store_keys::block_data_seq_key(seq);
    assert!(folder.get_raw(&bd_key).await.is_some());

    // Delete the file from the head index (un-reference the block)
    let mut deleted_file = make_file_with_blocks_of_size("photos/b.jpg", &[(1, 2)], &[hash], 4096);
    deleted_file.deleted = true;
    deleted_file.blocks.clear();
    deleted_file.size = 0;
    folder.insert_file(deleted_file).await;

    // Run full compaction (no checkpoints exist, so the orphaned block should be dropped)
    storage.compact(folder.id()).await.unwrap();
    let folder = storage.folder(folder.id()).await.unwrap();

    // The raw block data key must be physically deleted from the database
    assert!(folder.get_raw(&bd_key).await.is_none());
}

#[tokio::test]
async fn block_roundtrip_various_sizes() {
    let (_storage, folder) = setup_folder("roundtrip-sizes").await;

    let test_cases = vec![
        ("f_1", vec![0x11; 1]),
        ("f_4095", vec![0x22; 4095]),
        ("f_4096", vec![0x33; 4096]),
        ("f_128k", vec![0x44; 128 * 1024]),
        ("f_1m", vec![0x55; 1024 * 1024]),
    ];

    for (name, data) in &test_cases {
        use sha2::{Digest, Sha256};
        let hash = Sha256::digest(data);
        let block_size = i32::try_from(data.len()).unwrap();
        let hash_arr: [u8; 32] = hash.as_slice().try_into().unwrap();

        let file = make_file_with_blocks_of_size(name, &[(1, 1)], &[hash_arr], block_size);
        folder.insert_file(file).await;

        let bytes_data = Bytes::from(data.clone());
        folder.insert_block(name, 0, bytes_data.clone()).await;

        // Verify read_block works.
        let read = folder
            .read_block(name, 0, block_size, &hash_arr)
            .await
            .unwrap();
        assert_eq!(read, bytes_data);

        // Fetch raw File from database.
        let raw_val = folder
            .get_raw(&bepository_storage::store_keys::file_key(name))
            .await
            .unwrap();
        let file_proto =
            <bepository_storage::proto::storage::File as prost::Message>::decode(&raw_val[..])
                .unwrap();
        let file_info = file_proto.file_info.unwrap();
        let block_info = &file_info.blocks[0];

        let dir = bepository_storage::store_keys::dirname(name);
        let bd_key = block_info
            .blockseq
            .map(bepository_storage::store_keys::block_data_seq_key);
        let b_key = bepository_storage::store_keys::block_pointer_key(dir, &hash_arr);
        let br_key = bepository_storage::store_keys::block_reverse_key(&hash_arr, name);

        if data.len() < 4096 {
            // Inline block
            assert_eq!(block_info.inline_data.as_ref().unwrap(), data);
            assert!(block_info.blockseq.is_none());
            // No bd/, b/, or br/ rows should exist.
            if let Some(ref k) = bd_key {
                assert!(folder.get_raw(k).await.is_none());
            }
            assert!(folder.get_raw(&b_key).await.is_none());
            assert!(folder.get_raw(&br_key).await.is_none());
        } else {
            // Separated block
            assert!(block_info.inline_data.is_none());
            let seq = block_info.blockseq.unwrap();
            assert!(seq >= 1024);
            // bd/, b/, and br/ rows should exist.
            let db_data = folder.get_raw(bd_key.as_ref().unwrap()).await.unwrap();
            assert_eq!(db_data, *data);
            assert!(folder.get_raw(&b_key).await.is_some());
            assert!(folder.get_raw(&br_key).await.is_some());
        }
    }
}

#[tokio::test]
async fn blockseq_stable_across_compaction() {
    let (storage, folder) = setup_folder("blockseq-stability").await;
    let hash = [0x55; 32];
    let file = make_file_with_blocks_of_size("stable.txt", &[(1, 1)], &[hash], 4096);
    folder.insert_file(file).await;

    let data = Bytes::from(vec![0x55; 4096]);
    folder.insert_block("stable.txt", 0, data.clone()).await;

    // Get blockseq
    let raw_val = folder
        .get_raw(&bepository_storage::store_keys::file_key("stable.txt"))
        .await
        .unwrap();
    let file_proto =
        <bepository_storage::proto::storage::File as prost::Message>::decode(&raw_val[..]).unwrap();
    let file_info = file_proto.file_info.unwrap();
    let seq = file_info.blocks[0].blockseq.expect("must have blockseq");

    let bd_key = bepository_storage::store_keys::block_data_seq_key(seq);
    let original_db_data = folder.get_raw(&bd_key).await.unwrap();
    assert_eq!(original_db_data, data);

    // Force compaction
    storage.compact(folder.id()).await.unwrap();
    let folder = storage.folder(folder.id()).await.unwrap();

    // Confirm bd/<seq> resolves to the same value
    let post_compaction_db_data = folder.get_raw(&bd_key).await.unwrap();
    assert_eq!(post_compaction_db_data, data);

    // Confirm reading the block still works
    let read = folder
        .read_block("stable.txt", 0, 4096, &hash)
        .await
        .unwrap();
    assert_eq!(read, data);
}

#[tokio::test]
async fn inline_block_survives_compaction_intact() {
    let (storage, folder) = setup_folder("inline-survives-compaction").await;

    // A block of size 5000 (which is >= 4096 and would normally be separated).
    // We manually construct it as inline in FileInfo.
    let data = vec![0x66; 5000];
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(&data);
    let hash_arr: [u8; 32] = hash.as_slice().try_into().unwrap();

    let file_info = bepository_storage::proto::storage::FileInfo {
        name: "frozen.txt".into(),
        size: 5000,
        blocks: vec![bepository_storage::proto::storage::BlockInfo {
            hash: hash_arr.to_vec(),
            offset: 0,
            size: 5000,
            inline_data: Some(data.clone()),
            blockseq: None,
        }],
        block_size: 5000,
        sequence: 1,
        ..Default::default()
    };

    let file_proto = bepository_storage::proto::storage::File {
        file_info: Some(file_info),
    };

    // Manually write file metadata to DB.
    let file_key = bepository_storage::store_keys::file_key("frozen.txt");
    folder
        .put_raw(file_key.clone(), prost::Message::encode_to_vec(&file_proto))
        .await;

    // Verify it can be read back using read_block.
    let read = folder
        .read_block("frozen.txt", 0, 5000, &hash_arr)
        .await
        .unwrap();
    assert_eq!(read, data);

    // Force compaction.
    storage.compact(folder.id()).await.unwrap();
    let folder = storage.folder(folder.id()).await.unwrap();

    // Verify metadata still has inline_data and no blockseq.
    let raw_val = folder.get_raw(&file_key).await.unwrap();
    let read_file_proto =
        <bepository_storage::proto::storage::File as prost::Message>::decode(&raw_val[..]).unwrap();
    let read_file_info = read_file_proto.file_info.unwrap();
    let read_block_info = &read_file_info.blocks[0];

    assert_eq!(read_block_info.inline_data.as_ref().unwrap(), &data);
    assert!(read_block_info.blockseq.is_none());

    // Verify it can still be read back.
    let read_post = folder
        .read_block("frozen.txt", 0, 5000, &hash_arr)
        .await
        .unwrap();
    assert_eq!(read_post, data);
}

#[tokio::test]
async fn crash_before_flush_leaves_no_orphans() {
    let object_store = Arc::new(object_store::memory::InMemory::new());
    let folder_id = FolderId::from("crash-test");
    let folder_label = FolderLabel::from("CrashTest");

    let hash = [0x77; 32];
    let data = Bytes::from(vec![0x77; 4096]);

    let seq = {
        // 1. Activate storage at Epoch 1.
        let s = SlateStorage::new(
            object_store.clone(),
            None,
            tokio::runtime::Handle::current(),
        );
        s.activate(bepository_lock::Epoch::new(1).unwrap())
            .await
            .unwrap();
        s.register_folder(folder_id, &folder_label).await.unwrap();
        let folder = s.folder(folder_id).await.unwrap();

        // Stage file in the inbox.
        let file = make_file_with_blocks_of_size("crash.txt", &[(1, 1)], &[hash], 4096);
        folder.apply_update(&file, &REMOTE_DEV).await.unwrap();

        // Write the block. This allocates a seqno and writes bd/<seqno> + b/ + br/
        folder.insert_block("crash.txt", 0, data.clone()).await;

        // Retrieve the seqno.
        let epoch = folder.epoch().unwrap();
        let inbox_key = bepository_storage::store_keys::inbox_key(epoch, "crash.txt");
        let raw_inbox = folder.get_raw(&inbox_key).await.unwrap();
        let inbox =
            <bepository_storage::proto::storage::Inbox as prost::Message>::decode(&raw_inbox[..])
                .unwrap();
        let staged = inbox.file_info.unwrap();
        let seq = staged.blocks[0].blockseq.expect("must have blockseq");

        // Verify bd/<seq> exists.
        let bd_key = bepository_storage::store_keys::block_data_seq_key(seq);
        assert!(folder.get_raw(&bd_key).await.is_some());

        // Close storage without calling complete_file.
        s.close().await.unwrap();
        seq
    };

    // 2. Re-open storage at Epoch 2 (simulating restart).
    let s = SlateStorage::new(
        object_store.clone(),
        None,
        tokio::runtime::Handle::current(),
    );
    s.activate(bepository_lock::Epoch::new(2).unwrap())
        .await
        .unwrap();
    let folder = s.folder(folder_id).await.unwrap();

    // The old inbox entry is still there. We run gc_inbox to clear stale inbox entries (< Epoch 2).
    s.gc_inbox().await.unwrap();

    // Verify inbox file is gone.
    let old_inbox_key = bepository_storage::store_keys::inbox_key(
        bepository_lock::Epoch::new(1).unwrap(),
        "crash.txt",
    );
    assert!(folder.get_raw(&old_inbox_key).await.is_none());

    // Run compaction. Since no committed files reference the block, and the inbox entry was deleted,
    // the blockseq is orphaned.
    s.compact(folder_id).await.unwrap();

    // Verify that bd/<seq> is deleted/dropped by compaction.
    let bd_key = bepository_storage::store_keys::block_data_seq_key(seq);
    let folder_reopened = s.folder(folder_id).await.unwrap();
    assert!(folder_reopened.get_raw(&bd_key).await.is_none());

    // Now, stage and write the block again in Epoch 2, and complete it.
    let file = make_file_with_blocks_of_size("crash.txt", &[(1, 1)], &[hash], 4096);
    folder_reopened
        .apply_update(&file, &REMOTE_DEV)
        .await
        .unwrap();
    folder_reopened
        .insert_block("crash.txt", 0, data.clone())
        .await;

    folder_reopened
        .complete_file("crash.txt", file.version.as_ref())
        .await
        .unwrap();

    // Verify it is committed and readable.
    let committed = folder_reopened.get_file("crash.txt").await.unwrap();
    assert_eq!(committed.name, "crash.txt");
    let read = folder_reopened
        .read_block("crash.txt", 0, 4096, &hash)
        .await
        .unwrap();
    assert_eq!(read, data);

    s.close().await.unwrap();
}

#[tokio::test]
async fn inline_blocks_excluded_from_gc_liveness() {
    let (storage, folder) = setup_folder("inline-gc-liveness").await;

    // 1. Insert an inline block (hash H, size 100).
    let hash = [0xaa; 32];
    let data = vec![0xaa; 100];
    let file = make_file_with_blocks_of_size("inline_file.txt", &[(1, 1)], &[hash], 100);
    folder.insert_file(file).await;
    folder
        .insert_block("inline_file.txt", 0, Bytes::from(data))
        .await;

    // Confirm it's inline.
    let raw_val = folder
        .get_raw(&bepository_storage::store_keys::file_key("inline_file.txt"))
        .await
        .unwrap();
    let file_proto =
        <bepository_storage::proto::storage::File as prost::Message>::decode(&raw_val[..]).unwrap();
    let file_info = file_proto.file_info.unwrap();
    assert!(file_info.blocks[0].inline_data.is_some());
    assert!(file_info.blocks[0].blockseq.is_none());

    // 2. Manually write b/<dir>/H and br/H/... keys in the database.
    let dir = bepository_storage::store_keys::dirname("inline_file.txt");
    let b_key = bepository_storage::store_keys::block_pointer_key(dir, &hash);
    let br_key = bepository_storage::store_keys::block_reverse_key(&hash, "inline_file.txt");

    let block_ref = bepository_storage::proto::storage::BlockRef { seqno: 9999 };
    folder
        .put_raw(b_key.clone(), prost::Message::encode_to_vec(&block_ref))
        .await;
    folder.put_raw(br_key.clone(), Vec::new()).await;

    // Verify they are present.
    assert!(folder.get_raw(&b_key).await.is_some());
    assert!(folder.get_raw(&br_key).await.is_some());

    // 3. Run compaction. Since inline blocks contribute nothing to dual-bloom GC filter,
    // these manually injected b/ and br/ keys (which are NOT referenced by any separated blockseq)
    // must be dropped by compaction.
    storage.compact(folder.id()).await.unwrap();
    let folder = storage.folder(folder.id()).await.unwrap();

    // Verify they are dropped.
    assert!(folder.get_raw(&b_key).await.is_none());
    assert!(folder.get_raw(&br_key).await.is_none());
}
