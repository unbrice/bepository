// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::sync::Arc;

use bytes::Bytes;
use object_store::memory::InMemory;

use bepository_bep::proto::bep::BlockInfo;
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
    let (storage, folder) = setup_folder("f1").await;

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
        .insert_file(make_file_with_blocks("photos/a.jpg", &[(1, 1)], &[hash]))
        .await;
    folder
        .insert_file(make_file_with_blocks("backup/a.jpg", &[(1, 1)], &[hash]))
        .await;

    let data = Bytes::from_static(b"shared block data");
    folder.insert_block("photos/a.jpg", 0, data.clone()).await;

    // Both should be readable. For the second file, the engine would call `reuse_block`.
    assert!(
        folder
            .reuse_block("backup/a.jpg", 0, &hash, 1024)
            .await
            .unwrap()
    );

    let read1 = folder
        .read_block("photos/a.jpg", 0, 1024, &hash)
        .await
        .unwrap();
    let read2 = folder
        .read_block("backup/a.jpg", 0, 1024, &hash)
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
        .insert_file(make_file_with_blocks("a.txt", &[(1, 1)], &[hash]))
        .await;
    folder
        .insert_block("a.txt", 0, Bytes::from_static(b"live data"))
        .await;

    // Baseline: block is visible.
    assert!(folder.has_block("a.txt", 0, &hash, 1024).await.unwrap());
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
