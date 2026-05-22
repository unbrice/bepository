// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

use object_store::memory::InMemory;
use prost::Message;
use slatedb::Db;
use std::sync::Arc;
use tokio::sync::Mutex;

use bepository_storage::SeqAllocator;
use bepository_storage::proto::storage::FolderIndexMeta;
use bepository_storage::store_keys;

async fn make_db() -> Arc<Db> {
    let object_store = Arc::new(InMemory::new());
    let db = Db::builder("test_folder".to_string(), object_store)
        .build()
        .await
        .unwrap();
    Arc::new(db)
}

#[tokio::test]
async fn test_blockseq_floor_and_reopen() {
    let object_store = Arc::new(InMemory::new());
    let path = "test_folder".to_string();

    // 1. Initial run on empty DB: first allocation must be >= 1024
    let db = Db::builder(path.clone(), object_store.clone())
        .build()
        .await
        .unwrap();
    let db = Arc::new(db);
    let seq_lock = Arc::new(Mutex::new(()));

    let allocator = SeqAllocator::load(db.clone(), seq_lock.clone())
        .await
        .unwrap();

    let first = allocator.allocate().await.unwrap();
    assert!(
        first >= store_keys::MIN_BLOCK_SEQ,
        "first blockseq should be >= {}, got {first}",
        store_keys::MIN_BLOCK_SEQ
    );

    // Allocate a few more
    for i in 1..10 {
        let seq = allocator.allocate().await.unwrap();
        assert_eq!(seq, first + i);
    }

    // Close DB
    db.close().await.unwrap();

    // 2. Reopen DB: must preserve and continue sequence allocation without reset
    let db2 = Db::builder(path, object_store).build().await.unwrap();
    let db2 = Arc::new(db2);
    let allocator2 = SeqAllocator::load(db2.clone(), seq_lock.clone())
        .await
        .unwrap();

    let next_seq = allocator2.allocate().await.unwrap();
    // Since RESERVATION_N is 256, the first run reserved [1024, 1280).
    // The next run's first allocation should start at 1280.
    assert!(
        next_seq >= first + 256,
        "reopened allocation should start at next reservation bound, got {next_seq}"
    );

    db2.close().await.unwrap();
}

#[tokio::test]
async fn test_blockseq_concurrency() {
    let db = make_db().await;
    let seq_lock = Arc::new(Mutex::new(()));
    let allocator = Arc::new(SeqAllocator::load(db.clone(), seq_lock).await.unwrap());

    let num_tasks = 8;
    let allocations_per_task = 100;

    let mut handles = vec![];
    for _ in 0..num_tasks {
        let allocator = allocator.clone();
        handles.push(tokio::spawn(async move {
            let mut results = vec![];
            for _ in 0..allocations_per_task {
                let seq = allocator.allocate().await.unwrap();
                results.push(seq);
            }
            results
        }));
    }

    let mut all_seqs = vec![];
    for handle in handles {
        let res = handle.await.unwrap();
        all_seqs.extend(res);
    }

    assert_eq!(all_seqs.len(), num_tasks * allocations_per_task);

    // Ensure all sequence numbers are unique
    let mut sorted_seqs = all_seqs.clone();
    sorted_seqs.sort_unstable();
    let len_before = sorted_seqs.len();
    sorted_seqs.dedup();
    assert_eq!(
        len_before,
        sorted_seqs.len(),
        "duplicate sequence numbers detected"
    );

    // Ensure floor holds
    for seq in &all_seqs {
        assert!(
            *seq >= store_keys::MIN_BLOCK_SEQ,
            "sequence number {seq} is less than {}",
            store_keys::MIN_BLOCK_SEQ
        );
    }
}

#[tokio::test]
async fn test_blockseq_durability() {
    let db = make_db().await;
    let seq_lock = Arc::new(Mutex::new(()));
    let allocator = SeqAllocator::load(db.clone(), seq_lock).await.unwrap();

    // Perform several allocations
    for _ in 0..10 {
        let seq = allocator.allocate().await.unwrap();

        // Read directly from DB to verify that the persisted ix.next_blockseq strictly exceeds the allocated sequence
        let ix_bytes = db.get(store_keys::IX_KEY).await.unwrap().unwrap();
        let meta = FolderIndexMeta::decode(ix_bytes).unwrap();
        let persisted_limit = meta.next_blockseq.unwrap();

        assert!(
            seq < persisted_limit,
            "allocated seq {seq} must be less than persisted limit {persisted_limit}"
        );
    }
}

#[tokio::test]
async fn test_blockseq_crash_safety() {
    let object_store = Arc::new(InMemory::new());
    let path = "crash_folder".to_string();
    let seq_lock = Arc::new(Mutex::new(()));

    // 1. First run: open, allocate a few
    let db = Db::builder(path.clone(), object_store.clone())
        .build()
        .await
        .unwrap();
    let db = Arc::new(db);
    let allocator = SeqAllocator::load(db.clone(), seq_lock.clone())
        .await
        .unwrap();

    let seq1 = allocator.allocate().await.unwrap();
    assert_eq!(seq1, store_keys::MIN_BLOCK_SEQ);

    // Simulate sudden crash by dropping the Db and allocator without closing
    drop(allocator);
    drop(db);

    // 2. Second run: reopen DB and allocate again.
    // It must NEVER reuse any sequence number that was handed out previously.
    // Since the first run reserved [1024, 1280) in the DB, the next run should
    // start at least from 1280.
    let db2 = Db::builder(path, object_store).build().await.unwrap();
    let db2 = Arc::new(db2);
    let allocator2 = SeqAllocator::load(db2.clone(), seq_lock).await.unwrap();

    let seq2 = allocator2.allocate().await.unwrap();
    assert!(
        seq2 >= store_keys::MIN_BLOCK_SEQ + 256,
        "crash safety: sequence number {seq2} must be >= {} (no reuse)",
        store_keys::MIN_BLOCK_SEQ + 256
    );
}
