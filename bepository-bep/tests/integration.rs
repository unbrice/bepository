// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

#![allow(
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

//! Integration tests for the BEP protocol library.
//!
//! These tests spin up two `BepEngine` instances connected via in-memory
//! duplex streams and verify end-to-end protocol behaviour.
//!
//! Run with:
//! ```text
//! cargo test -p bepository-bep --features test-utils --test integration
//! ```

use std::collections::HashSet;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use bepository_bep::fault::{FaultStorage, FaultStream, StorageMethod, StreamMethod};
use bepository_bep::storage::StorageInspectorForTests;
use bepository_bep::test_utils::{
    BackupResolver, MemoryStorage, TEST_BLOCK_SIZE, make_device, make_file_with_blocks,
};
use bepository_bep::{
    BepEngine, BepError, CloseReason, ConnectionOptions, DeviceId, EngineEvent, FolderId,
    ImmediateRetry, Storage, StorageError, StorageFolder,
};
use bytes::Bytes;

fn resolver() -> Arc<dyn bepository_bep::ConflictResolver> {
    Arc::new(BackupResolver)
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("bepository_bep=debug")
        .with_test_writer()
        .try_init();
}

// ---------------------------------------------------------------------------
// Smoke: two engines connect, exchange hellos + cluster config, then shutdown
// ---------------------------------------------------------------------------

#[tokio::test]
async fn two_engines_connect_and_shutdown() {
    init_tracing();

    let dev_a = make_device(1);
    let dev_b = make_device(2);

    let engine_a = BepEngine::new(
        MemoryStorage::new(),
        dev_a,
        "node-a".into(),
        vec!["shared".into()],
        resolver(),
    );
    let engine_b = BepEngine::new(
        MemoryStorage::new(),
        dev_b,
        "node-b".into(),
        vec!["shared".into()],
        resolver(),
    );

    let (stream_a, stream_b) = tokio::io::duplex(64 * 1024);

    let handle_a = engine_a.connect(stream_a, dev_b).await.unwrap();
    let handle_b = engine_b.accept(stream_b, dev_a).await.unwrap();

    // Allow protocol exchange to proceed.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Both engines should see one peer.
    assert_eq!(engine_a.peers().len(), 1);
    assert_eq!(engine_b.peers().len(), 1);

    // Request graceful shutdown from A's side.
    handle_a.shutdown.cancel();

    // B should observe the close.
    let reason = tokio::time::timeout(Duration::from_secs(5), handle_b.closed)
        .await
        .expect("should not time out")
        .expect("channel should deliver");

    // B sees it as a remote close (A sent a Close message).
    assert!(
        matches!(reason, CloseReason::Remote(_)),
        "expected Remote close, got {reason:?}"
    );
}

// ---------------------------------------------------------------------------
// Index exchange: A has files, B should receive them via apply_update
// ---------------------------------------------------------------------------

#[tokio::test]
async fn index_exchange_applies_to_peer() {
    init_tracing();

    let dev_a = make_device(1);
    let dev_b = make_device(2);

    let storage_a = MemoryStorage::new();
    // Seed A with two files.
    let f_a = storage_a.folder(FolderId::from("shared")).await.unwrap();
    f_a.insert_file(make_file_with_blocks("hello.txt", &[(1, 1)], &[]))
        .await;
    f_a.insert_file(make_file_with_blocks("world.txt", &[(1, 2)], &[]))
        .await;

    let engine_a = BepEngine::new(
        storage_a,
        dev_a,
        "node-a".into(),
        vec!["shared".into()],
        resolver(),
    );

    let storage_b = MemoryStorage::new();
    let f_b = storage_b.folder(FolderId::from("shared")).await.unwrap();
    let engine_b = BepEngine::new(
        storage_b,
        dev_b,
        "node-b".into(),
        vec!["shared".into()],
        resolver(),
    );

    let (stream_a, stream_b) = tokio::io::duplex(64 * 1024);
    let handle_a = engine_a.connect(stream_a, dev_b).await.unwrap();
    let handle_b = engine_b.accept(stream_b, dev_a).await.unwrap();

    // Wait for index exchange to complete.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // If the protocol completed without error, both connections are still alive.
    assert_eq!(engine_a.peers().len(), 1);
    assert_eq!(engine_b.peers().len(), 1);

    // B's storage should now contain the files from A's index with correct versions.
    let hello = f_b
        .get_file("hello.txt")
        .await
        .expect("B should have received hello.txt from A's index");
    let world = f_b
        .get_file("world.txt")
        .await
        .expect("B should have received world.txt from A's index");

    // Verify version vectors were preserved through the exchange.
    let hello_ver = hello.version.as_ref().expect("hello should have version");
    assert_eq!(hello_ver.counters.len(), 1);
    assert_eq!(hello_ver.counters[0].id, 1);
    assert_eq!(hello_ver.counters[0].value, 1);

    let world_ver = world.version.as_ref().expect("world should have version");
    assert_eq!(world_ver.counters[0].value, 2);

    // Shutdown cleanly.
    handle_a.shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle_b.closed).await;
}

// ---------------------------------------------------------------------------
// Block request/response: A has blocks, B requests them after receiving index
// ---------------------------------------------------------------------------

#[tokio::test]
async fn block_requests_are_served() {
    init_tracing();

    let dev_a = make_device(1);
    let dev_b = make_device(2);

    let storage_a = MemoryStorage::new();
    let f_a = storage_a.folder(FolderId::from("shared")).await.unwrap();
    let block_data = Bytes::from_static(b"the quick brown fox");
    let block_hash: [u8; 32] = *b"fakehash________________________";

    // Seed A with a file that has one block.
    f_a.insert_file(make_file_with_blocks("fox.txt", &[(1, 1)], &[block_hash]))
        .await;
    f_a.insert_block("fox.txt", 0, block_data.clone()).await;

    let storage_b = MemoryStorage::new();
    let f_b = storage_b.folder(FolderId::from("shared")).await.unwrap();

    let engine_a = BepEngine::new(
        storage_a,
        dev_a,
        "node-a".into(),
        vec!["shared".into()],
        resolver(),
    );
    let engine_b = BepEngine::new(
        storage_b,
        dev_b,
        "node-b".into(),
        vec!["shared".into()],
        resolver(),
    );

    let (stream_a, stream_b) = tokio::io::duplex(64 * 1024);
    let handle_a = engine_a.connect(stream_a, dev_b).await.unwrap();
    let handle_b = engine_b.accept(stream_b, dev_a).await.unwrap();

    // Wait for index exchange + block request/response cycle.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Both sides should still be connected (no protocol errors).
    assert_eq!(engine_a.peers().len(), 1, "A should still have peer");
    assert_eq!(engine_b.peers().len(), 1, "B should still have peer");

    // B should have stored the block data received from A.
    let stored_block = f_b.get_block("fox.txt", 0).await;
    assert_eq!(
        stored_block.as_deref(),
        Some(block_data.as_ref()),
        "B should have persisted the block data from A"
    );

    // Clean shutdown.
    handle_b.shutdown.cancel();
    let reason = tokio::time::timeout(Duration::from_secs(5), handle_a.closed)
        .await
        .expect("should not time out")
        .expect("channel should deliver");
    assert!(
        matches!(reason, CloseReason::Remote(_)),
        "expected CloseReason::Remote, got: {reason:?}"
    );
}

// ---------------------------------------------------------------------------
// No shared folders: engines connect but exchange empty indexes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn auto_accepts_peer_folders() {
    init_tracing();

    let dev_a = make_device(1);
    let dev_b = make_device(2);

    let storage_a = MemoryStorage::new();
    // Seed A with a file in folder-a (metadata + block data).
    let f_a = storage_a.folder(FolderId::from("folder-a")).await.unwrap();
    f_a.insert_file(make_file_with_blocks("secret.txt", &[(1, 1)], &[[1u8; 32]]))
        .await;
    f_a.insert_block("secret.txt", 0, Bytes::from_static(b"secret content"))
        .await;

    // B starts with no pre-configured folders — it should auto-accept folder-a from A.
    let storage_b = MemoryStorage::new();

    let engine_a = BepEngine::new(
        storage_a,
        dev_a,
        "node-a".into(),
        vec!["folder-a".into()],
        resolver(),
    );
    let engine_b = BepEngine::new(
        storage_b.clone(),
        dev_b,
        "node-b".into(),
        vec![],
        resolver(),
    );

    let (stream_a, stream_b) = tokio::io::duplex(64 * 1024);
    let handle_a = engine_a.connect(stream_a, dev_b).await.unwrap();
    let handle_b = engine_b.accept(stream_b, dev_a).await.unwrap();

    // Allow time for ClusterConfig + Index exchange to complete.
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(engine_a.peers().len(), 1);
    assert_eq!(engine_b.peers().len(), 1);

    // B should have auto-accepted folder-a and received A's file.
    let f_a_b = storage_b.folder(FolderId::from("folder-a")).await.unwrap();
    assert!(
        f_a_b.get_file("secret.txt").await.is_some(),
        "B should have received secret.txt from A via auto-accepted folder-a"
    );

    handle_a.shutdown.cancel();
    handle_b.shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Connection drops when the underlying stream closes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stream_drop_disconnects() {
    init_tracing();

    let dev_a = make_device(1);
    let engine_a = BepEngine::new(
        MemoryStorage::new(),
        dev_a,
        "node-a".into(),
        vec!["shared".into()],
        resolver(),
    );

    let (stream_a, stream_b) = tokio::io::duplex(64 * 1024);
    let handle_a = engine_a.connect(stream_a, make_device(2)).await.unwrap();

    // Drop B's end of the stream without ever running a protocol on it.
    // A should see an I/O error and close.
    drop(stream_b);

    let reason = tokio::time::timeout(Duration::from_secs(5), handle_a.closed)
        .await
        .expect("should not time out")
        .expect("channel should deliver");
    assert!(
        matches!(reason, CloseReason::Error(_)),
        "expected Error close, got {reason:?}"
    );

    // Peer should be removed from the engine.
    assert_eq!(engine_a.peers().len(), 0);
}

// ---------------------------------------------------------------------------
// Event handler rejects unknown devices
// ---------------------------------------------------------------------------

#[tokio::test]
async fn event_handler_rejects_device() {
    init_tracing();

    let dev_a = make_device(1);
    let dev_b = make_device(2);
    let dev_c = make_device(3);

    let mut engine_a = BepEngine::new(
        MemoryStorage::new(),
        dev_a,
        "node-a".into(),
        vec!["shared".into()],
        resolver(),
    );

    let mut events = engine_a.take_event_receiver().unwrap();

    // Only allow device C.
    let allowed: HashSet<DeviceId> = [dev_c].into_iter().collect();
    tokio::spawn(async move {
        while let Some(evt) = events.recv().await {
            if let EngineEvent::DeviceConnecting { device, respond } = evt {
                let _ = respond.send(allowed.contains(&device));
            }
        }
    });

    let (stream_a, _stream_b) = tokio::io::duplex(64 * 1024);
    let err = engine_a
        .connect(stream_a, dev_b)
        .await
        .err()
        .expect("should reject");
    assert!(matches!(err, BepError::DeviceRejected), "got {err}");
}

// ---------------------------------------------------------------------------
// Event handler accepts known devices
// ---------------------------------------------------------------------------

#[tokio::test]
async fn event_handler_accepts_device() {
    init_tracing();

    let dev_a = make_device(1);
    let dev_b = make_device(2);

    let mut engine_a = BepEngine::new(
        MemoryStorage::new(),
        dev_a,
        "node-a".into(),
        vec!["shared".into()],
        resolver(),
    );

    let mut events = engine_a.take_event_receiver().unwrap();

    // Accept all devices.
    tokio::spawn(async move {
        while let Some(evt) = events.recv().await {
            if let EngineEvent::DeviceConnecting { respond, .. } = evt {
                let _ = respond.send(true);
            }
        }
    });

    let (stream_a, _stream_b) = tokio::io::duplex(64 * 1024);
    assert!(engine_a.connect(stream_a, dev_b).await.is_ok());
}

// ---------------------------------------------------------------------------
// Event bus emits DeviceConnecting and DeviceDisconnected
// ---------------------------------------------------------------------------

#[tokio::test]
async fn event_bus_lifecycle() {
    init_tracing();

    let dev_a = make_device(1);
    let dev_b = make_device(2);

    let mut engine_a = BepEngine::new(
        MemoryStorage::new(),
        dev_a,
        "node-a".into(),
        vec!["shared".into()],
        resolver(),
    );
    let engine_b = BepEngine::new(
        MemoryStorage::new(),
        dev_b,
        "node-b".into(),
        vec!["shared".into()],
        resolver(),
    );

    let mut events = engine_a
        .take_event_receiver()
        .expect("first call returns Some");
    assert!(
        engine_a.take_event_receiver().is_none(),
        "second call returns None"
    );

    // Spawn connect in a task since it blocks waiting for event response.
    let dev_b_clone = dev_b;
    let (stream_a, stream_b) = tokio::io::duplex(64 * 1024);
    let connect_task = tokio::spawn(async move { engine_a.connect(stream_a, dev_b_clone).await });

    // Should receive DeviceConnecting — accept it.
    let evt = tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await
        .expect("timeout")
        .expect("channel open");
    match evt {
        EngineEvent::DeviceConnecting {
            ref device,
            respond,
        } => {
            assert_eq!(*device, dev_b);
            respond.send(true).unwrap();
        }
        other => panic!("expected DeviceConnecting, got {other:?}"),
    }

    let handle_a = connect_task.await.unwrap().unwrap();
    let _handle_b = engine_b.accept(stream_b, dev_a).await.unwrap();

    // Allow protocol exchange.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Trigger disconnect.
    handle_a.shutdown.cancel();

    // Should receive DeviceDisconnected.
    let evt = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("timeout")
        .expect("channel open");
    assert!(
        matches!(&evt, EngineEvent::DeviceDisconnected { device, .. } if *device == dev_b),
        "expected DeviceDisconnected, got {evt:?}"
    );
}

// ---------------------------------------------------------------------------
// Bidirectional index exchange: both sides have files, both receive the other's
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bidirectional_index_exchange() {
    init_tracing();

    let dev_a = make_device(1);
    let dev_b = make_device(2);

    let storage_a = MemoryStorage::new();
    let f_a = storage_a.folder(FolderId::from("shared")).await.unwrap();
    f_a.insert_file(make_file_with_blocks("from-a.txt", &[(1, 1)], &[]))
        .await;

    let storage_b = MemoryStorage::new();
    let f_b = storage_b.folder(FolderId::from("shared")).await.unwrap();
    f_b.insert_file(make_file_with_blocks("from-b.txt", &[(2, 1)], &[]))
        .await;

    let engine_a = BepEngine::new(
        storage_a,
        dev_a,
        "node-a".into(),
        vec!["shared".into()],
        resolver(),
    );
    let engine_b = BepEngine::new(
        storage_b,
        dev_b,
        "node-b".into(),
        vec!["shared".into()],
        resolver(),
    );

    let (stream_a, stream_b) = tokio::io::duplex(64 * 1024);
    let handle_a = engine_a.connect(stream_a, dev_b).await.unwrap();
    let handle_b = engine_b.accept(stream_b, dev_a).await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // A should have received B's file.
    assert!(
        f_a.get_file("from-b.txt").await.is_some(),
        "A should have received from-b.txt"
    );
    // B should have received A's file.
    assert!(
        f_b.get_file("from-a.txt").await.is_some(),
        "B should have received from-a.txt"
    );

    handle_a.shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle_b.closed).await;
}

// ---------------------------------------------------------------------------
// Conflict resolution: both sides have concurrent versions of the same file
// ---------------------------------------------------------------------------

#[tokio::test]
async fn conflict_resolution_during_index_exchange() {
    init_tracing();

    let dev_a = make_device(1);
    let dev_b = make_device(2);

    // Both sides have the same file with concurrent versions (different counter IDs).
    let storage_a = MemoryStorage::new();
    storage_a
        .folder(FolderId::from("shared"))
        .await
        .unwrap()
        .insert_file(make_file_with_blocks("doc.txt", &[(1, 5)], &[]))
        .await;
    let storage_b = MemoryStorage::new();
    let f_b = storage_b.folder(FolderId::from("shared")).await.unwrap();
    f_b.insert_file(make_file_with_blocks("doc.txt", &[(2, 3)], &[]))
        .await;

    let engine_a = BepEngine::new(
        storage_a,
        dev_a,
        "node-a".into(),
        vec!["shared".into()],
        resolver(),
    );
    let engine_b = BepEngine::new(
        storage_b,
        dev_b,
        "node-b".into(),
        vec!["shared".into()],
        resolver(),
    );

    let (stream_a, stream_b) = tokio::io::duplex(64 * 1024);
    let handle_a = engine_a.connect(stream_a, dev_b).await.unwrap();
    let handle_b = engine_b.accept(stream_b, dev_a).await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Both connections should still be alive (conflict was resolved, not an error).
    assert_eq!(engine_a.peers().len(), 1, "A still connected");
    assert_eq!(engine_b.peers().len(), 1, "B still connected");

    // At least one side should have a conflict file.
    // BackupResolver: larger total version wins.  A has total 5, B has total 3.
    // So when B processes A's index: A's version (counter 1, value 5) vs B's local
    // (counter 2, value 3) → concurrent → A wins (larger total). B should have
    // the conflict file for its loser.
    let conflict = f_b.get_file("doc.txt.sync-conflict").await;
    assert!(
        conflict.is_some(),
        "B should have created a conflict file for the loser"
    );

    // The winner (A's version) should be at the original path on B.
    let winner = f_b.get_file("doc.txt").await.unwrap();
    let winner_ver = winner.version.as_ref().unwrap();
    assert_eq!(
        winner_ver.counters[0].value, 5,
        "winner should be A's version (total 5)"
    );

    handle_a.shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle_b.closed).await;
}

// ---------------------------------------------------------------------------
// Block transfer with multiple blocks per file
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multi_block_file_transfer() {
    init_tracing();

    let dev_a = make_device(1);
    let dev_b = make_device(2);

    let storage_a = MemoryStorage::new();
    let f_a = storage_a.folder(FolderId::from("shared")).await.unwrap();
    let block1 = Bytes::from_static(b"first block data");
    let block2 = Bytes::from_static(b"second block data");
    let hash1: [u8; 32] = *b"hash1___________________________"; // 32 bytes
    let hash2: [u8; 32] = *b"hash2___________________________";

    f_a.insert_file(make_file_with_blocks(
        "multi.bin",
        &[(1, 1)],
        &[hash1, hash2],
    ))
    .await;
    f_a.insert_block("multi.bin", 0, block1.clone()).await;
    f_a.insert_block("multi.bin", TEST_BLOCK_SIZE as i64, block2.clone())
        .await;

    let storage_b = MemoryStorage::new();
    let f_b = storage_b.folder(FolderId::from("shared")).await.unwrap();

    let engine_a = BepEngine::new(
        storage_a,
        dev_a,
        "node-a".into(),
        vec!["shared".into()],
        resolver(),
    );
    let engine_b = BepEngine::new(
        storage_b,
        dev_b,
        "node-b".into(),
        vec!["shared".into()],
        resolver(),
    );

    let (stream_a, stream_b) = tokio::io::duplex(64 * 1024);
    let handle_a = engine_a.connect(stream_a, dev_b).await.unwrap();
    let handle_b = engine_b.accept(stream_b, dev_a).await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Both blocks should have been transferred and stored.
    let got1 = f_b.get_block("multi.bin", 0).await;
    let got2 = f_b.get_block("multi.bin", TEST_BLOCK_SIZE as i64).await;
    assert_eq!(
        got1.as_deref(),
        Some(block1.as_ref()),
        "first block should be stored on B"
    );
    assert_eq!(
        got2.as_deref(),
        Some(block2.as_ref()),
        "second block should be stored on B"
    );

    handle_a.shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle_b.closed).await;
}

// ---------------------------------------------------------------------------
// Deferred blocks: all blocks transfer even when max_pending_requests is tiny
// ---------------------------------------------------------------------------

#[tokio::test]
async fn deferred_blocks_are_eventually_sent() {
    init_tracing();

    let dev_a = make_device(1);
    let dev_b = make_device(2);

    let storage_a = MemoryStorage::new();
    let f_a = storage_a.folder(FolderId::from("shared")).await.unwrap();
    let blocks_data: Vec<(Bytes, [u8; 32])> = (0..4)
        .map(|i| {
            let data = Bytes::from(format!("block-data-{i}"));
            let mut hash = [0u8; 32];
            hash[0] = i as u8;
            (data, hash)
        })
        .collect();

    let block_hashes: Vec<[u8; 32]> = blocks_data.iter().map(|(_, hash)| *hash).collect();

    f_a.insert_file(make_file_with_blocks("big.bin", &[(1, 1)], &block_hashes))
        .await;
    for (i, (data, _hash)) in blocks_data.iter().enumerate() {
        let offset = i as i64 * TEST_BLOCK_SIZE as i64;
        f_a.insert_block("big.bin", offset, data.clone()).await;
    }

    let storage_b = MemoryStorage::new();
    let f_b = storage_b.folder(FolderId::from("shared")).await.unwrap();

    let engine_a = BepEngine::new(
        storage_a,
        dev_a,
        "node-a".into(),
        vec!["shared".into()],
        resolver(),
    );
    let mut engine_b = BepEngine::new(
        storage_b,
        dev_b,
        "node-b".into(),
        vec!["shared".into()],
        resolver(),
    );

    // Only allow 1 in-flight request at a time — forces deferral.
    engine_b.set_connection_options(ConnectionOptions {
        max_pending_requests: 1,
        ..Default::default()
    });

    let (stream_a, stream_b) = tokio::io::duplex(64 * 1024);
    let handle_a = engine_a.connect(stream_a, dev_b).await.unwrap();
    let handle_b = engine_b.accept(stream_b, dev_a).await.unwrap();

    tokio::time::sleep(Duration::from_millis(1000)).await;

    // ALL 4 blocks should have been transferred despite max_pending_requests=1.
    for (i, (data, _hash)) in blocks_data.iter().enumerate() {
        let offset = i as i64 * TEST_BLOCK_SIZE as i64;
        let got = f_b.get_block("big.bin", offset).await;
        assert_eq!(
            got.as_deref(),
            Some(data.as_ref()),
            "block at offset {offset} should have been transferred"
        );
    }

    // File should be promoted to committed index (complete_file called).
    assert!(
        f_b.get_file("big.bin").await.is_some(),
        "file should be promoted to committed index after all blocks arrive"
    );

    handle_a.shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle_b.closed).await;
}

// ---------------------------------------------------------------------------
// Two-phase intake: file appears in committed index only after all blocks
// ---------------------------------------------------------------------------

#[tokio::test]
async fn complete_file_called_after_all_blocks() {
    init_tracing();

    let dev_a = make_device(1);
    let dev_b = make_device(2);

    let storage_a = MemoryStorage::new();
    let f_a = storage_a.folder(FolderId::from("shared")).await.unwrap();
    let block_data = Bytes::from_static(b"complete-test-data");
    let block_hash: [u8; 32] = *b"completehash____________________"; // 32 bytes

    f_a.insert_file(make_file_with_blocks("done.txt", &[(1, 1)], &[block_hash]))
        .await;
    f_a.insert_block("done.txt", 0, block_data.clone()).await;

    let storage_b = MemoryStorage::new();
    let f_b = storage_b.folder(FolderId::from("shared")).await.unwrap();

    let engine_a = BepEngine::new(
        storage_a,
        dev_a,
        "node-a".into(),
        vec!["shared".into()],
        resolver(),
    );
    let engine_b = BepEngine::new(
        storage_b,
        dev_b,
        "node-b".into(),
        vec!["shared".into()],
        resolver(),
    );

    let (stream_a, stream_b) = tokio::io::duplex(64 * 1024);
    let handle_a = engine_a.connect(stream_a, dev_b).await.unwrap();
    let handle_b = engine_b.accept(stream_b, dev_a).await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // File should be in committed index (not just inbox).
    let committed = f_b.get_file("done.txt").await;
    assert!(
        committed.is_some(),
        "file should be in committed index after block transfer"
    );
    assert_eq!(
        committed.unwrap().version.as_ref().unwrap().counters[0].value,
        1
    );

    // Inbox should be empty.
    assert!(
        f_b.get_inbox_file((), "done.txt").await.is_none(),
        "inbox should be empty after promotion"
    );

    handle_a.shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle_b.closed).await;
}

// ---------------------------------------------------------------------------
// Fault wrappers as pure passthroughs — verifies transparency (no faults set)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fault_wrappers_are_transparent() {
    init_tracing();

    let dev_a = make_device(1);
    let dev_b = make_device(2);

    // Wrap storage with FaultStorage — no faults configured.
    let (storage_a, _config_a) = FaultStorage::new(MemoryStorage::new());
    let (storage_b, _config_b) = FaultStorage::new(MemoryStorage::new());

    let block_hash: [u8; 32] = *b"fakehash________________________";
    let block_data = bytes::Bytes::from_static(b"the quick brown fox");

    let f_a = storage_a.folder(FolderId::from("shared")).await.unwrap();
    f_a.insert_file(make_file_with_blocks("fox.txt", &[(1, 1)], &[block_hash]))
        .await;
    f_a.insert_block("fox.txt", 0, block_data.clone()).await;

    let f_b = storage_b.folder(FolderId::from("shared")).await.unwrap();

    let engine_a = BepEngine::new(
        storage_a,
        dev_a,
        "node-a".into(),
        vec!["shared".into()],
        resolver(),
    );
    let engine_b = BepEngine::new(
        storage_b,
        dev_b,
        "node-b".into(),
        vec!["shared".into()],
        resolver(),
    );

    // Wrap streams with FaultStream — no faults configured.
    let (raw_a, raw_b) = tokio::io::duplex(64 * 1024);
    let (fault_a, _stream_config_a) = FaultStream::new(raw_a);
    let (fault_b, _stream_config_b) = FaultStream::new(raw_b);

    let handle_a = engine_a.connect(fault_a, dev_b).await.unwrap();
    let handle_b = engine_b.accept(fault_b, dev_a).await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Both peers still connected — no spurious errors from the wrappers.
    assert_eq!(engine_a.peers().len(), 1, "A should still have peer");
    assert_eq!(engine_b.peers().len(), 1, "B should still have peer");

    // Block transfer worked through the fault wrappers.
    let got = f_b.get_block("fox.txt", 0).await;
    assert_eq!(
        got.as_deref(),
        Some(block_data.as_ref()),
        "block should transfer through fault wrappers"
    );

    handle_a.shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle_b.closed).await;
}

// ---------------------------------------------------------------------------
// Fault injection tests — each verifies one recovery path end-to-end.
// ---------------------------------------------------------------------------

// Recovery path: Storage TransientIo on StoreBlock → engine retries, block arrives.
#[tokio::test]
async fn storage_transient_io_on_store_block() {
    init_tracing();

    let dev_a = make_device(1);
    let dev_b = make_device(2);

    let block_hash: [u8; 32] = *b"hash_transient__________________";
    let block_data = Bytes::from_static(b"transient-io test payload");

    let (storage_a, _fault_a) = FaultStorage::new(MemoryStorage::new());
    let f_a = storage_a.folder(FolderId::from("shared")).await.unwrap();
    f_a.insert_file(make_file_with_blocks("data.bin", &[(1, 1)], &[block_hash]))
        .await;
    f_a.insert_block("data.bin", 0, block_data.clone()).await;

    let (storage_b, fault_b) = FaultStorage::new(MemoryStorage::new());
    let f_b = storage_b.folder(FolderId::from("shared")).await.unwrap();

    // Inject 3 transient failures on StoreBlock: the engine must retry and eventually succeed.
    fault_b.set(
        StorageMethod::StoreBlock,
        NonZeroU32::new(3).unwrap(),
        StorageError::TransientIo("injected".into()),
    );

    let engine_a = BepEngine::new(
        storage_a,
        dev_a,
        "node-a".into(),
        vec!["shared".into()],
        resolver(),
    );
    let mut engine_b = BepEngine::new(
        storage_b,
        dev_b,
        "node-b".into(),
        vec!["shared".into()],
        resolver(),
    );
    // Use ImmediateRetry so the test doesn't wait for exponential backoff delays.
    engine_b.set_connection_options(ConnectionOptions {
        retry_policy: Arc::new(ImmediateRetry { max_attempts: 5 }),
        ..ConnectionOptions::default()
    });

    let (stream_a, stream_b) = tokio::io::duplex(64 * 1024);
    let handle_a = engine_a.connect(stream_a, dev_b).await.unwrap();
    let handle_b = engine_b.accept(stream_b, dev_a).await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Retry succeeded: block arrived despite 3 initial failures.
    assert_eq!(
        f_b.get_block("data.bin", 0).await.as_deref(),
        Some(block_data.as_ref()),
        "block should arrive after retries"
    );
    assert_eq!(
        engine_b.peers().len(),
        1,
        "connection still alive after retries"
    );

    handle_a.shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle_b.closed).await;
}

// Recovery path: Stream Read ConnectionReset → connection closes with NetworkError.
#[tokio::test]
async fn stream_read_error_disconnects() {
    init_tracing();

    let dev_a = make_device(1);
    let dev_b = make_device(2);

    let engine_a = BepEngine::new(
        MemoryStorage::new(),
        dev_a,
        "node-a".into(),
        vec!["shared".into()],
        resolver(),
    );
    let engine_b = BepEngine::new(
        MemoryStorage::new(),
        dev_b,
        "node-b".into(),
        vec!["shared".into()],
        resolver(),
    );

    let (raw_a, raw_b) = tokio::io::duplex(64 * 1024);
    let (fault_a, config_a) = FaultStream::new(raw_a);
    let (fault_b, _config_b) = FaultStream::new(raw_b);

    // Inject one read failure on A's stream: A's next read (recv_hello) returns ConnectionReset.
    config_a.set(
        StreamMethod::Read,
        NonZeroU32::MIN,
        std::io::ErrorKind::ConnectionReset,
    );

    let handle_a = engine_a.connect(fault_a, dev_b).await.unwrap();
    let _handle_b = engine_b.accept(fault_b, dev_a).await.unwrap();

    // A's read fault fires during hello exchange → closes with NetworkError.
    let reason_a = tokio::time::timeout(Duration::from_secs(5), handle_a.closed)
        .await
        .expect("A should close within timeout")
        .expect("close reason should be sent");
    assert!(
        matches!(reason_a, CloseReason::Error(BepError::NetworkError(_))),
        "expected NetworkError, got: {reason_a:?}"
    );
    assert_eq!(
        engine_a.peers().len(),
        0,
        "A should have no peers after close"
    );
}

// Recovery path: Stream Write BrokenPipe → peer sees NetworkError, connection closes.
#[tokio::test]
async fn stream_write_error_disconnects() {
    init_tracing();

    let dev_a = make_device(1);
    let dev_b = make_device(2);

    let storage_a = MemoryStorage::new();
    storage_a
        .folder(FolderId::from("shared"))
        .await
        .unwrap()
        .insert_file(make_file_with_blocks("msg.txt", &[(1, 1)], &[]))
        .await;

    let engine_a = BepEngine::new(
        storage_a,
        dev_a,
        "node-a".into(),
        vec!["shared".into()],
        resolver(),
    );
    let engine_b = BepEngine::new(
        MemoryStorage::new(),
        dev_b,
        "node-b".into(),
        vec!["shared".into()],
        resolver(),
    );

    let (raw_a, raw_b) = tokio::io::duplex(64 * 1024);
    let (fault_a, config_a) = FaultStream::new(raw_a);
    let (fault_b, _config_b) = FaultStream::new(raw_b);

    // Inject one write failure on A's stream: A's first write (send_hello) returns BrokenPipe.
    config_a.set(
        StreamMethod::Write,
        NonZeroU32::MIN,
        std::io::ErrorKind::BrokenPipe,
    );

    let handle_a = engine_a.connect(fault_a, dev_b).await.unwrap();
    let _handle_b = engine_b.accept(fault_b, dev_a).await.unwrap();

    // A's write fault fires on send_hello → closes with NetworkError.
    let reason_a = tokio::time::timeout(Duration::from_secs(5), handle_a.closed)
        .await
        .expect("A should close within timeout")
        .expect("close reason should be sent");
    assert!(
        matches!(reason_a, CloseReason::Error(BepError::NetworkError(_))),
        "expected NetworkError, got: {reason_a:?}"
    );
    assert_eq!(
        engine_a.peers().len(),
        0,
        "A should have no peers after close"
    );
}

// Recovery path: Storage Corruption on ApplyUpdate → fatal, connection closes.
#[tokio::test]
async fn storage_corruption_on_apply_update_is_fatal() {
    init_tracing();

    let dev_a = make_device(1);
    let dev_b = make_device(2);

    // A has a file with no blocks so apply_update fires without a StoreBlock call.
    let storage_a = MemoryStorage::new();
    storage_a
        .folder(FolderId::from("shared"))
        .await
        .unwrap()
        .insert_file(make_file_with_blocks("corrupt.txt", &[(1, 1)], &[]))
        .await;

    let (storage_b, fault_b) = FaultStorage::new(MemoryStorage::new());
    let _f_b = storage_b.folder(FolderId::from("shared")).await.unwrap();

    // Corruption is non-retryable: one failure closes the connection immediately.
    fault_b.set(
        StorageMethod::ApplyUpdate,
        NonZeroU32::MIN,
        StorageError::Corruption("injected".into()),
    );

    let engine_a = BepEngine::new(
        storage_a,
        dev_a,
        "node-a".into(),
        vec!["shared".into()],
        resolver(),
    );
    let engine_b = BepEngine::new(
        storage_b,
        dev_b,
        "node-b".into(),
        vec!["shared".into()],
        resolver(),
    );

    let (stream_a, stream_b) = tokio::io::duplex(64 * 1024);
    let _handle_a = engine_a.connect(stream_a, dev_b).await.unwrap();
    let handle_b = engine_b.accept(stream_b, dev_a).await.unwrap();

    // B closes with Corruption error as soon as apply_update is called.
    let reason_b = tokio::time::timeout(Duration::from_secs(5), handle_b.closed)
        .await
        .expect("B should close within timeout")
        .expect("close reason should be sent");
    assert!(
        matches!(reason_b, CloseReason::Error(BepError::Corruption(_))),
        "expected Corruption, got: {reason_b:?}"
    );
    assert_eq!(
        engine_b.peers().len(),
        0,
        "B should have no peers after fatal error"
    );
}

// Malformed wire FileInfo (negative block size) → PeerBadMessage at the Index
// boundary, connection closes before any storage call.
#[tokio::test]
async fn malformed_index_file_info_closes_connection() {
    init_tracing();

    let dev_a = make_device(1);
    let dev_b = make_device(2);

    // A announces a file whose block has a negative size.
    let storage_a = MemoryStorage::new();
    let mut bad = make_file_with_blocks("bad.bin", &[(1, 1)], &[[7; 32]]);
    bad.blocks[0].size = -1;
    storage_a
        .folder(FolderId::from("shared"))
        .await
        .unwrap()
        .insert_file(bad)
        .await;

    let storage_b = MemoryStorage::new();
    let _f_b = storage_b.folder(FolderId::from("shared")).await.unwrap();

    let engine_a = BepEngine::new(
        storage_a,
        dev_a,
        "node-a".into(),
        vec!["shared".into()],
        resolver(),
    );
    let engine_b = BepEngine::new(
        storage_b,
        dev_b,
        "node-b".into(),
        vec!["shared".into()],
        resolver(),
    );

    let (stream_a, stream_b) = tokio::io::duplex(64 * 1024);
    let _handle_a = engine_a.connect(stream_a, dev_b).await.unwrap();
    let handle_b = engine_b.accept(stream_b, dev_a).await.unwrap();

    let reason_b = tokio::time::timeout(Duration::from_secs(5), handle_b.closed)
        .await
        .expect("B should close within timeout")
        .expect("close reason should be sent");
    assert!(
        matches!(reason_b, CloseReason::Error(BepError::PeerBadMessage(_))),
        "expected PeerBadMessage, got: {reason_b:?}"
    );
}

// Recovery path: Storage Internal error → fatal (same close path as Corruption).
#[tokio::test]
async fn storage_internal_error_is_fatal() {
    init_tracing();

    let dev_a = make_device(1);
    let dev_b = make_device(2);

    let storage_a = MemoryStorage::new();
    storage_a
        .folder(FolderId::from("shared"))
        .await
        .unwrap()
        .insert_file(make_file_with_blocks("internal.txt", &[(1, 1)], &[]))
        .await;

    let (storage_b, fault_b) = FaultStorage::new(MemoryStorage::new());
    let _f_b = storage_b.folder(FolderId::from("shared")).await.unwrap();

    // Internal error is non-retryable: one failure closes the connection immediately.
    fault_b.set(
        StorageMethod::ApplyUpdate,
        NonZeroU32::MIN,
        StorageError::Internal("injected".into()),
    );

    let engine_a = BepEngine::new(
        storage_a,
        dev_a,
        "node-a".into(),
        vec!["shared".into()],
        resolver(),
    );
    let engine_b = BepEngine::new(
        storage_b,
        dev_b,
        "node-b".into(),
        vec!["shared".into()],
        resolver(),
    );

    let (stream_a, stream_b) = tokio::io::duplex(64 * 1024);
    let _handle_a = engine_a.connect(stream_a, dev_b).await.unwrap();
    let handle_b = engine_b.accept(stream_b, dev_a).await.unwrap();

    // B closes with Internal error as soon as apply_update is called.
    let reason_b = tokio::time::timeout(Duration::from_secs(5), handle_b.closed)
        .await
        .expect("B should close within timeout")
        .expect("close reason should be sent");
    assert!(
        matches!(reason_b, CloseReason::Error(BepError::Internal(_))),
        "expected Internal, got: {reason_b:?}"
    );
    assert_eq!(
        engine_b.peers().len(),
        0,
        "B should have no peers after fatal error"
    );
}

// Recovery path: Storage Standby on StoreBlock → connection closes immediately.
// Another slave has taken the distributed lock. Continuing with
// the same cert/device ID would cause two instances to answer requests for the same
// device, confusing remote peers. The daemon should wait for re-activation before
// reconnecting.
#[tokio::test]
async fn storage_standby_closes_connection() {
    init_tracing();

    let dev_a = make_device(1);
    let dev_b = make_device(2);

    let block_hash: [u8; 32] = *b"hash_standby____________________";
    let block_data = Bytes::from_static(b"standby test payload");

    let (storage_a, _fault_a) = FaultStorage::new(MemoryStorage::new());
    let f_a = storage_a.folder(FolderId::from("shared")).await.unwrap();
    f_a.insert_file(make_file_with_blocks(
        "master.bin",
        &[(1, 1)],
        &[block_hash],
    ))
    .await;
    f_a.insert_block("master.bin", 0, block_data.clone()).await;

    let (storage_b, fault_b) = FaultStorage::new(MemoryStorage::new());
    let _f_b = storage_b.folder(FolderId::from("shared")).await.unwrap();

    // Standby is non-retryable: one failure closes the connection immediately.
    // The daemon must wait for lock re-activation before reconnecting.
    fault_b.set(
        StorageMethod::StoreBlock,
        NonZeroU32::MIN,
        StorageError::Standby("injected".into()),
    );

    let engine_a = BepEngine::new(
        storage_a,
        dev_a,
        "node-a".into(),
        vec!["shared".into()],
        resolver(),
    );
    let engine_b = BepEngine::new(
        storage_b,
        dev_b,
        "node-b".into(),
        vec!["shared".into()],
        resolver(),
    );

    let (stream_a, stream_b) = tokio::io::duplex(64 * 1024);
    let _handle_a = engine_a.connect(stream_a, dev_b).await.unwrap();
    let handle_b = engine_b.accept(stream_b, dev_a).await.unwrap();

    // B closes with Standby as soon as store_block is called.
    let reason_b = tokio::time::timeout(Duration::from_secs(5), handle_b.closed)
        .await
        .expect("B should close within timeout")
        .expect("close reason should be sent");
    assert!(
        matches!(reason_b, CloseReason::Error(BepError::Standby(_))),
        "expected Standby, got: {reason_b:?}"
    );
    assert_eq!(
        engine_b.peers().len(),
        0,
        "B should have no peers after Standby"
    );
}

// ---------------------------------------------------------------------------
// Concurrent index and block processing: no duplicate completions
// ---------------------------------------------------------------------------
//
// Processing an Index batch runs in a separate task from handling Responses.
// Both can call into `complete_and_notify` for files staged by the index loop.
// This test asserts that the completion mechanism is idempotent: even under
// concurrent pressure, a file is committed exactly once, and B's
// `local_sequence` increments exactly once per distinct file.
#[tokio::test]
async fn many_files_complete_exactly_once_under_concurrent_pressure() {
    init_tracing();

    let dev_a = make_device(1);
    let dev_b = make_device(2);

    let storage_a = MemoryStorage::new();
    let f_a = storage_a.folder(FolderId::from("shared")).await.unwrap();

    // 20 single-block files. Each unique payload → unique block hash.
    const N: usize = 20;
    let mut files: Vec<(String, Bytes, [u8; 32])> = Vec::with_capacity(N);
    for i in 0..N {
        let name = format!("file-{i:02}.bin");
        let data = Bytes::from(format!("payload-for-{name}"));
        let mut hash = [0u8; 32];
        hash[0] = i as u8;
        hash[1] = (i >> 8) as u8;
        files.push((name, data, hash));
    }
    for (name, data, hash) in &files {
        f_a.insert_file(make_file_with_blocks(name, &[(1, 1)], &[*hash]))
            .await;
        f_a.insert_block(name, 0, data.clone()).await;
    }

    let storage_b = MemoryStorage::new();
    let f_b = storage_b.folder(FolderId::from("shared")).await.unwrap();

    let engine_a = BepEngine::new(
        storage_a,
        dev_a,
        "node-a".into(),
        vec!["shared".into()],
        resolver(),
    );
    let engine_b = BepEngine::new(
        storage_b,
        dev_b,
        "node-b".into(),
        vec!["shared".into()],
        resolver(),
    );

    let (stream_a, stream_b) = tokio::io::duplex(128 * 1024);
    let handle_a = engine_a.connect(stream_a, dev_b).await.unwrap();
    let handle_b = engine_b.accept(stream_b, dev_a).await.unwrap();

    // Wait until B has every file in its committed index. Poll with a short
    // sleep until completion.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let mut all_present = true;
        for (name, _, _) in &files {
            if f_b.get_file(name).await.is_none() {
                all_present = false;
                break;
            }
        }
        if all_present {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("not all files were committed on B within 10s");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Every file present with its block stored.
    for (name, data, _) in &files {
        let got = f_b.get_block(name, 0).await;
        assert_eq!(
            got.as_deref(),
            Some(data.as_ref()),
            "block for {name} should be stored on B"
        );
    }

    // The key invariant: B's local_sequence advances exactly once per file
    // completion. If complete_file ran twice for any file, the counter
    // would be > N. (>N is also possible from index re-staging, but A
    // sends each file exactly once in a single Index.)
    let seq = f_b.local_sequence().await.unwrap().get();
    assert_eq!(
        seq, N as i64,
        "B's local_sequence must equal N={N} (one increment per completion); \
         a higher value indicates duplicate complete_file calls"
    );

    handle_a.shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle_b.closed).await;
}
