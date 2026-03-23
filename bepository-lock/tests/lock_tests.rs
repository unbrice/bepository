// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

use bepository_lock::{AcquisitionStatus, Epoch, Lock};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use tempfile::tempdir;

fn epoch(n: u64) -> Epoch {
    Epoch::new(n).unwrap()
}

#[tokio::test]
async fn test_basic_acquisition() {
    let tmp = tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(tmp.path()).unwrap();
    let prefix = Path::from("test-lock");

    let lock = Lock::new(&store, prefix.clone(), "node1".to_string(), 10, 30);

    let status = lock.acquire().await.unwrap();
    assert_eq!(status, AcquisitionStatus::Owner(epoch(0)));
}

#[tokio::test]
async fn test_contention_same_priority() {
    let tmp = tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(tmp.path()).unwrap();
    let prefix = Path::from("test-lock");

    let lock1 = Lock::new(&store, prefix.clone(), "node1".to_string(), 10, 30);
    let lock2 = Lock::new(&store, prefix.clone(), "node2".to_string(), 10, 30);

    assert_eq!(
        lock1.acquire().await.unwrap(),
        AcquisitionStatus::Owner(epoch(0))
    );
    assert_eq!(lock2.acquire().await.unwrap(), AcquisitionStatus::NotOwner);
}

#[tokio::test]
async fn test_priority_preemption_queuing() {
    let tmp = tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(tmp.path()).unwrap();
    let prefix = Path::from("test-lock");

    let lock1 = Lock::new(&store, prefix.clone(), "node1".to_string(), 10, 30);
    let lock2 = Lock::new(&store, prefix.clone(), "node2".to_string(), 20, 30);

    // Node 1 becomes owner
    assert_eq!(
        lock1.acquire().await.unwrap(),
        AcquisitionStatus::Owner(epoch(0))
    );

    // Node 2 (higher priority) tries to acquire and gets Queued
    assert_eq!(lock2.acquire().await.unwrap(), AcquisitionStatus::Queued);

    // Node 1 tries to renew (acquire again) and should Fail because Node 2 is queued
    assert_eq!(lock1.acquire().await.unwrap(), AcquisitionStatus::Failed);

    // Node 2 tries again and should become Owner (after scavenge)
    assert!(matches!(
        lock2.acquire().await.unwrap(),
        AcquisitionStatus::Owner(_)
    ));
}

#[tokio::test]
async fn test_release() {
    let tmp = tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(tmp.path()).unwrap();
    let prefix = Path::from("test-lock");

    let lock1 = Lock::new(&store, prefix.clone(), "node1".to_string(), 10, 30);
    let lock2 = Lock::new(&store, prefix.clone(), "node2".to_string(), 10, 30);

    assert_eq!(
        lock1.acquire().await.unwrap(),
        AcquisitionStatus::Owner(epoch(0))
    );
    lock1.release().await.unwrap();

    assert_eq!(
        lock2.acquire().await.unwrap(),
        AcquisitionStatus::Owner(epoch(0))
    );
}

#[tokio::test]
async fn test_renewal() {
    let tmp = tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(tmp.path()).unwrap();
    let prefix = Path::from("test-lock");

    let lock = Lock::new(&store, prefix.clone(), "node1".to_string(), 10, 30);

    let status = lock.acquire().await.unwrap();
    assert_eq!(status, AcquisitionStatus::Owner(epoch(0)));
    // Renewal should succeed because fast-path skips us (we are the owner)
    // and the old epoch is scavenged under the "own file" rule.
    let status = lock.acquire().await.unwrap();
    assert_eq!(status, AcquisitionStatus::Owner(epoch(1)));
}

#[tokio::test]
async fn test_scavenge_expired() {
    let tmp = tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(tmp.path()).unwrap();
    let prefix = Path::from("test-lock");

    // Node 1 requests a lease of 0 seconds. This means it is instantly expired
    // the moment another file is created, making the test fully deterministic
    // without relying on wall-clock sleep delays.
    let lock1 = Lock::new(&store, prefix.clone(), "node1".to_string(), 10, 0);
    let lock2 = Lock::new(&store, prefix.clone(), "node2".to_string(), 10, 30);

    assert_eq!(
        lock1.acquire().await.unwrap(),
        AcquisitionStatus::Owner(epoch(0))
    );

    // Node 2 should be able to acquire immediately, scavenging Node 1's instantly expired file.
    // (Fast-path is bypassed because now < expiry evaluates to false for a 0-sec duration).
    assert!(matches!(
        lock2.acquire().await.unwrap(),
        AcquisitionStatus::Owner(_)
    ));
}

#[tokio::test]
async fn test_invalid_file_returns_error() {
    let tmp = tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(tmp.path()).unwrap();
    let prefix = Path::from("test-lock");

    // Manually put a bad file in the lock directory (epoch 0)
    let bad_path = prefix.child("00000000.json");
    store
        .put(&bad_path, "not a json file".into())
        .await
        .unwrap();

    let lock = Lock::new(&store, prefix.clone(), "node1".to_string(), 10, 30);

    // Attempting to acquire should fail to parse the bad file and return an error
    let res = lock.acquire().await;
    assert!(res.is_err());
    if let Err(e) = res {
        assert!(matches!(e, bepository_lock::Error::Serde(_)));
    }
}

#[tokio::test]
async fn test_unsafe_break_locks() {
    use futures::StreamExt;

    let tmp = tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(tmp.path()).unwrap();
    let prefix = Path::from("test-lock");

    // 1. Valid owner
    let owner_lock = Lock::new(&store, prefix.clone(), "owner".to_string(), 10, 30);
    assert_eq!(
        owner_lock.acquire().await.unwrap(),
        AcquisitionStatus::Owner(epoch(0))
    );

    // 2. Queued wait (will be deleted)
    let queued_lock = Lock::new(&store, prefix.clone(), "queued".to_string(), 20, 30);
    assert_eq!(
        queued_lock.acquire().await.unwrap(),
        AcquisitionStatus::Queued
    );

    // Run the breaker
    let breaker = Lock::new(&store, prefix.clone(), "admin".to_string(), 10, 30);
    breaker.unsafe_break_locks().await.unwrap();

    // Verify:
    // - Owner should still be able to renew (its file was spared)
    assert!(matches!(
        owner_lock.acquire().await.unwrap(),
        AcquisitionStatus::Owner(_)
    ));

    let mut list = store.list(Some(&prefix));
    let mut count = 0;
    while list.next().await.is_some() {
        count += 1;
    }

    // Should be exactly 1 file left (the active owner's renewed file)
    assert_eq!(count, 1);
}
