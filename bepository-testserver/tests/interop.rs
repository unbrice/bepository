#![allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Interop tests: verify our Rust BEP implementation can talk to the real
//! Go master over TLS.
//!
//! Uses `bepository_testserver::TestServer` which wires together
//! `bepository-tls` (identity + TLS) and `bepository-bep` (protocol engine).
//!
//! Requires a `syncthing` binary on PATH or via `SYNCTHING_BIN`.
//!
//! ```text
//! cargo test -p bepository-testserver --test interop
//! ```

use std::sync::Arc;
use std::time::Duration;

use bepository_bep::storage::StorageInspectorForTests;
use bepository_bep::{DeviceId, FolderId, FolderLabel, Storage};
use bepository_harness::Harness;
use bepository_storage::SlateStorage;
use bepository_testserver::TestServer;
use bytes::Bytes;
use object_store::memory::InMemory;

fn init_tracing() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let _ = tracing_subscriber::fmt()
        .with_env_filter("bepository_bep=debug,bepository_testserver=debug,interop=debug")
        .with_test_writer()
        .try_init();
}

async fn start_harness() -> Harness {
    Harness::start().await.expect("failed to start Go master")
}

// ---------------------------------------------------------------------------
// Test 1: TLS handshake + BEP Hello exchange with Go binary
// ---------------------------------------------------------------------------

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore = "requires syncthing binary")]
async fn hello_exchange_with_go() {
    init_tracing();

    let harness = start_harness().await;
    let server = TestServer::new(vec![]);

    // Register the Rust device with the Go instance so it accepts us.
    harness
        .client()
        .add_device(&server.device_id().to_string(), "dynamic")
        .await
        .expect("add rust device to Go config");

    // Allow the Go device on our side.
    let go_device_id = DeviceId::parse(harness.device_id()).expect("parse Go device ID");
    server.allow_device(go_device_id);

    let handle = server
        .connect_to(harness.listen_addr())
        .await
        .expect("BEP connect");

    // Give time for Hello + ClusterConfig exchange.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Connection should still be alive (no protocol errors).
    assert_eq!(server.peers().len(), 1, "should be connected to Go peer");

    // Graceful shutdown.
    handle.shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 2: manual Hello+ClusterConfig exchange (raw codec, no engine)
// ---------------------------------------------------------------------------

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore = "requires syncthing binary")]
async fn manual_cluster_config_exchange() {
    init_tracing();

    let harness = start_harness().await;
    let identity = bepository_tls::Identity::generate().expect("generate identity");
    let rust_id = *identity.device_id();

    harness
        .client()
        .add_device(&rust_id.to_string(), "dynamic")
        .await
        .expect("add device");

    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("test.txt"), "hello").unwrap();
    let fh = harness.share(dir.path()).await.expect("share");
    fh.add_peer(&rust_id.to_string(), "dynamic")
        .await
        .expect("add peer");
    let folder_id = fh.folder_id().to_owned();

    tokio::time::sleep(Duration::from_secs(3)).await;

    let bep_stream = bepository_tls::connect(harness.listen_addr(), &identity)
        .await
        .expect("TLS connect");
    let mut stream = bep_stream.stream;

    // Send Hello manually (no split).
    bepository_bep::framing::send_hello(&mut stream, "bepository-test")
        .await
        .expect("send hello");

    let _peer_hello = bepository_bep::framing::recv_hello(&mut stream)
        .await
        .expect("recv hello");

    // Send ClusterConfig.
    use bepository_bep::proto::bep::*;
    bepository_bep::framing::write_message(
        &mut stream,
        &Header {
            r#type: MessageType::ClusterConfig as i32,
            compression: MessageCompression::None as i32,
        },
        &{
            use prost::Message;
            let go_device_id = DeviceId::parse(harness.device_id()).expect("parse Go device ID");
            ClusterConfig {
                folders: vec![Folder {
                    id: folder_id.clone(),
                    devices: vec![
                        Device {
                            id: rust_id.as_bytes().to_vec(),
                            max_sequence: 0,
                            ..Default::default()
                        },
                        Device {
                            id: go_device_id.as_bytes().to_vec(),
                            max_sequence: 0,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                secondary: false,
            }
            .encode_to_vec()
        },
        false,
    )
    .await
    .expect("send ClusterConfig");

    // Read peer ClusterConfig.
    let msg = tokio::time::timeout(
        Duration::from_secs(10),
        bepository_bep::framing::read_message(&mut stream),
    )
    .await
    .expect("timeout reading ClusterConfig")
    .expect("read ClusterConfig");
    assert_eq!(msg.header.r#type, MessageType::ClusterConfig as i32);

    // Read Index.
    let idx_msg = tokio::time::timeout(
        Duration::from_secs(10),
        bepository_bep::framing::read_message(&mut stream),
    )
    .await
    .expect("timeout reading Index")
    .expect("read Index");
    assert_eq!(idx_msg.header.r#type, MessageType::Index as i32);
}

// ---------------------------------------------------------------------------
// Test 3: shared folder → Index exchange with Go binary (via TestServer)
// ---------------------------------------------------------------------------

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore = "requires syncthing binary")]
async fn index_exchange_with_go() {
    init_tracing();

    let harness = start_harness().await;

    // Create a temp directory with a file for the Go side.
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("go_file.txt"), "hello from Go").unwrap();

    // Share the directory on the Go side.
    let folder_handle = harness.share(dir.path()).await.expect("share folder");
    let folder_id = folder_handle.folder_id().to_owned();

    let server = TestServer::new(vec![FolderId::from(folder_id.clone())]);

    // Seed a file on the Rust side so the engine sends it in the Index.
    let rust_content = b"hello from Rust";
    let rust_block_hash = {
        use sha2::{Digest, Sha256};
        Sha256::digest(rust_content).to_vec()
    };
    {
        use bepository_bep::proto::bep::*;
        let fi = FileInfo {
            name: "rust_file.txt".into(),
            size: rust_content.len() as i64,
            modified_s: 1700000000,
            permissions: 0o644,
            version: Some(Vector {
                counters: vec![Counter { id: 1, value: 1 }],
            }),
            sequence: 1,
            block_size: rust_content.len() as i32,
            blocks: vec![BlockInfo {
                offset: 0,
                size: rust_content.len() as i32,
                hash: rust_block_hash,
            }],
            ..Default::default()
        };
        let f = server
            .storage()
            .folder(FolderId::from(folder_id.clone()))
            .await
            .unwrap();
        f.insert_file(fi).await;
        f.insert_block("rust_file.txt", 0, Bytes::from_static(rust_content))
            .await;
    }

    // Add the Rust device as a peer in the Go folder.
    folder_handle
        .add_peer(&server.device_id().to_string(), "dynamic")
        .await
        .expect("add rust peer to folder");

    // Give Go a moment to scan the directory.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Allow the Go device on our side.
    let go_device_id = DeviceId::parse(harness.device_id()).expect("parse Go device ID");
    server.allow_device(go_device_id);

    let handle = server
        .connect_to(harness.listen_addr())
        .await
        .expect("BEP connect");

    // Wait for Go's index to arrive on the Rust side.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if server
            .storage()
            .folder(FolderId::from(folder_id.clone()))
            .await
            .unwrap()
            .get_file("go_file.txt")
            .await
            .is_some()
        {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for Go's index to arrive");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Wait for the Rust file to appear on the Go filesystem.
    let rust_file_path = dir.path().join("rust_file.txt");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if rust_file_path.exists() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for rust_file.txt to appear on Go side");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let go_content = std::fs::read_to_string(&rust_file_path).expect("read rust_file.txt from Go");
    assert_eq!(
        go_content, "hello from Rust",
        "Go should have received the file from Rust"
    );

    assert_eq!(server.peers().len(), 1);
    handle.shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 4: Index exchange with Go binary using SlateDB storage backend
// ---------------------------------------------------------------------------

async fn make_slate_server(shared_folders: Vec<FolderId>) -> TestServer<SlateStorage> {
    let identity = bepository_tls::Identity::generate().expect("cert generation");
    let object_store = Arc::new(InMemory::new());
    let storage = SlateStorage::new(object_store, None, tokio::runtime::Handle::current());
    storage
        .activate(bepository_storage::Epoch::new(1).unwrap())
        .await
        .expect("activate storage");

    for folder in &shared_folders {
        let label = FolderLabel::from(format!("LabelFor{folder}"));
        storage.register_folder(*folder, &label).await.unwrap();
    }

    TestServer::with_storage(
        identity,
        storage,
        shared_folders,
        Arc::new(bepository_bep::test_utils::BackupResolver),
    )
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore = "requires syncthing binary")]
async fn index_exchange_with_go_slate() {
    init_tracing();

    let harness = start_harness().await;

    // Create a temp directory with a file for the Go side.
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("go_file.txt"), "hello from Go").unwrap();

    // Share the directory on the Go side.
    let folder_handle = harness.share(dir.path()).await.expect("share folder");
    let folder_id = folder_handle.folder_id().to_owned();

    let server = make_slate_server(vec![FolderId::from(folder_id.clone())]).await;

    // Seed a file on the Rust side so the engine sends it in the Index.
    let rust_content = b"hello from Rust via SlateDB";
    let rust_block_hash = {
        use sha2::{Digest, Sha256};
        Sha256::digest(rust_content).to_vec()
    };
    {
        use bepository_bep::proto::bep::*;
        let fi = FileInfo {
            name: "rust_slate_file.txt".into(),
            size: rust_content.len() as i64,
            modified_s: 1700000000,
            permissions: 0o644,
            version: Some(Vector {
                counters: vec![Counter { id: 1, value: 1 }],
            }),
            sequence: 1,
            block_size: rust_content.len() as i32,
            blocks: vec![BlockInfo {
                offset: 0,
                size: rust_content.len() as i32,
                hash: rust_block_hash,
            }],
            ..Default::default()
        };
        let f = server
            .storage()
            .folder(FolderId::from(folder_id.clone()))
            .await
            .unwrap();
        f.insert_file(fi).await;
        f.insert_block("rust_slate_file.txt", 0, Bytes::from_static(rust_content))
            .await;
    }

    // Add the Rust device as a peer in the Go folder.
    folder_handle
        .add_peer(&server.device_id().to_string(), "dynamic")
        .await
        .expect("add rust peer to folder");

    // Give Go a moment to scan the directory.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Allow the Go device on our side.
    let go_device_id = DeviceId::parse(harness.device_id()).expect("parse Go device ID");
    server.allow_device(go_device_id);

    let handle = server
        .connect_to(harness.listen_addr())
        .await
        .expect("BEP connect");

    // Wait for Go's index to arrive on the Rust side.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if server
            .storage()
            .folder(FolderId::from(folder_id.clone()))
            .await
            .unwrap()
            .get_file("go_file.txt")
            .await
            .is_some()
        {
            break;
        }

        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for Go's index to arrive in SlateDB");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Wait for the Rust file to appear on the Go filesystem.
    let rust_file_path = dir.path().join("rust_slate_file.txt");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if rust_file_path.exists() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for rust_slate_file.txt to appear on Go side");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let go_content =
        std::fs::read_to_string(&rust_file_path).expect("read rust_slate_file.txt from Go");
    assert_eq!(
        go_content, "hello from Rust via SlateDB",
        "Go should have received the file from Rust (SlateDB backend)"
    );

    assert_eq!(server.peers().len(), 1);
    handle.shutdown.cancel();
}
