// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

//! End-to-end tests that exercise bepository as a subprocess.
//!
//! Topology per test:
//!   Phase 1: go-A (with files) ↔ bepository-A → shared cold storage
//!   Phase 2: go-B (with files) ↔ bepository-B → same cold storage
//!   Phase 3: bepository-C (same storage) ↔ go-C (empty) → verify go-C gets all files
//!
//! Run with:
//!   cargo build --bin bepository && cargo test -p bepository-e2etest -- --ignored --nocapture

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use bepository_harness::Harness;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn bepository_binary() -> PathBuf {
    if let Ok(bin) = std::env::var("BEPOSITORY_BIN") {
        return PathBuf::from(bin);
    }
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let workspace = Path::new(manifest_dir).parent().unwrap();
    for profile in ["debug", "release"] {
        let bin = workspace.join(format!("target/{profile}/bepository"));
        if bin.exists() {
            return bin;
        }
    }
    panic!("bepository binary not found. Run `cargo build --bin bepository` first.");
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

// ---------------------------------------------------------------------------
// Cold CLI wrappers
// ---------------------------------------------------------------------------

fn bepository_run(args: &[&str]) -> std::process::Output {
    Command::new(bepository_binary())
        .args(args)
        .output()
        .expect("failed to run bepository")
}

/// Run `bepository init` and return the device ID.
fn bepository_init(storage_uri: &str) -> String {
    let output = bepository_run(&["init", "-s", storage_uri]);
    assert!(
        output.status.success(),
        "init failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    stdout
        .lines()
        .find(|l| l.contains("Device ID:"))
        .expect("no Device ID in init output")
        .rsplit("Device ID:")
        .next()
        .unwrap()
        .trim()
        .to_string()
}

/// Forcibly clear the distributed lock on storage.
fn bepository_clear_lock(storage_uri: &str) {
    let _ = bepository_run(&["fsck", "-s", storage_uri, "--clear-lock"]);
}

// ---------------------------------------------------------------------------
// Cold process management
// ---------------------------------------------------------------------------

struct BepositoryProcess {
    child: Option<Child>,
}

impl BepositoryProcess {
    /// Start `bepository serve` and block until the TCP listener is ready.
    fn start(storage_uri: &str, peer_device_id: &str, port: u16) -> Self {
        let listen = format!("127.0.0.1:{port}");

        let child = Command::new(bepository_binary())
            .args([
                "serve",
                "-s",
                storage_uri,
                peer_device_id,
                "--listen",
                &listen,
                "--lease",
                "180",
            ])
            .env(
                "RUST_LOG",
                "bepository_bep=debug,bepository_storage=debug,bepository_cli=debug",
            )
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("failed to start bepository");

        let mut proc = BepositoryProcess { child: Some(child) };

        // Poll the port until the listener is accepting connections.
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if std::net::TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
                break;
            }
            if Instant::now() >= deadline {
                proc.stop();
                panic!("timed out waiting for bepository to listen on port {port}");
            }
            if let Some(ref mut child) = proc.child
                && let Ok(Some(status)) = child.try_wait()
            {
                panic!("bepository exited early with status: {status}");
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        proc
    }

    /// Send SIGINT for graceful shutdown (releases the distributed lock).
    #[allow(clippy::cast_possible_wrap)]
    fn stop(&mut self) {
        if let Some(ref mut child) = self.child {
            unsafe {
                libc::kill(child.id() as libc::pid_t, libc::SIGINT);
            }
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                match child.try_wait() {
                    Ok(Some(_)) => {
                        self.child = None;
                        return;
                    }
                    Ok(None) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(100));
                    }
                    _ => break,
                }
            }
            let _ = child.kill();
            let _ = child.wait();
            self.child = None;
        }
    }
}

impl Drop for BepositoryProcess {
    fn drop(&mut self) {
        self.stop();
    }
}

// ---------------------------------------------------------------------------
// Async helpers
// ---------------------------------------------------------------------------

async fn wait_for_file(path: &Path, timeout: Duration) {
    let start = Instant::now();
    loop {
        if path.exists() {
            return;
        }
        if start.elapsed() > timeout {
            panic!("timed out waiting for file: {}", path.display());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Start a Go master, create a shared folder, and register the
/// slave.
async fn setup_master_slave(
    sync_dir: &Path,
    folder_id: &str,
    bepository_device_id: &str,
    bepository_port: u16,
) -> (Harness, bepository_harness::FolderHandle) {
    let go = Harness::start().await.expect("start Go master");
    let folder = go
        .share_named(sync_dir, folder_id)
        .await
        .expect("share folder");
    folder
        .add_peer(
            bepository_device_id,
            &format!("tcp://127.0.0.1:{bepository_port}"),
        )
        .await
        .expect("add bepository peer to folder");
    (go, folder)
}

/// Run phases 1 + 2: sync files from two separate Go masters into shared bepository
/// storage. Returns the expected file list.
async fn run_ingest_phases(
    storage_uri: &str,
    folder_id: &str,
    bepository_device_id: &str,
) -> Vec<(&'static str, &'static str)> {
    // --- Phase 1: go-A → bepository ---
    let dir_a = tempfile::tempdir().unwrap();
    std::fs::write(dir_a.path().join("file-a1.txt"), "content from A1").unwrap();
    std::fs::write(dir_a.path().join("file-a2.txt"), "content from A2").unwrap();

    let bepository_port = free_port();
    let (go_a, folder_a) = setup_master_slave(
        dir_a.path(),
        folder_id,
        bepository_device_id,
        bepository_port,
    )
    .await;

    eprintln!("Phase 1: syncing go-A → bepository ...");
    let mut bepository = BepositoryProcess::start(storage_uri, go_a.device_id(), bepository_port);
    tokio::time::sleep(Duration::from_secs(10)).await;
    bepository.stop();
    bepository_clear_lock(storage_uri);
    drop(folder_a);
    drop(go_a);
    eprintln!("Phase 1 complete.");

    // --- Phase 2: go-B → bepository ---
    let dir_b = tempfile::tempdir().unwrap();
    std::fs::write(dir_b.path().join("file-b1.txt"), "content from B1").unwrap();
    std::fs::write(dir_b.path().join("file-b2.txt"), "content from B2").unwrap();

    let bepository_port = free_port();
    let (go_b, folder_b) = setup_master_slave(
        dir_b.path(),
        folder_id,
        bepository_device_id,
        bepository_port,
    )
    .await;

    eprintln!("Phase 2: syncing go-B → bepository ...");
    let mut bepository = BepositoryProcess::start(storage_uri, go_b.device_id(), bepository_port);
    tokio::time::sleep(Duration::from_secs(10)).await;
    bepository.stop();
    bepository_clear_lock(storage_uri);
    drop(folder_b);
    drop(go_b);
    eprintln!("Phase 2 complete.");

    vec![
        ("file-a1.txt", "content from A1"),
        ("file-a2.txt", "content from A2"),
        ("file-b1.txt", "content from B1"),
        ("file-b2.txt", "content from B2"),
    ]
}

/// Phase 3: start a fresh Go master and verify it receives all expected files
/// from bepository storage.
async fn verify_retrieval(
    storage_uri: &str,
    folder_id: &str,
    bepository_device_id: &str,
    expected: &[(&str, &str)],
) {
    let dir_c = tempfile::tempdir().unwrap();
    let bepository_port = free_port();
    let (go_c, _folder_c) = setup_master_slave(
        dir_c.path(),
        folder_id,
        bepository_device_id,
        bepository_port,
    )
    .await;

    eprintln!("Retrieval phase: syncing bepository → go-C ...");
    let _bepository_c = BepositoryProcess::start(storage_uri, go_c.device_id(), bepository_port);

    let timeout = Duration::from_secs(60);
    for &(name, _) in expected {
        wait_for_file(&dir_c.path().join(name), timeout).await;
    }

    for &(name, content) in expected {
        let actual = std::fs::read_to_string(dir_c.path().join(name))
            .unwrap_or_else(|e| panic!("failed to read {name}: {e}"));
        assert_eq!(actual, content, "file {name} has wrong content");
    }

    eprintln!("All {} files verified in go-C!", expected.len());
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Two Go masters each sync files into shared bepository storage. A third Go master
/// connected to the same cold storage receives all files.
#[tokio::test]
#[ignore = "e2e: requires syncthing and bepository binaries"]
async fn files_sync_through_bepository_storage() {
    let folder_id = "e2e-sync";
    let storage_dir = tempfile::tempdir().unwrap();
    let storage_uri = format!("file://{}/{folder_id}", storage_dir.path().display());

    let bepository_device_id = bepository_init(&storage_uri);
    eprintln!("Cold device ID: {bepository_device_id}");

    let expected = run_ingest_phases(&storage_uri, folder_id, &bepository_device_id).await;
    verify_retrieval(&storage_uri, folder_id, &bepository_device_id, &expected).await;
}

/// Same as above, but the bepository daemon is restarted twice between ingest and
/// retrieval to verify the storage survives process restarts.
#[tokio::test]
#[ignore = "e2e: requires syncthing and bepository binaries"]
async fn files_survive_bepository_restarts() {
    let folder_id = "e2e-restart";
    let storage_dir = tempfile::tempdir().unwrap();
    let storage_uri = format!("file://{}/{folder_id}", storage_dir.path().display());

    let bepository_device_id = bepository_init(&storage_uri);
    eprintln!("Cold device ID: {bepository_device_id}");

    let expected = run_ingest_phases(&storage_uri, folder_id, &bepository_device_id).await;

    // Start a throwaway Go instance whose device ID we use for the restart
    // cycles (cold needs a valid peer ID even if nobody connects).
    let go_dummy = Harness::start().await.expect("start dummy Go");
    let dummy_peer_id = go_dummy.device_id().to_string();
    drop(go_dummy);

    for i in 1..=2 {
        eprintln!("Restart cycle {i} ...");
        let port = free_port();
        let mut bepository = BepositoryProcess::start(&storage_uri, &dummy_peer_id, port);
        // Let the process open storage, exercise startup, then stop.
        tokio::time::sleep(Duration::from_secs(3)).await;
        bepository.stop();
        bepository_clear_lock(&storage_uri);
        eprintln!("Restart cycle {i} complete.");
    }

    verify_retrieval(&storage_uri, folder_id, &bepository_device_id, &expected).await;
}
