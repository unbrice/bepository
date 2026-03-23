// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration test: three Syncthing instances each contribute one file to a
//! shared folder and verify that all three files appear everywhere.
//!
//! Requires a real `syncthing` binary on `PATH` or via `SYNCTHING_BIN`.
//! The `integration` feature (on by default) enables the test; disable it
//! in CI environments without the binary:
//!
//! ```text
//! cargo test -p bepository-harness --no-default-features
//! ```

use bepository_harness::Harness;
use std::time::{Duration, Instant};

const SYNC_TIMEOUT: Duration = Duration::from_secs(60);

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore = "requires syncthing binary")]
async fn three_instances_sync_files() {
    // -----------------------------------------------------------------------
    // 1. Start three independent instances in parallel.
    // -----------------------------------------------------------------------
    let (a, b, c) = tokio::try_join!(Harness::start(), Harness::start(), Harness::start(),)
        .expect("all three instances should start");

    // -----------------------------------------------------------------------
    // 2. Create a local directory for each instance and seed one file.
    // -----------------------------------------------------------------------
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let dir_c = tempfile::tempdir().unwrap();

    std::fs::write(dir_a.path().join("from_a.txt"), "hello from A").unwrap();
    std::fs::write(dir_b.path().join("from_b.txt"), "hello from B").unwrap();
    std::fs::write(dir_c.path().join("from_c.txt"), "hello from C").unwrap();

    // -----------------------------------------------------------------------
    // 3. Each instance shares its directory.  A generates the folder ID;
    //    B and C join the same folder by ID.
    // -----------------------------------------------------------------------
    let ha = a.share(dir_a.path()).await.expect("A share");
    let hb = b
        .share_named(dir_b.path(), ha.folder_id())
        .await
        .expect("B share");
    let hc = c
        .share_named(dir_c.path(), ha.folder_id())
        .await
        .expect("C share");

    // -----------------------------------------------------------------------
    // 4. Wire peers.  FolderHandle::add_peer serialises concurrent calls on
    //    the same handle internally, so all six calls can run in parallel.
    // -----------------------------------------------------------------------
    tokio::try_join!(
        ha.add_peer(b.device_id(), b.listen_addr()),
        ha.add_peer(c.device_id(), c.listen_addr()),
        hb.add_peer(a.device_id(), a.listen_addr()),
        hb.add_peer(c.device_id(), c.listen_addr()),
        hc.add_peer(a.device_id(), a.listen_addr()),
        hc.add_peer(b.device_id(), b.listen_addr()),
    )
    .expect("peer wiring should succeed");

    // -----------------------------------------------------------------------
    // 5. Poll until every directory contains all three files, or time out.
    // -----------------------------------------------------------------------
    let deadline = Instant::now() + SYNC_TIMEOUT;

    let all_present = |dir: &std::path::Path| {
        dir.join("from_a.txt").exists()
            && dir.join("from_b.txt").exists()
            && dir.join("from_c.txt").exists()
    };

    loop {
        if all_present(dir_a.path()) && all_present(dir_b.path()) && all_present(dir_c.path()) {
            break;
        }
        if Instant::now() >= deadline {
            for (label, dir) in [
                ("A", dir_a.path()),
                ("B", dir_b.path()),
                ("C", dir_c.path()),
            ] {
                let entries: Vec<_> = std::fs::read_dir(dir)
                    .unwrap()
                    .filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect();
                eprintln!("{label} has: {entries:?}");
            }
            panic!("sync did not complete within {SYNC_TIMEOUT:?}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // -----------------------------------------------------------------------
    // 6. Verify file contents (not just presence).
    // -----------------------------------------------------------------------
    for dir in [dir_a.path(), dir_b.path(), dir_c.path()] {
        assert_eq!(
            std::fs::read_to_string(dir.join("from_a.txt")).unwrap(),
            "hello from A"
        );
        assert_eq!(
            std::fs::read_to_string(dir.join("from_b.txt")).unwrap(),
            "hello from B"
        );
        assert_eq!(
            std::fs::read_to_string(dir.join("from_c.txt")).unwrap(),
            "hello from C"
        );
    }
}
