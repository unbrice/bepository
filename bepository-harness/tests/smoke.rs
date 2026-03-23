// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration smoke test for the Syncthing test harness.
//!
//! Requires a real `syncthing` binary. Set `SYNCTHING_BIN` or ensure
//! `syncthing` is on `PATH`. The `integration` feature (on by default) enables
//! the test; disable it in CI environments without the binary:
//!
//! ```text
//! cargo test -p bepository-harness --no-default-features
//! ```

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore = "requires syncthing binary")]
async fn start_share_drop() {
    let harness = bepository_harness::Harness::start()
        .await
        .expect("harness should start");

    let device_id = harness.device_id().to_owned();
    assert!(!device_id.is_empty(), "device ID should be non-empty");
    assert_eq!(
        device_id.len(),
        63,
        "device ID should be 63 chars (8×7 + 7 dashes)"
    );

    let dir = tempfile::tempdir().expect("tmp dir");
    let handle = harness
        .share(dir.path())
        .await
        .expect("share should succeed");

    assert!(
        !handle.folder_id().is_empty(),
        "folder ID should be non-empty"
    );
    assert_eq!(handle.device_id(), device_id, "device IDs should match");

    // Dropping the handle should remove the folder from the config.
    let _folder_id = handle.folder_id().to_owned();
    drop(handle);
    // Give the spawned DELETE task a moment to complete.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Harness and temp dir clean up on drop.
    drop(harness);
    drop(dir);
}
