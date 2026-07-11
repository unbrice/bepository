// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::tempdir;

#[test]
fn test_cli_help() {
    let mut cmd = Command::cargo_bin("bepository").unwrap();
    cmd.arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Cold storage bridge daemon for Syncthing",
        ));
}

#[test]
fn test_cli_init_and_get_id() {
    let dir = tempdir().unwrap();
    let storage_uri = format!("file://{}", dir.path().to_str().unwrap());

    // 1. Initial GetId should fail
    let mut cmd = Command::cargo_bin("bepository").unwrap();
    cmd.arg("get-id")
        .arg("--storage-uri")
        .arg(&storage_uri)
        .assert()
        .failure()
        .stderr(predicate::str::contains("No identity found"));

    // 2. Init should succeed
    let mut cmd = Command::cargo_bin("bepository").unwrap();
    cmd.arg("init")
        .arg("--storage-uri")
        .arg(&storage_uri)
        .assert()
        .success()
        .stdout(predicate::str::contains("Initialized"))
        .stdout(predicate::str::contains("Device ID:"));

    // 3. GetId should now succeed
    let mut cmd = Command::cargo_bin("bepository").unwrap();
    cmd.arg("get-id")
        .arg("--storage-uri")
        .arg(&storage_uri)
        .assert()
        .success()
        .stdout(predicate::str::is_match(r"^[A-Z0-9-]{50,70}").unwrap());
}

#[test]
fn test_cli_fsck() {
    let dir = tempdir().unwrap();
    let storage_uri = format!("file://{}", dir.path().to_str().unwrap());

    // Init first
    let mut cmd = Command::cargo_bin("bepository").unwrap();
    cmd.arg("init")
        .arg("--storage-uri")
        .arg(&storage_uri)
        .assert()
        .success();

    // Run fsck
    let mut cmd = Command::cargo_bin("bepository").unwrap();
    cmd.arg("fsck")
        .arg("--storage-uri")
        .arg(&storage_uri)
        .assert()
        .success()
        .stdout(predicate::str::contains("Lock status: Unlocked"));

    // Run fsck with regenerate-id
    let mut cmd = Command::cargo_bin("bepository").unwrap();
    cmd.arg("fsck")
        .arg("--storage-uri")
        .arg(&storage_uri)
        .arg("--regenerate-id")
        .assert()
        .success()
        .stdout(predicate::str::contains("New Device ID:"));

    // Run fsck with --compact
    let mut cmd = Command::cargo_bin("bepository").unwrap();
    cmd.arg("fsck")
        .arg("--storage-uri")
        .arg(&storage_uri)
        .arg("--compact")
        .assert()
        .success()
        .stdout(predicate::str::contains("Compaction complete."));
}

#[cfg(feature = "self-manage")]
#[test]
fn test_print_service() {
    let mut cmd = Command::cargo_bin("bepository").unwrap();
    cmd.arg("print-service")
        .assert()
        .success()
        .stdout(predicate::str::contains("DynamicUser=yes"))
        .stdout(predicate::str::contains(
            "EnvironmentFile=/etc/bepository/env",
        ))
        .stdout(predicate::str::contains(
            "ExecStart=/usr/bin/env bepository serve",
        ))
        .stdout(predicate::str::contains("ProtectSystem=strict"));
}

#[cfg(feature = "self-manage")]
#[test]
fn test_package_managed_hides_subcommands_and_refuses() {
    // When BEPOSITORY_PACKAGE_MANAGED is set, --help omits the self-manage
    // subcommands...
    let mut cmd = Command::cargo_bin("bepository").unwrap();
    cmd.env(
        "BEPOSITORY_PACKAGE_MANAGED",
        "update via 'nix flake update'",
    )
    .arg("--help")
    .assert()
    .success()
    .stdout(predicate::str::contains("install-service").not())
    .stdout(predicate::str::contains("print-service").not())
    .stdout(predicate::str::contains("upgrade").not());

    // ...and invoking one refuses with the hint.
    let mut cmd = Command::cargo_bin("bepository").unwrap();
    cmd.env(
        "BEPOSITORY_PACKAGE_MANAGED",
        "update via 'nix flake update'",
    )
    .arg("print-service")
    .assert()
    .failure()
    .stderr(predicate::str::contains("package-managed"))
    .stderr(predicate::str::contains("update via 'nix flake update'"));
}
