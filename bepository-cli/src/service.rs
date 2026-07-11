// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

//! `install-service` / `print-service` / `uninstall-service` subcommands.
//!
//! Gated behind the `self-manage` feature. Emits a hardened systemd unit for
//! `bepository serve` and (optionally) a daily self-upgrade timer.

use std::fs;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};

/// The example env file, embedded so `install-service` is self-contained.
const ENV_EXAMPLE: &str = include_str!("../../deploy/env.example");

/// `/etc/systemd/system/bepository.service` and friends.
const SYSTEMD_DIR: &str = "/etc/systemd/system";
const SERVICE_NAME: &str = "bepository.service";
const UPGRADE_SERVICE_NAME: &str = "bepository-upgrade.service";
const UPGRADE_TIMER_NAME: &str = "bepository-upgrade.timer";
const ENV_DIR: &str = "/etc/bepository";
const ENV_PATH: &str = "/etc/bepository/env";

/// The hardened systemd unit for `bepository serve`.
///
/// Port/listen address comes from the env file (`BEPOSITORY_LISTEN`), not the
/// unit. `DynamicUser=yes` means there is no stable UID, so the binary itself
/// must auto-init on serve (no root one-shot `init` first).
const BEPOSITORY_SERVICE: &str = "\
[Unit]
Description=bepository cold-storage bridge for Syncthing
After=network-online.target
Wants=network-online.target
Conflicts=sleep.target
Before=sleep.target

[Service]
Type=simple
ExecStart=/usr/local/bin/bepository serve
EnvironmentFile=/etc/bepository/env
StateDirectory=bepository
CacheDirectory=bepository
DynamicUser=yes
ProtectSystem=strict
PrivateTmp=yes
Restart=on-failure
RestartSec=10s

[Install]
WantedBy=multi-user.target
";

/// Oneshot service that runs `bepository upgrade` and restarts the daemon.
const BEPOSITORY_UPGRADE_SERVICE: &str = "\
[Unit]
Description=bepository self-upgrade
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
ExecStart=/usr/local/bin/bepository upgrade --restart-unit bepository.service
";

/// Daily upgrade timer, randomized to spread load.
const BEPOSITORY_UPGRADE_TIMER: &str = "\
[Unit]
Description=Daily bepository self-upgrade

[Timer]
OnCalendar=daily
RandomizedDelaySec=1h
Persistent=true

[Install]
WantedBy=timers.target
";

/// Print the `bepository.service` unit to stdout.
pub(crate) fn print_service() -> Result<()> {
    print!("{BEPOSITORY_SERVICE}");
    Ok(())
}

/// Install the systemd units (and the upgrade timer, unless `--no-auto-upgrade`),
/// reload systemd, enable the units, and seed `/etc/bepository/env` if missing.
///
/// Idempotent: re-running overwrites the unit files and re-enables. The env
/// file is never overwritten once it exists. No UID probing: a `PermissionDenied`
/// from the writes is mapped to a clear "requires root" error.
pub(crate) fn install_service(no_auto_upgrade: bool) -> Result<()> {
    write_unit(SERVICE_NAME, BEPOSITORY_SERVICE)?;
    if !no_auto_upgrade {
        write_unit(UPGRADE_SERVICE_NAME, BEPOSITORY_UPGRADE_SERVICE)?;
        write_unit(UPGRADE_TIMER_NAME, BEPOSITORY_UPGRADE_TIMER)?;
    } else {
        // If the user previously installed the timer and now opts out, remove
        // the stale timer pair so `--no-auto-upgrade` is the effective state.
        let _ = disable_and_remove_unit(UPGRADE_TIMER_NAME);
        let _ = disable_and_remove_unit(UPGRADE_SERVICE_NAME);
    }

    systemctl(&["daemon-reload"])?;
    systemctl(&["enable", SERVICE_NAME])?;
    if !no_auto_upgrade {
        // The main service is deliberately enabled-but-not-started (the user
        // must edit /etc/bepository/env first). The upgrade timer has no such
        // prerequisite — arm it immediately so auto-upgrade works without a
        // reboot.
        systemctl(&["enable", "--now", UPGRADE_TIMER_NAME])?;
    }

    if !Path::new(ENV_PATH).exists() {
        fs::create_dir_all(ENV_DIR).with_context(|| format!("failed to create {ENV_DIR}"))?;
        // create_new: never clobber an existing env file (the `exists` check
        // guards the common case; create_new closes the TOCTOU window).
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(ENV_PATH)
            .with_context(|| format!("failed to create {ENV_PATH}"))?;
        f.write_all(ENV_EXAMPLE.as_bytes())
            .with_context(|| format!("failed to write {ENV_PATH}"))?;
        println!("Installed example config at {ENV_PATH} (mode 600).");
    }

    println!("Service installed and enabled.");
    if no_auto_upgrade {
        println!("Auto-upgrade timer skipped (--no-auto-upgrade).");
    }
    println!("Edit {ENV_PATH}, then: systemctl start {SERVICE_NAME}");
    Ok(())
}

/// Disable and remove the units, then daemon-reload. Leaves `/etc/bepository/`
/// in place (it holds the user's config and credentials).
pub(crate) fn uninstall_service() -> Result<()> {
    disable_and_remove_unit(UPGRADE_TIMER_NAME)?;
    disable_and_remove_unit(UPGRADE_SERVICE_NAME)?;
    disable_and_remove_unit(SERVICE_NAME)?;
    systemctl(&["daemon-reload"])?;
    println!("Service units removed. Left {ENV_DIR} in place (it may hold credentials).");
    Ok(())
}

/// Write a unit file to the systemd dir, mapping permission errors to a "needs
/// root" hint.
fn write_unit(name: &str, content: &str) -> Result<()> {
    let path = Path::new(SYSTEMD_DIR).join(name);
    fs::write(&path, content).map_err(|e| {
        if e.kind() == std::io::ErrorKind::PermissionDenied {
            anyhow!(
                "install-service requires root: permission denied writing {}",
                path.display()
            )
        } else {
            anyhow::Error::from(e).context(format!("failed to write {}", path.display()))
        }
    })?;
    Ok(())
}

/// `systemctl disable --now` + remove the unit file. Missing units are not an
/// error (idempotent uninstall / partial installs).
fn disable_and_remove_unit(name: &str) -> Result<()> {
    let _ = systemctl(&["disable", "--now", name]);
    let path = Path::new(SYSTEMD_DIR).join(name);
    if path.exists() {
        fs::remove_file(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::PermissionDenied {
                anyhow!(
                    "uninstall-service requires root: permission denied removing {}",
                    path.display()
                )
            } else {
                anyhow::Error::from(e).context(format!("failed to remove {}", path.display()))
            }
        })?;
    }
    Ok(())
}

/// Run `systemctl <args>`, treating the binary's absence as a soft skip
/// (the install is still useful on a host without a running systemd — e.g.
/// building an image). Other failures propagate.
fn systemctl(args: &[&str]) -> Result<()> {
    let status = match std::process::Command::new("systemctl").args(args).status() {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::warn!("systemctl not found on PATH; skipping {args:?}");
            return Ok(());
        }
        Err(e) => return Err(anyhow::Error::from(e).context("failed to run systemctl")),
    };
    if !status.success() {
        bail!("systemctl {args:?} failed with status {status}");
    }
    Ok(())
}

/// When `BEPOSITORY_PACKAGE_MANAGED` is set, the self-manage subcommands refuse
/// to run and point the user at the packager's update path. Returns the hint
/// text if set, `None` otherwise. Used by every self-manage handler.
pub(crate) fn package_managed_hint() -> Option<String> {
    // SAFETY: read-only env access; the value is captured for display.
    std::env::var("BEPOSITORY_PACKAGE_MANAGED")
        .ok()
        .filter(|s| !s.is_empty())
}

/// Build the "refused because package-managed" error used by every self-manage
/// subcommand. Kept here so the wording is uniform.
pub(crate) fn package_managed_error(hint: &str) -> anyhow::Error {
    anyhow!(
        "this bepository is package-managed and manages its own service files \
         and updates; update instead via: {hint}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    /// The ExecStart binary the unit references; used to gate the
    /// systemd-analyze test (verify resolves it against the real fs).
    const BIN_REF: &str = "/usr/local/bin/bepository";

    #[test]
    fn service_unit_has_required_hardening_keys() {
        for needle in [
            "DynamicUser=yes",
            "EnvironmentFile=/etc/bepository/env",
            "ExecStart=",
            "Conflicts=sleep.target",
            "Before=sleep.target",
            "ProtectSystem=strict",
            "PrivateTmp=yes",
            "StateDirectory=bepository",
            "CacheDirectory=bepository",
            "Restart=on-failure",
            "RestartSec=10s",
            "After=network-online.target",
            "Wants=network-online.target",
        ] {
            assert!(
                BEPOSITORY_SERVICE.contains(needle),
                "service unit missing {needle:?}"
            );
        }
        assert!(BEPOSITORY_SERVICE.contains("ExecStart=/usr/local/bin/bepository serve"));
    }

    #[test]
    fn upgrade_timer_is_daily() {
        assert!(BEPOSITORY_UPGRADE_TIMER.contains("OnCalendar=daily"));
        assert!(BEPOSITORY_UPGRADE_TIMER.contains("RandomizedDelaySec=1h"));
        assert!(BEPOSITORY_UPGRADE_TIMER.contains("Persistent=true"));
    }

    #[test]
    fn upgrade_service_restarts_unit() {
        assert!(BEPOSITORY_UPGRADE_SERVICE.contains("upgrade --restart-unit bepository.service"));
    }

    /// If `systemd-analyze` is on PATH, validate the emitted unit parses.
    /// Skipped (not failed) when the binary is absent or the environment can't
    /// meaningfully verify (e.g. `/usr/local/bin/bepository` not installed, or
    /// running outside a full systemd context).
    #[test]
    fn systemd_analyze_verify_emitted_unit() {
        let bin = which_systemd_analyze();
        let Some(bin) = bin else {
            eprintln!("skipping: systemd-analyze not on PATH");
            return;
        };
        // verify resolves ExecStart against the real filesystem; if the binary
        // isn't installed here (dev machine, CI without install), the check is
        // not meaningful.
        if !Path::new(BIN_REF).exists() {
            eprintln!("skipping: {BIN_REF} not installed");
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        let unit = tmp.path().join(SERVICE_NAME);
        fs::write(&unit, BEPOSITORY_SERVICE).unwrap();
        let output = std::process::Command::new(bin)
            .arg("verify")
            .arg(&unit)
            .output()
            .expect("systemd-analyze vanished after PATH check");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Missing default targets (sysinit.target etc.) mean we're outside a
            // full systemd boot context — not a real unit defect.
            if stderr.contains("not found") || stderr.contains("not executable") {
                eprintln!("skipping: systemd-analyze verify not meaningful here:\n{stderr}");
                return;
            }
            panic!("systemd-analyze verify failed:\n{stderr}");
        }
    }

    fn which_systemd_analyze() -> Option<std::path::PathBuf> {
        // We avoid a `which` crate dependency; search PATH manually.
        let path = std::env::var_os("PATH")?;
        for dir in std::env::split_paths(&path) {
            let cand = dir.join("systemd-analyze");
            if cand.is_file() {
                return Some(cand);
            }
        }
        None
    }
}
