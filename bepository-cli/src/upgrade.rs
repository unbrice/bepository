// SPDX-FileCopyrightText: 2026 Brice Arnolder
//
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Placeholder for the `upgrade` subcommand. Implemented in PLAN-0.8 Phase 3.

#![allow(dead_code)]

use anyhow::Result;

/// Run the self-upgrade. Placeholder until Phase 3.
pub(crate) async fn run(_restart_unit: Option<String>, _dry_run: bool) -> Result<()> {
    anyhow::bail!("self-upgrade is not implemented in this build")
}
