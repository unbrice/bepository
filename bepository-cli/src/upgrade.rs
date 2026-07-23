// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

//! `upgrade` subcommand: self-update from GitHub releases.
//!
//! Gated behind the `self-manage` feature. Queries the GitHub releases API,
//! compares the latest tag against `CARGO_PKG_VERSION` (semver, strictly
//! newer), downloads the asset matching the compile-time target triple,
//! verifies its sha256 against `SHA256SUMS`, and atomically self-replaces the
//! running binary.
//!
//! The core decision logic (version comparison, asset selection, checksum
//! verification) lives in pure functions so it is unit-testable without a
//! network fixture; [`run`] is the thin network-bound orchestrator.

use anyhow::{Context, Result, anyhow, bail};
use semver::Version;
use sha2::{Digest, Sha256};

/// Compile-time target triple, set by `build.rs`. Used to pick the release asset.
const TARGET_TRIPLE: &str = env!("TARGET_TRIPLE");

/// The GitHub API/releases origin. Overridable via env for tests.
fn release_base_url() -> String {
    std::env::var("BEPOSITORY_RELEASE_BASE_URL")
        .unwrap_or_else(|_| "https://api.github.com/repos/unbrice/bepository".to_string())
}

/// The current version of this binary, parsed from `CARGO_PKG_VERSION`.
fn current_version() -> Result<Version> {
    let v = env!("CARGO_PKG_VERSION");
    Version::parse(v).with_context(|| format!("failed to parse CARGO_PKG_VERSION ({v})"))
}

/// Strip a leading `v` and parse semver. `semver::Version::parse("v0.8.0")` fails.
fn parse_tag(tag: &str) -> Result<Version> {
    let stripped = tag.strip_prefix('v').unwrap_or(tag);
    Version::parse(stripped)
        .with_context(|| format!("release tag {tag:?} is not a valid semver version"))
}

/// Pick the asset whose name ends with `-<target>`. Returns `None` if none match.
fn pick_asset<'a>(release: &'a Release, target: &str) -> Option<&'a Asset> {
    let needle = format!("-{target}");
    release.assets.iter().find(|a| a.name.ends_with(&needle))
}

/// Locate the line in `SHA256SUMS` for `asset_name` and return its hex digest
/// (lowercased). Returns `None` if the asset is not listed.
fn checksum_for(sums: &str, asset_name: &str) -> Option<String> {
    for line in sums.lines() {
        let line = line.trim();
        let mut parts = line.split_whitespace();
        let (Some(hash), Some(name), None) = (parts.next(), parts.next(), parts.next()) else {
            continue;
        };
        if name == asset_name {
            return Some(hash.to_ascii_lowercase());
        }
    }
    None
}

/// Verify a downloaded asset's sha256 against an expected hex digest. Returns
/// `Ok(())` on match, an error naming both digests on mismatch.
fn verify_hash(bytes: &[u8], expected_hex: &str) -> Result<()> {
    let actual = hex_encode(&Sha256::digest(bytes));
    if actual == expected_hex.to_ascii_lowercase() {
        Ok(())
    } else {
        bail!(
            "checksum mismatch: expected {expected_hex}, got {actual} — \
             leaving the current binary untouched"
        )
    }
}

/// Entry point for `bepository upgrade [--restart-unit <unit>] [--dry-run]`.
///
/// The `BEPOSITORY_PACKAGE_MANAGED` guard is checked by the caller (the dispatch
/// in `main.rs`); this function assumes it has already passed.
pub(crate) async fn run(restart_unit: Option<String>, dry_run: bool) -> Result<()> {
    let client = Client::new();

    let latest = client.latest_release().await?;
    let latest_version = parse_tag(&latest.tag_name)?;

    let current = current_version()?;
    if latest_version <= current {
        println!("Already up to date (current {current}, latest {latest_version}); nothing to do.");
        return Ok(());
    }

    let asset = pick_asset(&latest, TARGET_TRIPLE).with_context(|| {
        format!(
            "no release asset matching target triple {TARGET_TRIPLE:?} in tag {}; \
             assets: {:?}",
            latest.tag_name,
            latest.assets.iter().map(|a| &a.name).collect::<Vec<_>>()
        )
    })?;

    println!(
        "Upgrade available: {current} → {latest_version} ({})",
        asset.name
    );

    if dry_run {
        println!(
            "--dry-run: would download {} and replace this binary.",
            asset.url
        );
        return Ok(());
    }

    let bytes = client.download_and_verify(&latest, asset).await?;

    println!("Replacing this binary...");
    // self_replace takes a path to the new executable, not bytes: stage the
    // verified payload in a temp file first. self_replace's atomic semantics
    // mean the current binary is untouched if the swap fails.
    let tmp_path = stage_binary(&bytes)?;
    if let Err(e) = self_replace::self_replace(&tmp_path) {
        // Best-effort cleanup of the staged file on failure.
        let _ = std::fs::remove_file(&tmp_path);
        return Err(map_replace_error(e));
    }

    if let Some(unit) = restart_unit.as_deref() {
        restart_systemd_unit(unit);
    }
    println!("Upgrade complete.");
    Ok(())
}

/// Map a self-replace failure into a hint that the binary lives on a read-only
/// or package-managed filesystem.
fn map_replace_error(e: std::io::Error) -> anyhow::Error {
    if matches!(
        e.kind(),
        std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::ReadOnlyFilesystem
    ) {
        anyhow::Error::from(e).context(
            "binary location is read-only — is this install managed by a package manager? \
             (set BEPOSITORY_PACKAGE_MANAGED if so)",
        )
    } else {
        anyhow::Error::from(e).context("failed to replace binary")
    }
}

/// Stage the verified payload as an executable temp file and return its path.
/// Uses only stdlib: avoids adding a tempfile dependency. The caller is
/// responsible for removing the file on a failed swap.
fn stage_binary(bytes: &[u8]) -> Result<std::path::PathBuf> {
    use std::io::Write as _;
    use std::os::unix::fs::PermissionsExt;
    let mut dir = std::env::temp_dir();
    // Keep it near the real exe's dir if possible so self_replace can rename
    // atomically (cross-device rename falls back to copy+unlink inside
    // self_replace, but same-dir avoids that).
    if let Ok(exe) = std::env::current_exe()
        && let Some(parent) = exe.parent()
        && parent.is_dir()
    {
        dir = parent.to_path_buf();
    }
    let path = dir.join(format!(
        ".bepository-upgrade-staging.{}",
        std::process::id()
    ));
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .with_context(|| format!("failed to create staging file {}", path.display()))?;
    f.write_all(bytes)
        .context("failed to write staged binary")?;
    // Sync before chmod so the bytes are durable on crash; best-effort.
    let _ = f.sync_all();
    f.set_permissions(std::fs::Permissions::from_mode(0o755))
        .context("failed to set exec permissions on staged binary")?;
    Ok(path)
}

/// Best-effort `systemctl try-restart <unit>`; absence of systemctl is a soft skip.
fn restart_systemd_unit(unit: &str) {
    match std::process::Command::new("systemctl")
        .args(["try-restart", unit])
        .status()
    {
        Ok(s) if s.success() => println!("Restarted {unit}."),
        Ok(s) => tracing::warn!(status = %s, unit = unit, "systemctl try-restart did not succeed"),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::warn!("systemctl not found on PATH; could not restart {unit}");
        }
        Err(e) => tracing::warn!(error = %e, unit = unit, "failed to run systemctl try-restart"),
    }
}

/// Minimal GitHub release API types.
#[derive(serde::Deserialize)]
struct Release {
    tag_name: String,
    assets: Vec<Asset>,
}

#[derive(serde::Deserialize)]
struct Asset {
    name: String,
    /// Direct download URL for the asset's binary content. GitHub's asset JSON
    /// has two URL fields: `url` (the API metadata endpoint, returns JSON unless
    /// sent `Accept: application/octet-stream`) and `browser_download_url`
    /// (the direct binary link). We need the binary — use the latter.
    #[serde(rename = "browser_download_url")]
    url: String,
}

/// HTTP client wrapper: a configured reqwest client with a User-Agent
/// (GitHub's API returns 403 without one).
struct Client {
    http: reqwest::Client,
}

impl Client {
    fn new() -> Self {
        let http = reqwest::Client::builder()
            .user_agent(format!("bepository-updater/{}", env!("CARGO_PKG_VERSION")))
            .build()
            .expect("reqwest client build cannot fail with only a user agent");
        Self { http }
    }

    async fn latest_release(&self) -> Result<Release> {
        let url = format!("{}/releases/latest", release_base_url());
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .context("GitHub API request failed")?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("GitHub API {url} returned {status}: {body}");
        }
        resp.json::<Release>()
            .await
            .with_context(|| format!("failed to parse release JSON from {url}"))
    }

    async fn download_asset(&self, url: &str) -> Result<Vec<u8>> {
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .context("asset download request failed")?;
        let status = resp.status();
        if !status.is_success() {
            bail!("asset download {url} returned {status}");
        }
        let bytes = resp
            .bytes()
            .await
            .with_context(|| format!("failed to read asset body from {url}"))?;
        Ok(bytes.to_vec())
    }

    /// Download a text asset (by name) attached to a release.
    async fn download_text_asset(&self, release: &Release, name: &str) -> Result<String> {
        let asset = release
            .assets
            .iter()
            .find(|a| a.name == name)
            .ok_or_else(|| anyhow!("release {} has no {name} asset", release.tag_name))?;
        let bytes = self.download_asset(&asset.url).await?;
        String::from_utf8(bytes).context(format!("{name} is not valid UTF-8"))
    }

    /// Download a release's primary asset and verify it against the release's
    /// `SHA256SUMS`. Returns the verified bytes. Extracted from `run` so the
    /// real network + verify path (the part that broke in B1) is testable
    /// against a local fixture without self-replacing the test binary.
    async fn download_and_verify(&self, release: &Release, asset: &Asset) -> Result<Vec<u8>> {
        let bytes = self.download_asset(&asset.url).await?;
        let sums = self.download_text_asset(release, "SHA256SUMS").await?;
        let expected = checksum_for(&sums, &asset.name)
            .with_context(|| format!("no entry for {} in SHA256SUMS:\n{sums}", asset.name))?;
        verify_hash(&bytes, &expected)?;
        Ok(bytes)
    }
}

/// Inline hex encoder for sha256 (64 hex chars). Kept local to avoid adding a
/// `hex` crate the plan didn't list.
fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn asset(name: &str, url: &str) -> Asset {
        Asset {
            name: name.to_string(),
            url: url.to_string(),
        }
    }

    fn release(tag: &str, assets: Vec<Asset>) -> Release {
        Release {
            tag_name: tag.to_string(),
            assets,
        }
    }

    #[test]
    fn parse_tag_strips_v_prefix() {
        assert_eq!(parse_tag("v0.8.0").unwrap(), Version::new(0, 8, 0));
        assert_eq!(parse_tag("0.8.1").unwrap(), Version::new(0, 8, 1));
        assert!(parse_tag("not-a-version").is_err());
    }

    #[test]
    fn pick_asset_matches_triple_suffix() {
        let r = release(
            "v0.8.0",
            vec![
                asset("bepository-aarch64-unknown-linux-musl", "u1"),
                asset("bepository-x86_64-unknown-linux-musl", "u2"),
                asset("SHA256SUMS", "u3"),
            ],
        );
        let picked = pick_asset(&r, "x86_64-unknown-linux-musl").unwrap();
        assert_eq!(picked.name, "bepository-x86_64-unknown-linux-musl");
        assert_eq!(picked.url, "u2");
        assert!(pick_asset(&r, "nope-triple").is_none());
    }

    #[test]
    fn checksum_for_parses_sums_file() {
        let sums = "\
abcdef0123456789  bepository-x86_64-unknown-linux-musl\n\
FEDCBA9876543210  bepository-aarch64-unknown-linux-musl\n";
        // lowercased regardless of input case
        assert_eq!(
            checksum_for(sums, "bepository-x86_64-unknown-linux-musl").unwrap(),
            "abcdef0123456789"
        );
        assert_eq!(
            checksum_for(sums, "bepository-aarch64-unknown-linux-musl").unwrap(),
            "fedcba9876543210"
        );
        assert!(checksum_for(sums, "missing").is_none());
    }

    #[test]
    fn verify_hash_accepts_match_rejects_mismatch() {
        let payload = b"hello bepository";
        let digest = hex_encode(&Sha256::digest(payload));
        // Correct hash → Ok.
        assert!(verify_hash(payload, &digest).is_ok());
        // Wrong hash → Err, and the message names both digests.
        let err = verify_hash(
            payload,
            "0000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("checksum mismatch"), "{msg}");
    }

    /// A tiny hyper server impersonating the GitHub releases API for the
    /// upgrade path. Serves `/releases/latest` (JSON with `browser_download_url`
    /// pointing back at the fixture), plus the binary asset and SHA256SUMS.
    /// Returns the base URL to pass via `BEPOSITORY_RELEASE_BASE_URL`.
    async fn github_fixture(
        asset_name: String,
        asset_bytes: Vec<u8>,
        sums: String,
        tag: String,
    ) -> String {
        use std::convert::Infallible;
        use std::net::SocketAddr;

        use http_body_util::Full;
        use hyper::body::Bytes;
        use hyper::server::conn::http1;
        use hyper::service::service_fn;
        use hyper::{Request, Response, StatusCode};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        let asset_url = format!("http://{addr}/{asset_name}");
        let sums_url = format!("http://{addr}/SHA256SUMS");
        let release_json = format!(
            r#"{{"tag_name":"{tag}","assets":[
               {{"name":"{asset_name}","browser_download_url":"{asset_url}"}},
               {{"name":"SHA256SUMS","browser_download_url":"{sums_url}"}}
             ]}}"#
        );

        tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(s) => s,
                    Err(_) => break,
                };
                let release_json = release_json.clone();
                let asset_bytes = asset_bytes.clone();
                let sums = sums.clone();
                let asset_name = asset_name.clone();
                let io = hyper_util::rt::TokioIo::new(stream);
                tokio::spawn(async move {
                    let svc = service_fn(move |req: Request<hyper::body::Incoming>| {
                        let release_json = release_json.clone();
                        let asset_bytes = asset_bytes.clone();
                        let sums = sums.clone();
                        let asset_name = asset_name.clone();
                        async move {
                            let path = req.uri().path();
                            let resp: Result<Response<Full<Bytes>>, Infallible> = Ok(match path {
                                "/releases/latest" => {
                                    Response::new(Full::new(Bytes::from(release_json)))
                                }
                                "/SHA256SUMS" => Response::new(Full::new(Bytes::from(sums))),
                                p if p == format!("/{asset_name}") => {
                                    Response::new(Full::new(Bytes::from(asset_bytes)))
                                }
                                _ => Response::builder()
                                    .status(StatusCode::NOT_FOUND)
                                    .body(Full::new(Bytes::new()))
                                    .unwrap(),
                            });
                            resp
                        }
                    });
                    let _ = http1::Builder::new().serve_connection(io, svc).await;
                });
            }
        });
        format!("http://{addr}")
    }

    /// End-to-end test of the network + verify path against a local fixture.
    /// This is the test that would have caught B1 (downloading JSON instead of
    /// the binary): `download_and_verify` must return the real asset bytes, and
    /// a wrong checksum must fail without returning bytes.
    #[tokio::test]
    async fn download_and_verify_against_fixture_returns_binary() {
        let _env = crate::test_env::EnvGuard::lock_async(&["BEPOSITORY_RELEASE_BASE_URL"]).await;
        let asset_name = "bepository-fake-triple";
        let asset_bytes = b"this is a fake binary payload";
        let mut hasher = Sha256::new();
        hasher.update(asset_bytes);
        let digest = hex_encode(&hasher.finalize());
        let sums = format!("{digest}  {asset_name}\n");

        let base = github_fixture(
            asset_name.to_string(),
            asset_bytes.to_vec(),
            sums,
            "v99.0.0".to_string(),
        )
        .await;
        // Safety: serialized by _env.
        unsafe { std::env::set_var("BEPOSITORY_RELEASE_BASE_URL", &base) };

        let client = Client::new();
        let release = client.latest_release().await.unwrap();
        let asset = pick_asset(&release, "fake-triple").expect("fixture has the asset");

        // The real path B1 broke: must get the binary bytes, not JSON metadata.
        let got = client.download_and_verify(&release, asset).await.unwrap();
        assert_eq!(got, asset_bytes);
    }

    /// A checksum mismatch must fail and surface both digests — the current
    /// binary would be left untouched (run() never reaches self_replace).
    #[tokio::test]
    async fn checksum_mismatch_against_fixture_fails_without_returning_bytes() {
        let _env = crate::test_env::EnvGuard::lock_async(&["BEPOSITORY_RELEASE_BASE_URL"]).await;
        let asset_name = "bepository-fake-triple";
        let asset_bytes = b"real payload";
        // Deliberately wrong checksum.
        let sums = format!(
            "0000000000000000000000000000000000000000000000000000000000000000  {asset_name}\n"
        );

        let base = github_fixture(
            asset_name.to_string(),
            asset_bytes.to_vec(),
            sums,
            "v99.0.0".to_string(),
        )
        .await;
        // Safety: serialized by _env.
        unsafe { std::env::set_var("BEPOSITORY_RELEASE_BASE_URL", &base) };

        let client = Client::new();
        let release = client.latest_release().await.unwrap();
        let asset = pick_asset(&release, "fake-triple").unwrap();
        let err = client
            .download_and_verify(&release, asset)
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("checksum mismatch"));
    }

    /// Older and equal releases are no-ops; the fixture is never hit. Uses the
    /// pure functions (no server needed) to cover the version-compare branch.
    #[test]
    fn version_gating_is_strictly_newer() {
        let current = Version::new(0, 8, 0);
        assert!(Version::new(0, 7, 5) <= current); // older → noop
        assert!(Version::new(0, 8, 0) <= current); // equal → noop
        assert!(Version::new(0, 8, 1) > current); // newer → upgrade
    }
}
