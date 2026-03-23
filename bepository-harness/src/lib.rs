// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Test harness for running and managing Syncthing binaries.
//!
//! # Usage
//!
//! ```rust,no_run
//! # #[tokio::main] async fn main() -> Result<(), bepository_harness::HarnessError> {
//! use std::path::Path;
//! use bepository_harness::Harness;
//!
//! let a = Harness::start().await?;
//! let b = Harness::start().await?;
//!
//! let ha = a.share(Path::new("/tmp/folder-a")).await?;
//! let hb = b.share_named(Path::new("/tmp/folder-b"), ha.folder_id()).await?;
//!
//! ha.add_peer(b.device_id(), b.listen_addr()).await?;
//! hb.add_peer(a.device_id(), a.listen_addr()).await?;
//! // Both sides now sync the same folder.
//! # Ok(())
//! # }
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::{Client, Response};
use tempfile::TempDir;
use thiserror::Error;
use tokio::process::{Child, Command};
use tokio::runtime::Handle;
use tokio::sync::Mutex;

#[derive(Debug, Error)]
pub enum HarnessError {
    #[error("syncthing binary not found (set SYNCTHING_BIN or add it to PATH)")]
    BinaryNotFound,

    #[error("failed to spawn syncthing process: {0}")]
    Io(#[from] std::io::Error),

    #[error("syncthing API did not become ready within the timeout")]
    StartupTimeout,

    #[error("syncthing API error: HTTP {status}: {body}")]
    ApiError { status: u16, body: String },

    #[error("certificate generation failed: {0}")]
    CertGen(#[from] rcgen::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("HTTP client error: {0}")]
    Reqwest(#[from] reqwest::Error),
}

/// A running Syncthing instance managed by the test harness.
///
/// Dropping this value kills the child process and removes the temporary
/// config/data directory.
pub struct Harness {
    _temp_dir: TempDir,
    _child: Child,
    client: RestClient,
    device_id: String,
    listen_addr: String,
}

impl Harness {
    /// Spawn a new Syncthing instance and wait for its REST API to become ready.
    ///
    /// Looks for the binary via the `SYNCTHING_BIN` env-var, then `PATH`.
    pub async fn start() -> Result<Self, HarnessError> {
        let temp_dir = tempfile::tempdir()?;

        let api_port = find_free_port()?;
        let bep_port = find_free_port()?;
        let cfg = generate_config(api_port, bep_port)?;
        tokio::fs::write(temp_dir.path().join("cert.pem"), &cfg.cert_pem).await?;
        tokio::fs::write(temp_dir.path().join("key.pem"), &cfg.key_pem).await?;
        tokio::fs::write(temp_dir.path().join("config.xml"), &cfg.config_xml).await?;

        let binary = find_binary()?;
        let child = spawn_process(&binary, temp_dir.path())?;

        let client = RestClient::new(cfg.api_port, &cfg.api_key);
        wait_for_ready(&client, Duration::from_secs(15)).await?;

        let device_id = client.get_my_id().await?;
        let listen_addr = format!("tcp://127.0.0.1:{}", cfg.bep_port);

        tracing::info!(
            api_port = cfg.api_port,
            bep_port = cfg.bep_port,
            "syncthing started"
        );

        Ok(Self {
            _temp_dir: temp_dir,
            _child: child,
            client,
            device_id,
            listen_addr,
        })
    }

    /// The Syncthing device ID for this instance, in canonical
    /// `XXXXXXX-XXXXXXX-…` format.
    #[must_use]
    pub fn device_id(&self) -> &str {
        &self.device_id
    }

    /// The BEP listen address for this instance, e.g. `"tcp://127.0.0.1:22000"`.
    #[must_use]
    pub fn listen_addr(&self) -> &str {
        &self.listen_addr
    }

    /// The REST API client for this instance (for advanced test scenarios).
    #[must_use]
    pub fn client(&self) -> &RestClient {
        &self.client
    }

    /// Share `local_path` as a new Syncthing folder with a random ID.
    ///
    /// Use [`FolderHandle::add_peer`] to add peers to the folder after creation.
    /// The folder is removed when the returned handle is dropped.
    pub async fn share(&self, local_path: &Path) -> Result<FolderHandle, HarnessError> {
        use rand::RngExt;
        let mut rng = rand::rng();
        let folder_id = format!("{:08x}-{:08x}", rng.random::<u32>(), rng.random::<u32>());
        self.share_named(local_path, &folder_id).await
    }

    /// Share `local_path` as a Syncthing folder with the given `folder_id`.
    ///
    /// Use this when multiple instances need to sync the same folder: call
    /// `share` on the first instance to get a folder ID, then `share_named`
    /// on the remaining instances with that same ID.
    ///
    /// Use [`FolderHandle::add_peer`] to wire instances together.
    /// The folder is removed when the returned handle is dropped.
    pub async fn share_named(
        &self,
        local_path: &Path,
        folder_id: &str,
    ) -> Result<FolderHandle, HarnessError> {
        let body = serde_json::json!({
            "id": folder_id,
            "label": "",
            "path": local_path.to_string_lossy(),
            "type": "sendreceive",
            "devices": [],
            "rescanIntervalS": 5,
            "fsWatcherEnabled": true,
            "fsWatcherDelayS": 1,
        });
        self.client.post_json("/rest/config/folders", &body).await?;
        Ok(FolderHandle::new(
            folder_id.to_owned(),
            self.device_id.clone(),
            self.client.clone(),
        ))
    }
}

// ---------------------------------------------------------------------------
// REST Client
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct RestClient {
    inner: Client,
    api_base: String,
    api_key: String,
}

impl RestClient {
    pub(crate) fn new(api_port: u16, api_key: &str) -> Self {
        Self {
            inner: Client::new(),
            api_base: format!("http://127.0.0.1:{api_port}"),
            api_key: api_key.to_owned(),
        }
    }

    pub async fn ping(&self) -> Result<(), HarnessError> {
        self.get("/rest/system/ping").await?;
        Ok(())
    }

    /// Returns the device ID Syncthing computed from its own certificate.
    pub async fn get_my_id(&self) -> Result<String, HarnessError> {
        let body: serde_json::Value = self.get("/rest/system/status").await?.json().await?;
        let id = body["myID"]
            .as_str()
            .ok_or_else(|| HarnessError::ApiError {
                status: 0,
                body: "missing myID".into(),
            })?
            .to_owned();
        Ok(id)
    }

    pub async fn post_json(
        &self,
        path: &str,
        body: &impl serde::Serialize,
    ) -> Result<(), HarnessError> {
        let resp = self
            .inner
            .post(format!("{}{path}", self.api_base))
            .header("X-API-Key", &self.api_key)
            .json(body)
            .send()
            .await?;
        tracing::debug!(method = "POST", path = %path, status = resp.status().as_u16(), "rest call");
        check_status(resp).await?;
        Ok(())
    }

    pub async fn delete(&self, path: &str) -> Result<(), HarnessError> {
        let resp = self
            .inner
            .delete(format!("{}{path}", self.api_base))
            .header("X-API-Key", &self.api_key)
            .send()
            .await?;
        tracing::debug!(method = "DELETE", path = %path, status = resp.status().as_u16(), "rest call");
        check_status(resp).await?;
        Ok(())
    }

    pub async fn delete_folder(&self, folder_id: &str) -> Result<(), HarnessError> {
        self.delete(&format!("/rest/config/folders/{folder_id}"))
            .await
    }

    pub async fn get_folder(&self, folder_id: &str) -> Result<serde_json::Value, HarnessError> {
        Ok(self
            .get(&format!("/rest/config/folders/{folder_id}"))
            .await?
            .json()
            .await?)
    }

    pub async fn put_folder(
        &self,
        folder_id: &str,
        config: &serde_json::Value,
    ) -> Result<(), HarnessError> {
        let path = format!("/rest/config/folders/{folder_id}");
        let resp = self
            .inner
            .put(format!("{}{path}", self.api_base))
            .header("X-API-Key", &self.api_key)
            .json(config)
            .send()
            .await?;
        tracing::debug!(method = "PUT", path = %path, status = resp.status().as_u16(), "rest call");
        check_status(resp).await?;
        Ok(())
    }

    pub async fn add_device(&self, device_id: &str, addr: &str) -> Result<(), HarnessError> {
        let body = serde_json::json!({
            "deviceID": device_id,
            "name": "",
            "addresses": [addr],
            "compression": "metadata",
            "introducer": false,
            "skipIntroductionRemovals": false,
        });
        self.post_json("/rest/config/devices", &body).await
    }

    async fn get(&self, path: &str) -> Result<Response, HarnessError> {
        let resp = self
            .inner
            .get(format!("{}{path}", self.api_base))
            .header("X-API-Key", &self.api_key)
            .send()
            .await?;
        tracing::debug!(method = "GET", path = %path, status = resp.status().as_u16(), "rest call");
        check_status(resp).await
    }
}

async fn check_status(resp: Response) -> Result<Response, HarnessError> {
    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let body = resp
            .text()
            .await
            .unwrap_or_else(|e| format!("failed to read body: {e}"));
        return Err(HarnessError::ApiError { status, body });
    }
    Ok(resp)
}

// ---------------------------------------------------------------------------
// Folder Handle
// ---------------------------------------------------------------------------

/// Handle to a folder shared via a [`Harness`] instance.
///
/// Dropping this handle removes the folder from the Syncthing instance's
/// config via the REST API (best-effort; errors are logged to stderr).
pub struct FolderHandle {
    folder_id: String,
    device_id: String,
    client: RestClient,
    rt: Handle,
    /// Serialises concurrent `add_peer` calls: the GET-modify-PUT sequence
    /// must not interleave with itself on the same folder.
    mu: Arc<Mutex<()>>,
}

impl FolderHandle {
    pub(crate) fn new(folder_id: String, device_id: String, client: RestClient) -> Self {
        Self {
            folder_id,
            device_id,
            client,
            rt: Handle::current(),
            mu: Arc::new(Mutex::new(())),
        }
    }

    /// The Syncthing folder ID (e.g. `"a1b2c3d4-e5f6a7b8"`).
    #[must_use]
    pub fn folder_id(&self) -> &str {
        &self.folder_id
    }

    /// The device ID of the owning [`Harness`] instance.
    #[must_use]
    pub fn device_id(&self) -> &str {
        &self.device_id
    }

    /// Register `peer_device_id` with this Syncthing instance and add it as a
    /// participant in this folder.
    ///
    /// `peer_listen_addr` is the BEP address the peer advertises, e.g.
    /// `"tcp://127.0.0.1:22000"`.
    pub async fn add_peer(
        &self,
        peer_device_id: &str,
        peer_listen_addr: &str,
    ) -> Result<(), HarnessError> {
        // 1. Register the device with this instance (idempotent, no lock needed).
        self.client
            .add_device(peer_device_id, peer_listen_addr)
            .await?;

        // 2. Serialise the GET-modify-PUT so concurrent calls on the same handle
        //    don't clobber each other's device list.
        let _guard = self.mu.lock().await;
        let mut config = self.client.get_folder(&self.folder_id).await?;
        let devices = config["devices"]
            .as_array_mut()
            .ok_or_else(|| HarnessError::ApiError {
                status: 0,
                body: "missing devices array".into(),
            })?;
        devices.push(serde_json::json!({
            "deviceID": peer_device_id,
            "introducedBy": "",
            "encryptionPassword": "",
        }));
        self.client.put_folder(&self.folder_id, &config).await?;
        tracing::info!(remote_device = %peer_device_id, "peer added");
        Ok(())
    }
}

impl Drop for FolderHandle {
    fn drop(&mut self) {
        let client = self.client.clone();
        let folder_id = self.folder_id.clone();
        self.rt.spawn(async move {
            if let Err(e) = client.delete_folder(&folder_id).await {
                eprintln!("bepository-harness: failed to remove folder {folder_id}: {e}");
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Process Management
// ---------------------------------------------------------------------------

fn find_free_port() -> Result<u16, HarnessError> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

fn find_binary() -> Result<PathBuf, HarnessError> {
    if let Ok(val) = std::env::var("SYNCTHING_BIN") {
        let p = PathBuf::from(val);
        if p.exists() {
            return Ok(p);
        }
        return Err(HarnessError::BinaryNotFound);
    }

    std::env::var("PATH")
        .ok()
        .and_then(|paths| {
            paths.split(':').find_map(|dir| {
                let candidate = Path::new(dir).join("syncthing");
                candidate.exists().then_some(candidate)
            })
        })
        .ok_or(HarnessError::BinaryNotFound)
}

fn spawn_process(binary: &Path, home_dir: &Path) -> Result<Child, HarnessError> {
    let child = Command::new(binary)
        .arg("--no-browser")
        .arg("--no-restart")
        .arg(format!("--home={}", home_dir.display()))
        .kill_on_drop(true)
        .spawn()?;
    Ok(child)
}

async fn wait_for_ready(client: &RestClient, timeout: Duration) -> Result<(), HarnessError> {
    let deadline = Instant::now() + timeout;
    loop {
        if Instant::now() >= deadline {
            return Err(HarnessError::StartupTimeout);
        }
        if client.ping().await.is_ok() {
            tracing::debug!("syncthing ready");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// ---------------------------------------------------------------------------
// Config Generation
// ---------------------------------------------------------------------------

struct InstanceConfig {
    api_key: String,
    cert_pem: String,
    key_pem: String,
    config_xml: String,
    api_port: u16,
    bep_port: u16,
}

fn generate_config(api_port: u16, bep_port: u16) -> Result<InstanceConfig, HarnessError> {
    let cert = rcgen::generate_simple_self_signed(vec!["syncthing".to_string()])?;
    let cert_pem = cert.cert.pem();
    let key_pem = cert.signing_key.serialize_pem();
    let api_key = generate_api_key();
    let config_xml = config_xml_str(&api_key, api_port, bep_port);
    Ok(InstanceConfig {
        api_key,
        cert_pem,
        key_pem,
        config_xml,
        api_port,
        bep_port,
    })
}

fn generate_api_key() -> String {
    use rand::RngExt;
    let mut rng = rand::rng();
    (0..32)
        .map(|_| format!("{:02x}", rng.random::<u8>()))
        .collect()
}

fn config_xml_str(api_key: &str, api_port: u16, bep_port: u16) -> String {
    format!(
        r#"<configuration version="37">
  <gui enabled="true" tls="false" debugging="false">
    <address>127.0.0.1:{api_port}</address>
    <apikey>{api_key}</apikey>
  </gui>
  <options>
    <listenAddress>tcp://127.0.0.1:{bep_port}</listenAddress>
    <globalAnnounceEnabled>false</globalAnnounceEnabled>
    <localAnnounceEnabled>false</localAnnounceEnabled>
    <relaysEnabled>false</relaysEnabled>
    <startBrowser>false</startBrowser>
    <reconnectionIntervalS>5</reconnectionIntervalS>
    <autoUpgradeIntervalH>0</autoUpgradeIntervalH>
    <urAccepted>-1</urAccepted>
  </options>
</configuration>
"#
    )
}
