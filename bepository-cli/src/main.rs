// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

use anyhow::{Context, Result, anyhow};
use bepository_bep::ids::FolderId;
use bepository_bep::proto::bep::FileInfo;
use bepository_bep::{
    BepEngine, ConflictResolution, ConflictResolver, DeviceId, EngineEvent, StorageError,
};
use bepository_lock::{LockGuard, LockLost};
use bepository_storage::{CacheProvider, CheckpointSchedule, SlateStorage};
use bepository_tls::Identity;
use clap::{Parser, Subcommand};
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use std::collections::HashMap;
use std::fs;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use uuid::Uuid;

/// Conflict resolver that always accepts the remote (non-bepository) version.
/// The local version is noted but not backed up — the remote host retains it.
struct TheirsResolver;

#[cfg(feature = "self-manage")]
mod service;
#[cfg(feature = "self-manage")]
mod upgrade;

impl ConflictResolver for TheirsResolver {
    fn resolve<'a>(
        &self,
        local: &'a FileInfo,
        _local_device: &DeviceId,
        remote: &'a FileInfo,
        _remote_device: &DeviceId,
    ) -> bepository_bep::error::Result<ConflictResolution<'a>> {
        Ok(ConflictResolution {
            winner: remote,
            loser: local,
            loser_path: None,
        })
    }
}

#[derive(Parser)]
#[command(name = "bepository")]
#[command(about = "Cold storage bridge daemon for Syncthing", long_about = None)]
struct Cli {
    /// Override the machine ID (for tests or cross-machine identity).
    #[arg(long, global = true, env = "BEPOSITORY_MACHINE_ID")]
    machine_id: Option<String>,
    /// Set the tracing level (e.g., info, debug, trace)
    #[arg(long, global = true, env = "BEPOSITORY_LOG")]
    trace: Option<String>,
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Args, Clone)]
struct CacheArgs {
    /// Override the Foyer block-cache directory.
    /// Defaults to $BEPOSITORY_CACHE_DIRECTORY, $CACHE_DIRECTORY (systemd) or the XDG cache dir.
    #[arg(long, value_name = "PATH")]
    cache_dir: Option<PathBuf>,
    /// Disable the Foyer block cache entirely.
    #[arg(long, env = "BEPOSITORY_NO_CACHE", conflicts_with = "cache_dir")]
    no_cache: bool,
}

impl CacheProvider for CacheArgs {
    fn get_cache_dir(&self, device_id: &DeviceId) -> Option<PathBuf> {
        if self.no_cache {
            return None;
        }
        let base = self.cache_dir.clone().or_else(get_cache_dir)?;
        Some(base.join(device_id.to_string()))
    }
}

#[derive(clap::Args)]
struct StorageCli {
    /// The path or URI to the SlateDB storage (e.g., s3://bucket/path, file:///tmp/sync).
    #[arg(long, short = 's', env = "BEPOSITORY_STORAGE_URI")]
    storage_uri: String,
    #[command(flatten)]
    cache: CacheArgs,
}

type StorageStack = (
    Arc<dyn ObjectStore>,
    Arc<dyn CacheProvider + Send + Sync>,
    SlateStorage,
);

impl StorageCli {
    fn open(&self, create: bool, runtime: tokio::runtime::Handle) -> Result<StorageStack> {
        let store = parse_storage_uri(&self.storage_uri, create)?;
        let cache = Arc::new(self.cache.clone());
        let db = SlateStorage::new(store.clone(), Some(cache.clone()), runtime);
        Ok((store, cache, db))
    }
}

#[derive(Subcommand)]
enum Commands {
    /// Initializes storage and generates a TLS identity.
    ///
    /// Creates a new TLS certificate on first run. Idempotent — re-running
    /// on an already-initialized store is a no-op.
    /// Acquires the distributed lock to prevent races with serve.
    ///
    /// Example: bepository init -s s3://my-bucket/backup
    Init {
        #[command(flatten)]
        storage: StorageCli,
    },
    /// Permanently remove a folder and its data from storage.
    ///
    /// Acquires the distributed lock and recursively deletes all object store
    /// keys associated with the folder.
    RemoveFolder {
        #[command(flatten)]
        storage: StorageCli,
        /// The folder ID to remove (as shown in Syncthing).
        folder: String,
    },
    /// Retrieves the Device ID of this bepository instance.
    ///
    /// This ID must be added to your local Syncthing "Remote Devices" list.
    GetId {
        #[command(flatten)]
        storage: StorageCli,
    },
    /// Runs the bepository daemon.
    ///
    /// Automatically accepts and registers all folders proposed by the peer.
    ///
    /// Example: bepository serve -s s3://my-bucket/backup L773...
    Serve {
        #[command(flatten)]
        storage: StorageCli,
        /// The Device ID of the master of this instance.
        #[arg(env = "BEPOSITORY_MASTER_DEVICE_ID", value_name = "MASTER_DEVICE_ID")]
        master_device_id: String,
        /// The address to listen on for BEP connections.
        #[arg(long, env = "BEPOSITORY_LISTEN", default_value = "127.0.0.1:0")]
        listen: String,
        /// Lock priority. Higher priority can preempt lower ones.
        #[arg(long, env = "BEPOSITORY_PRIORITY", default_value_t = 100)]
        priority: u32,
        /// Lease duration in seconds (minimum 180).
        #[arg(long, env = "BEPOSITORY_LEASE", default_value_t = 180, value_parser = clap::value_parser!(u64).range(180..))]
        lease: u64,
    },
    /// Manage checkpoint schedules for point-in-time recovery.
    ///
    /// Checkpoints are stored inside each folder's SlateDB instance and managed
    /// by SlateDB's built-in checkpoint GC (via TTL). Requires SlateDB storage —
    /// in-memory storage does not persist checkpoints across restarts.
    ///
    /// Examples:
    ///   bepository checkpoint -s s3://bucket/path every 1h ttl 7d
    ///   bepository checkpoint -s s3://bucket/path every 1h remove
    ///   bepository checkpoint -s s3://bucket/path list
    Checkpoint {
        #[command(flatten)]
        storage: StorageCli,
        #[command(subcommand)]
        action: CheckpointAction,
    },
    /// Integrity check and manual maintenance tool.
    Fsck {
        #[command(flatten)]
        storage: StorageCli,
        /// Run filesystem consistency checks at the specified level.
        #[arg(long, value_enum)]
        check: Option<FsckLevel>,
        /// Generate a new TLS certificate and replace the one in the SlateDB index.
        #[arg(long)]
        regenerate_id: bool,
        /// Forcibly clear distributed locks.
        #[arg(long)]
        clear_lock: bool,
        /// Trigger compaction on the SlateDB instance, reclaiming space from
        /// orphaned blocks and optimizing read performance.
        #[arg(long)]
        compact: bool,
    },
    /// Print the systemd service unit to stdout.
    #[cfg(feature = "self-manage")]
    PrintService,
    /// Install the systemd service unit (and upgrade timer, unless
    /// `--no-auto-upgrade`), enable it, and seed `/etc/bepository/env`.
    ///
    /// Requires root. Idempotent.
    #[cfg(feature = "self-manage")]
    InstallService {
        /// Do not install the daily self-upgrade timer.
        #[arg(long)]
        no_auto_upgrade: bool,
    },
    /// Disable and remove the systemd service units. Leaves config in place.
    #[cfg(feature = "self-manage")]
    UninstallService,
    /// Self-upgrade this binary from the latest GitHub release.
    #[cfg(feature = "self-manage")]
    Upgrade {
        /// Restart this systemd unit after a successful upgrade.
        #[arg(long, value_name = "UNIT")]
        restart_unit: Option<String>,
        /// Print what would happen, but make no changes.
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum FsckLevel {
    Quick,
    Structural,
    Full,
}

#[derive(Subcommand)]
enum CheckpointAction {
    /// Add or update a checkpoint schedule.
    ///
    /// Duration formats: 10m, 1h, 6h, 1d, 7d, etc.
    /// Minimum interval is 10 minutes.
    ///
    /// Example: bepository checkpoint s3://bucket/path every 1h ttl 7d
    Every {
        /// Checkpoint interval (e.g. "1h", "1d"). Minimum 10 minutes.
        #[arg(value_parser = humantime::parse_duration)]
        interval: Duration,
        #[command(subcommand)]
        op: EveryOp,
    },
    /// List checkpoint schedules and existing checkpoints.
    List,
    /// Serve a read-only WebDAV server exposing checkpoints for browsing and restore.
    ///
    /// The server is read-only and requires no distributed lock. Snapshots are
    /// loaded once at startup; restart to pick up new checkpoints.
    ///
    /// Requires BEPOSITORY_DAV_PASSWORD to be set (non-empty). The env var is
    /// cleared from the process environment immediately after reading.
    ///
    /// Example: BEPOSITORY_DAV_PASSWORD=secret bepository checkpoint -s s3://bucket/path serve 127.0.0.1:8080
    Serve {
        /// Listen address, e.g. 127.0.0.1:8080 or 0.0.0.0:8080
        addr: String,

        /// WebDAV password. Can be set via BEPOSITORY_DAV_PASSWORD environment variable.
        #[arg(long, env = "BEPOSITORY_DAV_PASSWORD", hide_env_values = true)]
        webdav_password: secrecy::SecretString,
    },
}

#[derive(Subcommand)]
enum EveryOp {
    /// Set the TTL for checkpoints created at this interval.
    Ttl {
        /// How long to keep each checkpoint (e.g. "1d", "7d").
        #[arg(value_parser = humantime::parse_duration)]
        value: Duration,
    },
    /// Remove this checkpoint schedule.
    Remove,
}

impl Commands {
    fn storage_uri(&self) -> &str {
        match self {
            Commands::Init { storage }
            | Commands::RemoveFolder { storage, .. }
            | Commands::GetId { storage }
            | Commands::Serve { storage, .. }
            | Commands::Fsck { storage, .. }
            | Commands::Checkpoint { storage, .. } => &storage.storage_uri,
            // The self-manage subcommands do not open the store; give them a
            // distinct lock scope so they don't collide with a running daemon.
            #[cfg(feature = "self-manage")]
            Commands::PrintService
            | Commands::InstallService { .. }
            | Commands::UninstallService
            | Commands::Upgrade { .. } => "self-manage",
        }
    }
}

/// opendal's built-in registry omits sftp even when the feature is compiled in.
static REGISTER_SFTP: LazyLock<()> = LazyLock::new(|| {
    opendal::DEFAULT_OPERATOR_REGISTRY
        .register::<opendal::services::Sftp>(opendal::services::SFTP_SCHEME);
});

fn merge_object_store_opts(parsed_url: &url::Url) -> Vec<(String, String)> {
    // Merge opts: defaults (lowest precedence), then URL query params, then env vars.
    // Later entries win when object_store's builder sees duplicates.
    //
    // The object_store default per-request timeout is 30s, which is far too short
    // for large SST uploads during SlateDB compaction (50–100 MB parts at cloud
    // storage throughput). Increase it to 10 minutes; users can still override via
    // URL query param or env var.
    let mut opts: Vec<(String, String)> = vec![("timeout".to_string(), "10min".to_string())];
    opts.extend(
        parsed_url
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .chain(std::env::vars()),
    );

    // Translate aliases that object_store's GCS builder doesn't recognise natively.
    // These cover both env-var usage and URL query-param usage (case-insensitive).
    // Without this, GCS silently falls back to the GCE metadata endpoint and times
    // out when running off-cloud.
    const ALIASES: &[(&str, &str)] = &[
        (
            "google_application_credentials",
            "google_service_account_path",
        ),
        ("cloudsdk_auth_access_token", "bearer_token"),
    ];
    let extra: Vec<(String, String)> = opts
        .iter()
        .filter_map(|(k, v)| {
            let k_lower = k.to_lowercase();
            ALIASES
                .iter()
                .find(|(alias, _)| *alias == k_lower)
                .map(|(_, canonical)| (canonical.to_string(), v.clone()))
        })
        .collect();
    opts.extend(extra);
    opts
}

fn open_memory() -> Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(object_store::memory::InMemory::new()))
}

fn open_local(uri: &str, create: bool) -> Result<Arc<dyn ObjectStore>> {
    let path_str = uri.strip_prefix("file://").unwrap_or(uri);
    let path = Path::new(path_str);

    if !path.exists() {
        if create {
            std::fs::create_dir_all(path).with_context(|| {
                format!("failed to create storage directory: {}", path.display())
            })?;
        } else {
            return Err(anyhow!(
                "Storage directory '{path_str}' does not exist. Hint: use 'init' to create it."
            ));
        }
    }

    let store = LocalFileSystem::new_with_prefix(path)
        .with_context(|| format!("failed to initialize local storage at {}", path.display()))?;
    Ok(Arc::new(store))
}

fn open_sftp(uri: &str) -> Result<Arc<dyn ObjectStore>> {
    LazyLock::force(&REGISTER_SFTP);
    let op = opendal::Operator::from_uri(uri)
        .with_context(|| format!("failed to initialize sftp operator for {uri}"))?;
    Ok(Arc::new(object_store_opendal::OpendalStore::new(op)))
}

fn open_object_store(parsed_url: url::Url) -> Result<Arc<dyn ObjectStore>> {
    let opts = merge_object_store_opts(&parsed_url);

    // Strip query string: parse_url_opts uses the URL only for scheme + bucket.
    let mut store_url = parsed_url.clone();
    store_url.set_query(None);

    let (store, prefix) = object_store::parse_url_opts(&store_url, opts).with_context(|| {
        let scheme = store_url.scheme();
        let hint = match scheme {
            "s3" => " (Hint: check region, access keys, or endpoint)",
            "gs" => " (Hint: check project ID or credentials)",
            _ => "",
        };
        format!("failed to initialize {scheme} object store at {store_url}{hint}")
    })?;

    let store: Arc<dyn ObjectStore> = if prefix.as_ref().is_empty() {
        Arc::new(store)
    } else {
        Arc::new(object_store::prefix::PrefixStore::new(store, prefix))
    };
    Ok(store)
}

/// Parses a storage URI into an ObjectStore.
///
/// Local paths and `file://` URIs are handled via `LocalFileSystem`; `sftp://`
/// goes through opendal; everything else (s3, gs, http/https) uses the native
/// object_store implementation, configured from environment variables.
fn parse_storage_uri(uri: &str, create: bool) -> Result<Arc<dyn ObjectStore>> {
    if uri.starts_with("memory://") {
        return open_memory();
    }

    let is_local = !uri.contains("://") || uri.starts_with("file://");
    if is_local {
        return open_local(uri, create);
    }

    let parsed_url = url::Url::parse(uri).with_context(|| format!("invalid storage URI: {uri}"))?;

    if parsed_url.scheme() == "sftp" {
        return open_sftp(uri);
    }

    open_object_store(parsed_url)
}

/// Acquire the distributed lock, run `f`, then release the lock.
///
/// The lock is always released on exit (even if `f` fails) to avoid leaving
/// stale locks after short-lived admin commands.
async fn with_admin_lock<F, Fut, T>(
    object_store: Arc<dyn ObjectStore>,
    storage: &SlateStorage,
    holder: &str,
    lease_secs: u64,
    f: F,
) -> Result<T>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let lock = bepository_lock::Lock::new(
        &*object_store,
        LOCK_PATH.clone(),
        holder.to_string(),
        0,
        lease_secs,
    );
    let status = lock.acquire().await.context("failed to acquire lock")?;
    let epoch = match status {
        bepository_lock::AcquisitionStatus::Owner(epoch) => epoch,
        _ => {
            return Err(anyhow!(
                "Could not acquire lock. Is another process running?"
            ));
        }
    };

    // Once the lock is acquired, every fallible operation must be captured
    // so we always reach release() — even on activate/close/f failure.
    let result = async {
        storage
            .activate(epoch)
            .await
            .map_err(into_activation_error)?;
        let r = f().await;
        storage.close().await?;
        r
    }
    .await;

    lock.release().await.context("failed to release lock")?;

    result
}

pub static LOCK_PATH: LazyLock<object_store::path::Path> =
    LazyLock::new(|| object_store::path::Path::from("_lock"));

/// Acquire the local single-instance lock for this machine + storage URI.
///
/// Read-only commands that never open SlateDB skip this and may run
/// alongside an active daemon.
fn acquire_single_instance(holder: &str) -> Result<single_instance::SingleInstance> {
    // Hash into a compact name to avoid platform/socket limits on lock identifier length.
    let lock_name = format!(
        "https://github.com/unbrice/bepository/#lock-{}",
        Uuid::new_v5(&Uuid::NAMESPACE_URL, holder.as_bytes()).simple()
    );
    let instance = single_instance::SingleInstance::new(&lock_name)
        .map_err(|e| anyhow!("failed to initialize single instance lock: {e}"))?;
    if !instance.is_single() {
        return Err(anyhow!(
            "Another instance of bepository is already running for this storage URI with this machine ID."
        ));
    }
    Ok(instance)
}

async fn require_identity(storage: &SlateStorage, locked: bool) -> Result<Identity> {
    let res = if locked {
        storage.get_identity()
    } else {
        storage.get_identity_unlocked().await
    };
    res.context("failed to read identity")?
        .ok_or_else(|| anyhow!("No identity found. Run 'init' first."))
}

fn spawn_cancel_on_ctrl_c(cancel: CancellationToken, multi_press_force_quit: bool) {
    tokio::spawn(
        async move {
            let _ = tokio::signal::ctrl_c().await;
            if multi_press_force_quit {
                tracing::info!("shutdown requested via Ctrl+C");
                println!("\nShutting down gracefully... (Ctrl+C 2 more times to force quit)");
                cancel.cancel();
                let _ = tokio::signal::ctrl_c().await;
                tracing::warn!("force quit requested");
                println!("Press Ctrl+C once more to force quit.");
                let _ = tokio::signal::ctrl_c().await;
                std::process::exit(0);
            } else {
                tracing::info!("shutdown requested");
                println!("\nShutting down gracefully...");
                cancel.cancel();
            }
        }
        .in_current_span(),
    );
}

async fn store_identity(storage: &SlateStorage, identity: &Identity) -> Result<()> {
    storage
        .put_identity(identity.cert_der(), identity.key_der())
        .await
        .context("failed to store identity")
}

/// Reject duration values below 10 minutes.
fn validate_checkpoint_duration(d: Duration) -> Result<Duration> {
    if d < Duration::from_secs(10 * 60) {
        return Err(anyhow!(
            "duration '{}' is too short — minimum checkpoint interval is 10 minutes",
            humantime::format_duration(d)
        ));
    }
    Ok(d)
}

#[tracing::instrument(level = "info", skip_all)]
pub async fn run_init(storage: &SlateStorage) -> Result<String> {
    let mut generated = false;
    let identity = if let Some(identity) = storage.get_identity()? {
        identity
    } else {
        generated = true;
        let id = Identity::generate().context("failed to generate TLS identity")?;
        store_identity(storage, &id).await?;
        id
    };
    storage
        .set_default_checkpoints()
        .await
        .context("failed to write default checkpoint schedules")?;

    let id = identity.device_id();
    if generated {
        tracing::info!(device_id = %id, "identity generated");
    } else {
        tracing::info!(device_id = %id, "identity loaded");
    }
    Ok(id.to_string())
}

pub async fn run_get_id(storage: &SlateStorage) -> Result<String> {
    let identity = require_identity(storage, false).await?;
    Ok(identity.device_id().to_string())
}

pub async fn fsck_regenerate_id(storage: &SlateStorage) -> Result<String> {
    let identity = Identity::generate().context("failed to generate new TLS identity")?;
    store_identity(storage, &identity).await?;
    Ok(identity.device_id().to_string())
}

pub async fn fsck_check(
    storage: &SlateStorage,
    level: bepository_storage::FsckLevel,
) -> Result<()> {
    let stream = storage.check_integrity(level);
    tokio::pin!(stream);
    let mut all_passed = true;
    while let Some(res) = stream.next().await {
        match res {
            Ok(bepository_storage::FsckEvent::FolderStarted { id }) => {
                println!("Checking '{id}'...");
            }
            Ok(bepository_storage::FsckEvent::FolderError { error, id }) => {
                tracing::error!(folder_id = %id, %error, "FSCK folder error");
                println!("  ERROR: {error}");
            }
            Ok(bepository_storage::FsckEvent::FolderFinished { errors_found, .. }) => {
                if errors_found == 0 {
                    println!("  OK.");
                } else {
                    all_passed = false;
                }
            }
            Err(e) => {
                return Err(anyhow::anyhow!("FSCK encountered a storage error: {e}"));
            }
        }
    }
    if !all_passed {
        Err(anyhow::anyhow!("FSCK found corruption"))
    } else {
        Ok(())
    }
}

pub async fn fsck_compact(storage: &SlateStorage) -> Result<()> {
    let folders = storage
        .list_folders()
        .context("failed to list folders for compaction")?;
    for (id, _, _dir_name) in &folders {
        storage
            .compact(*id)
            .await
            .with_context(|| format!("compaction failed for '{id}'"))?;
    }
    Ok(())
}

struct ServeConfig {
    store: Arc<dyn ObjectStore>,
    allowed_device: DeviceId,
    listen: String,
    holder: String,
    priority: u32,
    lease: u64,
    storage_uri: String,
    cache: Arc<dyn CacheProvider + Send + Sync>,
    storage_runtime: tokio::runtime::Handle,
}

async fn cmd_serve(
    storage: StorageCli,
    master_device_id: String,
    listen: String,
    priority: u32,
    lease: u64,
    holder: &str,
    runtime: tokio::runtime::Handle,
) -> Result<()> {
    let _instance = acquire_single_instance(holder)?;
    let allowed_device =
        DeviceId::parse(&master_device_id).ok_or_else(|| anyhow!("Invalid peer device ID"))?;
    let storage_uri = storage.storage_uri.clone();
    let (store, cache, _) = storage.open(false, runtime.clone())?;

    run_serve(&ServeConfig {
        store,
        allowed_device,
        listen,
        holder: holder.to_string(),
        priority,
        lease,
        storage_uri,
        cache,
        storage_runtime: runtime,
    })
    .await?;
    Ok(())
}

async fn run_serve(s: &ServeConfig) -> Result<()> {
    let ctrlc = CancellationToken::new();
    spawn_cancel_on_ctrl_c(ctrlc.clone(), true);

    loop {
        tracing::info!("acquiring distributed lock on storage");
        println!("Acquiring distributed lock on storage...");
        let lock = bepository_lock::Lock::new(
            &*s.store,
            LOCK_PATH.clone(),
            s.holder.clone(),
            s.priority,
            s.lease,
        );
        let mut guard = match lock.hold(s.store.clone(), &ctrlc).await {
            Ok(g) => g,
            Err(bepository_lock::Error::Cancelled) => return Ok(()),
            Err(bepository_lock::Error::ClockRegression) => {
                return Err(anyhow!(
                    "Clock regression detected. Cannot resolve automatically.\n\
                      Use 'fsck --clear-lock' to manually break the lock."
                ));
            }
            Err(e) => return Err(e.into()),
        };
        let epoch = guard.epoch();
        tracing::info!(epoch = %epoch.as_base32(), "lock acquired");
        println!("Lock acquired (epoch {epoch}).");

        // Fresh storage each cycle — epoch is write-once and close() is permanent.
        let storage = SlateStorage::new(
            s.store.clone(),
            Some(s.cache.clone()),
            s.storage_runtime.clone(),
        );
        let lost = serve_locked(s, &storage, &ctrlc, &mut guard).await;
        const STORAGE_CLOSE_TIMEOUT: Duration = Duration::from_secs(10);
        match tokio::time::timeout(STORAGE_CLOSE_TIMEOUT, storage.close()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::warn!("failed to close storage cleanly: {:?}", e),
            Err(_) => {
                tracing::warn!(
                    "storage.close() exceeded {}s timeout; exiting (compaction will resume on next start)",
                    STORAGE_CLOSE_TIMEOUT.as_secs()
                );
            }
        }
        let _ = guard.release().await;

        match lost {
            ServeOutcome::Done => return Ok(()),
            ServeOutcome::Fatal(e) => return Err(e),
            ServeOutcome::Lost(reason) => {
                tracing::warn!(?reason, "lock lost");
                println!("Lock lost ({reason}). Re-acquiring...");
            }
        }
    }
}

enum ServeOutcome {
    /// Clean shutdown (Ctrl+C or accept loop ended).
    Done,
    /// Lock lost — caller should retry acquisition.
    Lost(LockLost),
    /// Unrecoverable error — caller should exit.
    Fatal(anyhow::Error),
}

fn build_engine(
    storage: SlateStorage,
    identity: Arc<Identity>,
    folder_ids: Vec<FolderId>,
) -> BepEngine<SlateStorage> {
    BepEngine::new(
        storage,
        *identity.device_id(),
        "bepository".into(),
        folder_ids,
        Arc::new(TheirsResolver),
    )
}

fn spawn_engine_events(
    mut events: tokio::sync::mpsc::Receiver<EngineEvent>,
    allowed_device: DeviceId,
) {
    tokio::spawn(
        async move {
            while let Some(evt) = events.recv().await {
                match evt {
                    EngineEvent::DeviceConnecting { device, respond } => {
                        let _ = respond.send(device == allowed_device);
                    }
                    EngineEvent::DeviceDisconnected { device, reason } => {
                        tracing::info!(%device, ?reason, "device disconnected");
                    }
                }
            }
        }
        .in_current_span(),
    );
}

async fn spawn_checkpoint_tasks(
    storage: &SlateStorage,
    meta: &bepository_storage::meta::Meta,
    cancel: CancellationToken,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut checkpoint_handles = Vec::new();

    let checkpoint_ages = storage
        .list_all_checkpoint_ages()
        .await
        .unwrap_or_else(|e| {
            tracing::warn!("failed to list checkpoint ages: {}", e);
            HashMap::new()
        });

    for (&interval_dur, sched) in &meta.checkpoint {
        if interval_dur < Duration::from_secs(10 * 60) {
            tracing::warn!(
                "skipping checkpoint schedule '{}': interval is below the 10-minute minimum",
                humantime::format_duration(interval_dur)
            );
            continue;
        }

        let storage_clone = storage.clone();
        let cancel_clone = cancel.clone();
        let age = checkpoint_ages.get(&interval_dur).copied();
        let ttl = sched.ttl;

        checkpoint_handles.push(tokio::spawn(
            async move {
                // Compute how long until the next checkpoint should fire.
                // If none exists or the interval has already elapsed, fire immediately (zero delay).
                let initial_delay = age
                    .and_then(|a| interval_dur.checked_sub(a))
                    .unwrap_or(Duration::ZERO);

                tokio::select! {
                    _ = tokio::time::sleep(initial_delay) => {}
                    _ = cancel_clone.cancelled() => return,
                }

                loop {
                    if let Err(e) = storage_clone.create_checkpoints(interval_dur, ttl).await {
                        tracing::warn!(
                            "checkpoint '{}' failed: {}",
                            humantime::format_duration(interval_dur),
                            e
                        );
                    } else {
                        tracing::info!(
                            "checkpoint '{}' created",
                            humantime::format_duration(interval_dur)
                        );
                    }

                    tokio::select! {
                        _ = tokio::time::sleep(interval_dur) => {}
                        _ = cancel_clone.cancelled() => break,
                    }
                }
            }
            .in_current_span(),
        ));
    }
    checkpoint_handles
}

/// Map an activation error into an `anyhow::Error`, adding a concrete remedy
/// when the store was written by a newer format version. Only suggests the
/// `upgrade` subcommand in builds that have it (the `self-manage` feature);
/// both activation paths (`serve_locked`, `with_admin_lock`) route through this
/// so the hint cannot drift between them. Non-`UnsupportedVersion` errors pass
/// through unchanged.
fn into_activation_error(e: StorageError) -> anyhow::Error {
    let hint = match e {
        StorageError::UnsupportedVersion { found, supported } => {
            let base = format!(
                "this store was written by format version {found}, but this \
                 instance only supports version {supported}."
            );
            #[cfg(feature = "self-manage")]
            let tail = " Run `bepository upgrade`, then restart.";
            #[cfg(not(feature = "self-manage"))]
            let tail = " Upgrade this instance and restart.";
            format!("{base}{tail}")
        }
        // Any other error: no actionable hint to add.
        other => return anyhow::Error::from(other),
    };
    anyhow::Error::from(e).context(hint)
}

#[tracing::instrument(level = "info", skip(s, storage, cancel, guard), fields(epoch = %guard.epoch().as_base32()))]
async fn serve_locked(
    s: &ServeConfig,
    storage: &SlateStorage,
    cancel: &CancellationToken,
    guard: &mut LockGuard,
) -> ServeOutcome {
    if let Err(e) = storage.activate(guard.epoch()).await {
        return ServeOutcome::Fatal(into_activation_error(e));
    }
    if let Err(e) = storage.gc_inbox().await {
        return ServeOutcome::Fatal(e.into());
    }

    // Auto-init: under DynamicUser there is no stable UID for a root-run
    // one-shot `init`, so serve performs the idempotent init itself when the
    // store has no identity yet. Safe to call on every serve — run_init is a
    // no-op once an identity exists.
    let needs_init = match storage.get_identity() {
        Ok(None) => true,
        Ok(Some(_)) => false,
        Err(e) => {
            return ServeOutcome::Fatal(anyhow::Error::from(e).context("failed to read identity"));
        }
    };
    if needs_init {
        tracing::info!("uninitialized store — running init");
        if let Err(e) = run_init(storage).await {
            return ServeOutcome::Fatal(e);
        }
    }

    // Load identity under lock with epoch fencing — ensures we see a
    // consistent version even if fsck --regenerate-id ran concurrently.
    let identity = match require_identity(storage, true).await {
        Ok(id) => Arc::new(id),
        Err(e) => return ServeOutcome::Fatal(e),
    };

    // Spawn checkpoint tasks based on the schedule in meta.
    // Tasks are cancelled via checkpoint_cancel, which fires on all serve_locked exit paths.
    let meta = match storage.read_meta() {
        Ok(m) => m,
        Err(e) => return ServeOutcome::Fatal(e.into()),
    };
    // Separate token cancelled on all serve_locked exit paths (lock loss, ctrl-c, accept
    // loop end). The ctrl-c token alone is not enough — if the lock is lost, serve_locked
    // returns and storage is closed, but ctrl-c is still live.
    let checkpoint_cancel = CancellationToken::new();
    let checkpoint_handles =
        spawn_checkpoint_tasks(storage, &meta, checkpoint_cancel.clone()).await;

    // Load already-registered folder IDs to advertise in our initial ClusterConfig.
    // New folders proposed by the peer will be auto-registered during the connection.
    let folder_ids: Vec<FolderId> = storage
        .list_folders()
        .expect("in-memory list_folders should not fail")
        .into_iter()
        .map(|(id, _, _)| id)
        .collect();

    let device_id = identity.device_id();
    let allowed_device = &s.allowed_device;
    let storage_uri = &s.storage_uri;
    tracing::info!(
        device_id = %device_id,
        remote_device = %allowed_device,
        storage_uri = %storage_uri,
        "starting bepository bridge"
    );
    println!("Starting bepository bridge...");
    println!("  My Device ID:    {device_id}");
    println!("  Allowed Peer ID: {allowed_device}");
    println!("  Storage URI:     {storage_uri}");

    let mut engine = build_engine(storage.clone(), identity.clone(), folder_ids);
    let Some(events) = engine.take_event_receiver() else {
        return ServeOutcome::Fatal(anyhow!("event receiver already taken"));
    };
    spawn_engine_events(events, s.allowed_device);

    let engine = Arc::new(engine);
    let listener = match TcpListener::bind(&s.listen).await {
        Ok(l) => l,
        Err(e) => {
            return ServeOutcome::Fatal(
                anyhow::Error::from(e).context("failed to bind TCP listener"),
            );
        }
    };
    let local_addr = listener
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| s.listen.clone());
    println!("  Listening on:    {local_addr}");

    let outcome = tokio::select! {
        res = accept_loop(&listener, &engine, &identity) => {
            match res {
                Ok(()) => ServeOutcome::Done,
                Err(e) => ServeOutcome::Fatal(e),
            }
        }
        reason = guard.lost_ref() => {
            engine.shutdown_all();
            ServeOutcome::Lost(reason)
        }
        _ = cancel.cancelled() => {
            engine.shutdown_all();
            ServeOutcome::Done
        }
    };
    // Cancel and abort checkpoint tasks regardless of why we're exiting (lock loss,
    // ctrl-c, or accept loop end). Aborting — not just signalling — ensures tasks are
    // not mid-create_checkpoint when storage.close() is called in the caller.
    checkpoint_cancel.cancel();
    for handle in checkpoint_handles {
        handle.abort();
    }
    outcome
}

#[tracing::instrument(level = "info", skip(listener, engine, identity))]
async fn accept_loop(
    listener: &TcpListener,
    engine: &Arc<BepEngine<SlateStorage>>,
    identity: &Arc<Identity>,
) -> Result<()> {
    loop {
        let (stream, addr) = listener.accept().await?;
        tracing::info!("Accepted connection from {}", addr);
        let id = identity.clone();
        let eng = engine.clone();
        tokio::spawn(
            async move {
                match bepository_tls::accept(stream, &id).await {
                    Ok(bep) => {
                        tracing::info!("TLS established with {:?}", bep.peer_device_id);
                        match eng.accept(bep.stream, bep.peer_device_id).await {
                            Ok(handle) => {
                                let _ = handle.closed.await;
                            }
                            Err(e) => {
                                tracing::error!("BEP error: {:?}", e);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("TLS handshake failed from {}: {:?}", addr, e);
                    }
                }
            }
            .in_current_span(),
        );
    }
}

/// Auto-detects a suitable base directory for the Foyer disk block-cache.
///
/// Used by [`CacheArgs`] when `--cache-dir` is not explicitly set.
/// Priority:
/// 1. `$BEPOSITORY_CACHE_DIRECTORY`
/// 2. `$CACHE_DIRECTORY` (systemd `CacheDirectory=`), first colon-separated path.
/// 3. XDG / platform cache dir via `directories::ProjectDirs`.
/// 4. `None` if neither is available (no home directory, unusual environments).
fn get_cache_dir() -> Option<PathBuf> {
    if let Ok(dir) =
        std::env::var("BEPOSITORY_CACHE_DIRECTORY").or_else(|_| std::env::var("CACHE_DIRECTORY"))
    {
        return dir.split(':').next().map(PathBuf::from);
    }
    directories::ProjectDirs::from("net", "vleu", "bepository").map(|d| d.cache_dir().to_path_buf())
}

fn init_tracing(trace_level: Option<String>) -> Result<()> {
    let is_terminal = std::io::stdout().is_terminal();

    let effective_level = trace_level.unwrap_or_else(|| {
        if is_terminal {
            "warn".to_string()
        } else {
            "info".to_string()
        }
    });

    let filter = EnvFilter::builder()
        .with_default_directive(effective_level.parse()?)
        .from_env_lossy();

    let registry = tracing_subscriber::registry();

    #[cfg(all(feature = "tokio-console", tokio_unstable))]
    let registry = registry.with(
        console_subscriber::ConsoleLayer::builder()
            .with_default_env()
            .spawn(),
    );

    if is_terminal {
        let terminal_layer = tracing_subscriber::fmt::layer()
            .with_ansi(true)
            .with_target(false)
            .compact()
            .with_filter(filter);
        registry.with(terminal_layer).init();
    } else {
        match tracing_journald::layer() {
            Ok(journald_layer) => {
                let journald_layer = journald_layer.with_filter(filter);
                registry.with(journald_layer).init();
            }
            Err(e) => {
                eprintln!("Failed to connect to journald: {e}. Falling back to stderr.");
                let fallback_layer = tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(std::io::stderr)
                    .with_filter(filter);
                registry.with(fallback_layer).init();
            }
        }
    }
    Ok(())
}

/// Lowest-precedence configuration layer: read `/etc/bepository/env` (or the
/// `BEPOSITORY_ENV_FILE` override, used by tests) and set any KEY=VALUE pairs
/// that are not already present in the process environment. Mirrors systemd's
/// `EnvironmentFile` semantics: `KEY=VALUE` lines, `#` comments, blanks, and
/// one surrounding pair of double-quotes stripped from the value. **No** shell
/// expansion is performed — values are literal.
///
/// # Safety
///
/// Must be called single-threaded (before the tokio runtime is built). Env var
/// mutation is otherwise `unsafe` under the 2024 edition.
unsafe fn load_env_file() {
    let path =
        std::env::var("BEPOSITORY_ENV_FILE").unwrap_or_else(|_| "/etc/bepository/env".into());
    // read_to_string fails for both "missing" and "exists but unreadable".
    // Missing is the normal case for ad-hoc runs outside the service; unreadable
    // (e.g. root-owned /etc/bepository/env run as a non-root user) is the one
    // case where silence misleads — the user gets a confusing "storage URI not
    // set" downstream. Distinguish them with a visible stderr hint. We use
    // eprintln! (not tracing!) because this runs before init_tracing installs a
    // subscriber.
    let contents = match fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
        Err(e) => {
            eprintln!(
                "bepository: env file {path} exists but could not be read ({e}); \
                 ad-hoc commands may miss config — try running with sudo, \
                 or set BEPOSITORY_* variables explicitly."
            );
            return;
        }
    };
    for raw in contents.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            eprintln!("bepository: ignoring malformed env line in {path}: {raw}");
            continue;
        };
        // Existing process env wins — never override. systemd EnvironmentFile is
        // similarly the lowest layer.
        if std::env::var_os(key).is_some() {
            continue;
        }
        // Strip one surrounding pair of double-quotes, matching systemd semantics.
        let value = value
            .strip_prefix('"')
            .and_then(|v| v.strip_suffix('"'))
            .unwrap_or(value);
        // Safety: single-threaded per the function's contract.
        unsafe { std::env::set_var(key, value) };
    }
}

/// Parse the CLI. When `BEPOSITORY_PACKAGE_MANAGED` is set, the self-manage
/// subcommands are hidden from `--help` (their handlers still refuse execution,
/// independently — clap's `hide` only affects help text).
#[cfg(feature = "self-manage")]
fn parse_cli() -> Cli {
    use clap::{CommandFactory, FromArgMatches};
    let mut cmd = Cli::command();
    if std::env::var_os("BEPOSITORY_PACKAGE_MANAGED").is_some_and(|v| !v.is_empty()) {
        for name in [
            "install-service",
            "print-service",
            "uninstall-service",
            "upgrade",
        ] {
            cmd = cmd.mut_subcommand(name, |c| c.hide(true));
        }
    }
    let matches = cmd.get_matches();
    Cli::from_arg_matches(&matches).expect("Cli derives Parser; from_arg_matches cannot fail here")
}

#[cfg(not(feature = "self-manage"))]
fn parse_cli() -> Cli {
    Cli::parse()
}

fn main() -> Result<()> {
    // Load /etc/bepository/env before anything else — before rustls init, before
    // tokio, and crucially before Cli::parse() (clap binds env vars at parse
    // time). This gives ad-hoc commands the same config the systemd unit reads
    // via EnvironmentFile. Existing process env always wins (lowest precedence).
    // Safety: this runs single-threaded, before any runtime or spawned thread.
    unsafe { load_env_file() };

    let _ = rustls::crypto::ring::default_provider().install_default();

    let cli = parse_cli();
    // Scrub the env var so child processes can't inherit it.
    // Safety: single-threaded at this point; no concurrent env reads.
    unsafe { std::env::remove_var("BEPOSITORY_DAV_PASSWORD") };

    init_tracing(cli.trace.clone()).context("failed to initialize tracing")?;

    let storage_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("slatedb-worker")
        .enable_all()
        .build()
        .context("failed to create slatedb runtime")?;
    let storage_runtime_handle = storage_runtime.handle().clone();

    let main_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to create main runtime")?;

    main_runtime.block_on(async_main(cli, storage_runtime_handle))
}

async fn cmd_init(
    storage: StorageCli,
    holder: &str,
    runtime: tokio::runtime::Handle,
) -> Result<()> {
    let _instance = acquire_single_instance(holder)?;
    let (store, _, db) = storage.open(true, runtime)?;
    let dev_id = with_admin_lock(store, &db, holder, 60, || run_init(&db)).await?;
    println!("Initialized. Device ID: {dev_id}");
    Ok(())
}

async fn cmd_remove_folder(
    storage: StorageCli,
    holder: &str,
    folder: String,
    runtime: tokio::runtime::Handle,
) -> Result<()> {
    let _instance = acquire_single_instance(holder)?;
    let (store, _, db) = storage.open(false, runtime)?;
    let folder_id = FolderId::new(&folder);
    with_admin_lock(store, &db, holder, 60, || async {
        // Verify the folder exists before claiming success.
        let folders = db.list_folders().context("failed to list folders")?;
        if !folders.iter().any(|(id, _, _)| *id == folder_id) {
            return Err(anyhow!("Folder '{folder}' is not registered."));
        }
        db.remove_folder(folder_id)
            .await
            .context("failed to remove folder")?;
        println!("Folder '{folder}' removed.");
        Ok(())
    })
    .await?;
    Ok(())
}

async fn cmd_get_id(storage: StorageCli, runtime: tokio::runtime::Handle) -> Result<()> {
    let (_, _, db) = storage.open(false, runtime)?;
    let dev_id = run_get_id(&db).await?;
    println!("{dev_id}");
    db.close().await?;
    Ok(())
}

async fn async_main(cli: Cli, storage_runtime_handle: tokio::runtime::Handle) -> Result<()> {
    let machine_id = cli.machine_id.clone().unwrap_or_else(|| {
        machine_uid::get().unwrap_or_else(|_| {
            tracing::warn!(
                "Could not retrieve machine UID, lock holder IDs may not be stable across restarts"
            );
            uuid::Uuid::new_v4().to_string()
        })
    });

    let storage_uri = cli.command.storage_uri();
    let holder = format!("{machine_id}/{storage_uri}");

    match cli.command {
        Commands::Init { storage } => {
            cmd_init(storage, &holder, storage_runtime_handle).await?;
        }
        Commands::RemoveFolder { storage, folder } => {
            cmd_remove_folder(storage, &holder, folder, storage_runtime_handle).await?;
        }
        Commands::GetId { storage } => {
            cmd_get_id(storage, storage_runtime_handle).await?;
        }
        Commands::Serve {
            storage,
            master_device_id,
            listen,
            priority,
            lease,
        } => {
            cmd_serve(
                storage,
                master_device_id,
                listen,
                priority,
                lease,
                &holder,
                storage_runtime_handle,
            )
            .await?;
        }
        Commands::Fsck {
            storage,
            check,
            regenerate_id,
            clear_lock,
            compact,
        } => {
            cmd_fsck(
                storage,
                check,
                regenerate_id,
                clear_lock,
                compact,
                &holder,
                storage_runtime_handle,
            )
            .await?;
        }
        Commands::Checkpoint { storage, action } => {
            cmd_checkpoint(storage, action, &holder, storage_runtime_handle).await?;
        }
        #[cfg(feature = "self-manage")]
        Commands::PrintService => {
            if let Some(hint) = service::package_managed_hint() {
                return Err(service::package_managed_error(&hint));
            }
            service::print_service()?;
        }
        #[cfg(feature = "self-manage")]
        Commands::InstallService { no_auto_upgrade } => {
            if let Some(hint) = service::package_managed_hint() {
                return Err(service::package_managed_error(&hint));
            }
            service::install_service(no_auto_upgrade)?;
        }
        #[cfg(feature = "self-manage")]
        Commands::UninstallService => {
            if let Some(hint) = service::package_managed_hint() {
                return Err(service::package_managed_error(&hint));
            }
            service::uninstall_service()?;
        }
        #[cfg(feature = "self-manage")]
        Commands::Upgrade {
            restart_unit,
            dry_run,
        } => {
            if let Some(hint) = service::package_managed_hint() {
                return Err(service::package_managed_error(&hint));
            }
            upgrade::run(restart_unit, dry_run).await?;
        }
    }
    Ok(())
}

async fn report_lock_status(lock: &bepository_lock::Lock<'_, dyn ObjectStore>) -> Result<()> {
    let Some(entry) = lock.current_owner().await? else {
        println!("  Lock status: Unlocked");
        return Ok(());
    };

    let now = chrono::Utc::now();
    let expiry = entry.expires_at();
    let holder = &entry.file.holder;
    let priority = entry.file.priority;
    let acquired = entry.meta.last_modified;
    let epoch = entry.epoch.as_u64();

    if now >= expiry {
        println!(
            "  Lock status: Stale (Owner: {holder}, Priority: {priority}, Epoch: {epoch}, Acquired: {acquired}, Expired: {expiry})"
        );
    } else {
        let remaining = (expiry - now).num_seconds();
        println!(
            "  Lock status: Active (Owner: {holder}, Priority: {priority}, Epoch: {epoch}, Acquired: {acquired}, Expires in: {remaining}s)"
        );
    }
    Ok(())
}

async fn clear_lock(lock: &bepository_lock::Lock<'_, dyn ObjectStore>) -> Result<()> {
    tracing::warn!("forcibly clearing locks");
    println!("Forcibly clearing locks...");
    lock.unsafe_break_locks()
        .await
        .context("failed to clear locks")?;
    println!("Locks cleared.");
    Ok(())
}

async fn cmd_fsck(
    storage: StorageCli,
    check: Option<FsckLevel>,
    regenerate_id: bool,
    clear_lock_flag: bool,
    compact: bool,
    holder: &str,
    runtime: tokio::runtime::Handle,
) -> Result<()> {
    let _instance = acquire_single_instance(holder)?;
    let (store, _, db) = storage.open(false, runtime)?;
    let needs_lock = compact || regenerate_id || check.is_some();
    let lock = bepository_lock::Lock::new(&*store, LOCK_PATH.clone(), holder.to_string(), 0, 300);

    report_lock_status(&lock).await?;

    if clear_lock_flag {
        clear_lock(&lock).await?;
    }

    if compact {
        // `fsck-compact` is a short-lived admin op: collapse the prod 60 s
        // compactor poll so submitted jobs start within a second instead of
        // blocking the operator for a full tick. Must be set before activate.
        db.set_compactor_poll_interval(Duration::from_secs(1));
    }

    if needs_lock {
        let action = if compact {
            "compaction"
        } else if regenerate_id {
            "regenerate-id"
        } else {
            "fsck"
        };
        tracing::info!(%action, "acquiring lock for admin action");
        println!("Acquiring lock for {action}...");
        with_admin_lock(store, &db, holder, 300, || async {
            let storage_level = check.map(|l| match l {
                FsckLevel::Quick => bepository_storage::FsckLevel::Quick,
                FsckLevel::Structural => bepository_storage::FsckLevel::Structural,
                FsckLevel::Full => bepository_storage::FsckLevel::Full,
            });

            if regenerate_id {
                tracing::info!("regenerating identity");
                let new_id = fsck_regenerate_id(&db).await?;
                println!("New Device ID: {new_id}");
            }
            if let Some(level) = storage_level {
                tracing::info!(?level, "running FSCK");
                println!("Running FSCK with level: {level:?}");
                fsck_check(&db, level).await?;
                println!("FSCK passed.");
            }
            if compact {
                tracing::info!("triggering manual compaction");
                println!("Triggering compaction...");
                fsck_compact(&db).await?;
                println!("Compaction complete.");
            }
            Ok(())
        })
        .await?;
    } else {
        db.close().await?;
    }

    Ok(())
}

async fn checkpoint_set_ttl(
    storage: StorageCli,
    interval: Duration,
    value: Duration,
    holder: &str,
    runtime: tokio::runtime::Handle,
) -> Result<()> {
    let interval_dur = validate_checkpoint_duration(interval)?;
    let ttl = validate_checkpoint_duration(value)?;
    let _instance = acquire_single_instance(holder)?;
    let (store, _, db) = storage.open(false, runtime)?;
    let schedule = CheckpointSchedule { ttl };
    with_admin_lock(store, &db, holder, 60, || async {
        db.update_checkpoint_schedule(interval_dur, Some(schedule))
            .await
            .context("failed to update checkpoint schedule")?;
        db.refresh_checkpoints(interval_dur, ttl)
            .await
            .context("failed to refresh existing checkpoints")?;
        let interval_fmt = humantime::format_duration(interval_dur);
        let ttl_fmt = humantime::format_duration(ttl);
        tracing::info!(interval = %interval_fmt, ttl = %ttl_fmt, "checkpoint schedule updated");
        println!("Checkpoint schedule updated: every {interval_fmt} with TTL {ttl_fmt}.");
        Ok(())
    })
    .await?;
    Ok(())
}

async fn checkpoint_remove(
    storage: StorageCli,
    interval: Duration,
    holder: &str,
    runtime: tokio::runtime::Handle,
) -> Result<()> {
    let interval_dur = validate_checkpoint_duration(interval)?;
    let _instance = acquire_single_instance(holder)?;
    let (store, _, db) = storage.open(false, runtime)?;
    with_admin_lock(store, &db, holder, 60, || async {
        db.update_checkpoint_schedule(interval_dur, None)
            .await
            .context("failed to remove checkpoint schedule")?;
        let interval_fmt = humantime::format_duration(interval_dur);
        tracing::info!(interval = %interval_fmt, "checkpoint schedule removed");
        println!("Checkpoint schedule '{interval_fmt}' removed.");
        Ok(())
    })
    .await?;
    Ok(())
}

async fn checkpoint_list(storage: StorageCli, runtime: tokio::runtime::Handle) -> Result<()> {
    let (_, _, db) = storage.open(false, runtime)?;
    let (schedules, folder_checkpoints) = db
        .list_checkpoints_unlocked()
        .await
        .context("failed to list checkpoints")?;

    println!("Checkpoint schedules:");
    if schedules.is_empty() {
        println!("  (none)");
    } else {
        // Schedules are already sorted by Duration in the BTreeMap.
        for (interval, sched) in schedules {
            let interval_fmt = humantime::format_duration(interval);
            let ttl_fmt = humantime::format_duration(sched.ttl);
            println!("  every {interval_fmt} ttl {ttl_fmt}");
        }
    }

    println!();
    println!("Existing checkpoints:");
    let mut any = false;
    for (label, _dir, checkpoints) in &folder_checkpoints {
        if checkpoints.is_empty() {
            continue;
        }
        any = true;
        println!("  Folder '{label}':");
        for cp in checkpoints {
            let id = cp.id;
            let name = cp.name.as_deref().unwrap_or("(unnamed)");
            let created = cp.create_time.to_rfc3339();
            let expire = cp
                .expire_time
                .map(|t| t.to_rfc3339())
                .unwrap_or_else(|| "never".to_string());
            println!("    [{id}] name={name} created={created} expires={expire}");
        }
    }
    if !any {
        println!("  (none)");
    }
    Ok(())
}

async fn checkpoint_serve_dav(
    storage: StorageCli,
    addr: String,
    password: secrecy::SecretString,
    runtime: tokio::runtime::Handle,
) -> Result<()> {
    let (store, cache, _) = storage.open(false, runtime.clone())?;
    let db = Arc::new(SlateStorage::new(store, Some(cache), runtime));

    let cancel = CancellationToken::new();
    spawn_cancel_on_ctrl_c(cancel.clone(), false);

    bepository_dav::serve(db, &addr, &password, cancel)
        .await
        .map_err(|e| anyhow!("{e}"))?;
    Ok(())
}

async fn cmd_checkpoint(
    storage: StorageCli,
    action: CheckpointAction,
    holder: &str,
    runtime: tokio::runtime::Handle,
) -> Result<()> {
    match action {
        CheckpointAction::Every {
            interval,
            op: EveryOp::Ttl { value },
        } => checkpoint_set_ttl(storage, interval, value, holder, runtime).await,
        CheckpointAction::Every {
            interval,
            op: EveryOp::Remove,
        } => checkpoint_remove(storage, interval, holder, runtime).await,
        CheckpointAction::List => checkpoint_list(storage, runtime).await,
        CheckpointAction::Serve {
            addr,
            webdav_password,
        } => checkpoint_serve_dav(storage, addr, webdav_password, runtime).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bepository_bep::ids::FolderLabel;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn test_cli_handlers() {
        let store = Arc::new(InMemory::new());
        let storage = SlateStorage::new(store, None, tokio::runtime::Handle::current());
        storage
            .activate(bepository_lock::Epoch::new(1).unwrap())
            .await
            .unwrap();

        let get_err = run_get_id(&storage).await;
        assert!(get_err.is_err());
        assert!(
            get_err
                .unwrap_err()
                .to_string()
                .contains("No identity found")
        );

        let init_id = run_init(&storage).await.expect("init should succeed");
        assert!(!init_id.is_empty());

        // Re-running init is idempotent (returns same identity).
        let init_again = run_init(&storage).await.expect("re-init should succeed");
        assert_eq!(init_again, init_id);

        // Register a folder separately.
        storage
            .register_folder(FolderId::from("test"), &FolderLabel::from("LabelFor-test"))
            .await
            .expect("register folder should succeed");
        let reg_err = storage
            .register_folder(FolderId::from("test"), &FolderLabel::from("LabelFor-test"))
            .await;
        assert!(reg_err.is_err());
        assert!(format!("{:#}", reg_err.unwrap_err()).contains("already registered"));

        let got_id = run_get_id(&storage)
            .await
            .expect("get_id should succeed after init");
        assert_eq!(got_id, init_id);

        fsck_check(&storage, bepository_storage::FsckLevel::Quick)
            .await
            .expect("fsck should succeed");

        let regenerated_id = fsck_regenerate_id(&storage)
            .await
            .expect("fsck regenerate should succeed");
        assert_ne!(regenerated_id, init_id);

        let got_new_id = run_get_id(&storage)
            .await
            .expect("get_id should succeed after fsck");
        assert_eq!(got_new_id, regenerated_id);
    }

    #[tokio::test]
    async fn test_parse_and_handlers_memory_uri() {
        let uri = "memory://test-folder";
        let store = parse_storage_uri(uri, true).unwrap();

        let storage = SlateStorage::new(store, None, tokio::runtime::Handle::current());
        storage
            .activate(bepository_lock::Epoch::new(1).unwrap())
            .await
            .unwrap();
        assert!(run_get_id(&storage).await.is_err());

        let init_id = run_init(&storage).await.unwrap();
        let get_id = run_get_id(&storage).await.unwrap();
        assert_eq!(init_id, get_id);

        // Register and verify folder separately.
        storage
            .register_folder(
                FolderId::from("test-folder"),
                &FolderLabel::from("LabelFor-test-folder"),
            )
            .await
            .unwrap();
        let registered = storage.list_folders().unwrap();
        assert_eq!(registered.len(), 1);
        assert_eq!(registered[0].0, FolderId::from("test-folder"));
        assert_eq!(registered[0].1, FolderLabel::from("LabelFor-test-folder"));
    }

    // --- env-file load (PLAN-0.8 Phase 2) ---

    /// The serve auto-init predicate: a fresh store has no identity, and
    /// `run_init` (which `serve_locked` calls in that case) produces one.
    /// `serve_locked` itself is exercised end-to-end by the e2e suite
    /// (files_survive_bepository_restarts); this unit test pins the auto-init
    /// branch's precondition + effect directly.
    #[tokio::test]
    async fn serve_auto_init_predicate() {
        let store = Arc::new(InMemory::new());
        let storage = SlateStorage::new(store, None, tokio::runtime::Handle::current());
        storage
            .activate(bepository_lock::Epoch::new(1).unwrap())
            .await
            .unwrap();
        // Fresh store: no identity yet — this is the precondition serve_locked checks.
        assert!(storage.get_identity().unwrap().is_none());
        // run_init is the idempotent path serve_locked calls; it must produce an identity.
        let id = run_init(&storage).await.unwrap();
        assert!(!id.is_empty());
        // After init, the identity is present — serve would not re-init.
        assert!(storage.get_identity().unwrap().is_some());
        // Idempotent: running again returns the same identity.
        assert_eq!(run_init(&storage).await.unwrap(), id);
    }

    /// Serializes env-file tests: both load_env_file and the env-file-missing
    /// check mutate the global `BEPOSITORY_ENV_FILE` var, and cargo runs tests
    /// on parallel threads. Holding a sync mutex across the synchronous
    /// load_env_file (no awaits) is fine.
    static ENV_FILE_TEST_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// `load_env_file` sets unset vars, leaves already-set vars untouched, and
    /// treats a missing file as a no-op. Merged into one test so the two
    /// `BEPOSITORY_ENV_FILE` mutations can't race under cargo's parallel runner.
    #[test]
    fn env_file_load_and_missing_file_are_handled() {
        let _guard = ENV_FILE_TEST_GUARD.lock().unwrap();
        const NEW: &str = "BEPOSITORY_TEST_ENV_FILE_NEW";
        const EXISTING: &str = "BEPOSITORY_TEST_ENV_FILE_EXISTING";
        // Safety: serialized by ENV_FILE_TEST_GUARD; clean up so the test is
        // order-independent and leaves no residue.
        unsafe {
            std::env::remove_var(NEW);
            std::env::set_var(EXISTING, "from-process");
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("env");
        std::fs::write(
            &path,
            "# a comment\n\
             BEPOSITORY_TEST_ENV_FILE_NEW=from-file\n\
             BEPOSITORY_TEST_ENV_FILE_EXISTING=from-file\n\
             BEPOSITORY_TEST_ENV_FILE_QUOTED=\"quoted value\"\n",
        )
        .unwrap();
        unsafe {
            std::env::set_var("BEPOSITORY_ENV_FILE", &path);
            load_env_file();
        }
        assert_eq!(std::env::var(NEW).unwrap(), "from-file");
        // Process env wins over the file.
        assert_eq!(std::env::var(EXISTING).unwrap(), "from-process");
        assert_eq!(
            std::env::var("BEPOSITORY_TEST_ENV_FILE_QUOTED").unwrap(),
            "quoted value"
        );

        // A missing env file is a no-op (normal for ad-hoc runs).
        unsafe {
            std::env::set_var("BEPOSITORY_ENV_FILE", "/nonexistent/bepository-env-test");
            load_env_file();
        }

        // Cleanup.
        unsafe {
            std::env::remove_var(NEW);
            std::env::remove_var(EXISTING);
            std::env::remove_var("BEPOSITORY_TEST_ENV_FILE_QUOTED");
            std::env::remove_var("BEPOSITORY_ENV_FILE");
        }
    }
}
