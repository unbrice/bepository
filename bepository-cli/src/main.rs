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

mod envfile;
#[cfg(feature = "self-manage")]
mod service;
#[cfg(test)]
mod test_env;
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
#[command(
    after_help = "CONFIGURATION:\n    Settings are layered: command-line flags override environment variables,\n    which override /etc/bepository/env (created at install time).\n    Most commands need no flags on an installed machine."
)]
struct Cli {
    /// Override the machine ID (for tests or cross-machine identity).
    #[arg(long, global = true, env = "BEPOSITORY_MACHINE_ID")]
    machine_id: Option<String>,
    /// Set the tracing level (e.g., info, debug, trace)
    #[arg(long, global = true, env = "BEPOSITORY_LOG")]
    trace: Option<String>,
    /// The path or URI to the SlateDB storage (e.g., s3://bucket/path, file:///tmp/sync).
    /// Usually set via BEPOSITORY_STORAGE_URI in /etc/bepository/env.
    #[arg(long, short = 's', global = true, env = "BEPOSITORY_STORAGE_URI")]
    storage_uri: Option<String>,
    #[command(flatten)]
    cache: CacheArgs,
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Args, Clone)]
struct CacheArgs {
    /// Override the Foyer block-cache directory.
    /// Defaults to $BEPOSITORY_CACHE_DIRECTORY, $CACHE_DIRECTORY (systemd) or the XDG cache dir.
    #[arg(long, global = true, value_name = "PATH")]
    cache_dir: Option<PathBuf>,
    /// Disable the Foyer block cache entirely.
    #[arg(
        long,
        global = true,
        env = "BEPOSITORY_NO_CACHE",
        conflicts_with = "cache_dir"
    )]
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

type StorageStack = (
    Arc<dyn ObjectStore>,
    Arc<dyn CacheProvider + Send + Sync>,
    SlateStorage,
);

/// Open the object store, block cache, and SlateStorage from a resolved URI.
fn open_storage(
    storage_uri: &str,
    cache: &CacheArgs,
    create: bool,
    runtime: tokio::runtime::Handle,
) -> Result<StorageStack> {
    let store = parse_storage_uri(storage_uri, create)?;
    let cache = Arc::new(cache.clone());
    let db = SlateStorage::new(store.clone(), Some(cache.clone()), runtime);
    Ok((store, cache, db))
}

#[derive(Subcommand)]
enum Commands {
    /// Initializes storage and generates a TLS identity.
    ///
    /// Creates a new TLS certificate on first run. Idempotent — re-running
    /// on an already-initialized store is a no-op.
    /// Acquires the distributed lock to prevent races with serve.
    ///
    /// Example: bepository init   (or: bepository init -s s3://my-bucket/backup)
    Init,
    /// Permanently remove a folder and its data from storage.
    ///
    /// Acquires the distributed lock and recursively deletes all object store
    /// keys associated with the folder.
    RemoveFolder {
        /// The folder ID to remove (as shown in Syncthing).
        folder: String,
    },
    /// List registered folders.
    ///
    /// Reads storage directly; does not acquire the distributed lock and can
    /// run alongside an active daemon.
    ListFolders,
    /// Retrieves the Device ID of this bepository instance.
    ///
    /// This ID must be added to your local Syncthing "Remote Devices" list.
    GetId,
    /// Runs the bepository daemon.
    ///
    /// Automatically accepts and registers all folders proposed by the peer.
    ///
    /// Example: bepository serve L773...   (or: bepository serve -s s3://my-bucket/backup L773...)
    Serve {
        /// The Device ID of the master of this instance.
        #[arg(env = "BEPOSITORY_MASTER_DEVICE_ID", value_name = "MASTER_DEVICE_ID")]
        master_device_id: Option<String>,
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
    ///   bepository checkpoint every 1h ttl 7d
    ///   bepository checkpoint every 1h remove
    ///   bepository checkpoint list
    Checkpoint {
        #[command(subcommand)]
        action: CheckpointAction,
    },
    /// Integrity check and manual maintenance tool.
    Fsck {
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
    /// A `--storage-uri` passed on the command line is persisted into the env
    /// file, and absolute credential paths in it are wired into
    /// `LoadCredential` drop-ins. Requires root. Idempotent.
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
    /// Example: bepository checkpoint every 1h ttl 7d
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
    /// Requires a WebDAV password (via BEPOSITORY_DAV_PASSWORD env var or
    /// --webdav-password). The env var is cleared from the process environment
    /// immediately after reading.
    ///
    /// Example: BEPOSITORY_DAV_PASSWORD=secret bepository checkpoint serve 127.0.0.1:8080
    Serve {
        /// Listen address, e.g. 127.0.0.1:8080 or 0.0.0.0:8080
        addr: String,

        /// WebDAV password. Can be set via BEPOSITORY_DAV_PASSWORD environment variable.
        #[arg(long, env = "BEPOSITORY_DAV_PASSWORD", hide_env_values = true)]
        webdav_password: Option<secrecy::SecretString>,
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

impl Cli {
    /// Resolve the storage URI, failing late with a friendly message when it is
    /// absent and the subcommand needs storage.
    fn require_storage_uri(&self) -> Result<&str> {
        self.storage_uri
            .as_deref()
            .ok_or_else(|| anyhow!("no storage configured\nset BEPOSITORY_STORAGE_URI in /etc/bepository/env or pass -s/--storage-uri"))
    }
}

impl Commands {
    /// The self-manage subcommands do not open the store; give them a distinct
    /// lock scope so they don't collide with a running daemon.
    #[cfg(feature = "self-manage")]
    fn is_self_manage(&self) -> bool {
        matches!(
            self,
            Commands::PrintService
                | Commands::InstallService { .. }
                | Commands::UninstallService
                | Commands::Upgrade { .. }
        )
    }

    #[cfg(not(feature = "self-manage"))]
    fn is_self_manage(&self) -> bool {
        false
    }
}

/// opendal's built-in registry omits sftp even when the feature is compiled in.
static REGISTER_SFTP: LazyLock<()> = LazyLock::new(|| {
    opendal::DEFAULT_OPERATOR_REGISTRY
        .register::<opendal::services::Sftp>(opendal::services::SFTP_SCHEME);
});

/// object_store's config-key parser only accepts lowercase spellings and
/// silently drops unrecognised keys, so conventional uppercase env vars
/// (`AWS_ACCESS_KEY_ID`, ...) would never reach the builder — S3 then falls
/// back to the EC2 metadata endpoint. Mirror `AmazonS3Builder::from_env`:
/// lowercase `AWS_`-prefixed keys only; anything else stays untouched so a
/// stray `TOKEN`/`ENDPOINT`/`PROXY_URL` env var can't silently reconfigure
/// the store. GCS credentials go through the alias translation below.
fn lowercase_aws_key(key: String) -> String {
    if key.starts_with("AWS_") {
        key.to_ascii_lowercase()
    } else {
        key
    }
}

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
            .map(|(k, v)| (k.to_lowercase(), v.into_owned()))
            .chain(std::env::vars().map(|(k, v)| (lowercase_aws_key(k), v))),
    );

    // Empty values are absent: the env template ships empty AWS_* placeholders.
    opts.retain(|(_, v)| !v.is_empty());

    // Steer the conventional GCS env var to service-account handling (it
    // conventionally points at a service-account JSON). Covers both env-var
    // usage and URL query-param usage (case-insensitive).
    const ALIASES: &[(&str, &str)] = &[(
        "google_application_credentials",
        "google_service_account_path",
    )];
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

/// Mirrors object_store's boolean config parsing (`1|true|on|yes|y`,
/// case-insensitive) so the preflight accepts any value the builder would.
fn is_truthy(v: &str) -> bool {
    matches!(
        v.to_ascii_lowercase().as_str(),
        "1" | "true" | "on" | "yes" | "y"
    )
}

/// Fails fast unless the storage URI carries explicit S3/GCS credentials.
///
/// object_store 0.12 has no "disable metadata" flag: with no credentials it
/// silently probes the EC2 IMDS (`169.254.169.254`, ~14s × 10-retry hang
/// off-cloud) and the GCE metadata server. This preflight turns that into an
/// actionable error. `use_ambient_creds=true` opts back into the implicit
/// lookup (stripped before opts reach object_store).
///
/// The well-known GCS ADC file (`~/.config/gcloud/application_default_credentials.json`)
/// is intentionally NOT consulted: the systemd unit runs under `DynamicUser=yes`
/// with no home, so ambient file creds are unreachable by the daemon anyway.
fn require_explicit_credentials(parsed_url: &url::Url, opts: &[(String, String)]) -> Result<()> {
    // Our own key, matched loosely (the env file spells it USE_AMBIENT_CREDS)
    // and stripped before opts reach object_store.
    if opts
        .iter()
        .any(|(k, v)| k.eq_ignore_ascii_case("use_ambient_creds") && is_truthy(v))
    {
        return Ok(());
    }

    // object_store also routes these https hosts to the S3 builder (parse.rs).
    let scheme = match (parsed_url.scheme(), parsed_url.host_str()) {
        ("https", Some(host))
            if host.ends_with("amazonaws.com") || host.ends_with("r2.cloudflarestorage.com") =>
        {
            "s3"
        }
        (scheme, _) => scheme,
    };

    // Provider keys must match the exact lowercase spellings object_store's
    // case-sensitive parser accepts (query keys and AWS_* env vars arrive
    // lowercased). Matching loosely would count env vars the builder silently
    // drops (GOOGLE_SERVICE_ACCOUNT_PATH, SKIP_SIGNATURE, ...) as configured
    // and re-admit the metadata probe this check exists to prevent.
    let configured = match scheme {
        // Pod/machine identity (web identity, ECS/EKS container creds) is
        // ambient too and cannot be verified from opts (build() resolves
        // web identity from env only) — gated behind use_ambient_creds.
        "s3" | "s3a" => opts.iter().any(|(k, v)| match k.as_str() {
            "aws_skip_signature" | "skip_signature" => is_truthy(v),
            "aws_access_key_id" | "access_key_id" => true,
            _ => false,
        }),
        "gs" => opts.iter().any(|(k, v)| match k.as_str() {
            "google_skip_signature" | "skip_signature" => is_truthy(v),
            // google_application_credentials is normalized to
            // google_service_account_path by the alias pass upstream.
            "google_service_account"
            | "service_account"
            | "google_service_account_path"
            | "service_account_path"
            | "google_service_account_key"
            | "service_account_key"
            | "application_credentials" => true,
            _ => false,
        }),
        _ => return Ok(()),
    };
    if configured {
        return Ok(());
    }

    match scheme {
        "s3" | "s3a" => Err(anyhow!(
            "no explicit s3 credentials configured; refusing to probe the cloud \
             metadata endpoint — set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, put \
             access_key_id=/secret_access_key= in the storage URI, add \
             skip_signature=true for anonymous access, or set \
             use_ambient_creds=true to use ambient machine/pod credentials \
             (instance metadata, web identity, ECS/EKS)"
        )),
        "gs" => Err(anyhow!(
            "no explicit GCS credentials configured; refusing to probe the cloud \
             metadata endpoint — set GOOGLE_APPLICATION_CREDENTIALS, put \
             google_service_account_path=/google_service_account_key= in the storage \
             URI, add skip_signature=true for anonymous access, or set \
             use_ambient_creds=true to use ambient machine credentials \
             (instance metadata, gcloud ADC)"
        )),
        _ => Ok(()),
    }
}

fn open_object_store(parsed_url: url::Url) -> Result<Arc<dyn ObjectStore>> {
    let opts = merge_object_store_opts(&parsed_url);
    require_explicit_credentials(&parsed_url, &opts)?;

    // Keep our own escape-hatch key out of object_store's config namespace.
    let opts: Vec<_> = opts
        .into_iter()
        .filter(|(k, _)| !k.eq_ignore_ascii_case("use_ambient_creds"))
        .collect();

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

#[allow(clippy::too_many_arguments)]
async fn cmd_serve(
    storage_uri: &str,
    cache: &CacheArgs,
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
    let storage_uri = storage_uri.to_string();
    let (store, cache, _) = open_storage(storage_uri.as_str(), cache, false, runtime.clone())?;

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
/// that are not already present in the process environment. Parsing rules are
/// shared with the service installer — see [`envfile`].
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
        if !line.is_empty() && !line.starts_with('#') && !line.contains('=') {
            eprintln!("bepository: ignoring malformed env line in {path}: {raw}");
        }
    }
    for (key, value) in envfile::parse_env_lines(&contents) {
        // Existing process env wins — never override. systemd EnvironmentFile is
        // similarly the lowest layer.
        if std::env::var_os(key).is_some() {
            continue;
        }
        // Safety: single-threaded per the function's contract.
        unsafe { std::env::set_var(key, value) };
    }
}

/// Parse the CLI. When `BEPOSITORY_PACKAGE_MANAGED` is set, the self-manage
/// subcommands are hidden from `--help` (their handlers still refuse execution,
/// independently — clap's `hide` only affects help text).
///
/// The second return value is the storage URI **only when given on the command
/// line**, for `install-service` to persist.
#[cfg(feature = "self-manage")]
fn parse_cli() -> (Cli, Option<String>) {
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
    let storage_uri = command_line_storage_uri(&matches);
    let cli = Cli::from_arg_matches(&matches)
        .expect("Cli derives Parser; from_arg_matches cannot fail here");
    (cli, storage_uri)
}

/// The storage URI when its value source is the command line. Values sourced
/// from the environment (`BEPOSITORY_STORAGE_URI`, or the env file loaded
/// before parsing) must never be persisted into `/etc/bepository/env`.
/// `storage_uri` is a global arg, so the value may sit in the subcommand's
/// matches — walk down the tree.
#[cfg(feature = "self-manage")]
fn command_line_storage_uri(matches: &clap::ArgMatches) -> Option<String> {
    std::iter::successors(Some(matches), |m| m.subcommand().map(|(_, sub)| sub))
        .find(|m| m.value_source("storage_uri") == Some(clap::parser::ValueSource::CommandLine))
        .and_then(|m| m.get_one::<String>("storage_uri").cloned())
}

#[cfg(not(feature = "self-manage"))]
fn parse_cli() -> (Cli, Option<String>) {
    (Cli::parse(), None)
}

fn main() -> Result<()> {
    // Load /etc/bepository/env before anything else — before rustls init, before
    // tokio, and crucially before Cli::parse() (clap binds env vars at parse
    // time). This gives ad-hoc commands the same config the systemd unit reads
    // via EnvironmentFile. Existing process env always wins (lowest precedence).
    // Safety: this runs single-threaded, before any runtime or spawned thread.
    unsafe { load_env_file() };

    let _ = rustls::crypto::ring::default_provider().install_default();

    let (cli, cli_storage_uri) = parse_cli();
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

    main_runtime.block_on(async_main(cli, cli_storage_uri, storage_runtime_handle))
}

async fn cmd_init(
    storage_uri: &str,
    cache: &CacheArgs,
    holder: &str,
    runtime: tokio::runtime::Handle,
) -> Result<()> {
    let _instance = acquire_single_instance(holder)?;
    let (store, _, db) = open_storage(storage_uri, cache, true, runtime)?;
    let dev_id = with_admin_lock(store, &db, holder, 60, || run_init(&db)).await?;
    println!("Initialized. Device ID: {dev_id}");
    Ok(())
}

async fn cmd_remove_folder(
    storage_uri: &str,
    cache: &CacheArgs,
    holder: &str,
    folder: String,
    runtime: tokio::runtime::Handle,
) -> Result<()> {
    let _instance = acquire_single_instance(holder)?;
    let (store, _, db) = open_storage(storage_uri, cache, false, runtime)?;
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

async fn cmd_list_folders(
    storage_uri: &str,
    cache: &CacheArgs,
    runtime: tokio::runtime::Handle,
) -> Result<()> {
    let (_, _, db) = open_storage(storage_uri, cache, false, runtime)?;
    let folders = db
        .list_folders_unlocked()
        .await
        .context("failed to list folders")?;
    if folders.is_empty() {
        println!("No folders registered.");
    } else {
        // Only the first two columns need padding; the storage key is last.
        let (h0, h1, h2) = ("ID", "LABEL", "OBJECT_STORE");
        let (w0, w1) = folders
            .iter()
            .fold((h0.len(), h1.len()), |(m0, m1), (id, label, _)| {
                (m0.max(id.as_str().len()), m1.max(label.as_str().len()))
            });
        println!("  {h0:<w0$}  {h1:<w1$}  {h2}");
        for (id, label, sk) in &folders {
            println!("  {id:<w0$}  {label:<w1$}  {sk}");
        }
    }
    db.close().await?;
    Ok(())
}

async fn cmd_get_id(
    storage_uri: &str,
    cache: &CacheArgs,
    runtime: tokio::runtime::Handle,
) -> Result<()> {
    let (_, _, db) = open_storage(storage_uri, cache, false, runtime)?;
    let dev_id = run_get_id(&db).await?;
    println!("{dev_id}");
    db.close().await?;
    Ok(())
}

async fn async_main(
    cli: Cli,
    // Only read by the `InstallService` arm (self-manage builds).
    #[cfg_attr(not(feature = "self-manage"), allow(unused_variables))] cli_storage_uri: Option<
        String,
    >,
    storage_runtime_handle: tokio::runtime::Handle,
) -> Result<()> {
    let machine_id = cli.machine_id.clone().unwrap_or_else(|| {
        machine_uid::get().unwrap_or_else(|_| {
            tracing::warn!(
                "Could not retrieve machine UID, lock holder IDs may not be stable across restarts"
            );
            uuid::Uuid::new_v4().to_string()
        })
    });

    // The self-manage subcommands do not open the store and must work with no
    // storage configured. Give them a distinct lock scope (so they don't collide
    // with a running daemon) and skip the storage-URI check entirely.
    let is_self_manage = cli.command.is_self_manage();
    let storage_uri: String = if is_self_manage {
        String::new()
    } else {
        cli.require_storage_uri()?.to_owned()
    };
    let holder = if is_self_manage {
        format!("{machine_id}/self-manage")
    } else {
        format!("{machine_id}/{storage_uri}")
    };
    let cache = cli.cache.clone();

    match cli.command {
        Commands::Init => {
            cmd_init(&storage_uri, &cache, &holder, storage_runtime_handle).await?;
        }
        Commands::RemoveFolder { folder } => {
            cmd_remove_folder(
                &storage_uri,
                &cache,
                &holder,
                folder,
                storage_runtime_handle,
            )
            .await?;
        }
        Commands::ListFolders => {
            cmd_list_folders(&storage_uri, &cache, storage_runtime_handle).await?;
        }
        Commands::GetId => {
            cmd_get_id(&storage_uri, &cache, storage_runtime_handle).await?;
        }
        Commands::Serve {
            master_device_id,
            listen,
            priority,
            lease,
        } => {
            let master_device_id = master_device_id.ok_or_else(|| {
                anyhow!("no master device ID configured\nset BEPOSITORY_MASTER_DEVICE_ID in /etc/bepository/env or pass it as an argument: bepository serve <MASTER_DEVICE_ID>")
            })?;
            cmd_serve(
                &storage_uri,
                &cache,
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
            check,
            regenerate_id,
            clear_lock,
            compact,
        } => {
            cmd_fsck(
                &storage_uri,
                &cache,
                check,
                regenerate_id,
                clear_lock,
                compact,
                &holder,
                storage_runtime_handle,
            )
            .await?;
        }
        Commands::Checkpoint { action } => {
            cmd_checkpoint(
                &storage_uri,
                &cache,
                action,
                &holder,
                storage_runtime_handle,
            )
            .await?;
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
            service::install_service(no_auto_upgrade, cli_storage_uri.as_deref())?;
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

#[allow(clippy::too_many_arguments)]
async fn cmd_fsck(
    storage_uri: &str,
    cache: &CacheArgs,
    check: Option<FsckLevel>,
    regenerate_id: bool,
    clear_lock_flag: bool,
    compact: bool,
    holder: &str,
    runtime: tokio::runtime::Handle,
) -> Result<()> {
    let _instance = acquire_single_instance(holder)?;
    let (store, _, db) = open_storage(storage_uri, cache, false, runtime)?;
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
    storage_uri: &str,
    cache: &CacheArgs,
    interval: Duration,
    value: Duration,
    holder: &str,
    runtime: tokio::runtime::Handle,
) -> Result<()> {
    let interval_dur = validate_checkpoint_duration(interval)?;
    let ttl = validate_checkpoint_duration(value)?;
    let _instance = acquire_single_instance(holder)?;
    let (store, _, db) = open_storage(storage_uri, cache, false, runtime)?;
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
    storage_uri: &str,
    cache: &CacheArgs,
    interval: Duration,
    holder: &str,
    runtime: tokio::runtime::Handle,
) -> Result<()> {
    let interval_dur = validate_checkpoint_duration(interval)?;
    let _instance = acquire_single_instance(holder)?;
    let (store, _, db) = open_storage(storage_uri, cache, false, runtime)?;
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

async fn checkpoint_list(
    storage_uri: &str,
    cache: &CacheArgs,
    runtime: tokio::runtime::Handle,
) -> Result<()> {
    let (_, _, db) = open_storage(storage_uri, cache, false, runtime)?;
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
    storage_uri: &str,
    cache: &CacheArgs,
    addr: String,
    password: secrecy::SecretString,
    runtime: tokio::runtime::Handle,
) -> Result<()> {
    let (store, cache, _) = open_storage(storage_uri, cache, false, runtime.clone())?;
    let db = Arc::new(SlateStorage::new(store, Some(cache), runtime));

    let cancel = CancellationToken::new();
    spawn_cancel_on_ctrl_c(cancel.clone(), false);

    bepository_dav::serve(db, &addr, &password, cancel)
        .await
        .map_err(|e| anyhow!("{e}"))?;
    Ok(())
}

async fn cmd_checkpoint(
    storage_uri: &str,
    cache: &CacheArgs,
    action: CheckpointAction,
    holder: &str,
    runtime: tokio::runtime::Handle,
) -> Result<()> {
    match action {
        CheckpointAction::Every {
            interval,
            op: EveryOp::Ttl { value },
        } => checkpoint_set_ttl(storage_uri, cache, interval, value, holder, runtime).await,
        CheckpointAction::Every {
            interval,
            op: EveryOp::Remove,
        } => checkpoint_remove(storage_uri, cache, interval, holder, runtime).await,
        CheckpointAction::List => checkpoint_list(storage_uri, cache, runtime).await,
        CheckpointAction::Serve {
            addr,
            webdav_password,
        } => {
            let webdav_password = webdav_password.ok_or_else(|| {
                anyhow!("no WebDAV password configured\nset BEPOSITORY_DAV_PASSWORD in /etc/bepository/env or pass --webdav-password")
            })?;
            checkpoint_serve_dav(storage_uri, cache, addr, webdav_password, runtime).await
        }
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

    /// `load_env_file` sets unset vars, leaves already-set vars untouched, and
    /// treats a missing file as a no-op. Merged into one test so the two
    /// `BEPOSITORY_ENV_FILE` mutations can't race under cargo's parallel runner.
    #[test]
    fn env_file_load_and_missing_file_are_handled() {
        const NEW: &str = "BEPOSITORY_TEST_ENV_FILE_NEW";
        const EXISTING: &str = "BEPOSITORY_TEST_ENV_FILE_EXISTING";
        let _env = test_env::EnvGuard::lock(&[
            NEW,
            EXISTING,
            "BEPOSITORY_TEST_ENV_FILE_QUOTED",
            "BEPOSITORY_ENV_FILE",
        ]);
        // Safety: serialized by _env; restored on Drop.
        unsafe {
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
    }
    /// `merge_object_store_opts` must lowercase `AWS_*` env keys (object_store's
    /// parser is case-sensitive and silently drops unrecognised keys — the bug
    /// that sent S3 to the EC2 metadata endpoint), while leaving other env vars
    /// alone so a stray `TOKEN` can't become the S3 session token.
    #[test]
    fn merge_object_store_opts_lowercases_aws_env_keys_only() {
        let _env = test_env::EnvGuard::lock(&[
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "TOKEN",
        ]);
        // Safety: serialized by _env.
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", "TestKeyId");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "Secret/MixedCase");
            std::env::set_var("AWS_SESSION_TOKEN", "");
            std::env::set_var("TOKEN", "must-not-leak");
        }

        let url = url::Url::parse(
            "s3://bucket/prefix?region=eu-central-003&ENDPOINT=https://ep.example.com",
        )
        .unwrap();
        let opts = merge_object_store_opts(&url);

        let has = |k: &str, v: &str| opts.iter().any(|(ok, ov)| ok == k && ov == v);
        // AWS_* env keys arrive lowercased; values keep their case.
        assert!(has("aws_access_key_id", "TestKeyId"));
        assert!(has("aws_secret_access_key", "Secret/MixedCase"));
        assert!(!opts.iter().any(|(k, _)| k == "AWS_ACCESS_KEY_ID"));
        // Empty values are filtered out (the env template ships empty AWS_*).
        assert!(!opts.iter().any(|(k, _)| k == "aws_session_token"));
        // Query-param keys are lowercased too; values untouched.
        assert!(has("region", "eu-central-003"));
        assert!(has("endpoint", "https://ep.example.com"));
        // A stray non-provider env var must not be offered as a config key.
        assert!(!opts.iter().any(|(k, _)| k == "token"));
    }

    /// Without explicit credentials, `require_explicit_credentials` must fail
    /// fast instead of letting object_store silently probe EC2/GCE metadata.
    /// `use_ambient_creds=true` is the documented escape hatch.
    #[test]
    fn require_explicit_credentials_blocks_implicit_metadata_fallback() {
        let _env = test_env::EnvGuard::lock(&[
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            "AWS_ROLE_ARN",
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "GOOGLE_APPLICATION_CREDENTIALS",
            "GOOGLE_SERVICE_ACCOUNT_PATH",
            "HOME",
        ]);

        let check = |uri: &str| {
            let url = url::Url::parse(uri).unwrap();
            require_explicit_credentials(&url, &merge_object_store_opts(&url))
        };
        let ok = |uri: &str| assert!(check(uri).is_ok(), "expected preflight to pass: {uri}");
        let blocked = |uri: &str| {
            let err = check(uri)
                .err()
                .unwrap_or_else(|| panic!("expected preflight to block: {uri}"));
            assert!(
                err.to_string().contains("metadata"),
                "error should mention metadata for {uri}, got: {err}"
            );
        };

        // No credentials → blocked for both cloud schemes.
        blocked("s3://bucket/prefix");
        blocked("gs://bucket/prefix");

        // object_store also routes these https hosts to the S3 builder.
        blocked("https://bucket.s3.us-east-1.amazonaws.com/prefix");
        blocked("https://s3.us-east-1.amazonaws.com/bucket/prefix");
        blocked("https://account-id.r2.cloudflarestorage.com/bucket/prefix");

        // S3: explicit creds, anonymous, and the escape hatch pass.
        ok("s3://b/p?access_key_id=x&secret_access_key=y");
        ok("https://bucket.s3.amazonaws.com/p?access_key_id=x&secret_access_key=y");
        ok("s3://b/p?skip_signature=true");
        ok("s3://b/p?aws_skip_signature=true");
        ok("s3://b/p?use_ambient_creds=true");
        // Truthy spellings mirror object_store's boolean config parsing
        // (1|true|on|yes|y), consistently for provider keys and our own.
        ok("s3://b/p?skip_signature=yes");
        ok("s3://b/p?aws_skip_signature=1");
        ok("s3://b/p?use_ambient_creds=on");

        // Empty env credentials count as absent (the env template ships empty
        // AWS_* placeholders) — they must neither configure S3 nor shadow the
        // ambient chain the escape hatch enables.
        // Safety: serialized by _env.
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", "");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "");
        }
        blocked("s3://b/p");
        ok("s3://b/p?use_ambient_creds=true");
        // Safety: serialized by _env.
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", "envkey");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "envsecret");
        }
        ok("s3://b/p");
        // Safety: serialized by _env.
        unsafe {
            std::env::remove_var("AWS_ACCESS_KEY_ID");
            std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        }

        // Pod/machine identity is ambient too — gated behind use_ambient_creds
        // (build() resolves web identity from env only; container creds hit a
        // metadata endpoint themselves).
        blocked("s3://b/p?aws_web_identity_token_file=/tmp/tok");
        blocked("s3://b/p?aws_container_credentials_relative_uri=/role");
        blocked("s3://b/p?aws_container_credentials_full_uri=http://x");
        ok("s3://b/p?aws_web_identity_token_file=/tmp/tok&use_ambient_creds=true");

        // GCS: explicit creds, anonymous, and the escape hatch pass.
        // skip_signature never fetches a token (gcp get_credential returns
        // None), so it is metadata-free like on S3.
        ok("gs://b/p?google_service_account=/tmp/sa.json");
        ok("gs://b/p?service_account=/tmp/sa.json");
        ok("gs://b/p?google_service_account_path=/tmp/sa.json");
        ok("gs://b/p?service_account_path=/tmp/sa.json");
        ok("gs://b/p?google_service_account_key={}");
        ok("gs://b/p?google_application_credentials=/tmp/sa.json");
        ok("gs://b/p?skip_signature=true");
        ok("gs://b/p?google_skip_signature=y");
        ok("gs://b/p?use_ambient_creds=true");

        // Uppercase env vars object_store silently drops must NOT count as
        // configured — that would let the metadata probe back in.
        // Safety: serialized by _env; removed again below.
        unsafe {
            std::env::set_var("GOOGLE_SERVICE_ACCOUNT_PATH", "/tmp/sa.json");
        }
        blocked("gs://b/p");
        unsafe {
            std::env::remove_var("GOOGLE_SERVICE_ACCOUNT_PATH");
        }

        // The well-known ADC file is intentionally NOT consulted: even with
        // HOME pointed at a dir containing it, GCS is still blocked.
        let home = tempfile::tempdir().unwrap();
        let gcloud_dir = home.path().join(".config/gcloud");
        std::fs::create_dir_all(&gcloud_dir).unwrap();
        std::fs::write(
            gcloud_dir.join("application_default_credentials.json"),
            "{\"type\": \"authorized_user\"}",
        )
        .unwrap();
        // Safety: serialized by _env; restored on Drop.
        unsafe {
            std::env::set_var("HOME", home.path());
        }
        blocked("gs://b/p");

        // No credential chain for these schemes → always allowed.
        ok("https://example.com/bucket");
        ok("file:///tmp/x");
        ok("sftp://h/p?key=/tmp/k");
        ok("memory://");
    }

    /// `install-service --storage-uri x` must report a CommandLine value
    /// source (so the URI gets persisted) even though `storage_uri` is a
    /// global arg passed after the subcommand; the same URI via
    /// `BEPOSITORY_STORAGE_URI` must NOT be persisted.
    #[cfg(feature = "self-manage")]
    #[test]
    fn storage_uri_source_distinguishes_command_line_from_env() {
        use clap::CommandFactory;
        let _env = test_env::EnvGuard::lock(&["BEPOSITORY_STORAGE_URI"]);
        const URI: &str = "sftp://user@example.com/srv/bepository?key=/tmp/id";

        // Flag after the subcommand → CommandLine source → persisted.
        let matches = Cli::command()
            .try_get_matches_from(["bepository", "install-service", "--storage-uri", URI])
            .unwrap();
        assert_eq!(
            matches.value_source("storage_uri"),
            Some(clap::parser::ValueSource::CommandLine)
        );
        assert_eq!(command_line_storage_uri(&matches).as_deref(), Some(URI));

        // Flag before the subcommand → same.
        let matches = Cli::command()
            .try_get_matches_from(["bepository", "--storage-uri", URI, "install-service"])
            .unwrap();
        assert_eq!(command_line_storage_uri(&matches).as_deref(), Some(URI));

        // Same URI from the environment → env source → not persisted.
        // Safety: serialized by _env.
        unsafe { std::env::set_var("BEPOSITORY_STORAGE_URI", URI) };
        let matches = Cli::command()
            .try_get_matches_from(["bepository", "install-service"])
            .unwrap();
        assert_eq!(
            matches.value_source("storage_uri"),
            Some(clap::parser::ValueSource::EnvVariable)
        );
        assert_eq!(command_line_storage_uri(&matches), None);
    }

    /// Canary: the preflight gate hardcodes object_store's config-key
    /// spellings. If an upgrade changes them, this fails in the upgrade PR
    /// instead of silently desyncing the gate.
    #[test]
    fn object_store_config_key_spellings_match_the_gate() {
        use object_store::aws::AmazonS3ConfigKey;
        use object_store::gcp::GoogleConfigKey;
        use std::str::FromStr;

        for s in ["aws_access_key_id", "access_key_id"] {
            assert!(
                matches!(
                    AmazonS3ConfigKey::from_str(s),
                    Ok(AmazonS3ConfigKey::AccessKeyId)
                ),
                "{s}"
            );
        }
        for s in ["aws_skip_signature", "skip_signature"] {
            assert!(
                matches!(
                    AmazonS3ConfigKey::from_str(s),
                    Ok(AmazonS3ConfigKey::SkipSignature)
                ),
                "{s}"
            );
        }
        for s in [
            "google_service_account",
            "service_account",
            "google_service_account_path",
            "service_account_path",
        ] {
            assert!(
                matches!(
                    GoogleConfigKey::from_str(s),
                    Ok(GoogleConfigKey::ServiceAccount)
                ),
                "{s}"
            );
        }
        for s in ["google_service_account_key", "service_account_key"] {
            assert!(
                matches!(
                    GoogleConfigKey::from_str(s),
                    Ok(GoogleConfigKey::ServiceAccountKey)
                ),
                "{s}"
            );
        }
        for s in ["google_application_credentials", "application_credentials"] {
            assert!(
                matches!(
                    GoogleConfigKey::from_str(s),
                    Ok(GoogleConfigKey::ApplicationCredentials)
                ),
                "{s}"
            );
        }
        // The deleted CLOUDSDK_AUTH_ACCESS_TOKEN alias mapped to bearer_token,
        // which 0.12 rejects — the alias was dead code.
        assert!(GoogleConfigKey::from_str("bearer_token").is_err());
    }
}
