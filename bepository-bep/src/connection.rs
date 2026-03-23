// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use parking_lot::Mutex;
use prost::Message;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::oneshot;
use tokio::time::{self, Duration};
use tokio_util::sync::CancellationToken;

use crate::conflict::ConflictResolver;
use crate::device_id::DeviceId;
use crate::error::{BepError, Result, StorageError};
use crate::framing;
use crate::ids::{FolderId, FolderLabel};
use crate::proto::bep::*;
use crate::retry::{ExponentialBackoff, RetryPolicy};
use crate::storage::{Sequence, Storage, StorageFolder, UpdateResult};

/// Per-connection tuning knobs.
#[derive(Clone)]
pub struct ConnectionOptions {
    /// Max concurrent outstanding block requests per connection.
    pub max_pending_requests: usize,
    /// Send a Ping if no message received for this long.
    pub ping_interval: Duration,
    /// Controls retry behaviour and `StorageError` → `BepError` mapping.
    pub retry_policy: Arc<dyn RetryPolicy>,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            max_pending_requests: 16,
            ping_interval: Duration::from_secs(90),
            retry_policy: Arc::new(ExponentialBackoff::default()),
        }
    }
}

/// Why a connection closed.
#[derive(Debug, Clone)]
pub enum CloseReason {
    /// We initiated graceful shutdown.
    Local,
    /// Remote sent a Close message.
    Remote(String),
    /// An error occurred.
    Error(BepError),
}

/// Lightweight handle returned to the caller after connect/accept.
pub struct ConnectionHandle {
    /// The remote peer's device ID.
    pub device_id: DeviceId,
    /// Resolves when the connection closes, with the reason.
    pub closed: oneshot::Receiver<CloseReason>,
    /// Cancel to request graceful shutdown of this connection.
    pub shutdown: CancellationToken,
}

/// Common metadata for a single block within a file.
#[derive(Clone)]
struct FileBlock<F> {
    folder: F,
    name: String,
    offset: i64,
    hash: Vec<u8>,
    version: Option<crate::proto::bep::Vector>,
}

/// A block request that was deferred because `max_pending_requests` was reached.
struct DeferredRequest<F> {
    block: FileBlock<F>,
    size: i32,
    block_no: i32,
}

/// Internal state for a running connection.
struct ConnectionInner<S: Storage> {
    storage: Arc<S>,
    /// Folders both peers agreed to share during ClusterConfig exchange.
    mutual_folders: std::collections::HashMap<FolderId, u64>,
    /// Folder IDs included in the last CC we sent to the peer.
    /// Used to decide whether to send an updated CC when new folders become mutual.
    our_cc_folders: std::collections::HashSet<FolderId>,
    /// Pending request IDs → block metadata for storing responses.
    pending_requests: HashMap<i32, FileBlock<S::Folder>>,
    /// Next request ID.
    next_request_id: i32,
    /// Max concurrent outstanding block requests.
    max_pending_requests: usize,
    /// Blocks deferred because we were at capacity; drained as responses arrive.
    deferred_blocks: std::collections::VecDeque<DeferredRequest<S::Folder>>,
}

impl<S: Storage> ConnectionInner<S> {
    /// Returns the next available request ID, skipping any that are currently in-flight.
    ///
    /// NOTE: We use a simple wrapping counter to cycle through the full i32 space,
    /// rather than using an ID allocator (like `intid-allocator`). While an allocator
    /// might allow for a more compact storage (e.g. using a `Vec` for `pending_requests`),
    /// cycling through IDs ensures we don't reuse the same ID for a long time. This
    /// prevents misidentifying responses if an old request's response arrives very
    /// late or out-of-order, making the protocol more robust and easier to debug.
    fn next_request_id(&mut self) -> i32 {
        loop {
            let id = self.next_request_id;
            self.next_request_id = id.wrapping_add(1);
            if !self.pending_requests.contains_key(&id) {
                return id;
            }
        }
    }
}

struct ConnectionContext<'a> {
    remote_device: DeviceId,
    local_device: DeviceId,
    resolver: &'a dyn ConflictResolver,
    policy: &'a dyn RetryPolicy,
    shutdown: &'a CancellationToken,
}

impl<'a> ConnectionContext<'a> {
    /// Helper to run storage operations with the context's retry policy and shutdown token.
    async fn retry<F, Fut, T>(&self, op: &str, f: F) -> crate::error::Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, StorageError>> + Send,
    {
        crate::retry::retry_storage_op(self.policy, self.shutdown, op, f).await
    }
}

async fn perform_hello<R, W>(reader: &mut R, writer: &mut W, device_name: &str) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    framing::send_hello(writer, device_name).await?;
    let peer_hello = framing::recv_hello(reader).await?;
    tracing::info!(
        peer_name = %peer_hello.device_name,
        peer_client = %peer_hello.client_name,
        peer_version = %peer_hello.client_version,
        "hello exchange complete"
    );
    Ok(())
}

/// Send our initial ClusterConfig and process the peer's reply.
///
/// We compute the initial folder list from both the engine's configured
/// `shared_folders` AND the current storage state. This ensures that folders
/// auto-registered during prior connections are included even though
/// `shared_folders` is fixed at engine-creation time.
///
/// If the peer proposes folders we didn't include in our initial CC,
/// `process_peer_cc` registers them in storage and sends an updated CC so the
/// peer can include them in its mutual set. Further CC updates from the peer
/// are handled identically by `handle_cluster_config_update` in the message
/// loop.
///
/// Returns `(mutual_folders, our_cc_folders)`.
async fn exchange_initial_cluster_config<S, R, W>(
    storage: &Arc<S>,
    reader: &mut R,
    writer: &mut W,
    ctx: &ConnectionContext<'_>,
    shared_folders: &[FolderId],
) -> Result<(
    std::collections::HashMap<FolderId, u64>,
    std::collections::HashSet<FolderId>,
)>
where
    S: Storage,
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut initial_folders: Vec<FolderId> = shared_folders.to_vec();
    let folders = ctx.retry("list_folders", || storage.list_folders()).await?;
    for (id, _, _) in folders {
        if !initial_folders.contains(&id) {
            initial_folders.push(id);
        }
    }

    let cluster_config = build_cluster_config(
        storage,
        ctx,
        &ctx.local_device,
        &ctx.remote_device,
        &initial_folders,
    )
    .await?;
    tracing::debug!(folders = ?initial_folders, "sending ClusterConfig");
    send_typed_message(writer, MessageType::ClusterConfig, &cluster_config).await?;

    let peer_cc_msg = framing::read_message(reader).await?;
    if peer_cc_msg.header.r#type != MessageType::ClusterConfig as i32 {
        return Err(BepError::PeerBadMessage(format!(
            "unexpected message type {} in state cluster_config",
            peer_cc_msg.header.r#type
        )));
    }
    let peer_cc = ClusterConfig::decode(peer_cc_msg.body)
        .map_err(|e| BepError::PeerBadMessage(format!("protobuf decode error: {e}")))?;

    let our_cc_initial: std::collections::HashSet<FolderId> = initial_folders.into_iter().collect();
    let (mutual_folders, new_for_our_cc) = process_peer_cc(
        &peer_cc,
        storage,
        writer,
        ctx,
        &std::collections::HashMap::new(),
        &our_cc_initial,
    )
    .await?;
    let our_cc_folders: std::collections::HashSet<FolderId> =
        our_cc_initial.into_iter().chain(new_for_our_cc).collect();

    tracing::info!(mutual = ?mutual_folders, "cluster config exchanged");

    Ok((mutual_folders.into_iter().collect(), our_cc_folders))
}

/// Run the BEP connection protocol over a stream.
///
/// This is the main entry point called by BepEngine::connect/accept.
/// It handles Hello, ClusterConfig, and then the main message loop.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_connection<S, T>(
    storage: Arc<S>,
    resolver: Arc<dyn ConflictResolver>,
    local_device: DeviceId,
    remote_device: DeviceId,
    device_name: String,
    shared_folders: Vec<FolderId>,
    options: ConnectionOptions,
    stream: T,
    close_tx: oneshot::Sender<CloseReason>,
    shutdown: CancellationToken,
) where
    S: Storage,
    T: AsyncRead + AsyncWrite + Send + 'static,
{
    let result = run_connection_inner(
        storage,
        resolver,
        local_device,
        remote_device,
        device_name,
        shared_folders,
        options,
        stream,
        &shutdown,
    )
    .await;

    let reason = match result {
        Ok(reason) => reason,
        Err(e) => CloseReason::Error(e),
    };

    let _ = close_tx.send(reason);
}

#[allow(clippy::too_many_arguments)]
#[tracing::instrument(level = "info", skip(storage, resolver, local_device, device_name, shared_folders, options, stream, shutdown), fields(remote_device = %remote_device))]
async fn run_connection_inner<S, T>(
    storage: Arc<S>,
    resolver: Arc<dyn ConflictResolver>,
    local_device: DeviceId,
    remote_device: DeviceId,
    device_name: String,
    shared_folders: Vec<FolderId>,
    options: ConnectionOptions,
    stream: T,
    shutdown: &CancellationToken,
) -> Result<CloseReason>
where
    S: Storage,
    T: AsyncRead + AsyncWrite + Send + 'static,
{
    let (mut reader, mut writer) = tokio::io::split(stream);

    perform_hello(&mut reader, &mut writer, &device_name).await?;

    let ctx = ConnectionContext {
        remote_device,
        local_device,
        resolver: resolver.as_ref(),
        policy: options.retry_policy.as_ref(),
        shutdown,
    };

    let (mutual_folders, our_cc_folders) =
        exchange_initial_cluster_config(&storage, &mut reader, &mut writer, &ctx, &shared_folders)
            .await?;

    let inner = Arc::new(Mutex::new(ConnectionInner {
        storage: storage.clone(),
        mutual_folders,
        our_cc_folders,
        pending_requests: HashMap::new(),
        next_request_id: 0,
        max_pending_requests: options.max_pending_requests,
        deferred_blocks: std::collections::VecDeque::new(),
    }));

    run_message_loop(
        &inner,
        &mut reader,
        &mut writer,
        &ctx,
        options.ping_interval,
    )
    .await
}
async fn build_cluster_config<S: Storage>(
    storage: &Arc<S>,
    ctx: &ConnectionContext<'_>,
    local_device: &DeviceId,
    remote_device: &DeviceId,
    folders: &[FolderId],
) -> Result<ClusterConfig> {
    let mut cc_folders = Vec::with_capacity(folders.len());
    for folder_id in folders {
        let f = ctx.retry("folder", || storage.folder(*folder_id)).await?;
        let seq = f.local_sequence().await.unwrap_or(Sequence::ZERO).get();
        cc_folders.push(Folder {
            id: folder_id.to_string(),
            label: f.label().to_string(),
            devices: vec![
                Device {
                    id: local_device.as_bytes().to_vec(),
                    max_sequence: seq,
                    ..Default::default()
                },
                Device {
                    id: remote_device.as_bytes().to_vec(),
                    max_sequence: 0,
                    ..Default::default()
                },
            ],
            ..Default::default()
        });
    }
    Ok(ClusterConfig {
        folders: cc_folders,
        secondary: false,
    })
}

/// Process a received [`ClusterConfig`] from the peer.
///
/// Ensures all proposed folders are registered in storage, sends our Index for
/// newly-mutual folders, and sends an updated CC back if the peer introduced
/// folders we hadn't advertised yet.
///
/// Returns `(new_mutual, new_for_our_cc)` where:
/// - `new_mutual`: folders the peer proposed that weren't already in `current_mutual`
/// - `new_for_our_cc`: subset of `new_mutual` absent from `our_cc` (caller adds to its CC set)
async fn process_peer_cc<S: Storage, W: AsyncWrite + Unpin>(
    peer_cc: &ClusterConfig,
    storage: &Arc<S>,
    writer: &mut W,
    ctx: &ConnectionContext<'_>,
    current_mutual: &std::collections::HashMap<FolderId, u64>,
    our_cc: &std::collections::HashSet<FolderId>,
) -> Result<(Vec<(FolderId, u64)>, Vec<FolderId>)> {
    let mut folder_refs: Vec<(FolderId, FolderLabel)> = Vec::new();
    let mut parsed_folders: Vec<(FolderId, u64)> = Vec::new();

    for f in &peer_cc.folders {
        let folder_id = FolderId::from(f.id.clone());
        let dev = f
            .devices
            .iter()
            .find(|d| d.id == ctx.remote_device.as_bytes())
            .ok_or_else(|| {
                BepError::PeerBadMessage(format!(
                    "peer omitted themselves from folder {} in ClusterConfig",
                    f.id
                ))
            })?;
        folder_refs.push((folder_id, FolderLabel::from(f.label.clone())));
        parsed_folders.push((folder_id, dev.index_id));
    }

    let created = ctx
        .retry("ensure_folders", || storage.ensure_folders(&folder_refs))
        .await?;
    for ((folder_id, _), is_new) in folder_refs.iter().zip(&created) {
        if *is_new {
            tracing::info!(id = %folder_id, "auto-registered new folder");
        }
    }

    tracing::debug!(
        folders = ?peer_cc.folders.iter().map(|f| &f.id).collect::<Vec<_>>(),
        "received ClusterConfig"
    );

    // Folders the peer proposed that weren't already in our mutual set.
    let new_mutual: Vec<(FolderId, u64)> = parsed_folders
        .into_iter()
        .filter(|(id, _)| !current_mutual.contains_key(id))
        .collect();

    // Subset of new_mutual that we haven't advertised in our own CC yet.
    let new_for_our_cc: Vec<FolderId> = new_mutual
        .iter()
        .filter(|(id, _)| !our_cc.contains(id))
        .map(|(id, _)| *id)
        .collect();

    // Reflect new folders back in our CC so the peer's mutual set stays in sync.
    if !new_for_our_cc.is_empty() {
        let all_our_cc: Vec<FolderId> = our_cc
            .iter()
            .cloned()
            .chain(new_for_our_cc.iter().cloned())
            .collect();
        let updated_cc = build_cluster_config(
            storage,
            ctx,
            &ctx.local_device,
            &ctx.remote_device,
            &all_our_cc,
        )
        .await?;
        tracing::debug!(folders = ?all_our_cc, "sending updated ClusterConfig");
        send_typed_message(writer, MessageType::ClusterConfig, &updated_cc).await?;
    }

    // Send Index for newly-mutual folders so the peer can see our local sequence.
    for (folder_id, _) in &new_mutual {
        send_index(storage, writer, *folder_id, ctx).await?;
    }

    Ok((new_mutual, new_for_our_cc))
}

#[tracing::instrument(level = "info", skip(storage, writer, ctx), fields(folder_id = %folder_id))]
async fn send_index<S: Storage, W: AsyncWrite + Unpin>(
    storage: &Arc<S>,
    writer: &mut W,
    folder_id: FolderId,
    ctx: &ConnectionContext<'_>,
) -> Result<()> {
    let folder = ctx.retry("folder", || storage.folder(folder_id)).await?;
    let mut stream = ctx.retry("index", || folder.index(Sequence::ZERO)).await?;
    let mut files = Vec::new();
    let mut last_seq = 0i64;

    while let Some(result) = stream.next().await {
        let fi = result.map_err(|e| ctx.policy.map_error(e))?;
        if fi.sequence > last_seq {
            last_seq = fi.sequence;
        }
        files.push(fi);
    }

    let index = Index {
        folder: folder_id.to_string(),
        files,
        last_sequence: last_seq,
    };
    send_typed_message(writer, MessageType::Index, &index).await
}

async fn apply_remote_index<S, W>(
    inner: &Arc<Mutex<ConnectionInner<S>>>,
    writer: &mut W,
    folder_id_str: &str,
    files: &[FileInfo],
    last_sequence: i64,
    ctx: &ConnectionContext<'_>,
) -> Result<()>
where
    S: Storage,
    W: AsyncWrite + Unpin,
{
    let (storage, peer_index_id) = {
        let conn = inner.lock();
        let peer_index_id = conn
            .mutual_folders
            .get(&FolderId::new(folder_id_str))
            .copied();
        (Arc::clone(&conn.storage), peer_index_id)
    };

    let index_id = peer_index_id.ok_or_else(|| {
        BepError::PeerBadMessage(format!(
            "Index for non-mutual folder {folder_id_str} (ClusterConfig not yet exchanged)"
        ))
    })?;

    let folder = ctx
        .retry("folder", || storage.folder(FolderId::new(folder_id_str)))
        .await?;

    let mut needed: Vec<FileInfo> = Vec::new();
    for file in files {
        let result = ctx
            .retry("apply_update", || {
                folder.apply_update(file, &ctx.remote_device)
            })
            .await?;
        match result {
            UpdateResult::NeedBlocks(fi) => {
                tracing::debug!(file = %fi.name, blocks = fi.blocks.len(), "need blocks");
                needed.push(fi);
            }
            UpdateResult::NoAction => {
                tracing::debug!(file = %file.name, "no action needed");
            }
            UpdateResult::Applied(fi) => {
                tracing::debug!(file = %fi.name, "applied (no blocks needed)");
                send_index_update(&fi, folder.id(), writer).await?;
            }
            UpdateResult::Concurrent { local, remote } => {
                let resolution =
                    ctx.resolver
                        .resolve(&local, &ctx.local_device, &remote, &ctx.remote_device)?;
                tracing::info!(
                    file = %resolution.winner.name,
                    conflict = resolution.loser_path.as_deref().unwrap_or("(discarded)"),
                    "conflict resolved"
                );
                ctx.retry("resolve_conflict", || {
                    folder.resolve_conflict(
                        resolution.winner,
                        resolution.loser,
                        resolution.loser_path.as_deref(),
                    )
                })
                .await?;
                if !resolution.winner.deleted {
                    needed.push(resolution.winner.clone());
                }
            }
        }
    }

    for fi in &needed {
        request_blocks(inner, writer, &folder, fi, ctx).await?;
        complete_and_notify(inner, &folder, &fi.name, fi.version.as_ref(), writer, ctx).await?;
    }

    let new_state = crate::storage::RemoteIndexState {
        index_id,
        max_sequence: Sequence(last_sequence),
    };
    ctx.retry("set_remote_state", || {
        folder.set_remote_state(&ctx.remote_device, new_state.clone())
    })
    .await?;

    tracing::info!(
        files_count = files.len(),
        needed_count = needed.len(),
        "index handled"
    );

    Ok(())
}

#[tracing::instrument(level = "info", skip(inner, writer, ctx, index), fields(folder_id = %index.folder), err)]
async fn handle_index<S: Storage, W: AsyncWrite + Unpin>(
    inner: &Arc<Mutex<ConnectionInner<S>>>,
    writer: &mut W,
    index: Index,
    ctx: &ConnectionContext<'_>,
) -> Result<()> {
    tracing::debug!(
        folder = %index.folder,
        files = index.files.len(),
        "received Index"
    );
    apply_remote_index(
        inner,
        writer,
        &index.folder,
        &index.files,
        index.last_sequence,
        ctx,
    )
    .await
}

#[tracing::instrument(level = "debug", skip(inner, writer, ctx), fields(folder_id = %update.folder), err)]
async fn handle_index_update<S: Storage, W: AsyncWrite + Unpin>(
    inner: &Arc<Mutex<ConnectionInner<S>>>,
    writer: &mut W,
    update: IndexUpdate,
    ctx: &ConnectionContext<'_>,
) -> Result<()> {
    tracing::debug!(
        folder = %update.folder,
        files = update.files.len(),
        "received IndexUpdate"
    );
    apply_remote_index(
        inner,
        writer,
        &update.folder,
        &update.files,
        update.last_sequence,
        ctx,
    )
    .await
}

async fn run_message_loop<S, R, W>(
    inner: &Arc<Mutex<ConnectionInner<S>>>,
    reader: &mut R,
    writer: &mut W,
    ctx: &ConnectionContext<'_>,
    ping_interval_duration: Duration,
) -> Result<CloseReason>
where
    S: Storage,
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut ping_interval = time::interval(ping_interval_duration);
    ping_interval.reset(); // Don't fire immediately

    loop {
        tokio::select! {
            // Graceful shutdown
            _ = ctx.shutdown.cancelled() => {
                let close = Close { reason: "shutdown requested".into() };
                let _ = send_typed_message(writer, MessageType::Close, &close).await;
                return Ok(CloseReason::Local);
            }

            // Keepalive
            _ = ping_interval.tick() => {
                let ping = Ping {};
                send_typed_message(writer, MessageType::Ping, &ping).await?;
            }

            // Inbound message
            msg = framing::read_message(reader) => {
                let msg = msg?;
                ping_interval.reset();

                match MessageType::try_from(msg.header.r#type) {
                    Ok(MessageType::Index) => {
                        let index = Index::decode(msg.body).map_err(|e| BepError::PeerBadMessage(format!("protobuf decode error: {e}")))?;
                        handle_index(inner, writer, index, ctx).await?;
                    }
                    Ok(MessageType::IndexUpdate) => {
                        let update = IndexUpdate::decode(msg.body).map_err(|e| BepError::PeerBadMessage(format!("protobuf decode error: {e}")))?;
                        handle_index_update(inner, writer, update, ctx).await?;
                    }
                    Ok(MessageType::Request) => {
                        let request = Request::decode(msg.body).map_err(|e| BepError::PeerBadMessage(format!("protobuf decode error: {e}")))?;
                        handle_request(inner, writer, request, ctx).await?;
                    }
                    Ok(MessageType::Response) => {
                        let response = Response::decode(msg.body).map_err(|e| BepError::PeerBadMessage(format!("protobuf decode error: {e}")))?;
                        handle_response(inner, writer, response, ctx).await?;
                    }
                    Ok(MessageType::Ping) => {
                        // No-op; receipt already reset the timer
                    }
                    Ok(MessageType::Close) => {
                        let close = Close::decode(msg.body).map_err(|e| BepError::PeerBadMessage(format!("protobuf decode error: {e}")))?;
                        tracing::info!(reason = %close.reason, "peer closed connection");
                        return Ok(CloseReason::Remote(close.reason));
                    }
                    Ok(MessageType::ClusterConfig) => {
                        let cc = ClusterConfig::decode(msg.body)
                            .map_err(|e| BepError::PeerBadMessage(format!("protobuf decode error: {e}")))?;
                        handle_cluster_config_update(inner, writer, cc, ctx).await?;
                    }
                    Ok(MessageType::DownloadProgress) => {
                        let progress = DownloadProgress::decode(msg.body)
                            .map_err(|e| BepError::PeerBadMessage(format!("protobuf decode error: {e}")))?;
                        tracing::debug!(
                            device = %ctx.remote_device,
                            folder = %progress.folder,
                            updates = progress.updates.len(),
                            "received download progress"
                        );
                    }
                    Err(_) => {
                        tracing::warn!(msg_type = msg.header.r#type, "unknown message type, ignoring");
                    }
                }
            }
        }
    }
}

/// Handle a `ClusterConfig` message received in the steady-state message loop.
///
/// BEP allows ClusterConfig to be sent more than once to add or update
/// folders. We snapshot the current state without holding the connection lock
/// across awaits, run `process_peer_cc`, then merge the new entries back.
async fn handle_cluster_config_update<S, W>(
    inner: &Arc<Mutex<ConnectionInner<S>>>,
    writer: &mut W,
    cc: ClusterConfig,
    ctx: &ConnectionContext<'_>,
) -> Result<()>
where
    S: Storage,
    W: AsyncWrite + Unpin,
{
    let (storage, current_mutual, our_cc_snap) = {
        let conn = inner.lock();
        (
            Arc::clone(&conn.storage),
            conn.mutual_folders.clone(),
            conn.our_cc_folders.clone(),
        )
    };

    let (new_mutual, new_for_our_cc) =
        process_peer_cc(&cc, &storage, writer, ctx, &current_mutual, &our_cc_snap).await?;

    {
        let mut conn = inner.lock();
        for (id, index_id) in new_mutual {
            conn.mutual_folders.insert(id, index_id);
        }
        for id in new_for_our_cc {
            conn.our_cc_folders.insert(id);
        }
    }

    Ok(())
}

async fn handle_request<S: Storage, W: AsyncWrite + Unpin>(
    inner: &Arc<Mutex<ConnectionInner<S>>>,
    writer: &mut W,
    request: Request,
    ctx: &ConnectionContext<'_>,
) -> Result<()> {
    let storage = {
        let conn = inner.lock();
        Arc::clone(&conn.storage)
    };
    let folder = ctx
        .retry("folder", || storage.folder(FolderId::new(&request.folder)))
        .await?;

    let result = if request.offset < 0 || request.size < 0 {
        tracing::warn!(
            id = request.id,
            offset = request.offset,
            size = request.size,
            folder = %request.folder,
            file = %request.name,
            "rejecting request with negative offset or size"
        );
        Err(StorageError::InvalidInput("negative offset or size".into()))
    } else {
        folder
            .read_block(&request.name, request.offset, request.size, &request.hash)
            .await
    };

    let response = match result {
        Ok(data) => Response {
            id: request.id,
            data: data.to_vec(),
            code: ErrorCode::NoError as i32,
        },
        Err(e) => {
            let code = match e {
                StorageError::NotFound(_) => ErrorCode::NoSuchFile,
                _ => ErrorCode::Generic,
            };
            Response {
                id: request.id,
                data: Vec::new(),
                code: code as i32,
            }
        }
    };

    send_typed_message(writer, MessageType::Response, &response).await
}

async fn handle_response<S: Storage, W: AsyncWrite + Unpin>(
    inner: &Arc<Mutex<ConnectionInner<S>>>,
    writer: &mut W,
    response: Response,
    ctx: &ConnectionContext<'_>,
) -> Result<()> {
    let pending = {
        let mut conn = inner.lock();
        conn.pending_requests.remove(&response.id)
    };

    let completed_file = match pending {
        Some(block) => {
            if response.code != ErrorCode::NoError as i32 {
                tracing::warn!(
                    id = response.id,
                    code = response.code,
                    folder = %block.folder.id(),
                    file = %block.name,
                    "peer returned error for block request"
                );
                return Err(BepError::PeerError {
                    code: response.code,
                    path: format!("{}/{}", block.folder.id(), block.name),
                });
            }

            let data = Bytes::from(response.data);
            ctx.retry("store_block", || {
                block
                    .folder
                    .store_block(&block.name, block.offset, &block.hash, data.clone())
            })
            .await?;

            tracing::debug!(
                folder = %block.folder.id(),
                file = %block.name,
                offset = block.offset,
                "stored block from peer"
            );

            Some((block.folder, block.name, block.version))
        }
        None => {
            tracing::warn!(id = response.id, "received response for unknown request");
            None
        }
    };

    // Drain deferred blocks now that a slot has freed up.
    drain_deferred(inner, writer, ctx).await?;

    // Check if all blocks for this file have been received.
    if let Some((folder, name, version)) = completed_file {
        complete_and_notify(inner, &folder, &name, version.as_ref(), writer, ctx).await?;
    }

    Ok(())
}

/// Send a single-file `IndexUpdate` message to the peer.
///
/// Pure messaging — no storage interaction. `fi` must already be committed to the
/// index with its sequence number assigned (e.g. returned as `Applied(fi)` from
/// `apply_update`, or as `Some(fi)` from `complete_file`).
async fn send_index_update<W: AsyncWrite + Unpin>(
    fi: &FileInfo,
    folder_id: FolderId,
    writer: &mut W,
) -> Result<()> {
    let seq = fi.sequence;
    tracing::debug!(file = %fi.name, sequence = seq, "sending IndexUpdate");
    send_typed_message(
        writer,
        MessageType::IndexUpdate,
        &IndexUpdate {
            folder: folder_id.to_string(),
            files: vec![fi.clone()],
            last_sequence: seq,
            prev_sequence: 0,
        },
    )
    .await
}

/// Call `complete_file` on storage and, if the file was committed, send a single-file
/// `IndexUpdate` to the peer.
///
/// Returns without sending if the file still has pending or deferred block requests,
/// or if `complete_file` returns `None` (version mismatch / not staged).
async fn complete_and_notify<S: Storage, W: AsyncWrite + Unpin>(
    inner: &Arc<Mutex<ConnectionInner<S>>>,
    folder: &S::Folder,
    name: &str,
    version: Option<&crate::proto::bep::Vector>,
    writer: &mut W,
    ctx: &ConnectionContext<'_>,
) -> Result<()> {
    if let Some(fi) = maybe_complete_file(inner, folder, name, version, ctx).await? {
        let seq = fi.sequence;
        send_typed_message(
            writer,
            MessageType::IndexUpdate,
            &IndexUpdate {
                folder: folder.id().to_string(),
                files: vec![fi],
                last_sequence: seq,
                prev_sequence: 0,
            },
        )
        .await?;
    }
    Ok(())
}

async fn maybe_complete_file<S: Storage>(
    inner: &Arc<Mutex<ConnectionInner<S>>>,
    folder: &S::Folder,
    name: &str,
    version: Option<&crate::proto::bep::Vector>,
    ctx: &ConnectionContext<'_>,
) -> Result<Option<FileInfo>> {
    let should_complete = {
        let conn = inner.lock();
        let folder_id = folder.id();
        let has_pending = conn
            .pending_requests
            .values()
            .any(|p| p.folder.id() == folder_id && p.name == name);
        let has_deferred = conn
            .deferred_blocks
            .iter()
            .any(|d| d.block.folder.id() == folder_id && d.block.name == name);
        !has_pending && !has_deferred
    };

    if should_complete {
        let committed = ctx
            .retry("complete_file", || folder.complete_file(name, version))
            .await?;
        if committed.is_some() {
            tracing::info!(folder = %folder.id(), file = %name, "file transfer complete, promoted to index");
        }
        return Ok(committed);
    }
    Ok(None)
}

/// Submit a single block `Request` to the peer, or defer it if the pipeline
/// is full.
///
/// Happy path: allocates a request ID, registers the request, and
/// writes the `Request` message to the peer.
///
/// If `pending_requests` is at `max_pending_requests`, pushes a
/// `DeferredRequest` onto the back of `deferred_blocks` instead. The deferred
/// queue is drained from the front by `drain_deferred` as responses come in.
async fn submit_or_defer_block<S, W>(
    inner: &Arc<Mutex<ConnectionInner<S>>>,
    writer: &mut W,
    block: FileBlock<S::Folder>,
    size: i32,
    block_no: i32,
) -> Result<()>
where
    S: Storage,
    W: AsyncWrite + Unpin,
{
    let request = {
        let mut conn = inner.lock();

        if conn.pending_requests.len() >= conn.max_pending_requests {
            tracing::debug!(
                folder = %block.folder.id(),
                file = %block.name,
                block_no,
                max = conn.max_pending_requests,
                "max pending requests reached, deferring block"
            );
            conn.deferred_blocks.push_back(DeferredRequest {
                block,
                size,
                block_no,
            });
            return Ok(());
        }

        let id = conn.next_request_id();
        let req = Request {
            id,
            folder: block.folder.id().to_string(),
            name: block.name.clone(),
            offset: block.offset,
            size,
            hash: block.hash.clone(),
            from_temporary: false,
            block_no,
        };
        conn.pending_requests.insert(id, block);
        req
    };

    send_typed_message(writer, MessageType::Request, &request).await
}

async fn drain_deferred<S: Storage, W: AsyncWrite + Unpin>(
    inner: &Arc<Mutex<ConnectionInner<S>>>,
    writer: &mut W,
    ctx: &ConnectionContext<'_>,
) -> Result<()> {
    loop {
        let deferred = {
            let mut conn = inner.lock();
            if conn.pending_requests.len() >= conn.max_pending_requests {
                break;
            }
            match conn.deferred_blocks.pop_front() {
                Some(d) => d,
                None => break,
            }
        };

        // Skip if we already have this block now (may have arrived via
        // another path).
        let reused = ctx
            .retry("reuse_block", || {
                deferred.block.folder.reuse_block(
                    &deferred.block.name,
                    deferred.block.offset,
                    &deferred.block.hash,
                    deferred.size,
                )
            })
            .await?;

        if reused {
            tracing::debug!(
                folder = %deferred.block.folder.id(),
                file = %deferred.block.name,
                block_no = deferred.block_no,
                "deferred block already present, skipping"
            );
            complete_and_notify(
                inner,
                &deferred.block.folder,
                &deferred.block.name,
                deferred.block.version.as_ref(),
                writer,
                ctx,
            )
            .await?;
            continue;
        }

        // Capacity may have refilled during the await; if so,
        // submit_or_defer_block will push this entry back to the deferred
        // queue and we'll keep looping until either capacity runs out at the
        // top of the loop or the queue is empty.
        submit_or_defer_block(
            inner,
            writer,
            deferred.block,
            deferred.size,
            deferred.block_no,
        )
        .await?;
    }
    Ok(())
}

#[tracing::instrument(level = "debug", skip(inner, writer, folder, ctx), fields(file = %file.name), err)]
async fn request_blocks<S: Storage, W: AsyncWrite + Unpin>(
    inner: &Arc<Mutex<ConnectionInner<S>>>,
    writer: &mut W,
    folder: &S::Folder,
    file: &FileInfo,
    ctx: &ConnectionContext<'_>,
) -> Result<()> {
    tracing::debug!(file = %file.name, total_blocks = file.blocks.len(), "requesting blocks");
    for (i, block) in file.blocks.iter().enumerate() {
        // Skip if we already have this block (e.g. from a rename/move).
        let reused = ctx
            .retry("reuse_block", || {
                folder.reuse_block(&file.name, block.offset, &block.hash, block.size)
            })
            .await?;
        tracing::debug!(file = %file.name, block_no = i, reused, "reuse_block");
        if reused {
            tracing::debug!(
                folder = %folder.id(), file = %file.name, block_no = i,
                "block already present, skipping request"
            );
            continue;
        }

        let block_no: i32 = i
            .try_into()
            .map_err(|_| BepError::Internal("block index exceeds i32".to_string()))?;

        let file_block = FileBlock {
            folder: folder.clone(),
            name: file.name.clone(),
            offset: block.offset,
            hash: block.hash.clone(),
            version: file.version.clone(),
        };

        submit_or_defer_block(inner, writer, file_block, block.size, block_no).await?;
    }
    Ok(())
}

async fn send_typed_message<W: AsyncWrite + Unpin, M: Message>(
    writer: &mut W,
    msg_type: MessageType,
    msg: &M,
) -> Result<()> {
    let header = Header {
        r#type: msg_type as i32,
        compression: MessageCompression::None as i32,
    };
    let body = msg.encode_to_vec();
    framing::write_message(writer, &header, &body, false).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn close_reason_debug() {
        let r = CloseReason::Local;
        assert!(format!("{r:?}").contains("Local"));
    }
}
