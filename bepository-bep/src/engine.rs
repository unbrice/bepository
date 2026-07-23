// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::conflict::ConflictResolver;
use crate::connection::{self, CloseReason, ConnectionHandle, ConnectionOptions};
use crate::device_id::DeviceId;
use crate::error::{BepError, Result};
use crate::events::{EngineEvent, EventReceiver, EventSender};
use crate::ids::FolderId;
use crate::storage::Storage;

/// Default event channel capacity.
const EVENT_CHANNEL_CAPACITY: usize = 64;

/// How long to wait for the event handler to accept/reject a device.
const ACCEPT_TIMEOUT: Duration = Duration::from_secs(30);

/// Max queued connection-spawn requests. Steady-state depth is ~1 (the
/// supervisor drains straight into its JoinSet); the bound exists so a wedged
/// supervisor fails new connections fast instead of growing a queue.
const SPAWN_QUEUE_CAPACITY: usize = 8;

/// Coordinator that manages multiple BEP connections.
///
/// Generic over the storage backend. Callers provide already-established
/// byte streams (e.g. TLS connections); the engine handles protocol logic.
///
/// Use [`take_event_receiver`](Self::take_event_receiver) to get notified of
/// connection lifecycle events. If no receiver is taken, all connections are
/// accepted by default.
pub struct BepEngine<S: Storage> {
    storage: Arc<S>,
    resolver: Arc<dyn ConflictResolver>,
    device_id: DeviceId,
    device_name: String,
    shared_folders: Vec<FolderId>,
    connection_options: ConnectionOptions,
    connections: Arc<RwLock<HashMap<DeviceId, Arc<ConnectionEntry>>>>,
    spawn_tx: mpsc::Sender<PendingConnection>,
    event_tx: EventSender,
    event_rx: Option<EventReceiver>,
    /// True once take_event_receiver() has been called.
    has_event_listener: bool,
}

struct ConnectionEntry {
    shutdown: CancellationToken,
}

/// A connection handed to the supervisor for spawning and reaping.
struct PendingConnection {
    task: Pin<Box<dyn Future<Output = ()> + Send>>,
    device: DeviceId,
    entry: Arc<ConnectionEntry>,
}

/// Sole spawner and reaper of connection tasks: the only place where
/// connection-map entries are removed. Exits once the engine is dropped
/// (spawn channel closed) and every connection task has finished.
async fn run_supervisor(
    mut spawn_rx: mpsc::Receiver<PendingConnection>,
    connections: Arc<RwLock<HashMap<DeviceId, Arc<ConnectionEntry>>>>,
) {
    let mut join_set: JoinSet<(DeviceId, Arc<ConnectionEntry>)> = JoinSet::new();
    // Task id → registration, so panicked tasks (which produce no output) can
    // still have their map entry reaped.
    let mut by_id: HashMap<tokio::task::Id, (DeviceId, Arc<ConnectionEntry>)> = HashMap::new();

    loop {
        tokio::select! {
            req = spawn_rx.recv() => {
                match req {
                    Some(req) => {
                        let reg = (req.device, req.entry.clone());
                        let handle = join_set.spawn(async move {
                            (req.task).await;
                            (req.device, req.entry)
                        });
                        by_id.insert(handle.id(), reg);
                    }
                    None if join_set.is_empty() => break, // engine dropped
                    None => {} // keep reaping until the JoinSet drains
                }
            }
            done = join_set.join_next_with_id(), if !join_set.is_empty() => {
                let (device, entry) = match done {
                    Some(Ok((id, output))) => {
                        by_id.remove(&id);
                        output
                    }
                    Some(Err(e)) => {
                        let Some(reg) = by_id.remove(&e.id()) else { continue };
                        tracing::error!(remote_device = %reg.0, error = %e, "connection task panicked");
                        reg
                    }
                    None => continue, // unreachable: guarded by is_empty above
                };
                // Remove only if the entry is still this connection — a newer
                // connection for the same device must not be unregistered.
                let mut conns = connections.write();
                if conns.get(&device).is_some_and(|e| Arc::ptr_eq(e, &entry)) {
                    conns.remove(&device);
                }
            }
        }
    }
}

impl<S: Storage> BepEngine<S> {
    /// Create a new engine.
    ///
    /// `resolver` determines how concurrent edits (conflicts) are handled.
    /// For tests, use [`test_utils::BackupResolver`](crate::test_utils::BackupResolver).
    pub fn new(
        storage: S,
        device_id: DeviceId,
        device_name: String,
        shared_folders: Vec<FolderId>,
        resolver: Arc<dyn ConflictResolver>,
    ) -> Self {
        let (event_tx, event_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        let connections = Arc::new(RwLock::new(HashMap::new()));
        let (spawn_tx, spawn_rx) = mpsc::channel(SPAWN_QUEUE_CAPACITY);
        // The JoinHandle is dropped: dropping a handle never kills or leaks a
        // task — the supervisor runs until every `spawn_tx` sender is dropped
        // (i.e. the engine is gone) and its JoinSet has drained, then exits.
        tokio::spawn(run_supervisor(spawn_rx, connections.clone()));
        Self {
            storage: Arc::new(storage),
            resolver,
            device_id,
            device_name,
            shared_folders,
            connection_options: ConnectionOptions::default(),
            connections,
            spawn_tx,
            event_tx,
            event_rx: Some(event_rx),
            has_event_listener: false,
        }
    }

    /// Override connection options (e.g. max pending requests).
    pub fn set_connection_options(&mut self, options: ConnectionOptions) {
        self.connection_options = options;
    }

    /// Take the event receiver. Can only be called once; returns `None` after.
    ///
    /// The receiver yields [`EngineEvent`]s including device acceptance
    /// requests and disconnection notifications. If no receiver is taken,
    /// all devices are accepted automatically and no events are emitted.
    /// Events are best-effort: a full channel rejects new connections and
    /// drops disconnect notifications rather than blocking.
    pub fn take_event_receiver(&mut self) -> Option<EventReceiver> {
        self.event_rx
            .take()
            .inspect(|_| self.has_event_listener = true)
    }

    /// Run BEP over an outgoing stream. Spawns a background task.
    ///
    /// `remote_device` is the peer's device ID, typically extracted from
    /// their TLS certificate. The event handler (if any) will be asked to
    /// accept or reject the connection.
    pub async fn connect<T: AsyncRead + AsyncWrite + Send + 'static>(
        &self,
        stream: T,
        remote_device: DeviceId,
    ) -> Result<ConnectionHandle> {
        self.start_connection(stream, remote_device).await
    }

    /// Run BEP over an incoming stream. Spawns a background task.
    ///
    /// `remote_device` is the peer's device ID, typically extracted from
    /// their TLS certificate. The event handler (if any) will be asked to
    /// accept or reject the connection.
    pub async fn accept<T: AsyncRead + AsyncWrite + Send + 'static>(
        &self,
        stream: T,
        remote_device: DeviceId,
    ) -> Result<ConnectionHandle> {
        self.start_connection(stream, remote_device).await
    }

    /// Currently connected peers.
    #[must_use]
    pub fn peers(&self) -> Vec<DeviceId> {
        self.connections.read().keys().cloned().collect()
    }

    /// Shut down all active connections.
    ///
    /// Sends the shutdown signal to every tracked connection. Entries are removed
    /// later by per-task cleanup. Connections close asynchronously — this method returns
    /// as soon as the signals are sent.
    pub fn shutdown_all(&self) {
        let conns = self.connections.read();
        let n = conns.len();
        for entry in conns.values() {
            entry.shutdown.cancel();
        }
        tracing::info!(count = n, "shutdown signalled to connections");
    }

    async fn start_connection<T: AsyncRead + AsyncWrite + Send + 'static>(
        &self,
        stream: T,
        remote_device: DeviceId,
    ) -> Result<ConnectionHandle> {
        // Ask the event handler for permission (if one is listening).
        if self.has_event_listener {
            let (accept_tx, accept_rx) = oneshot::channel();
            // Non-blocking: a full channel means the listener is stalled.
            // Reject immediately — a blocking send would wedge every new
            // connection behind the stalled listener (ACCEPT_TIMEOUT only
            // covers the reply wait, which starts after the send).
            if let Err(e) = self.event_tx.try_send(EngineEvent::DeviceConnecting {
                device: remote_device,
                respond: accept_tx,
            }) {
                tracing::warn!(remote_device = %remote_device, error = %e, "event channel unavailable, rejecting connection");
                return Err(BepError::DeviceRejected);
            }

            let accepted = tokio::time::timeout(ACCEPT_TIMEOUT, accept_rx)
                .await
                .unwrap_or(Ok(false)) // timeout → reject
                .unwrap_or(false); // channel dropped → reject

            if !accepted {
                return Err(BepError::DeviceRejected);
            }
        }

        let (close_tx, close_rx) = oneshot::channel();
        let shutdown = CancellationToken::new();

        let storage = self.storage.clone();
        let resolver = self.resolver.clone();
        let local_device = self.device_id;
        let device_name = self.device_name.clone();
        let shared_folders = self.shared_folders.clone();
        let options = self.connection_options.clone();
        let connections = self.connections.clone();
        let rd = remote_device;

        // Track this connection. A duplicate DeviceId replaces the old entry and
        // cancels its shutdown token so the displaced connection terminates.
        let entry = Arc::new(ConnectionEntry {
            shutdown: shutdown.clone(),
        });
        {
            let mut conns = connections.write();
            if let Some(old) = conns.insert(remote_device, entry.clone()) {
                tracing::warn!(remote_device = %rd, "duplicate connection, replacing existing");
                old.shutdown.cancel();
            }
        }

        // Hand the connection to the supervisor — the sole spawner and reaper
        // of connection tasks.
        let conn_shutdown = shutdown.clone();
        let task = Box::pin(
            async move {
                connection::run_connection(
                    storage,
                    resolver,
                    local_device,
                    rd,
                    device_name,
                    shared_folders,
                    options,
                    stream,
                    close_tx,
                    conn_shutdown,
                )
                .await;
            }
            .in_current_span(),
        );
        if let Err(e) = self.spawn_tx.try_send(PendingConnection {
            task,
            device: rd,
            entry: entry.clone(),
        }) {
            // Supervisor wedged (Full) or dead (Closed): undo our registration
            // and fail the connection — the remote reconnects on its normal
            // backoff.
            tracing::warn!(remote_device = %rd, error = ?e, "connection spawn rejected");
            let mut conns = connections.write();
            if conns.get(&rd).is_some_and(|e| Arc::ptr_eq(e, &entry)) {
                conns.remove(&rd);
            }
            return Err(BepError::Internal(
                "connection supervisor unavailable".into(),
            ));
        }

        // Spawn a task to forward the close to the user handle and emit the
        // disconnect event. The event is best-effort: skipped entirely when no
        // listener was ever taken, dropped (not blocked on) when the channel
        // is full — a stalled listener must not wedge this detached task.
        let event_tx = self.has_event_listener.then(|| self.event_tx.clone());
        let rd_disconnect = rd;
        let (close_tx_user, close_rx_user) = oneshot::channel();
        tokio::spawn(
            async move {
                let (reason, user_reason) = match close_rx.await {
                    Ok(reason) => (reason.clone(), Some(reason)),
                    Err(_) => (
                        CloseReason::Error(BepError::NetworkError(
                            "connection task dropped".into(),
                        )),
                        None,
                    ),
                };
                if let Some(event_tx) = event_tx
                    && let Err(e) = event_tx.try_send(EngineEvent::DeviceDisconnected {
                        device: rd_disconnect,
                        reason,
                    })
                {
                    tracing::warn!(remote_device = %rd_disconnect, error = %e, "disconnect event dropped");
                }
                if let Some(user_reason) = user_reason {
                    let _ = close_tx_user.send(user_reason);
                }
            }
            .in_current_span(),
        );

        tracing::info!(remote_device = %rd, "connection started");

        Ok(ConnectionHandle {
            device_id: rd,
            closed: close_rx_user,
            shutdown,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{BackupResolver, LOCAL_DEV, MemoryStorage, REMOTE_DEV};

    fn test_engine() -> BepEngine<MemoryStorage> {
        BepEngine::new(
            MemoryStorage::new(),
            LOCAL_DEV,
            "test".into(),
            vec!["folder1".into()],
            Arc::new(BackupResolver),
        )
    }

    /// Reaping goes through the supervisor task, so map removal trails task
    /// exit by a scheduling hop; poll until the supervisor has caught up.
    /// Bounded by wall-clock time so a broken supervisor fails the test
    /// instead of hanging it (neither the stock harness nor `#[tokio::test]`
    /// has a per-test timeout).
    async fn wait_until(what: &str, timeout: Duration, cond: impl Fn() -> bool) {
        tokio::time::timeout(timeout, async {
            while !cond() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
    }

    #[tokio::test]
    async fn engine_creation() {
        let engine = test_engine();
        assert!(engine.peers().is_empty());
    }

    #[tokio::test]
    async fn closed_connection_is_untracked() {
        let engine = test_engine();
        let (stream, _peer) = tokio::io::duplex(1024);
        let h = engine.connect(stream, REMOTE_DEV).await.unwrap();
        assert_eq!(engine.peers(), vec![REMOTE_DEV]);

        h.shutdown.cancel();
        let _ = h.closed.await;
        wait_until("peer to be untracked", Duration::from_secs(5), || {
            engine.peers().is_empty()
        })
        .await;
    }

    #[tokio::test]
    async fn duplicate_device_connection_cancels_old_and_keeps_new_tracked() {
        let engine = test_engine();

        let (stream1, _peer1) = tokio::io::duplex(1024);
        let h1 = engine.connect(stream1, REMOTE_DEV).await.unwrap();
        let (stream2, _peer2) = tokio::io::duplex(1024);
        let h2 = engine.connect(stream2, REMOTE_DEV).await.unwrap();

        // The displaced connection is cancelled; the new one stays tracked.
        assert!(h1.shutdown.is_cancelled());
        assert!(!h2.shutdown.is_cancelled());
        let _ = h1.closed.await;
        // Reaping the old connection must not remove the new one's entry.
        wait_until(
            "new connection to stay tracked",
            Duration::from_secs(5),
            || engine.peers() == vec![REMOTE_DEV],
        )
        .await;

        h2.shutdown.cancel();
        let _ = h2.closed.await;
        wait_until("peer to be untracked", Duration::from_secs(5), || {
            engine.peers().is_empty()
        })
        .await;
    }

    /// Fill the engine's event channel with filler disconnect events.
    fn fill_event_channel<S: Storage>(engine: &BepEngine<S>) {
        for _ in 0..EVENT_CHANNEL_CAPACITY {
            engine
                .event_tx
                .try_send(EngineEvent::DeviceDisconnected {
                    device: REMOTE_DEV,
                    reason: CloseReason::Error(BepError::Internal("filler".into())),
                })
                .expect("channel should accept filler events");
        }
    }

    #[tokio::test]
    async fn stalled_event_listener_rejects_connections_fast() {
        let mut engine = test_engine();
        // Listener taken but never drained; channel full.
        let _events = engine.take_event_receiver().unwrap();
        fill_event_channel(&engine);

        let (stream, _peer) = tokio::io::duplex(1024);
        let result =
            tokio::time::timeout(Duration::from_secs(5), engine.connect(stream, REMOTE_DEV)).await;
        assert!(
            matches!(result, Ok(Err(BepError::DeviceRejected))),
            "expected fast DeviceRejected"
        );
    }

    #[tokio::test]
    async fn full_event_channel_without_listener_does_not_wedge_close() {
        // No listener ever taken: the receiver lives inside the engine, so the
        // channel stays open and fills up. Closing a connection must still
        // deliver `closed` to the user handle.
        let engine = test_engine();
        fill_event_channel(&engine);

        let (stream, _peer) = tokio::io::duplex(1024);
        let h = engine.connect(stream, REMOTE_DEV).await.unwrap();
        h.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(5), h.closed)
            .await
            .expect("close wedged behind a full event channel")
            .ok();
    }

    #[tokio::test]
    async fn stalled_listener_drops_disconnect_event_but_delivers_close() {
        let mut engine = test_engine();
        let mut events = engine.take_event_receiver().unwrap();
        let engine = Arc::new(engine);

        // Accept one connection normally.
        let (stream, _peer) = tokio::io::duplex(1024);
        let connectee = engine.clone();
        let connect = tokio::spawn(async move { connectee.connect(stream, REMOTE_DEV).await });
        match events.recv().await {
            Some(EngineEvent::DeviceConnecting { respond, .. }) => respond.send(true).unwrap(),
            other => panic!("expected DeviceConnecting, got {other:?}"),
        }
        let h = connect.await.unwrap().unwrap();

        // Stall the listener, then close: the disconnect event is dropped, but
        // the close notification must still reach the user handle.
        fill_event_channel(&engine);
        h.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(5), h.closed)
            .await
            .expect("close wedged behind stalled listener")
            .ok();
    }
}
