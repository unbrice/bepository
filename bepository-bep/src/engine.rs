// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot};
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
    connections: Arc<RwLock<HashMap<DeviceId, ConnectionEntry>>>,
    event_tx: EventSender,
    event_rx: Option<EventReceiver>,
    /// True once take_event_receiver() has been called.
    has_event_listener: bool,
}

struct ConnectionEntry {
    shutdown: CancellationToken,
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
        Self {
            storage: Arc::new(storage),
            resolver,
            device_id,
            device_name,
            shared_folders,
            connection_options: ConnectionOptions::default(),
            connections: Arc::new(RwLock::new(HashMap::new())),
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
    /// all devices are accepted automatically.
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
    /// Sends the shutdown signal to every tracked connection and clears the
    /// connection map. Connections close asynchronously — this method returns
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
            let _ = self
                .event_tx
                .send(EngineEvent::DeviceConnecting {
                    device: remote_device,
                    respond: accept_tx,
                })
                .await;

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

        // Track this connection.
        {
            let mut conns = connections.write();
            conns.insert(
                remote_device,
                ConnectionEntry {
                    shutdown: shutdown.clone(),
                },
            );
        }

        // Spawn the connection task.
        let conn_shutdown = shutdown.clone();
        let connections_cleanup = connections.clone();
        let rd_cleanup = rd;
        tokio::spawn(
            async move {
                connection::run_connection(
                    storage,
                    resolver,
                    local_device,
                    remote_device,
                    device_name,
                    shared_folders,
                    options,
                    stream,
                    close_tx,
                    conn_shutdown,
                )
                .await;

                connections_cleanup.write().remove(&rd_cleanup);
            }
            .in_current_span(),
        );

        // Spawn a task to emit the disconnect event when the connection closes.
        let event_tx = self.event_tx.clone();
        let rd_disconnect = rd;
        let (close_tx_user, close_rx_user) = oneshot::channel();
        tokio::spawn(
            async move {
                match close_rx.await {
                    Ok(reason) => {
                        let event_reason = reason.clone();
                        let _ = event_tx
                            .send(EngineEvent::DeviceDisconnected {
                                device: rd_disconnect,
                                reason: event_reason,
                            })
                            .await;
                        let _ = close_tx_user.send(reason);
                    }
                    Err(_) => {
                        let _ = event_tx
                            .send(EngineEvent::DeviceDisconnected {
                                device: rd_disconnect,
                                reason: CloseReason::Error(BepError::NetworkError(
                                    "connection task dropped".into(),
                                )),
                            })
                            .await;
                    }
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
    use crate::test_utils::{BackupResolver, LOCAL_DEV, MemoryStorage};

    #[tokio::test]
    async fn engine_creation() {
        let storage = MemoryStorage::new();
        let engine = BepEngine::new(
            storage,
            LOCAL_DEV,
            "test".into(),
            vec!["folder1".into()],
            Arc::new(BackupResolver),
        );
        assert!(engine.peers().is_empty());
    }
}
