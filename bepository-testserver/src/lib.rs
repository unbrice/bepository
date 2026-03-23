// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Reference test server wiring `bepository-tls` and `bepository-bep`.
//!
//! Provides a minimal Syncthing-compatible node for interop testing.
//! Generic over the storage backend.

use std::collections::HashSet;
use std::sync::Arc;

use bepository_bep::ids::FolderId;
use bepository_bep::test_utils::{BackupResolver, MemoryStorage};
use bepository_bep::{BepEngine, ConflictResolver, DeviceId, EngineEvent, EventReceiver, Storage};
use bepository_tls::{BepStream, Identity};
use tokio::sync::mpsc;

/// A minimal Syncthing-compatible node for testing.
///
/// Wraps a [`BepEngine`] with an [`Identity`] and an event handler that
/// accepts only devices in the allow set. Generic over the storage backend.
pub struct TestServer<S: Storage> {
    identity: Identity,
    engine: BepEngine<S>,
    storage: S,
    allow_tx: mpsc::UnboundedSender<DeviceId>,
}

impl TestServer<MemoryStorage> {
    /// Create a test server with a fresh identity, using in-memory storage.
    #[must_use]
    pub fn new(shared_folders: Vec<FolderId>) -> Self {
        let identity = Identity::generate().expect("cert generation");
        Self::with_identity(identity, shared_folders)
    }

    /// Create a test server with a specific identity, using in-memory storage.
    pub fn with_identity(identity: Identity, shared_folders: Vec<FolderId>) -> Self {
        let storage = MemoryStorage::new();
        Self::with_storage(identity, storage, shared_folders, Arc::new(BackupResolver))
    }
}

impl<S: Storage + Clone> TestServer<S> {
    /// Create a test server with an explicit storage backend and conflict resolver.
    pub fn with_storage(
        identity: Identity,
        storage: S,
        shared_folders: Vec<FolderId>,
        resolver: Arc<dyn ConflictResolver>,
    ) -> Self {
        let device_id = *identity.device_id();
        let mut engine = BepEngine::new(
            storage.clone(),
            device_id,
            "bepository-testserver".into(),
            shared_folders,
            resolver,
        );

        let events = engine
            .take_event_receiver()
            .expect("BUG: take_event_receiver called more than once");
        let (allow_tx, allow_rx) = mpsc::unbounded_channel();

        tokio::spawn(run_event_handler(events, allow_rx));

        Self {
            identity,
            engine,
            storage,
            allow_tx,
        }
    }

    /// Allow a device to connect.
    pub fn allow_device(&self, device: DeviceId) {
        self.allow_tx.send(device).expect("event handler alive");
    }

    /// The server's device ID.
    pub fn device_id(&self) -> &DeviceId {
        self.identity.device_id()
    }

    /// The server's TLS identity.
    pub fn identity(&self) -> &Identity {
        &self.identity
    }

    /// The underlying storage backend (for test assertions).
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Connect to a remote peer via TLS.
    pub async fn connect_to(
        &self,
        addr: &str,
    ) -> Result<bepository_bep::ConnectionHandle, Box<dyn std::error::Error>> {
        let bep_stream = bepository_tls::connect(addr, &self.identity).await?;
        let handle = self
            .engine
            .connect(bep_stream.stream, bep_stream.peer_device_id)
            .await?;
        Ok(handle)
    }

    /// Accept a connection from a `BepStream` (already TLS-established).
    pub async fn accept_stream<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + 'static>(
        &self,
        stream: BepStream<T>,
    ) -> Result<bepository_bep::ConnectionHandle, bepository_bep::BepError> {
        self.engine
            .accept(stream.stream, stream.peer_device_id)
            .await
    }

    /// Currently connected peers.
    pub fn peers(&self) -> Vec<DeviceId> {
        self.engine.peers()
    }
}

async fn run_event_handler(
    mut events: EventReceiver,
    mut allow_rx: mpsc::UnboundedReceiver<DeviceId>,
) {
    let mut allowed: HashSet<DeviceId> = HashSet::new();

    loop {
        tokio::select! {
            Some(device) = allow_rx.recv() => {
                allowed.insert(device);
            }
            Some(evt) = events.recv() => {
                match evt {
                    EngineEvent::DeviceConnecting { device, respond } => {
                        let accepted = allowed.contains(&device);
                        tracing::info!(%device, accepted, "device connecting");
                        let _ = respond.send(accepted);
                    }
                    EngineEvent::DeviceDisconnected { device, reason } => {
                        tracing::info!(%device, ?reason, "device disconnected");
                    }
                }
            }
            else => break,
        }
    }
}
