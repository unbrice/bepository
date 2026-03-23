// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Counter-based fault injection for storage and network layers (test-utils only).
//!
//! Provides decorator wrappers that intercept calls before delegating to the
//! real implementation. Configure "fail the next N calls to method X with error E",
//! then let the (N+1)th call succeed.
//!
//! - [`FaultStorage`] / [`FaultFolder`] — wraps any [`Storage`] impl (Layer 1)
//! - [`FaultStream`] — wraps any `AsyncRead + AsyncWrite` stream (Layer 2)

use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::Stream;
use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::device_id::DeviceId;
use crate::error::StorageError;
use crate::ids::{FolderId, FolderLabel, FolderLabelRef};
use crate::proto::bep::{FileInfo, Vector};
use crate::storage::{Sequence, Storage, StorageFolder, StorageInspectorForTests, UpdateResult};

// ---------------------------------------------------------------------------
// Shared counter machinery
// ---------------------------------------------------------------------------

/// Counter-based fault state shared by all config types.
///
/// Stores at most one `(remaining, error)` pair per method key.
/// `check()` decrements the counter and returns the error while remaining > 0;
/// removes the rule when it reaches zero.
struct FaultState<M: Eq + std::hash::Hash, E: Clone> {
    rules: Mutex<HashMap<M, (u32, E)>>,
}

impl<M: Eq + std::hash::Hash, E: Clone> FaultState<M, E> {
    fn new() -> Self {
        Self {
            rules: Mutex::new(HashMap::new()),
        }
    }

    fn set(&self, method: M, count: u32, error: E) {
        self.rules.lock().insert(method, (count, error));
    }

    fn clear(&self, method: M) {
        self.rules.lock().remove(&method);
    }

    fn check(&self, method: M) -> Result<(), E> {
        let mut rules = self.rules.lock();
        if let Some((count, error)) = rules.get_mut(&method) {
            let err = error.clone();
            *count -= 1;
            if *count == 0 {
                rules.remove(&method);
            }
            return Err(err);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Layer 1: Storage / StorageFolder fault injection
// ---------------------------------------------------------------------------

/// Methods on the `Storage` / `StorageFolder` traits that can have faults injected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageMethod {
    Folder,
    Index,
    ApplyUpdate,
    ReadBlock,
    StoreBlock,
    CompleteFile,
    ReuseBlock,
    HasBlock,
    ResolveConflict,
    LocalSequence,
    RemoteState,
    SetRemoteState,
}

/// Shared fault configuration for [`FaultStorage`] and all [`FaultFolder`] handles.
///
/// Cheaply cloneable — all clones share the same rule set via `Arc`.
/// Configure faults via [`set`](Self::set) before the engine makes calls.
#[derive(Clone)]
pub struct FaultConfig {
    inner: Arc<FaultState<StorageMethod, StorageError>>,
}

impl FaultConfig {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(FaultState::new()),
        }
    }

    /// Fail the next `count` calls to `method` with `error`.
    pub fn set(&self, method: StorageMethod, count: u32, error: StorageError) {
        self.inner.set(method, count, error);
    }

    /// Clear any pending fault for `method`.
    pub fn clear(&self, method: StorageMethod) {
        self.inner.clear(method);
    }

    fn check(&self, method: StorageMethod) -> Result<(), StorageError> {
        self.inner.check(method)
    }
}

impl Default for FaultConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Wraps any [`Storage`] impl to enable fault injection.
///
/// Use [`FaultStorage::new`] to obtain the storage and its shared [`FaultConfig`].
pub struct FaultStorage<S> {
    inner: S,
    config: FaultConfig,
}

impl<S> FaultStorage<S> {
    /// Create a fault-injectable wrapper around `inner`.
    ///
    /// Returns `(FaultStorage, FaultConfig)`. The config is shared with all
    /// folder handles produced from this storage instance.
    pub fn new(inner: S) -> (Self, FaultConfig) {
        let config = FaultConfig::new();
        let storage = Self {
            inner,
            config: config.clone(),
        };
        (storage, config)
    }
}

#[async_trait]
impl<S: Storage> Storage for FaultStorage<S> {
    type Folder = FaultFolder<S::Folder>;
    type StorageKey = S::StorageKey;

    async fn folder(&self, id: FolderId) -> Result<FaultFolder<S::Folder>, StorageError> {
        self.config.check(StorageMethod::Folder)?;
        let inner = self.inner.folder(id).await?;
        Ok(FaultFolder {
            inner,
            faults: self.config.clone(),
        })
    }

    async fn list_folders(
        &self,
    ) -> Result<Vec<(FolderId, FolderLabel, S::StorageKey)>, StorageError> {
        self.inner.list_folders().await
    }

    async fn ensure_folders(
        &self,
        folders: &[(FolderId, FolderLabel)],
    ) -> Result<Vec<bool>, StorageError> {
        self.inner.ensure_folders(folders).await
    }
}

/// Per-folder handle produced by [`FaultStorage`].
///
/// Cheaply cloneable (shares `Arc` state from [`FaultConfig`] and the inner folder).
#[derive(Clone)]
pub struct FaultFolder<F> {
    pub inner: F,
    faults: FaultConfig,
}

#[async_trait]
impl<F: StorageFolder> StorageFolder for FaultFolder<F> {
    fn id(&self) -> FolderId {
        self.inner.id()
    }

    fn label(&self) -> &FolderLabelRef {
        self.inner.label()
    }

    async fn index(
        &self,
        since: Sequence,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<FileInfo, StorageError>> + Send>>, StorageError>
    {
        self.faults.check(StorageMethod::Index)?;
        self.inner.index(since).await
    }

    async fn read_block(
        &self,
        name: &str,
        offset: i64,
        size: i32,
        hash: &[u8],
    ) -> Result<Bytes, StorageError> {
        self.faults.check(StorageMethod::ReadBlock)?;
        self.inner.read_block(name, offset, size, hash).await
    }

    async fn apply_update(
        &self,
        file: &FileInfo,
        remote_device: &DeviceId,
    ) -> Result<UpdateResult, StorageError> {
        self.faults.check(StorageMethod::ApplyUpdate)?;
        self.inner.apply_update(file, remote_device).await
    }

    async fn local_sequence(&self) -> Result<Sequence, StorageError> {
        self.faults.check(StorageMethod::LocalSequence)?;
        self.inner.local_sequence().await
    }

    async fn remote_state(
        &self,
        device: &DeviceId,
    ) -> Result<crate::storage::RemoteIndexState, StorageError> {
        self.faults.check(StorageMethod::RemoteState)?;
        self.inner.remote_state(device).await
    }

    async fn set_remote_state(
        &self,
        device: &DeviceId,
        state: crate::storage::RemoteIndexState,
    ) -> Result<(), StorageError> {
        self.faults.check(StorageMethod::SetRemoteState)?;
        self.inner.set_remote_state(device, state).await
    }

    async fn resolve_conflict(
        &self,
        winner: &FileInfo,
        loser: &FileInfo,
        loser_path: Option<&str>,
    ) -> Result<(), StorageError> {
        self.faults.check(StorageMethod::ResolveConflict)?;
        self.inner.resolve_conflict(winner, loser, loser_path).await
    }

    async fn store_block(
        &self,
        name: &str,
        offset: i64,
        hash: &[u8],
        data: Bytes,
    ) -> Result<(), StorageError> {
        self.faults.check(StorageMethod::StoreBlock)?;
        self.inner.store_block(name, offset, hash, data).await
    }

    async fn complete_file(
        &self,
        name: &str,
        expected_version: Option<&Vector>,
    ) -> Result<Option<FileInfo>, StorageError> {
        self.faults.check(StorageMethod::CompleteFile)?;
        self.inner.complete_file(name, expected_version).await
    }

    async fn reuse_block(
        &self,
        name: &str,
        offset: i64,
        hash: &[u8],
        size: i32,
    ) -> Result<bool, StorageError> {
        self.faults.check(StorageMethod::ReuseBlock)?;
        self.inner.reuse_block(name, offset, hash, size).await
    }

    async fn has_block(
        &self,
        file: &str,
        offset: i64,
        hash: &[u8],
        size: i32,
    ) -> Result<bool, StorageError> {
        self.faults.check(StorageMethod::HasBlock)?;
        self.inner.has_block(file, offset, hash, size).await
    }
}

#[async_trait]
impl<F> StorageInspectorForTests for FaultFolder<F>
where
    F: StorageFolder + StorageInspectorForTests,
    F::Epoch: Send,
{
    type Epoch = F::Epoch;

    async fn insert_file(&self, file: FileInfo) {
        self.inner.insert_file(file).await
    }

    async fn insert_block(&self, name: &str, offset: i64, data: Bytes) {
        self.inner.insert_block(name, offset, data).await
    }

    async fn get_file(&self, name: &str) -> Option<FileInfo> {
        self.inner.get_file(name).await
    }

    async fn get_inbox_file(&self, epoch: F::Epoch, name: &str) -> Option<FileInfo> {
        self.inner.get_inbox_file(epoch, name).await
    }

    async fn get_block(&self, name: &str, offset: i64) -> Option<Bytes> {
        self.inner.get_block(name, offset).await
    }
}

// ---------------------------------------------------------------------------
// Layer 2: Network / stream fault injection
// ---------------------------------------------------------------------------

/// Methods on the network stream that can have faults injected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamMethod {
    Read,
    Write,
}

/// Shared fault configuration for [`FaultStream`].
#[derive(Clone)]
pub struct StreamFaultConfig {
    inner: Arc<FaultState<StreamMethod, io::ErrorKind>>,
}

impl StreamFaultConfig {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(FaultState::new()),
        }
    }

    /// Fail the next `count` polls of `method` with an error of `kind`.
    ///
    /// To simulate a permanent connection drop: `set(Read, u32::MAX, ConnectionReset)`.
    pub fn set(&self, method: StreamMethod, count: u32, kind: io::ErrorKind) {
        self.inner.set(method, count, kind);
    }

    /// Clear any pending fault for `method`.
    pub fn clear(&self, method: StreamMethod) {
        self.inner.clear(method);
    }

    fn check(&self, method: StreamMethod) -> Result<(), io::Error> {
        self.inner.check(method).map_err(io::Error::from)
    }
}

impl Default for StreamFaultConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Wraps any `AsyncRead + AsyncWrite` stream to enable fault injection.
///
/// Faults fire at the `poll_read` / `poll_write` level (not per BEP message).
/// Setting `count = u32::MAX` simulates a permanent connection drop.
pub struct FaultStream<T> {
    pub inner: T,
    faults: StreamFaultConfig,
}

impl<T> FaultStream<T> {
    /// Wrap `inner` in a fault-injectable stream.
    ///
    /// Returns `(FaultStream, StreamFaultConfig)`. The config controls which
    /// polls fail and with what error.
    pub fn new(inner: T) -> (Self, StreamFaultConfig) {
        let faults = StreamFaultConfig::new();
        let stream = Self {
            inner,
            faults: faults.clone(),
        };
        (stream, faults)
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for FaultStream<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Err(e) = this.faults.check(StreamMethod::Read) {
            return Poll::Ready(Err(e));
        }
        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for FaultStream<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if let Err(e) = this.faults.check(StreamMethod::Write) {
            return Poll::Ready(Err(e));
        }
        Pin::new(&mut this.inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}
