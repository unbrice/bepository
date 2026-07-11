// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

use thiserror::Error;

/// Categorised storage-layer error.
///
/// [`Storage`](crate::Storage) implementations return this type.
/// The BEP engine converts it to [`BepError`] when propagating errors
/// up through the protocol layer.
#[derive(Debug, Error, Clone)]
pub enum StorageError {
    /// Transient I/O failure that may succeed on retry.
    ///
    /// Includes object-store timeouts, network blips, and SlateDB
    /// unavailability (background task failure, WAL errors, etc.).
    #[error("transient I/O error: {0}")]
    TransientIo(String),

    /// Stored data is inconsistent or unreadable.
    ///
    /// Includes protobuf decode failures, checksum mismatches, and missing
    /// required fields in index entries.
    #[error("data corruption: {0}")]
    Corruption(String),

    /// On-disk store was written by a newer format version than this instance
    /// understands. Not corruption — the data is well-formed, the binary is
    /// just too old to honor it. The caller should surface a clear "upgrade
    /// this instance" message rather than treating it as decode failure.
    #[error(
        "store written by format version {found}, but this instance only supports version {supported} — upgrade this instance"
    )]
    UnsupportedVersion { found: u32, supported: u32 },

    /// Requested resource does not exist.
    ///
    /// Includes missing blocks, unregistered folders, and absent index entries.
    #[error("not found: {0}")]
    NotFound(String),

    /// This process is no longer the active writer.
    ///
    /// Returned when the storage epoch has not been activated (via `activate()`)
    /// or when SlateDB detects a newer client has taken over (fencing). The
    /// caller should stop writing and wait for re-activation.
    #[error("standby: {0}")]
    Standby(String),

    /// Input rejected before reaching storage.
    ///
    /// Includes empty file names, path traversal attempts, and incorrect hash
    /// sizes.
    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// Internal logic error not caused by storage corruption.
    ///
    /// Includes programming errors, assertion failures, and unexpected
    /// internal state that indicates a bug in the code.
    #[error("internal error: {0}")]
    Internal(String),
}

impl StorageError {
    /// Returns true if the error is temporary and the operation should be retried.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::TransientIo(_))
    }

    /// Returns true if the error indicates unrecoverable state requiring process shutdown.
    #[must_use]
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::Corruption(_) | Self::Internal(_) | Self::UnsupportedVersion { .. }
        )
    }
}

#[derive(Debug, Error, Clone)]
pub enum BepError {
    /// Transient I/O errors that may succeed on retry
    #[error("transient I/O error: {0}")]
    TransientIo(String),

    /// Data corruption or protocol violations from peer
    #[error("peer protocol violation: {0}")]
    PeerBadHello(String),

    #[error("peer sent bad message: {0}")]
    PeerBadMessage(String),

    #[error("peer error code {code} for {path}")]
    PeerError { code: i32, path: String },

    /// Data corruption or decode failures
    #[error("data corruption: {0}")]
    Corruption(String),

    /// On-disk store written by a newer format version than this binary supports.
    #[error(
        "store written by format version {found}, but this instance only supports version {supported} — upgrade this instance"
    )]
    UnsupportedVersion { found: u32, supported: u32 },

    /// Internal logic error not caused by storage corruption
    #[error("internal error: {0}")]
    Internal(String),

    /// This process is no longer the active writer
    #[error("standby: {0}")]
    Standby(String),

    /// Connection management errors
    #[error("peer closed connection: {0}")]
    PeerClosed(String),

    #[error("network error: {0}")]
    NetworkError(String),

    /// The writer task has exited; sends through MessageWriter no longer succeed.
    /// This is a proxy error — the writer's real exit reason (network I/O failure,
    /// panic, etc.) is reported separately as that task's own error. WriterClosed
    /// has a low priority in the WorkerError ranking — only `PeerClosed` (the
    /// clean peer-initiated close) ranks lower — so it is unlikely to mask a
    /// more informative error from a sibling task.
    #[error("writer task closed")]
    WriterClosed,

    #[error("device rejected by event handler")]
    DeviceRejected,
}

impl BepError {
    /// Returns true if the error is temporary and the operation should be retried.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::TransientIo(_) | Self::NetworkError(_))
    }

    /// Returns true if the error indicates unrecoverable state requiring process shutdown.
    #[must_use]
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::Corruption(_) | Self::Internal(_) | Self::UnsupportedVersion { .. }
        )
    }

    /// Returns true if the error was caused by the remote peer's protocol violation.
    #[must_use]
    pub fn is_peer_fault(&self) -> bool {
        matches!(
            self,
            Self::PeerBadHello(_) | Self::PeerBadMessage(_) | Self::PeerError { .. }
        )
    }
}

pub type Result<T> = std::result::Result<T, BepError>;
