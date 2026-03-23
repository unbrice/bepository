// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

use tokio::sync::{mpsc, oneshot};

use crate::{CloseReason, DeviceId};

/// Events emitted by the engine during the lifecycle of connections.
#[derive(Debug)]
pub enum EngineEvent {
    /// A device completed the TLS handshake and wants to connect.
    ///
    /// Send `true` on the channel to accept, `false` to reject.
    /// Dropping the sender rejects the connection.
    DeviceConnecting {
        device: DeviceId,
        respond: oneshot::Sender<bool>,
    },

    /// A previously accepted device disconnected.
    DeviceDisconnected {
        device: DeviceId,
        reason: CloseReason,
    },
}

/// Receiver half for engine events.
pub type EventReceiver = mpsc::Receiver<EngineEvent>;

/// Sender half, held internally by the engine.
pub(crate) type EventSender = mpsc::Sender<EngineEvent>;
