# bepository-bep

Rust BEP v1 library: wire protocol, session state machine, and storage
abstraction. Networking and TLS are provided by the caller.

## Scope

- **In**: Message serialization (`prost`), LZ4 framing, BEP messages (Hello to
  Close).
- **Out**: Download progress, introducers, NAT/relays, filesystem storage
  (provided by the `bepository-storage` crate).

## Protocol & State

- **Framing**: `[u16 HeaderLen][Header][u32 BodyLen][Body]` (Big-Endian). LZ4
  compression per Header flag.
- **Hello**: Sent concurrently after TLS. `[u32 Magic][u16 Len][Hello]`.
- **Flow**: `Connected → Hello → HelloDone → ClusterConfig → Ready`.
- **Ready State**: Handles `Index`, `IndexUpdate`, `Request`, `Response`,
  `Ping`, and `Close`.
- **Two-Phase Intake**: Files are staged in an implementation-defined "inbox"
  via `apply_update`. Atomic promotion to the committed index occurs via
  `complete_file` only after all blocks are stored.

## Conflict Resolution

- **Version Vectors**: A dominates B if all counters A[i] ≥ B[i] and at least
  one is strictly >. Neither dominates = `Concurrent`.
- **Delegation**: The engine detects `Concurrent` updates and delegates to an
  injected `ConflictResolver`.
- **Resolution**: Typically favors larger version, then non-deleted status, then
  larger Device ID.
- **Loser Backup**: Losers may be persisted at `<name>.sync-conflict` or
  discarded based on `loser_path` metadata.

## Resilience & Faults

- **Retry Policy**: `RetryPolicy` trait controls failure handling.
  - **Exponential Backoff**: Retries `TransientIo` (default: 1s base, 2.0
    multiplier, 60s max, 10 attempts).
  - **Terminal Errors**: `Corruption`, `Internal`, and `Standby` (lock loss) are
    immediately fatal.
- **Fault Injection**: `FaultStorage` and `FaultStream` decorators provide
  counter-based failure simulation (e.g., "fail next N calls") for testing
  storage and network layers.

## Key Invariants

- **Request/Response Ordering**: Pending requests are registered **before**
  writing to the wire to ensure responses always find a match.
- **Deferred Requests**: If `max_pending_requests` is reached, blocks are queued
  in memory. Promotion to pending happens as slots free.
- **Atomic Promotion**: `complete_file` is only called when no pending or
  deferred requests remain for a file, ensuring data integrity.
- **Event Capacity**: Event sends never block. A full 64-slot channel rejects
  new connections (`DeviceConnecting` overflow) and drops `DeviceDisconnected`
  notifications; disconnect events are only emitted once a listener is taken.
- **Connection Registry**: A single supervisor task owns connection tasks and
  alone removes registry entries (on task reap, identity-checked). A duplicate
  `DeviceId` connection cancels and replaces the displaced one.
