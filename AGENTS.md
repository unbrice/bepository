# bepository

Rust implementation of the Syncthing BEP v1 protocol for cold/archival storage
backed by object stores (S3, GCS, etc.). Multi-crate Cargo workspace:

- **`bepository-bep`** — BEP v1 protocol, state machine, `Storage` trait. No
  networking/TLS.
- **`bepository-tls`** — TLS identity, device ID derivation, handshake helpers
  (`rustls`).
- **`bepository-storage`** — SlateDB-backed `Storage` impl; deduplication, delta
  index, checkpoints.
- **`bepository-cli`** — `bepository` binary; roaming sidecar daemon backed by
  object store.
- **`bepository-dav`** — Read-only WebDAV server exposing checkpoints
  (`/folder/checkpoint/files`).
- **`bepository-harness`** — Test library; spawns throwaway Go Syncthing masters
  for interop tests.
- **`bepository-testserver`** — Minimal reference server for interop testing.
- **`bepository-lock`** — Epoch-based distributed locking over object stores.
- **`bepository-e2etest`** — E2e tests exercising `bepository` as a subprocess.

**Stack:** Rust 2024, `prost` (protobufs), SlateDB, `rustls`. Each crate has an
`OVERVIEW.md` — consult it before making changes.

## Commands

Use `rtk cargo` instead of raw `cargo`. Run `just` to list all recipes.

### Fast feedback (preferred)

```
rtk cargo check -p <crate>    # type-check
rtk cargo clippy -p <crate>   # lint
rtk cargo test -p <crate>     # unit tests
```

### Full suite

```
just build / just release      # debug / optimized
just build-cli / release-cli   # CLI only
just test-unit                 # unit tests
just test-e2e                  # e2e (downloads syncthing if needed; USE_SYSTEM_SYNCTHING=0 to force)
just test                      # unit + e2e (builds CLI first)
just lint / just fmt
```

## Permissions

**Allowed:** read files, `rtk cargo check/clippy/fmt`, `just test-unit`,
`just test-e2e`, `just lint`, `just fmt`.

**Require approval:** `just test` (full suite), `git commit/push`,
adding/removing dependencies, `.github/` changes, destructive ops.

## Conventions

- **Crate boundaries:** Networking stays out of `bepository-bep`; storage
  accessed only via the `Storage` trait (never SlateDB directly).
- **Testing:** Unit tests for logic; interop tests use `bepository-harness`
  against real Go masters.
- **Protobufs:** Defined in `bep.proto`, vendored in `proto/`, generated at
  build time via `prost-build`.
- **Conflict resolution:** Injected, never hardcoded.
- **Active/Standby:** The active `bepository` instance is the one holding the
  epoch lock; others are standby.
- **Master/Slave:** On a given machine, the Master (always Go syncthing) is the
  source of truth for conflict resolution; the Slave (always bepository) follows
  it.
- **Commits:** `<type>: <summary>` (imperative). Types:
  `feat fix refactor docs test chore style perf`. Body: explain *why*, use
  bullet points.
- **Tracing & Logging:**
  - **Levels:** `error!` (unrecoverable); `warn!` (retryable/unusual); `info!`
    (lifecycle events); `debug!` (file/folder operations); `trace!` (reserved).
  - **Instrumentation:** Use `#[tracing::instrument(level = "...")]` only for
    large chunks of blocking code.
    - Always `skip(...)` complex types (streams, `Arc`, etc.) or use `skip_all`.
    - `fields(...)` must only reference function arguments (macro evaluates at
      entry).
    - **Never** instrument hot paths (per-block handlers); span creation isn't
      free.
  - **Async Spawning:** Spawns lose context. Always use `.in_current_span()`
    from `tracing::Instrument` with `tokio::spawn`.
  - **Fields:** Use exact names: `folder_id` (%), `folder_label` (%),
    `remote_device` (%), `epoch` (%, use `.as_base32()`), `count` (numeric),
    `error` (?).
    - **Never** format inside a field value.
  - **Forbidden:**
    - Tracking `elapsed_ms` manually (let the subscriber handle timing).
    - Changing existing logs, error-handling, or control flow for tracing.
      Tracing must be strictly additive.
- **Locking:** `parking_lot` is the default for in-memory locks; `tokio::sync`
  is reserved for locks held across an `.await`. Use `arc_lock` sparingly, only
  when a guard must outlive the lock's borrow scope (e.g., returned from a
  function). It is not a workaround for `clippy::await_holding_lock`.
- **When uncertain** about crate boundaries or protocol compliance, ask —
  `OVERVIEW.md` is authoritative but may not cover every edge case.

## Idiomatic Rust

Prefer iterator combinators (`.map()`, `.filter()`, `.fold()`, `.windows()`)
over index loops. Use `Option`/`Result` combinators (`.map_or()`, `.inspect()`)
over verbose `match`/`if let`.

Avoid using `.unwrap_or_default()`, `.unwrap_or(0)` or `.unwrap_or(MAX)` when it
could mask a real issue (e.g., corrupted data, transient failures). In such
cases, surface the error to the caller or retry if appropriate.

## Code Examples

Good: `bepository-bep/src/storage.rs` (Storage trait),
`bepository-tls/src/lib.rs` (device ID). Avoid: direct SlateDB access,
networking in `bepository-bep`.
