# bepository

Rust implementation of the Syncthing BEP v1 protocol for cold/archival storage
backed by object stores (S3, GCS, etc.).

**How the pieces connect:** a Go Syncthing master syncs over TLS/BEP with the
`bepository` sidecar daemon. `bepository-bep`'s engine drives the protocol and
persists everything through its `Storage` trait; `bepository-storage` implements
that trait with one SlateDB instance per shared folder inside the object store.
`bepository-lock` elects the single active instance (epoch lock);
`bepository-dav` exposes point-in-time checkpoints read-only over WebDAV. The
harness, testserver, and e2etest crates exist only for testing.

Multi-crate Cargo workspace:

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
`OVERVIEW.md` — consult it before making changes; `bepository-storage` also
documents its TOML metadata file in `CONFIG.md`.

## Where things live

- `Storage` trait (the contract everything implements):
  `bepository-bep/src/storage.rs`
- Key schema, prefixes, segment extractor:
  `bepository-storage/src/store_keys.rs`
- SlateDB settings (`make_db_settings`), folder lifecycle, metadata I/O:
  `bepository-storage/src/api.rs` (~1.8k lines — search it, don't scan it)
- Compaction GC and scheduler: `bepository-storage/src/compaction.rs`
- Inbox, commit, block dedup: `bepository-storage/src/store.rs`
- TOML metadata schema: `bepository-storage/src/meta.rs` (docs:
  `bepository-storage/CONFIG.md`)
- BEP engine/state machine: `bepository-bep/src/engine.rs`; wire framing:
  `framing.rs`; retry policy: `retry.rs`
- Lock algorithm: `bepository-lock/src/epoch.rs`; renewal/guard: `guard.rs`
- CLI (subcommands, storage-URI parsing, serve loop):
  `bepository-cli/src/main.rs` — no longer a single file: service-management is
  in `service.rs`, self-upgrade in `upgrade.rs`, both behind the default
  `self-manage` feature.

## Vocabulary

- **Active/Standby** — the active `bepository` instance is the one holding the
  epoch lock; others are standby.
- **Master/Slave** — on a given machine, the Master (always Go Syncthing) is the
  source of truth for conflict resolution; the Slave (always bepository) follows
  it.
- **Epoch** — two unrelated meanings. Usually: the `bepository-lock` lease
  counter (8-char base32; appears in `bepository-{epoch}.toml` and inbox keys).
  SlateDB also has internal epochs — don't conflate them.
- **Segment** — a key-prefix partition of a folder's SlateDB keyspace: the `b`
  segment holds block data (`bd` keys); metadata segments hold the rest.
- **Inbox / promotion** — incoming files stage in the inbox (`mi` keys) while
  blocks transfer; `complete_file` atomically promotes them to the committed
  index (`mn` + `ms` keys).
- **Peer floor** — the lowest sequence number still needed by any peer; sequence
  entries below it are garbage-collectable.
- **Checkpoint** — a TTL-bounded SlateDB snapshot, scheduled via the TOML
  metadata file.

## Sharp edges

- `min_compaction_sources` and `max_compaction_sources` must be overridden
  together — a min above the default max (8) silently wedges compaction. See the
  comment in `make_db_settings` (`bepository-storage/src/api.rs`).
- `commit_with_new_seq` is the sole writer of `mn`/`ms` keys, enforced at the
  type level by `LockedFileName`. Don't add writers.
- `complete_file` reads the inbox and commits under `seq_lock`. Don't move
  sequence allocation into the inbox.
- Input validation (e.g. the 10-minute checkpoint interval minimum) lives in
  `bepository-cli`, not in `bepository-storage`.

## Commands

Use `rtk cargo` instead of raw `cargo`. Run `just` to list all recipes.

### Fast feedback (preferred)

```
rtk cargo check -p <crate>    # type-check
rtk cargo clippy -p <crate>   # lint
rtk cargo test -p <crate>     # unit tests
```

Before declaring a change done, run `rtk cargo clippy -p <crate>` and
`rtk cargo test -p <crate>` on every crate you touched, then `just fmt`.

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
- **Docs:** When changing behavior documented in an `OVERVIEW.md` or
  `CONFIG.md`, update that doc in the same commit.
- **Doc comments:** terse. Don't narrate the change, enumerate callers, or
  restate what the type system already guarantees. For APIs: one-line summary,
  then only the non-obvious invariant.
- **Commits:** `<type>: <summary>` (imperative). Types:
  `feat fix refactor docs test chore style perf`. Body: explain *why*, use
  bullet points.
- **Tracing & Logging:** strictly additive — never change existing logs, error
  handling, or control flow for tracing.
  - Levels: `error!` unrecoverable; `warn!` retryable/unusual; `info!` lifecycle
    events; `debug!` file/folder operations; `trace!` reserved.
  - `#[tracing::instrument]` only on large chunks of blocking code, never on hot
    paths (per-block handlers). Always `skip(...)`/`skip_all` complex types;
    `fields(...)` may only reference function arguments (evaluated at entry).
  - `tokio::spawn` loses span context — always use `.in_current_span()`.
  - Field names are fixed: `folder_id` (%), `folder_label` (%), `remote_device`
    (%), `epoch` (%, `.as_base32()`), `count` (numeric), `error` (?). Never
    format inside a field value; never track `elapsed_ms` manually (the
    subscriber handles timing).
- **Locking:** `parking_lot` is the default for in-memory locks; `tokio::sync`
  is reserved for locks held across an `.await`. Use `arc_lock` sparingly, only
  when a guard must outlive the lock's borrow scope (e.g., returned from a
  function). It is not a workaround for `clippy::await_holding_lock`.
- **When uncertain** about crate boundaries or protocol compliance, ask —
  `OVERVIEW.md` is authoritative but may not cover every edge case.

## Idiomatic Rust

Prefer iterator and `Option`/`Result` combinators over index loops and verbose
`match`/`if let`. Avoid `.unwrap_or_default()`, `.unwrap_or(0)`, or
`.unwrap_or(MAX)` when it could mask a real issue (corrupted data, transient
failures) — surface the error to the caller or retry instead.
