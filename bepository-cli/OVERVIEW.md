# bepository-cli

The `bepository` binary: cold-storage bridge daemon for Syncthing. Single source
file (`src/main.rs`): clap definitions, storage-URI parsing, subcommand
implementations, serve loop.

## Subcommands

- **`init`** — create storage, generate TLS identity. Idempotent; takes the
  lock.
- **`get-id`** — print the device ID. Lock-free (`read_meta_unlocked`).
- **`serve <MASTER_DEVICE_ID>`** — run the daemon. Holds the distributed lock
  for its lifetime; auto-registers folders proposed by the master.
- **`remove-folder`** — delete a folder's object-store keys. Takes the lock.
- **`checkpoint`** — `every <interval> (ttl <ttl> | remove)` manages schedules;
  `list` shows schedules and existing checkpoints; `serve <addr>` exposes them
  over read-only WebDAV (`bepository-dav`).
- **`fsck`** — `--check <quick|structural|full>`, `--regenerate-id`,
  `--clear-lock`, `--compact` (one-shot full compaction pass).

## Key decisions

- **Storage URIs** (`parse_storage_uri`): plain paths and `file://` use
  `LocalFileSystem`; `sftp://` goes through opendal; `s3://`, `gs://`,
  `http(s)://` use native `object_store`, configured from environment variables;
  `memory://` for tests.
- **Locking:** short-lived admin commands wrap work in `with_lock` (acquire,
  run, always release). `serve` takes `--priority` (preemption) and `--lease`
  (seconds, minimum 180).
- **Conflicts:** `TheirsResolver` always accepts the master's version — the
  master retains the local copy. Consistent with the Master/Slave convention.
- **Block cache:** optional Foyer disk cache, keyed per device ID. Directory
  auto-detected from `$BEPOSITORY_CACHE_DIRECTORY`, systemd `$CACHE_DIRECTORY`,
  then the XDG cache dir; `--no-cache` disables.
- **Validation lives here, not in storage:** e.g. `validate_checkpoint_duration`
  enforces the 10-minute minimum for checkpoint intervals and TTLs.
- **Env vars:** most flags mirror `BEPOSITORY_*` variables (`STORAGE_URI`,
  `MASTER_DEVICE_ID`, `LISTEN`, `PRIORITY`, `LEASE`, `LOG`, `MACHINE_ID`,
  `DAV_PASSWORD`, `NO_CACHE`).
- **Shutdown:** Ctrl-C cancels via `CancellationToken`; repeated presses force
  quit.
