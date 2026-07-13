# bepository-cli

The `bepository` binary: cold-storage bridge daemon for Syncthing. Entry point
is `src/main.rs` (clap definitions, storage-URI parsing, the serve loop, and
subcommand dispatch); service-management and self-upgrade live in `service.rs`
and `upgrade.rs` respectively, behind the default `self-manage` feature.

## Subcommands

- **`init`** — create storage, generate TLS identity. Idempotent; takes the
  lock. (Kept for compatibility; `serve` auto-inits on startup when the store is
  uninitialized, which is required under the hardened systemd unit —
  `DynamicUser=yes` means there is no stable UID for a root-run one-shot
  `init`.)
- **`get-id`** — print the device ID. Lock-free (`read_meta_unlocked`).
- **`serve [MASTER_DEVICE_ID]`** — run the daemon. Holds the distributed lock
  for its lifetime; auto-registers folders proposed by the master. Runs the
  idempotent init path when the store has no identity, and logs the device ID at
  `info!` on every startup.
- **`remove-folder`** — delete a folder's object-store keys. Takes the lock.
- **`list-folders`** — list registered folders and their storage keys. Lock-free
  (`list_folders_unlocked`); runs alongside an active daemon.
- **`checkpoint`** — `every <interval> (ttl <ttl> | remove)` manages schedules;
  `list` shows schedules and existing checkpoints; `serve <addr>` exposes them
  over read-only WebDAV (`bepository-dav`).
- **`fsck`** — `--check <quick|structural|full>`, `--regenerate-id`,
  `--clear-lock`, `--compact` (one-shot full compaction pass).
- **`install-service` / `print-service` / `uninstall-service`** *(default
  `self-manage` feature)* — manage a hardened systemd unit (`DynamicUser=yes`,
  `ProtectSystem=strict`, `EnvironmentFile=/etc/bepository/env`).
  `install-service [--no-auto-upgrade]` also installs a daily self-upgrade
  timer. Distros that own service files build with `--no-default-features`.
- **`upgrade [--restart-unit <unit>] [--dry-run]`** *(default `self-manage`
  feature)* — self-update from GitHub releases (see `upgrade.rs`).

## Key decisions

- **`self-manage` feature** (on by default): gates `install-service`,
  `print-service`, `uninstall-service`, and `upgrade`. `--no-default-features`
  builds contain no service-management or networking code, letting distro
  packagers own service files and updates.
- **Package-managed contract:** when `BEPOSITORY_PACKAGE_MANAGED` is set (e.g.
  `update via 'nix flake update'`), the four `self-manage` subcommands are
  hidden from `--help` and refuse execution with the value as the update hint.
  Two separate mechanisms — clap's `hide` only affects help text, so each
  handler guards independently.
- **Env-file load:** at the top of `main()`, before `Cli::parse()`, the CLI
  loads `/etc/bepository/env` (overridable via `BEPOSITORY_ENV_FILE`) as the
  lowest-precedence config layer — existing process env and flags always win.
  Gives ad-hoc commands the same config the systemd unit reads via
  `EnvironmentFile`. Mirrors systemd's literal-value semantics (no shell
  expansion).
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
  then the XDG cache dir; `--no-cache` disables. The cache dir must not be
  shared with the running service (no cross-process lock on the on-disk index).
- **Validation lives here, not in storage:** e.g. `validate_checkpoint_duration`
  enforces the 10-minute minimum for checkpoint intervals and TTLs.
- **Env vars:** most flags mirror `BEPOSITORY_*` variables (`STORAGE_URI`,
  `MASTER_DEVICE_ID`, `LISTEN`, `PRIORITY`, `LEASE`, `LOG`, `MACHINE_ID`,
  `DAV_PASSWORD`, `NO_CACHE`). Storage URI, master device ID, and WebDAV
  password are optional flags backed by env — on an installed machine (env file
  present) most commands take no flags; flags only override the env.
- **Shutdown:** Ctrl-C cancels via `CancellationToken`; repeated presses force
  quit.
