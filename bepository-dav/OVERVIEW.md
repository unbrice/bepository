# bepository-dav

Read-only WebDAV server exposing bepository checkpoints for browsing and
restore. Serves a `SnapshotFs` (from `bepository-storage`) via `dav-server` +
hyper.

## Scope

- **In:** WebDAV virtual filesystem over checkpoints, HTTP Basic auth.
- **Out:** writes of any kind, distributed locking (none needed), checkpoint
  creation/expiry (owned by storage and the CLI).

## Layout

```text
/                            ← folder labels as directories
/photos/                     ← checkpoints, named %Y-%m-%dT%H-%M
/photos/2026-04-09T10-00/    ← files at that checkpoint
```

## Key decisions

- **Static VFS:** snapshots are listed once at startup; restart the server to
  pick up new checkpoints.
- **Auth:** single password over HTTP Basic, compared in constant time
  (`subtle::ConstantTimeEq`), held as `SecretString`.
- **Generic over `SnapshotFs`** so tests can serve fakes without SlateDB.
