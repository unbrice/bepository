# bepository-testserver

Minimal Syncthing-compatible reference node wiring `bepository-tls` and
`bepository-bep`, for interop testing. Not a production server.

## Key decisions

- `TestServer<S: Storage>` wraps a `BepEngine` with an `Identity` and an
  allow-set of device IDs; connections from devices outside the set are
  rejected.
- Defaults to `MemoryStorage` + `BackupResolver` (both from
  `bepository_bep::test_utils`); `with_storage` injects any backend and conflict
  resolver.
- Kept minimal on purpose: no persistence, no lock, no CLI — just enough to
  exercise the protocol from tests.
