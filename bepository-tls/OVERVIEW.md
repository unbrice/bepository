# bepository-tls

TLS identity and connection helpers for BEP. Pure crypto, callers persist/load
identity bytes.

## Scope

- **In:** self-signed cert generation ("syncthing" DNS SAN), identity from DER,
  device ID derivation, rustls client/server configs, TLS connect/accept
  helpers.
- **Out:** filesystem I/O, cert revocation, CA trust chains, BEP logic.

## Key decisions

- Debug redacts key material. Private key zeroed on drop to minimize
  embarrassing leaks (logs etc).
- ALPN: `["bep/1.0"]` as required by Syncthing.
- Accept any peer cert, trust is via device IDs, not CA chains.
