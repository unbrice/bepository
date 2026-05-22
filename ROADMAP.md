> **Warning:** This project is pre-1.0 and is **not yet ready for production
> use.** The on-disk storage format is unstable — any version bump before 1.0
> may introduce breaking changes that require a full re-sync or data migration.

- 0.8 Encryption support.
- 0.9 Battery savings by syncing with system timer, and better batching of
  events (e.g.: `complete_and_notify`).
- 1.0 On-disk format stability.
