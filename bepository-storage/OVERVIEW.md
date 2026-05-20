# bepository-storage

SlateDB-backed `Storage` for bepository. Each shared folder gets its own SlateDB
instance. Designed for cold/archival storage backed by object stores (S3, GCS,
local filesystem, in-memory for tests).

## Scope

**In:** file index persistence, block storage with cross-directory dedup, delta
index support via sequence tracking, remote peer state, conflict resolution,
point-in-time checkpointing.

**Out:** GC sweep (block/tombstone pruning), sequence pruning below peer floor,
global config store, multi-device remote index storage, filesystem watching.

## Data model

### Metadata file

Persistent metadata (TLS identity, folder registry, checkpoint schedules) is
stored as a TOML file `bepository-{epoch}.toml` in the object store root. The
epoch in the filename is the distributed lock epoch that last wrote it. See
`CONFIG.md` for the full schema.

The folder registry maps numeric IDs to user-visible folder labels. Directory
names are derived using Crockford Base32, avoiding filesystem-unsafe characters
in user labels and keeping object store paths predictable.

### Per-folder data

One SlateDB instance per folder, opened lazily on first access. All state for a
folder lives in a single key namespace with prefixed keys.

### Key layout

| Key                                   | Value                       | Purpose                                                   |
| ------------------------------------- | --------------------------- | --------------------------------------------------------- |
| `n/<dir>//<basename>`                 | protobuf `File`             | Primary file index, grouped by directory                  |
| `s/<seq_be8>`                         | file name (UTF-8)           | Sequence → name mapping for delta index                   |
| `b/<dir>/<hash_32>`                   | raw block bytes             | Block data, scoped by parent directory                    |
| `b/<dir>/<hash_32>/ref`               | protobuf `BlockRef`         | Cross-dir dedup pointer to canonical copy                 |
| `br/<hash_32>/<dir>//<basename>`      | empty                       | Reverse ref: which files reference a block hash           |
| `in/<epoch_base32>/<dir>//<basename>` | protobuf `Inbox`            | Inbox: file staged during block transfer                  |
| `ix`                                  | protobuf `FolderIndexMeta`  | Local index metadata (index_id, max_sequence, peer_floor) |
| `dx/<devid_32>`                       | protobuf `RemoteIndexState` | Per-peer remote sequence tracking                         |

Sequence numbers are big-endian u64 to preserve lexicographic ordering.

### Block storage and cross-directory dedup

Blocks are stored scoped by parent directory. When a block with the same hash
already exists in a different directory, a reference is written instead of
duplicating data. Reverse refs enable this lookup and support GC.

### Delta index

Full index: scan the `n/` prefix. Incremental index: scan `s/` from the sequence
number forward, resolve each entry to its `n/` file. When a file is updated, the
old sequence mapping is deleted and a new one is written.

### Inbox and two-phase file intake

Files are not committed to the index until all their blocks have been stored.
The inbox holds file metadata during block transfer.

**Lifecycle:**

1. Incoming updates are placed in the inbox without allocating a sequence
   number.
2. Block data and reverse references are written to storage.
3. Once all blocks arrive, the file is promoted: the inbox entry and old
   sequence mapping are deleted, and the new file entry and sequence mapping are
   written atomically.

**Concurrency:** Per-name locks (`name_locks`) protect staging and promotion; a
global `seq_lock` serializes sequence allocation. This ensures safe, idempotent
promotion (e.g., concurrent Index/Message loop updates) without blocking
operations on different files.

Version comparison uses the committed index only. The inbox is not
authoritative.

**Inbox GC:** On startup after acquiring a new lock epoch, inbox entries from
prior epochs are deleted.

### Conflict resolution

On concurrent edits, the conflict is resolved by the engine. The winner is
stored at its original path, and the loser is stored at a conflict path.

## Protobuf messages

Defined in `proto/storage.proto`:

- **`BlockRef`** — Points to canonical block data.
- **`FolderIndexMeta`**, **`RemoteIndexState`** — Tracking metadata and
  sequences.
- **`File`**, **`Inbox`** — Envelopes for committed entries and in-progress
  transfers.
- **`FileInfo`**, **`BlockInfo`**, etc. — Storage-local copies of the BEP wire
  types.

Maintaining a separate storage schema from the BEP wire protocol decouples
persistence from the wire format, ensuring stability across bepository versions.

## Invariants

- **Index mutation serialization:** `name_locks` serializes `n/`, `s/`, `in/`
  mutations per name; `seq_lock` serializes the `IX_KEY` allocation.
  `commit_with_new_seq` must remain the sole writer of `n/` and `s/`; enforced
  at the type level by `LockedFileName`. Compaction may drop dead `n/`, `s/`,
  `in/` entries outside any lock (see Compaction GC); `ix` is preserved.
- **Index commit atomicity:** All mutations for file promotion happen in a
  single atomic batch.
- **Block reference integrity:** Canonical block data is verified to exist
  before returning it.
- **Block dedup atomicity:** Block data and reverse references are written
  atomically.
- **Sequence monotonicity:** Sequence numbers are strict non-negative integers.
- **File name validation:** Absolute paths, null bytes, and traversal components
  are rejected.

## Checkpointing

Checkpoints are point-in-time snapshots with TTL-based expiry.

**Schedules** are stored in metadata with human-readable intervals and TTLs.
Default schedules are created on initialization.

**Creating checkpoints:** Flushes the memtable to ensure durability, then
creates a snapshot across all folders.

**Detached Admin:** Checkpoint metadata can be read without holding a full lock
or opening the database.

## Snapshot Filesystem

A read-only, checkpoint-scoped view of the file tree, decoupling checkpoint
browsing from storage internals.

Provides a virtual filesystem interface:

- **Listing:** Maps internal checkpoints to snapshot references.
- **Reading directories:** Scans file entries and detects virtual directories by
  prefix.
- **Reading files:** Point-lookups file metadata.
- **Reading bytes:** Walks block lists, fetching blocks or following
  cross-directory deduplication references.

## SlateDB configuration

Tuned for cold/archival workloads on battery-powered devices over slow
connections.

### Write path

- **Flush interval & size:** Tuned for frequent flushes (e.g., 60s, 4MB) to
  bound L0 SST upload times.
- **Parallelism:** Serialized L0 uploads as cold storage does not need high
  flush throughput.

### Compactor

- **Poll interval:** Reduced to minimize idle wake-ups.
- **Max SST size:** Tuned down so uploads complete within connection timeout
  budgets.
- **Concurrency:** Serialized to reduce CPU burst.

## Compaction GC

Garbage collection of orphaned blocks and stale inbox entries is performed via
compaction filters.

### Overview

1. At the start of a compaction job, a Bloom filter of all live block hashes is
   constructed from committed files and current-epoch inbox entries.
2. The Bloom filter and a "written-since" set are registered for the duration of
   the compaction.
3. During compaction, keys referencing dead blocks or stale epochs are dropped
   or tombstoned.

### Compaction Filter Decisions

- Block data (`b/`, `b/.../ref`) and reverse references (`br/`) are
  dropped/tombstoned if not in the Bloom filter.
- Inbox entries (`in/`) from old epochs are tombstoned.
- Sequence mappings (`s/`) and deleted files below the global peer floor are
  dropped at the lowest sorted run, or tombstoned otherwise.

### Concurrent Compaction Safety

Multiple compaction jobs may run concurrently. Block queries check all active
compactions' Bloom filters and "written-since" sets. Blocks written during
compaction are added to the "written-since" set to prevent accidental deletion
or skipping.

- **No data loss:** Live blocks are protected by the Bloom filter.
- **Re-request safety:** If a block is missed by the Bloom filter (false
  negative not possible, but if dropped), it forces a re-request from peers,
  safely rewriting it.
- **Crash safety:** Bloom filters are in-memory. SlateDB handles crash recovery
  independently. s are in-memory. SlateDB handles crash recovery independently.
