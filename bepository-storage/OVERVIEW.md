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

| Key                                  | Value                       | Purpose                                                   |
| ------------------------------------ | --------------------------- | --------------------------------------------------------- |
| `mn<dir>//<basename>`                | protobuf `File`             | Primary file index, grouped by directory                  |
| `ms<seq_be8>`                        | file name (UTF-8)           | Sequence → name mapping for delta index                   |
| `bd<seq_be8>`                        | raw block bytes             | Block data (block segment)                                |
| `mb<dir>/<hash_32>`                  | protobuf `BlockRef`         | Block pointer, scoped by parent directory                 |
| `mr<hash_32>/<dir>//<basename>`      | empty                       | Reverse ref: which files reference a block hash           |
| `mi<epoch_base32>/<dir>//<basename>` | protobuf `Inbox`            | Inbox: file staged during block transfer                  |
| `mx`                                 | protobuf `FolderIndexMeta`  | Local index metadata (index_id, max_sequence, peer_floor) |
| `md<devid_32>`                       | protobuf `RemoteIndexState` | Per-peer remote sequence tracking                         |

Sequence numbers are big-endian u64 to preserve lexicographic ordering.

### Block storage and cross-directory dedup

Blocks are stored scoped by parent directory. When a block with the same hash
already exists in a different directory, a reference is written instead of
duplicating data. Reverse refs enable this lookup and support GC.

### Delta index

Full index: scan the `mn` prefix. Incremental index: scan `ms` from the sequence
number forward, resolve each entry to its `mn` file. When a file is updated, the
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

- **Index mutation serialization:** `name_locks` serializes `mn`, `ms`, `mi`
  mutations per name; `seq_lock` serializes the `IX_KEY` allocation.
  `commit_with_new_seq` must remain the sole writer of `mn` and `ms`; enforced
  at the type level by `LockedFileName`. Compaction may drop dead `mn`, `ms`,
  `mi` entries outside any lock (see Compaction GC); `mx` is preserved.
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

- **Flush interval & size:** Tuned for frequent flushes (60 s,
  `l0_sst_size_bytes = 8 MiB`) to bound L0 SST upload times.
- **Parallelism:** Serialized L0 uploads (`l0_flush_parallelism = 1`) as cold
  storage does not need high flush throughput.
- **L0 headroom (`l0_max_ssts`):** Set to `512` to clear the worst-case L0
  occupancy of the `bd` segment under sustained sync on a slow uplink.

  The per-segment math (under sustained sync on a slow uplink):

  - **Trigger:** size-tiered compaction on the `b` segment fires at
    `min_compaction_sources` = 32 L0 SSTs.
  - **Pin window:** those 32 source SSTs remain in `manifest.l0()` until the
    manifest commit — about ~100 s wall-clock to rewrite ~32 × 8 MiB ≈ 256 MiB
    at a ~2.5 MB/s effective uplink.
  - **Arrivals:** during the pin window new memtable freezes land one L0 SST per
    freeze in the `b` segment at a similar ~3 s cadence, so ~32 new SSTs
    accumulate before the sources are released.

  Effective L0 peak ≈ `2 × min_compaction_sources` = 64.

  If `l0_max_ssts` is tighter than that peak, the flusher's per-segment
  `can_dispatch` refuses to dispatch the next immutable memtable, and writes
  block on `max_unflushed_bytes` backpressure for the duration of the in-flight
  compaction. 512 puts the ceiling well above the worst case with margin to
  spare; the memory cost is only the in-RAM filter+index for each L0 SST, a few
  MB total.

  *Note:* this prevents *entering* the saturation stall. There is a separate
  slatedb-side recovery latency — the compactor and the in-process flusher
  tracker share no direct state path; the flusher only learns about compactor L0
  drops via periodic manifest re-read (gated by
  `Settings::manifest_poll_interval`). If `l0_max_ssts` were ever reached,
  exiting the stall would wait up to one poll interval. At the current
  configuration the cap is unreachable in practice, so the wake-up latency is
  not on the hot path.

### Compactor

- **Poll interval:** Lengthened to 60 s (slatedb default is 5 s) so the
  compactor wakes the radio less often when there is no work. Short-lived
  callers that submit a spec and wait for it (e.g. `fsck-compact`, tests)
  override this per-folder via `SlateStorage::set_compactor_poll_interval`,
  snapshotted into each folder at activation time.
- **Max SST size:** Tuned down to 128 MiB so each compaction-output upload
  completes within a single GCS retry window on the slow link.
- **Concurrency:** Serialized (`max_concurrent_compactions = 1`) to reduce CPU
  burst.

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

- Block data (`bd`, `mb`) and reverse references (`mr`) are dropped/tombstoned
  if not in the Bloom filter.
- Inbox entries (`mi`) from old epochs are tombstoned.
- Sequence mappings (`ms`) and deleted files below the global peer floor are
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
  independently.

## Caveats and follow-ups

These are known suboptimalities or deferred design decisions. Each is explicitly
*not* on the critical path for the current release but is worth revisiting.

### `manifest_poll_interval = 10 s` is a placeholder

`Settings::manifest_poll_interval` governs how often the in-process writer
re-reads the manifest from object storage. In single-writer mode this is the
*only* path by which the writer's flusher tracker learns that the in-process
compactor has dropped L0 SSTs — the compactor and the flusher tracker share no
direct in-memory state. So this knob also bounds the worst-case stall after a
saturation event: if `can_dispatch` ever refuses an imm because of L0
saturation, the flusher waits up to one poll interval for the next re-read
before it can resume dispatching.

The slatedb library default is 1 s. We previously used 120 s to minimise battery
and request cost; under the saturation math in the L0 headroom section that
turned out to be load-bearing for a stall floor we did hit. With
`l0_max_ssts = 512` the cap is unreachable in practice, so the stall floor is
mostly theoretical — but 120 s would still be the recovery time if it ever did
hit.

10 s is a defensive compromise: a short enough floor that any future saturation
would surface quickly, with a modest GET-per-hour cost (~360/h even at idle). It
is *almost certainly* not the right answer:

- on a foreground-active device 1 s would be free and consistent with the
  library default;
- on a battery-powered idle device even 360 GETs/h is wasteful — most of those
  GETs see no change.

The architecturally correct fix is upstream: have the slatedb compactor notify
the in-process flusher tracker directly when it commits a manifest update with
reduced L0 counts, so the writer doesn't depend on the periodic re-read at all.
That would let us keep `manifest_poll_interval` high without the stall-floor
risk. File against slatedb when revisiting.

### `find_block_dir` defensive `bd<seq>` existence check

`bepository-storage/src/store.rs` `find_block_dir` (see the `TODO(phase-7)` doc
comment on the function) ends each candidate iteration with a defensive
`db.get(bd<block_ref.seqno>)` before returning a hit. It is load-bearing today
because the dual-bloom GC builds `known_live_hashes` and `known_live_seqs` in
separate compaction jobs with potentially-different snapshots, leaving a
"pointer outlives data" window that the defensive `get` self-heals by falling
through to the new-block branch.

Once the dual-bloom GC shares a snapshot epoch across the two jobs (or otherwise
proves "`mb` outlives `bd`" is unreachable), the second `db.get` is pure
overhead — one extra block-segment lookup per dedup hit on the write hot path.
Acceptance: a test that constructs the "pointer survives, data dropped"
interleaving and shows it cannot occur, then remove the check and re-measure
dedup throughput.

### Skip refetching blocks already present in the prior version of a file

When the master sends an updated `FileInfo` for a name already tracked, the
inbox / commit path currently treats every block on the new block list as
something that *may* need to be fetched from the master and re-stored. For each
block whose `(hash, offset, size)` triple appears unchanged between the old `mn`
entry and the incoming one, we should:

1. skip the network fetch entirely,
2. reuse the existing `BlockInfo` (carrying its `blockseq` for separated blocks
   or its inline `bytes` for inline blocks) directly in the new `mn` entry, and
3. skip emitting a fresh `mr` reverse ref since the existing one already covers
   this name.

This is per-file dedup-by-prior-version, distinct from the cross-file cross-dir
dedup that `reuse_block` already does via the `mb<dir>/<hash>` lookup. The
seqno-keyed layout makes it especially cheap: the prior `BlockInfo.blockseq` is
compaction-stable and the pointed-at `bd<seq>` row is guaranteed live by virtue
of the old `mn` entry still referencing it.

Lives in the commit path (likely `bepository-bep` / inbox machinery), not in the
storage crate. Acceptance: a test that commits version v1 of a file, then
commits v2 sharing N of v1's blocks, and asserts no network fetches for those N
blocks (and no `bd<new_seq>` rows allocated for them).

### Compaction scheduler

Every segment runs stock size-tiered. The per-row GC filter (`GcFilter`) fires
on every key that passes through any compaction, so tombstone reclamation works
correctly under the stock scheduler. The block segment's settle-and-stay
property comes from the segment extractor + seqno ordering (cold high-key SRs
stop being re-selected), not from any custom policy.

A prior custom scheduler (`BepCompactionScheduler`) forced full compaction on
the metadata segment for instant tombstone reclamation. Field measurement showed
it was paying a very bad write-amp tax: ~100% compactor duty cycle and ~270×
write-amplification per actually-pruned row under realistic ingest. On the
target hardware profile (battery-powered roaming sidecar, slow uplink) a bounded
tombstone backlog is cheaper than a continuously saturated compactor pushing
rewrites through the radio, so it was removed in favor of stock size-tiered.
Operators can still trigger a one-shot full pass via `SlateStorage::compact()` /
the `fsck-compact` CLI subcommand, which submits per-segment full-merge specs to
the running compactor.

*(Post-change steady-state numbers to be recorded here after manual field
measurement.)*
