# bepository-EPOCH.toml

Persistent metadata for a bepository storage instance. Stored as a TOML file in
the object store root alongside the per-folder SlateDB directories.

The `EPOCH` in the filename is the distributed lock epoch that last wrote the
file. On startup, the holder reads the latest existing file (regardless of
epoch), writes a new copy at its own epoch, and deletes old copies via
`clean_meta`. `get-id` and other read-only commands use `read_meta_unlocked`,
which lists files and reads the last one without requiring the lock.

## Fields

### `format_version`

**Type:** integer **Default:** `1` (current supported version)

On-disk format version. An instance refuses to activate a store whose version is
higher than it supports, preventing an older binary from silently clobbering a
store written by a newer release. Old meta files lacking the field are treated
as version `1`. Lock-free readers (`get-id`, `checkpoint list`) warn instead of
failing.

### `next_folder_key`

**Type:** integer **Default:** `0`

Monotonically increasing counter. Incremented each time a folder is registered.
Folder IDs are never reused.

### `[identity]`

**Optional.** Absent until `init` generates the TLS certificate.

| Key        | Type   | Description                                   |
| ---------- | ------ | --------------------------------------------- |
| `cert_der` | string | Base64-encoded DER X.509 certificate          |
| `key_der`  | string | Base64-encoded DER private key (PKCS#8 or EC) |

### `[folders]`

**Default:** empty table

Maps BASE32(id) keys to folder entries. The key is the Crockford Base32 encoding
of the numeric folder ID (e.g. `"00000000"` for ID 0). Each folder's SlateDB
instance lives at `folder_<key>/` in the object store.

| Subkey  | Type   | Description              |
| ------- | ------ | ------------------------ |
| `label` | string | User-visible folder name |

### `[checkpoint]`

**Default:** empty table (written by `init` with two default entries)

Maps humantime interval strings to checkpoint schedule entries. The key is used
as the SlateDB checkpoint name (e.g. `"1h"`, `"1d"`).

| Subkey | Type   | Description                                         |
| ------ | ------ | --------------------------------------------------- |
| `ttl`  | string | How long to keep each checkpoint (humantime format) |

Minimum interval and TTL: 10 minutes, enforced by the CLI (`checkpoint every`)
before writing — the storage crate itself does not validate. SlateDB expires
checkpoints automatically via background GC when their TTL elapses.

## Example

```toml
format_version = 1
next_folder_key = 2

[identity]
cert_der = "MIIBkTCB+wIJAL..."
key_der = "MIGHAgEAMBMGB..."

[folders."00000000"]
label = "photos"

[folders."00000001"]
label = "documents"

[checkpoint.1h]
ttl = "1day"

[checkpoint.1d]
ttl = "7days"
```

## Lifecycle

1. **`init`** — acquires lock, reads existing meta (if any), writes identity and
   default checkpoint schedules if absent, writes `bepository-{epoch}.toml`,
   cleans old copies.
2. **`serve`** — acquires lock, reads meta, cleans old copies. Does not modify
   meta during normal operation (checkpoint tasks write their own via
   `create_checkpoint` on the SlateDB layer, not the TOML meta). New folders
   proposed by the peer are automatically registered in meta.
3. **`checkpoint every`** — acquires lock, modifies the relevant section, writes
   new copy, cleans old copies.
4. **`fsck --regenerate-id`** — acquires lock, replaces `[identity]`, writes new
   copy, cleans old copies.
5. **`get-id`** / **`checkpoint list`** — reads meta without a lock.
