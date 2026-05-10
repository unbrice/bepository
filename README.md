<!--
SPDX-FileCopyrightText: 2026 Brice Arnould

SPDX-License-Identifier: MIT OR Apache-2.0
-->

# bepository

> [!WARNING]
> **Pre-1.0:** The on-disk storage format is not yet stable. There are other
> important limitations, see the [corresponding section](#limitations).

**Deduplicated incremental backups with peer-to-peer sync between N hosts.**

`bepository` presents an object store (S3, GCS, SFTP …) as a regular
[Syncthing](https://syncthing.net/) peer, with snapshot support.

It can be run from multiple machines at the same time, only the device with the
highest priority will be active. It speaks standard
[Syncthing protocol](https://docs.syncthing.net/specs/bep-v1.html), any existing
Syncthing setup works to synchronize online devices as well as offline backups.

**Use it to:**

- **Sync devices that are not online together.** Your laptop writes to cold
  storage; days later, your desktop reads from it.
- **Archive into cheap object storage** with automatic point-in-time checkpoints
  for recovery, block deduplication across files and snapshots.

## Features

- **Point-in-time recovery.** Automatic checkpoints (hourly for 24 h, daily for
  7 days by default) exposed over WebDAV.
- **Content-addressed storage.** Identical blocks are deduplicated across files
  and snapshots.
- **Drop-in compatible.** Works as an add-on for existing Syncthing setups, and
  takes advantage of Syncthing features (read-only sources, write-only
  backups...).
- **Cheap, durable backing.** File data lives as a
  [SlateDB](https://slatedb.io/) on top of a dumb object store (S3, GCS, SFTP,
  etc.).
- **Portable identity.** The Device certificate lives inside the object store.
  Any host with access to the storage URI can resume.
- **Reasonably Fast.** A [Foyer](https://foyer.rs/) hybrid disk cache keeps
  bloom filters and indices local (default: `/var/cache/bepository`).

## Limitations

First and foremost, reliability:

- Not much field testing.
- CI is broken and installation instructions don't work.
- On-disk format not stable yet (that will be 1.0).
- Partially implemented using LLMs. (Or should I say? Leverages a *vibe-coding*
  🤠⚡ paradigm 🧠 to establish a robust Plausible Deniability Protocol 🥸 .
  ✨🚀 — —) I've been careful to not trust it blindly but it probably managed to
  slip in some sloppy code.

On the feature front:

- Currently only the active host sees upload progress, other hosts see the
  `bepository` instance offline. Need to decide between:
  - A custom UI + a setting in Syncthing's configuration file to link to it.
  - Having the inactive processes report progress to their local Syncthing
    master.
  - Having the active process report progress to non-local Syncthing masters.
- Encryption support. Need to decide between:
  - Encrypting at SlateDB level, which allows deduplication to work at the
    folder level and keeps webdav working but requires custom crypto.
  - Relying on Syncthing's built-in encryption, which is more secure.

## Contributing

If you have insight into how to solve these problems, please do reach out.
Please *do not* send huge machine-generated PRs, let's discuss design first. I'm
well aware that Claude or Gemini would vibe-code a solution to the above, but
I'm trying to keep the codebase reviewable by humans.

Fellow nix users, a flake lives in `nix/dev`, use it with
`nix develop ./nix/dev`.

---

## How it works

(Illustration shows two laptops, it works for any number of machines.)

```
╭──────────────────────────────────────╮       ╭────────────────────────────────────╮
│                Laptop A              │       │              Laptop B              │
│   ███████████████    ╭───────────╮   │       │   ╭───────────╮    ░░░░░░░░░░░░░░░ │
│   █  bepository █    │ syncthing │   │  P2P  │   │ syncthing │    ░  bepository ░ │
│   █  (active)   █◀━━▶│           │◀━━━━━━━━━━━━━▶│           │◀┈┈▶░  (standby)  ░ │
│   ███████████████    ╰───────────╯   │  SYNC │   ╰───────────╯    ░░░░░░░░░░░░░░░ │
│           ┃                          │       │                                ┊   │
╰───────────┃──────────────────────────╯       ╰────────────────────────────────┊───╯
            ┃ writes                                                            ┊ waits for
            ┃                   ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄                         ┊   lock
            ┗━━━━━━━━━━━━━━━━━━▶█      Snapshots      █┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┘
                                █   (AWS, GCS, ...)   █
                                ▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
```

- `bepository` sits next to a regular Syncthing instance.
- Multiple `bepository` instances can share the same object store. An
  epoch-based distributed lock ensures only the highest-priority instance writes
  at any time; the rest stay on standby and take over automatically if the
  active one loses its lease (e.g., after a long suspend).
- When multiple devices are online, they sync directly; `bepository` passively
  records changes and takes periodic checkpoints.
- A device coming back online syncs from `bepository` if no other peer is
  reachable — two laptops can stay in sync without ever being online
  simultaneously.

## Getting started

See [INSTALLATION.md](INSTALLATION.md) for the full guide:

1. Pick a storage backend (S3, GCS, SFTP, …) and configure credentials.
2. Install the daemon (Systemd Quadlet, NixOS flake, Podman Compose, or from
   source).
3. Pair it with your Syncthing instance.

## Point-in-time recovery

> [!TIP]
> **Alias tip:** Depending on your install method, define a shortcut first.
>
> **Quadlet:**
>
> ```sh
> alias bepository='sudo podman run --rm \
>   --env-file=/etc/bepository/env \
>   -v /etc/bepository:/etc/bepository:ro \
>   ghcr.io/unbrice/bepository:latest'
> ```
>
> **Compose:** `alias bepository='podman compose run --rm bepository'`
>
> **Source:** `alias bepository='./target/release/bepository'` (export
> `BEPOSITORY_*` yourself, or pass flags)

The Quadlet and Compose aliases load `BEPOSITORY_STORAGE_URI` and credentials
from `/etc/bepository/env` automatically (and bind-mount `/etc/bepository/` so
file-based GCS keys work); the commands below assume that.

Checkpoints are taken automatically. To browse or download files from
checkpoints, set `BEPOSITORY_DAV_PASSWORD` in `/etc/bepository/env` and start
the WebDAV server:

```sh
bepository checkpoint serve 0.0.0.0:8080
```

Open `http://localhost:8080` in a WebDAV client (or a browser) and log in with
the password you set. Files are organised as:

```
/<folder-label>/<timestamp>/path/to/file
```

To adjust the checkpoint schedule:

```sh
# Keep hourly checkpoints for 48 hours instead of 24
bepository checkpoint every 1h ttl 2d

# Stop taking hourly checkpoints
bepository checkpoint every 1h remove

# List current schedules and existing checkpoints
bepository checkpoint list
```

## Maintenance

Run `fsck` to check and repair storage:

```sh
# Run a quick integrity check
bepository fsck --check quick
```

The `--check` levels are:

- `quick` — validates inbox entries and basic key structure.
- `structural` — additionally checks sequence mappings, index metadata, and
  directory block references.
- `full` — performs all checks including block hash verification.

<details>
<summary>Other commands</summary>

```sh
# Replace the TLS certificate (changes the Device ID — requires re-pairing)
bepository fsck --regenerate-id

# Force a full compaction
bepository fsck --compact

# Clear a stuck distributed lock
bepository fsck --clear-lock
```

</details>
