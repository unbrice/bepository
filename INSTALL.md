# Installation

Installing and configuring `bepository` is a 3-step process:

1. Pick your storage backend and configure credentials.
2. Install the binary.
3. Configure Syncthing.

## Contents

- [Step 1: Storage and Credentials](#step-1-storage-and-credentials) —
  [Storage URI](#storage-uri) · [Credentials](#credentials) ·
  [Credential files under systemd](#credential-files-under-systemd-loadcredential)
  · [Cache](#optional-configure-cache)
- [Step 2: Install the Service](#step-2-install-the-service) —
  [Native binary](#native-binary-recommended) · [NixOS](#nixos) ·
  [Build from source](#build-from-source)
- [Step 3: Syncthing Integration](#step-3-syncthing-integration)
- [Verify & Troubleshoot](#verify--troubleshoot)

---

## Step 1: Storage and Credentials

Before running the service, you need to decide where to store the data and
provide the corresponding configuration.

### Storage URI

The `BEPOSITORY_STORAGE_URI` defines where to store data. Non-secret config
(region, project, endpoint) goes in the URI as query parameters.

| Backend                        | Example                                                                     |
| ------------------------------ | --------------------------------------------------------------------------- |
| Amazon S3                      | `s3://my-bucket/syncthing?region=us-east-1`                                 |
| S3-compatible (MinIO, B2, R2…) | `s3://my-bucket/syncthing?region=auto&endpoint=https://minio.example.com`   |
| Google Cloud Storage           | `gs://my-bucket/syncthing?project=my-gcp-project`                           |
| SFTP                           | `sftp://user@host:22/remote/path`                                           |
| Local path (NAS, testing)      | `file:///var/lib/bepository/store` (writable under the service's state dir) |

> [!WARNING]
> An SFTP URI names a machine, and the store's identity lives on that machine:
> every host sharing the store must reach the *same* SFTP server, so use a
> stable hostname or IP — never `localhost` (each machine would silently sync
> into its own private store).

### Credentials

Credentials live in `/etc/bepository/env`, which the service reads (but cannot
write to). `install-service` (Step 2) creates this file from an annotated
example if it doesn't exist yet — create it manually now only if you want
credentials in place beforehand.

| Backend | Variable                         | Notes                                                                                                                        |
| ------- | -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| AWS     | `AWS_ACCESS_KEY_ID`              | Your AWS access key ID                                                                                                       |
| AWS     | `AWS_SECRET_ACCESS_KEY`          | Your AWS secret access key                                                                                                   |
| AWS     | `AWS_SESSION_TOKEN`              | Optional: AWS session token                                                                                                  |
| GCS     | `GOOGLE_APPLICATION_CREDENTIALS` | Path to a service-account JSON key (recommended) — under systemd see [below](#credential-files-under-systemd-loadcredential) |
| SFTP    | *(URI only)*                     | Key auth: append `?key=…` to the URI — under systemd see [below](#credential-files-under-systemd-loadcredential)             |

Without configured credentials, `s3://` and `gs://` storage URIs fail fast
rather than silently probing the EC2/GCE metadata endpoints; append
`?use_ambient_creds=true` to the URI to opt back into that lookup.

For GCS with a service-account key, drop it in place:

```sh
sudo install -m 600 /path/to/sa-key.json /etc/bepository/sa-key.json
```

> [!WARNING]
> A root-owned `0600` key file is **not readable** by the service, which runs
> under `DynamicUser=yes`. Credential files opened by path must be handed to
> systemd — see
> [Credential files under systemd](#credential-files-under-systemd-loadcredential).
> Inline env values (`AWS_*`) are unaffected.

### Credential files under systemd (LoadCredential)

The service runs under `DynamicUser=yes` (a fresh, unprivileged UID per boot).
`EnvironmentFile` works because systemd reads *it* as root, but a file the
process opens itself — like `GOOGLE_APPLICATION_CREDENTIALS` or an SFTP `?key=…`
— is opened as the dynamic user and gets `Permission denied`.

Hand the key to systemd instead: it reads the file as root at start and
re-exposes it owned by the dynamic user. Add a drop-in:

```sh
sudo systemctl edit bepository
# In the editor, add:
#   [Service]
#   LoadCredential=sa-key.json:/etc/bepository/sa-key.json
```

then point the env file at the credential path systemd exposes:

```sh
# in /etc/bepository/env:
GOOGLE_APPLICATION_CREDENTIALS=/run/credentials/bepository.service/sa-key.json
```

The same applies to an SFTP key:
`LoadCredential=id_ed25519:/etc/bepository/id_ed25519` and
`?key=/run/credentials/bepository.service/id_ed25519` in the storage URI.

**`install-service` wires this up automatically.** On every run it reads
`/etc/bepository/env` and, for an SFTP `?key=/absolute/path` or a
`GOOGLE_APPLICATION_CREDENTIALS=/absolute/path`, writes the drop-in and rewrites
the value to the `/run/credentials/…` path (warning if the source file is
missing). If you add such a path by hand, re-run
`sudo bepository
install-service`. Because systemd re-reads the source at every
service start, rotating the key at the source path keeps working.
`uninstall-service` restores the original paths before removing the drop-ins.

The manual recipe above remains relevant for NixOS users — see the
[NixOS section](#nixos) below; the module wires `LoadCredential` via
`systemd.services.bepository.serviceConfig` — and for drop-ins you manage
yourself.

### Optional: Configure Cache

By default, `bepository` uses a local cache to avoid unnecessary reads. It
follows the `$BEPOSITORY_CACHE_DIRECTORY` or `$CACHE_DIRECTORY` env variable if
set, otherwise XDG guidelines.

To disable the cache (e.g. when the object store is a local NAS), set
`BEPOSITORY_NO_CACHE=1` in `/etc/bepository/env` and restart the service. For
ad-hoc commands, pass `--no-cache`, or override the directory with
`--cache-dir`.

> [!WARNING]
> **Do not** point `BEPOSITORY_CACHE_DIRECTORY` at the service's
> `CacheDirectory` (`/var/cache/bepository`) when running ad-hoc commands. The
> on-disk cache has no cross-process lock; a concurrent ad-hoc command and the
> running daemon can corrupt each other's index. Leave it unset so ad-hoc runs
> use a separate XDG cache dir.

## Step 2: Install the Service

Choose the installation method that best fits your environment.

### Decision Table

| Method                | Best for               | Notes                                                                |
| --------------------- | ---------------------- | -------------------------------------------------------------------- |
| **Native binary**     | Most Linux distros     | **Recommended**. One-liner install script; daily auto-upgrade timer. |
| **NixOS**             | NixOS users            | Native declarative module using the prebuilt release binary.         |
| **Build from source** | Developers / non-Linux | Requires Rust stable and `protoc`.                                   |

### Native binary (Recommended)

Prebuilt static binaries (musl) are published for **x86_64** and **aarch64**
Linux only — on other architectures, [build from source](#build-from-source).

```sh
curl -fsSL https://raw.githubusercontent.com/unbrice/bepository/master/install.sh | sh
```

The script downloads the latest release for your architecture, installs it to
`/usr/local/bin`, and installs the systemd service (see below). It uses `sudo`
only for the two privileged steps, never for the download. On NixOS it refuses
and points at the [flake module](#nixos) instead.

It asks up to two questions. Without a terminal, the script requires
`BEPOSITORY_STORAGE_URI` to be set (the storage URI prompt is skipped if the
variable is set; the second question is only asked when a terminal is present):

1. **Your storage URI** (Step 1), offering an SFTP example. An empty answer is
   not accepted.
2. **Whether to run `bepository init`** — recommended: it validates the URI and
   credentials and creates the storage identity, printing your Device ID. If
   init fails, nothing is installed.

To pin a version instead of tracking `latest`, set `BEPOSITORY_VERSION` (with or
without the leading `v`); a pinned install skips the auto-upgrade timer so the
pin is not upgraded away:

```sh
curl -fsSL https://raw.githubusercontent.com/unbrice/bepository/master/install.sh | BEPOSITORY_VERSION=0.8.0 sh
```

`install-service` writes `/etc/systemd/system/bepository.service`, enables it
and the `bepository-upgrade.timer`, and — if `/etc/bepository/env` does not yet
exist — installs the example config there (mode 600); a `--storage-uri <URI>` on
the command line is persisted into it. Credential-file paths in that file get
`LoadCredential` drop-ins automatically — see
[Credential files under systemd](#credential-files-under-systemd-loadcredential).
Edit the env file (set at least `BEPOSITORY_STORAGE_URI`,
`BEPOSITORY_MASTER_DEVICE_ID`, and `BEPOSITORY_LISTEN`) before starting.

The unit runs hardened: `DynamicUser=yes`, `ProtectSystem=strict`, with state in
`/var/lib/bepository` and cache in `/var/cache/bepository`.

<details>
<summary>Manual install (without the script)</summary>

```sh
# 1. Download the static binary for your architecture
curl -fsSL -o /tmp/bepository \
  https://github.com/unbrice/bepository/releases/latest/download/bepository-$(uname -m)-unknown-linux-musl

# 2. Install it
sudo install -m 755 /tmp/bepository /usr/local/bin/bepository

# 3. Install the systemd service (and daily upgrade timer)
sudo bepository install-service

# 4. Edit the config it just created, then start
sudoedit /etc/bepository/env
sudo systemctl start bepository
```

</details>

#### Local storage outside `/var/lib/bepository`

Because the unit sets `ProtectSystem=strict`, a `file://` storage path outside
`/var/lib/bepository` is not writable by default. Grant access with a drop-in:

```sh
sudo systemctl edit bepository
# In the editor, add:
#   [Service]
#   ReadWritePaths=/your/storage/path
```

#### Updates

By default `install-service` also installs `bepository-upgrade.timer`, which
runs `bepository upgrade` daily and restarts the service. To opt out at install
time, pass `--no-auto-upgrade`. To disable an already-installed timer:

```sh
sudo systemctl disable --now bepository-upgrade.timer
```

> [!WARNING]
> **Pre-1.0 caveat:** the on-disk format is not yet stable. A breaking release
> will refuse to activate a store it cannot read (the format-version fence), but
> it cannot make an older release forward-compatible. If you run multiple
> instances sharing one store, keep them on the same version — the auto-upgrade
> timer does not coordinate across hosts.
>
> To pin a specific version instead of tracking `latest`, run the install script
> with `BEPOSITORY_VERSION` (implies `--no-auto-upgrade`), or download a tagged
> asset URL (e.g.
> `…/releases/download/v0.8.0/bepository-x86_64-unknown-linux-musl`) and install
> with `--no-auto-upgrade`.

### NixOS

The NixOS module runs a plain `systemd.services.bepository` using the prebuilt
static release binary (`bepository-bin`, fetched with a pinned sha256). It sets
`BEPOSITORY_PACKAGE_MANAGED` so the self-manage subcommands defer to
`nix flake update`; nix owns updates, so no upgrade timer is installed. Custom
builds are welcome — override `services.bepository.package` to a source build
(e.g. from `nix/dev`), but host it independently and mention it in bug reports
so the maintainer knows the binary isn't the release artifact.

<details>
<summary>NixOS Configuration Example</summary>

```nix
# flake.nix
inputs.bepository.url = "github:unbrice/bepository";

outputs = { nixpkgs, bepository, ... }: {
  nixosConfigurations.myhost = nixpkgs.lib.nixosSystem {
    modules = [
      bepository.nixosModules.default
      ({ ... }: {
        services.bepository = {
          enable         = true;

          # required
          storageUri     = "s3://my-bucket/backup?region=us-east-1"; # non-secret config in URI
          masterDeviceId = "XXXXXXX-...";   # The device ID of your local Syncthing.

          # optional
          listen         = "127.0.0.1:22001"; # default loopback; 0.0.0.0:22001 to accept remote
          priority       = 100;   # distributed-lock priority
          lease          = 180;   # lock lease in seconds (minimum 180)
          enableCache    = true;  # set false to disable the disk cache

          # Credential files opened by path (GCS service-account key, SFTP key)
          # must be handed to systemd via LoadCredential, not read directly:
          # the service runs under DynamicUser=yes, so a root-owned 0600 key
          # under /etc/bepository/ is unreadable by the process. systemd reads
          # it as root at start and re-exposes it owned by the dynamic user.
          extraEnv.GOOGLE_APPLICATION_CREDENTIALS =
            "/run/credentials/bepository.service/sa-key.json";
        };

        # Hand the key file to systemd (reads it as root, re-exposes it owned by
        # the dynamic user under /run/credentials/bepository.service/).
        systemd.services.bepository.serviceConfig.LoadCredential = [
          "sa-key.json:/etc/bepository/sa-key.json"
        ];

        # Example secret placement with sops-nix:
        # sops.secrets."bepository-sa-key" = {
        #   path = "/etc/bepository/sa-key.json";
        #   mode = "0400";
        # };
      })
    ];
  };
};
```

</details>

### Build from source

Requires Rust stable and `protoc`. The project uses
[just](https://github.com/casey/just):

```sh
git clone https://github.com/unbrice/bepository
cd bepository
just build-cli          # binary lands in target/debug/bepository
just release-cli        # optimised build in target/release/bepository
```

---

## Step 3: Syncthing Integration

`serve` initializes storage automatically on first startup (generating the TLS
identity that defines bepository's Device ID), so there is no separate `init`
step for the native and NixOS installs. You just need the Device ID to pair with
Syncthing. The master's device ID is normally set via
`BEPOSITORY_MASTER_DEVICE_ID` in the env file; for one-off runs you can pass it
positionally instead: `bepository serve <MASTER_DEVICE_ID>`.

### 1. Get bepository's Device ID

The Device ID is logged on every startup as a structured field. Read it from the
journal:

```sh
sudo journalctl -u bepository | grep device_id
```

Or print it directly (needs read access to `/etc/bepository/env`, so run as root
or with `sudo`):

```sh
sudo bepository get-id
```

### 2. Connect

1. Copy the Device ID.
2. In your master Syncthing web UI, go to **Add Remote Device** and paste the
   ID.
3. Set the address to `tcp://127.0.0.1:22001` (match `BEPOSITORY_LISTEN`, and
   the host if running remotely). The installed config pins `127.0.0.1:22001`;
   the binary default is an ephemeral loopback port (`127.0.0.1:0`), with the
   bound address printed at startup as `Listening on: …`.
4. Share folders with the new device. Syncthing will connect, exchange indexes,
   and start syncing with cold storage. If multiple bepository instances share
   cold storage, only one will be active at a time.

## Verify & Troubleshoot

Watch the logs while Syncthing connects:

```sh
sudo journalctl -u bepository -f
```

Syncthing can take several minutes to discover the new device; pausing and
resuming it in the Syncthing UI forces a reconnect. Once connected, the UI shows
the device as **Connected** and syncing.

- **Connection rejected in the logs** — the connecting device is not
  `BEPOSITORY_MASTER_DEVICE_ID`; each instance accepts exactly one master.
- **Storage errors at startup** — check the URI and credentials;
  `sudo bepository get-id` tests them without starting the daemon. With
  credentials wired via `LoadCredential`, that only works while the service is
  running — see
  [Running ad-hoc commands without the service](#running-ad-hoc-commands-without-the-service).
- **Lock/standby messages** — another instance holds the lock; expected when
  several machines share a store. `sudo bepository fsck --clear-lock` forces it
  free, but only when no other instance is running.

### Running ad-hoc commands without the service

`get-id`, `fsck`, and friends read the same `/etc/bepository/env` the daemon
does (your process environment always wins over the file, matching systemd's
`EnvironmentFile` semantics). When credentials are wired via `LoadCredential` —
automatically by `install-service`, or by hand — the env file points at
`/run/credentials/bepository.service/…`, which only exists while the service is
running. Ad-hoc commands therefore fail when the daemon is stopped; the
workarounds below depend on the backend.

**GCS:** pipe the key in and override the path to `/dev/stdin` (Linux symlinks
it to your input; bepository reads it once at startup, then holds the parsed
credentials in memory). Run as a normal user — no `sudo` on `bepository` itself:

```sh
sudo cat /etc/bepository/sa-key.json | \
  GOOGLE_APPLICATION_CREDENTIALS=/dev/stdin bepository fsck
```

The `sudo cat` is the only privileged step: it reads the `root:root 0600` key
that the dynamic-user daemon also can't open by path. The same pattern works for
`get-id`.

**SFTP:** the key lives in the URI, not an env var, so override the whole URI
(your process environment wins) with the real key path:

```sh
BEPOSITORY_STORAGE_URI='sftp://user@host/path?key=/home/user/.ssh/id_ed25519' \
  bepository fsck
```

**Uninstall:** `sudo bepository uninstall-service`, then remove
`/etc/bepository/`, `/var/lib/bepository`, and `/var/cache/bepository`. Synced
data lives in the object store and is not touched.
