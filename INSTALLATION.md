# Installation

Installing and configuring `bepository` is a 3-step process:

1. Pick your storage backend and configure credentials.
2. Install the binary.
3. Configure Syncthing.

---

## Step 1: Storage and Credentials

Before running the service, you need to decide where to store the data and
provide the corresponding configuration.

### Create the Environment File

Download the sample environment file and save it as `bepository.env`:

```sh
curl -o bepository.env https://raw.githubusercontent.com/unbrice/bepository/master/deploy/env.example
```

Open `bepository.env` in your editor and configure `BEPOSITORY_STORAGE_URI`,
`BEPOSITORY_MASTER_DEVICE_ID` (the Device ID of your local Syncthing), and your
storage credentials if applicable.

### Storage URI

The `BEPOSITORY_STORAGE_URI` defines where to store data. Non-secret config
(region, project, endpoint) goes in the URI as query parameters.

| Backend                        | Example                                                                   |
| ------------------------------ | ------------------------------------------------------------------------- |
| Amazon S3                      | `s3://my-bucket/syncthing?region=us-east-1`                               |
| S3-compatible (MinIO, B2, R2…) | `s3://my-bucket/syncthing?region=auto&endpoint=https://minio.example.com` |
| Google Cloud Storage           | `gs://my-bucket/syncthing?project=my-gcp-project`                         |
| SFTP                           | `sftp://user@host:22/remote/path`                                         |

### Credentials

For cloud storage, set credentials in an environment file.

| Backend | Variable                         | Notes                                 |
| ------- | -------------------------------- | ------------------------------------- |
| AWS     | `AWS_ACCESS_KEY_ID`              | Your AWS access key ID                |
| AWS     | `AWS_SECRET_ACCESS_KEY`          | Your AWS secret access key            |
| AWS     | `AWS_SESSION_TOKEN`              | Optional: AWS session token           |
| GCS     | `GOOGLE_SERVICE_ACCOUNT_KEY`     | JSON content of a service-account key |
| GCS     | `GOOGLE_APPLICATION_CREDENTIALS` | Path to a service-account key file    |
| GCS     | `CLOUDSDK_AUTH_ACCESS_TOKEN`     | Short-lived bearer token              |

### Optional: Configure Cache

By default, `bepository` uses a local cache to avoid unnecessary reads. It
follows the `$BEPOSITORY_CACHE_DIRECTORY` or `$CACHE_DIRECTORY` env variable if
set, otherwise XDG guidelines.

If the cold storage is always local, you can disable it with `--no-cache`. Cache
can also be overridden with `--cache-dir`.

## Step 2: Install the Service

Choose the installation method that best fits your environment.

> [!WARNING]
> **Pre-1.0 warning:** The on-disk format is not yet stable. If you enable
> auto-updates (Quadlet's `AutoUpdate=registry`, or pulling `:latest` on a
> schedule), you may pull breaking changes. Consider pinning to a specific image
> digest, or disabling auto-update, until 1.0.

### Decision Table

| Method                      | Best for                                | Notes                                                                               |
| --------------------------- | --------------------------------------- | ----------------------------------------------------------------------------------- |
| **Systemd Quadlet**         | Most distros (Fedora, RHEL, Debian 12+) | **Recommended**. Integrates with systemd for auto-updates and lifecycle management. |
| **NixOS**                   | NixOS users                             | Native declarative module available.                                                |
| **Podman / Docker Compose** | Quick evaluation                        | Not recommended for production as it lacks native systemd service integration.      |
| **Source**                  | Developers                              | Requires Rust stable and `protoc`.                                                  |

### Systemd Quadlet (Recommended)

Ensure you have **Podman 4.4+**
[installed](https://podman.io/docs/installation#linux-distributions) (required
for Quadlet support).

```sh
# Install the unit
sudo mkdir -p /etc/containers/systemd /etc/bepository
sudo curl -o /etc/containers/systemd/bepository.container \
  https://raw.githubusercontent.com/unbrice/bepository/master/deploy/bepository.container

# Install the environment file
sudo cp bepository.env /etc/bepository/env

# Create the credentials file. Either leave it empty (and keep all settings in
# /etc/bepository/env), or move secrets here so the main env file can be
# world-readable while credentials stay 0600.
sudo install -m 600 /dev/null /etc/bepository/credentials

sudo systemctl daemon-reload
```

**Verify setup:** Check that systemd successfully generated the service from the
`.container` file:

```sh
systemctl status bepository
```

It should report
`Loaded: loaded (/etc/containers/systemd/bepository.container; generated)`. It
is normal for the service to be `inactive (dead)` at this point — it hasn't been
started yet. If it says `not-found`, check your podman installation or file
syntax.

**Start and enable auto-update:**

```sh
sudo systemctl enable --now bepository
sudo systemctl enable --now podman-auto-update.timer
```

The unit provisions a block cache in `/var/cache/bepository` to speed up access
to frequent data. To disable the cache (e.g. if the object store is a local
NAS), set `BEPOSITORY_NO_CACHE=1` in `/etc/bepository/env` and run
`sudo systemctl restart bepository`.

### NixOS

The NixOS module is a thin wrapper around the Quadlet path: it drops the same
`bepository.container` into `/etc/containers/systemd/` and runs the same OCI
image as the standalone Quadlet/Compose installs. The container is not built by
this flake, on purpose: any custom build is welcome but should be hosted
independently, and reported as such in bug reports.

Add the flake to your inputs and import the module:

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
        # Required: Podman 4.4+ for Quadlet support.
        virtualisation.podman.enable = true;

        services.bepository = {
          enable         = true;

          # required
          storageUri     = "s3://my-bucket/backup?region=us-east-1"; # non-secret config in URI
          masterDeviceId = "XXXXXXX-...";   # The device ID of your local Syncthing.

          # optional
          port           = 22001; # host port to publish (default: 22001)
          priority       = 100;   # distributed-lock priority
          lease          = 180;   # lock lease in seconds (minimum 180)
          enableCache    = true;  # set false to pass --no-cache (default: true)

          # File containing credentials (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY,
          # or CLOUDSDK_AUTH_ACCESS_TOKEN / GOOGLE_SERVICE_ACCOUNT_KEY for GCS).
          # Symlinked into /etc/bepository/credentials so secrets stay out of
          # the Nix store.
          environmentFile = "/run/secrets/bepository.env";
        };
      })
    ];
  };
};
```

</details>

### Podman / Docker Compose

Pre-built images are published to the GitHub Container Registry on every
release: `ghcr.io/unbrice/bepository:latest`

The `deploy/` directory contains a ready-to-run Compose file. Copy it to your
deployment machine:

```sh
mkdir ~/bepository && cd ~/bepository
curl -O https://raw.githubusercontent.com/unbrice/bepository/master/deploy/compose.yml
curl -o .env https://raw.githubusercontent.com/unbrice/bepository/master/deploy/env.example
```

Then edit `.env` with your settings and run:

```sh
podman compose up -d
```

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

Once the daemon is installed, you must initialize the storage, find its Device
ID, and connect it to your master Syncthing node.

> [!TIP]
> **Alias tip:** Depending on your install method, define a shortcut first.
>
> **Quadlet:**
>
> ```sh
> alias bepository='sudo podman run --rm \
>   --env-file=/etc/bepository/env \
>   --env-file=/etc/bepository/credentials \
>   ghcr.io/unbrice/bepository:latest'
> ```
>
> **Compose:** `alias bepository='podman compose run --rm bepository'`
>
> **Source:** `alias bepository='./target/release/bepository'`

### 1. Initialize Storage

Initialization generates the TLS identity that defines bepository's Device ID
and writes the default checkpoint schedules. It is safe to re-run (it is a no-op
if already initialized).

```sh
bepository init
```

*(For Compose, you can alternatively run the pre-configured shortcut:
`podman compose run --rm init`)*

### 2. Get bepository's Device ID

Print the slave's Device ID so you can add it to your Syncthing master.

```sh
bepository get-id
```

*(For Compose, you can alternatively run the pre-configured shortcut:
`podman compose run --rm get-id`)*

### 3. Connect

1. Copy the printed Device ID.
2. In your master Syncthing web UI, go to **Add Remote Device** and paste the
   ID.
3. Set the address to `tcp://127.0.0.1:22001` (or whatever port you configured
   if running remotely).
4. Share folders with the new device. Syncthing will connect, exchange indexes,
   and start syncing with cold storage. If multiple bepository share cold
   storage, only one will be active at a time.
