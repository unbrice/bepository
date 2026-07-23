#!/bin/sh
# SPDX-FileCopyrightText: 2026 Brice Arnould
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# Installs the latest bepository release and its systemd service.
#
# Manual end-to-end verification (disposable container, -it for the prompts):
#   podman run --rm -it -v "$PWD:/x:ro" debian:stable-slim sh -c \
#     'apt-get update -qq && apt-get install -y -qq curl ca-certificates >/dev/null && sh /x/install.sh'
#
# Served from master but downloads the LATEST release — must stay compatible
# with that release's asset names (bepository-<triple>).
#
# Environment:
#   BEPOSITORY_VERSION      install this release (0.8.0 or v0.8.0) instead of
#                           tracking latest, and skip the auto-upgrade timer
#   BEPOSITORY_STORAGE_URI  storage backend URI (required; env var skips the
#                           prompt, fails if unset in unattended mode)
#   BEPOSITORY_SKIP_NIXOS_CHECK  set to 1 to install on NixOS anyway (unsupported)

set -eu

REPO=https://github.com/unbrice/bepository
GUIDE=$REPO/blob/master/INSTALL.md

die() { echo "install.sh: error: $*" >&2; exit 1; }

need_cmd() { command -v "$1" >/dev/null 2>&1 || die "'$1' is required but was not found — install it with your package manager and re-run"; }

# True when a controlling terminal exists, even when stdin is a pipe (curl|sh).
have_tty() { ( : < /dev/tty ) 2>/dev/null; }

main() {
    if [ -z "${BEPOSITORY_SKIP_NIXOS_CHECK:-}" ] && [ -f /etc/os-release ] && grep -Eq '^ID="?nixos"?$' /etc/os-release 2>/dev/null; then
        cat <<'EOF'
Running on NixOS, which manages system files declaratively and discourages
mutating live state — this script would write to /usr/local/bin and
/etc/systemd/system behind nix's back. Use the flake module instead:

  # flake.nix
  inputs.bepository.url = "github:unbrice/bepository";

  # configuration:
  imports = [ bepository.nixosModules.default ];
  services.bepository.enable = true;

Guide: https://github.com/unbrice/bepository/blob/master/INSTALL.md#nixos

To bypass this check and install the unmanaged way anyway, re-run with
BEPOSITORY_SKIP_NIXOS_CHECK=1 — but things will break: hand-written unit
files and the auto-upgrade timer will fight the declarative configuration.
EOF
        exit 1
    fi

    [ "$(uname -s)" = Linux ] || die "unsupported OS: $(uname -s) — prebuilt binaries are Linux-only; build from source instead: $GUIDE#build-from-source"
    case "$(uname -m)" in
        x86_64)  triple=x86_64-unknown-linux-musl ;;
        aarch64) triple=aarch64-unknown-linux-musl ;;
        *) die "unsupported architecture: $(uname -m) — prebuilt binaries exist only for x86_64 and aarch64; build from source instead: $GUIDE#build-from-source" ;;
    esac
    need_cmd curl
    need_cmd install
    need_cmd mktemp
    need_cmd systemctl
    SUDO=
    if [ "$(id -u)" -ne 0 ]; then
        command -v sudo >/dev/null 2>&1 || die "not running as root and 'sudo' is missing — this installer writes /usr/local/bin and /etc/systemd/system; re-run as root or install sudo"
        SUDO=sudo
    fi

    version=${BEPOSITORY_VERSION:-}
    pinned=
    if [ -n "$version" ]; then
        case "$version" in v*) ;; *) version=v$version ;; esac
        url=$REPO/releases/download/$version/bepository-$triple
        pinned=1
    else
        url=$REPO/releases/latest/download/bepository-$triple
    fi

    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    echo "Downloading $url"
    curl -fsSL -o "$tmp/bepository" "$url" || die "download failed: $url
Check that the release and its bepository-$triple asset exist: $REPO/releases"
    chmod 755 "$tmp/bepository"

    uri=${BEPOSITORY_STORAGE_URI:-}
    if [ -z "$uri" ]; then
        if have_tty; then
            example=sftp://${USER:-$(id -un)}@$(uname -n)/srv/bepository
            [ -z "${HOME:-}" ] || example="$example?key=$HOME/.ssh/id_ed25519"
            cat <<EOF

Where should bepository store the synced data? This becomes
BEPOSITORY_STORAGE_URI. Examples:
  s3://my-bucket/syncthing?region=us-east-1
  $example

Choosing a backend: $GUIDE#storage-uri
EOF
            while [ -z "$uri" ]; do
                printf 'Storage URI: '
                read -r uri < /dev/tty || die "failed to read storage URI"
            done
        else
            die "BEPOSITORY_STORAGE_URI is required. Re-run with BEPOSITORY_STORAGE_URI=<uri> (e.g. curl ... | BEPOSITORY_STORAGE_URI=... sh)
Choosing a backend: $GUIDE#storage-uri"
        fi
    fi

    device_id=
    run_init=1
    if have_tty; then
        printf "Validate the URI and credentials now with 'bepository init' (creates the identity, prints this device's ID)? [Y/n] "
        read -r answer < /dev/tty || answer=
        case "$answer" in [nN]*) run_init= ;; esac
    fi
    if [ -n "$run_init" ]; then
        if ! init_out=$(BEPOSITORY_STORAGE_URI="$uri" "$tmp/bepository" init); then
            die "'bepository init' failed (see its error above) — fix the URI/credentials and re-run this script; nothing was installed"
        fi
        echo "$init_out"
        device_id=$(printf '%s\n' "$init_out" | sed -n 's/^Initialized\. Device ID: //p' | head -n 1)
    fi

    $SUDO install -m 755 "$tmp/bepository" /usr/local/bin/bepository
    echo "Installed /usr/local/bin/bepository"

    set -- install-service --storage-uri "$uri"
    [ -z "$pinned" ] || set -- "$@" --no-auto-upgrade
    $SUDO /usr/local/bin/bepository "$@"

    echo
    echo "Next steps:"
    if [ -n "$device_id" ]; then
        echo "  - Add your backend credentials (e.g. AWS_ACCESS_KEY_ID) to /etc/bepository/env too —"
        echo "    init validated your shell's, but the service only reads the file"
    fi
    echo "  - Set BEPOSITORY_MASTER_DEVICE_ID in /etc/bepository/env to the device ID of the Syncthing instance to follow (the master)"
    echo "  - Start the service: sudo systemctl start bepository"
    if [ -n "$device_id" ]; then
        echo "  - Pair it in Syncthing with bepository's device ID: $device_id"
    else
        echo "  - Pair it in Syncthing with bepository's device ID (get it with: sudo bepository get-id)"
    fi
    echo "    ($GUIDE#step-3-syncthing-integration)"
}

main "$@"
