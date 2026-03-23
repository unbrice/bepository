bepository_version := "1.29.5"
use_system_syncthing := env("USE_SYSTEM_SYNCTHING", "1")
_local_syncthing := justfile_directory() / "target/tools/syncthing"

cargo := `([ -n "${GEMINI_CLI:-}" ] || [ -n "${CLAUDE:-}" ]) && command -v rtk >/dev/null 2>/dev/null && echo "rtk cargo" || echo "cargo"`

export SYNCTHING_BIN := if use_system_syncthing == "1" {
    `command -v syncthing 2>/dev/null || echo "{{_local_syncthing}}"`
} else {
    _local_syncthing
}

default:
    @just --list

build:
    {{cargo}} build --all-features

release:
    {{cargo}} build --release --all-features

build-cli:
    {{cargo}} build --bin bepository --all-features

release-cli:
    {{cargo}} build --release --bin bepository --all-features

# Ensure syncthing binary is available (downloads if not in PATH and not already fetched)
fetch-syncthing:
    #!/usr/bin/env bash
    set -euo pipefail
    [[ -x "$SYNCTHING_BIN" ]] && exit 0
    mkdir -p target/tools
    arch=$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
    os=$(uname -s | tr A-Z a-z)
    curl -sL "https://github.com/syncthing/syncthing/releases/download/v{{bepository_version}}/syncthing-${os}-${arch}-v{{bepository_version}}.tar.gz" \
        | tar xz -C target/tools --strip-components=1 "syncthing-${os}-${arch}-v{{bepository_version}}/syncthing"
    echo "Downloaded syncthing v{{bepository_version}} to target/tools/syncthing"

# Run all tests (unit + e2e)
test: build-cli fetch-syncthing
    {{cargo}} test --all-features
    {{cargo}} test -p bepository-e2etest --all-features -- --ignored --nocapture

test-unit:
    {{cargo}} test --all-features

test-e2e: build-cli fetch-syncthing
    {{cargo}} test -p bepository-e2etest --all-features -- --ignored --nocapture

lint:
    {{cargo}} clippy --workspace --all-targets --all-features
    reuse lint --lines

fmt:
    {{cargo}} fmt --all
    dprint fmt

fmt-check:
    {{cargo}} fmt --all -- --check
    dprint check

# Configure git to use hooks from the .githooks directory
setup-hooks:
    git config core.hooksPath .githooks

clean:
    {{cargo}} clean
