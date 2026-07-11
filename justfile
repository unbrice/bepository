# Syncthing (Go) version downloaded by fetch-syncthing for interop tests.
# This is NOT the bepository version — that lives in Cargo.toml.
syncthing_version := "1.29.5"
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
    {{cargo}} build --release

build-cli:
    {{cargo}} build --bin bepository --all-features

release-cli:
    {{cargo}} build --release --bin bepository

# Ensure syncthing binary is available (downloads if not in PATH and not already fetched)
fetch-syncthing:
    #!/usr/bin/env bash
    set -euo pipefail
    [[ -x "$SYNCTHING_BIN" ]] && exit 0
    mkdir -p target/tools
    arch=$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
    os=$(uname -s | tr A-Z a-z)
    curl -sL "https://github.com/syncthing/syncthing/releases/download/v{{syncthing_version}}/syncthing-${os}-${arch}-v{{syncthing_version}}.tar.gz" \
        | tar xz -C target/tools --strip-components=1 "syncthing-${os}-${arch}-v{{syncthing_version}}/syncthing"
    echo "Downloaded syncthing v{{syncthing_version}} to target/tools/syncthing"

# Run all tests (unit + e2e)
test: test-unit test-e2e

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

# Verify the CLI builds without the default self-manage feature (distro packager tier).
check-packager:
    {{cargo}} check -p bepository-cli --no-default-features

# Configure git to use hooks from the .githooks directory
setup-hooks:
    git config core.hooksPath .githooks

# Amend the last commit with AI co-authorship trailers
credit-ai:
    git commit --amend --no-edit \
        --trailer "Co-authored-by: Gemini <noreply@google.com>" \
        --trailer "Co-authored-by: Claude <noreply@anthropic.com>"

# Bump version and commit, then echo the commands to tag and push (accepts numerical VERSION)
push-tag version:
    sed -i 's/^version = ".*"/version = "{{version}}"/' Cargo.toml
    {{cargo}} check
    git commit -am "chore: bump version to {{version}}"
    @echo "Version bumped to {{version}}. Verify the commit and then run:"
    @echo "  git tag v{{version}} && git push origin HEAD v{{version}}"
    @echo "Pushing the tag triggers release CI: it builds and uploads static binaries,"
    @echo "then auto-pins nix/release-hashes.json as a bot commit — verify that lands."

clean:
    {{cargo}} clean
