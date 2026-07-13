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
        --trailer "Co-authored-by: Claude <noreply@anthropic.com>" \
        --trailer "Co-authored-by: GLM <noreply@z.ai>"


# Push REV (default @-) as a PR on a <hostname>/<changeid> branch; auto-merges when CI passes
ship rev="@-":
    #!/usr/bin/env bash
    set -euo pipefail
    cid=$(jj log -r '{{rev}}' --no-graph -T 'change_id.short()')
    bookmark="$(hostname)/${cid}"
    jj bookmark set "$bookmark" -r '{{rev}}' --allow-backwards
    jj git push --bookmark "$bookmark" --allow-new
    gh pr view "$bookmark" --json number >/dev/null 2>&1 \
        || gh pr create --head "$bookmark" --fill
    gh pr merge "$bookmark" --rebase --auto

# Fetch master and rebase the stack; merged changes and their bookmarks evaporate
sync:
    jj git fetch
    jj rebase -d 'trunk()' --skip-emptied

# On merge, release CI detects the new Cargo.toml version, tags v{{version}},
# builds and uploads static binaries, then opens a PR pinning
# nix/release-hashes.json (assigned to you — merge it to finish the release).
#
# Bump the version on a fresh change atop trunk and ship it as a PR
cut-release version: sync
    #!/usr/bin/env bash
    set -euo pipefail
    jj new 'trunk()' -m "chore: bump version to {{version}}"
    sed -i 's/^version = ".*"/version = "{{version}}"/' Cargo.toml
    {{cargo}} check
    just ship @
    jj new

clean:
    {{cargo}} clean
