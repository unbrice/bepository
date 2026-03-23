# Contributing

If you have insight into how to solve one of the
[open problems](./README.md#limitations), please do reach out. Please *do not*
send huge machine-generated PRs, let's discuss design first. I'm well aware that
Claude or Gemini would vibe-code a solution, but I'm trying to keep the codebase
reviewable by humans.

Fellow nix users, a flake lives in `nix/dev`, use it with
`nix develop ./nix/dev`. On Debian:

```sh
sudo apt install just protobuf-compiler libssl-dev pkg-config syncthing
```

Then `just` lists the recipes — `just test-unit`, `just test-e2e`, `just fmt`
and `just lint` are the ones you'll want.

## Licensing

Dual-licensed **MIT OR Apache-2.0**. Commits must be signed off per the
[DCO](https://developercertificate.org/):

```sh
git commit -s
```

That's just a `Signed-off-by:` trailer — no GPG key needed. A bot blocks
unsigned PRs.
