# SPDX-FileCopyrightText: 2026 Brice Arnould
#
# SPDX-License-Identifier: MIT OR Apache-2.0

{
  description = "bepository dev shell and source-built package (contributor flake)";

  inputs = {
    nixpkgs.url = "nixpkgs";
    # `follows` makes rust-overlay track the root nixpkgs instead of carrying
    # its own separate (older) pin — one fewer lockfile node, one fewer nixpkgs
    # eval. rust-overlay is designed to support this.
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";
  };

  outputs = { self, nixpkgs, rust-overlay, crane }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAll = nixpkgs.lib.genAttrs systems;
      pkgsFor = system: import nixpkgs {
        inherit system;
        overlays = [ (import rust-overlay) ];
      };

      mkPkg = system:
        let
          pkgs = pkgsFor system;

          rustToolchain = pkgs.rust-bin.stable.latest.default.override {
            extensions = [ "rust-src" "rust-analyzer" ];
          };

          craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

          commonArgs = {
            src = pkgs.lib.cleanSourceWith {
              src = craneLib.path ./../..;
              filter = path: type:
                (pkgs.lib.hasSuffix ".proto" path) ||
                # Non-Rust files embedded via include_str! in bepository-cli
                # (systemd units, deploy/env.example).
                (pkgs.lib.hasSuffix ".service" path) ||
                (pkgs.lib.hasSuffix ".timer" path) ||
                (pkgs.lib.hasSuffix "/deploy/env.example" path) ||
                (craneLib.filterCargoSources path type);
            };
            strictDeps = true;

            nativeBuildInputs = [
              pkgs.pkg-config
              pkgs.protobuf
            ];

            buildInputs = [
              pkgs.openssl
            ];

            PROTOC = "${pkgs.protobuf}/bin/protoc";
          };

          cargoArtifacts = craneLib.buildDepsOnly commonArgs;

          bepository = craneLib.buildPackage (commonArgs // {
            inherit cargoArtifacts;
            pname = "bepository";
            cargoExtraArgs = "-p bepository-cli";
            meta.mainProgram = "bepository";
          });
        in
        {
          inherit pkgs craneLib commonArgs cargoArtifacts bepository;
        };
    in
    {
      packages = forAll (system:
        let p = mkPkg system; in {
          default = p.bepository;
        });

      apps = forAll (system: {
        default = {
          type = "app";
          program = "${(mkPkg system).bepository}/bin/bepository";
        };
      });

      devShells = forAll (system:
        let
          p = mkPkg system;
          # Rust with the musl targets for the static release binaries. Same
          # rustc as mkPkg's toolchain (same flake.lock pin); defined separately
          # so crane's cargoArtifacts derivation is unaffected.
          crossToolchain = p.pkgs.rust-bin.stable.latest.default.override {
            targets = [ "x86_64-unknown-linux-musl" "aarch64-unknown-linux-musl" ];
          };
        in
        {
          # Shell for the GitHub workflows (ci.yml and release.yml): test
          # tooling plus the release cross-build tooling, so a `nix flake
          # update` that breaks the release toolchain fails CI on the next
          # push instead of on tag day. cmake is for aws-lc-sys; zig is the
          # cross C/asm toolchain cargo-zigbuild drives (ring, aws-lc, zstd,
          # lz4). Deliberately NOT inputsFrom = [ p.bepository ]: that would
          # put crane's musl-less rust toolchain on PATH alongside
          # crossToolchain with unspecified ordering.
          ci = p.pkgs.mkShell {
            buildInputs = with p.pkgs; [
              crossToolchain
              cargo-zigbuild
              zig
              cmake
              pkg-config
              protobuf
              # openssl serves the *native* test builds (same as commonArgs);
              # the release binary itself links no OpenSSL.
              openssl
              syncthing
              just
              bubblewrap
              socat
            ];
            PROTOC = "${p.pkgs.protobuf}/bin/protoc";
          };

          # Contributor shell: day-to-day human tooling, no cross-build weight.
          default = p.pkgs.mkShell {
            inputsFrom = [ p.bepository ];

            buildInputs = with p.pkgs; [
              syncthing
              just
              bubblewrap
              socat
              dprint
              reuse
              rtk
            ];

            shellHook = ''
              export PROTOC="${p.pkgs.protobuf}/bin/protoc"
              # Automatically configure git hooks
              just setup-hooks 2>/dev/null || true
            '';
          };
        });

      checks = forAll (system:
        let p = mkPkg system; in {
          bepository = p.bepository;

          bepository-tests = p.craneLib.cargoNextest (p.commonArgs // {
            inherit (p) cargoArtifacts;
            nativeBuildInputs = p.commonArgs.nativeBuildInputs ++ [ p.pkgs.syncthing ];
          });

          bepository-fmt = p.craneLib.cargoFmt {
            inherit (p.commonArgs) src;
          };

          bepository-clippy = p.craneLib.cargoClippy (p.commonArgs // {
            inherit (p) cargoArtifacts;
            cargoClippyExtraArgs = "--all-targets -- --deny warnings";
          });
        });

      formatter = forAll (system: (pkgsFor system).nixpkgs-fmt);
    };
}
