# SPDX-FileCopyrightText: 2026 Brice Arnould
#
# SPDX-License-Identifier: MIT OR Apache-2.0

{
  description = "bepository dev shell and source-built package (contributor flake)";

  inputs = {
    nixpkgs.url = "nixpkgs";
    rust-overlay.url = "github:oxalica/rust-overlay";
    crane.url = "github:ipetkov/crane";
    llm-agents.url = "github:numtide/llm-agents.nix";
  };

  outputs = { self, nixpkgs, rust-overlay, crane, llm-agents }:
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
        let p = mkPkg system; in {
          default = p.pkgs.mkShell {
            inputsFrom = [ p.bepository ];

            buildInputs = with p.pkgs; [
              syncthing
              just
              bubblewrap
              socat
              dprint
              reuse
              llm-agents.packages.${system}.rtk
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
