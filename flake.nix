# SPDX-FileCopyrightText: 2026 Brice Arnould
#
# SPDX-License-Identifier: MIT OR Apache-2.0

{
  description = "bepository: NixOS module + prebuilt binary for the Syncthing cold-storage bridge";

  # End-user flake. The contributor flake at nix/dev/ provides the dev shell
  # and `nix flake check` — invoke it with `./nix/dev`.
  inputs.nixpkgs.url = "nixpkgs";

  outputs = { self, nixpkgs, ... }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forAll = nixpkgs.lib.genAttrs systems;

      # Per-system mapping from Nix system to the release asset's target triple.
      tripleFor = system: {
        "x86_64-linux" = "x86_64-unknown-linux-musl";
        "aarch64-linux" = "aarch64-unknown-linux-musl";
      }.${system};

      # The pinned release version + sha256s, maintained by release CI.
      hashes = builtins.fromJSON (builtins.readFile ./nix/release-hashes.json);

      # Prebuilt static musl binary fetched from GitHub releases, wrapped to set
      # BEPOSITORY_PACKAGE_MANAGED so the self-manage subcommands defer to nix.
      bepositoryBinFor = pkgs: system:
        let
          triple = tripleFor system;
          url = "https://github.com/unbrice/bepository/releases/download/v${hashes.version}/bepository-${triple}";
          raw = pkgs.fetchurl {
            inherit url;
            sha256 = hashes.${system};
            # fetchurl needs this so the store path carries the exec bit.
            executable = true;
          };
        in
        pkgs.runCommand "bepository-bin-${hashes.version}"
          {
            nativeBuildInputs = [ pkgs.makeWrapper ];
            meta.mainProgram = "bepository";
          }
          ''
            mkdir -p "$out/bin" "$out/lib/systemd/system"
            # makeWrapper needs the interpreter to resolve; the static musl
            # binary has none, so we can copy it directly and wrap by exec.
            cp "${raw}" "$out/bin/bepository"
            chmod 0755 "$out/bin/bepository"
            wrapProgram "$out/bin/bepository" \
              --set BEPOSITORY_PACKAGE_MANAGED "update via 'nix flake update'"
            # Install the canonical unit so the NixOS module can consume it via
            # systemd.packages — same file install-service emits, byte-identical.
            cp "${./bepository-cli/units/bepository.service}" \
               "$out/lib/systemd/system/bepository.service"
            chmod 0644 "$out/lib/systemd/system/bepository.service"
          '';
    in
    {
      packages = forAll (system:
        let pkgs = nixpkgs.legacyPackages.${system}; in
        {
          default = self.packages.${system}.bepository-bin;
          bepository-bin = bepositoryBinFor pkgs system;
        });

      nixosModules.default = { pkgs, lib, ... }: {
        imports = [ ./nix/module.nix ];
        # Default the package to the prebuilt release binary. Users can override
        # services.bepository.package to build from source (e.g. via nix/dev).
        config.services.bepository.package =
          lib.mkDefault self.packages.${pkgs.system}.bepository-bin;
      };

      nixosModules.bepository = self.nixosModules.default;
    };
}
