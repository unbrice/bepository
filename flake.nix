# SPDX-FileCopyrightText: 2026 Brice Arnould
#
# SPDX-License-Identifier: MIT OR Apache-2.0

{
  description = "bepository: NixOS module for the Syncthing cold-storage bridge";

  # End-user flake.  The contributor flake at nix/dev/ provides the dev shell,
  # OCI image build, and `nix flake check` — invoke it with `./nix/dev`.
  inputs.nixpkgs.url = "nixpkgs";

  outputs = { self, nixpkgs, ... }: {
    nixosModules.default = import ./nix/module.nix;
    nixosModules.bepository = import ./nix/module.nix;
  };
}
