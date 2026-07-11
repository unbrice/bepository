# SPDX-FileCopyrightText: 2026 Brice Arnould
#
# SPDX-License-Identifier: MIT OR Apache-2.0

# Flake-agnostic NixOS module for the bepository cold-storage bridge.
#
# The `package` option has no default here on purpose: a flake that imports this
# module is expected to set `services.bepository.package` (the root flake's
# `nixosModules.default` wires it to `self.packages.${system}.bepository-bin`,
# the prebuilt static release binary, via mkDefault).

{ config, lib, pkgs, ... }:

let
  cfg = config.services.bepository;
in
{
  options.services.bepository = {
    enable = lib.mkEnableOption "bepository cold-storage bridge daemon";

    package = lib.mkOption {
      type = lib.types.package;
      description = ''
        The bepository derivation to run. By default the importing flake
        supplies the prebuilt static release binary
        (<literal>bepository-bin</literal>); override this to build from source
        or pin a different version.
      '';
    };

    storageUri = lib.mkOption {
      type = lib.types.str;
      example = "s3://my-bucket/backup?region=us-east-1";
      description = ''
        URI for the SlateDB storage backend. The URI encodes both the location
        and non-secret configuration (region, GCS project, custom endpoint):
          file:///var/lib/bepository/store
          s3://bucket/prefix?region=eu-west-1
          s3://bucket/prefix?region=auto&endpoint=https://minio.example.com
          gs://bucket/prefix?project=my-gcp-project
        Credentials must be placed under <filename>/etc/bepository/</filename>
        out-of-band (e.g. via sops-nix dropping a service-account JSON at
        <filename>/etc/bepository/sa-key.json</filename>, or by extending
        <option>environment.etc."bepository/env".text</option>). The service
        reads that directory but cannot write to it.
      '';
    };

    masterDeviceId = lib.mkOption {
      type = lib.types.str;
      example = "XXXXXXX-XXXXXXX-XXXXXXX-XXXXXXX-XXXXXXX-XXXXXXX-XXXXXXX-XXXXXXX";
      description = ''
        Device ID of the master Syncthing peer that is allowed to connect to
        this bridge.  This is the remote Syncthing device, not bepository
        itself.  Obtain it from the peer's Syncthing web UI (Actions → Show ID)
        or by running `syncthing --device-id` on the peer machine.
      '';
    };

    listen = lib.mkOption {
      type = lib.types.str;
      default = "127.0.0.1:22001";
      example = "0.0.0.0:22001";
      description = ''
        Address for bepository to listen on for BEP connections. Defaults to
        loopback; set to <literal>0.0.0.0:22001</literal> to accept connections
        from other hosts.
      '';
    };

    enableCache = lib.mkOption {
      type = lib.types.bool;
      default = true;
      description = ''
        Whether to use the Foyer hybrid disk cache (at
        <filename>/var/cache/bepository</filename>, the service's
        <literal>CacheDirectory=</literal>).  Set to <literal>false</literal>
        to disable caching entirely (the module sets
        <literal>BEPOSITORY_NO_CACHE=1</literal>).
      '';
    };

    priority = lib.mkOption {
      type = lib.types.ints.unsigned;
      default = 100;
      description = "Distributed-lock priority (higher can preempt lower).";
    };

    lease = lib.mkOption {
      type = lib.types.ints.positive;
      default = 180;
      description = "Distributed-lock lease duration in seconds (minimum 180).";
    };

    extraEnv = lib.mkOption {
      type = lib.types.attrsOf lib.types.str;
      default = { };
      example = lib.literalExpression ''
        {
          GOOGLE_APPLICATION_CREDENTIALS = "/etc/bepository/sa-key.json";
        }
      '';
      description = ''
        Extra <literal>KEY=value</literal> pairs to append to
        <filename>/etc/bepository/env</filename>. Use for paths to credential
        files dropped under <filename>/etc/bepository/</filename> (e.g. via
        sops-nix).

        <emphasis role="strong">Warning:</emphasis> the generated env file is a
        world-readable symlink into the Nix store, so anything put here is
        readable by every user on the host. Reference credential files by path
        (dropped under <filename>/etc/bepository/</filename> out-of-band with a
        restricted mode, e.g. via sops-nix) rather than inlining secret values.
      '';
    };
  };

  config = lib.mkIf cfg.enable {
    # The EnvironmentFile the service reads. Generated from the options above.
    environment.etc."bepository/env".text =
      ''
        BEPOSITORY_STORAGE_URI=${cfg.storageUri}
        BEPOSITORY_MASTER_DEVICE_ID=${cfg.masterDeviceId}
        BEPOSITORY_LISTEN=${cfg.listen}
        BEPOSITORY_PRIORITY=${toString cfg.priority}
        BEPOSITORY_LEASE=${toString cfg.lease}
        ${lib.optionalString (!cfg.enableCache) "BEPOSITORY_NO_CACHE=1"}
      ''
      + lib.concatStringsSep "\n"
        (lib.mapAttrsToList (k: v: "${k}=${v}") cfg.extraEnv)
      + "\n";

    # Install the unit shipped in cfg.package ($out/lib/systemd/system/).
    # The unit itself is the canonical source (bepository-cli/units/), consumed
    # byte-identical by install-service and here — so the two installs cannot
    # drift on hardening keys, sleep.target coupling, etc.
    systemd.packages = [ cfg.package ];

    systemd.services.bepository = {
      # Packaged units are not auto-enabled; wire the default target.
      wantedBy = [ "multi-user.target" ];
      # ExecStart is `/usr/bin/env bepository serve` — put the wrapped binary
      # on the service's PATH so `env` resolves it.
      path = [ cfg.package ];
      # Restart whenever the generated env file changes (editing storageUri,
      # listen, extraEnv, etc.), since the unit file itself is stable.
      restartTriggers = [ config.environment.etc."bepository/env".text ];
    };

    # The service must not run an upgrade timer — nix owns updates
    # (`nix flake update` rebuilds the package).
  };
}
