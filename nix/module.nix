{ config, lib, ... }:

let
  cfg = config.services.bepository;
in
{
  options.services.bepository = {
    enable = lib.mkEnableOption "bepository cold-storage bridge daemon";

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
        <filename>/etc/bepository/sa-key.json</filename>, or extending
        <option>environment.etc."bepository/env".text</option>). The container
        bind-mounts that directory read-only.
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

    port = lib.mkOption {
      type = lib.types.port;
      default = 22001;
      description = ''
        Host port to expose the BEP listener on.  The container listens on
        0.0.0.0 inside its own network namespace and the port is published
        to the host via Podman.
      '';
    };

    enableCache = lib.mkOption {
      type = lib.types.bool;
      default = true;
      description = ''
        Whether to use the Foyer hybrid disk cache (mounted at
        <filename>/var/cache/bepository</filename> via Quadlet's
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
        sops-nix). Do <emphasis>not</emphasis> put secret values here directly —
        they would land in the Nix store.
      '';
    };
  };

  config = lib.mkIf cfg.enable {
    assertions = [{
      assertion = config.virtualisation.podman.enable;
      message = "services.bepository requires virtualisation.podman.enable = true (Podman 4.4+ for Quadlet support).";
    }];

    environment.etc."containers/systemd/bepository.container".source =
      ../deploy/bepository.container;

    environment.etc."bepository/env".text =
      ''
        BEPOSITORY_STORAGE_URI=${cfg.storageUri}
        BEPOSITORY_MASTER_DEVICE_ID=${cfg.masterDeviceId}
        BEPOSITORY_PORT=${toString cfg.port}
        BEPOSITORY_PRIORITY=${toString cfg.priority}
        BEPOSITORY_LEASE=${toString cfg.lease}
        ${lib.optionalString (!cfg.enableCache) "BEPOSITORY_NO_CACHE=1"}
      ''
      + lib.concatStringsSep "\n"
        (lib.mapAttrsToList (k: v: "${k}=${v}") cfg.extraEnv)
      + "\n";
  };
}
