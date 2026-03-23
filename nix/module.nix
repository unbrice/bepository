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
        Credentials are supplied separately via <option>environmentFile</option>.
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
        to disable caching entirely (the module passes
        <literal>--no-cache</literal> to the binary via
        <literal>EXTRA_ARGS</literal>).
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

    environmentFile = lib.mkOption {
      type = lib.types.nullOr lib.types.path;
      default = null;
      example = "/run/secrets/bepository.env";
      description = ''
        Path to a file containing credential environment variables.  The file
        is symlinked into <filename>/etc/bepository/credentials</filename>
        so the secret never lands in the Nix store.
        Each line must be in the form VAR=value.
        AWS:  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN
        GCS:  CLOUDSDK_AUTH_ACCESS_TOKEN  (short-lived; from gcloud auth print-access-token)
              GOOGLE_SERVICE_ACCOUNT_KEY  (JSON content of a service-account key file)
              GOOGLE_APPLICATION_CREDENTIALS  (path to a service-account key file on disk)
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

    environment.etc."bepository/env".text = ''
      STORAGE_URI=${cfg.storageUri}
      MASTER_DEVICE_ID=${cfg.masterDeviceId}
      LISTEN_PORT=${toString cfg.port}
      PRIORITY=${toString cfg.priority}
      LEASE=${toString cfg.lease}
      ${lib.optionalString (!cfg.enableCache) "EXTRA_ARGS=--no-cache"}
    '';

    systemd.tmpfiles.settings = lib.mkIf (cfg.environmentFile != null) {
      "10-bepository"."/etc/bepository/credentials" = {
        "L+".argument = toString cfg.environmentFile;
      };
    };
  };
}
