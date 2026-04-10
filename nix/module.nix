{ config, lib, pkgs, ... }:
let
  cfg = config.services.meridian;

  # Select the correct binary variant at NixOS system build time based on
  # the storage backend and cluster options. Users can override via cfg.package.
  meridianPackage =
    if cfg.package != null then
      cfg.package
    else if cfg.cluster.enable && cfg.storage == "postgres" then
      pkgs.meridian-pg-sync
    else if cfg.cluster.enable then
      pkgs.meridian-cluster
    else if cfg.storage == "postgres" then
      pkgs.meridian-postgres
    else
      pkgs.meridian;

  # Extract the port number from the bind string "host:port" for firewall rules.
  bindPort = lib.toInt (lib.last (lib.splitString ":" cfg.bind));
in
{
  options.services.meridian = {
    enable = lib.mkEnableOption "Meridian CRDT sync engine";

    package = lib.mkOption {
      type        = lib.types.nullOr lib.types.package;
      default     = null;
      description = ''
        Override the Meridian binary package. When null (default), the module
        automatically selects the variant matching the storage and cluster settings.
        Most users should leave this unset.
      '';
    };

    # ── Secrets ────────────────────────────────────────────────────────────────
    signingKeyFile = lib.mkOption {
      type        = lib.types.path;
      description = ''
        Path to a file containing the signing key in systemd EnvironmentFile format:

          MERIDIAN_SIGNING_KEY=<32-byte hex string>

        Generate a key:
          openssl rand -hex 32 | install -m 0400 /dev/stdin /etc/meridian/signing-key

        This file must NOT be in the Nix store. Manage it with agenix, sops-nix,
        or a manual activation script. It is read by systemd before the process
        starts and is never exposed to other users.

        Example with agenix:
          age.secrets.meridian-key.file = ./secrets/meridian-key.age;
          services.meridian.signingKeyFile = config.age.secrets.meridian-key.path;

        Example with sops-nix:
          sops.secrets.meridian-key = { sopsFile = ./secrets/meridian.yaml; format = "dotenv"; };
          services.meridian.signingKeyFile = config.sops.secrets.meridian-key.path;
      '';
    };

    extraSecretEnvironmentFile = lib.mkOption {
      type        = lib.types.nullOr lib.types.path;
      default     = null;
      description = ''
        Optional path to a second EnvironmentFile for additional secrets such as
        DATABASE_URL, REDIS_URL, or MERIDIAN_WEBHOOK_SECRET. Same format as
        signingKeyFile. Variables in this file override those in signingKeyFile.
      '';
    };

    # ── Network ─────────────────────────────────────────────────────────────────
    bind = lib.mkOption {
      type        = lib.types.str;
      default     = "0.0.0.0:3000";
      example     = "127.0.0.1:3000";
      description = "Bind address and port for the HTTP/WebSocket server (MERIDIAN_BIND).";
    };

    openFirewall = lib.mkOption {
      type        = lib.types.bool;
      default     = false;
      description = "Open the TCP port extracted from the bind address in the firewall.";
    };

    # ── Storage ──────────────────────────────────────────────────────────────────
    dataDir = lib.mkOption {
      type        = lib.types.str;
      default     = "/var/lib/meridian";
      description = ''
        Data directory for sled storage and WAL files (MERIDIAN_DATA_DIR).
        Must be under /var/lib/ — systemd's StateDirectory= creates it and sets
        ownership to the DynamicUser allocated for this service.
      '';
    };

    storage = lib.mkOption {
      type        = lib.types.enum [ "sled" "postgres" "redis" ];
      default     = "sled";
      description = ''
        Storage backend:
        - "sled"     — embedded key-value store, no external services required.
        - "postgres" — PostgreSQL via DATABASE_URL. Requires databaseUrl.
        - "redis"    — Redis via REDIS_URL. Requires redisUrl.
      '';
    };

    databaseUrl = lib.mkOption {
      type        = lib.types.nullOr lib.types.str;
      default     = null;
      example     = "postgresql://meridian:secret@localhost/meridian";
      description = ''
        PostgreSQL connection URL (DATABASE_URL). Required when storage = "postgres".
        For security, prefer passing this via extraSecretEnvironmentFile so the
        value does not appear in the Nix store.
      '';
    };

    redisUrl = lib.mkOption {
      type        = lib.types.nullOr lib.types.str;
      default     = null;
      example     = "redis://localhost:6379";
      description = ''
        Redis connection URL (REDIS_URL). Required when storage = "redis" or
        cluster.enable = true.
      '';
    };

    # ── Cluster ──────────────────────────────────────────────────────────────────
    cluster = {
      enable = lib.mkEnableOption "multi-node Redis pub/sub clustering";

      nodeId = lib.mkOption {
        type        = lib.types.nullOr lib.types.str;
        default     = null;
        example     = "1";
        description = ''
          MERIDIAN_NODE_ID — unique integer identifier for this node in the cluster.
          When null, the server derives an ID from the hostname and port.
          Must be unique across all cluster nodes.
        '';
      };
    };

    # ── WAL ───────────────────────────────────────────────────────────────────────
    walConnstr = lib.mkOption {
      type        = lib.types.nullOr lib.types.str;
      default     = null;
      description = ''
        MERIDIAN_WAL_CONNSTR — PostgreSQL replication connection string for WAL
        payloads larger than 8 KB. Only relevant with the pg-sync binary variant.
      '';
    };

    # ── Escape hatches ────────────────────────────────────────────────────────────
    extraEnvironment = lib.mkOption {
      type    = lib.types.attrsOf lib.types.str;
      default = {};
      example = {
        RUST_LOG              = "meridian=debug,tower_http=info";
        MERIDIAN_WEBHOOK_URL  = "https://hooks.example.com/meridian";
        MERIDIAN_ANTI_ENTROPY_SECS = "30";
      };
      description = ''
        Additional non-secret environment variables passed to the service.
        These appear in the systemd unit file. For secrets use extraSecretEnvironmentFile.
      '';
    };
  };

  config = lib.mkIf cfg.enable {
    # ── Assertions ────────────────────────────────────────────────────────────────
    assertions = [
      {
        assertion = cfg.storage == "postgres" -> cfg.databaseUrl != null;
        message   = "services.meridian.databaseUrl must be set when storage = \"postgres\"";
      }
      {
        assertion = cfg.storage == "redis" -> cfg.redisUrl != null;
        message   = "services.meridian.redisUrl must be set when storage = \"redis\"";
      }
      {
        assertion = cfg.cluster.enable -> cfg.redisUrl != null;
        message   = "services.meridian.redisUrl must be set when cluster.enable = true";
      }
      {
        assertion = lib.hasPrefix "/var/lib/" cfg.dataDir;
        message   = "services.meridian.dataDir must be under /var/lib/ (required by DynamicUser + StateDirectory)";
      }
    ];

    # ── Firewall ──────────────────────────────────────────────────────────────────
    networking.firewall.allowedTCPPorts = lib.mkIf cfg.openFirewall [ bindPort ];

    # ── Systemd service ───────────────────────────────────────────────────────────
    systemd.services.meridian = {
      description = "Meridian CRDT sync engine";
      documentation = [ "https://github.com/Chahine-tech/meridian" ];
      wantedBy = [ "multi-user.target" ];
      after    = [ "network-online.target" ]
        ++ lib.optionals (cfg.storage == "postgres" || cfg.cluster.enable) [ "postgresql.service" ]
        ++ lib.optionals (cfg.storage == "redis"    || cfg.cluster.enable) [ "redis.service" ];
      requires = [ "network-online.target" ];

      serviceConfig = {
        ExecStart  = "${meridianPackage}/bin/meridian";
        Restart    = "on-failure";
        RestartSec = "5s";

        # DynamicUser allocates a transient UID/GID at service start.
        # StateDirectory creates /var/lib/meridian and chowns it to that UID
        # automatically — no manual adduser or chown activation scripts needed.
        DynamicUser        = true;
        StateDirectory     = "meridian";
        StateDirectoryMode = "0750";
        RuntimeDirectory     = "meridian";
        RuntimeDirectoryMode = "0750";

        # EnvironmentFile= is read by systemd (as root) before exec, then injected
        # into the process environment. The files are never in the Nix store.
        EnvironmentFile = [ cfg.signingKeyFile ]
          ++ lib.optional (cfg.extraSecretEnvironmentFile != null) cfg.extraSecretEnvironmentFile;

        # Non-secret environment built from module options.
        Environment = lib.mapAttrsToList (k: v: "${k}=${v}") (
          {
            MERIDIAN_BIND     = cfg.bind;
            MERIDIAN_DATA_DIR = cfg.dataDir;
          }
          // lib.optionalAttrs (cfg.databaseUrl != null)  { DATABASE_URL          = cfg.databaseUrl; }
          // lib.optionalAttrs (cfg.redisUrl != null)     { REDIS_URL             = cfg.redisUrl; }
          // lib.optionalAttrs (cfg.cluster.nodeId != null) { MERIDIAN_NODE_ID    = cfg.cluster.nodeId; }
          // lib.optionalAttrs (cfg.walConnstr != null)   { MERIDIAN_WAL_CONNSTR = cfg.walConnstr; }
          // cfg.extraEnvironment
        );

        # ── Filesystem hardening ──────────────────────────────────────────────
        ProtectSystem   = "strict";
        ProtectHome     = true;
        PrivateTmp      = true;
        PrivateDevices  = true;
        ReadWritePaths  = [ "/var/lib/meridian" ];

        # ── Capability: only grant NET_BIND_SERVICE for privileged ports (<1024)
        AmbientCapabilities = lib.mkIf (bindPort < 1024) [ "CAP_NET_BIND_SERVICE" ];

        # ── Process hardening ─────────────────────────────────────────────────
        NoNewPrivileges        = true;
        ProtectKernelTunables  = true;
        ProtectKernelModules   = true;
        ProtectKernelLogs      = true;
        ProtectControlGroups   = true;
        RestrictAddressFamilies = [ "AF_INET" "AF_INET6" "AF_UNIX" ];
        RestrictNamespaces     = true;
        RestrictRealtime       = true;
        RestrictSUIDSGID       = true;
        LockPersonality        = true;
        # Meridian is pure Rust with no JIT — safe to deny writable+executable pages.
        MemoryDenyWriteExecute = true;
        SystemCallFilter       = [ "@system-service" ];
        SystemCallArchitectures = "native";

        # WebSocket servers hold many connections open; increase the fd limit.
        LimitNOFILE = 65536;
      };
    };
  };
}
