# NixOS module for nisshi (milestone M1): renders the TOML of `nisshi serve` with
# pkgs.formats.toml and runs nisshi.service hardened as genkan's module (ADR 0003, 0006, 0008).
self:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  inherit (lib) mkOption types;
  cfg = config.services.nisshi;
  toml = pkgs.formats.toml { };
  credentials = "/run/credentials/nisshi.service";
  configFile = toml.generate "nisshi.toml" (
    {
      inherit (cfg) listen;
      data_dir = cfg.dataDir;
      tls = {
        cert_file = "${credentials}/cert.pem";
        key_file = "${credentials}/key.pem";
      };
      inherit (cfg) tokens;
      retention_days = if cfg.retentionDays == null then 0 else cfg.retentionDays;
      disk_reserve = cfg.diskReserve;
    }
    // lib.optionalAttrs (cfg.metrics.listen != null) { listen_metrics = cfg.metrics.listen; }
  );
in
{
  options.services.nisshi = {
    enable = lib.mkEnableOption "nisshi, the log store";

    package = mkOption {
      type = types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.nisshi;
      defaultText = lib.literalExpression "nisshi.packages.\${system}.nisshi";
      description = "The nisshi package.";
    };

    listen = mkOption {
      type = types.nonEmptyListOf types.str;
      example = [ "[2001:db8::10]:9473" ];
      description = "Go `host:port` addresses of the HTTPS ingestion API (ADR 0003). No firewall port is opened.";
    };

    dataDir = mkOption {
      type = types.path;
      default = "/var/lib/nisshi";
      description = "Store root (docs/storage-format.md). Keep it on persistent storage.";
    };

    tls = {
      certFile = mkOption {
        type = types.str;
        example = "/var/lib/hanko/nisshi/fullchain.pem";
        description = "PEM certificate chain, read through systemd `LoadCredential` (ADR 0008).";
      };
      keyFile = mkOption {
        type = types.str;
        example = "/var/lib/hanko/nisshi/key.pem";
        description = "PEM private key, read through systemd `LoadCredential`: a path, never a value.";
      };
    };

    tokens = mkOption {
      type = types.attrsOf (
        types.strMatching "^(node:[a-z0-9][a-z0-9-]{0,62}|reader)$"
        // {
          description = "\"node:<name>\" or \"reader\"";
        }
      );
      default = { };
      example = lib.literalExpression ''
        {
          "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08" = "node:node-a";
        }
      '';
      description = "Hex SHA-256 of each bearer token to its identity (ADR 0008). The hashes are not secrets.";
    };

    retentionDays = mkOption {
      type = types.nullOr types.ints.positive;
      default = 400;
      description = "Days after which the data file of a sealed day is deleted, its manifest kept (ADR 0006). `null` keeps everything.";
    };

    diskReserve = mkOption {
      type = types.ints.unsigned;
      default = 2 * 1024 * 1024 * 1024;
      description = "Bytes of free space below which pushes get 507 (ADR 0006).";
    };

    metrics.listen = mkOption {
      type = types.nullOr types.str;
      default = null;
      example = "127.0.0.1:9474";
      description = "Loopback `host:port` of the plain-HTTP `/metrics` listener (masu's metric convention); `null` disables it.";
    };
  };

  config = lib.mkIf cfg.enable {
    assertions = [
      {
        assertion =
          cfg.tokens != { }
          -> lib.all (k: builtins.match "[0-9a-f]{64}" k != null) (lib.attrNames cfg.tokens);
        message = "services.nisshi.tokens: keys are lowercase hex SHA-256 digests.";
      }
    ];

    environment.systemPackages = [ cfg.package ];

    users.users.nisshi = {
      isSystemUser = true;
      group = "nisshi";
    };
    users.groups.nisshi = { };

    systemd.tmpfiles.rules = [ "d ${cfg.dataDir} 0750 nisshi nisshi -" ];

    systemd.services.nisshi = {
      description = "nisshi log store";
      wantedBy = [ "multi-user.target" ];
      wants = [ "network-online.target" ];
      after = [ "network-online.target" ];
      environment.GOMEMLIMIT = "64MiB";
      serviceConfig = {
        Type = "notify";
        ExecStart = "${lib.getExe cfg.package} serve ${configFile}";
        Restart = "on-failure";
        User = "nisshi";
        Group = "nisshi";
        LoadCredential = [
          "cert.pem:${cfg.tls.certFile}"
          "key.pem:${cfg.tls.keyFile}"
        ];
        ReadWritePaths = [ cfg.dataDir ];
        MemoryMax = "128M";
        NoNewPrivileges = true;
        ProtectSystem = "strict";
        ProtectHome = true;
        PrivateTmp = true;
        PrivateDevices = true;
        RestrictAddressFamilies = [
          "AF_INET"
          "AF_INET6"
          "AF_UNIX"
        ];
        LimitNOFILE = 65536;
      };
    };
  };
}
