# M0: installs the package only. The `services.nisshi` server module is M1.
self:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.nisshi;
in
{
  options.services.nisshi = {
    enable = lib.mkEnableOption "nisshi, the log store (M0: the `nisshi validate` command only)";
    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.nisshi;
      defaultText = lib.literalExpression "nisshi.packages.\${system}.nisshi";
      description = "The nisshi package.";
    };
  };

  config = lib.mkIf cfg.enable {
    environment.systemPackages = [ cfg.package ];
  };
}
