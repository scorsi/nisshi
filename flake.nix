{
  description = "nisshi (日誌, the logbook) — log store of the observability stack: schema, validator, store";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-26.05-darwin";

    flake-parts = {
      url = "github:hercules-ci/flake-parts";
      inputs.nixpkgs-lib.follows = "nixpkgs";
    };

    treefmt-nix = {
      url = "github:numtide/treefmt-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    crane.url = "github:ipetkov/crane";
  };

  outputs =
    inputs@{ self, flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "aarch64-darwin"
        "aarch64-linux"
        "x86_64-linux"
      ];

      imports = [ inputs.treefmt-nix.flakeModule ];

      flake = {
        lib = {
          # Path of the nisshi binary per system, for other repositories' tests (ADR 0004):
          # `journalctl -u <unit> -o cat | ${inputs.nisshi.lib.validateLogs.${pkgs.stdenv.hostPlatform.system}} validate --logs`.
          validateLogs = builtins.mapAttrs (_: p: "${p.nisshi}/bin/nisshi") self.packages;
          # fixtures/logs (ADR 0004) and fixtures/delivery (ADR 0012).
          fixtures = ./fixtures;
        };

        nixosModules.default = import ./nix/module.nix self;
      };

      perSystem =
        { pkgs, lib, ... }:
        let
          craneLib = inputs.crane.mkLib pkgs;
          nisshi = pkgs.callPackage ./nix/package.nix { inherit craneLib; };
        in
        {
          packages = {
            inherit nisshi;
            default = nisshi;
          };

          checks = import ./nix/checks.nix {
            inherit
              self
              pkgs
              lib
              nisshi
              craneLib
              ;
          };

          devShells.default = pkgs.mkShellNoCC {
            packages = with pkgs; [
              cargo
              rustc
              rust-analyzer
              clippy
              rustfmt
              buf
              protobuf
              jq
              zstd
            ];
            PROTOC = "${pkgs.protobuf}/bin/protoc";
          };

          treefmt = {
            projectRootFile = "flake.nix";
            programs = {
              nixfmt.enable = true;
              rustfmt.enable = true;
            };
            settings.excludes = [
              "target/**"
              "Cargo.lock"
              "fixtures/**"
            ];
          };
        };
    };
}
