# `nix flake check` (ADR 0011): the Rust tests (every fixture line through the validator, every
# log-schema.md example a valid fixture, the store and server tests), clippy, the fixtures through
# the real binary, the schema, the scenarios' format, the module.
{
  self,
  pkgs,
  lib,
  nisshi,
  craneLib,
}:
let
  fixtures = ../fixtures;
in
{
  inherit nisshi;

  cargo-test = craneLib.cargoTest (
    nisshi.passthru.common // { inherit (nisshi.passthru) cargoArtifacts; }
  );
  cargo-clippy = craneLib.cargoClippy (
    nisshi.passthru.common
    // {
      inherit (nisshi.passthru) cargoArtifacts;
      cargoClippyExtraArgs = "--workspace --all-targets -- --deny warnings";
    }
  );

  buf-lint = pkgs.runCommand "nisshi-buf-lint" { nativeBuildInputs = [ pkgs.buf ]; } ''
    export HOME=$TMPDIR
    cd ${
      lib.fileset.toSource {
        root = ../.;
        fileset = lib.fileset.unions [
          ../buf.yaml
          ../proto
        ];
      }
    }
    buf lint
    touch $out
  '';

  # The real binary, through lib.validateLogs, `nisshi validate --logs` on every service-line fixture: verdict and reason.
  fixtures-cli =
    pkgs.runCommand "nisshi-fixtures-cli"
      {
        nativeBuildInputs = [ pkgs.jq ];
      }
      ''
        n=0
        while IFS= read -r fx; do
          n=$((n + 1))
          want=$(jq -r .reason <<<"$fx")
          if jq -e 'has("input_b64")' >/dev/null <<<"$fx"; then
            jq -j .input_b64 <<<"$fx" | base64 -d >in
          else
            jq -j 'if has("input") then .input else .input_repeat | .prefix + (.repeat * .count) + .suffix end' <<<"$fx" >in
          fi
          echo >>in # a line as journalctl -o cat prints it
          if ${
            self.lib.validateLogs.${pkgs.stdenv.hostPlatform.system}
          } validate --logs <in 2>err; then got=""; else got=$(sed -n 's/^line 1: //p' err); fi
          [ "$got" = "$want" ] || { echo "service-lines.ndjson:$n: got '$got', want '$want'"; cat err; exit 1; }
        done <${fixtures}/logs/service-lines.ndjson
        echo "$n service-line fixtures checked"
        touch $out
      '';

  # Valid fixtures satisfy schema/log-v1.json; invalid ones whose rule the schema can express
  # (missing or reserved field, err outside warn/error) do not.
  schema =
    pkgs.runCommand "nisshi-schema"
      {
        nativeBuildInputs = [ (pkgs.python3.withPackages (p: [ p.jsonschema ])) ];
      }
      ''
        python3 - ${../schema/log-v1.json} ${fixtures}/logs <<'EOF'
        import json, sys
        from jsonschema import Draft202012Validator as V
        schema = json.load(open(sys.argv[1]))
        V.check_schema(schema)
        for name, ref in (("service-lines", "#/$defs/serviceLine"), ("records", "#/$defs/record")):
            v = V({"$ref": ref, "$defs": schema["$defs"]})
            for i, line in enumerate(open(f"{sys.argv[2]}/{name}.ndjson"), 1):
                fx = json.loads(line)
                if "input" not in fx:
                    continue
                if fx["valid"]:
                    errs = list(v.iter_errors(json.loads(fx["input"])))
                    assert not errs, f"{name}:{i}: valid fixture rejected by the schema: {errs[0].message}"
                elif fx["reason"].startswith(("missing-field:", "reserved-field:", "err-format")):
                    assert not v.is_valid(json.loads(fx["input"])), f"{name}:{i}: schema accepts {fx['reason']}"
        EOF
        touch $out
      '';

  # Delivery scenarios use only the vocabulary of docs/delivery-scenarios.md.
  delivery-scenarios =
    pkgs.runCommand "nisshi-delivery-scenarios" { nativeBuildInputs = [ pkgs.jq ]; }
      ''
        for f in ${fixtures}/delivery/*.json; do
          jq -e --arg name "$(basename "$f" .json)" '
            def only($k): (keys - $k) == [];
            (only(["name", "steps"]) and .name == $name and (.steps | length > 0))
            and all(.steps[];
              (keys | length) as $n
              | if has("push") then
                  (.push | only(["first_seq", "count", "token", "node", "invalid_at", "stream"]) and has("first_seq") and has("count"))
                  and (.push.token // "valid" | IN("valid", "none", "unknown", "other-node"))
                  and (.push.node // "self" | IN("self", "other"))
                  and (.push.stream // "a" | IN("a", "b"))
                  and ($n == 1 or (has("expect") and $n == 2 and (.expect | only(["status", "acked_seq", "record_index", "reason"]) and has("status"))))
                elif has("check") then $n == 1 and (.check | only(["stored_seqs", "duplicates", "gaps"]))
                elif has("restart") then $n == 1 and .restart == {"kill": true}
                elif has("disk") then $n == 1 and (.disk == {"fill": true} or .disk == {"free": true})
                else false end)
          ' "$f" >/dev/null || { echo "$f: not in the format of docs/delivery-scenarios.md"; exit 1; }
        done
        touch $out
      '';

  # The module evaluates in a NixOS system (aarch64-linux when this system is Darwin).
  module =
    let
      nixos = lib.nixosSystem {
        modules = [
          self.nixosModules.default
          {
            nixpkgs.hostPlatform =
              if pkgs.stdenv.hostPlatform.isLinux then pkgs.stdenv.hostPlatform.system else "aarch64-linux";
            boot.loader.grub.enable = false;
            fileSystems."/" = {
              device = "none";
              fsType = "tmpfs";
            };
            system.stateVersion = "26.05";
            services.nisshi = {
              enable = true;
              listen = [ "[::1]:9473" ];
              tls = {
                certFile = "/var/lib/hanko/nisshi/fullchain.pem";
                keyFile = "/var/lib/hanko/nisshi/key.pem";
              };
              tokens.${builtins.hashString "sha256" "example"} = "node:node-a";
              retentionDays = null;
            };
          }
        ];
      };
    in
    assert lib.any (p: lib.getName p == "nisshi") nixos.config.environment.systemPackages;
    assert nixos.config.systemd.services.nisshi.serviceConfig.Type == "notify";
    pkgs.writeText "nisshi-module-eval" (
      builtins.unsafeDiscardStringContext nixos.config.system.build.toplevel.drvPath
    );
}
# The VM tests of nix/vm-tests.nix need a Linux builder; the orchestrator runs them.
// lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux (import ./vm-tests.nix self { inherit pkgs; })
