# The nisshi workspace built by crane (ADR 0015): the data CLI `nisshi` and the datastore
# `nisshi-store` (ADR 0016) in one package. build.rs compiles proto/ with prost-build, which
# needs protoc. `passthru.testpush` is the test pusher of ADR 0011, for the nixosTests only.
{
  lib,
  protobuf,
  craneLib,
}:
let
  common = {
    pname = "nisshi";
    version = (lib.importTOML ../Cargo.toml).workspace.package.version;
    strictDeps = true;
    nativeBuildInputs = [ protobuf ];
    PROTOC = "${protobuf}/bin/protoc";
    src = lib.fileset.toSource {
      root = ../.;
      fileset = lib.fileset.unions [
        (craneLib.fileset.commonCargoSources ../.)
        ../proto
        # Tests read the fixtures and the schema's examples.
        ../fixtures
        ../docs/log-schema.md
      ];
    };
  };
  cargoArtifacts = craneLib.buildDepsOnly common;
in
craneLib.buildPackage (
  common
  // {
    inherit cargoArtifacts;
    cargoExtraArgs = "--locked -p nisshi -p nisshi-store";
    doCheck = false; # the tests run in checks.cargo-test
    meta = {
      description = "Log store of the observability stack";
      homepage = "https://github.com/scorsi/nisshi";
      mainProgram = "nisshi";
    };
    passthru = {
      inherit common cargoArtifacts;
      testpush = craneLib.buildPackage (
        common
        // {
          inherit cargoArtifacts;
          pname = "nisshi-testpush";
          cargoExtraArgs = "--locked -p nisshi-testpush";
          doCheck = false;
          meta.mainProgram = "nisshi-testpush";
        }
      );
    };
  }
)
