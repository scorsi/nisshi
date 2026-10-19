{ lib, buildGoModule }:
buildGoModule (finalAttrs: {
  pname = "nisshi";
  version = "0.1.0-dev";

  # cmd/nisshi-testpush is test-only (ADR 0011): nix/vm-tests.nix builds it with subPackages.
  subPackages = [ "cmd/nisshi" ];

  # Only what the Go build and its tests read.
  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      ../go.mod
      ../go.sum
      (lib.fileset.fileFilter (f: f.hasExt "go") ../.)
      ../fixtures/logs
      ../docs/log-schema.md
    ];
  };

  # Changes whenever go.sum does: set to lib.fakeHash and read the new value from the build error.
  vendorHash = "sha256-bKtIfjtfZ8OnzM7uaOPMZe4r0sj3ma0sucvGNlLEqzc=";

  env.CGO_ENABLED = 0;
  ldflags = [
    "-s"
    "-w"
    "-X nisshi/internal/serve.Version=${finalAttrs.version}"
  ];

  # `nix flake check` runs this phase: vet first, then the default `go test ./...`.
  preCheck = ''
    go vet ./...
  '';

  meta = {
    description = "Log store of the observability stack";
    homepage = "https://github.com/scorsi/nisshi";
    mainProgram = "nisshi";
  };
})
