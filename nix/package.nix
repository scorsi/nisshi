{ lib, buildGoModule }:
buildGoModule {
  pname = "nisshi";
  version = "0.1.0-dev";

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
  vendorHash = "sha256-aF4c3loEsXDF8dQt9b6bZ9JV4nTjVfKsU0Jyb/lq63M=";

  env.CGO_ENABLED = 0;
  ldflags = [
    "-s"
    "-w"
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
}
