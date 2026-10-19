# nixosTests of milestone M1 (ADR 0011): nisshi.service through the module, driven by the test
# pusher, with zstdcat, jq, sha256sum and curl as the oracle. Three tests so a failure is
# localised: delivery scenarios, record fixtures, seal/chain/retention/read with a faked clock.
self:
{ pkgs, ... }:
let
  inherit (pkgs) lib;
  nisshi = self.packages.${pkgs.stdenv.hostPlatform.system}.nisshi;
  testpush = nisshi.overrideAttrs {
    pname = "nisshi-testpush";
    subPackages = [ "cmd/nisshi-testpush" ];
    doCheck = false;
  };
  fixtures = self.lib.fixtures;

  certs = pkgs.runCommand "nisshi-test-certs" { nativeBuildInputs = [ pkgs.openssl ]; } ''
    mkdir $out && cd $out
    openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj /CN=nisshi-test \
      -addext subjectAltName=IP:127.0.0.1 -keyout server.key -out server.pem
  '';

  tokens = {
    node-a = "node-a-token";
    node-b = "node-b-token";
    reader = "reader-token";
  };
  sha = builtins.hashString "sha256";
  root = "/var/lib/nisshi";
  url = "https://127.0.0.1:9473";
  mib = 1024 * 1024;

  node =
    { retentionDays }:
    {
      imports = [ self.nixosModules.default ];

      # A small filesystem so the disk-full scenario can fill it (disk_reserve 8 MiB).
      fileSystems.${root} = {
        device = "tmpfs";
        fsType = "tmpfs";
        options = [
          "size=32m"
          "mode=0750"
        ];
      };

      services.nisshi = {
        enable = true;
        listen = [ "127.0.0.1:9473" ];
        tls = {
          certFile = "${certs}/server.pem";
          keyFile = "${certs}/server.key";
        };
        tokens = {
          ${sha tokens.node-a} = "node:node-a";
          ${sha tokens.node-b} = "node:node-b";
          ${sha tokens.reader} = "reader";
        };
        diskReserve = 8 * mib;
        metrics.listen = "127.0.0.1:9474";
        inherit retentionDays;
      };

      environment.systemPackages = [
        pkgs.zstd
        pkgs.jq
        pkgs.curl
        testpush
      ];
    };

  pusher = lib.concatStringsSep " " [
    "nisshi-testpush --url ${url} --ca ${certs}/server.pem --node node-a --other-node node-b"
    "--token-valid ${tokens.node-a} --token-other-node ${tokens.node-b} --root ${root}"
    "--restart 'systemctl kill -s KILL nisshi; systemctl start nisshi'"
    "--fill 'dd if=/dev/zero of=${root}/fill bs=1M count=28 status=none' --free 'rm ${root}/fill'"
  ];

  # Shared checks after a run: integrity, every file a valid zstd stream, the open day readable
  # with jq, the service's own journal lines in the log schema.
  common = ''
    def common_checks():
        machine.succeed("nisshi verify ${root}")
        machine.succeed("for f in ${root}/*/*.ndjson.zst; do zstd -t $f; done")
        machine.succeed("zstdcat ${root}/node-a/*.ndjson.zst | jq -c .ts >/dev/null")
        machine.succeed("journalctl _SYSTEMD_UNIT=nisshi.service -o cat | nisshi validate --logs")
        assert machine.succeed("systemctl show -P MemoryMax nisshi").strip() == str(128 * 1024 * 1024)
        machine.succeed("systemctl show -P Environment nisshi | grep -q GOMEMLIMIT=64MiB")

    def status():
        return machine.succeed("curl -sS --cacert ${certs}/server.pem -H 'Authorization: Bearer ${tokens.reader}' ${url}/v1/status")

    machine.wait_for_unit("nisshi.service")
    machine.wait_for_open_port(9473)
  '';

  mkTest =
    name: retentionDays: script:
    pkgs.testers.runNixOSTest {
      name = "nisshi-${name}";
      nodes.machine = node { inherit retentionDays; };
      testScript = common + script;
    };
in
{
  # Every scenario of fixtures/delivery/ (ADR 0012), then metrics and status.
  vm-delivery = mkTest "delivery" 400 ''
    with subtest("delivery scenarios"):
        print(machine.succeed("${pusher} ${fixtures}/delivery/*.json"))

    common_checks()

    with subtest("metrics on loopback, in the convention"):
        m = machine.succeed("curl -sS http://127.0.0.1:9474/metrics")
        for want in ['nisshi_records_total{source_node="node-a"} ', "nisshi_seq_gaps_total 1\n",
                     'nisshi_push_requests_total{code="507"} 1\n', 'nisshi_push_requests_total{code="401"} 2\n',
                     "nisshi_disk_free_bytes ", 'nisshi_build_info{version="${nisshi.version}"} 1\n']:
            assert want in m, f"missing {want!r} in\n{m}"
        bad = [l for l in m.splitlines() if l and not l.startswith("#") and not l.startswith("nisshi_")]
        assert not bad, bad

    with subtest("status for the reader token only"):
        s = status()
        machine.succeed(f"jq -e '(.streams | length) >= 10 and .diskReserveBytes == \"${toString (8 * mib)}\" and .chainHeads == {{}}' <<'EOF'\n{s}\nEOF")
        machine.fail("curl -sSf --cacert ${certs}/server.pem -H 'Authorization: Bearer ${tokens.node-a}' ${url}/v1/status")
        machine.fail("curl -sSf --cacert ${certs}/server.pem ${url}/v1/status")
  '';

  # Every record fixture through the ingestion path (ADR 0011): verdicts, reasons, index,
  # stored bytes.
  vm-fixtures = mkTest "fixtures" 400 ''
    with subtest("record fixtures through POST /v1/push"):
        print(machine.succeed("${pusher} --fixtures ${fixtures}/logs/records.ndjson"))

    common_checks()
  '';

  # Faked clock (NISSHI_FAKE_NOW): seal at start, manifest and chain checked with standard tools,
  # retention (2 days) keeping manifests, nisshi read across sealed and open days.
  vm-seal = mkTest "seal" 2 ''
    def day(d):
        machine.succeed(f"systemctl set-environment NISSHI_FAKE_NOW=2026-10-{d}T12:00:00Z && systemctl restart nisshi")
        machine.wait_for_open_port(9473)

    def push(scenario):
        print(machine.succeed("${pusher} ${fixtures}/delivery/" + scenario + ".json"))

    def sha256(path):
        return machine.succeed(f"sha256sum {path} | cut -d' ' -f1").strip()

    def manifest(d, expr):
        machine.succeed(f"jq -e '{expr}' ${root}/node-a/2026-10-{d}.manifest.json")

    day(19)
    push("in-order")
    assert "manifest" not in machine.succeed("ls ${root}/node-a"), "open day sealed"

    day(20)
    with subtest("first seal: manifest from the content, no previous link"):
        data = "${root}/node-a/2026-10-19.ndjson.zst"
        content = machine.succeed(f"zstdcat {data} | sha256sum | cut -d' ' -f1").strip()
        size = machine.succeed(f"zstdcat {data} | wc -c").strip()
        manifest(19, f'.v == 1 and .node == "node-a" and .day == "2026-10-19" and .records == 25 '
                     f'and .content_sha256 == "{content}" and .uncompressed_bytes == {size} '
                     f'and .prev_day == null and .prev_manifest_sha256 == null '
                     f'and .min_ts == "2026-10-19T08:00:01.000000000Z" and .max_ts == "2026-10-19T08:00:25.000000000Z" '
                     f'and .services == {{"annai": 25}} and (.streams | length) == 1 and .streams[0].first_seq == 1 and .streams[0].last_seq == 25 '
                     f'and (.sealed_at | startswith("2026-10-20T12:00:00"))')
        machine.succeed(f"zstdcat {data} | jq -c . | wc -l | grep -qx 25")
        before = sha256("${root}/node-a/2026-10-19.manifest.json")
        machine.succeed("systemctl restart nisshi")  # idempotent
        machine.wait_for_open_port(9473)
        assert sha256("${root}/node-a/2026-10-19.manifest.json") == before, "seal rewrote the manifest"
        machine.succeed("ls ${root}/node-a | grep -c manifest | grep -qx 1")
    push("two-streams")
    push("gap")

    day(21)
    with subtest("second seal links to the first manifest"):
        prev = sha256("${root}/node-a/2026-10-19.manifest.json")
        manifest(20, f'.prev_day == "2026-10-19" and .prev_manifest_sha256 == "{prev}" and .records == 46 '
                     f'and .services == {{"annai": 45, "nisshi": 1}} and (.streams | length) == 4')
        head = sha256("${root}/node-a/2026-10-20.manifest.json")
        machine.succeed(f"jq -e '.chainHeads == {{\"node-a\": \"{head}\"}}' <<'EOF'\n{status()}\nEOF")
        machine.succeed("zstdcat ${root}/node-a/2026-10-20.ndjson.zst | jq -c 'select(.event == \"seq-gap\")' | grep -q '\"service\":\"nisshi\"'")
    push("exact-duplicate")

    with subtest("nisshi read across sealed and open days, sorted by ts"):
        out = machine.succeed("nisshi read ${root} --node node-a --service annai --event query "
                              "--from 2026-10-19T07:00:00Z --to 2026-10-19T08:00:05Z")
        lines = out.strip().splitlines()
        assert len(lines) == 30, (len(lines), out)
        ts = [machine.succeed(f"jq -r .ts <<'EOF'\n{l}\nEOF").strip() for l in lines[:1] + lines[10:11] + lines[-1:]]
        assert ts == ["2026-10-19T07:00:01.000000000Z", "2026-10-19T08:00:01.000000000Z", "2026-10-19T08:00:05.000000000Z"], ts
        machine.succeed(f"jq -r .ts <<'EOF' | sort -c\n{out}\nEOF")
        only_b = machine.succeed("nisshi read ${root} --node node-a --from 2026-10-19T07:00:00Z --to 2026-10-19T07:59:59Z | wc -l").strip()
        assert only_b == "10", only_b

    day(22)
    with subtest("retention deletes data, keeps manifests, verify passes"):
        machine.succeed("test ! -e ${root}/node-a/2026-10-19.ndjson.zst")
        machine.succeed("test -e ${root}/node-a/2026-10-19.manifest.json")
        machine.succeed("test -e ${root}/node-a/2026-10-20.ndjson.zst")
        machine.succeed("ls ${root}/node-a | grep -c manifest | grep -qx 3")
        machine.succeed("nisshi read ${root} --node node-a --from 2026-10-19T07:00:00Z --to 2026-10-19T08:00:05Z | wc -l | grep -qx 25")
        m = machine.succeed("curl -sS http://127.0.0.1:9474/metrics")
        assert 'nisshi_seals_total{result="ok"} 1\n' in m, m  # counters restart with the process

    common_checks()

    with subtest("a tampered sealed day fails verify"):
        machine.succeed("systemctl stop nisshi")
        machine.succeed("printf x | zstd -q >> ${root}/node-a/2026-10-20.ndjson.zst")
        machine.fail("nisshi verify ${root}")
  '';
}
