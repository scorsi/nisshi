# nisshi — notes for AI assistants

Read [`docs/adr/`](docs/adr/) before making an architectural choice. [`docs/milestones.md`](docs/milestones.md)
is the plan: its acceptance criteria are the definition of done for a milestone.

## Mirrored with masu (`~/repositories/masu`)

These files exist, byte-identical, in both repositories (nisshi ADR 0012). Neither repository
depends on the other, so nothing enforces it but this rule:

- `fixtures/delivery/` (every file)
- `docs/delivery-scenarios.md`

A change to one of them is made in **both** repositories, together with the store-side change
it implies (Rust in both), before either is committed. Check with
`sha256sum fixtures/delivery/* docs/delivery-scenarios.md` in both: same output.

The delivery semantics themselves (ADR 0003: streams, `hwm`, the response table,
`PushRejected`, `recv_ts`) mirror masu ADR 0003; a change to one is proposed for the other.

## Contracts owned here

- `docs/log-schema.md` and `fixtures/logs/` are the log rules; the fixtures are the executable
  definition. A rule change is a fixture change first. hikyaku runs these fixtures too: a
  change here is a change to hikyaku's validator (`~/repositories/hikyaku`).
- `proto/nisshi/v1/` is the ingestion API; hikyaku compiles it.
- `docs/storage-format.md` is a public contract (kenbun, yagura, hanshou read it): a change is
  a new format version, never an in-place change.

## Conventions

- Dependencies limited to ADR 0015 (the register); any other crate is an ADR.
- Binaries (ADR 0016): `nisshi-core` (library), `nisshi` (data CLI), `nisshi-store`
  (datastore). `nisshi-query` is reserved for the future query daemon.
- Check with `nix develop -c cargo test`, `nix develop -c cargo clippy --all-targets -- -D warnings`.
- Format with `nix fmt`.
- Public repository: no host names, domains, addresses or keys of any deployment.
- Code, comments, docs and commits in English.
