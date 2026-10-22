# Architecture Decision Records

MADR-lite: one decision per file, `NNNN-slug.md`, sections Context / Options considered /
Decision / Consequences. A decision is changed by a new ADR that supersedes the old one.

| ADR | Title |
|---|---|
| [0001](0001-scope.md) | Scope and boundaries |
| [0002](0002-language-and-dependencies.md) | Language and dependencies |
| [0003](0003-ingestion-api.md) | Ingestion API and delivery semantics |
| [0004](0004-log-schema.md) | Common log schema and its publication |
| [0005](0005-storage.md) | Storage: NDJSON segments by receive day |
| [0006](0006-sealing-and-retention.md) | Sealing, compaction and retention |
| [0007](0007-integrity.md) | Integrity: hash chain from v0 |
| [0008](0008-authentication.md) | Authentication before and after hanko |
| [0009](0009-service-log-output.md) | How services write logs |
| [0010](0010-read-and-status.md) | Reading and status |
| [0011](0011-testing.md) | Testing |
| [0012](0012-delivery-scenarios.md) | Delivery scenarios as shared data |
| [0013](0013-forbidden-for-wrong-role.md) | 403 for a valid token of the wrong role |
| [0014](0014-stream-owner.md) | A stream belongs to the node that first pushed it |
