# Resource budget

Estimates; replaced by measurements in M1.

## Assumptions

| Source | Raw per day |
|---|---|
| internal resolver, per-query logs (50 000 × 250 B) | 12.5 MB |
| public authoritative, per-query logs (30 000 × 250 B) | 7.5 MB |
| reverse proxy, per-request logs (10 000 × 350 B) | 3.5 MB |
| other journald units, three nodes | 15 MB |
| **total** | **≈ 40 MB** |

Compression: open day (one frame per commit) ≈ 5×, sealed day ≈ 10×.

## Disk

| | Per day | 30 days | 90 days | 400 days (default retention) |
|---|---|---|---|---|
| sealed | 4 MB | 0.12 GB | 0.36 GB | 1.6 GB |

Plus the open day (≈ 8 MB), manifests (negligible), and the free-space reserve (2 GiB).

## Memory and CPU

`nisshi serve` ≤ 64 MiB RSS (`GOMEMLIMIT=64MiB`, `MemoryMax=128M`). CPU idle except the
nightly seal (seconds).
