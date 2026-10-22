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

`GOMEMLIMIT` is a GC target, not a ceiling: a push needs its decompressed body in memory. One
push at a time is processed from body read to commit (others wait, unread, in their
connection); records alias the decompressed body and are streamed into the frame, and at most 256 bytes
of an agent status' `version` are copied out of it, so the worst case is about one
decompressed body plus the encoder. Max RSS of `nisshi serve` with
`GOMEMLIMIT=64MiB`, one push, measured on macOS arm64 on 2026-10-22 (before → after the review
fixes):

| Push | Before | After |
|---|---|---|
| 15 records of 4 MiB (59 MiB decompressed) | 264 MiB | 97 MiB |
| 400 000 small records (63 MiB decompressed) | 356 MiB | 117 MiB |
| 8 Mi empty records (16 MiB decompressed, invalid) | 744 MiB | 77 MiB |

The 64 MiB batch fits `MemoryMax=128M` with about 10 % margin; hikyaku's batches (4 MiB) are
far below. A measurement under systemd on the deployed node is still due (M1).
