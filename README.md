# bitcoin-subtx-generator

Random BSV-over-UDP frame generator for load and functional testing of
[`bitcoin-shard-proxy`](https://github.com/lightwebinc/bitcoin-shard-proxy)
and [`bitcoin-shard-listener`](https://github.com/lightwebinc/bitcoin-shard-listener).

Supports v1 (44-byte header) and BRC-124/v2 (92-byte header, with
`PrevSeq`, `CurSeq`, `SubtreeID`) frame formats and is designed for
multi-core line-rate emission. Note: `PrevSeq` and `CurSeq` are emitted
as zero; the proxy stamps them in-place before multicast forwarding.

## Features

- **Random BSV-shaped tx payloads** — shape-correct (version / vin / vout /
  locktime), seeded per worker, no shared PRNG contention.
- **Subtree ID pool** — N deterministic 32-byte IDs derived from a user
  seed. Same seed ⇒ same IDs across runs, machines, and test scenarios.
- **Sequence numbers** — shared atomic allocator with optional gap
  injection (permanent or delayed retransmission) to drive listener-side
  NACK / retry tests.
- **Multi-core sender** — one UDP conn per worker, lock-free hot path,
  token-bucket pacer (smooth at ≤ 1 kpps, burst mode above).
- **Deterministic Subtree pick** — `SubtreeID = pool[uint64(TxID[:8]) % N]`
  so listeners filtering on a single subtree see a predictable traffic
  fraction (≈ `1/N`).

## Install

```bash
go install github.com/lightwebinc/bitcoin-subtx-generator/cmd/subtx-gen@latest
```

Or local build:

```bash
make build           # produces ./subtx-gen
make install-source  # lxc file push to the `source` LXD VM
```

## Usage

```bash
subtx-gen \
  -addr [fd20::2]:9000 \
  -frame-version 2 \
  -shard-bits 2 \
  -subtrees 8 \
  -subtree-seed 'lax-lab-2026' \
  -pps 1000 \
  -duration 10s \
  -payload-size 512 \
  -workers 0
```

### Gap injection (NACK / retransmit tests)

```bash
# Permanent gap — every 500th seq number is skipped; listener reports
# bsl_gaps_detected_total and (after NACK retries exhausted) bsl_nacks_unrecovered_total.
subtx-gen -pps 1000 -duration 30s -seq-gap-every 500

# Delayed retransmit — listener sees a gap, emits a NACK, and the
# generator resends the missing seq 50 ms later so bsl_gaps_suppressed_total
# (or forwarded-after-recovery) should rise.
subtx-gen -pps 1000 -duration 30s -seq-gap-every 500 -seq-gap-delay 50ms
```

### Inspect the generated subtree pool

```bash
subtx-gen -subtrees 8 -subtree-seed 'lax-lab-2026' -print-subtrees
```

## Layout

```
cmd/subtx-gen/        — CLI entrypoint
internal/tx/          — random BSV-shaped tx payload builder
internal/subtree/     — deterministic subtree-ID pool
internal/seq/         — shared seq allocator + gap injector
internal/frame/       — v1/v2 encoder wrapper around bitcoin-shard-common
internal/rate/        — token-bucket pacer (smooth / burst)
internal/sender/      — worker pool driving net.UDPConn per worker
```

## License

Apache 2.0 — see [LICENSE](LICENSE).
