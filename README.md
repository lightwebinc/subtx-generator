# bitcoin-subtx-generator

[![CI](https://github.com/lightwebinc/bitcoin-subtx-generator/actions/workflows/ci.yml/badge.svg)](https://github.com/lightwebinc/bitcoin-subtx-generator/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/lightwebinc/bitcoin-subtx-generator.svg)](https://pkg.go.dev/github.com/lightwebinc/bitcoin-subtx-generator)
[![Go Report Card](https://goreportcard.com/badge/github.com/lightwebinc/bitcoin-subtx-generator)](https://goreportcard.com/report/github.com/lightwebinc/bitcoin-subtx-generator)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Random BSV-over-UDP frame generator for load and functional testing of
[`bitcoin-shard-proxy`](https://github.com/lightwebinc/bitcoin-shard-proxy)
and [`bitcoin-shard-listener`](https://github.com/lightwebinc/bitcoin-shard-listener).

Supports v1 (44-byte header) and BRC-124/v2 (92-byte header, with
`HashKey`, `SeqNum`, `SubtreeID`) frame formats and is designed for
multi-core line-rate emission. Note: `HashKey` and `SeqNum` are emitted
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
  -subtree-seed 'multicast-lab-bsv' \
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

### BRC-127 SubtreeAnnounce sender

```bash
# Connect to the proxy TCP ingress and periodically announce all subtree IDs
# in the pool to the CtrlGroupSubtreeAnnounce control-plane multicast group.
subtx-gen \
  -addr [fd20::2]:9000 \
  -subtrees 8 \
  -subtree-seed 'multicast-lab-bsv' \
  -subtree-group bfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbf \
  -announce-addr [fd20::2]:9002 \
  -announce-interval 10s \
  -announce-ttl 0 \
  -pps 1000 -duration 30s
```

#### Phased mode — time-varying group membership

Set `-announce-phase-size` and `-announce-phase-interval` to add subtrees to
the group incrementally. The sender starts with zero active subtrees and adds
`phase-size` more every `phase-interval`, up to the full pool. The re-announce
ticker (`-announce-interval`) continues to fire to refresh TTLs of already-active
subtrees. This produces a visible ramp in dashboard time-series and is used by
[scenario 21](https://github.com/lightwebinc/bitcoin-multicast-test/tree/main/vm-lab/scenarios/21-subtree-group-ramp).

```bash
# Announce 1 new subtree every 75s (8 subtrees → full coverage after ~10 min).
# Re-announce every 12s to keep TTL=90s entries alive.
subtx-gen \
  -addr [fd20::2]:9000 \
  -subtrees 8 \
  -subtree-seed 'multicast-lab-bsv' \
  -subtree-group bfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbf \
  -announce-addr [fd20::2]:9002 \
  -announce-interval 12s \
  -announce-ttl 90 \
  -announce-phase-size 1 \
  -announce-phase-interval 75s \
  -pps 1000 -duration 12m
```

| Flag | Default | Description |
|------|---------|-------------|
| `-subtree-group` | | Comma-separated 32-char hex GroupIDs to announce |
| `-announce-addr` | | Proxy TCP address for SubtreeAnnounce (empty = disabled) |
| `-announce-interval` | `10s` | Re-announce period (TTL refresh for active subtrees) |
| `-announce-ttl` | `0` | TTL field in datagram; 0 = use listener default |
| `-announce-phase-size` | `0` | Subtrees to add per phase tick; 0 = announce full pool immediately |
| `-announce-phase-interval` | `0` | How often to advance the phase; 0 = phased mode disabled |

### Inspect the generated subtree pool

```bash
subtx-gen -subtrees 8 -subtree-seed 'multicast-lab-bsv' -print-subtrees
```

## Layout

```
cmd/subtx-gen/            — CLI entry point (BRC-124/128 frame generator)
cmd/send-block-announce/  — BRC-131 block announce sender (TCP)
cmd/send-subtree-data/    — BRC-132 subtree data sender (TCP)
cmd/send-anchor-frame/    — BRC-134 anchor transaction sender (TCP)
internal/tx/              — random BSV-shaped tx payload builder
internal/subtree/         — deterministic subtree-ID pool
internal/seq/             — shared seq allocator + gap injector
internal/frame/           — v1/v2 encoder wrapper around bitcoin-shard-common
internal/rate/            — token-bucket pacer (smooth / burst)
internal/sender/          — worker pool driving net.UDPConn per worker
internal/announce/        — BRC-127 SubtreeAnnounce TCP sender
```

See [docs/architecture.md](docs/architecture.md) and [docs/configuration.md](docs/configuration.md) for detailed documentation.

## Container image

The Dockerfile produces a single `gcr.io/distroless/static:nonroot` image
containing all four binaries:

```
/usr/local/bin/subtx-gen             (continuous BRC-124/BRC-128 frame generator)
/usr/local/bin/send-anchor-frame     (one-shot BRC-134 anchor)
/usr/local/bin/send-block-announce   (one-shot BRC-131 announce)
/usr/local/bin/send-subtree-data     (one-shot BRC-127 subtree-data)
```

**No `ENTRYPOINT` is set** — the consumer (Helm chart `mode` selector,
`docker run --entrypoint=…`, Kubernetes `command:` field) picks which binary
to invoke. Running the image without an explicit entrypoint will fail. The
[`bitcoin-subtx-generator-helm`](https://github.com/lightwebinc/bitcoin-subtx-generator-helm)
chart automates this via `.Values.mode`.

## Helm chart

A Kubernetes Helm chart is published from a dedicated chart repository:

- Repository: [`lightwebinc/bitcoin-subtx-generator-helm`](https://github.com/lightwebinc/bitcoin-subtx-generator-helm)
- HTTPS:
  ```
  helm repo add bsg https://lightwebinc.github.io/bitcoin-subtx-generator-helm
  helm install gen bsg/bitcoin-subtx-generator --set mode=subtx-gen
  ```
- OCI: `helm install gen oci://ghcr.io/lightwebinc/charts/bitcoin-subtx-generator --version 0.1.0`

The chart packages a single multi-binary image and selects which binary to run via `.Values.mode` (`subtx-gen` | `send-anchor-frame` | `send-block-announce` | `send-subtree-data`). Because these binaries accept **CLI flags only** (no env vars), the chart renders the matching per-mode `args` block into the container's `command` + `args`. Both `Deployment` and `Job` workload types are supported. See the chart README for the full reference.

## License

Apache 2.0 — see [LICENSE](LICENSE).
