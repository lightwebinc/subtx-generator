# subtx-generator

[![CI](https://github.com/lightwebinc/subtx-generator/actions/workflows/ci.yml/badge.svg)](https://github.com/lightwebinc/subtx-generator/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/lightwebinc/subtx-generator.svg)](https://pkg.go.dev/github.com/lightwebinc/subtx-generator)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

> Part of the [**BSV Layered Multicast**](https://github.com/lightwebinc/bsv-multicast) open-source project — see the main repository for the full architecture, design docs, and BRC specifications.

Random BSV-over-UDP frame generator for load and functional testing of
[`shard-proxy`](https://github.com/lightwebinc/shard-proxy)
and [`shard-listener`](https://github.com/lightwebinc/shard-listener).

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
- **Consumer tunnel sink** — `tunnel-sink` receives and logs the tunnel
  delivery lanes (tx / BRC-143 subtree / BRC-144 block) with per-object
  diagnostic lines and exit-time summary statistics; optional submit relay
  logs the sent direction too.
- **Unified structured logging** — uses `shard-common/logging` (no more plain
  `log`); set `LOG_FORMAT=json` for JSON-on-stdout matching the rest of the
  fleet. See the [Unified Logging Plan](https://github.com/lightwebinc/shard-common/blob/main/docs/logging.md).

## Install

```bash
go install github.com/lightwebinc/subtx-generator/cmd/subtx-gen@latest
```

Or local build:

```bash
make build           # builds every cmd/* binary into the repo root
make install-source  # lxc file push to the `source` LXD VM
```

## Usage

```bash
subtx-gen \
  -addr [fd20::2]:8725 \
  -frame-version 2 \
  -shard-bits 2 \
  -subtrees 8 \
  -subtree-seed 'multicast-lab-bsv' \
  -pps 1000 \
  -duration 10s \
  -payload-size 512 \
  -workers 0
```

### direct-multicast mode (skip the proxy)

```bash
# Emit directly to FF35::B:idx (SSM site scope) — useful for fabric
# load validation and SSM scenarios where the generator is the
# data-plane publisher.
# Operators MUST add -bind-source to the shard-manifest -publishers
# list so receivers' (S,G) joins include this generator.
subtx-gen \
  -mode direct-multicast \
  -bind-source fd20::abc \
  -egress-iface eth0 \
  -source-mode ssm \
  -scope site \
  -shard-bits 2 \
  -egress-port 9001 \
  -pps 1000 -duration 30s
```

See [bsv-multicast SSM Support Plan](https://github.com/lightwebinc/bsv-multicast/blob/main/DESIGN.md#source-specific-multicast-ssm)
for the full design.

### Gap injection (NACK / retransmit tests)

```bash
# Permanent gap — every 500th seq number is skipped; listener reports
# bsl_gaps_detected_total and (after NACK retries exhausted) bsl_gaps_unrecovered_total.
subtx-gen -pps 1000 -duration 30s -seq-gap-every 500

# Delayed retransmit — listener sees a gap, emits a NACK, and the
# generator resends the missing seq 50 ms later so bsl_gaps_suppressed_total
# (or forwarded-after-recovery) should rise.
subtx-gen -pps 1000 -duration 30s -seq-gap-every 500 -seq-gap-delay 50ms
```

### BRC-127 SubtreeGroupAnnounce sender

```bash
# Connect to the proxy TCP ingress and periodically announce all subtree IDs
# in the pool to the GroupSubtreeGroupAnnounce control-plane multicast group.
subtx-gen \
  -addr [fd20::2]:8725 \
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
scenario 21 in [multicast-test SCENARIOS.md](https://github.com/lightwebinc/multicast-test/blob/main/SCENARIOS.md).

```bash
# Announce 1 new subtree every 75s (8 subtrees → full coverage after ~10 min).
# Re-announce every 12s to keep TTL=90s entries alive.
subtx-gen \
  -addr [fd20::2]:8725 \
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
| `-announce-addr` | | Proxy TCP address for SubtreeGroupAnnounce (empty = disabled) |
| `-announce-interval` | `10s` | Re-announce period (TTL refresh for active subtrees) |
| `-announce-ttl` | `0` | TTL field in datagram; 0 = use listener default |
| `-announce-phase-size` | `0` | Subtrees to add per phase tick; 0 = announce full pool immediately |
| `-announce-phase-interval` | `0` | How often to advance the phase; 0 = phased mode disabled |

### Consumer tunnel sink (receive side)

`tunnel-sink` is the receiving-side counterpart to the senders above: a
consumer-side diagnostic sink for the tunnel delivery plane. It listens on the
consumer's SDA (default `:8833`), auto-detects each connection's lane class
(raw/EF tx, BRC-143 subtree, BRC-144 block — or a framed BRC-124 stream by
network magic), and logs one line per object with timestamp, direction,
interface, BRC number, class, object id, and the class detail (tx size /
subtree node count / block subtree count). On exit (Ctrl-C) it prints
per-class session statistics.

```bash
tunnel-sink -listen '[fd00:1b5e::1]:8833'

# also log the SENT direction: relay local submissions to the edge's
# ingress lanes (tx 8725 / subtree 8726 / block 8727) and log each object
tunnel-sink -listen :8833 -submit-edge edge.example.net
```

### Exercise all lanes at once

[`scripts/exercise-lanes.sh`](scripts/exercise-lanes.sh) drives every push
format at one edge endpoint concurrently: a continuous transaction stream
(256-byte payloads at 10 pps by default), one BRC-143 subtree per second, and
one BRC-144 block per minute. Rates, intervals, sizes, ports, and duration are
all flags; it runs until Ctrl-C unless `-duration` is set.

```bash
make build
scripts/exercise-lanes.sh -host edge.example.net              # defaults
scripts/exercise-lanes.sh -host ::1 -tx-pps 500 -tx-size 512 \
  -subtree-interval 250ms -block-interval 10s -duration 2m
scripts/exercise-lanes.sh -host ::1 -port 8833                # ALL lanes to one
  # port — for a lab tunnel-sink or the submit relay; a real edge admits
  # subtree/block only on its per-class ports (the miner-tier gate)
```

### Inspect the generated subtree pool

```bash
subtx-gen -subtrees 8 -subtree-seed 'multicast-lab-bsv' -print-subtrees
```

## Layout

```
cmd/subtx-gen/            — CLI entry point (BRC-124/128 frame generator)
cmd/send-block-announce/  — BRC-131 block announce sender (TCP, legacy)
cmd/send-subtree-data/    — BRC-132 subtree data sender (TCP, legacy)
cmd/send-anchor-frame/    — BRC-134 anchor transaction sender (UDP default, -tcp opt)
cmd/send-subtree-push/    — BRC-143 subtree push sender (TCP, lane 8726)
cmd/send-block-push/      — BRC-144 block push sender (TCP, lane 8727)
cmd/tunnel-sink/          — consumer tunnel delivery sink + submit relay (diagnostic logger)
scripts/exercise-lanes.sh — drive tx + subtree + block lanes at one edge concurrently
internal/tx/              — exact-size walkable tx payload builder (raw + BRC-30 EF; never pads)
internal/subtree/         — deterministic subtree-ID pool
internal/seq/             — shared seq allocator + gap injector
internal/frame/           — v1/v2 encoder wrapper around shard-common
internal/rate/            — token-bucket pacer (smooth / burst)
internal/sender/          — worker pool driving net.UDPConn per worker
internal/announce/        — BRC-127 SubtreeGroupAnnounce TCP sender
internal/blockhdr/        — synthetic PoW-valid 80-byte block-header builder
```

> **Miner-tier gate:** `send-block-announce` and `send-subtree-data` emit
> privileged BRC-131/132 multicast frames. The proxy's miner TCP ingress
> (`-miner-tcp-listen-port`) and `-tx-accept-privileged` were **removed
> (2026-07-07)** — these legacy senders work only against legacy/dev setups
> that drive a privileged ingress class; the transaction ingress silently
> drops their frames. The current path is the BRC-143/144 push lanes:
> `send-subtree-push` → 8726, `send-block-push` → 8727. Anchors
> (`send-anchor-frame`, BRC-134) and BRC-127 SubtreeGroupAnnounce stay
> ungated. See the
> [shard-proxy transaction-only ingress](https://github.com/lightwebinc/shard-proxy/blob/main/docs/configuration.md#ingress-is-transaction-only-miner-port-deprecated).

See [docs/architecture.md](docs/architecture.md) and [docs/configuration.md](docs/configuration.md) for detailed documentation.

## Container image

The Dockerfile produces a single `gcr.io/distroless/static:nonroot` image
containing all seven binaries:

```
/usr/local/bin/subtx-gen             (continuous BRC-124/BRC-128 frame generator)
/usr/local/bin/send-anchor-frame     (one-shot BRC-134 anchor)
/usr/local/bin/send-block-announce   (one-shot BRC-131 announce, multicast ingress)
/usr/local/bin/send-subtree-data     (one-shot BRC-132 subtree-data, multicast ingress)
/usr/local/bin/send-subtree-push     (one-shot BRC-143 subtree push → proxy lane 8726)
/usr/local/bin/send-block-push       (one-shot BRC-144 block push → proxy lane 8727)
/usr/local/bin/tunnel-sink           (consumer tunnel delivery sink + submit relay, diagnostic)
```

The two push senders target the proxy's current privileged ingest lanes
(`-subtree-listen-port` / `-block-listen-port`); the multicast senders above
them exercise the legacy fabric-internal path.

**No `ENTRYPOINT` is set** — the consumer (Helm chart `mode` selector,
`docker run --entrypoint=…`, Kubernetes `command:` field) picks which binary
to invoke. Running the image without an explicit entrypoint will fail. The
[`subtx-generator-helm`](https://github.com/lightwebinc/subtx-generator-helm)
chart automates this via `.Values.mode`.

## Helm chart

A Kubernetes Helm chart is published from a dedicated chart repository:

- Repository: [`lightwebinc/subtx-generator-helm`](https://github.com/lightwebinc/subtx-generator-helm)
- HTTPS:
  ```
  helm repo add bsg https://lightwebinc.github.io/subtx-generator-helm
  helm install gen bsg/subtx-generator --set mode=subtx-gen
  ```
- OCI: `helm install gen oci://ghcr.io/lightwebinc/charts/subtx-generator --version 0.3.0`

The chart packages a single multi-binary image and selects which binary to run via `.Values.mode` (`subtx-gen` | `send-anchor-frame` | `send-block-announce` | `send-subtree-data`). Because these binaries accept **CLI flags only** (no env vars), the chart renders the matching per-mode `args` block into the container's `command` + `args`. Both `Deployment` and `Job` workload types are supported. See the chart README for the full reference.

## License

Apache 2.0 — see [LICENSE](LICENSE).
