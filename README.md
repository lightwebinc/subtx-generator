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
  fleet. See the [Unified Component Logging](https://github.com/lightwebinc/shard-common/blob/main/docs/logging.md).

## Install

```bash
go install github.com/lightwebinc/subtx-generator/cmd/subtx-gen@latest
```

Or local build:

```bash
make build           # builds every cmd/* binary into the repo root
```

## Quick start

```bash
subtx-gen \
  -addr [fd20::2]:8725 \
  -frame-version 2 \
  -shard-bits 2 \
  -subtrees 8 \
  -subtree-seed 'example-seed' \
  -pps 1000 \
  -duration 10s
```

Direct-multicast mode, gap injection, BRC-127 announce (incl. phased mode),
`tunnel-sink`, and `scripts/exercise-lanes.sh` examples are in
[docs/usage.md](docs/usage.md).

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

`send-block-announce` and `send-subtree-data` are legacy privileged senders; see
[Miner port deprecation](https://github.com/lightwebinc/shard-proxy/blob/main/docs/configuration.md#ingress-is-transaction-only-miner-port-deprecated).

## Documentation

- [Usage](docs/usage.md) — worked examples for every binary
- [Configuration](docs/configuration.md) — every flag
- [Architecture](docs/architecture.md) — pipeline position, frame generation, package structure

## Container image

Images are published per release tag as
`ghcr.io/lightwebinc/subtx-generator:<tag>` (current `0.4.0`): one
`gcr.io/distroless/static:nonroot` image with all seven binaries and no
`ENTRYPOINT`, so the caller picks the binary. See
[docs/usage.md § Container image](docs/usage.md#container-image).

## Helm chart

A Kubernetes Helm chart is published from a dedicated chart repository:

- Repository: [`charts/subtx-generator`](https://github.com/lightwebinc/charts/tree/main/charts/subtx-generator)
- Install: `helm install subtx-generator oci://ghcr.io/lightwebinc/charts/subtx-generator`

The chart packages a single multi-binary image and selects which binary to run via `.Values.mode` — see the chart README for the mode list. Because these binaries accept **CLI flags only** (no env vars), the chart renders the matching per-mode `args` block into the container's `command` + `args`. Both `Deployment` and `Job` workload types are supported. See the chart README for the full reference.

## Releases

Releases and release notes live on [GitHub Releases](https://github.com/lightwebinc/subtx-generator/releases); there is no CHANGELOG.

## License

Apache 2.0 — see [LICENSE](LICENSE).
