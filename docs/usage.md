# subtx-generator Usage

Worked examples for each binary. Every flag is listed in
[configuration.md](configuration.md); internals are in
[architecture.md](architecture.md).

## subtx-gen

```bash
subtx-gen \
  -addr [2001:db8::2]:8725 \
  -frame-version 2 \
  -shard-bits 2 \
  -subtrees 8 \
  -subtree-seed 'multicast-lab-bsv' \
  -pps 1000 \
  -duration 10s \
  -payload-size 512 \
  -workers 0
```

## direct-multicast mode (skip the proxy)

```bash
# Emit directly to FF35::B:idx (SSM site scope); useful for fabric
# load validation and SSM scenarios where the generator is the
# data-plane publisher.
# Operators MUST add -bind-source to the shard-manifest -publishers
# list so receivers' (S,G) joins include this generator.
subtx-gen \
  -mode direct-multicast \
  -bind-source 2001:db8::abc \
  -egress-iface eth0 \
  -source-mode ssm \
  -scope site \
  -shard-bits 2 \
  -egress-port 9001 \
  -pps 1000 -duration 30s
```

See the [bsv-multicast Source-Specific Multicast design](https://github.com/lightwebinc/bsv-multicast/blob/main/DESIGN.md#source-specific-multicast-ssm)
for the full design.

## Gap injection (NACK / retransmit tests)

The generator stamps the SeqNums itself, so the proxy it sends to must run
with `-allow-stamped-ingress`, and gap injection needs `-frame-version 2` (see
[configuration.md](configuration.md#gap-injection)).

```bash
# Permanent gap: each flow skips a sequence number every 500 frames; the listener reports
# bsl_gaps_detected_total and (after NACK retries exhausted) bsl_gaps_unrecovered_total.
subtx-gen -pps 1000 -duration 30s -seq-gap-every 500

# Late gap fill: the listener sees a gap and NACKs it, and 500 ms after the
# gap opened the generator sends the missing seq in a late frame of the same
# flow, so bsl_gaps_suppressed_total rises. After -duration the run keeps
# going until every queued fill is out (the last comes due up to 500 ms
# later, and building and sending can take longer); the done line reports
# the fills as late=.
subtx-gen -pps 1000 -duration 30s -seq-gap-every 500 -seq-gap-delay 500ms
```

## BRC-127 SubtreeGroupAnnounce sender

```bash
# Connect to the proxy TCP ingress and periodically announce all subtree IDs
# in the pool to the GroupSubtreeGroupAnnounce control-plane multicast group.
subtx-gen \
  -addr [2001:db8::2]:8725 \
  -subtrees 8 \
  -subtree-seed 'multicast-lab-bsv' \
  -subtree-group bfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbf \
  -announce-addr [2001:db8::2]:9002 \
  -announce-interval 10s \
  -announce-ttl 0 \
  -pps 1000 -duration 30s
```

### Phased mode (time-varying group membership)

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
  -addr [2001:db8::2]:8725 \
  -subtrees 8 \
  -subtree-seed 'multicast-lab-bsv' \
  -subtree-group bfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbf \
  -announce-addr [2001:db8::2]:9002 \
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

## Consumer tunnel sink (receive side)

`tunnel-sink` is the receiving-side counterpart to the senders above: a
consumer-side diagnostic sink for the tunnel delivery plane. It listens on the
consumer's SDA (default `:8833`), auto-detects each connection's lane class
(raw/EF tx, BRC-143 subtree, BRC-144 block, or a framed BRC-124 stream by
network magic), and logs one line per object with timestamp, direction,
interface, BRC number, class, object id, and the class detail (tx size /
subtree node count / block subtree count). On exit (Ctrl-C) it prints
per-class session statistics.

```bash
tunnel-sink -listen '[2001:db8::1]:8833'

# also log the SENT direction: relay local submissions to the edge's
# ingress lanes (tx 8725 / subtree 8726 / block 8727) and log each object
tunnel-sink -listen :8833 -submit-edge edge.example.net
```

## Exercise all lanes at once

[`scripts/exercise-lanes.sh`](../scripts/exercise-lanes.sh) drives every push
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
  # port: for a lab tunnel-sink or the submit relay; a real edge admits
  # subtree/block only on its per-class ports (the miner-tier gate)
```

## Inspect the generated subtree pool

```bash
subtx-gen -subtrees 8 -subtree-seed 'multicast-lab-bsv' -print-subtrees
```

## Container image

The single `gcr.io/distroless/static:nonroot` image carries all seven binaries:

```
/usr/local/bin/subtx-gen             (continuous BRC-124/BRC-128 frame generator)
/usr/local/bin/send-anchor-frame     (one-shot BRC-134 anchor)
/usr/local/bin/send-block-announce   (one-shot BRC-131 announce, legacy multicast ingress)
/usr/local/bin/send-subtree-data     (one-shot BRC-132 subtree-data, legacy multicast ingress)
/usr/local/bin/send-subtree-push     (one-shot BRC-143 subtree push → proxy lane 8726)
/usr/local/bin/send-block-push       (one-shot BRC-144 block push → proxy lane 8727)
/usr/local/bin/tunnel-sink           (consumer tunnel delivery sink + submit relay, diagnostic)
```

Release images are published by a manual workflow dispatch as
`ghcr.io/lightwebinc/subtx-generator:<tag>`, the git tag without its `v`
(for example `ghcr.io/lightwebinc/subtx-generator:0.4.4`).

**No `ENTRYPOINT` is set**: the consumer (Helm chart `mode` selector,
`docker run --entrypoint=…`, Kubernetes `command:` field) picks which binary
to invoke.
