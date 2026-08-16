# subtx-generator — Configuration Reference

All parameters are accepted as CLI flags only, with one exception: every binary
(`subtx-gen`, `send-block-announce`, `send-subtree-data`, `send-anchor-frame`,
`send-subtree-push`, `send-block-push`, `tunnel-sink`)
reads the `LOG_FORMAT` environment variable (`text` default | `json`) for
unified structured logging via `shard-common/logging` — see the
[canonical logging doc](https://github.com/lightwebinc/shard-common/blob/main/docs/logging.md).

---

## subtx-gen

Generates random BRC-124/BRC-128 UDP frames at configurable rates and optionally sends
BRC-127 SubtreeGroupAnnounce datagrams via TCP.

| Flag | Default | Description |
|---|---|---|
| `-addr` | `[::1]:8725` | Target `host:port` (UDP by default; TCP with `-tcp`) |
| `-tcp` | `false` | Submit over TCP — the standard 8725 submission lane (a stream of BRC frames, no envelope). UDP submission is deprecated. `-mode unicast` only |
| `-frame-version` | `2` | Frame version to emit: `1` (BRC-12, 44-byte header) or `2` (BRC-124/128, 92-byte header) |
| `-shard-bits` | `2` | Informational: shard-bits the proxy uses (for predicted-group diagnostic logging) |
| `-subtrees` | `8` | Number of deterministic subtree IDs in the pool (0 = no SubtreeID field set) |
| `-subtree-seed` | `subtx-generator-default` | Seed for subtree ID pool derivation; plain string or hex |
| `-pps` | `1000` | Target packets per second (0 = unlimited) |
| `-duration` | `0` | Max run time. `0` (default) runs until `-count` is reached or SIGINT; if `>0`, stops at whichever of count or duration comes first |
| `-count` | `0` | Stop after N frames (0 = unlimited) |
| `-workers` | `runtime.NumCPU()` | Worker goroutine count (0 = NumCPU) |
| `-payload-size` | `512` | Exact transaction payload size in bytes. Every payload is exactly one structurally valid tx of this size (sizing slack is absorbed inside the script fields, never as trailing padding — padding desyncs TCP objfmt streams). Minimum 75 for `brc128`/`mixed`, 60 for `brc124`; below-minimum sizes fail at startup |
| `-payload-format` | `brc128` | Payload encoding: `brc128` (BRC-30 EF; default — the fabric is EF-native), `brc124` (raw tx; legacy/miner lanes), or `mixed`. The frame TxID is stamped canonically (`objfmt.TxID`, i.e. over the standard serialization — EF extras excluded) |
| `-seq-start` | `1` | First sequence number |
| `-seq-gap-every` | `0` | Inject a gap every N frames; 0 = disabled |
| `-seq-gap-size` | `1` | Number of sequence numbers to skip per gap |
| `-seq-gap-delay` | `0` | Delay before retransmitting the skipped sequence(s); 0 = permanent gap |
| `-log-interval` | `1s` | Periodic statistics log interval |
| `-print-subtrees` | `false` | Print all subtree IDs in the pool and exit |
| `-subtree-group` | `""` | Comma-separated 32-char hex GroupIDs for BRC-127 announce (empty = disabled) |
| `-announce-addr` | `""` | Proxy TCP address for BRC-127 SubtreeGroupAnnounce (empty = disabled) |
| `-announce-interval` | `10s` | SubtreeGroupAnnounce re-announce period (TTL refresh cadence for active subtrees) |
| `-announce-ttl` | `0` | TTL field in SubtreeGroupAnnounce datagrams; 0 = use listener default |
| `-announce-phase-size` | `0` | Subtrees to add per phase tick; 0 = announce full pool immediately |
| `-announce-phase-interval` | `0` | Phase tick interval; 0 = phased mode disabled |
| `-corrupt-txid-rate` | `0` | Percentage of frames with a corrupted TxID field (0–100); for listener payload-hash verification tests |
| `-mode` | `unicast` | Send mode: `unicast` (default — forward to proxy via `-addr`) or `direct-multicast` (skip the proxy and emit directly to the SSM data plane; see [SSM Support Plan](https://github.com/lightwebinc/bsv-multicast/blob/main/DESIGN.md#source-specific-multicast-ssm)). |
| `-bind-source` | `""` | direct-multicast: IPv6 literal bound on every egress socket. Required when `-mode=direct-multicast`. MUST be added to the shard-manifest publishers list so receivers' `(S,G)` joins include this generator. |
| `-egress-iface` | `""` | direct-multicast: outbound interface for multicast egress (used for `IPV6_MULTICAST_IF`). |
| `-source-mode` | `asm` | direct-multicast: addressing model `asm` or `ssm` (selects FF05/FF35/FF3E prefix via `shard.Prefix`). |
| `-scope` | `site` | direct-multicast: multicast scope (`site` or `global`). Combined with `-source-mode` to compute the prefix. |
| `-mc-group-id` | `0x000B` | direct-multicast: IANA group-id (bytes 12–13 of the destination IPv6). |
| `-egress-port` | `9001` | direct-multicast: destination UDP port written into every multicast datagram. |

### direct-multicast mode

`-mode=direct-multicast` bypasses the shard-proxy: each worker opens
its own IPv6 multicast egress socket bound to `-bind-source` on
`-egress-iface`, derives the destination group from each TxID via
`shard.Engine.Addr(groupIdx, -egress-port)`, and writes directly to
the resulting `(S=-bind-source, G)` address. The generator stamps
SeqNum (per-flow, matching the proxy's BRC-128 semantics) and HashKey
= XXH64(BindSource ∥ groupIdx ∥ subtreeID) so SSM listeners see
deterministic flows and gap detection works without a proxy in the
loop.

Use this mode for fabric load-validation without the proxy in the
path, or any SSM scenario where the generator should be a first-class
data-plane publisher.

### Gap Injection

Three flags control gap injection:

- **`-seq-gap-every N`** — the allocator skips `seq-gap-size` sequence numbers every N
  allocated sequences. Setting N=500 creates one gap per 500 frames.
- **`-seq-gap-size S`** — how many consecutive sequence numbers to skip per gap event.
  The listener opens S individual gap entries.
- **`-seq-gap-delay D`** — if non-zero, the allocator resends the skipped sequence(s)
  after duration D. A value of `0` (the default) creates a permanent gap that exhausts
  all NACK retries and increments `bsl_gaps_unrecovered_total`. A value such as `50ms`
  creates a delayed retransmit that should suppress the NACK via `bsl_gaps_suppressed_total`.

Example — permanent gap:
```
subtx-gen -pps 1000 -duration 30s -seq-gap-every 500
```

Example — delayed retransmit (NACK recovery test):
```
subtx-gen -pps 1000 -duration 30s -seq-gap-every 500 -seq-gap-delay 50ms
```

---

## send-block-announce

Connects to the proxy over TCP and sends BRC-131 block control frame pairs
(BlockAnnounce + CoinbaseTx) for integration testing.

> **Miner-tier gate:** BRC-131/132 multicast frames are privileged. The proxy's
> miner TCP ingress (`-miner-tcp-listen-port`) and `-tx-accept-privileged` were
> **removed (2026-07-07)** — this legacy sender works only against legacy/dev
> setups that drive a privileged ingress class; the transaction ingress
> silently drops these frames. The current path is the BRC-143/144 push lanes
> (`send-subtree-push` → 8726, `send-block-push` → 8727). See the
> [shard-proxy transaction-only ingress](https://github.com/lightwebinc/shard-proxy/blob/main/docs/configuration.md#ingress-is-transaction-only-miner-port-deprecated).
> Anchor frames (`send-anchor-frame`, BRC-134) and BRC-127 SubtreeGroupAnnounce
> remain ungated.

| Flag | Default | Description |
|---|---|---|
| `-addr` | `[::1]:9002` | Proxy TCP address (`host:port`) — legacy default; the OSS proxy's `-tcp-listen-port` defaults to 0 (disabled) and drops privileged frames. See gate note above |
| `-blocks` | `10` | Number of simulated blocks to announce |
| `-subtrees` | `4` | Subtree hashes per BlockAnnounce frame |
| `-interval` | `100ms` | Delay between successive block pairs |
| `-coinbase` | `true` | Also send a CoinbaseTx frame (MsgType 0x02) for each block |

Each BlockAnnounce carries a random 80-byte block header with ContentID set to
`SHA256d(blockHeader)`. When `-coinbase=true`, a CoinbaseTx frame follows immediately
with a structurally valid (walkable) coinbase transaction and its canonical TxID
(`objfmt.TxID`) as ContentID — never random bytes, which would desync a
downstream tx-class objfmt stream.

---

## send-subtree-data

Connects to the proxy over TCP and sends BRC-132 subtree data frames for integration
testing. BRC-132 frames are privileged — the miner-tier gate note under
[send-block-announce](#send-block-announce) applies here too; the current path
is `send-subtree-push` → 8726.

| Flag | Default | Description |
|---|---|---|
| `-addr` | `[::1]:9002` | Proxy TCP address (`host:port`) — legacy default; the OSS proxy's `-tcp-listen-port` defaults to 0 (disabled) and drops privileged frames. See gate note above |
| `-frames` | `20` | Number of BRC-132 frames to send |
| `-msg-type` | `hashes` | Payload type: `hashes` (hashes-only, 32 bytes/node) or `full` (full-nodes, 48 bytes/node) |
| `-nodes` | `16` | Number of subtree nodes per frame |
| `-payload-size` | `0` | Override total payload size in bytes (0 = derived from `-nodes` × node size) |
| `-subtree-count` | `0` | Unique subtree IDs to cycle through (0 = fresh random ID per frame) |
| `-interval` | `50ms` | Delay between frames |

---

## send-anchor-frame

Sends BRC-134 anchor transaction frames (`FrameVerV6`) to the proxy — UDP by default
(matching the BRC-124/128 data path), or TCP with `-tcp`. Anchor frames are **not**
subject to the miner-tier ingress gate; the consumer ingress accepts them.

| Flag | Default | Description |
|---|---|---|
| `-addr` | `[::1]:8725` | Proxy address (`host:port`); UDP by default |
| `-count` | `10` | Number of anchor frames to send |
| `-payload-size` | `256` | Exact anchor tx payload size in bytes — one structurally valid raw tx of exactly this size (min 60), TxID = `objfmt.TxID` |
| `-interval` | `50ms` | Delay between frames |
| `-tcp` | `false` | Send over TCP instead of UDP (point `-addr` at the proxy's `-tcp-listen-port`) |

---

## send-subtree-push

Streams BRC-143 subtree push objects (header-stripped, self-delimiting) to the
proxy's tunnel-bound subtree push lane (standard 8726) over TCP — the current
path for miner subtree ingest. The proxy reframes each object into a BRC-132
multicast frame.

| Flag | Default | Description |
|---|---|---|
| `-addr` | `[::1]:8726` | Proxy subtree push ingress address (`host:port`) |
| `-interval` | `1s` | Delay between subtree objects |
| `-count` | `0` | Stop after N objects (0 = until `-duration` / SIGINT) |
| `-duration` | `0` | Stop after this long (0 = until `-count` / SIGINT) |
| `-nodes` | `16` | Leaf node hashes per subtree |
| `-coinbase-placeholder` | `true` | Set node[0] to the `0xFF`×32 coinbase placeholder (BRC-143 convention) |
| `-seed` | `subtree-push` | PRNG seed (identifies this source for the delivery matrix) |
| `-log-hashes` | `false` | Print every subtree root (for end-to-end hash compare) |

---

## send-block-push

Streams BRC-144 block push objects to the proxy's tunnel-bound block push lane
(standard 8727) over TCP — the current path for miner block ingest.

| Flag | Default | Description |
|---|---|---|
| `-addr` | `[::1]:8727` | Proxy block push ingress address (`host:port`) |
| `-interval` | `9m` | Delay between block objects |
| `-count` | `0` | Stop after N blocks (0 = until `-duration` / SIGINT) |
| `-duration` | `0` | Stop after this long (0 = until `-count` / SIGINT) |
| `-subtrees` | `4` | Subtree roots per block |
| `-coinbase-size` | `200` | Inline coinbase transaction size in bytes |
| `-bump-size` | `0` | Coinbase BUMP (BRC-74) byte length (0 = none) |
| `-height-start` | `800000` | Block height of the first block |
| `-seed` | `block-push` | PRNG seed (identifies this source for the delivery matrix) |
| `-log-hashes` | `false` | Print every block hash (for end-to-end hash compare) |

---

## tunnel-sink

Consumer-side diagnostic sink for the tunnel delivery plane. Listens on the
consumer's SDA, accepts the edge's push connections, and logs one line per
object (timestamp, direction, interface, BRC number, class, object id, class
detail). Lane class is auto-detected per connection (bare lanes carry no type
tag); a framed BRC-124 stream is recognized by network magic. Prints
per-class session statistics on SIGINT/SIGTERM.

| Flag | Default | Description |
|---|---|---|
| `-listen` | `:8833` | Delivery listen address — the consumer SDA (empty host = all interfaces; 8833 is the standard Teranode propagation port) |
| `-lane` | `auto` | Force the delivery lane class: `auto\|tx\|subtree\|block` (auto = per-connection sniff) |
| `-summary` | `true` | Print session summary statistics on exit |
| `-max-object` | `268435456` | Maximum single object size in bytes (256 MiB) |
| `-submit-edge` | *(empty)* | Edge host: enables the submit relay (SENT direction) toward this edge |
| `-submit-listen` | `localhost:8724` | Submit relay listen address (used with `-submit-edge`) |
| `-submit-tx-port` | `8725` | Edge tx ingress port |
| `-submit-subtree-port` | `8726` | Edge subtree push ingress port |
| `-submit-block-port` | `8727` | Edge block push ingress port |

Notes:

- The delivery lanes are one-way (edge → consumer); the sink never writes on
  a delivery connection.
- The submit relay auto-detects the class of the submitted stream and dials
  the edge port that matches (`tx`/framed → `-submit-tx-port`, BRC-143 →
  `-submit-subtree-port`, BRC-144 → `-submit-block-port`), forwarding bytes
  verbatim. Subtree/block submissions are miner-tier gated at the edge; the
  relay forwards regardless and the edge applies policy.
- Object ids are logged in internal byte order (matching the emitters'
  `-log-hashes` output), truncated to the leading 8 bytes.
