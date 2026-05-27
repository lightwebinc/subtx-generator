# subtx-generator — Configuration Reference

All parameters are accepted as CLI flags only. No environment variable fallbacks are defined.

---

## subtx-gen

Generates random BRC-124/BRC-128 UDP frames at configurable rates and optionally sends
BRC-127 SubtreeAnnounce datagrams via TCP.

| Flag | Default | Description |
|---|---|---|
| `-addr` | `[::1]:9000` | Target `host:port` for UDP frame sending |
| `-frame-version` | `2` | Frame version to emit: `1` (BRC-12, 44-byte header) or `2` (BRC-124/128, 92-byte header) |
| `-shard-bits` | `2` | Informational: shard-bits the proxy uses (for predicted-group diagnostic logging) |
| `-subtrees` | `8` | Number of deterministic subtree IDs in the pool (0 = no SubtreeID field set) |
| `-subtree-seed` | `subtx-generator-default` | Seed for subtree ID pool derivation; plain string or hex |
| `-pps` | `1000` | Target packets per second (0 = unlimited) |
| `-duration` | `10s` | Run time (0 = run until `-count` reached or SIGINT) |
| `-count` | `0` | Stop after N frames (0 = unlimited) |
| `-workers` | `runtime.NumCPU()` | Worker goroutine count (0 = NumCPU) |
| `-payload-size` | `512` | Random transaction payload size in bytes |
| `-payload-format` | `brc124` | Payload encoding: `brc124` (raw tx), `brc128` (BRC-30 EF), or `mixed` |
| `-seq-start` | `1` | First sequence number |
| `-seq-gap-every` | `0` | Inject a gap every N frames; 0 = disabled |
| `-seq-gap-size` | `1` | Number of sequence numbers to skip per gap |
| `-seq-gap-delay` | `0` | Delay before retransmitting the skipped sequence(s); 0 = permanent gap |
| `-log-interval` | `1s` | Periodic statistics log interval |
| `-print-subtrees` | `false` | Print all subtree IDs in the pool and exit |
| `-subtree-group` | `""` | Comma-separated 32-char hex GroupIDs for BRC-127 announce (empty = disabled) |
| `-announce-addr` | `""` | Proxy TCP address for BRC-127 SubtreeAnnounce (empty = disabled) |
| `-announce-interval` | `10s` | SubtreeAnnounce re-announce period (TTL refresh cadence for active subtrees) |
| `-announce-ttl` | `0` | TTL field in SubtreeAnnounce datagrams; 0 = use listener default |
| `-announce-phase-size` | `0` | Subtrees to add per phase tick; 0 = announce full pool immediately |
| `-announce-phase-interval` | `0` | Phase tick interval; 0 = phased mode disabled |
| `-corrupt-txid-rate` | `0` | Percentage of frames with a corrupted TxID field (0–100); for listener payload-hash verification tests |

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

Connects to the proxy TCP ingress and sends BRC-131 block control frame pairs
(BlockAnnounce + CoinbaseTx) for integration testing.

| Flag | Default | Description |
|---|---|---|
| `-addr` | `[::1]:9002` | Proxy TCP address (`host:port`) |
| `-blocks` | `10` | Number of simulated blocks to announce |
| `-subtrees` | `4` | Subtree hashes per BlockAnnounce frame |
| `-interval` | `100ms` | Delay between successive block pairs |
| `-coinbase` | `true` | Also send a CoinbaseTx frame (MsgType 0x02) for each block |

Each BlockAnnounce carries a random 80-byte block header with ContentID set to
`SHA256d(blockHeader)`. When `-coinbase=true`, a CoinbaseTx frame follows immediately
with a random coinbase transaction and its SHA256d as ContentID.

---

## send-subtree-data

Connects to the proxy TCP ingress and sends BRC-132 subtree data frames for integration
testing.

| Flag | Default | Description |
|---|---|---|
| `-addr` | `[::1]:9002` | Proxy TCP address (`host:port`) |
| `-frames` | `20` | Number of BRC-132 frames to send |
| `-msg-type` | `hashes` | Payload type: `hashes` (hashes-only, 32 bytes/node) or `full` (full-nodes, 48 bytes/node) |
| `-nodes` | `16` | Number of subtree nodes per frame |
| `-payload-size` | `0` | Override total payload size in bytes (0 = derived from `-nodes` × node size) |
| `-subtree-count` | `0` | Unique subtree IDs to cycle through (0 = fresh random ID per frame) |
| `-interval` | `50ms` | Delay between frames |
