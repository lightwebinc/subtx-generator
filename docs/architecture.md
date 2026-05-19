# bitcoin-subtx-generator — Architecture

## Overview

`bitcoin-subtx-generator` is a test traffic generator for the BSV multicast pipeline.
It generates random BRC-124/BRC-128 UDP frames at configurable rates, with controlled
subtree ID assignment and optional sequence gap injection to exercise the NACK/retransmission
path of `bitcoin-shard-listener` and `bitcoin-retry-endpoint`.

It also provides two standalone tools (`send-block-announce` and `send-subtree-data`) for
injecting BRC-131 and BRC-132 control-plane frames into `bitcoin-shard-proxy` via TCP.

`bitcoin-subtx-generator` is a **test tool, not a production component**. It sends frames
only; it never joins multicast groups or receives frames.

## Pipeline Position

```text
bitcoin-subtx-generator (subtx-gen)
      │  BRC-124/128 frames (UDP, port 9000)
      ▼
bitcoin-shard-proxy  ──multicast──►  bitcoin-shard-listener

bitcoin-subtx-generator (send-block-announce)
      │  BRC-131 frames (TCP, port 9002)
      ▼
bitcoin-shard-proxy  ──CtrlGroupControl──►  bitcoin-shard-listener

bitcoin-subtx-generator (send-subtree-data)
      │  BRC-132 frames (TCP, port 9002)
      ▼
bitcoin-shard-proxy  ──CtrlGroupSubtreeAnnounce──►  bitcoin-shard-listener
```

## Package Structure

```
bitcoin-subtx-generator/
  cmd/subtx-gen/           CLI entry point for the BRC-124/128 frame generator
  cmd/send-block-announce/ Standalone BRC-131 sender (BlockAnnounce + CoinbaseTx pairs)
  cmd/send-subtree-data/   Standalone BRC-132 sender (subtree node data)
  internal/tx/             Random BSV-shaped transaction payload builder
  internal/subtree/        Deterministic subtree-ID pool (seed → N stable 32-byte IDs)
  internal/seq/            Shared atomic sequence allocator with gap injection
  internal/frame/          BRC-124/128 encoder wrapper around bitcoin-shard-common
  internal/rate/           Token-bucket pacer (smooth at ≤1 kpps, burst mode above)
  internal/sender/         Worker pool: one net.UDPConn per worker goroutine
  internal/announce/       BRC-127 SubtreeAnnounce TCP sender
```

## Frame Generation (subtx-gen)

`subtx-gen` starts a pool of worker goroutines. Each worker owns an independent
`net.UDPConn` and a seeded PRNG, eliminating lock contention on the hot path.

**Sequence allocation:** a shared `seq.Allocator` distributes monotonically increasing
sequence numbers across workers using an atomic counter. Each frame gets a unique SeqNum.

**Subtree assignment:** the subtree ID pool is pre-computed from the user seed as
`pool[i] = SHA256(seed ∥ uint64_be(i))`. Each frame's SubtreeID is selected deterministically:
`pool[uint64(txID[:8]) % N]`. The same TxID always maps to the same subtree; the same seed
produces identical IDs across runs and machines.

**HashKey and SeqNum** are emitted as zero by the generator. `bitcoin-shard-proxy` stamps
them in-place before multicast forwarding.

**Rate pacing:** the `rate.Bucket` token-bucket pacer issues one token per millisecond.
Below ~1 kpps each send is followed by a sleep; above that threshold, frames are sent in
batches until the bucket is empty, then a single longer sleep occurs.

## Gap Injection

Gap injection exercises the NACK/retransmission path end-to-end without requiring any
manual packet loss.

When `-seq-gap-every N` is set, the `seq.Allocator` skips `seq-gap-size` sequence numbers
every N allocated sequences. The resulting gap is immediately detectable by the listener
(`bsl_gaps_detected_total` rises).

When `-seq-gap-delay D` is also set, the allocator enqueues the skipped sequence number(s)
for retransmission after duration D. If the listener has already sent a NACK and the retry
endpoint has served it, the generator's delayed retransmit may arrive as a duplicate;
`bsl_gaps_suppressed_total` should rise. If D is zero (the default), the gap is permanent
and ultimately increments `bsl_gaps_unrecovered_total` on the listener.

## BRC-127 SubtreeAnnounce (announce mode)

When both `-announce-addr` and `-subtree-group` are set, an `announce.Sender` goroutine
connects to the proxy's TCP ingress and periodically sends BRC-127 SubtreeAnnounce
datagrams (64 bytes each) for all subtree IDs in the pool that belong to the configured
group(s). The proxy's `handleConn` recognises the SubtreeAnnounce version byte (`0x07`) and
forwards it via `ForwardControl` to `CtrlGroupSubtreeAnnounce` without stamping.

The re-announce ticker fires at `-announce-interval` to refresh TTLs of already-active
subtrees. This prevents eviction from the listener's dynamic filter registry.

**Phased mode:** when `-announce-phase-size` and `-announce-phase-interval` are both
non-zero, the sender starts with zero active subtrees and adds `phase-size` more every
`phase-interval`. The re-announce ticker continues to fire for already-active subtrees.
Phased mode produces a visible ramp in dashboard time-series and is used in subtree group
scaling scenarios.

## send-block-announce

`send-block-announce` connects to the proxy TCP ingress and sends pairs of BRC-131 frames:

1. **BlockAnnounce** (MsgType `0x01`): carries a random 80-byte block header, the block
   hash as ContentID (`SHA256d(blockHeader)`), and `subtrees` random subtree hashes appended
   to the payload.
2. **CoinbaseTx** (MsgType `0x02`, unless `-coinbase=false`): carries a random coinbase
   transaction with the CoinbaseTxID as ContentID.

`HashKey` and `SeqNum` are left zero; the proxy stamps them. Intended to test the listener's
`processBlockFrame` path, block header egress, and BRC-131 fragment reassembly.

## send-subtree-data

`send-subtree-data` connects to the proxy TCP ingress and sends BRC-132 frames with
configurable `MsgType` (hashes-only or full-nodes), node count, and subtree ID pool.

When `-subtree-count > 0`, a pool of that many random subtree IDs is pre-generated and
cycled frame-by-frame. When zero, a fresh random SubtreeID is used per frame.

`HashKey` and `SeqNum` are left zero; the proxy stamps them. Intended to test the listener's
`processSubtreeDataFrame` path and BRC-132 fragment reassembly.
