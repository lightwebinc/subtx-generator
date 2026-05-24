// Package sender is the worker pool that generates and transmits frames.
//
// Concurrency model:
//   - One net.UDPConn per worker (Dial'ed once). Each worker owns its
//     encoding buffer and per-worker PRNG, so the hot path is lock-free.
//   - A central pacer (internal/rate) gates emission. Workers pull tokens
//     from a shared channel; backpressure is natural because Wait() blocks.
//   - Sequence numbers come from a shared atomic allocator (internal/seq).
//   - Subtree IDs are chosen deterministically from a read-only Pool.
package sender

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	common "github.com/lightwebinc/bitcoin-shard-common/frame"

	myframe "github.com/lightwebinc/bitcoin-subtx-generator/internal/frame"
	"github.com/lightwebinc/bitcoin-subtx-generator/internal/rate"
	"github.com/lightwebinc/bitcoin-subtx-generator/internal/seq"
	"github.com/lightwebinc/bitcoin-subtx-generator/internal/subtree"
	"github.com/lightwebinc/bitcoin-subtx-generator/internal/tx"
)

// PayloadFormat selects the BSV transaction encoding written into each
// frame's payload field. The frame header (BRC-12 vs BRC-124) is governed
// independently by Config.FrameVersion.
type PayloadFormat int

const (
	// PayloadBRC124 emits BRC-12 raw transaction payloads (default).
	// Frames carrying BRC-124 payloads are "BRC-124 frames".
	PayloadBRC124 PayloadFormat = iota
	// PayloadBRC128 emits BRC-30 Extended Format (EF) transaction payloads.
	// Frames carrying EF payloads are "BRC-128 frames".
	PayloadBRC128
	// PayloadMixed alternates between BRC-124 and BRC-128 payloads on a
	// per-frame, per-worker basis. Used to verify infrastructure handles
	// both formats coexisting on the same multicast group.
	PayloadMixed
)

// String returns the canonical CLI/env spelling.
func (p PayloadFormat) String() string {
	switch p {
	case PayloadBRC128:
		return "brc128"
	case PayloadMixed:
		return "mixed"
	default:
		return "brc124"
	}
}

// Config tunes the sender.
type Config struct {
	Addr            string // target host:port
	FrameVersion    myframe.Version
	Workers         int
	PPS             int
	Duration        time.Duration // 0 = run until Count frames sent or ctx canceled
	Count           uint64        // 0 = unlimited
	PayloadSize     int
	PayloadFormat   PayloadFormat
	LogInterval     time.Duration
	CorruptTxIDRate uint // percentage of frames to corrupt TxID (0-100)
	ShardBits       uint // proxy shard-bits; used to compute per-flow groupIdx for gap injection
}

// Runner ties together the pacer, seq allocator, subtree pool, and worker pool.
type Runner struct {
	cfg   Config
	pool  *subtree.Pool
	alloc *seq.Allocator
	pfa   *seq.PerFlowAllocator // active when GapEnabled to inject per-flow gaps

	sent   atomic.Uint64
	bytes  atomic.Uint64
	errors atomic.Uint64
}

// New creates a Runner.
func New(cfg Config, pool *subtree.Pool, alloc *seq.Allocator) *Runner {
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.PayloadSize <= 0 {
		cfg.PayloadSize = 256
	}
	if cfg.LogInterval <= 0 {
		cfg.LogInterval = time.Second
	}
	r := &Runner{cfg: cfg, pool: pool, alloc: alloc}
	// When gap injection is enabled we issue SeqNums per (groupIdx, subtreeID)
	// flow so each flow has its own monotonic counter with deliberate gaps.
	// This mirrors the proxy's per-flow stamping, allowing the listener's
	// per-flow gap tracker to detect the injected gaps without false positives
	// from cross-flow sequence sparsity.
	if alloc != nil && alloc.GapEnabled() {
		gc := alloc.GapConfig()
		gc.Start = 1
		r.pfa = seq.NewPerFlow(gc)
	}
	return r
}

// groupIdx returns the shard group index for txid given the configured
// ShardBits. Replicates shard.Engine.GroupIndex so we can compute the same
// flow key the proxy will use, without pulling in the full shard package.
// When ShardBits is 0 or unset, every frame maps to group 0.
func (r *Runner) groupIdx(txid [32]byte) uint32 {
	bits := r.cfg.ShardBits
	if bits == 0 {
		return 0
	}
	prefix32 := binary.BigEndian.Uint32(txid[0:4])
	mask := uint32(1<<bits) - 1
	return (prefix32 >> (32 - bits)) & mask
}

// Run blocks until ctx is canceled, Count is reached, or Duration elapses.
// Returns the number of frames transmitted.
func (r *Runner) Run(ctx context.Context) (uint64, error) {
	// Derive a run deadline if Duration is set.
	runCtx := ctx
	if r.cfg.Duration > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, r.cfg.Duration)
		defer cancel()
	}

	pacer := rate.New(r.cfg.PPS)
	defer pacer.Stop()

	tokens := make(chan struct{}, r.cfg.Workers*2)
	var wg sync.WaitGroup

	// Dispatcher goroutine: drives pacer, counts issued tokens against Count.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(tokens)
		for {
			if err := runCtx.Err(); err != nil {
				return
			}
			if r.cfg.Count > 0 && r.sent.Load() >= r.cfg.Count {
				return
			}
			if !pacer.Wait() {
				return
			}
			select {
			case tokens <- struct{}{}:
			case <-runCtx.Done():
				return
			}
		}
	}()

	// Workers.
	for i := 0; i < r.cfg.Workers; i++ {
		wg.Add(1)
		go r.worker(runCtx, i, tokens, &wg)
	}

	// Periodic logger.
	logDone := make(chan struct{})
	go r.logger(runCtx, logDone)

	wg.Wait()
	close(logDone)
	return r.sent.Load(), nil
}

func (r *Runner) worker(ctx context.Context, id int, tokens <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := net.Dial("udp", r.cfg.Addr)
	if err != nil {
		log.Printf("worker %d: dial %s: %v", id, r.cfg.Addr, err)
		return
	}
	defer func() { _ = conn.Close() }()

	// Per-worker PRNG seed.
	var seed [32]byte
	if _, err := cryptorand.Read(seed[:]); err != nil {
		log.Printf("worker %d: seed: %v", id, err)
		return
	}
	seed[0] ^= byte(id)

	builder := tx.New(seed)

	hdrSize := myframe.HeaderSize(r.cfg.FrameVersion)
	buf := make([]byte, hdrSize+r.cfg.PayloadSize)
	payload := make([]byte, r.cfg.PayloadSize)

	f := &common.Frame{}

	// Per-worker frame counter; drives PayloadMixed alternation.
	var local uint64

	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-tokens:
			if !ok {
				return
			}
		}

		if r.cfg.Count > 0 && r.sent.Load() >= r.cfg.Count {
			return
		}

		// Random payload (format chosen per Config.PayloadFormat).
		useEF := false
		switch r.cfg.PayloadFormat {
		case PayloadBRC128:
			useEF = true
		case PayloadMixed:
			useEF = local%2 == 1
		}
		local++
		if useEF {
			payload = builder.BuildEF(payload[:0:cap(payload)], r.cfg.PayloadSize)
		} else {
			payload = builder.Build(payload[:0:cap(payload)], r.cfg.PayloadSize)
		}
		f.Payload = payload

		// Compute TxID as SHA256d(payload) for valid frames.
		first := sha256.Sum256(payload)
		second := sha256.Sum256(first[:])
		copy(f.TxID[:], second[:])

		// Optionally corrupt TxID (flip a random bit) based on corrupt rate.
		if r.cfg.CorruptTxIDRate > 0 {
			var randByte [1]byte
			if _, err := cryptorand.Read(randByte[:]); err == nil {
				if uint(randByte[0])%100 < r.cfg.CorruptTxIDRate {
					// Flip a random bit in the TxID to invalidate the hash.
					bit := randByte[0]
					byteIdx := bit / 8
					bitIdx := bit % 8
					f.TxID[byteIdx] ^= (1 << bitIdx)
				}
			}
		}

		// SubtreeID chosen by txid high bits so listeners filtering on a
		// single subtree see a predictable fraction of traffic.
		sel := binary.BigEndian.Uint64(f.TxID[:8])
		f.SubtreeID = r.pool.Pick(sel)

		// Gap injection (when enabled) pre-stamps f.SeqNum from a per-flow
		// allocator keyed by (groupIdx, subtreeID). This matches the proxy's
		// per-flow stamping so the listener's per-flow gap tracker can detect
		// the injected gaps. When gap injection is disabled, leave f.SeqNum=0
		// so the proxy stamps per-flow monotonic SeqNums as normal.
		if r.pfa != nil {
			groupIdx := r.groupIdx(f.TxID)
			f.SeqNum = r.pfa.Next(seq.FlowKey{GroupIdx: groupIdx, SubtreeID: f.SubtreeID})
		} else {
			// Drive the global allocator's counter (used by tests / pacing
			// observers) without pre-stamping.
			_ = r.alloc.Next()
		}

		n, err := myframe.Encode(r.cfg.FrameVersion, f, buf)
		if err != nil {
			r.errors.Add(1)
			continue
		}
		if _, err := conn.Write(buf[:n]); err != nil {
			r.errors.Add(1)
			if errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		r.sent.Add(1)
		r.bytes.Add(uint64(n))
	}
}

func (r *Runner) logger(ctx context.Context, done <-chan struct{}) {
	t := time.NewTicker(r.cfg.LogInterval)
	defer t.Stop()
	var lastSent, lastBytes uint64
	lastTime := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case now := <-t.C:
			s := r.sent.Load()
			b := r.bytes.Load()
			dt := now.Sub(lastTime).Seconds()
			if dt <= 0 {
				continue
			}
			pps := float64(s-lastSent) / dt
			mbps := float64(b-lastBytes) * 8 / dt / 1e6
			fmt.Fprintf(os.Stderr, "[subtx-gen] sent=%d pps=%.0f mbps=%.2f errs=%d\n",
				s, pps, mbps, r.errors.Load())
			lastSent = s
			lastBytes = b
			lastTime = now
		}
	}
}

// Sent returns the total frames successfully transmitted so far.
func (r *Runner) Sent() uint64 { return r.sent.Load() }

// Errors returns the total send errors observed.
func (r *Runner) Errors() uint64 { return r.errors.Load() }
