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
	"bufio"
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"

	common "github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/shard-common/seqhash"
	"github.com/lightwebinc/shard-common/shard"

	myframe "github.com/lightwebinc/subtx-generator/internal/frame"
	"github.com/lightwebinc/subtx-generator/internal/rate"
	"github.com/lightwebinc/subtx-generator/internal/seq"
	"github.com/lightwebinc/subtx-generator/internal/subtree"
	"github.com/lightwebinc/subtx-generator/internal/tx"
)

// PayloadFormat selects the BSV transaction encoding written into each
// frame's payload field. The frame header (BRC-12 vs BRC-124) is governed
// independently by Config.FrameVersion.
type PayloadFormat int

const (
	// PayloadBRC124 emits BRC-12 raw transaction payloads (legacy/miner
	// lanes). Frames carrying BRC-124 payloads are "BRC-124 frames".
	PayloadBRC124 PayloadFormat = iota
	// PayloadBRC128 emits BRC-30 Extended Format (EF) transaction payloads
	// (the CLI default, the fabric is EF-native). Frames carrying EF
	// payloads are "BRC-128 frames".
	PayloadBRC128
	// PayloadMixed alternates between BRC-124 and BRC-128 payloads on a
	// per-frame, per-worker basis. Used to verify infrastructure handles
	// both formats coexisting on the same multicast group.
	PayloadMixed
)

// MinPayloadSize returns the smallest valid -payload-size for p. Every
// emitted payload is EXACTLY one structurally valid transaction (no trailing
// padding, padding desyncs TCP objfmt streams), so the floor is the format's
// minimum tx size. PayloadMixed emits both formats at one size and therefore
// takes the larger (EF) floor.
func (p PayloadFormat) MinPayloadSize() int {
	switch p {
	case PayloadBRC128, PayloadMixed:
		return tx.MinEFSize
	default:
		return tx.MinRawSize
	}
}

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

// Mode selects how frames are emitted.
type Mode uint8

const (
	// ModeUnicast (default) opens one UDP socket per worker and
	// unicasts every frame to Config.Addr (the shard-proxy ingress
	// port). The proxy stamps SeqNum/HashKey before multicasting.
	ModeUnicast Mode = iota

	// ModeDirectMulticast skips the proxy. Each worker opens an IPv6
	// multicast egress socket bound to Config.BindSource on
	// Config.EgressIface, derives the destination group from the TxID
	// via a shard.Engine, and writes directly to that (S=BindSource, G)
	// group address. The generator stamps SeqNum (per-flow via the
	// internal allocator, matching the proxy's BRC-128 stamping
	// semantics) and HashKey (XXH64(BindSource ∥ groupIdx ∥ subtreeID))
	// so SSM listeners see deterministic flows and gap detection works
	// without a proxy in the loop. Operators MUST add Config.BindSource
	// to the shard-manifest publishers list so receivers' (S,G) joins
	// include this generator.
	ModeDirectMulticast
)

// Config tunes the sender.
type Config struct {
	Mode Mode   // ModeUnicast (default) | ModeDirectMulticast
	Addr string // unicast target host:port (ModeUnicast only)
	// TCP submits over the proxy's TCP framed submission lane (the standard 8725
	// lane: a stream of BRC frames, no envelope) instead of UDP. ModeUnicast only.
	// UDP submission is deprecated; TCP is the supported submit transport.
	TCP             bool
	FrameVersion    myframe.Version
	Workers         int
	PPS             int
	Duration        time.Duration // 0 = run until Count frames sent or ctx canceled
	Count           uint64        // 0 = unlimited
	PayloadSize     int
	PayloadFormat   PayloadFormat
	LogInterval     time.Duration
	CorruptTxIDRate uint // percentage of frames to corrupt TxID (0-100)
	ShardBits       uint // shard-bits used to compute per-flow groupIdx

	// ModeDirectMulticast fields.
	EgressIface *net.Interface // outbound NIC for multicast egress
	BindSource  net.IP         // IPv6 source bound on every egress socket; included in HashKey
	MCPrefix    uint16         // upper 16 bits of the IPv6 group address (e.g. 0xFF35 for SSM/site)
	MCGroupID   uint16         // IANA group-id (bytes 12-13); default 0x000B
	EgressPort  int            // destination UDP port written into every multicast datagram
}

// Runner ties together the pacer, seq allocator, subtree pool, and worker pool.
type Runner struct {
	cfg   Config
	pool  *subtree.Pool
	alloc *seq.Allocator
	pfa   *seq.PerFlowAllocator // per-flow SeqNums: gap injection or direct-multicast; else nil

	issued atomic.Uint64 // tokens issued by the dispatcher; gates Count exactly
	sent   atomic.Uint64 // every frame written, late gap fills included
	late   atomic.Uint64 // late gap fills written (-seq-gap-delay)
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
	// Direct-multicast bypasses the proxy, so the generator stamps per-flow
	// SeqNums itself even without gap injection. Built here, once, because
	// every worker shares it.
	if r.pfa == nil && cfg.Mode == ModeDirectMulticast {
		r.pfa = seq.NewPerFlow(seq.Config{Start: 1})
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
// With delayed gaps (seq.Config.GapDelay) it then keeps going until every
// queued late gap fill is sent, unless ctx is canceled: the last fill comes
// due up to GapDelay after the last regular frame, and building and sending
// the fills can take longer than that.
// Returns the number of frames transmitted, late gap fills included.
func (r *Runner) Run(ctx context.Context) (uint64, error) {
	// Exact-size payload contract: below the format minimum no valid tx of
	// the requested size exists, so refuse loudly up front rather than let
	// every worker fail (or worse, pad).
	if floor := r.cfg.PayloadFormat.MinPayloadSize(); r.cfg.PayloadSize < floor {
		return 0, fmt.Errorf("payload-size %d below %s minimum %d",
			r.cfg.PayloadSize, r.cfg.PayloadFormat, floor)
	}
	// A version 1 (BRC-12) header has no SeqNum field, so every injected gap
	// would vanish at encode time and the run would look like a clean one.
	if r.pfa != nil && r.pfa.GapEnabled() && r.cfg.FrameVersion == myframe.V1 {
		return 0, errors.New("gap injection needs frame version 2: a version 1 header carries no SeqNum")
	}

	// The late sender's socket and PRNG are set up before any frame goes
	// out: a late sender that cannot start fails the run, instead of the
	// run completing with every delayed gap left open.
	var lateEm *emitter
	var lateB *lateBuilder
	if r.pfa != nil && r.pfa.Delayed() {
		em, err := r.openEmitter()
		if err != nil {
			return 0, fmt.Errorf("late sender: %w", err)
		}
		var seed [32]byte
		if _, err := cryptorand.Read(seed[:]); err != nil {
			em.close()
			return 0, fmt.Errorf("late sender: seed: %w", err)
		}
		lateEm, lateB = em, r.newLateBuilder(seed)
	}

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
			// Gate on tokens already issued (not on frames sent): a worker
			// increments sent only after the write completes, so gating on
			// sent races the dispatcher ahead and over-issues, letting
			// workers overshoot Count. Issuing exactly Count tokens caps it.
			if r.cfg.Count > 0 && r.issued.Load() >= r.cfg.Count {
				return
			}
			if !pacer.Wait() {
				return
			}
			select {
			case tokens <- struct{}{}:
				r.issued.Add(1)
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

	// Late gap fills: one goroutine on its own socket sends a frame for each
	// skipped number as its GapDelay elapses, outside the pacer. It outlives
	// the regular stream (streamDone) until the queue is empty, so the gaps
	// opened by the last frames are filled too; only ctx stops it early.
	streamDone := make(chan struct{})
	var lateWG sync.WaitGroup
	if lateEm != nil {
		lateWG.Add(1)
		go func() {
			defer lateWG.Done()
			defer lateEm.close()
			r.sendLate(ctx, lateEm, lateB, streamDone)
		}()
	}

	// Periodic logger.
	logDone := make(chan struct{})
	go r.logger(runCtx, logDone)

	wg.Wait()
	close(streamDone)
	lateWG.Wait()
	close(logDone)
	return r.sent.Load(), nil
}

func (r *Runner) worker(ctx context.Context, id int, tokens <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	const tcpFlushBytes = 4096 // flush the TCP submit buffer once ~this many bytes of frames accumulate
	em, err := r.openEmitter()
	if err != nil {
		infof("worker %d: %v", id, err)
		return
	}
	defer em.close() // drains any buffered TCP tail (ctx cancel, count reached, error)

	srcIPv6 := r.bindSource16()
	delayed := r.pfa != nil && r.pfa.Delayed()

	// Per-worker PRNG seed.
	var seed [32]byte
	if _, err := cryptorand.Read(seed[:]); err != nil {
		infof("worker %d: seed: %v", id, err)
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

		// Each token corresponds to exactly one frame the dispatcher has
		// already counted against Count (via r.issued), so every received
		// token must be processed; no per-worker Count re-check here.

		// Random payload (format chosen per Config.PayloadFormat).
		useEF := false
		switch r.cfg.PayloadFormat {
		case PayloadBRC128:
			useEF = true
		case PayloadMixed:
			useEF = local%2 == 1
		}
		local++
		var berr error
		if useEF {
			payload, berr = builder.BuildEF(payload[:0:cap(payload)], r.cfg.PayloadSize)
		} else {
			payload, berr = builder.Build(payload[:0:cap(payload)], r.cfg.PayloadSize)
		}
		if berr != nil {
			// Cannot happen after the Run() size validation; treat as a
			// hard misconfiguration rather than a silent per-frame skip.
			infof("worker %d: build payload: %v", id, berr)
			r.errors.Add(1)
			r.issued.Add(^uint64(0)) // release the Count slot
			return
		}
		f.Payload = payload

		// Stamp the canonical TxID exactly as the system derives it
		// (objfmt.TxID = SHA256d over the STANDARD serialization): for a
		// BRC-12 raw payload that is SHA256d(payload); for a BRC-30 EF
		// payload the marker and per-input extensions are excluded, the
		// same id the proxy stamps on the bare-tx push path
		// (objfmt.MulticastFrame) and the id consumers dedup on. Hashing
		// the raw EF bytes instead would stamp an id no other component
		// ever derives.
		id, ierr := objfmt.TxID(payload)
		if ierr != nil {
			r.errors.Add(1)
			r.issued.Add(^uint64(0)) // release the Count slot
			continue
		}
		f.TxID = id

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

		// Stamp SeqNum:
		//   - Unicast mode: the proxy stamps per-flow monotonic SeqNums;
		//     leave 0 unless gap injection is active.
		//   - DirectMulticast mode: the proxy is bypassed, so the
		//     generator MUST stamp per-flow SeqNums itself.
		// Either way r.pfa (built in New) is the per-flow allocator.
		groupIdx := r.groupIdx(f.TxID)
		if r.pfa != nil {
			// With delayed gaps, wait while the late sender is MaxPending
			// fills behind, before taking a SeqNum: the stream slows to what
			// the fills can keep up with, and stopping here (ctx done)
			// leaves no allocated number unsent.
			if delayed {
				if err := r.pfa.Throttle(ctx); err != nil {
					return
				}
			}
			f.SeqNum = r.pfa.Next(seq.FlowKey{GroupIdx: groupIdx, SubtreeID: f.SubtreeID})
		} else {
			_ = r.alloc.Next()
		}

		// In direct-multicast mode stamp HashKey from BindSource so the
		// listener-side per-flow tracker keys on a stable identity.
		if r.cfg.Mode == ModeDirectMulticast {
			f.HashKey = seqhash.Hash(srcIPv6, groupIdx, f.SubtreeID)
		}

		n, err := myframe.Encode(r.cfg.FrameVersion, f, buf)
		if err != nil {
			r.errors.Add(1)
			// Token spent without a successful send; release the Count slot
			// so the dispatcher reissues and we still emit exactly Count.
			r.issued.Add(^uint64(0))
			continue
		}
		werr := em.write(buf[:n], groupIdx)
		// TCP lane: two flush triggers with different jobs. The size
		// threshold batches the unpaced blast (-pps 0): there the token queue
		// sits near empty by construction, so an idle-gated flush would fire
		// every frame and defeat batching. The paced-idle flush (PPS > 0, no
		// token queued) bounds per-frame LATENCY at paced rates: with
		// per-worker connections a paced worker may buffer for many seconds
		// before reaching the size threshold, frames must leave when the
		// pacer has nothing more queued, or a paced run delivers only
		// end-of-run flushes. The tail (< threshold) is drained by the
		// deferred close at run end, so no frame is dropped and -count stays
		// exact.
		if werr == nil && em.tcpW != nil && (em.tcpW.Buffered() >= tcpFlushBytes || (r.cfg.PPS > 0 && len(tokens) == 0)) {
			werr = em.flush()
		}
		if werr != nil {
			r.errors.Add(1)
			if errors.Is(werr, net.ErrClosed) {
				return
			}
			r.issued.Add(^uint64(0)) // release slot; see Encode-error note above
			continue
		}
		r.sent.Add(1)
		r.bytes.Add(uint64(n))
	}
}

// emitter is one sender's transport: a UDP or TCP connection to the proxy
// (ModeUnicast) or a multicast egress socket (ModeDirectMulticast). Each
// worker, and the late sender, owns one.
type emitter struct {
	uconn  net.Conn      // ModeUnicast: udp OR tcp stream to the proxy ingress
	tcpW   *bufio.Writer // buffered writer over uconn for the TCP lane (nil for UDP)
	mconn  *net.UDPConn  // ModeDirectMulticast egress socket
	engine *shard.Engine // ModeDirectMulticast: group index -> group address
	port   int           // ModeDirectMulticast destination UDP port
}

// openEmitter dials the transport Config.Mode selects.
func (r *Runner) openEmitter() (*emitter, error) {
	switch r.cfg.Mode {
	case ModeUnicast:
		proto := "udp"
		if r.cfg.TCP {
			proto = "tcp"
		}
		c, err := net.Dial(proto, r.cfg.Addr)
		if err != nil {
			return nil, fmt.Errorf("dial %s %s: %w", proto, r.cfg.Addr, err)
		}
		e := &emitter{uconn: c}
		// Buffer the TCP submission stream: coalesce many small tx writes into
		// larger segments so the generator pays one syscall per BURST instead
		// of one per tx, and the proxy reads more frames per recv (denser
		// coalescing). Flushed when the token burst drains (see worker), so a
		// tx is never held once the pacer has nothing more queued, latency is
		// bounded by the burst, not the buffer. UDP stays unbuffered (datagrams).
		if r.cfg.TCP {
			e.tcpW = bufio.NewWriterSize(c, 128*1024)
		}
		return e, nil
	case ModeDirectMulticast:
		c, err := openMulticastEgress(r.cfg.EgressIface, r.cfg.BindSource)
		if err != nil {
			return nil, fmt.Errorf("open mc egress: %w", err)
		}
		return &emitter{
			mconn:  c,
			engine: shard.New(r.cfg.MCPrefix, r.cfg.MCGroupID, r.cfg.ShardBits),
			port:   r.cfg.EgressPort,
		}, nil
	default:
		return nil, fmt.Errorf("unknown mode %d", r.cfg.Mode)
	}
}

// write sends one encoded frame: to groupIdx's group in direct-multicast
// mode, else to the proxy (buffered on the TCP lane until flush).
func (e *emitter) write(frame []byte, groupIdx uint32) error {
	var err error
	switch {
	case e.mconn != nil:
		_, err = e.mconn.WriteToUDP(frame, e.engine.Addr(groupIdx, e.port))
	case e.tcpW != nil:
		_, err = e.tcpW.Write(frame)
	default:
		_, err = e.uconn.Write(frame)
	}
	return err
}

// flush pushes buffered TCP bytes to the proxy; a no-op for datagrams.
func (e *emitter) flush() error {
	if e.tcpW == nil {
		return nil
	}
	return e.tcpW.Flush()
}

// close drains any buffered TCP tail, then closes the socket.
func (e *emitter) close() {
	if e.tcpW != nil {
		_ = e.tcpW.Flush()
	}
	if e.uconn != nil {
		_ = e.uconn.Close()
	}
	if e.mconn != nil {
		_ = e.mconn.Close()
	}
}

// bindSource16 returns the direct-multicast source address as the HashKey
// input (zero in unicast mode, where the proxy stamps HashKey).
func (r *Runner) bindSource16() [16]byte {
	var src [16]byte
	if r.cfg.Mode == ModeDirectMulticast {
		copy(src[:], r.cfg.BindSource.To16())
	}
	return src
}

// maxGroupSearch bounds the payload builds spent finding a transaction whose
// TxID falls in a late frame's group. A hit takes 2^ShardBits builds on
// average, so the bound only bites at shard widths far past BRC-129's 12.
const maxGroupSearch = 1 << 20

// lateBuilder builds the frames that fill delayed gaps. Only the late sender
// uses it, so it owns its PRNG and buffers.
type lateBuilder struct {
	tx      *tx.Builder
	payload []byte
	buf     []byte
	frame   common.Frame
	srcIPv6 [16]byte // direct-multicast HashKey input
	built   uint64   // late frames built; drives PayloadMixed alternation
	warned  bool     // a build failure has been logged
}

func (r *Runner) newLateBuilder(seed [32]byte) *lateBuilder {
	return &lateBuilder{
		tx:      tx.New(seed),
		payload: make([]byte, r.cfg.PayloadSize),
		buf:     make([]byte, myframe.HeaderSize(r.cfg.FrameVersion)+r.cfg.PayloadSize),
		srcIPv6: r.bindSource16(),
	}
}

// buildLate encodes the frame that fills l into b.buf and returns its
// length. It carries a fresh transaction of the configured format and size
// whose TxID falls in l.Flow's group, the flow's SubtreeID, and SeqNum
// l.Seq, so it joins the same flow at the listener (the same HashKey, from
// the proxy or, in direct-multicast mode, from the generator) as the frames
// around the gap. Late frames are never TxID-corrupted: a flipped bit could
// move the TxID to another group, and so the frame to another flow. The
// group search stops with ctx's error once ctx is done.
func (r *Runner) buildLate(ctx context.Context, b *lateBuilder, l seq.Late) (int, error) {
	useEF := false
	switch r.cfg.PayloadFormat {
	case PayloadBRC128:
		useEF = true
	case PayloadMixed:
		useEF = b.built%2 == 1
	}
	b.built++
	f := &b.frame
	for i := 0; ; i++ {
		if i == maxGroupSearch {
			return 0, fmt.Errorf("no transaction in group %d after %d builds", l.Flow.GroupIdx, maxGroupSearch)
		}
		if i%256 == 255 && ctx.Err() != nil {
			return 0, ctx.Err()
		}
		var err error
		if useEF {
			b.payload, err = b.tx.BuildEF(b.payload[:0:cap(b.payload)], r.cfg.PayloadSize)
		} else {
			b.payload, err = b.tx.Build(b.payload[:0:cap(b.payload)], r.cfg.PayloadSize)
		}
		if err != nil {
			return 0, err
		}
		id, err := objfmt.TxID(b.payload)
		if err != nil {
			return 0, err
		}
		if r.groupIdx(id) == l.Flow.GroupIdx {
			f.TxID = id
			break
		}
	}
	f.Payload = b.payload
	f.SubtreeID = l.Flow.SubtreeID
	f.SeqNum = l.Seq
	if r.cfg.Mode == ModeDirectMulticast {
		f.HashKey = seqhash.Hash(b.srcIPv6, l.Flow.GroupIdx, l.Flow.SubtreeID)
	}
	return myframe.Encode(r.cfg.FrameVersion, f, b.buf)
}

// sendLate sends a frame for each delayed gap as its GapDelay elapses, on its
// own emitter, until the regular stream has ended (streamDone) and no gap is
// left queued, or ctx is canceled: ctx is checked before every fill, so a
// canceled run stops mid-batch.
func (r *Runner) sendLate(ctx context.Context, em *emitter, b *lateBuilder, streamDone <-chan struct{}) {
	var timer *time.Timer
	done := streamDone
	for {
		for _, l := range r.pfa.DueRetransmits(time.Now()) {
			if ctx.Err() != nil {
				return
			}
			r.sendOneLate(ctx, em, b, l)
		}
		if ctx.Err() != nil {
			return
		}

		at, ok := r.pfa.NextDue()
		if !ok {
			if done == nil {
				return // stream over, every gap filled
			}
			select {
			case <-ctx.Done():
				return
			case <-done:
				done = nil // no new gaps from here; drain what is queued
			case <-r.pfa.Queued():
			}
			continue
		}
		wait := time.Until(at)
		if wait <= 0 {
			continue
		}
		if timer == nil {
			timer = time.NewTimer(wait)
		} else {
			timer.Reset(wait)
		}
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// sendOneLate builds and sends the frame that fills l. The frame counts as
// sent once it has left the buffer: a datagram once written, a TCP frame once
// flushed (fills are sparse, so each gets its own flush).
func (r *Runner) sendOneLate(ctx context.Context, em *emitter, b *lateBuilder, l seq.Late) {
	n, err := r.buildLate(ctx, b, l)
	if err != nil {
		if ctx.Err() != nil {
			return // canceled mid-search: the run is stopping, not failing
		}
		r.errors.Add(1)
		if !b.warned {
			infof("late sender: seq %d of group %d: %v", l.Seq, l.Flow.GroupIdx, err)
			b.warned = true
		}
		return
	}
	if err := em.write(b.buf[:n], l.Flow.GroupIdx); err != nil {
		r.errors.Add(1)
		return
	}
	if err := em.flush(); err != nil {
		r.errors.Add(1)
		return
	}
	r.sent.Add(1)
	r.late.Add(1)
	r.bytes.Add(uint64(n))
}

// openMulticastEgress opens an IPv6 UDP socket bound to bindSource (or
// the wildcard when bindSource is nil) with IPV6_MULTICAST_IF set to
// iface. Used by ModeDirectMulticast workers as their per-worker egress
// socket.
func openMulticastEgress(iface *net.Interface, bindSource net.IP) (*net.UDPConn, error) {
	listenAddr := "[::]:0"
	if bindSource != nil {
		listenAddr = "[" + bindSource.String() + "]:0"
	}
	pc, err := net.ListenPacket("udp6", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", listenAddr, err)
	}
	uc, ok := pc.(*net.UDPConn)
	if !ok {
		_ = pc.Close()
		return nil, fmt.Errorf("unexpected conn type %T", pc)
	}
	if iface == nil {
		return uc, nil
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		_ = uc.Close()
		return nil, fmt.Errorf("SyscallConn: %w", err)
	}
	var setErr error
	if cerr := raw.Control(func(fd uintptr) {
		setErr = unix.SetsockoptInt(int(fd), unix.IPPROTO_IPV6, unix.IPV6_MULTICAST_IF, iface.Index)
	}); cerr != nil {
		_ = uc.Close()
		return nil, fmt.Errorf("control: %w", cerr)
	}
	if setErr != nil {
		_ = uc.Close()
		return nil, fmt.Errorf("IPV6_MULTICAST_IF: %w", setErr)
	}
	return uc, nil
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

// Sent returns the total frames successfully transmitted so far, late gap
// fills included.
func (r *Runner) Sent() uint64 { return r.sent.Load() }

// Late returns how many of the frames sent were late gap fills
// (seq.Config.GapDelay): frames carrying a skipped sequence number, sent once
// its delay elapsed.
func (r *Runner) Late() uint64 { return r.late.Load() }

// Errors returns the total send errors observed.
func (r *Runner) Errors() uint64 { return r.errors.Load() }

func infof(format string, args ...any) { slog.Info(fmt.Sprintf(format, args...)) }
