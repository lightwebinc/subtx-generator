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
	// (the CLI default — the fabric is EF-native). Frames carrying EF
	// payloads are "BRC-128 frames".
	PayloadBRC128
	// PayloadMixed alternates between BRC-124 and BRC-128 payloads on a
	// per-frame, per-worker basis. Used to verify infrastructure handles
	// both formats coexisting on the same multicast group.
	PayloadMixed
)

// MinPayloadSize returns the smallest valid -payload-size for p. Every
// emitted payload is EXACTLY one structurally valid transaction (no trailing
// padding — padding desyncs TCP objfmt streams), so the floor is the format's
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
	pfa   *seq.PerFlowAllocator // active when GapEnabled to inject per-flow gaps

	issued atomic.Uint64 // tokens issued by the dispatcher; gates Count exactly
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
	// Exact-size payload contract: below the format minimum no valid tx of
	// the requested size exists, so refuse loudly up front rather than let
	// every worker fail (or worse, pad).
	if floor := r.cfg.PayloadFormat.MinPayloadSize(); r.cfg.PayloadSize < floor {
		return 0, fmt.Errorf("payload-size %d below %s minimum %d",
			r.cfg.PayloadSize, r.cfg.PayloadFormat, floor)
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

	// Periodic logger.
	logDone := make(chan struct{})
	go r.logger(runCtx, logDone)

	wg.Wait()
	close(logDone)
	return r.sent.Load(), nil
}

func (r *Runner) worker(ctx context.Context, id int, tokens <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	const tcpFlushBytes = 4096 // flush the TCP submit buffer once ~this many bytes of frames accumulate
	var uconn net.Conn         // ModeUnicast: udp OR tcp stream to the proxy ingress
	var tcpW *bufio.Writer     // buffered writer over uconn for the TCP lane (nil for UDP)
	var mconn *net.UDPConn     // ModeDirectMulticast egress socket
	var engine *shard.Engine
	switch r.cfg.Mode {
	case ModeUnicast:
		proto := "udp"
		if r.cfg.TCP {
			proto = "tcp"
		}
		c, err := net.Dial(proto, r.cfg.Addr)
		if err != nil {
			infof("worker %d: dial %s %s: %v", id, proto, r.cfg.Addr, err)
			return
		}
		uconn = c
		// Buffer the TCP submission stream: coalesce many small tx writes into
		// larger segments so the generator pays one syscall per BURST instead
		// of one per tx, and the proxy reads more frames per recv (denser
		// coalescing). Flushed when the token burst drains (see below), so a
		// tx is never held once the pacer has nothing more queued — latency is
		// bounded by the burst, not the buffer. UDP stays unbuffered (datagrams).
		if r.cfg.TCP {
			tcpW = bufio.NewWriterSize(uconn, 128*1024)
		}
	case ModeDirectMulticast:
		c, err := openMulticastEgress(r.cfg.EgressIface, r.cfg.BindSource)
		if err != nil {
			infof("worker %d: open mc egress: %v", id, err)
			return
		}
		mconn = c
		engine = shard.New(r.cfg.MCPrefix, r.cfg.MCGroupID, r.cfg.ShardBits)
	default:
		infof("worker %d: unknown mode %d", id, r.cfg.Mode)
		return
	}
	defer func() {
		if tcpW != nil {
			_ = tcpW.Flush() // drain any buffered tail before close (ctx cancel, count reached, error)
		}
		if uconn != nil {
			_ = uconn.Close()
		}
		if mconn != nil {
			_ = mconn.Close()
		}
	}()

	// Pre-compute the per-worker source-IPv6 bytes used as input to the
	// HashKey computation in direct-multicast mode.
	var srcIPv6 [16]byte
	if r.cfg.Mode == ModeDirectMulticast {
		copy(srcIPv6[:], r.cfg.BindSource.To16())
	}

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
		// payload the marker and per-input extensions are excluded — the
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
		groupIdx := r.groupIdx(f.TxID)
		if r.pfa != nil {
			f.SeqNum = r.pfa.Next(seq.FlowKey{GroupIdx: groupIdx, SubtreeID: f.SubtreeID})
		} else if r.cfg.Mode == ModeDirectMulticast {
			// Lazily build a per-flow allocator on first use when running
			// in direct-multicast without explicit gap injection.
			if r.pfa == nil {
				r.pfa = seq.NewPerFlow(seq.Config{Start: 1})
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
		var werr error
		if r.cfg.Mode == ModeDirectMulticast {
			dst := engine.Addr(groupIdx, r.cfg.EgressPort)
			_, werr = mconn.WriteToUDP(buf[:n], dst)
		} else if tcpW != nil {
			_, werr = tcpW.Write(buf[:n])
			// Batch by accumulated bytes so the generator pays ~one write
			// syscall per ~tcpFlushBytes of frames instead of one per tx, and
			// the proxy reads more frames per recv. A size threshold (not
			// len(tokens)==0) is what actually batches at -pps 0: there the
			// generator keeps pace with the pacer, so the token queue sits near
			// empty and an idle-gated flush would fire every frame. The tail
			// (< threshold) is drained by the deferred Flush at run end, so no
			// frame is dropped and -count stays exact.
			if werr == nil && tcpW.Buffered() >= tcpFlushBytes {
				werr = tcpW.Flush()
			}
		} else {
			_, werr = uconn.Write(buf[:n])
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

// Sent returns the total frames successfully transmitted so far.
func (r *Runner) Sent() uint64 { return r.sent.Load() }

// Errors returns the total send errors observed.
func (r *Runner) Errors() uint64 { return r.errors.Load() }

func infof(format string, args ...any) { slog.Info(fmt.Sprintf(format, args...)) }
