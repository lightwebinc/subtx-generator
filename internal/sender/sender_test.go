package sender

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	common "github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"

	"github.com/lightwebinc/subtx-generator/internal/seq"
	"github.com/lightwebinc/subtx-generator/internal/subtree"
	"github.com/lightwebinc/subtx-generator/internal/tx"
)

// arrival is one frame a test sink received, and when.
type arrival struct {
	f  *common.Frame
	at time.Time
}

// frameSink collects the frames a Runner emits, over UDP or TCP.
type frameSink struct {
	addr  string
	mu    sync.Mutex
	got   []arrival
	errs  []error
	conns []net.Conn
}

func (s *frameSink) add(raw []byte) {
	now := time.Now()
	f, err := common.Decode(raw)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.errs = append(s.errs, err)
		return
	}
	s.got = append(s.got, arrival{f: f, at: now})
}

func newUDPSink(t *testing.T) *frameSink {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenPacket: %v", err)
	}
	_ = pc.(*net.UDPConn).SetReadBuffer(4 << 20)
	t.Cleanup(func() { _ = pc.Close() })
	s := &frameSink{addr: pc.LocalAddr().String()}
	go func() {
		buf := make([]byte, 64*1024)
		for {
			n, _, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			s.add(append([]byte(nil), buf[:n]...))
		}
	}()
	return s
}

// newTCPSink accepts any number of connections (the workers and the late
// sender each dial their own) and splits each stream into frames.
func newTCPSink(t *testing.T) *frameSink {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	s := &frameSink{addr: ln.Addr().String()}
	t.Cleanup(func() {
		_ = ln.Close()
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, c := range s.conns {
			_ = c.Close()
		}
	})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conns = append(s.conns, c)
			s.mu.Unlock()
			go func() {
				br := bufio.NewReader(c)
				for {
					hdr := make([]byte, common.HeaderSize)
					if _, err := io.ReadFull(br, hdr); err != nil {
						return
					}
					raw := make([]byte, common.HeaderSize+int(binary.BigEndian.Uint32(hdr[88:92])))
					copy(raw, hdr)
					if _, err := io.ReadFull(br, raw[common.HeaderSize:]); err != nil {
						return
					}
					s.add(raw)
				}
			}()
		}
	}()
	return s
}

// wait returns the frames received once n have arrived, or at the timeout.
func (s *frameSink) wait(t *testing.T, n int, timeout time.Duration) []arrival {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		s.mu.Lock()
		got := len(s.got)
		s.mu.Unlock()
		if got >= n || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.errs) > 0 {
		t.Fatalf("sink: %d undecodable frames, first: %v", len(s.errs), s.errs[0])
	}
	return append([]arrival(nil), s.got...)
}

// flowID is the listener's view of a flow: the group the TxID shards to,
// and the SubtreeID.
type flowID struct {
	group   uint32
	subtree [32]byte
}

func groupOf(txid [32]byte, bits uint) uint32 {
	if bits == 0 {
		return 0
	}
	return binary.BigEndian.Uint32(txid[:4]) >> (32 - bits)
}

// checkGapFills asserts that every flow in frames carries each sequence
// number from 1 to its highest exactly once: each number a gap skipped
// arrived later, in a frame of that same flow (same group and subtree). With
// strict (one worker, so the per-flow numbering is deterministic) it also
// checks which numbers were skipped, that each arrived at least about delay
// after the frame that opened its gap, and that the fills went out while the
// stream ran rather than all at its end. Returns the number of skipped
// numbers found (strict only).
func checkGapFills(t *testing.T, frames []arrival, bits uint, every, size uint64,
	delay time.Duration, payloadSize int, strict bool) (skipped int) {
	t.Helper()
	type rx struct {
		idx int
		at  time.Time
	}
	flows := map[flowID]map[uint64]rx{}
	for i, a := range frames {
		if len(a.f.Payload) != payloadSize {
			t.Fatalf("frame %d: payload %d bytes, want %d", i, len(a.f.Payload), payloadSize)
		}
		if id, err := objfmt.TxID(a.f.Payload); err != nil || id != a.f.TxID {
			t.Fatalf("frame %d: TxID is not the payload's canonical id (err %v)", i, err)
		}
		k := flowID{groupOf(a.f.TxID, bits), a.f.SubtreeID}
		m := flows[k]
		if m == nil {
			m = map[uint64]rx{}
			flows[k] = m
		}
		if _, dup := m[a.f.SeqNum]; dup {
			t.Fatalf("flow %d/%x: seq %d arrived twice", k.group, k.subtree[:4], a.f.SeqNum)
		}
		m[a.f.SeqNum] = rx{i, a.at}
	}
	period := every + size
	lastRegular, firstFill := -1, len(frames)
	for k, m := range flows {
		var highest uint64
		for s := range m {
			highest = max(highest, s)
		}
		for s := uint64(1); s <= highest; s++ {
			if _, ok := m[s]; !ok {
				t.Errorf("flow %d/%x: seq %d never arrived (highest %d): its gap was never filled",
					k.group, k.subtree[:4], s, highest)
			}
		}
		if !strict {
			continue
		}
		for s, r := range m {
			if (s-1)%period < every {
				lastRegular = max(lastRegular, r.idx)
				continue
			}
			skipped++
			firstFill = min(firstFill, r.idx)
			opener := (s-1)/period*period + every
			o, ok := m[opener]
			if !ok {
				t.Errorf("flow %d/%x: seq %d arrived but seq %d, which opened its gap, did not",
					k.group, k.subtree[:4], s, opener)
				continue
			}
			if d := r.at.Sub(o.at); d < delay/2 {
				t.Errorf("flow %d/%x: seq %d arrived %v after seq %d opened its gap, want about %v",
					k.group, k.subtree[:4], s, d, opener, delay)
			}
		}
	}
	if strict && skipped > 0 && firstFill > lastRegular {
		t.Errorf("every gap fill arrived after the last regular frame: fills must go out as they come due")
	}
	return skipped
}

// TestRunSendsDelayedGapFills is the end-to-end check of -seq-gap-delay:
// the output stream must carry each skipped sequence number in a late frame
// of its own flow once the delay has elapsed, so no flow keeps a hole, and
// Run returns only after the last fill is out.
func TestRunSendsDelayedGapFills(t *testing.T) {
	t.Parallel()
	const (
		bits  = 2
		every = 5
		size  = 2
		delay = 150 * time.Millisecond
		psize = 256
	)
	cases := []struct {
		name     string
		tcp      bool
		workers  int
		count    uint64
		duration time.Duration
		strict   bool
	}{
		{name: "udp", workers: 1, count: 600, strict: true},
		{name: "tcp", tcp: true, workers: 1, count: 600, strict: true},
		{name: "udp-4-workers-duration", workers: 4, duration: 400 * time.Millisecond},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			var sink *frameSink
			if c.tcp {
				sink = newTCPSink(t)
			} else {
				sink = newUDPSink(t)
			}
			cfg := Config{
				Mode: ModeUnicast, Addr: sink.addr, TCP: c.tcp, FrameVersion: 2,
				Workers: c.workers, PPS: 1000, Count: c.count, Duration: c.duration,
				PayloadSize: psize, PayloadFormat: PayloadMixed, ShardBits: bits,
			}
			alloc := seq.New(seq.Config{Start: 1, GapEvery: every, GapSize: size, GapDelay: delay})
			r := New(cfg, subtree.New(4, []byte("gap-delay-"+c.name)), alloc)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			start := time.Now()
			sent, err := r.Run(ctx)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if el := time.Since(start); el < delay {
				t.Errorf("Run returned after %v, before the %v gap delay could elapse", el, delay)
			}
			frames := sink.wait(t, int(sent), 5*time.Second)
			if uint64(len(frames)) != sent {
				t.Fatalf("sink got %d frames, Run reports %d sent", len(frames), sent)
			}
			skipped := checkGapFills(t, frames, bits, every, size, delay, psize, c.strict)
			if c.strict {
				if skipped == 0 {
					t.Fatal("no gap was injected; the test exercises nothing")
				}
				if got := uint64(len(frames)) - c.count; got != uint64(skipped) {
					t.Errorf("%d frames beyond -count, want one per skipped number (%d)", got, skipped)
				}
			}
		})
	}
}

// A v1 (BRC-12) header has no SeqNum, so gap injection on v1 would send a
// clean stream while claiming gaps: Run must refuse it. v1 without gaps runs.
func TestRunRefusesGapsOnFrameV1(t *testing.T) {
	t.Parallel()
	sink := newUDPSink(t)
	cfg := Config{
		Mode: ModeUnicast, Addr: sink.addr, FrameVersion: 1, Workers: 1,
		Count: 20, PayloadSize: 256, PayloadFormat: PayloadBRC124, ShardBits: 2,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, gc := range []seq.Config{
		{Start: 1, GapEvery: 5, GapSize: 1},
		{Start: 1, GapEvery: 5, GapSize: 1, GapDelay: 10 * time.Millisecond},
	} {
		r := New(cfg, subtree.New(4, []byte("v1")), seq.New(gc))
		sent, err := r.Run(ctx)
		if err == nil {
			t.Fatalf("gaps %+v: Run sent %d v1 frames, want an error", gc, sent)
		}
		if !strings.Contains(err.Error(), "frame version 2") {
			t.Errorf("gaps %+v: error %q does not name frame version 2", gc, err)
		}
	}
	r := New(cfg, subtree.New(4, []byte("v1")), seq.New(seq.Config{Start: 1}))
	if sent, err := r.Run(ctx); err != nil || sent != cfg.Count {
		t.Fatalf("v1 without gaps: sent %d, err %v; want %d, nil", sent, err, cfg.Count)
	}
}

// Canceling the context stops the late sender at once: waiting out the last
// fills applies only to a run that ends on its own (-count or -duration).
func TestRunCancelSkipsTheGapFillDrain(t *testing.T) {
	t.Parallel()
	sink := newUDPSink(t)
	cfg := Config{
		Mode: ModeUnicast, Addr: sink.addr, FrameVersion: 2, Workers: 1,
		Count: 100, PayloadSize: 256, ShardBits: 2,
	}
	r := New(cfg, subtree.New(4, []byte("cancel")),
		seq.New(seq.Config{Start: 1, GapEvery: 2, GapSize: 1, GapDelay: time.Hour}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	time.AfterFunc(300*time.Millisecond, cancel)
	start := time.Now()
	if _, err := r.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if el := time.Since(start); el > 10*time.Second {
		t.Fatalf("Run took %v; a canceled run must not wait out the gap delay", el)
	}
}

func TestModeIotaValues(t *testing.T) {
	t.Parallel()
	// Iota ordering is part of the public API (Config.Mode is serialised
	// in operator-facing logs and metrics).
	if ModeUnicast != 0 {
		t.Errorf("ModeUnicast = %d, want 0", ModeUnicast)
	}
	if ModeDirectMulticast != 1 {
		t.Errorf("ModeDirectMulticast = %d, want 1", ModeDirectMulticast)
	}
}

func TestOpenMulticastEgress_BindSource(t *testing.T) {
	t.Parallel()
	// Binding to ::1 must succeed on every Linux host; lo is always
	// present and the loopback address is always assigned to it.
	uc, err := openMulticastEgress(nil, net.ParseIP("::1"))
	if err != nil {
		t.Fatalf("openMulticastEgress(nil, ::1): %v", err)
	}
	defer func() { _ = uc.Close() }()
	la, ok := uc.LocalAddr().(*net.UDPAddr)
	if !ok {
		t.Fatalf("LocalAddr type = %T, want *net.UDPAddr", uc.LocalAddr())
	}
	if !la.IP.Equal(net.ParseIP("::1")) {
		t.Errorf("bound IP = %s, want ::1", la.IP)
	}
}

// TestRunCountWithZeroDuration guards the -duration default fix: with
// Duration=0 the run must continue until exactly Count frames are emitted
// (not stop early on a timeout). A non-zero default here previously
// truncated -count runs silently.
func TestRunCountWithZeroDuration(t *testing.T) {
	t.Parallel()

	// Loopback UDP sink so worker writes succeed and increment sent.
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenPacket: %v", err)
	}
	defer func() { _ = pc.Close() }()
	// Drain so the socket buffer never blocks the sender.
	go func() {
		buf := make([]byte, 2048)
		for {
			if _, _, derr := pc.ReadFrom(buf); derr != nil {
				return
			}
		}
	}()

	const want = 5000
	cfg := Config{
		Mode:         ModeUnicast,
		Addr:         pc.LocalAddr().String(),
		FrameVersion: 2,
		Workers:      4,
		PPS:          0, // unlimited: count is the only stop condition
		Duration:     0, // run until Count is reached
		Count:        want,
		PayloadSize:  256,
	}
	r := New(cfg, subtree.New(4, []byte("test-seed")), seq.New(seq.Config{Start: 1}))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	got, err := r.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if ctx.Err() != nil {
		t.Fatalf("Run hit the safety timeout; Duration=0 did not stop on Count")
	}
	if got != want {
		t.Errorf("emitted %d frames, want exactly %d", got, want)
	}
}

func TestOpenMulticastEgress_NoBindSource(t *testing.T) {
	t.Parallel()
	uc, err := openMulticastEgress(nil, nil)
	if err != nil {
		t.Fatalf("openMulticastEgress(nil, nil): %v", err)
	}
	defer func() { _ = uc.Close() }()
}

// TestRunRejectsPayloadBelowFormatMinimum guards the exact-size contract: a
// -payload-size below the format minimum must fail loudly, never pad.
func TestRunRejectsPayloadBelowFormatMinimum(t *testing.T) {
	t.Parallel()
	cases := []struct {
		pf   PayloadFormat
		size int
	}{
		{PayloadBRC124, tx.MinRawSize - 1},
		{PayloadBRC128, tx.MinEFSize - 1},
		{PayloadMixed, tx.MinEFSize - 1}, // mixed emits both formats → EF floor
	}
	for _, c := range cases {
		cfg := Config{
			Mode: ModeUnicast, Addr: "[::1]:9", FrameVersion: 2,
			Workers: 1, Count: 1, PayloadSize: c.size, PayloadFormat: c.pf,
		}
		r := New(cfg, subtree.New(1, []byte("s")), seq.New(seq.Config{Start: 1}))
		if _, err := r.Run(context.Background()); err == nil {
			t.Errorf("format=%s size=%d: Run succeeded, want minimum-size error", c.pf, c.size)
		}
	}
}

// TestWireFramesCarryExactEFTxAndCanonicalTxID captures emitted datagrams and
// asserts, per frame: the payload is EXACTLY one valid tx of the requested
// size (the property whose violation desyncs TCP objfmt streams) and the
// stamped TxID equals objfmt.TxID(payload), the canonical id (ToStandard for
// EF), matching what the proxy's bare-tx path stamps and consumers dedup on.
func TestWireFramesCarryExactEFTxAndCanonicalTxID(t *testing.T) {
	t.Parallel()

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenPacket: %v", err)
	}
	defer func() { _ = pc.Close() }()

	const (
		count = 16
		size  = 256
	)
	frames := make(chan []byte, count)
	go func() {
		buf := make([]byte, 4096)
		for {
			n, _, derr := pc.ReadFrom(buf)
			if derr != nil {
				close(frames)
				return
			}
			frames <- append([]byte(nil), buf[:n]...)
		}
	}()

	for _, pf := range []PayloadFormat{PayloadBRC128, PayloadBRC124, PayloadMixed} {
		cfg := Config{
			Mode: ModeUnicast, Addr: pc.LocalAddr().String(), FrameVersion: 2,
			Workers: 2, Count: count, PayloadSize: size, PayloadFormat: pf,
		}
		r := New(cfg, subtree.New(4, []byte("wire-test")), seq.New(seq.Config{Start: 1}))
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if _, err := r.Run(ctx); err != nil {
			cancel()
			t.Fatalf("format=%s: Run: %v", pf, err)
		}
		cancel()

		for i := 0; i < count; i++ {
			var raw []byte
			select {
			case raw = <-frames:
			case <-time.After(5 * time.Second):
				t.Fatalf("format=%s: frame %d not received", pf, i)
			}
			f, derr := common.Decode(raw)
			if derr != nil {
				t.Fatalf("format=%s frame %d: decode: %v", pf, i, derr)
			}
			n, terr := objfmt.TxSize(f.Payload)
			if terr != nil {
				t.Fatalf("format=%s frame %d: objfmt.TxSize: %v", pf, i, terr)
			}
			if n != size || len(f.Payload) != size {
				t.Fatalf("format=%s frame %d: payload len %d, tx walks %d, want exactly %d",
					pf, i, len(f.Payload), n, size)
			}
			id, ierr := objfmt.TxID(f.Payload)
			if ierr != nil {
				t.Fatalf("format=%s frame %d: objfmt.TxID: %v", pf, i, ierr)
			}
			if id != f.TxID {
				t.Fatalf("format=%s frame %d: stamped TxID %x != canonical objfmt.TxID %x",
					pf, i, f.TxID[:8], id[:8])
			}
		}
	}
}
