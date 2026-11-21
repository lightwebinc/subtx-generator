package sender

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	common "github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/shard-common/seqhash"

	"github.com/lightwebinc/subtx-generator/internal/seq"
	"github.com/lightwebinc/subtx-generator/internal/subtree"
)

// A late frame must land in the flow it fills: a TxID that shards to the
// flow's group, the flow's SubtreeID, the skipped SeqNum, and in
// direct-multicast mode the HashKey the generator stamps for that flow. Its
// payload is one canonical transaction of the configured format and size.
func TestBuildLateFillsTheFlow(t *testing.T) {
	t.Parallel()
	src := net.ParseIP("2001:db8::1")
	var src16 [16]byte
	copy(src16[:], src.To16())
	const (
		bits  = 4
		psize = 200
	)
	for _, pf := range []PayloadFormat{PayloadBRC128, PayloadBRC124, PayloadMixed} {
		cfg := Config{
			Mode: ModeDirectMulticast, BindSource: src, FrameVersion: 2,
			PayloadSize: psize, PayloadFormat: pf, ShardBits: bits,
			MCPrefix: 0xFF05, MCGroupID: 0x000B, EgressPort: 9001,
		}
		r := New(cfg, subtree.New(4, []byte("late")),
			seq.New(seq.Config{Start: 1, GapEvery: 3, GapSize: 1, GapDelay: time.Millisecond}))
		b := r.newLateBuilder([32]byte{7})
		for g := uint32(0); g < 1<<bits; g++ {
			l := seq.Late{Flow: seq.FlowKey{GroupIdx: g, SubtreeID: [32]byte{byte(g), 0xee}}, Seq: 100 + uint64(g)}
			n, err := r.buildLate(context.Background(), b, l)
			if err != nil {
				t.Fatalf("%s group %d: buildLate: %v", pf, g, err)
			}
			f, err := common.Decode(b.buf[:n])
			if err != nil {
				t.Fatalf("%s group %d: decode: %v", pf, g, err)
			}
			if got := r.groupIdx(f.TxID); got != g {
				t.Errorf("%s: TxID shards to group %d, want %d", pf, got, g)
			}
			if f.SubtreeID != l.Flow.SubtreeID || f.SeqNum != l.Seq {
				t.Errorf("%s group %d: subtree %x seq %d, want %x and %d",
					pf, g, f.SubtreeID[:2], f.SeqNum, l.Flow.SubtreeID[:2], l.Seq)
			}
			if want := seqhash.Hash(src16, g, l.Flow.SubtreeID); f.HashKey != want {
				t.Errorf("%s group %d: HashKey %#x, want %#x", pf, g, f.HashKey, want)
			}
			if sz, err := objfmt.TxSize(f.Payload); err != nil || sz != psize || len(f.Payload) != psize {
				t.Errorf("%s group %d: payload %d bytes, tx walks %d (err %v), want exactly %d",
					pf, g, len(f.Payload), sz, err, psize)
			}
			if id, err := objfmt.TxID(f.Payload); err != nil || id != f.TxID {
				t.Errorf("%s group %d: TxID is not the payload's canonical id (err %v)", pf, g, err)
			}
			wantEF := pf == PayloadBRC128 || (pf == PayloadMixed && g%2 == 1)
			if objfmt.IsEF(f.Payload) != wantEF {
				t.Errorf("%s late frame %d: EF = %v, want %v", pf, g, !wantEF, wantEF)
			}
		}
	}
}

// The group search for a late frame's TxID stops once ctx is done: at a shard
// width where a hit takes about 2^30 builds, a canceled run must not spin.
func TestBuildLateStopsOnCancel(t *testing.T) {
	t.Parallel()
	cfg := Config{Mode: ModeUnicast, FrameVersion: 2, PayloadSize: 128, ShardBits: 30}
	r := New(cfg, subtree.New(1, []byte("cancel")),
		seq.New(seq.Config{Start: 1, GapEvery: 1, GapSize: 1, GapDelay: time.Millisecond}))
	b := r.newLateBuilder([32]byte{1})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	// No 128-byte tx in a 2^30 search space lands in group 2^30-1 within 256
	// builds with any real chance, so only the ctx check can end this early.
	_, err := r.buildLate(ctx, b, seq.Late{Flow: seq.FlowKey{GroupIdx: 1<<30 - 1}, Seq: 2})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("buildLate on a canceled ctx: err %v, want context.Canceled", err)
	}
	if el := time.Since(start); el > 5*time.Second {
		t.Errorf("buildLate took %v to notice the canceled ctx", el)
	}
}

// A late sender that cannot open its socket fails the run up front, rather
// than the run completing with every delayed gap left open.
func TestRunFailsWhenTheLateSenderCannotStart(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // nothing listens there now: every dial is refused
	cfg := Config{
		Mode: ModeUnicast, Addr: addr, TCP: true, FrameVersion: 2, Workers: 1,
		Duration: 200 * time.Millisecond, PayloadSize: 256, ShardBits: 2,
	}
	r := New(cfg, subtree.New(4, []byte("late-dial")),
		seq.New(seq.Config{Start: 1, GapEvery: 5, GapSize: 1, GapDelay: 50 * time.Millisecond}))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sent, err := r.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "late sender") {
		t.Fatalf("Run: sent %d, err %v; want a late sender error", sent, err)
	}
	if sent != 0 {
		t.Errorf("sent %d frames before failing", sent)
	}
}

// When the late sender falls behind, the stream waits for it: the queue of
// skipped numbers stays within MaxPending plus one gap per worker, and every
// accepted gap is still filled.
func TestRunThrottlesOnTheFillBacklog(t *testing.T) {
	t.Parallel()
	const (
		count      = 200
		workers    = 2
		maxPending = 4
	)
	sink := newUDPSink(t)
	cfg := Config{
		Mode: ModeUnicast, Addr: sink.addr, FrameVersion: 2, Workers: workers,
		PPS: 0, Count: count, PayloadSize: 256, ShardBits: 2,
	}
	r := New(cfg, subtree.New(2, []byte("backlog")), seq.New(seq.Config{
		Start: 1, GapEvery: 1, GapSize: 1, GapDelay: 20 * time.Millisecond, MaxPending: maxPending,
	}))

	var peak int
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			select {
			case <-stop:
				return
			default:
			}
			peak = max(peak, r.pfa.Pending())
			time.Sleep(100 * time.Microsecond)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sent, err := r.Run(ctx)
	close(stop)
	<-sampled
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if limit := maxPending + workers; peak > limit {
		t.Errorf("queue peaked at %d gaps, want at most %d (MaxPending + one per worker)", peak, limit)
	}
	frames := sink.wait(t, int(sent), 5*time.Second)
	if uint64(len(frames)) != sent || sent != count+r.Late() || r.Late() != count {
		t.Errorf("sent %d, late %d, sink %d: want %d regular frames and one fill each",
			sent, r.Late(), len(frames), count)
	}
	checkGapFills(t, frames, 2, 1, 1, 0, 256, false)
}

// Runner.Late counts the gap fills among the frames sent, and they are the
// frames beyond -count.
func TestRunCountsLateFills(t *testing.T) {
	t.Parallel()
	const count = 200
	sink := newUDPSink(t)
	cfg := Config{
		Mode: ModeUnicast, Addr: sink.addr, FrameVersion: 2, Workers: 1,
		PPS: 2000, Count: count, PayloadSize: 256, ShardBits: 2,
	}
	r := New(cfg, subtree.New(4, []byte("late-count")),
		seq.New(seq.Config{Start: 1, GapEvery: 4, GapSize: 1, GapDelay: 50 * time.Millisecond}))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sent, err := r.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	frames := sink.wait(t, int(sent), 5*time.Second)
	if r.Late() == 0 {
		t.Fatal("Late() = 0 with delayed gaps injected")
	}
	if sent != count+r.Late() || uint64(len(frames)) != sent || r.Sent() != sent {
		t.Errorf("sent %d (Sent %d), late %d, sink got %d: want sent = %d + late = sink",
			sent, r.Sent(), r.Late(), len(frames), count)
	}
}
