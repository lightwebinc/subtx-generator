package sender

import (
	"context"
	"net"
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
			n, err := r.buildLate(b, l)
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
