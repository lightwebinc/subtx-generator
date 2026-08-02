package sender

import (
	"context"
	"net"
	"testing"
	"time"

	common "github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"

	"github.com/lightwebinc/subtx-generator/internal/seq"
	"github.com/lightwebinc/subtx-generator/internal/subtree"
	"github.com/lightwebinc/subtx-generator/internal/tx"
)

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
// stamped TxID equals objfmt.TxID(payload) — the canonical id (ToStandard for
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
