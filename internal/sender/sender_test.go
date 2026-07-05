package sender

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lightwebinc/subtx-generator/internal/seq"
	"github.com/lightwebinc/subtx-generator/internal/subtree"
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
