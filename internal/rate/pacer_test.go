package rate

import (
	"testing"
	"time"
)

func TestNewTypeSelection(t *testing.T) {
	if _, ok := New(0).(*freePacer); !ok {
		t.Error("pps=0 should yield freePacer")
	}
	if _, ok := New(-5).(*freePacer); !ok {
		t.Error("negative pps should yield freePacer")
	}
	if _, ok := New(500).(*smoothPacer); !ok {
		t.Error("pps<=1000 should yield smoothPacer")
	}
	if _, ok := New(1000).(*smoothPacer); !ok {
		t.Error("pps=1000 should yield smoothPacer")
	}
	if _, ok := New(5000).(*burstPacer); !ok {
		t.Error("pps>1000 should yield burstPacer")
	}
}

func TestFreePacerNeverBlocks(t *testing.T) {
	p := New(0)
	defer p.Stop()
	for i := 0; i < 1000; i++ {
		if !p.Wait() {
			t.Fatalf("freePacer.Wait returned false at %d", i)
		}
	}
}

func TestSmoothPacerPeriodFloor(t *testing.T) {
	// pps so large the computed period rounds to zero must clamp to 1µs
	// rather than panicking time.NewTicker with a non-positive duration.
	p := newSmoothPacer(2_000_000_000)
	defer p.Stop()
	if !p.Wait() {
		t.Error("clamped smoothPacer should still tick")
	}
}

func TestBurstPacerBurstSize(t *testing.T) {
	if got := newBurstPacer(5000).burst; got != 5 {
		t.Errorf("burst for 5000pps = %d, want 5", got)
	}
	// pps just above 1000 floors burst to at least 1.
	if got := newBurstPacer(1500).burst; got != 1 {
		t.Errorf("burst for 1500pps = %d, want 1", got)
	}
}

func TestBurstPacerEmitsBurst(t *testing.T) {
	p := newBurstPacer(5000) // burst = 5
	defer p.Stop()

	// First Wait consumes a tick and primes the remaining counter.
	if !p.Wait() {
		t.Fatal("first Wait should return true")
	}
	if p.remaining != 4 {
		t.Fatalf("after first Wait remaining = %d, want 4", p.remaining)
	}
	// The next four tokens come from the burst without waiting on the ticker.
	for want := 3; want >= 0; want-- {
		if !p.Wait() {
			t.Fatal("burst Wait should return true")
		}
		if p.remaining != want {
			t.Fatalf("remaining = %d, want %d", p.remaining, want)
		}
	}
}

func TestBurstPacerApproximateRate(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive")
	}
	p := newBurstPacer(10000) // burst = 10 per ms
	defer p.Stop()

	const want = 200
	start := time.Now()
	for i := 0; i < want; i++ {
		p.Wait()
	}
	elapsed := time.Since(start)
	// 200 tokens at 10/ms ⇒ ~20ms of ticks. Allow generous slack for CI.
	if elapsed > 500*time.Millisecond {
		t.Errorf("200 tokens took %v, expected well under 500ms", elapsed)
	}
}
