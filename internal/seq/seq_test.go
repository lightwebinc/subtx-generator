package seq

import (
	"testing"
	"time"
)

func TestMonotonic(t *testing.T) {
	a := New(Config{Start: 1})
	prev := uint64(0)
	for i := 0; i < 100; i++ {
		s := a.Next()
		if s <= prev {
			t.Fatalf("not monotonic: %d after %d", s, prev)
		}
		prev = s
	}
}

func TestGapPermanent(t *testing.T) {
	a := New(Config{Start: 1, GapEvery: 5, GapSize: 2, GapDelay: 0})
	seen := make(map[uint64]bool)
	for i := 0; i < 20; i++ {
		seen[a.Next()] = true
	}
	// Every 5 allocations skip 2. At 20 allocations: 4 gaps × 2 skipped = 8 missing.
	// Issued = 20; reserved-and-skipped = 8. next counter advanced by 28.
	missing := 0
	for n := uint64(1); n <= 28; n++ {
		if !seen[n] {
			missing++
		}
	}
	if missing != 8 {
		t.Errorf("want 8 missing seqs, got %d", missing)
	}
	if a.Pending() != 0 {
		t.Errorf("GapDelay=0 should not queue pending")
	}
}

// A PerFlowAllocator with a GapDelay queues every flow's skipped numbers on
// one queue, each tagged with its flow, and hands them out only once due.
func TestPerFlowDelayedGapsComeDueWithTheirFlow(t *testing.T) {
	const delay = 30 * time.Millisecond
	p := NewPerFlow(Config{Start: 1, GapEvery: 2, GapSize: 2, GapDelay: delay})
	if !p.Delayed() {
		t.Fatal("Delayed() = false with GapDelay > 0")
	}
	if _, ok := p.NextDue(); ok {
		t.Fatal("NextDue reported a gap before any allocation")
	}
	fa := FlowKey{GroupIdx: 1, SubtreeID: [32]byte{0xaa}}
	fb := FlowKey{GroupIdx: 2, SubtreeID: [32]byte{0xbb}}

	start := time.Now()
	// Flow a: 1, 2 (gap 3,4), 5. Flow b: 1, 2 (gap 3,4).
	var gotA, gotB []uint64
	for i := 0; i < 3; i++ {
		gotA = append(gotA, p.Next(fa))
	}
	for i := 0; i < 2; i++ {
		gotB = append(gotB, p.Next(fb))
	}
	if want := []uint64{1, 2, 5}; !equal(gotA, want) {
		t.Fatalf("flow a got %v, want %v", gotA, want)
	}
	if want := []uint64{1, 2}; !equal(gotB, want) {
		t.Fatalf("flow b got %v, want %v", gotB, want)
	}
	select {
	case <-p.Queued():
	default:
		t.Error("Queued() not signaled after a gap was queued")
	}
	if p.Pending() != 2 {
		t.Fatalf("Pending() = %d, want 2 gaps", p.Pending())
	}
	at, ok := p.NextDue()
	if !ok || at.Before(start.Add(delay)) {
		t.Fatalf("NextDue() = %v, %v; want ok and no earlier than start+%v", at, ok, delay)
	}
	if due := p.DueRetransmits(at.Add(-time.Nanosecond)); len(due) != 0 {
		t.Fatalf("DueRetransmits before the due time returned %v", due)
	}

	due := p.DueRetransmits(time.Now().Add(delay))
	want := []Late{{fa, 3}, {fa, 4}, {fb, 3}, {fb, 4}}
	if len(due) != len(want) {
		t.Fatalf("DueRetransmits = %v, want %v", due, want)
	}
	for i := range want {
		if due[i] != want[i] {
			t.Errorf("due[%d] = %+v, want %+v", i, due[i], want[i])
		}
	}
	if p.Pending() != 0 {
		t.Errorf("Pending() = %d after draining, want 0", p.Pending())
	}
	if _, ok := p.NextDue(); ok {
		t.Error("NextDue reported a gap after draining")
	}
}

// Without a GapDelay the skipped numbers are a permanent gap: nothing is
// queued for a late send.
func TestPerFlowPermanentGapQueuesNothing(t *testing.T) {
	p := NewPerFlow(Config{Start: 1, GapEvery: 2, GapSize: 2})
	if p.Delayed() {
		t.Fatal("Delayed() = true with GapDelay == 0")
	}
	k := FlowKey{GroupIdx: 0}
	var got []uint64
	for i := 0; i < 4; i++ {
		got = append(got, p.Next(k))
	}
	if want := []uint64{1, 2, 5, 6}; !equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if p.Pending() != 0 {
		t.Errorf("Pending() = %d, want 0", p.Pending())
	}
	if due := p.DueRetransmits(time.Now().Add(time.Hour)); len(due) != 0 {
		t.Errorf("DueRetransmits = %v, want none", due)
	}
}

func equal(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestGapDelayed(t *testing.T) {
	a := New(Config{Start: 1, GapEvery: 3, GapSize: 1, GapDelay: 5 * time.Millisecond})
	for i := 0; i < 9; i++ {
		a.Next()
	}
	if a.Pending() != 3 {
		t.Errorf("want 3 pending, got %d", a.Pending())
	}
	due := a.DueRetransmits(time.Now())
	if len(due) != 0 {
		t.Errorf("too early, got %d due", len(due))
	}
	time.Sleep(10 * time.Millisecond)
	due = a.DueRetransmits(time.Now())
	if len(due) != 3 {
		t.Errorf("want 3 due, got %d", len(due))
	}
	if a.Pending() != 0 {
		t.Errorf("expected queue drained, got %d", a.Pending())
	}
}
