// Package seq provides a shared sequence-number allocator with optional
// gap injection for testing listener-side NACK/retransmit behaviour.
//
// Gap model:
//
//   - GapEvery N: every N-th allocation skips GapSize sequence numbers.
//     The skipped numbers are either permanently missing (GapDelay == 0)
//     or queued for delayed retransmission after GapDelay has elapsed.
//
// The allocator is lock-free for the fast path (atomic.Uint64) and uses a
// small mutex-protected queue for deferred gap records. Safe for concurrent
// use by many workers.
package seq

import (
	"sync"
	"sync/atomic"
	"time"
)

// Allocator hands out monotonically-increasing sequence numbers, optionally
// with injected gaps.
type Allocator struct {
	next atomic.Uint64

	gapEvery uint64
	gapSize  uint64
	gapDelay time.Duration

	counter atomic.Uint64 // alloc count, drives gapEvery pacing

	mu      sync.Mutex
	pending []pendingGap // deferred retransmissions
}

type pendingGap struct {
	seqs  []uint64
	dueAt time.Time
}

// Config controls gap injection.
type Config struct {
	Start    uint64        // first sequence number
	GapEvery uint64        // 0 = disabled
	GapSize  uint64        // skip this many seq numbers per gap
	GapDelay time.Duration // 0 = permanent gap; otherwise retransmit after delay
}

// New returns a new Allocator.
func New(cfg Config) *Allocator {
	a := &Allocator{
		gapEvery: cfg.GapEvery,
		gapSize:  cfg.GapSize,
		gapDelay: cfg.GapDelay,
	}
	start := cfg.Start
	if start == 0 {
		start = 1
	}
	a.next.Store(start)
	return a
}

// Next returns the next sequence number. On gap-injection cycles, it also
// advances the underlying counter by GapSize and (if GapDelay > 0) queues
// the skipped numbers for delayed retransmission via [DueRetransmits].
func (a *Allocator) Next() uint64 {
	s := a.next.Add(1) - 1
	if a.gapEvery == 0 || a.gapSize == 0 {
		return s
	}
	if n := a.counter.Add(1); n%a.gapEvery == 0 {
		// Reserve gapSize skipped numbers.
		gap := make([]uint64, 0, a.gapSize)
		for i := uint64(0); i < a.gapSize; i++ {
			gap = append(gap, a.next.Add(1)-1)
		}
		if a.gapDelay > 0 {
			a.mu.Lock()
			a.pending = append(a.pending, pendingGap{
				seqs:  gap,
				dueAt: time.Now().Add(a.gapDelay),
			})
			a.mu.Unlock()
		}
	}
	return s
}

// DueRetransmits returns and removes any gap entries whose retransmit delay
// has elapsed. Returns a flat list of sequence numbers to send now.
func (a *Allocator) DueRetransmits(now time.Time) []uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.pending) == 0 {
		return nil
	}
	var due []uint64
	keep := a.pending[:0]
	for _, p := range a.pending {
		if !now.Before(p.dueAt) {
			due = append(due, p.seqs...)
		} else {
			keep = append(keep, p)
		}
	}
	a.pending = keep
	return due
}

// GapEnabled reports whether gap injection is active (GapEvery > 0 and GapSize > 0).
// When true, callers should pre-stamp the frame's SeqNum with the value returned
// by Next() so the proxy passes the frame through verbatim; the gaps in the
// sequence are the desired missing frames at the listener.
func (a *Allocator) GapEnabled() bool {
	return a.gapEvery != 0 && a.gapSize != 0
}

// GapConfig returns the underlying gap-injection configuration. Useful for
// constructing a [PerFlowAllocator] that mirrors a global [Allocator]'s
// pattern on a per-flow basis.
func (a *Allocator) GapConfig() Config {
	return Config{GapEvery: a.gapEvery, GapSize: a.gapSize, GapDelay: a.gapDelay}
}

// Pending returns the number of gap groups still awaiting retransmission.
func (a *Allocator) Pending() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.pending)
}

// FlowKey identifies a (groupIdx, subtreeID) flow for per-flow sequence
// allocation in [PerFlowAllocator].
type FlowKey struct {
	GroupIdx  uint32
	SubtreeID [32]byte
}

// PerFlowAllocator hands out monotonically-increasing per-flow sequence
// numbers, with optional per-flow gap injection. It is intended for use with
// the proxy's per-(sender, group, subtree) flow counter so the sender can
// pre-stamp SeqNum values that match what the proxy would otherwise stamp,
// while still injecting deterministic gaps in each individual flow.
//
// Each distinct FlowKey lazily gets its own underlying [Allocator] sharing
// the same Config (Start/GapEvery/GapSize/GapDelay).
type PerFlowAllocator struct {
	cfg Config

	mu     sync.Mutex
	allocs map[FlowKey]*Allocator
}

// NewPerFlow returns a new PerFlowAllocator using cfg for every flow.
func NewPerFlow(cfg Config) *PerFlowAllocator {
	return &PerFlowAllocator{
		cfg:    cfg,
		allocs: make(map[FlowKey]*Allocator),
	}
}

// Next returns the next sequence number for the given flow, creating a new
// underlying [Allocator] for the flow on first use.
func (p *PerFlowAllocator) Next(k FlowKey) uint64 {
	p.mu.Lock()
	a, ok := p.allocs[k]
	if !ok {
		a = New(p.cfg)
		p.allocs[k] = a
	}
	p.mu.Unlock()
	return a.Next()
}

// GapEnabled reports whether gap injection is active for this allocator.
func (p *PerFlowAllocator) GapEnabled() bool {
	return p.cfg.GapEvery != 0 && p.cfg.GapSize != 0
}
