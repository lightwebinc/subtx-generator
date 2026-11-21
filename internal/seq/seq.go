// Package seq provides a shared sequence-number allocator with optional
// gap injection for testing listener-side NACK/retransmit behavior.
//
// Gap model:
//
//   - GapEvery N: every N-th allocation skips GapSize sequence numbers.
//     The skipped numbers are either permanently missing (GapDelay == 0)
//     or queued for delayed retransmission after GapDelay has elapsed.
//
// The allocator is lock-free for the fast path (atomic.Uint64) and uses a
// small mutex-protected queue for deferred gap records. Safe for concurrent
// use by many workers. [PerFlowAllocator] keeps one queue across all its
// flows, which the sender drains with [PerFlowAllocator.DueRetransmits].
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
	s, gap := a.alloc()
	if gap != nil {
		a.mu.Lock()
		a.pending = append(a.pending, pendingGap{
			seqs:  gap,
			dueAt: time.Now().Add(a.gapDelay),
		})
		a.mu.Unlock()
	}
	return s
}

// alloc returns the next sequence number. On a gap-injection cycle it also
// skips GapSize numbers and, when GapDelay > 0, returns them for the caller
// to queue for a late send; otherwise the returned slice is nil.
func (a *Allocator) alloc() (uint64, []uint64) {
	s := a.next.Add(1) - 1
	if a.gapEvery == 0 || a.gapSize == 0 {
		return s, nil
	}
	if n := a.counter.Add(1); n%a.gapEvery != 0 {
		return s, nil
	}
	if a.gapDelay <= 0 {
		a.next.Add(a.gapSize) // permanent gap: skip the numbers, queue nothing
		return s, nil
	}
	gap := make([]uint64, 0, a.gapSize)
	for i := uint64(0); i < a.gapSize; i++ {
		gap = append(gap, a.next.Add(1)-1)
	}
	return s, gap
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
// by Next(); the gaps in the sequence are the desired missing frames at the
// listener. A proxy on the path forwards stamped frames only with
// -allow-stamped-ingress.
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
// the same Config (Start/GapEvery/GapSize/GapDelay). With a GapDelay, the
// numbers every flow skips go on one queue, in due order, for
// [PerFlowAllocator.DueRetransmits].
type PerFlowAllocator struct {
	cfg Config

	mu     sync.Mutex
	allocs map[FlowKey]*Allocator
	late   []flowGap     // delayed gaps, oldest (and so soonest due) first
	queued chan struct{} // signaled, never blocking, each time a gap is queued
}

// flowGap is one flow's skipped numbers, waiting for GapDelay to elapse.
type flowGap struct {
	flow  FlowKey
	seqs  []uint64
	dueAt time.Time
}

// Late is a skipped sequence number whose GapDelay has elapsed: the sender
// owes Flow a frame carrying Seq.
type Late struct {
	Flow FlowKey
	Seq  uint64
}

// NewPerFlow returns a new PerFlowAllocator using cfg for every flow.
func NewPerFlow(cfg Config) *PerFlowAllocator {
	return &PerFlowAllocator{
		cfg:    cfg,
		allocs: make(map[FlowKey]*Allocator),
		queued: make(chan struct{}, 1),
	}
}

// Next returns the next sequence number for the given flow, creating a new
// underlying [Allocator] for the flow on first use. On a gap cycle with a
// GapDelay it queues the skipped numbers for [PerFlowAllocator.DueRetransmits].
func (p *PerFlowAllocator) Next(k FlowKey) uint64 {
	p.mu.Lock()
	a, ok := p.allocs[k]
	if !ok {
		a = New(p.cfg)
		p.allocs[k] = a
	}
	p.mu.Unlock()
	s, gap := a.alloc()
	if gap != nil {
		p.mu.Lock()
		// Stamped under the lock so the queue stays in due order: every gap
		// waits the same GapDelay.
		p.late = append(p.late, flowGap{flow: k, seqs: gap, dueAt: time.Now().Add(p.cfg.GapDelay)})
		p.mu.Unlock()
		select {
		case p.queued <- struct{}{}:
		default:
		}
	}
	return s
}

// GapEnabled reports whether gap injection is active for this allocator.
func (p *PerFlowAllocator) GapEnabled() bool {
	return p.cfg.GapEvery != 0 && p.cfg.GapSize != 0
}

// Delayed reports whether skipped numbers are queued for a late send
// (gap injection with GapDelay > 0) rather than left as permanent gaps.
func (p *PerFlowAllocator) Delayed() bool {
	return p.GapEnabled() && p.cfg.GapDelay > 0
}

// DueRetransmits removes and returns, oldest first, every skipped sequence
// number whose GapDelay has elapsed at now, each with the flow it belongs to.
func (p *PerFlowAllocator) DueRetransmits(now time.Time) []Late {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for n < len(p.late) && !now.Before(p.late[n].dueAt) {
		n++
	}
	if n == 0 {
		return nil
	}
	var due []Late
	for _, g := range p.late[:n] {
		for _, s := range g.seqs {
			due = append(due, Late{Flow: g.flow, Seq: s})
		}
	}
	// Pop by reslicing: O(popped), not O(queue). append reallocates once the
	// tail capacity runs out, copying only the live entries, so the backing
	// array stays within a small factor of the queue length.
	clear(p.late[:n])
	p.late = p.late[n:]
	return due
}

// NextDue reports when the oldest queued gap comes due; ok is false when
// none is queued.
func (p *PerFlowAllocator) NextDue() (at time.Time, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.late) == 0 {
		return time.Time{}, false
	}
	return p.late[0].dueAt, true
}

// Queued is signaled each time a delayed gap is queued, so a sender idle on
// an empty queue can wake. A signal can be stale: re-check NextDue.
func (p *PerFlowAllocator) Queued() <-chan struct{} { return p.queued }

// Pending returns the number of delayed gaps not yet returned by
// DueRetransmits.
func (p *PerFlowAllocator) Pending() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.late)
}
