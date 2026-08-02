// Package tx builds random BSV-shaped transaction payloads for load
// generation. Output layout matches the BSV P2P "tx" message body:
// version (4 LE) | vin_count (varint) | vin[] | vout_count (varint) | vout[] | locktime (4 LE).
//
// This is not consensus-valid — scripts are random bytes. It is shape-correct
// so any downstream parser that walks the structure sees plausible data.
//
// Exact-size invariant: for any targetSize >= the format minimum the emitted
// payload is EXACTLY one structurally valid transaction of targetSize bytes —
// the slack is absorbed INSIDE the structure (script bytes), never appended as
// trailing padding. Trailing padding is invisible on a datagram lane (datagram
// boundary == object boundary) but desynchronises a TCP objfmt stream at the
// second object, so the builder errors loudly instead of ever padding.
package tx

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
)

// MinRawSize is the smallest valid BRC-12 raw payload the builder emits:
//
//	4 (version) + 1 (vin_count) + 32 (prev_hash) + 4 (prev_index) +
//	1 (unlock script varint, len 0) + 4 (sequence) + 1 (vout_count) +
//	8 (value) + 1 (lock script varint, len 0) + 4 (locktime) = 60 bytes.
const MinRawSize = 60

// Builder generates transaction payloads of a target byte length using a
// per-instance PRNG. Not safe for concurrent use; give each worker its own
// Builder.
type Builder struct {
	rng *rand.ChaCha8
}

// New creates a Builder seeded from the provided 32-byte seed.
func New(seed [32]byte) *Builder {
	return &Builder{rng: rand.NewChaCha8(seed)}
}

// varintSize returns the canonical Bitcoin CompactSize width for v.
func varintSize(v int) int {
	switch {
	case v < 0xFD:
		return 1
	case v <= 0xFFFF:
		return 3
	case int64(v) <= 0xFFFFFFFF:
		return 5
	default:
		return 9
	}
}

// putVarint writes the canonical CompactSize encoding of v at dst[off:] and
// returns the number of bytes written.
func putVarint(dst []byte, off, v int) int {
	switch {
	case v < 0xFD:
		dst[off] = byte(v)
		return 1
	case v <= 0xFFFF:
		dst[off] = 0xFD
		binary.LittleEndian.PutUint16(dst[off+1:], uint16(v))
		return 3
	case int64(v) <= 0xFFFFFFFF:
		dst[off] = 0xFE
		binary.LittleEndian.PutUint32(dst[off+1:], uint32(v))
		return 5
	default:
		dst[off] = 0xFF
		binary.LittleEndian.PutUint64(dst[off+1:], uint64(v))
		return 9
	}
}

// splitSlack distributes slack extra bytes between the input unlocking script
// (shim, 0–2 bytes) and the output locking script (bulk) such that
//
//	cost(shim) + cost(bulk) == slack, where cost(L) = L + varintSize(L) - 1
//
// (the format minimum already counts one varint byte per script). A single
// script cannot hit every total — canonical CompactSize jumps 1→3 bytes at
// L=253 and 3→5 at L=65536, leaving 2-byte-wide unreachable gaps — so the
// shim script absorbs 1–2 bytes whenever the bulk script lands in a gap.
// Every slack >= 0 (up to the 5-byte varint ceiling) is therefore exactly
// representable; anything else is an error, never silent padding.
func splitSlack(slack int) (shim, bulk int, err error) {
	for shim = 0; shim <= 2; shim++ {
		rem := slack - shim
		switch {
		case rem < 0:
			// try smaller shim values only; slack < 0 is caught by callers
		case rem <= 252:
			return shim, rem, nil // 1-byte varint
		case rem >= 255 && rem <= 65537:
			return shim, rem - 2, nil // 3-byte varint, bulk >= 253 (canonical)
		case rem >= 65540 && int64(rem) <= 0xFFFFFFFF+4:
			return shim, rem - 4, nil // 5-byte varint, bulk >= 65536 (canonical)
		}
	}
	return 0, 0, fmt.Errorf("tx: no canonical script split for %d slack bytes", slack)
}

// Build writes a random BRC-12 raw transaction of EXACTLY targetSize bytes
// into dst and returns the slice. dst is reused when cap(dst) >= targetSize.
//
// The transaction is 1-input/1-output; the sizing slack goes into the output
// locking script (plus a 0–2 byte unlocking-script shim for CompactSize
// boundary totals). Returns an error when targetSize < MinRawSize or the size
// is not exactly representable — it NEVER pads outside the tx structure.
func (b *Builder) Build(dst []byte, targetSize int) ([]byte, error) {
	if targetSize < MinRawSize {
		return nil, fmt.Errorf("tx: payload size %d below BRC-12 raw minimum %d", targetSize, MinRawSize)
	}
	shim, bulk, err := splitSlack(targetSize - MinRawSize)
	if err != nil {
		return nil, err
	}
	if cap(dst) < targetSize {
		dst = make([]byte, targetSize)
	}
	dst = dst[:targetSize]

	binary.LittleEndian.PutUint32(dst[0:4], 2) // version
	off := 4
	dst[off] = 1 // vin_count
	off++
	fillRand(b.rng, dst[off:off+36]) // prev_hash + prev_index
	off += 36
	off += putVarint(dst, off, shim) // unlocking script length
	if shim > 0 {
		fillRand(b.rng, dst[off:off+shim])
		off += shim
	}
	fillRand(b.rng, dst[off:off+4]) // sequence
	off += 4
	dst[off] = 1 // vout_count
	off++
	fillRand(b.rng, dst[off:off+8]) // value
	off += 8
	off += putVarint(dst, off, bulk) // locking script length
	if bulk > 0 {
		fillRand(b.rng, dst[off:off+bulk])
		off += bulk
	}
	binary.LittleEndian.PutUint32(dst[off:off+4], 0) // locktime
	off += 4

	if off != targetSize {
		return nil, fmt.Errorf("tx: internal sizing bug: built %d bytes, want %d", off, targetSize)
	}
	return dst, nil
}

// fillRand writes deterministic pseudo-random bytes using the ChaCha8 stream.
func fillRand(r *rand.ChaCha8, buf []byte) {
	for len(buf) >= 8 {
		binary.LittleEndian.PutUint64(buf, r.Uint64())
		buf = buf[8:]
	}
	if len(buf) > 0 {
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], r.Uint64())
		copy(buf, tmp[:])
	}
}
