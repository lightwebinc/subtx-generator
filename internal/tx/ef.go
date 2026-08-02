package tx

import (
	"encoding/binary"
	"fmt"
)

// efMarker is the 6-byte BRC-30 Extended Format marker that immediately
// follows the 4-byte LE version field in a BRC-128 payload.
var efMarker = [6]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0xEF}

// MinEFSize is the smallest valid BRC-30 Extended Format payload the builder
// emits:
//
//	4 (version) + 6 (marker) + 1 (vin_count) +
//	32 (prev_hash) + 4 (prev_index) + 1 (unlock script varint, len 0) +
//	4 (sequence) + 8 (spent_satoshis) + 1 (prev locking script varint, len 0) +
//	1 (vout_count) + 8 (value) + 1 (out script varint, len 0) +
//	4 (locktime) = 75 bytes.
const MinEFSize = 75

// BuildEF writes a shape-correct BRC-30 Extended Format BSV transaction of
// EXACTLY targetSize bytes into dst and returns the slice. Output layout (all
// multi-byte integers little-endian unless noted):
//
//	version (4) | EF marker (6) | vin_count=1 (1) |
//	  prev_hash (32) | prev_index (4) | script_len (varint) | script |
//	  sequence (4) | spent_satoshis (8) | locking_script_len (varint) |
//	vout_count=1 (1) | value (8) | script_len (varint) | script (M) |
//	locktime (4)
//
// The sizing slack goes into the output locking script (plus a 0–2 byte
// unlocking-script shim for CompactSize boundary totals, same scheme as
// Build); the EF prev-locking-script stays empty. Scripts are deterministic
// pseudo-random bytes — shape-correct, not consensus-valid.
//
// Returns an error when targetSize < MinEFSize or the size is not exactly
// representable — it NEVER pads outside the tx structure.
func (b *Builder) BuildEF(dst []byte, targetSize int) ([]byte, error) {
	if targetSize < MinEFSize {
		return nil, fmt.Errorf("tx: payload size %d below BRC-30 EF minimum %d", targetSize, MinEFSize)
	}
	shim, bulk, err := splitSlack(targetSize - MinEFSize)
	if err != nil {
		return nil, err
	}
	if cap(dst) < targetSize {
		dst = make([]byte, targetSize)
	}
	dst = dst[:targetSize]

	binary.LittleEndian.PutUint32(dst[0:4], 2) // version
	copy(dst[4:10], efMarker[:])               // EF marker
	off := 10
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
	binary.LittleEndian.PutUint64(dst[off:off+8], 0) // spent_satoshis
	off += 8
	off += putVarint(dst, off, 0) // prev locking script length = 0
	dst[off] = 1                  // vout_count
	off++
	binary.LittleEndian.PutUint64(dst[off:off+8], 0) // value
	off += 8
	off += putVarint(dst, off, bulk) // output script length
	if bulk > 0 {
		fillRand(b.rng, dst[off:off+bulk])
		off += bulk
	}
	binary.LittleEndian.PutUint32(dst[off:off+4], 0) // locktime
	off += 4

	if off != targetSize {
		return nil, fmt.Errorf("tx: internal sizing bug: built %d EF bytes, want %d", off, targetSize)
	}
	return dst, nil
}
