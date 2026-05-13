package tx

import "encoding/binary"

// efMarker is the 6-byte BRC-30 Extended Format marker that immediately
// follows the 4-byte LE version field in a BRC-128 payload.
var efMarker = [6]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0xEF}

// MinEFSize is the smallest viable EF payload:
//
//	4 (version) + 6 (marker) + 1 (vin_count) +
//	32+4+1+4+8+1 (EF input with empty script and 0 satoshis) +
//	1 (vout_count) + 8+1 (output value + script_len=0) +
//	4 (locktime) = 75 bytes.
//
// Build pads or grows the output script when targetSize exceeds the minimum.
const MinEFSize = 75

// BuildEF writes a shape-correct BRC-30 Extended Format BSV transaction into
// dst[:targetSize] and returns the slice. Output layout (all multi-byte
// integers little-endian unless noted):
//
//	version (4) | EF marker (6) | vin_count=1 (1) |
//	  prev_hash (32) | prev_index (4) | script_len=0 (1) | sequence (4) |
//	  spent_satoshis (8) | locking_script_len (varint) | locking_script (N) |
//	vout_count=1 (1) | value (8) | script_len (varint) | script (M) |
//	locktime (4)
//
// Scripts are filled with deterministic pseudo-random bytes from the Builder's
// PRNG; the result is shape-correct but not consensus-valid (consistent with
// Build).
//
// If targetSize < MinEFSize it is clamped to MinEFSize.
func (b *Builder) BuildEF(dst []byte, targetSize int) []byte {
	if targetSize < MinEFSize {
		targetSize = MinEFSize
	}
	if cap(dst) < targetSize {
		dst = make([]byte, targetSize)
	}
	dst = dst[:targetSize]

	// version = 2 (LE)
	binary.LittleEndian.PutUint32(dst[0:4], 2)
	// EF marker at bytes 4..10
	copy(dst[4:10], efMarker[:])
	// vin_count = 1
	dst[10] = 1

	// EF input begins at offset 11.
	// prev_hash[32] + prev_index[4] = 36 bytes of pseudo-random data.
	fillRand(b.rng, dst[11:11+36])
	// script_len = 0 (no unlocking script)
	dst[11+36] = 0
	// sequence[4]
	fillRand(b.rng, dst[11+36+1:11+36+1+4])

	// EF-specific tail of the input: spent_satoshis[8] + locking_script_len(varint) + locking_script.
	// Layout offset:
	//   inputEnd = 11 + 36 + 1 + 4 = 52
	const inputCore = 11 + 36 + 1 + 4 // 52

	// Reserve 4 trailing bytes for locktime; allow one output of value(8)+script_len(1)+empty script.
	// We'll size the locking script to consume most of the remaining bytes,
	// leaving a small fixed-size output.
	const tailReserved = 1 /*vout_count*/ + 8 /*value*/ + 1 /*output script_len=0*/ + 4 /*locktime*/

	// spent_satoshis[8]
	binary.LittleEndian.PutUint64(dst[inputCore:inputCore+8], 0)

	// Bytes remaining for locking_script_len + script bytes:
	remaining := targetSize - inputCore - 8 - tailReserved
	if remaining < 1 {
		remaining = 1
	}
	// Use a 1-byte VarInt (length < 0xFD ⇒ up to 252 bytes). If remaining
	// would push script length above 252, cap it and absorb the slack in the
	// output script later.
	scriptLen := remaining - 1
	if scriptLen < 0 {
		scriptLen = 0
	}
	if scriptLen > 252 {
		scriptLen = 252
	}
	dst[inputCore+8] = byte(scriptLen)
	scriptStart := inputCore + 8 + 1
	if scriptLen > 0 {
		fillRand(b.rng, dst[scriptStart:scriptStart+scriptLen])
	}

	// Position after the input.
	pos := scriptStart + scriptLen

	// Slack between end of locking script and the reserved tail goes into
	// the output script length (so total bytes == targetSize).
	tailStart := targetSize - 4 /*locktime*/ - 9 /*output value+scriptlen*/
	// If pos < tailStart - 1 (vout_count byte), we have outScriptLen bytes
	// to absorb between the input and the output value field.
	outScriptLen := tailStart - 1 - pos
	if outScriptLen < 0 {
		outScriptLen = 0
	}
	if outScriptLen > 252 {
		outScriptLen = 252
	}

	// vout_count = 1
	dst[pos] = 1
	pos++
	// output value[8] (LE)
	binary.LittleEndian.PutUint64(dst[pos:pos+8], 0)
	pos += 8
	// output script_len (1-byte varint)
	dst[pos] = byte(outScriptLen)
	pos++
	// output script bytes
	if outScriptLen > 0 {
		fillRand(b.rng, dst[pos:pos+outScriptLen])
	}
	pos += outScriptLen

	// Any tiny residual gap between pos and the locktime offset gets zero-
	// filled (and was accounted for by outScriptLen reservation). Locktime
	// always sits at the final 4 bytes.
	for i := pos; i < targetSize-4; i++ {
		dst[i] = 0
	}
	binary.LittleEndian.PutUint32(dst[targetSize-4:], 0)
	return dst
}
