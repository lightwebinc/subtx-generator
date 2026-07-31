// Package blockhdr builds synthetic 80-byte Bitcoin block headers that satisfy
// a real proof-of-work check.
//
// The listener and proxy block-control gate (-require-block-pow, default ON)
// runs pow.CheckHeader on every BRC-131 announce: the header's compact nBits
// must expand to a valid target no easier than the operator's floor, and
// SHA256d(header) must fall at or below that target. A synthetic emitter that
// fills those fields with random bytes — or serialises nBits in the wrong byte
// order — produces headers the fabric drops, which presents as blocks silently
// never arriving rather than as an emitter fault.
//
// The gate's own implementation is the reference: this package calls
// pow.CheckHeader rather than re-deriving the target, so an emitter can never
// drift from the validator it must satisfy.
package blockhdr

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/lightwebinc/shard-common/pow"
)

const (
	// Len is the fixed serialised size of a Bitcoin block header.
	Len = 80

	// nBitsOffset is where the compact difficulty field starts. Like every
	// other multi-byte header field it is serialised LITTLE-endian.
	nBitsOffset = 72
	// nonceOffset is where the 4-byte LE nonce starts.
	nonceOffset = 76

	// BitsRegtest is the regtest/devnet compact target (0x207fffff): the
	// easiest valid difficulty, at which roughly half of all candidate headers
	// already pass, so Grind terminates in a couple of tries. It matches the
	// devnet NetworkProfile PoW floor, so a fabric rendered from that profile
	// accepts these headers without the emitter doing real work.
	BitsRegtest = 0x207fffff
)

// SetBits writes the compact difficulty into hdr in the byte order the gate
// reads it. Use this rather than writing hdr[72:76] directly: a big-endian
// write of 0x207fffff decodes as 0xffff7f20, whose sign bit is set, so
// CompactToTarget rejects it and every announce fails the gate.
func SetBits(hdr []byte, bits uint32) {
	binary.LittleEndian.PutUint32(hdr[nBitsOffset:nBitsOffset+4], bits)
}

// Grind searches the nonce space until hdr satisfies the target encoded in its
// own nBits, mutating hdr's nonce in place and returning the resulting block
// hash. start seeds the search (pass the block height so a run is
// reproducible); the nonce wraps, so the whole 2^32 space is tried before
// giving up and returning the hash of the last candidate.
//
// The floor passed to pow.CheckHeader is nil — the header need only satisfy the
// difficulty it claims. An operator floor is a separate, stricter admission
// test; an emitter that claims [BitsRegtest] against a mainnet floor is
// correctly rejected, and grinding harder would not change that.
//
// At [BitsRegtest] the expected number of tries is about two. Grind is
// therefore safe on a generator's hot path at lab difficulty, and deliberately
// NOT safe at mainnet difficulty — synthetic emitters are a lab instrument.
func Grind(hdr []byte, start uint32) [32]byte {
	nonce := start
	for i := int64(0); i < 1<<32; i++ {
		binary.LittleEndian.PutUint32(hdr[nonceOffset:nonceOffset+4], nonce)
		if pow.CheckHeader(hdr, nil) {
			break
		}
		nonce++
	}
	return Hash(hdr)
}

// Hash returns SHA256d(hdr) — the block hash, in the internal little-endian
// byte order block hashes are compared and stored in.
func Hash(hdr []byte) [32]byte {
	h1 := sha256.Sum256(hdr)
	return sha256.Sum256(h1[:])
}
