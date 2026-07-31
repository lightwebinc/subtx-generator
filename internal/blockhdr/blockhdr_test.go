package blockhdr

import (
	"encoding/binary"
	"testing"

	"github.com/lightwebinc/shard-common/pow"
)

// TestSetBitsByteOrder pins the defect that motivated this package: nBits is a
// little-endian header field, and a big-endian write produces a compact value
// with the sign bit set, which CompactToTarget rejects outright — so every
// announce fails the gate rather than merely being hard to satisfy.
func TestSetBitsByteOrder(t *testing.T) {
	hdr := make([]byte, Len)
	SetBits(hdr, BitsRegtest)
	if got := pow.NBits(hdr); got != BitsRegtest {
		t.Fatalf("nBits round-trip: got %#x want %#x", got, uint32(BitsRegtest))
	}
	if pow.CompactToTarget(pow.NBits(hdr)) == nil {
		t.Fatal("CompactToTarget rejected the target we just wrote")
	}

	// The regression itself.
	binary.BigEndian.PutUint32(hdr[nBitsOffset:nBitsOffset+4], BitsRegtest)
	if pow.CompactToTarget(pow.NBits(hdr)) != nil {
		t.Fatal("big-endian nBits unexpectedly expanded to a valid target; " +
			"the byte-order trap this package exists to prevent has moved")
	}
}

// TestGrindSatisfiesGate proves a ground header passes the same check the
// listener and proxy run, at the devnet floor a rendered lab fabric uses.
func TestGrindSatisfiesGate(t *testing.T) {
	floor := pow.CompactToTarget(BitsRegtest)
	if floor == nil {
		t.Fatal("regtest floor did not expand")
	}
	for height := uint32(0); height < 32; height++ {
		hdr := make([]byte, Len)
		binary.LittleEndian.PutUint32(hdr[0:4], 2)
		binary.LittleEndian.PutUint32(hdr[68:72], 1700000000+height)
		SetBits(hdr, BitsRegtest)

		got := Grind(hdr, height)
		if !pow.CheckHeader(hdr, floor) {
			t.Fatalf("height %d: ground header fails CheckHeader at the devnet floor", height)
		}
		if got != Hash(hdr) {
			t.Fatalf("height %d: returned hash does not match the mutated header", height)
		}
	}
}

// TestGrindLeavesHeaderOtherwiseIntact guards against Grind clobbering fields a
// caller already set — only the nonce may move.
func TestGrindLeavesHeaderOtherwiseIntact(t *testing.T) {
	hdr := make([]byte, Len)
	for i := range hdr {
		hdr[i] = byte(i)
	}
	SetBits(hdr, BitsRegtest)
	before := append([]byte(nil), hdr...)

	Grind(hdr, 7)

	for i := 0; i < Len; i++ {
		if i >= nonceOffset && i < nonceOffset+4 {
			continue
		}
		if hdr[i] != before[i] {
			t.Fatalf("Grind mutated header byte %d outside the nonce", i)
		}
	}
}
