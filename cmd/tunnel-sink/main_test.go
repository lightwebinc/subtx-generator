package main

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/lightwebinc/shard-common/objfmt"
)

// minimalTx returns the smallest shape-correct raw (BRC-12) transaction:
// one input with an empty script, one output with an empty script.
func minimalTx() []byte {
	var b bytes.Buffer
	b.Write([]byte{0x01, 0x00, 0x00, 0x00}) // version 1 LE
	b.WriteByte(0x01)                       // vin count
	b.Write(make([]byte, 36))               // prevout
	b.WriteByte(0x00)                       // script len
	b.Write([]byte{0xff, 0xff, 0xff, 0xff}) // sequence
	b.WriteByte(0x01)                       // vout count
	b.Write(make([]byte, 8))                // value
	b.WriteByte(0x00)                       // script len
	b.Write(make([]byte, 4))                // locktime
	return b.Bytes()
}

func subtreePush(nodes int) []byte {
	var b bytes.Buffer
	root := make([]byte, 32)
	root[0] = 0xab
	b.Write(root)
	var cnt [8]byte
	binary.BigEndian.PutUint64(cnt[:], uint64(nodes))
	b.Write(cnt[:])
	b.Write(make([]byte, 32*nodes))
	return b.Bytes()
}

func blockPush(subtrees int) []byte {
	var b bytes.Buffer
	hdr := make([]byte, 80)
	binary.LittleEndian.PutUint32(hdr[0:4], 0x20000000) // header version
	b.Write(hdr)
	var u [8]byte
	binary.BigEndian.PutUint64(u[:], 5) // TransactionCount
	b.Write(u[:])
	binary.BigEndian.PutUint64(u[:], 1234) // SizeInBytes
	b.Write(u[:])
	binary.BigEndian.PutUint64(u[:], uint64(subtrees))
	b.Write(u[:])
	b.Write(make([]byte, 32*subtrees)) // subtree roots
	b.Write(minimalTx())               // inline coinbase
	b.Write(make([]byte, 8))           // height
	b.Write(make([]byte, 8))           // BUMP len = 0
	return b.Bytes()
}

func TestClassifyBuffer(t *testing.T) {
	cases := []struct {
		name string
		buf  []byte
		eof  bool
		want objfmt.Class
	}{
		{"tx stream", bytes.Repeat(minimalTx(), 3), false, objfmt.ClassTx},
		{"single tx at eof", minimalTx(), true, objfmt.ClassTx},
		{"subtree stream", subtreePush(16), false, objfmt.ClassSubtree},
		{"block stream", blockPush(2), false, objfmt.ClassBlock},
		{"two blocks", bytes.Repeat(blockPush(2), 2), false, objfmt.ClassBlock},
	}
	for _, c := range cases {
		got, ok := classifyBuffer(c.buf, c.eof)
		if !ok || got != c.want {
			t.Errorf("%s: classifyBuffer = (%v, %v), want (%v, true)", c.name, got, ok, c.want)
		}
	}
}

func TestClassifyUndecidedOnShortPrefix(t *testing.T) {
	// Fewer bytes than BlockPrefixSize with several candidates still viable
	// must not decide without EOF.
	buf := blockPush(2)[:60]
	if cls, ok := classifyBuffer(buf, false); ok {
		t.Errorf("classifyBuffer decided %v on a %d-byte prefix", cls, len(buf))
	}
}

func TestWalkCandidateRejectsCrossClass(t *testing.T) {
	if s := walkCandidate(objfmt.ClassSubtree, bytes.Repeat(minimalTx(), 2)); s.viable {
		t.Error("subtree candidate should be implausible on a tx stream")
	}
	if s := walkCandidate(objfmt.ClassTx, blockPush(2)); s.viable {
		t.Error("tx candidate should be implausible on a block stream (header version)")
	}
}
