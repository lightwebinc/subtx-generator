package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math/rand/v2"
	"testing"

	"github.com/lightwebinc/shard-common/objfmt"
)

func newRNG(s string) *rand.ChaCha8 { return rand.NewChaCha8(sha256.Sum256([]byte(s))) }

func TestBuildSubtree_DelimitsAndReframes(t *testing.T) {
	rng := newRNG("t")
	for _, nodes := range []int{1, 16, 1000} {
		obj, root := buildSubtree(rng, nodes, true)

		// Exact self-delimiting length — the proxy's objfmt.Reader relies on it.
		n, err := objfmt.SubtreeSize(obj)
		if err != nil || n != len(obj) {
			t.Fatalf("nodes=%d: SubtreeSize=%d len=%d err=%v", nodes, n, len(obj), err)
		}
		// In-band NodeCount and root must be what we recorded.
		if got := binary.BigEndian.Uint64(obj[32:40]); got != uint64(nodes) {
			t.Fatalf("nodes=%d: NodeCount on wire=%d", nodes, got)
		}
		if !bytes.Equal(obj[0:32], root[:]) {
			t.Fatalf("nodes=%d: root header != returned root", nodes)
		}
		// coinbase placeholder at node[0].
		off := objfmt.SubtreeHeaderSize
		if !bytes.Equal(obj[off:off+32], bytes.Repeat([]byte{0xFF}, 32)) {
			t.Fatalf("nodes=%d: node[0] is not the 0xFF coinbase placeholder", nodes)
		}
		// Exact reframe path the proxy runs (BRC-143 -> BRC-132).
		if _, err := objfmt.MulticastBytes(objfmt.ClassSubtree, obj); err != nil {
			t.Fatalf("nodes=%d: MulticastBytes: %v", nodes, err)
		}
	}
}

func TestBuildSubtree_StreamSplitsBackToBack(t *testing.T) {
	rng := newRNG("stream")
	var buf bytes.Buffer
	const want = 5
	for i := 0; i < want; i++ {
		obj, _ := buildSubtree(rng, 8, true)
		buf.Write(obj)
	}
	rd := objfmt.NewReader(&buf, objfmt.ClassSubtree)
	got := 0
	for {
		if _, err := rd.Next(); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("Next: %v", err)
		}
		got++
	}
	if got != want {
		t.Fatalf("reader yielded %d objects, want %d", got, want)
	}
}
