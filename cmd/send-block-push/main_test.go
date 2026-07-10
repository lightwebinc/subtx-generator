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
	"github.com/lightwebinc/subtx-generator/internal/tx"
)

func newRNG(s string) *rand.ChaCha8 { return rand.NewChaCha8(sha256.Sum256([]byte(s))) }

func TestBuildBlock_DelimitsAndReframes(t *testing.T) {
	rng := newRNG("t")
	txb := tx.New(sha256.Sum256([]byte("cb")))
	var prev [32]byte
	for _, subtrees := range []int{1, 4, 64} {
		for _, bump := range []int{0, 32} {
			obj, blockHash, cbTxID := buildBlock(rng, txb, subtrees, 200, bump, 800000, prev)

			n, err := objfmt.BlockSize(obj)
			if err != nil || n != len(obj) {
				t.Fatalf("subtrees=%d bump=%d: BlockSize=%d len=%d err=%v", subtrees, bump, n, len(obj), err)
			}
			// Block hash identity = SHA256d(header[:80]) — how the proxy derives ContentID.
			h1 := sha256.Sum256(obj[:blockHeaderLen])
			want := sha256.Sum256(h1[:])
			if want != blockHash {
				t.Fatalf("subtrees=%d: returned blockHash != SHA256d(header)", subtrees)
			}
			// SubtreeCount on wire.
			if got := binary.BigEndian.Uint64(obj[96:104]); got != uint64(subtrees) {
				t.Fatalf("subtrees=%d: SubtreeCount on wire=%d", subtrees, got)
			}
			// Inline coinbase TxID must match the recorded one.
			off := objfmt.BlockPrefixSize + subtrees*32
			gotID, err := objfmt.TxID(obj[off:])
			if err != nil || gotID != cbTxID {
				t.Fatalf("subtrees=%d: inline coinbase TxID mismatch err=%v", subtrees, err)
			}
			// Exact reframe path the proxy runs (BRC-144 body verbatim -> BRC-131).
			if _, err := objfmt.MulticastBytes(objfmt.ClassBlock, obj); err != nil {
				t.Fatalf("subtrees=%d: MulticastBytes: %v", subtrees, err)
			}
			prev = blockHash
		}
	}
}

func TestBuildBlock_StreamSplitsBackToBack(t *testing.T) {
	rng := newRNG("stream")
	txb := tx.New(sha256.Sum256([]byte("cb2")))
	var prev [32]byte
	var buf bytes.Buffer
	const want = 4
	for i := 0; i < want; i++ {
		obj, bh, _ := buildBlock(rng, txb, 3, 180, 16, uint64(800000+i), prev)
		buf.Write(obj)
		prev = bh
	}
	rd := objfmt.NewReader(&buf, objfmt.ClassBlock)
	rd.SetMaxObject(256 << 20)
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
