package merkle

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"testing"
)

// nodeHash is node i: SHA256d of i as a little-endian uint64, or the all-0xFF
// coinbase placeholder at index 0 when placeholder.
func nodeHash(i int, placeholder bool) [32]byte {
	var h [32]byte
	if placeholder && i == 0 {
		for k := range h {
			h[k] = 0xFF
		}
		return h
	}
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(i))
	a := sha256.Sum256(b[:])
	return sha256.Sum256(a[:])
}

// TestRoot_MatchesTeranode checks roots computed by Teranode's own merkle code
// (go-subtree BuildMerkleTreeStoreFromBytes, v1.4.2) for the same node sets.
func TestRoot_MatchesTeranode(t *testing.T) {
	for _, tc := range []struct {
		n           int
		placeholder bool
		root        string
	}{
		{1, false, "7ef0ca626bbb058dd443bb78e33b888bdec8295c96e51f5545f96370870c10b9"},
		{2, false, "26c582bf7254e2d2e10d8ab644fef80bbf39b557fc944ae1798bc37b31e93983"},
		{3, false, "61b203381a170e2f9a785cf90c413c47cc22bd072898548e59d01e696a3de235"},
		{5, false, "98e9dddb89c125d04810eb92f7922ad1c5498060b53640a6bb62d80c9de1ff71"},
		{1025, false, "a82169a9321adec842d9d3d27b99fb3cc22b74d639d6c6b84bd1433addf8a3ac"},
		{1, true, "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
		{7, true, "4865b7186a8d474625e02a2548de667cee12688a0fe6080a5a74d2ed5c8af52a"},
		{8, true, "1386a8acfe5a46dc02e8bfcec7447c9437f31df250395f5daff15374bc17708f"},
		{1024, true, "ce11e231f30779cc461def447f0faa7543604c054f19555d0df5603a544929aa"},
		{4097, true, "e64d97ef6b261efd5cb1c1f7f1f26b64b7dfba2a6a53b2d31af523f34a4f618b"},
	} {
		for _, stride := range []int{32, 48} {
			nodes := make([]byte, tc.n*stride)
			for i := 0; i < tc.n; i++ {
				h := nodeHash(i, tc.placeholder)
				copy(nodes[i*stride:], h[:])
				for k := i*stride + 32; k < (i+1)*stride; k++ {
					nodes[k] = 0xA5
				}
			}
			got := Root(nodes, stride, tc.n)
			if hex.EncodeToString(got[:]) != tc.root {
				t.Errorf("n=%d placeholder=%v stride=%d: root %x, want %s", tc.n, tc.placeholder, stride, got, tc.root)
			}
		}
	}
}
