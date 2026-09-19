// Package merkle computes a subtree merkle root the way Teranode does
// (go-subtree BuildMerkleTreeStoreFromBytes), so a generated subtree carries a
// root that a verifying proxy (shard-proxy -verify-subtree-root) accepts.
//
// A level with an odd count pairs its last hash with itself, a single node is
// its own root, and an all-zero hash stands for an absent node: a pair whose
// left side is zero hashes to zero, and a zero right side is treated as absent.
package merkle

import "crypto/sha256"

// Root returns the merkle root of the n node hashes in nodes, each the first
// 32 bytes of a stride-byte record (32 for hashes only, 48 for hash, fee and
// size). n must be at least 1.
func Root(nodes []byte, stride, n int) [32]byte {
	level := make([][32]byte, n)
	for i := range level {
		copy(level[i][:], nodes[i*stride:i*stride+32])
	}
	var zero [32]byte
	for len(level) > 1 {
		next := make([][32]byte, (len(level)+1)/2)
		for p := range next {
			left := level[2*p]
			if left == zero {
				continue
			}
			right := left
			if r := 2*p + 1; r < len(level) && level[r] != zero {
				right = level[r]
			}
			var pair [64]byte
			copy(pair[:32], left[:])
			copy(pair[32:], right[:])
			h := sha256.Sum256(pair[:])
			next[p] = sha256.Sum256(h[:])
		}
		level = next
	}
	return level[0]
}
