// Command send-subtree-data sends BRC-132 subtree data frames to
// bitcoin-shard-proxy via TCP for integration testing.
//
// It sends SubtreeData frames with configurable MsgType (hashes-only or
// full-nodes), payload size, and count. SeqNum and HashKey are left zero so
// the proxy stamps them in-place.
//
// Usage:
//
//	send-subtree-data -addr [fd20::2]:9002 -frames 20 -msg-type hashes
package main

import (
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"net"
	"time"
)

// BRC-132 wire constants — mirror of bitcoin-shard-common/frame.
const (
	magicBSV   = 0xE3E1F3E8
	protoVer   = 0x02BF
	frameVerV5 = 0x05
	headerSize = 92

	subtreeMsgHashesOnly = 0x01
	subtreeMsgFullNodes  = 0x02

	// Subtree data payload header: MsgType(1) + Reserved(3) + SubtreeID(32) +
	// NodeCount(4) + SubtreeHeight(4) = 44 bytes prepended before node data.
	// For testing we just write random node bytes after a minimal 8-byte header.
	subtreeNodeHashSize = 32 // bytes per node in hashes-only mode
	subtreeNodeFullSize = 48 // bytes per node in full-nodes mode (hash+fee+size)
)

func main() {
	addr := flag.String("addr", "[::1]:9002", "proxy TCP address (host:port)")
	frameCount := flag.Int("frames", 20, "number of subtree data frames to send")
	msgTypeStr := flag.String("msg-type", "hashes", "payload type: hashes | full")
	nodeCount := flag.Int("nodes", 16, "number of subtree nodes per frame")
	payloadSize := flag.Int("payload-size", 0,
		"override total payload size in bytes (0 = derived from nodes × node-size)")
	subtreeCount := flag.Int("subtree-count", 0,
		"number of unique subtree IDs to cycle through (0 = one fresh random ID per frame)")
	interval := flag.Duration("interval", 50*time.Millisecond, "delay between frames")
	flag.Parse()

	var msgType byte
	var nodeSize int
	switch *msgTypeStr {
	case "hashes", "hashes-only":
		msgType = subtreeMsgHashesOnly
		nodeSize = subtreeNodeHashSize
	case "full", "full-nodes":
		msgType = subtreeMsgFullNodes
		nodeSize = subtreeNodeFullSize
	default:
		log.Fatalf("unknown msg-type %q: want hashes or full", *msgTypeStr)
	}

	payLen := *payloadSize
	if payLen <= 0 {
		payLen = *nodeCount * nodeSize
	}
	if payLen < 1 {
		payLen = nodeSize
	}

	// Pre-generate the subtree ID pool when -subtree-count > 0.
	var subtreePool [][32]byte
	if *subtreeCount > 0 {
		subtreePool = make([][32]byte, *subtreeCount)
		for i := range subtreePool {
			mustRand(subtreePool[i][:])
		}
	}

	conn, err := net.DialTimeout("tcp", *addr, 5*time.Second)
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	defer func() { _ = conn.Close() }()
	log.Printf("connected to %s; sending %d BRC-132 frames (msg=%s payload=%dB)",
		*addr, *frameCount, *msgTypeStr, payLen)

	sent := 0
	for i := 0; i < *frameCount; i++ {
		// SubtreeID: cycle through the pool when -subtree-count is set,
		// otherwise generate a fresh random ID per frame.
		var subtreeID [32]byte
		if len(subtreePool) > 0 {
			subtreeID = subtreePool[i%len(subtreePool)]
		} else {
			mustRand(subtreeID[:])
		}

		// Random payload (node hashes or full-node records).
		payload := make([]byte, payLen)
		mustRand(payload)

		frame := encodeSubtreeDataFrame(msgType, subtreeID, payload)
		if err := writeFrame(conn, frame); err != nil {
			log.Fatalf("frame %d write: %v", i, err)
		}
		sent++

		fmt.Printf("frame %d: subtree_id=%x msg=%02X payload=%dB\n",
			i, subtreeID[:8], msgType, payLen)

		if i < *frameCount-1 {
			time.Sleep(*interval)
		}
	}

	log.Printf("done: sent=%d frames", sent)
}

// encodeSubtreeDataFrame builds a BRC-132 wire frame.
// HashKey (40:48) and SeqNum (48:56) are left zero — the proxy stamps them.
// SubtreeID occupies bytes 56:88 (the SubtreeID field in the V5 header).
func encodeSubtreeDataFrame(msgType byte, subtreeID [32]byte, payload []byte) []byte {
	buf := make([]byte, headerSize+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], magicBSV)
	binary.BigEndian.PutUint16(buf[4:6], protoVer)
	buf[6] = frameVerV5
	buf[7] = msgType
	// bytes 8:40 = SubtreeID (TxID slot in the V5 header layout)
	copy(buf[8:40], subtreeID[:])
	// bytes 40:48 = HashKey = 0 (proxy stamps)
	// bytes 48:56 = SeqNum  = 0 (proxy stamps)
	// bytes 56:88 = SubtreeID (second copy in the dedicated SubtreeID field)
	copy(buf[56:88], subtreeID[:])
	binary.BigEndian.PutUint32(buf[88:92], uint32(len(payload)))
	copy(buf[92:], payload)
	return buf
}

// writeFrame writes raw frame bytes over TCP.
func writeFrame(conn net.Conn, frame []byte) error {
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := conn.Write(frame)
	return err
}

func mustRand(b []byte) {
	if _, err := rand.Read(b); err != nil {
		log.Fatalf("rand.Read: %v", err)
	}
}
