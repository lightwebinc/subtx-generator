// Command send-subtree-data sends BRC-132 subtree data frames to
// shard-proxy via TCP for integration testing.
//
// It sends SubtreeData frames with configurable MsgType (hashes-only or
// full-nodes), node count, and frame count. Each payload is a well-formed
// BRC-132 payload of random node hashes, and each SubtreeID is the merkle root
// of those nodes as Teranode computes it, so a proxy running
// -verify-subtree-root forwards it. SeqNum and HashKey are left zero so the
// proxy stamps them in-place.
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
	"log/slog"

	"github.com/lightwebinc/shard-common/logging"
	"github.com/lightwebinc/subtx-generator/internal/merkle"
	"net"
	"os"
	"time"
)

// BRC-132 wire constants — mirror of shard-common/frame.
const (
	magicBSV   = 0xE3E1F3E8
	protoVer   = 0x02BF
	frameVerV5 = 0x05
	headerSize = 92

	subtreeMsgHashesOnly = 0x01
	subtreeMsgFullNodes  = 0x02

	// BRC-132 payload: TotalFees(8) + TotalSizeBytes(8) + NodeCount(8), the
	// nodes, then ConflictCount(8) (always zero here).
	payloadHeaderSize   = 24
	conflictCountSize   = 8
	subtreeNodeHashSize = 32 // bytes per node in hashes-only mode
	subtreeNodeFullSize = 48 // bytes per node in full-nodes mode (hash+fee+size)
)

func main() {
	addr := flag.String("addr", "[::1]:9002", "proxy TCP address (host:port)")
	frameCount := flag.Int("frames", 20, "number of subtree data frames to send")
	msgTypeStr := flag.String("msg-type", "hashes", "payload type: hashes | full")
	nodeCount := flag.Int("nodes", 16, "number of subtree nodes per frame")
	payloadSize := flag.Int("payload-size", 0,
		"approximate payload size in bytes; overrides -nodes with as many nodes as fit (0 = use -nodes)")
	subtreeCount := flag.Int("subtree-count", 0,
		"number of unique subtrees to cycle through (0 = one fresh random subtree per frame)")
	interval := flag.Duration("interval", 50*time.Millisecond, "delay between frames")
	flag.Parse()
	logging.Init(logging.Options{Service: "subtx-generator", Level: slog.LevelInfo, Format: logging.ParseFormat(os.Getenv("LOG_FORMAT"))})

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
		fatalf("unknown msg-type %q: want hashes or full", *msgTypeStr)
	}

	nodes := *nodeCount
	if *payloadSize > 0 {
		nodes = (*payloadSize - payloadHeaderSize - conflictCountSize) / nodeSize
	}
	if nodes < 1 {
		nodes = 1
	}
	payLen := payloadHeaderSize + nodes*nodeSize + conflictCountSize

	// Pre-generate the subtree pool when -subtree-count > 0.
	var subtreePool []subtree
	if *subtreeCount > 0 {
		subtreePool = make([]subtree, *subtreeCount)
		for i := range subtreePool {
			subtreePool[i] = newSubtree(msgType, nodeSize, nodes)
		}
	}

	conn, err := net.DialTimeout("tcp", *addr, 5*time.Second)
	if err != nil {
		fatalf("dial %s: %v", *addr, err)
	}
	defer func() { _ = conn.Close() }()
	infof("connected to %s; sending %d BRC-132 frames (msg=%s payload=%dB)",
		*addr, *frameCount, *msgTypeStr, payLen)

	sent := 0
	for i := 0; i < *frameCount; i++ {
		// Cycle through the pool when -subtree-count is set, otherwise
		// generate a fresh random subtree per frame.
		var st subtree
		if len(subtreePool) > 0 {
			st = subtreePool[i%len(subtreePool)]
		} else {
			st = newSubtree(msgType, nodeSize, nodes)
		}
		subtreeID, payload := st.root, st.payload

		frame := encodeSubtreeDataFrame(msgType, subtreeID, payload)
		if err := writeFrame(conn, frame); err != nil {
			fatalf("frame %d write: %v", i, err)
		}
		sent++

		fmt.Printf("frame %d: subtree_id=%x msg=%02X payload=%dB\n",
			i, subtreeID[:8], msgType, payLen)

		if i < *frameCount-1 {
			time.Sleep(*interval)
		}
	}

	infof("done: sent=%d frames", sent)
}

// encodeSubtreeDataFrame builds a BRC-132 wire frame.
// HashKey (40:48) and SeqNum (48:56) are left zero — the proxy stamps them.
// subtree is one generated subtree: its BRC-132 payload and merkle root.
type subtree struct {
	root    [32]byte
	payload []byte
}

// newSubtree builds a BRC-132 payload of n random nodes at nodeSize bytes each
// (full-node records get a fee of 1 and a size of 250 per node, with matching
// totals) and computes the merkle root the frame's SubtreeID must carry.
func newSubtree(msgType byte, nodeSize, n int) subtree {
	p := make([]byte, payloadHeaderSize+n*nodeSize+conflictCountSize)
	nodes := p[payloadHeaderSize : payloadHeaderSize+n*nodeSize]
	for i := 0; i < n; i++ {
		off := i * nodeSize
		mustRand(nodes[off : off+32])
		if msgType == subtreeMsgFullNodes {
			binary.BigEndian.PutUint64(nodes[off+32:off+40], 1)
			binary.BigEndian.PutUint64(nodes[off+40:off+48], 250)
		}
	}
	if msgType == subtreeMsgFullNodes {
		binary.BigEndian.PutUint64(p[0:8], uint64(n))
		binary.BigEndian.PutUint64(p[8:16], uint64(n)*250)
	}
	binary.BigEndian.PutUint64(p[16:24], uint64(n))
	return subtree{root: merkle.Root(nodes, nodeSize, n), payload: p}
}

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
		fatalf("rand.Read: %v", err)
	}
}

func fatalf(format string, args ...any) { slog.Error(fmt.Sprintf(format, args...)); os.Exit(1) }
func infof(format string, args ...any)  { slog.Info(fmt.Sprintf(format, args...)) }
