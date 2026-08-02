// Command send-block-announce sends BRC-131 block control frames to
// shard-proxy via TCP for integration testing.
//
// It sends pairs of BlockAnnounce (MsgType 0x01) + CoinbaseTx (MsgType 0x02)
// frames, one pair per simulated block. SeqNum is left zero so the proxy
// stamps it in-place.
//
// Usage:
//
//	send-block-announce -addr [fd20::2]:9002 -blocks 5
package main

import (
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"log/slog"

	"github.com/lightwebinc/shard-common/logging"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/subtx-generator/internal/blockhdr"
	"github.com/lightwebinc/subtx-generator/internal/tx"
	"net"
	"os"
	"time"
)

// BRC-131 wire constants — mirror of shard-common/frame.
const (
	magicBSV       = 0xE3E1F3E8
	protoVer       = 0x02BF
	frameVerV4     = 0x04
	msgAnnounce    = 0x01
	msgCoinbase    = 0x02
	blockHeaderLen = 80
	headerSize     = 92
)

func main() {
	addr := flag.String("addr", "[::1]:9002", "proxy TCP address (host:port)")
	blocks := flag.Int("blocks", 10, "number of simulated blocks to announce")
	subtrees := flag.Int("subtrees", 4, "subtree hashes per BlockAnnounce frame")
	interval := flag.Duration("interval", 100*time.Millisecond, "delay between block pairs")
	coinbase := flag.Bool("coinbase", true, "also send a CoinbaseTx frame for each block")
	flag.Parse()
	logging.Init(logging.Options{Service: "subtx-generator", Level: slog.LevelInfo, Format: logging.ParseFormat(os.Getenv("LOG_FORMAT"))})

	conn, err := net.DialTimeout("tcp", *addr, 5*time.Second)
	if err != nil {
		fatalf("dial %s: %v", *addr, err)
	}
	defer func() { _ = conn.Close() }()
	infof("connected to %s", *addr)

	// Coinbase payloads are plain transactions: build one WALKABLE tx of the
	// exact target size with the canonical TxID (objfmt.TxID), never random
	// bytes — random bytes desync a downstream tx-class objfmt stream and
	// stamp an id no consumer ever derives.
	var seed [32]byte
	mustRand(seed[:])
	txb := tx.New(seed)

	sent := 0
	for i := 0; i < *blocks; i++ {
		// Build an 80-byte block header. Version/prevHash/merkleRoot/time may be
		// arbitrary, but nBits and the nonce may NOT: the block-control gate
		// (-require-block-pow, default ON) runs a real pow.CheckHeader, so a
		// fully random header is dropped as failing proof-of-work and the
		// announce never reaches a listener.
		var blockHdr [blockHeaderLen]byte
		mustRand(blockHdr[:])
		blockhdr.SetBits(blockHdr[:], blockhdr.BitsRegtest)
		blockHash := blockhdr.Grind(blockHdr[:], uint32(i))

		// Walkable coinbase transaction payload (size varies slightly).
		coinbaseTx, err := txb.Build(nil, 128+i%64)
		if err != nil {
			fatalf("block %d coinbase build: %v", i, err)
		}
		coinbaseTxID, err := objfmt.TxID(coinbaseTx)
		if err != nil {
			fatalf("block %d coinbase TxID: %v", i, err)
		}

		// Random subtree hashes.
		subtreeHashes := make([][32]byte, *subtrees)
		for j := range subtreeHashes {
			mustRand(subtreeHashes[j][:])
		}

		// --- BlockAnnounce frame ---
		announcePay := encodeBlockAnnounce(blockHdr, coinbaseTxID, subtreeHashes)
		announceFrame := encodeBlockFrame(msgAnnounce, blockHash, announcePay)
		if err := writeFrame(conn, announceFrame); err != nil {
			fatalf("block %d announce write: %v", i, err)
		}
		sent++

		// --- CoinbaseTx frame ---
		if *coinbase {
			coinbaseFrame := encodeBlockFrame(msgCoinbase, coinbaseTxID, coinbaseTx)
			if err := writeFrame(conn, coinbaseFrame); err != nil {
				fatalf("block %d coinbase write: %v", i, err)
			}
			sent++
		}

		fmt.Printf("block %d: hash=%x coinbase_txid=%x subtrees=%d\n",
			i, blockHash[:8], coinbaseTxID[:8], *subtrees)

		if i < *blocks-1 {
			time.Sleep(*interval)
		}
	}

	infof("done: sent=%d frames (%d blocks)", sent, *blocks)
}

// encodeBlockFrame builds a BRC-131 wire frame (SeqNum=0; proxy stamps it).
func encodeBlockFrame(msgType byte, contentID [32]byte, payload []byte) []byte {
	buf := make([]byte, headerSize+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], magicBSV)
	binary.BigEndian.PutUint16(buf[4:6], protoVer)
	buf[6] = frameVerV4
	buf[7] = msgType
	copy(buf[8:40], contentID[:])
	// HashKey (40:48) = 0 — proxy stamps
	// SeqNum  (48:56) = 0 — proxy stamps
	// Reserved32 (56:88) = 0
	binary.BigEndian.PutUint32(buf[88:92], uint32(len(payload)))
	copy(buf[92:], payload)
	return buf
}

// encodeBlockAnnounce builds a BlockAnnounce payload.
func encodeBlockAnnounce(header [blockHeaderLen]byte, coinbaseTxID [32]byte, subtreeHashes [][32]byte) []byte {
	size := blockHeaderLen + 32 + 4 + len(subtreeHashes)*32
	buf := make([]byte, size)
	copy(buf[0:80], header[:])
	copy(buf[80:112], coinbaseTxID[:])
	binary.BigEndian.PutUint32(buf[112:116], uint32(len(subtreeHashes)))
	for i, h := range subtreeHashes {
		copy(buf[116+i*32:], h[:])
	}
	return buf
}

// writeFrame writes a length-prefixed frame over TCP (same framing as the proxy
// TCP ingress: raw wire bytes, no additional envelope).
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
