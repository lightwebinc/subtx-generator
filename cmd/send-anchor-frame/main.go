// Command send-anchor-frame sends BRC-134 chained anchor transaction frames to
// shard-proxy via UDP (default) or TCP for integration testing.
//
// Anchor frames use FrameVerV6 (0x06) with a 92-byte header identical to
// BRC-124. The proxy stamps HashKey and SeqNum in-place and forwards the
// frame to FF0E::B:FFFE (CtrlGroupControl).
//
// Usage:
//
//	send-anchor-frame -addr [fd20::2]:9000 -count 20
//	send-anchor-frame -addr [fd20::2]:9002 -tcp -count 20
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

// BRC-134 wire constants — mirror of shard-common/frame.
const (
	magicBSV   = 0xE3E1F3E8
	protoVer   = 0x02BF
	frameVerV6 = 0x06
	headerSize = 92
)

func main() {
	addr := flag.String("addr", "[::1]:9000", "proxy address (host:port); UDP by default")
	count := flag.Int("count", 10, "number of anchor frames to send")
	payloadSize := flag.Int("payload-size", 256, "raw anchor tx payload size in bytes")
	interval := flag.Duration("interval", 50*time.Millisecond, "delay between frames")
	useTCP := flag.Bool("tcp", false, "send over TCP instead of UDP")
	flag.Parse()

	var send func([]byte) error
	if *useTCP {
		conn, err := net.DialTimeout("tcp", *addr, 5*time.Second)
		if err != nil {
			log.Fatalf("tcp dial %s: %v", *addr, err)
		}
		defer func() { _ = conn.Close() }()
		log.Printf("connected TCP → %s", *addr)
		send = func(b []byte) error {
			_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			_, err := conn.Write(b)
			return err
		}
	} else {
		conn, err := net.Dial("udp", *addr)
		if err != nil {
			log.Fatalf("udp dial %s: %v", *addr, err)
		}
		defer func() { _ = conn.Close() }()
		log.Printf("sending UDP → %s", *addr)
		send = func(b []byte) error {
			_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			_, err := conn.Write(b)
			return err
		}
	}

	for i := 0; i < *count; i++ {
		var txid [32]byte
		mustRand(txid[:])

		payload := make([]byte, *payloadSize)
		mustRand(payload)

		frame := encodeAnchorFrame(txid, payload)
		if err := send(frame); err != nil {
			log.Fatalf("frame %d send: %v", i, err)
		}

		fmt.Printf("anchor %d: txid=%x payload_bytes=%d\n", i, txid[:8], len(payload))

		if i < *count-1 {
			time.Sleep(*interval)
		}
	}

	log.Printf("done: sent=%d anchor frames", *count)
}

// encodeAnchorFrame builds a BRC-134 wire frame (HashKey/SeqNum=0; proxy stamps).
func encodeAnchorFrame(txid [32]byte, payload []byte) []byte {
	buf := make([]byte, headerSize+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], magicBSV)
	binary.BigEndian.PutUint16(buf[4:6], protoVer)
	buf[6] = frameVerV6
	buf[7] = 0x00 // Reserved
	copy(buf[8:40], txid[:])
	// HashKey (40:48) = 0 — proxy stamps
	// SeqNum  (48:56) = 0 — proxy stamps
	// SubtreeID (56:88) = zeros — anchor frames have no subtree
	binary.BigEndian.PutUint32(buf[88:92], uint32(len(payload)))
	copy(buf[92:], payload)
	return buf
}

func mustRand(b []byte) {
	if _, err := rand.Read(b); err != nil {
		log.Fatalf("rand.Read: %v", err)
	}
}
