// Command send-subtree-push streams BRC-143 subtree PUSH objects to a
// shard-proxy tunnel-bound subtree ingress port (default 8726) over TCP.
//
// Each object is the header-stripped push wire form (shard-common/objfmt):
//
//	SubtreeMerkleRoot(32) | NodeCount(8, BE) | NodeCount x NodeHash(32)
//
// The stream is bare and single-class — objects are self-delimited by NodeCount,
// with no outer length prefix or type tag. The proxy reframes each object into a
// BRC-132 subtree-data multicast frame (hashes-only) and stamps HashKey/SeqNum
// from the observed source; the in-band merkle root becomes the frame SubtreeID.
//
// This drives the CURRENT tunnel-bound miner subtree ingest. It replaces the
// framed BRC-132 path of send-subtree-data, which targeted the removed :9002
// privileged multicast port and no longer has a proxy ingress.
//
// Every object is self-verified with objfmt.SubtreeSize before it is written, so
// a malformed emitter can never masquerade as a fabric/proxy fault.
//
// Usage:
//
//	send-subtree-push -addr [fd00:a::a]:8726 -interval 1s -nodes 16
package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lightwebinc/shard-common/logging"
	"github.com/lightwebinc/shard-common/objfmt"
)

func main() {
	addr := flag.String("addr", "[::1]:8726", "proxy subtree push ingress address (host:port)")
	interval := flag.Duration("interval", time.Second, "delay between subtree objects")
	count := flag.Int("count", 0, "stop after N objects (0 = until -duration / SIGINT)")
	duration := flag.Duration("duration", 0, "stop after this long (0 = until -count / SIGINT)")
	nodes := flag.Int("nodes", 16, "leaf node hashes per subtree")
	coinbasePlaceholder := flag.Bool("coinbase-placeholder", true,
		"set node[0] to the 0xFF*32 coinbase placeholder (BRC-143 convention)")
	seed := flag.String("seed", "subtree-push", "PRNG seed (identifies this source for the delivery matrix)")
	logHashes := flag.Bool("log-hashes", false, "print every subtree root (for end-to-end hash compare)")
	flag.Parse()

	logging.Init(logging.Options{Service: "subtx-generator", Level: slog.LevelInfo, Format: logging.ParseFormat(os.Getenv("LOG_FORMAT"))})

	if *nodes < 1 {
		fatalf("-nodes must be >= 1")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rng := rand.NewChaCha8(sha256.Sum256([]byte(*seed)))

	var deadline time.Time
	if *duration > 0 {
		deadline = time.Now().Add(*duration)
	}

	conn := dial(ctx, *addr)
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()

	sent := 0
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	for {
		obj, root := buildSubtree(rng, *nodes, *coinbasePlaceholder)

		// Self-verify: the object MUST delimit exactly, or we would be blaming
		// the fabric for a generator bug.
		if n, err := objfmt.SubtreeSize(obj); err != nil || n != len(obj) {
			fatalf("built malformed subtree object (n=%d len=%d err=%v)", n, len(obj), err)
		}

		if conn == nil {
			conn = dial(ctx, *addr)
			if conn == nil {
				return // ctx cancelled while dialling
			}
		}
		_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if _, err := conn.Write(obj); err != nil {
			slog.Warn("write failed, reconnecting", "err", err)
			_ = conn.Close()
			conn = nil
			continue
		}
		sent++
		if *logHashes {
			fmt.Printf("subtree %d root=%x nodes=%d bytes=%d\n", sent, root, *nodes, len(obj))
		}

		if *count > 0 && sent >= *count {
			break
		}

		select {
		case <-ctx.Done():
			infof("interrupted")
			infof("done: sent=%d subtree objects", sent)
			return
		case <-ticker.C:
			if !deadline.IsZero() && time.Now().After(deadline) {
				goto done
			}
		}
	}
done:
	infof("done: sent=%d subtree objects", sent)
}

// buildSubtree constructs one BRC-143 subtree push object and returns it with
// its merkle root. The root is SHA256d over the node hashes — a deterministic,
// content-binding stand-in identity carried in-band (objfmt is byte-only and
// does not recompute a consensus merkle root); the receiver compares the
// delivered root against this one for the hash proof.
func buildSubtree(rng *rand.ChaCha8, nodes int, coinbasePlaceholder bool) ([]byte, [32]byte) {
	obj := make([]byte, objfmt.SubtreeHeaderSize+nodes*32)
	for i := 0; i < nodes; i++ {
		off := objfmt.SubtreeHeaderSize + i*32
		if i == 0 && coinbasePlaceholder {
			for j := 0; j < 32; j++ {
				obj[off+j] = 0xFF
			}
			continue
		}
		fillRand(rng, obj[off:off+32])
	}
	h1 := sha256.Sum256(obj[objfmt.SubtreeHeaderSize:])
	root := sha256.Sum256(h1[:])
	copy(obj[0:32], root[:])
	binary.BigEndian.PutUint64(obj[32:40], uint64(nodes))
	return obj, root
}

// fillRand writes deterministic pseudo-random bytes from the ChaCha8 stream.
func fillRand(r *rand.ChaCha8, buf []byte) {
	for len(buf) >= 8 {
		binary.LittleEndian.PutUint64(buf, r.Uint64())
		buf = buf[8:]
	}
	if len(buf) > 0 {
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], r.Uint64())
		copy(buf, tmp[:])
	}
}

// dial connects with retry until it succeeds or ctx is cancelled.
func dial(ctx context.Context, addr string) net.Conn {
	backoff := 250 * time.Millisecond
	for {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err == nil {
			infof("connected to %s", addr)
			return conn
		}
		slog.Warn("dial failed, retrying", "addr", addr, "err", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

func fatalf(format string, args ...any) { slog.Error(fmt.Sprintf(format, args...)); os.Exit(1) }
func infof(format string, args ...any)  { slog.Info(fmt.Sprintf(format, args...)) }
