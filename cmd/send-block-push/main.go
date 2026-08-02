// Command send-block-push streams BRC-144 block PUSH objects to a shard-proxy
// tunnel-bound block ingress port (default 8727) over TCP.
//
// Each object is the header-stripped push wire form (shard-common/objfmt),
// strict model.Block.Bytes() parity with VarInts widened to uint64 BE:
//
//	Header(80) | TxCount(8,BE) | SizeInBytes(8,BE) | SubtreeCount(8,BE) |
//	SubtreeCount x SubtreeRoot(32) | InlineCoinbaseTx(self-delimiting) |
//	Height(8,BE) | CoinbaseBUMPLen(8,BE) | CoinbaseBUMP(bytes)
//
// The stream is bare and single-class — objects are self-delimited by their own
// counts, no outer framing. The proxy carries the whole BRC-144 body VERBATIM as
// the payload of a BRC-131 block-control frame (no lossy BlockAnnounce
// projection) and stamps HashKey/SeqNum from the observed source.
//
// This drives the CURRENT tunnel-bound miner block ingest. It replaces the
// framed BRC-131 path of send-block-announce, which targeted the removed :9002
// privileged multicast port and no longer has a proxy ingress.
//
// Headers are chained (each prevHash = the prior block hash) and carry REAL
// proof of work at the regtest-easy target 0x207fffff: the block-control gate
// (-require-block-pow) is on by default on both proxy and listener, so the
// nonce is ground until pow.CheckHeader passes (a couple of tries at that
// difficulty). A header claiming 0x207fffff is still rejected by a fabric whose
// floor is stricter — match -min-pow-bits to the network being driven. Every
// object is self-verified with objfmt.BlockSize before it is written.
//
// Usage:
//
//	send-block-push -addr [fd00:a::a]:8727 -interval 9m -subtrees 4
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
	"github.com/lightwebinc/subtx-generator/internal/blockhdr"
	"github.com/lightwebinc/subtx-generator/internal/tx"
)

const blockHeaderLen = blockhdr.Len

func main() {
	addr := flag.String("addr", "[::1]:8727", "proxy block push ingress address (host:port)")
	interval := flag.Duration("interval", 9*time.Minute, "delay between block objects")
	count := flag.Int("count", 0, "stop after N blocks (0 = until -duration / SIGINT)")
	duration := flag.Duration("duration", 0, "stop after this long (0 = until -count / SIGINT)")
	subtrees := flag.Int("subtrees", 4, "subtree roots per block")
	coinbaseSize := flag.Int("coinbase-size", 200, "inline coinbase transaction size in bytes")
	bumpSize := flag.Int("bump-size", 0, "coinbase BUMP (BRC-74) byte length (0 = none)")
	heightStart := flag.Int("height-start", 800000, "block height of the first block")
	seed := flag.String("seed", "block-push", "PRNG seed (identifies this source for the delivery matrix)")
	logHashes := flag.Bool("log-hashes", false, "print every block hash (for end-to-end hash compare)")
	flag.Parse()

	logging.Init(logging.Options{Service: "subtx-generator", Level: slog.LevelInfo, Format: logging.ParseFormat(os.Getenv("LOG_FORMAT"))})

	if *subtrees < 1 {
		fatalf("-subtrees must be >= 1")
	}
	if *coinbaseSize < tx.MinRawSize {
		fatalf("-coinbase-size must be >= %d (one valid raw tx, exactly)", tx.MinRawSize)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rng := rand.NewChaCha8(sha256.Sum256([]byte(*seed)))
	txb := tx.New(sha256.Sum256([]byte(*seed + ":coinbase")))

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
	var prevHash [32]byte // genesis prev = zero
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	for {
		height := *heightStart + sent
		obj, blockHash, cbTxID := buildBlock(rng, txb, *subtrees, *coinbaseSize, *bumpSize, uint64(height), prevHash)

		if n, err := objfmt.BlockSize(obj); err != nil || n != len(obj) {
			fatalf("built malformed block object (n=%d len=%d err=%v)", n, len(obj), err)
		}

		if conn == nil {
			conn = dial(ctx, *addr)
			if conn == nil {
				return
			}
		}
		_ = conn.SetWriteDeadline(time.Now().Add(15 * time.Second))
		if _, err := conn.Write(obj); err != nil {
			slog.Warn("write failed, reconnecting", "err", err)
			_ = conn.Close()
			conn = nil
			continue
		}
		sent++
		prevHash = blockHash
		if *logHashes {
			fmt.Printf("block %d height=%d hash=%x coinbase_txid=%x subtrees=%d bytes=%d\n",
				sent, height, blockHash, cbTxID, *subtrees, len(obj))
		}

		if *count > 0 && sent >= *count {
			break
		}

		select {
		case <-ctx.Done():
			infof("interrupted")
			infof("done: sent=%d block objects", sent)
			return
		case <-ticker.C:
			if !deadline.IsZero() && time.Now().After(deadline) {
				goto done
			}
		}
	}
done:
	infof("done: sent=%d block objects", sent)
}

// buildBlock constructs one BRC-144 block push object. It returns the object,
// the block hash (SHA256d of the 80-byte header — the object's identity for the
// hash proof, matching how the proxy derives ContentID), and the inline
// coinbase TxID.
func buildBlock(rng *rand.ChaCha8, txb *tx.Builder, subtrees, coinbaseSize, bumpSize int, height uint64, prevHash [32]byte) ([]byte, [32]byte, [32]byte) {
	// Subtree roots.
	roots := make([]byte, subtrees*32)
	fillRand(rng, roots)

	// Header: version(4 LE) | prevHash(32) | merkleRoot(32) | time(4 LE) | bits(4) | nonce(4).
	hdr := make([]byte, blockHeaderLen)
	binary.LittleEndian.PutUint32(hdr[0:4], 2)
	copy(hdr[4:36], prevHash[:])
	mr1 := sha256.Sum256(roots) // merkle root over the subtree roots (deterministic stand-in)
	mr := sha256.Sum256(mr1[:])
	copy(hdr[36:68], mr[:])
	binary.LittleEndian.PutUint32(hdr[68:72], uint32(1700000000+height)) // monotone stand-in time
	// nBits is serialised LITTLE-endian, like every other multi-byte header
	// field — pow.NBits reads it that way. Writing it big-endian yields the
	// compact value 0xffff7f20, whose sign bit is set, so CompactToTarget
	// rejects it and EVERY announce fails -require-block-pow.
	blockhdr.SetBits(hdr, blockhdr.BitsRegtest) // regtest-easy bits
	blockHash := blockhdr.Grind(hdr, uint32(height))

	// Inline coinbase (walkable BSV tx) + BUMP.
	coinbase, err := txb.Build(nil, coinbaseSize)
	if err != nil {
		fatalf("coinbase build: %v", err)
	}
	cbTxID, err := objfmt.TxID(coinbase)
	if err != nil {
		fatalf("coinbase TxID: %v", err)
	}
	bump := make([]byte, bumpSize)
	if bumpSize > 0 {
		fillRand(rng, bump)
	}

	// Assemble.
	total := objfmt.BlockPrefixSize + len(roots) + len(coinbase) + 16 + len(bump)
	obj := make([]byte, 0, total)
	obj = append(obj, hdr...)
	obj = appendU64(obj, uint64(subtrees)*1000+1) // TxCount (plausible; byte-only, not validated)
	obj = appendU64(obj, uint64(total))           // SizeInBytes
	obj = appendU64(obj, uint64(subtrees))        // SubtreeCount
	obj = append(obj, roots...)
	obj = append(obj, coinbase...)
	obj = appendU64(obj, height)            // Height
	obj = appendU64(obj, uint64(len(bump))) // CoinbaseBUMPLen
	obj = append(obj, bump...)
	return obj, blockHash, cbTxID
}

func appendU64(b []byte, v uint64) []byte {
	var x [8]byte
	binary.BigEndian.PutUint64(x[:], v)
	return append(b, x[:]...)
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
