// Command subtx-gen generates random BSV-over-UDP frames for load/functional
// testing of bitcoin-shard-proxy and bitcoin-shard-listener.
//
// See README.md for the full flag set. Example:
//
//	subtx-gen -addr [fd20::2]:9000 -shard-bits 2 -subtrees 8 -pps 1000 -duration 10s
package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/lightwebinc/bitcoin-subtx-generator/internal/announce"
	myframe "github.com/lightwebinc/bitcoin-subtx-generator/internal/frame"
	"github.com/lightwebinc/bitcoin-subtx-generator/internal/sender"
	"github.com/lightwebinc/bitcoin-subtx-generator/internal/seq"
	"github.com/lightwebinc/bitcoin-subtx-generator/internal/subtree"
)

// Version is overridden at build time via -ldflags "-X main.Version=<ver>".
var Version = "dev"

func main() {
	var (
		addr             = flag.String("addr", "[::1]:9000", "target host:port (UDP)")
		frameVer         = flag.Int("frame-version", 2, "frame version to emit (1 or 2)")
		shardBits        = flag.Uint("shard-bits", 2, "informational: shard-bits the proxy uses (for predicted-group logging)")
		subtrees         = flag.Int("subtrees", 8, "number of random subtree IDs (0 = no SubtreeID)")
		subtreeSeed      = flag.String("subtree-seed", "bitcoin-subtx-generator-default", "seed for deterministic subtree IDs (string or hex)")
		pps              = flag.Int("pps", 1000, "target packets per second (0 = unlimited)")
		duration         = flag.Duration("duration", 10*time.Second, "runtime (0 = until count reached or SIGINT)")
		count            = flag.Uint64("count", 0, "stop after N frames (0 = unlimited)")
		workers          = flag.Int("workers", 0, "worker goroutines (0 = runtime.NumCPU)")
		payloadSize      = flag.Int("payload-size", 512, "random transaction payload size in bytes")
		seqStart         = flag.Uint64("seq-start", 1, "first sequence number")
		seqGapEvery      = flag.Uint64("seq-gap-every", 0, "inject a gap every N frames (0 = disabled)")
		seqGapSize       = flag.Uint64("seq-gap-size", 1, "how many seq numbers to skip per gap")
		seqGapDelay      = flag.Duration("seq-gap-delay", 0, "delay before retransmitting the gap (0 = permanent gap)")
		logInterval      = flag.Duration("log-interval", time.Second, "periodic stats interval")
		printSubtrees    = flag.Bool("print-subtrees", false, "print all generated subtree IDs and exit")
		subtreeGroup     = flag.String("subtree-group", "", "comma-separated 32-char hex GroupIDs to announce (BRC-127)")
		announceAddr     = flag.String("announce-addr", "", "proxy TCP addr for SubtreeAnnounce (e.g. [::1]:9002); empty=disabled")
		announceInterval = flag.Duration("announce-interval", 10*time.Second, "SubtreeAnnounce re-announce period")
		announceTTL      = flag.Uint("announce-ttl", 0, "TTL field in SubtreeAnnounce; 0 = use listener default")
	)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "subtx-gen %s — BSV frame load generator\n\n", Version)
		fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n\nFlags:\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	// Resolve subtree seed: allow raw hex or plain string.
	var seedBytes []byte
	if b, err := hex.DecodeString(*subtreeSeed); err == nil && len(b) > 0 {
		seedBytes = b
	} else {
		seedBytes = []byte(*subtreeSeed)
	}
	pool := subtree.New(*subtrees, seedBytes)

	if *printSubtrees {
		for i := 0; i < pool.Len(); i++ {
			fmt.Printf("%02d  %s\n", i, pool.HexAt(i))
		}
		return
	}

	// Frame version.
	var fv myframe.Version
	switch *frameVer {
	case 1:
		fv = myframe.V1
	case 2:
		fv = myframe.V2
	default:
		log.Fatalf("frame-version must be 1 or 2, got %d", *frameVer)
	}

	w := *workers
	if w <= 0 {
		w = runtime.NumCPU()
	}

	// Allocator.
	alloc := seq.New(seq.Config{
		Start:    *seqStart,
		GapEvery: *seqGapEvery,
		GapSize:  *seqGapSize,
		GapDelay: *seqGapDelay,
	})

	// Graceful shutdown on SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Fprintf(os.Stderr, "subtx-gen %s: addr=%s frame=v%d pps=%d workers=%d subtrees=%d duration=%s\n",
		Version, *addr, *frameVer, *pps, w, pool.Len(), *duration)
	if pool.Len() > 0 {
		fmt.Fprintf(os.Stderr, "  subtree[0]=%s  subtree[n-1]=%s  shard-bits=%d\n",
			pool.HexAt(0), pool.HexAt(pool.Len()-1), *shardBits)
	}
	_ = shardBits // reserved for future predicted-group logging

	r := sender.New(sender.Config{
		Addr:         *addr,
		FrameVersion: fv,
		Workers:      w,
		PPS:          *pps,
		Duration:     *duration,
		Count:        *count,
		PayloadSize:  *payloadSize,
		LogInterval:  *logInterval,
	}, pool, alloc)

	// Start announce goroutine if configured.
	if *announceAddr != "" && *subtreeGroup != "" {
		groupIDs, err := announce.ParseGroupIDs(*subtreeGroup)
		if err != nil {
			log.Fatalf("subtree-group: %v", err)
		}
		sal := &announce.Sender{
			ProxyAddr: *announceAddr,
			GroupIDs:  groupIDs,
			Pool:      pool,
			Interval:  *announceInterval,
			TTL:       uint16(*announceTTL),
		}
		go func() {
			if err := sal.Run(ctx); err != nil && ctx.Err() == nil {
				log.Printf("announce: %v", err)
			}
		}()
		fmt.Fprintf(os.Stderr, "  announce: addr=%s groups=%d interval=%s\n",
			*announceAddr, len(groupIDs), *announceInterval)
	}

	start := time.Now()
	sent, err := r.Run(ctx)
	elapsed := time.Since(start)
	if err != nil {
		log.Fatalf("run: %v", err)
	}
	fmt.Fprintf(os.Stderr, "done: sent=%d errors=%d elapsed=%s avg_pps=%.0f\n",
		sent, r.Errors(), elapsed, float64(sent)/elapsed.Seconds())
}
