// Command subtx-gen generates random BSV-over-UDP frames for load/functional
// testing of shard-proxy and shard-listener.
//
// See README.md for the full flag set. Example:
//
//	subtx-gen -addr [fd20::2]:8725 -shard-bits 2 -subtrees 8 -pps 1000 -duration 10s
package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lightwebinc/shard-common/logging"
	shardpkg "github.com/lightwebinc/shard-common/shard"

	"github.com/lightwebinc/subtx-generator/internal/announce"
	myframe "github.com/lightwebinc/subtx-generator/internal/frame"
	"github.com/lightwebinc/subtx-generator/internal/sender"
	"github.com/lightwebinc/subtx-generator/internal/seq"
	"github.com/lightwebinc/subtx-generator/internal/subtree"
)

// Version is overridden at build time via -ldflags "-X main.Version=<ver>".
var Version = "dev"

func main() {
	var (
		addr                  = flag.String("addr", "[::1]:8725", "target host:port (UDP)")
		frameVer              = flag.Int("frame-version", 2, "frame version to emit (1 or 2)")
		shardBits             = flag.Uint("shard-bits", 2, "informational: shard-bits the proxy uses (for predicted-group logging)")
		subtrees              = flag.Int("subtrees", 8, "number of random subtree IDs (0 = no SubtreeID)")
		subtreeSeed           = flag.String("subtree-seed", "subtx-generator-default", "seed for deterministic subtree IDs (string or hex)")
		pps                   = flag.Int("pps", 1000, "target packets per second (0 = unlimited)")
		duration              = flag.Duration("duration", 0, "max runtime (0 = until -count reached or SIGINT; if >0, stops at min(count,duration))")
		count                 = flag.Uint64("count", 0, "stop after N frames (0 = unlimited)")
		workers               = flag.Int("workers", 0, "worker goroutines (0 = runtime.NumCPU)")
		payloadSize           = flag.Int("payload-size", 512, "random transaction payload size in bytes")
		payloadFormat         = flag.String("payload-format", "brc124", "payload encoding: brc124 (raw tx), brc128 (BRC-30 EF), or mixed")
		seqStart              = flag.Uint64("seq-start", 1, "first sequence number")
		seqGapEvery           = flag.Uint64("seq-gap-every", 0, "inject a gap every N frames (0 = disabled)")
		seqGapSize            = flag.Uint64("seq-gap-size", 1, "how many seq numbers to skip per gap")
		seqGapDelay           = flag.Duration("seq-gap-delay", 0, "delay before retransmitting the gap (0 = permanent gap)")
		logInterval           = flag.Duration("log-interval", time.Second, "periodic stats interval")
		printSubtrees         = flag.Bool("print-subtrees", false, "print all generated subtree IDs and exit")
		subtreeGroup          = flag.String("subtree-group", "", "comma-separated 32-char hex GroupIDs to announce (BRC-127)")
		announceAddr          = flag.String("announce-addr", "", "proxy TCP addr for SubtreeGroupAnnounce (e.g. [::1]:9002); empty=disabled")
		announceInterval      = flag.Duration("announce-interval", 10*time.Second, "SubtreeGroupAnnounce re-announce period")
		announceTTL           = flag.Uint("announce-ttl", 0, "TTL field in SubtreeGroupAnnounce; 0 = use listener default")
		announcePhaseSize     = flag.Int("announce-phase-size", 0, "subtrees to add per phase tick (0 = announce full pool immediately)")
		announcePhaseInterval = flag.Duration("announce-phase-interval", 0, "how often to advance the phase; 0 = disabled")
		corruptTxIDRate       = flag.Uint("corrupt-txid-rate", 0, "percentage of frames to corrupt TxID (0-100, 0=disabled)")
		mode                  = flag.String("mode", "unicast", "send mode: unicast (default; forward to proxy via -addr) | direct-multicast (skip proxy, emit (S=bind-source, G=engine.Addr(txid)) directly)")
		bindSource            = flag.String("bind-source", "", "direct-multicast: IPv6 source bound on every egress socket; MUST match the value operators publish in the shard-manifest publishers list")
		egressIface           = flag.String("egress-iface", "", "direct-multicast: outbound interface for multicast egress")
		mcGroupIDFlag         = flag.String("mc-group-id", "0x000B", "direct-multicast: IANA group-id (bytes 12-13)")
		sourceModeFlag        = flag.String("source-mode", "asm", "direct-multicast: addressing model asm|ssm (selects FF05/FF35/FF3E prefix)")
		mcScopeFlag           = flag.String("scope", "site", "direct-multicast: multicast scope (site|global; in SSM mode follows shard.Prefix)")
		egressPort            = flag.Int("egress-port", 9001, "direct-multicast: destination UDP port for multicast datagrams")
	)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "subtx-gen %s — BSV frame load generator\n\n", Version)
		fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n\nFlags:\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	logging.Init(logging.Options{
		Service: "subtx-generator",
		Version: Version,
		Level:   slog.LevelInfo,
		Format:  logging.ParseFormat(os.Getenv("LOG_FORMAT")),
	})

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
		fatalf("frame-version must be 1 or 2, got %d", *frameVer)
	}

	// Payload format (BRC-12 raw vs BRC-30 EF).
	var pf sender.PayloadFormat
	switch *payloadFormat {
	case "brc124", "raw":
		pf = sender.PayloadBRC124
	case "brc128", "ef":
		pf = sender.PayloadBRC128
	case "mixed":
		pf = sender.PayloadMixed
	default:
		fatalf("payload-format must be brc124, brc128, or mixed; got %q", *payloadFormat)
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

	fmt.Fprintf(os.Stderr, "subtx-gen %s: addr=%s frame=v%d payload=%s pps=%d workers=%d subtrees=%d duration=%s\n",
		Version, *addr, *frameVer, pf, *pps, w, pool.Len(), *duration)
	if pool.Len() > 0 {
		fmt.Fprintf(os.Stderr, "  subtree[0]=%s  subtree[n-1]=%s  shard-bits=%d\n",
			pool.HexAt(0), pool.HexAt(pool.Len()-1), *shardBits)
	}
	_ = shardBits // reserved for future predicted-group logging

	cfg := sender.Config{
		Addr:            *addr,
		FrameVersion:    fv,
		Workers:         w,
		PPS:             *pps,
		Duration:        *duration,
		Count:           *count,
		PayloadSize:     *payloadSize,
		PayloadFormat:   pf,
		LogInterval:     *logInterval,
		CorruptTxIDRate: *corruptTxIDRate,
		ShardBits:       *shardBits,
	}

	switch strings.ToLower(*mode) {
	case "unicast":
		cfg.Mode = sender.ModeUnicast
	case "direct-multicast":
		cfg.Mode = sender.ModeDirectMulticast
		if *bindSource == "" {
			fatal("direct-multicast: -bind-source is required")
		}
		ip := net.ParseIP(*bindSource)
		if ip == nil || ip.To4() != nil {
			fatalf("direct-multicast: invalid -bind-source %q: must be an IPv6 literal", *bindSource)
		}
		cfg.BindSource = ip
		if *egressIface != "" {
			iface, err := net.InterfaceByName(*egressIface)
			if err != nil {
				fatalf("direct-multicast: -egress-iface %q: %v", *egressIface, err)
			}
			cfg.EgressIface = iface
		}
		var sm shardpkg.SourceMode
		switch strings.ToLower(*sourceModeFlag) {
		case "asm":
			sm = shardpkg.SourceModeASM
		case "ssm":
			sm = shardpkg.SourceModeSSM
		default:
			fatalf("direct-multicast: invalid -source-mode %q (asm|ssm)", *sourceModeFlag)
		}
		scope, err := shardpkg.ParseScope(*mcScopeFlag)
		if err != nil {
			fatalf("direct-multicast: -scope: %v", err)
		}
		prefix, err := shardpkg.Prefix(sm, scope)
		if err != nil {
			fatalf("direct-multicast: %v", err)
		}
		cfg.MCPrefix = prefix
		gid, err := parseUint16(*mcGroupIDFlag)
		if err != nil {
			fatalf("direct-multicast: -mc-group-id: %v", err)
		}
		cfg.MCGroupID = gid
		if *egressPort < 1 || *egressPort > 65535 {
			fatalf("direct-multicast: -egress-port out of range: %d", *egressPort)
		}
		cfg.EgressPort = *egressPort
	default:
		fatalf("invalid -mode %q (unicast|direct-multicast)", *mode)
	}

	r := sender.New(cfg, pool, alloc)

	// Start announce goroutine if configured.
	if *announceAddr != "" && *subtreeGroup != "" {
		groupIDs, err := announce.ParseGroupIDs(*subtreeGroup)
		if err != nil {
			fatalf("subtree-group: %v", err)
		}
		sal := &announce.Sender{
			ProxyAddr:     *announceAddr,
			GroupIDs:      groupIDs,
			Pool:          pool,
			Interval:      *announceInterval,
			TTL:           uint16(*announceTTL),
			PhaseSize:     *announcePhaseSize,
			PhaseInterval: *announcePhaseInterval,
		}
		go func() {
			if err := sal.Run(ctx); err != nil && ctx.Err() == nil {
				infof("announce: %v", err)
			}
		}()
		if *announcePhaseSize > 0 && *announcePhaseInterval > 0 {
			fmt.Fprintf(os.Stderr, "  announce: addr=%s groups=%d interval=%s phase-size=%d phase-interval=%s\n",
				*announceAddr, len(groupIDs), *announceInterval, *announcePhaseSize, *announcePhaseInterval)
		} else {
			fmt.Fprintf(os.Stderr, "  announce: addr=%s groups=%d interval=%s\n",
				*announceAddr, len(groupIDs), *announceInterval)
		}
	}

	start := time.Now()
	sent, err := r.Run(ctx)
	elapsed := time.Since(start)
	if err != nil {
		fatalf("run: %v", err)
	}
	fmt.Fprintf(os.Stderr, "done: sent=%d errors=%d elapsed=%s avg_pps=%.0f\n",
		sent, r.Errors(), elapsed, float64(sent)/elapsed.Seconds())
}

// parseUint16 parses "0x000B" / "0B" / "11" forms into a uint16.
func parseUint16(s string) (uint16, error) {
	s = strings.TrimSpace(s)
	base := 10
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		s = s[2:]
		base = 16
	}
	v, err := strconv.ParseUint(s, base, 16)
	if err != nil {
		return 0, err
	}
	return uint16(v), nil
}

// fatalf logs a formatted error through the unified logger and exits non-zero.
func fatalf(format string, args ...any) {
	slog.Error(fmt.Sprintf(format, args...))
	os.Exit(1)
}

// fatal logs an error message through the unified logger and exits non-zero.
func fatal(msg string) {
	slog.Error(msg)
	os.Exit(1)
}

// infof logs a formatted info line through the unified logger.
func infof(format string, args ...any) {
	slog.Info(fmt.Sprintf(format, args...))
}
