// Command tunnel-sink is a consumer-side diagnostic sink for the tunnel
// delivery plane. It listens on the consumer's SDA (service delivery
// address, default :8833 — the standard Teranode propagation port), accepts
// the edge's push connections, and logs one line per delivered object:
// timestamp, direction, interface, frame type (BRC #), class, object id,
// and the class detail (tx payload size / subtree node count / block
// subtree count).
//
// Delivery lanes are bare and single-class (shard-common/objfmt): each TCP
// connection carries exactly one of raw/EF transactions (BRC-12/BRC-30),
// BRC-143 subtree push frames, or BRC-144 block bodies, self-delimiting with
// no tag or length prefix. The lane class of each connection is auto-detected
// from its first bytes (override with -lane).
//
// With -submit-edge the tool also logs the SENT direction: it accepts local
// submissions on -submit-listen, detects the class of the submitted stream,
// relays it to the edge's matching ingress port (tx 8725 / subtree 8726 /
// block 8727), and logs every relayed object. Framed BRC-124 submission
// streams (subtx-gen -tcp) are detected by network magic and relayed to the
// tx port.
//
// On SIGINT/SIGTERM (or listener failure) the tool prints per-class session
// summary statistics (disable with -summary=false).
//
// Usage:
//
//	tunnel-sink -listen '[fd00:1b5e::1]:8833'
//	tunnel-sink -listen :8833 -submit-edge edge.example.net
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/logging"
	"github.com/lightwebinc/shard-common/objfmt"
)

const (
	dirRecv = "RECV"
	dirSent = "SENT"

	sniffDecideAt = 64 << 10 // undecided past this: pick best-scoring class
	sniffCap      = 1 << 20  // absolute sniff bound; unclassifiable beyond it
)

func main() {
	listen := flag.String("listen", ":8833", "delivery listen address (the consumer SDA; host empty = all interfaces)")
	lane := flag.String("lane", "auto", "force the delivery lane class: auto|tx|subtree|block")
	summary := flag.Bool("summary", true, "print session summary statistics on exit")
	maxObject := flag.Int("max-object", 256<<20, "maximum single object size in bytes")
	submitEdge := flag.String("submit-edge", "", "edge host: enable the submit relay (SENT direction) toward this edge")
	submitListen := flag.String("submit-listen", "localhost:8724", "submit relay listen address (used with -submit-edge)")
	submitTxPort := flag.Int("submit-tx-port", 8725, "edge tx ingress port")
	submitSubtreePort := flag.Int("submit-subtree-port", 8726, "edge subtree push ingress port")
	submitBlockPort := flag.Int("submit-block-port", 8727, "edge block push ingress port")
	flag.Parse()

	logging.Init(logging.Options{Service: "subtx-generator", Level: slog.LevelInfo, Format: logging.ParseFormat(os.Getenv("LOG_FORMAT"))})

	var forced objfmt.Class
	switch *lane {
	case "auto":
	case "tx":
		forced = objfmt.ClassTx
	case "subtree":
		forced = objfmt.ClassSubtree
	case "block":
		forced = objfmt.ClassBlock
	default:
		fatalf("-lane must be auto|tx|subtree|block")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st := newStats()

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		fatalf("listen %s: %v", *listen, err)
	}
	slog.Info("delivery sink listening", "addr", ln.Addr().String(), "lane", *lane)

	var subLn net.Listener
	if *submitEdge != "" {
		subLn, err = net.Listen("tcp", *submitListen)
		if err != nil {
			fatalf("submit listen %s: %v", *submitListen, err)
		}
		slog.Info("submit relay listening", "addr", subLn.Addr().String(), "edge", *submitEdge,
			"tx_port", *submitTxPort, "subtree_port", *submitSubtreePort, "block_port", *submitBlockPort)
	}

	go acceptLoop(ln, func(c net.Conn) { handleDelivery(c, forced, *maxObject, st) })
	if subLn != nil {
		relay := &submitRelay{
			edge:        *submitEdge,
			txPort:      *submitTxPort,
			subtreePort: *submitSubtreePort,
			blockPort:   *submitBlockPort,
			maxObject:   *maxObject,
			st:          st,
		}
		go acceptLoop(subLn, relay.handle)
	}

	<-ctx.Done()
	_ = ln.Close()
	if subLn != nil {
		_ = subLn.Close()
	}
	if *summary {
		st.printSummary(os.Stdout, ln.Addr().String(), subLn, *submitEdge, *submitTxPort, *submitSubtreePort, *submitBlockPort)
	}
}

func acceptLoop(ln net.Listener, handle func(net.Conn)) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go handle(c)
	}
}

// ── delivery (RECV) ─────────────────────────────────────────────────────────

func handleDelivery(conn net.Conn, forced objfmt.Class, maxObject int, st *stats) {
	defer conn.Close()
	iface := ifaceFor(conn.LocalAddr())
	st.connOpen()
	defer st.connClose()

	var (
		cls    objfmt.Class
		framed bool
		pre    []byte
		err    error
	)
	if forced != 0 {
		cls = forced
	} else {
		cls, framed, pre, err = sniffClass(conn)
		if err != nil {
			slog.Warn("delivery connection unclassifiable", "remote", conn.RemoteAddr().String(), "err", err)
			st.parseError()
			return
		}
	}
	laneName := "framed"
	if !framed {
		laneName = cls.String()
	}
	slog.Info("delivery connection", "remote", conn.RemoteAddr().String(), "iface", iface, "lane", laneName)

	if framed {
		err = pumpFramed(conn, pre, dirRecv, iface, nil, maxObject, st)
	} else {
		err = pumpClassStream(conn, pre, cls, dirRecv, iface, nil, maxObject, st)
	}
	logStreamEnd("delivery", conn, err, st)
}

// ── submit relay (SENT) ─────────────────────────────────────────────────────

type submitRelay struct {
	edge        string
	txPort      int
	subtreePort int
	blockPort   int
	maxObject   int
	st          *stats
}

func (r *submitRelay) handle(client net.Conn) {
	defer client.Close()
	r.st.connOpen()
	defer r.st.connClose()

	cls, framed, pre, err := sniffClass(client)
	if err != nil {
		slog.Warn("submit connection unclassifiable", "remote", client.RemoteAddr().String(), "err", err)
		r.st.parseError()
		return
	}
	port := r.txPort // framed submissions ride the tx ingress (magic-detected by the proxy)
	switch cls {
	case objfmt.ClassSubtree:
		port = r.subtreePort
	case objfmt.ClassBlock:
		port = r.blockPort
	}
	addr := net.JoinHostPort(r.edge, strconv.Itoa(port))
	edge, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		slog.Warn("submit relay dial failed", "edge", addr, "err", err)
		return
	}
	defer edge.Close()
	iface := ifaceFor(edge.LocalAddr())
	laneName := "framed"
	if !framed {
		laneName = cls.String()
	}
	slog.Info("submit connection", "remote", client.RemoteAddr().String(), "edge", addr, "iface", iface, "lane", laneName)

	// The ingress lanes are one-way; drain the edge side only to notice close.
	go func() {
		_, _ = io.Copy(io.Discard, edge)
		_ = client.Close()
	}()

	if framed {
		err = pumpFramed(client, pre, dirSent, iface, edge, r.maxObject, r.st)
	} else {
		err = pumpClassStream(client, pre, cls, dirSent, iface, edge, r.maxObject, r.st)
	}
	logStreamEnd("submit", client, err, r.st)
}

// ── stream pumps ────────────────────────────────────────────────────────────

// pumpClassStream splits a bare single-class stream into objects, optionally
// forwards each verbatim, and logs one line per object.
func pumpClassStream(conn net.Conn, pre []byte, cls objfmt.Class, dir, iface string, forward net.Conn, maxObject int, st *stats) error {
	rd := objfmt.NewReader(io.MultiReader(bytes.NewReader(pre), conn), cls)
	rd.SetMaxObject(maxObject)
	for {
		obj, err := rd.Next()
		if err != nil {
			return err
		}
		if forward != nil {
			if _, werr := forward.Write(obj); werr != nil {
				return fmt.Errorf("relay write: %w", werr)
			}
		}
		logBareObject(dir, iface, cls, obj, st)
	}
}

func logBareObject(dir, iface string, cls objfmt.Class, obj []byte, st *stats) {
	switch cls {
	case objfmt.ClassTx:
		brc := "BRC-12"
		if objfmt.IsEF(obj) {
			brc = "BRC-30"
		}
		var idStr = "?"
		if id, err := objfmt.TxID(obj); err == nil {
			idStr = shortID(id[:])
		}
		logLine(dir, iface, brc, "txn", idStr, fmt.Sprintf("size=%s", byteSize(len(obj))))
		st.record(dir, "txn", len(obj), 0, 0)
	case objfmt.ClassSubtree:
		nodes := binary.BigEndian.Uint64(obj[32:40])
		logLine(dir, iface, "BRC-143", "subtree", shortID(obj[0:32]), fmt.Sprintf("nodes=%d", nodes))
		st.record(dir, "subtree", len(obj), nodes, 0)
	case objfmt.ClassBlock:
		id := sha256d(obj[:80])
		subtrees := binary.BigEndian.Uint64(obj[96:104])
		logLine(dir, iface, "BRC-144", "block", shortID(id[:]), fmt.Sprintf("subtrees=%d", subtrees))
		st.record(dir, "block", len(obj), 0, subtrees)
	}
}

// pumpFramed handles a full multicast-frame stream (network magic detected):
// the framed submit grammar of the proxy tx ingress, or the deprecated
// full-frame delivery mode. Frames are logged by FrameVer and forwarded
// verbatim when relaying.
func pumpFramed(conn net.Conn, pre []byte, dir, iface string, forward net.Conn, maxObject int, st *stats) error {
	br := bufio.NewReader(io.MultiReader(bytes.NewReader(pre), conn))
	hdr := make([]byte, frame.HeaderSizeV3)
	for {
		if _, err := io.ReadFull(br, hdr[:frame.HeaderSize]); err != nil {
			return err
		}
		if binary.BigEndian.Uint32(hdr[0:4]) != frame.MagicBSV {
			return fmt.Errorf("framed stream lost sync (bad magic)")
		}
		ver := hdr[6]
		hdrLen := frame.HeaderSize
		if ver == 0x03 {
			hdrLen = frame.HeaderSizeV3
			if _, err := io.ReadFull(br, hdr[frame.HeaderSize:hdrLen]); err != nil {
				return err
			}
		}
		if ver == 0x01 {
			return fmt.Errorf("legacy BRC-12 44-byte frames not supported")
		}
		payLen := int(binary.BigEndian.Uint32(hdr[88:92]))
		if payLen > maxObject {
			return fmt.Errorf("frame payload %d exceeds -max-object", payLen)
		}
		payload := make([]byte, payLen)
		if _, err := io.ReadFull(br, payload); err != nil {
			return err
		}
		if forward != nil {
			if _, werr := forward.Write(hdr[:hdrLen]); werr != nil {
				return fmt.Errorf("relay write: %w", werr)
			}
			if _, werr := forward.Write(payload); werr != nil {
				return fmt.Errorf("relay write: %w", werr)
			}
		}
		logFramed(dir, iface, ver, hdr[:hdrLen], payload, st)
	}
}

func logFramed(dir, iface string, ver byte, hdr, payload []byte, st *stats) {
	wire := len(hdr) + len(payload)
	brc, class, bucket := "BRC-?", fmt.Sprintf("ver%02x", ver), "other"
	id := hdr[8:40] // TxID header field
	switch ver {
	case 0x02:
		brc, class, bucket = "BRC-124", "txn", "txn"
		if objfmt.IsEF(payload) {
			brc = "BRC-128"
		}
		if allZero(id) {
			if txid, err := objfmt.TxID(payload); err == nil {
				id = txid[:]
			}
		}
	case 0x03:
		brc, class = "BRC-130", "frag"
	case 0x04:
		brc, class, bucket = "BRC-131", "block", "block"
		if len(payload) >= 80 {
			h := sha256d(payload[:80])
			id = h[:]
		}
	case 0x05:
		brc, class, bucket = "BRC-132", "subtree", "subtree"
		id = hdr[56:88] // SubtreeID header field
	case 0x06:
		brc, class = "BRC-134", "anchor"
	case 0x07:
		brc, class = "BRC-135", "header"
		if len(payload) == 80 {
			h := sha256d(payload)
			id = h[:]
		}
	case 0x08:
		brc, class = "BRC-142", "bundle"
	case 0x09:
		brc, class = "BRC-149", "beef"
	}
	logLine(dir, iface, brc, class, shortID(id), fmt.Sprintf("size=%s", byteSize(len(payload))))
	st.record(dir, bucket, wire, 0, 0)
}

func logStreamEnd(kind string, conn net.Conn, err error, st *stats) {
	switch {
	case err == nil, errors.Is(err, io.EOF):
		slog.Info(kind+" connection closed", "remote", conn.RemoteAddr().String())
	case errors.Is(err, io.ErrUnexpectedEOF):
		slog.Warn(kind+" connection closed mid-object", "remote", conn.RemoteAddr().String())
		st.parseError()
	case errors.Is(err, net.ErrClosed), strings.Contains(err.Error(), "use of closed"):
		slog.Info(kind+" connection closed", "remote", conn.RemoteAddr().String())
	default:
		slog.Warn(kind+" stream error", "remote", conn.RemoteAddr().String(), "err", err)
		st.parseError()
	}
}

// ── lane class sniffing ─────────────────────────────────────────────────────

// sniffClass reads the leading bytes of a connection and decides which
// single-class lane it carries. Lanes are bare (no tag), so the class is
// inferred: network magic ⇒ framed stream; otherwise each candidate codec
// walks the buffered prefix and implausible candidates are eliminated
// (tx version word, BRC-143 NodeCount, BRC-144 count fields). The consumed
// prefix is returned for replay into the stream reader.
func sniffClass(conn net.Conn) (cls objfmt.Class, framed bool, pre []byte, err error) {
	// The sniff loop arms a short read deadline to settle idle streams; it
	// must never escape into the steady-state pump, where a delivery pause
	// would then surface as a spurious i/o timeout that kills the connection.
	defer func() { _ = conn.SetReadDeadline(time.Time{}) }()
	buf := make([]byte, 0, 8<<10)
	tmp := make([]byte, 32<<10)
	eof := false
	for {
		if len(buf) >= 4 && binary.BigEndian.Uint32(buf[0:4]) == frame.MagicBSV {
			return 0, true, buf, nil
		}
		if cls, ok := classifyBuffer(buf, eof); ok {
			return cls, false, buf, nil
		}
		if eof {
			return 0, false, buf, fmt.Errorf("stream ended before a lane class could be inferred (%d bytes)", len(buf))
		}
		if len(buf) >= sniffCap {
			return 0, false, buf, fmt.Errorf("no lane class inferred within %d bytes", sniffCap)
		}
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, rerr := conn.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if rerr != nil {
			var nerr net.Error
			if errors.As(rerr, &nerr) && nerr.Timeout() {
				// Idle stream: settle for the best complete-object candidate.
				if cls, ok := bestCandidate(buf, true); ok {
					return cls, false, buf, nil
				}
				continue
			}
			eof = true
		}
	}
}

type candScore struct {
	viable   bool
	complete int // whole objects walked in the buffered prefix
}

func walkCandidate(cls objfmt.Class, buf []byte) candScore {
	s := candScore{viable: true}
	switch cls {
	case objfmt.ClassTx:
		if len(buf) >= 4 {
			if v := binary.LittleEndian.Uint32(buf[0:4]); v != 1 && v != 2 {
				return candScore{}
			}
		}
	case objfmt.ClassSubtree:
		if len(buf) >= objfmt.SubtreeHeaderSize {
			if n := binary.BigEndian.Uint64(buf[32:40]); n == 0 || n > 1<<24 {
				return candScore{}
			}
		}
	case objfmt.ClassBlock:
		if len(buf) >= objfmt.BlockPrefixSize {
			txCount := binary.BigEndian.Uint64(buf[80:88])
			size := binary.BigEndian.Uint64(buf[88:96])
			subtrees := binary.BigEndian.Uint64(buf[96:104])
			if subtrees == 0 || subtrees > 1<<20 || txCount > 1<<36 || size > 1<<40 {
				return candScore{}
			}
		}
	}
	off := 0
	for off < len(buf) {
		n, err := objfmt.Size(cls, buf[off:])
		if err == nil {
			s.complete++
			off += n
			continue
		}
		if errors.Is(err, objfmt.ErrShort) {
			break
		}
		return candScore{}
	}
	return s
}

var sniffOrder = []objfmt.Class{objfmt.ClassTx, objfmt.ClassSubtree, objfmt.ClassBlock}

// classifyBuffer decides once the evidence is unambiguous: a single viable
// candidate after all pre-checks had data to run, a single candidate that
// has walked whole objects while the rest walked none (a pending candidate
// whose plausibility fields landed on stray bytes cannot be eliminated, only
// out-scored), or a best-scoring pick at EOF / past the decision threshold.
func classifyBuffer(buf []byte, eof bool) (objfmt.Class, bool) {
	viable := make([]objfmt.Class, 0, 3)
	complete := make([]objfmt.Class, 0, 3)
	for _, c := range sniffOrder {
		s := walkCandidate(c, buf)
		if !s.viable {
			continue
		}
		viable = append(viable, c)
		if s.complete >= 1 {
			complete = append(complete, c)
		}
	}
	allChecked := eof || len(buf) >= objfmt.BlockPrefixSize
	if !allChecked {
		return 0, false
	}
	if len(viable) == 1 {
		return viable[0], true
	}
	if len(viable) > 1 {
		if len(complete) == 1 {
			return complete[0], true
		}
		if eof || len(buf) >= sniffDecideAt {
			return bestCandidate(buf, false)
		}
	}
	return 0, false
}

// bestCandidate picks the viable class that walked the most complete objects
// (requireComplete demands at least one). Ties resolve in sniffOrder.
func bestCandidate(buf []byte, requireComplete bool) (objfmt.Class, bool) {
	best, bestScore := objfmt.Class(0), -1
	for _, c := range sniffOrder {
		s := walkCandidate(c, buf)
		if s.viable && s.complete > bestScore {
			best, bestScore = c, s.complete
		}
	}
	if best == 0 || (requireComplete && bestScore < 1) {
		return 0, false
	}
	return best, true
}

// ── log lines ───────────────────────────────────────────────────────────────

var outMu sync.Mutex

func logLine(dir, iface, brc, class, id, detail string) {
	outMu.Lock()
	fmt.Printf("%s  %-4s  %-8s  %-7s  %-7s  id=%s  %s\n",
		time.Now().Format("2006-01-02 15:04:05.000"), dir, iface, brc, class, id, detail)
	outMu.Unlock()
}

// shortID renders the leading 8 bytes of an object identifier (internal byte
// order, matching the emitters' -log-hashes output and objsink id files).
func shortID(id []byte) string {
	n := 8
	if len(id) < n {
		n = len(id)
	}
	return hex.EncodeToString(id[:n]) + "…"
}

// ── interface resolution ────────────────────────────────────────────────────

var ifaceCache sync.Map // local IP string → interface name

// ifaceFor maps a connection's local address to the network interface that
// owns it (e.g. the WireGuard tunnel device for an overlay SDA).
func ifaceFor(addr net.Addr) string {
	tcp, ok := addr.(*net.TCPAddr)
	if !ok {
		return "?"
	}
	key := tcp.IP.String()
	if v, ok := ifaceCache.Load(key); ok {
		return v.(string)
	}
	name := "?"
	if ifs, err := net.Interfaces(); err == nil {
	scan:
		for _, ifc := range ifs {
			addrs, err := ifc.Addrs()
			if err != nil {
				continue
			}
			for _, a := range addrs {
				if ipn, ok := a.(*net.IPNet); ok && ipn.IP.Equal(tcp.IP) {
					name = ifc.Name
					break scan
				}
			}
		}
	}
	ifaceCache.Store(key, name)
	return name
}

// ── session statistics ──────────────────────────────────────────────────────

var statDirs = []string{dirRecv, dirSent}
var statBuckets = []string{"txn", "subtree", "block", "other"}

type laneTotals struct {
	objects  uint64
	bytes    uint64
	minSize  int
	maxSize  int
	nodes    uint64
	subtrees uint64
}

type stats struct {
	mu        sync.Mutex
	start     time.Time
	lanes     map[string]*laneTotals
	conns     int
	active    int
	parseErrs int
}

func newStats() *stats {
	return &stats{start: time.Now(), lanes: map[string]*laneTotals{}}
}

func (s *stats) lane(dir, bucket string) *laneTotals {
	k := dir + "/" + bucket
	lt := s.lanes[k]
	if lt == nil {
		lt = &laneTotals{minSize: -1}
		s.lanes[k] = lt
	}
	return lt
}

func (s *stats) record(dir, bucket string, size int, nodes, subtrees uint64) {
	s.mu.Lock()
	lt := s.lane(dir, bucket)
	lt.objects++
	lt.bytes += uint64(size)
	if lt.minSize < 0 || size < lt.minSize {
		lt.minSize = size
	}
	if size > lt.maxSize {
		lt.maxSize = size
	}
	lt.nodes += nodes
	lt.subtrees += subtrees
	s.mu.Unlock()
}

func (s *stats) connOpen() {
	s.mu.Lock()
	s.conns++
	s.active++
	s.mu.Unlock()
}

func (s *stats) connClose() {
	s.mu.Lock()
	s.active--
	s.mu.Unlock()
}

func (s *stats) parseError() {
	s.mu.Lock()
	s.parseErrs++
	s.mu.Unlock()
}

func (s *stats) printSummary(w io.Writer, listenAddr string, subLn net.Listener, edge string, txPort, subtreePort, blockPort int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	outMu.Lock()
	defer outMu.Unlock()

	fmt.Fprintf(w, "\n──────────────────────── tunnel-sink session summary ────────────────────────\n")
	fmt.Fprintf(w, "started       %s\n", s.start.Format("2006-01-02 15:04:05"))
	fmt.Fprintf(w, "duration      %s\n", time.Since(s.start).Round(time.Second))
	fmt.Fprintf(w, "delivery      %s   (connections: %d total, %d open at exit)\n", listenAddr, s.conns, s.active)
	if subLn != nil {
		fmt.Fprintf(w, "submit        %s → %s (tx %d / subtree %d / block %d)\n",
			subLn.Addr().String(), edge, txPort, subtreePort, blockPort)
	}
	fmt.Fprintf(w, "parse errors  %d\n\n", s.parseErrs)

	any := false
	for _, dir := range statDirs {
		for _, b := range statBuckets {
			if lt := s.lanes[dir+"/"+b]; lt != nil && lt.objects > 0 {
				any = true
			}
		}
	}
	if !any {
		fmt.Fprintf(w, "no objects observed\n")
		return
	}

	fmt.Fprintf(w, "%-4s  %-8s  %12s  %14s  %9s  %9s  %9s  %s\n",
		"dir", "class", "objects", "bytes", "avg", "min", "max", "totals")
	for _, dir := range statDirs {
		for _, b := range statBuckets {
			lt := s.lanes[dir+"/"+b]
			if lt == nil || lt.objects == 0 {
				continue
			}
			extra := ""
			switch b {
			case "subtree":
				extra = fmt.Sprintf("%s nodes", group(lt.nodes))
			case "block":
				extra = fmt.Sprintf("%s subtrees", group(lt.subtrees))
			}
			fmt.Fprintf(w, "%-4s  %-8s  %12s  %14s  %9s  %9s  %9s  %s\n",
				dir, b, group(lt.objects), group(lt.bytes),
				byteSize(int(lt.bytes/lt.objects)), byteSize(lt.minSize), byteSize(lt.maxSize), extra)
		}
	}
}

// ── small helpers ───────────────────────────────────────────────────────────

func sha256d(b []byte) [32]byte {
	h := sha256.Sum256(b)
	return sha256.Sum256(h[:])
}

func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

func byteSize(n int) string {
	switch {
	case n < 1<<10:
		return fmt.Sprintf("%d B", n)
	case n < 1<<20:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	case n < 1<<30:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	default:
		return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
	}
}

// group renders n with thousands separators.
func group(n uint64) string {
	s := strconv.FormatUint(n, 10)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	lead := len(s) % 3
	if lead > 0 {
		b.WriteString(s[:lead])
	}
	for i := lead; i < len(s); i += 3 {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "tunnel-sink: "+format+"\n", args...)
	os.Exit(1)
}
