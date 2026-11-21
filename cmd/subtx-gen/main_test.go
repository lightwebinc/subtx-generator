package main

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	common "github.com/lightwebinc/shard-common/frame"
)

// runAsMain makes the test binary run main() on its arguments instead of
// the tests, so a test can drive the real subtx-gen flag set end to end.
const runAsMain = "SUBTX_GEN_RUN_AS_MAIN"

func TestMain(m *testing.M) {
	if os.Getenv(runAsMain) == "1" {
		os.Args = append([]string{"subtx-gen"}, os.Args[1:]...)
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// runGenerator runs subtx-gen with args and returns its combined output.
func runGenerator(t *testing.T, args ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], args...)
	cmd.Env = append(os.Environ(), runAsMain+"=1")
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// udpSink collects decoded frames from a loopback UDP socket.
type udpSink struct {
	addr   string
	mu     sync.Mutex
	frames []*common.Frame
}

func newUDPSink(t *testing.T) *udpSink {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenPacket: %v", err)
	}
	_ = pc.(*net.UDPConn).SetReadBuffer(4 << 20)
	t.Cleanup(func() { _ = pc.Close() })
	s := &udpSink{addr: pc.LocalAddr().String()}
	go func() {
		buf := make([]byte, 64*1024)
		for {
			n, _, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			f, err := common.Decode(append([]byte(nil), buf[:n]...))
			if err != nil {
				continue
			}
			s.mu.Lock()
			s.frames = append(s.frames, f)
			s.mu.Unlock()
		}
	}()
	return s
}

func (s *udpSink) wait(n int, timeout time.Duration) []*common.Frame {
	deadline := time.Now().Add(timeout)
	for {
		s.mu.Lock()
		got := len(s.frames)
		s.mu.Unlock()
		if got >= n || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*common.Frame(nil), s.frames...)
}

var doneLine = regexp.MustCompile(`done: sent=(\d+)(?: late=(\d+))?`)

// The generator's output stream with -seq-gap-delay must carry every
// skipped sequence number, in a late frame of its own (group, subtree) flow,
// so no flow keeps a hole; the done line counts those fills.
func TestSeqGapDelaySendsLateFramesOnTheWire(t *testing.T) {
	const (
		count     = 300
		every     = 4
		size      = 2
		shardBits = 2
	)
	sink := newUDPSink(t)
	out, err := runGenerator(t,
		"-addr", sink.addr, "-count", strconv.Itoa(count), "-pps", "1000", "-workers", "1",
		"-shard-bits", strconv.Itoa(shardBits), "-subtrees", "4", "-payload-size", "200",
		"-seq-gap-every", strconv.Itoa(every), "-seq-gap-size", strconv.Itoa(size),
		"-seq-gap-delay", "100ms", "-log-interval", "1h")
	if err != nil {
		t.Fatalf("subtx-gen: %v\n%s", err, out)
	}
	m := doneLine.FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("no done line in output:\n%s", out)
	}
	t.Log(m[0])
	sent, _ := strconv.Atoi(m[1])
	late, _ := strconv.Atoi(m[2]) // empty (0) when the line has no late= field
	frames := sink.wait(sent, 5*time.Second)
	if len(frames) != sent {
		t.Fatalf("sink got %d frames, generator reports %d sent", len(frames), sent)
	}

	type flow struct {
		group   uint32
		subtree [32]byte
	}
	seqs := map[flow]map[uint64]bool{}
	for _, f := range frames {
		k := flow{binary.BigEndian.Uint32(f.TxID[:4]) >> (32 - shardBits), f.SubtreeID}
		if seqs[k] == nil {
			seqs[k] = map[uint64]bool{}
		}
		if seqs[k][f.SeqNum] {
			t.Fatalf("flow %d/%x: seq %d sent twice", k.group, k.subtree[:4], f.SeqNum)
		}
		seqs[k][f.SeqNum] = true
	}
	skipped := 0
	for k, s := range seqs {
		var highest uint64
		for n := range s {
			highest = max(highest, n)
		}
		for n := uint64(1); n <= highest; n++ {
			if !s[n] {
				t.Errorf("flow %d/%x: seq %d never sent (highest %d): its gap was never filled",
					k.group, k.subtree[:4], n, highest)
			}
			// One worker numbers each flow deterministically: every+size
			// numbers per cycle, the last size of them skipped, then filled.
			if (n-1)%(every+size) >= every && s[n] {
				skipped++
			}
		}
	}
	if skipped == 0 {
		t.Fatal("no skipped number was filled")
	}
	if late != skipped || sent != count+late {
		t.Errorf("done line: sent=%d late=%d; want late = %d fills and sent = %d + late", sent, late, skipped, count)
	}
}

// Gap injection on a v1 (BRC-12) frame, which has no SeqNum, must stop the
// generator with an error instead of sending a stream with no gaps in it.
func TestSeqGapOnFrameV1IsRefused(t *testing.T) {
	sink := newUDPSink(t)
	out, err := runGenerator(t, "-addr", sink.addr, "-frame-version", "1", "-payload-format", "brc124",
		"-count", "10", "-seq-gap-every", "5")
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("subtx-gen -frame-version 1 -seq-gap-every 5: err %v, want exit status 1\n%s", err, out)
	}
	if !strings.Contains(out, "-frame-version 2") {
		t.Errorf("output does not say gap injection needs -frame-version 2:\n%s", out)
	}
	if got := sink.wait(1, 300*time.Millisecond); len(got) != 0 {
		t.Errorf("sent %d frames before refusing", len(got))
	}
}
