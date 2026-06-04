package announce

import (
	"errors"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/subtx-generator/internal/subtree"
)

func TestParseGroupIDs(t *testing.T) {
	if g, err := ParseGroupIDs(""); err != nil || g != nil {
		t.Fatalf("empty: %v %v", g, err)
	}
	hexA := strings.Repeat("11", 16)
	hexB := strings.Repeat("ab", 16)
	g, err := ParseGroupIDs(hexA + "," + hexB)
	if err != nil {
		t.Fatal(err)
	}
	if len(g) != 2 {
		t.Fatalf("len = %d", len(g))
	}
	if g[0][0] != 0x11 || g[0][15] != 0x11 {
		t.Errorf("first id = %x", g[0])
	}
	if g[1][0] != 0xab {
		t.Errorf("second id = %x", g[1])
	}
	// Empty members between commas are skipped.
	g, err = ParseGroupIDs(hexA + ",")
	if err != nil || len(g) != 1 {
		t.Fatalf("trailing comma: %v len=%d", err, len(g))
	}
}

func TestParseGroupIDs_Errors(t *testing.T) {
	cases := []string{
		"zz" + strings.Repeat("11", 15), // non-hex nibble
		strings.Repeat("11", 8),         // 8 bytes, want 16
		strings.Repeat("1", 31),         // odd length
	}
	for _, in := range cases {
		if _, err := ParseGroupIDs(in); err == nil {
			t.Errorf("ParseGroupIDs(%q) expected error", in)
		}
	}
	// The error type carries the offending token.
	_, err := ParseGroupIDs("zzzz")
	var pe *parseError
	if !errors.As(err, &pe) {
		t.Fatalf("want *parseError, got %T", err)
	}
	if !strings.Contains(pe.Error(), "zzzz") {
		t.Errorf("error message lacks token: %q", pe.Error())
	}
}

func TestHexNibble(t *testing.T) {
	for c, want := range map[byte]byte{'0': 0, '9': 9, 'a': 10, 'f': 15, 'A': 10, 'F': 15} {
		if got := hexNibble(c); got != want {
			t.Errorf("hexNibble(%q) = %d, want %d", c, got, want)
		}
	}
	if hexNibble('g') != 0xFF || hexNibble('/') != 0xFF {
		t.Error("invalid chars should map to 0xFF")
	}
}

func TestSendUpTo_EmitsAllPairs(t *testing.T) {
	pool := subtree.New(3, []byte("seed"))
	s := &Sender{
		Pool:     pool,
		GroupIDs: [][16]byte{{0x01}, {0x02}},
		TTL:      60,
	}
	const limit = 3
	want := limit * len(s.GroupIDs) * frame.SubtreeGroupAnnounceSize

	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	got := make(chan int, 1)
	errc := make(chan error, 1)
	go func() {
		buf := make([]byte, want)
		n, err := io.ReadFull(server, buf)
		got <- n
		errc <- err
		// Decode the first datagram to confirm wire validity.
		if err == nil {
			if _, derr := frame.DecodeSubtreeGroupAnnounce(buf[:frame.SubtreeGroupAnnounceSize]); derr != nil {
				errc <- derr
			}
		}
	}()

	if err := s.sendUpTo(client, limit); err != nil {
		t.Fatalf("sendUpTo: %v", err)
	}
	if n := <-got; n != want {
		t.Errorf("read %d bytes, want %d", n, want)
	}
	if err := <-errc; err != nil {
		t.Errorf("reader/decode error: %v", err)
	}
}

func TestSendUpTo_ZeroLimitNoop(t *testing.T) {
	s := &Sender{Pool: subtree.New(1, []byte("x")), GroupIDs: [][16]byte{{0x01}}}
	// nil conn must never be touched when limit <= 0.
	if err := s.sendUpTo(nil, 0); err != nil {
		t.Errorf("zero limit should be a no-op, got %v", err)
	}
}
