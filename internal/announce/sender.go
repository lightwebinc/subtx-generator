// Package announce implements a periodic BRC-127 SubtreeAnnounce sender for
// bitcoin-subtx-generator. It connects to a proxy TCP ingress address and
// transmits one 64-byte SubtreeAnnounce datagram per (SubtreeID, GroupID) pair
// at the configured interval.
//
// The proxy detects the MsgTypeSubtreeAnnounce byte (0x30) at offset 6 and
// forwards the datagram to the CtrlGroupSubtreeAnnounce multicast group
// instead of treating it as a BRC-124 data frame.
package announce

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/lightwebinc/bitcoin-shard-common/frame"
	"github.com/lightwebinc/bitcoin-subtx-generator/internal/subtree"
)

// Sender periodically transmits SubtreeAnnounce datagrams for all (SubtreeID,
// GroupID) pairs over a TCP connection to the proxy.
type Sender struct {
	// ProxyAddr is the TCP address of the proxy's TCP ingress port.
	ProxyAddr string

	// GroupIDs is the list of 128-bit group identifiers to announce for every
	// subtree in the pool.
	GroupIDs [][16]byte

	// Pool is the subtree ID pool; all IDs in the pool are announced.
	Pool *subtree.Pool

	// Interval is the re-announce period. Recommended 10–30 seconds.
	Interval time.Duration

	// TTL is placed in the wire format. 0 = use listener default.
	TTL uint16
}

// Run connects to ProxyAddr and periodically sends SubtreeAnnounce datagrams
// for all (SubtreeID, GroupID) pairs. Blocks until ctx is cancelled.
func (s *Sender) Run(ctx context.Context) error {
	if s.Interval <= 0 {
		s.Interval = 10 * time.Second
	}
	conn, err := net.DialTimeout("tcp", s.ProxyAddr, 5*time.Second)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	// Close connection when context is done.
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	if err := s.sendAll(conn); err != nil {
		return err
	}

	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.sendAll(conn); err != nil {
				return err
			}
		}
	}
}

func (s *Sender) sendAll(conn net.Conn) error {
	epoch := uint32(time.Now().Unix())
	buf := make([]byte, frame.SubtreeAnnounceSize)
	for i := 0; i < s.Pool.Len(); i++ {
		sid := s.Pool.At(i)
		for _, gid := range s.GroupIDs {
			ann := &frame.SubtreeAnnounce{
				SubtreeID: sid,
				GroupID:   gid,
				Epoch:     epoch,
				TTL:       s.TTL,
			}
			if _, err := frame.EncodeSubtreeAnnounce(ann, buf); err != nil {
				return err
			}
			if _, err := conn.Write(buf); err != nil {
				return err
			}
		}
	}
	log.Printf("announce: sent %d datagrams (%d subtrees × %d groups)",
		s.Pool.Len()*len(s.GroupIDs), s.Pool.Len(), len(s.GroupIDs))
	return nil
}

// ParseGroupIDs parses a comma-separated list of 32-char hex group IDs into
// [][16]byte. Returns an error if any value is malformed.
func ParseGroupIDs(s string) ([][16]byte, error) {
	if s == "" {
		return nil, nil
	}
	var out [][16]byte
	for _, part := range splitComma(s) {
		if part == "" {
			continue
		}
		b, err := hexDecode(part)
		if err != nil || len(b) != 16 {
			return nil, &parseError{part}
		}
		var id [16]byte
		copy(id[:], b)
		out = append(out, id)
	}
	return out, nil
}

type parseError struct{ s string }

func (e *parseError) Error() string {
	return "announce: invalid 32-char hex group ID: " + e.s
}

func hexDecode(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		return nil, &parseError{s}
	}
	b := make([]byte, len(s)/2)
	for i := range b {
		hi := hexNibble(s[2*i])
		lo := hexNibble(s[2*i+1])
		if hi == 0xFF || lo == 0xFF {
			return nil, &parseError{s}
		}
		b[i] = hi<<4 | lo
	}
	return b, nil
}

func hexNibble(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10
	}
	return 0xFF
}

func splitComma(s string) []string {
	var out []string
	start := 0
	for i, c := range s {
		if c == ',' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	out = append(out, s[start:])
	return out
}
