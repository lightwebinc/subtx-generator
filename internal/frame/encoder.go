// Package frame wraps github.com/lightwebinc/shard-common/frame to
// provide a v1/v2-aware encoder for the subtx-gen load generator.
//
// v2 encoding uses the upstream frame.Encode (always writes v2 headers).
// v1 encoding writes the legacy 44-byte header directly; shard-common no
// longer emits v1, but listeners must accept it, so we encode it here.
package frame

import (
	"encoding/binary"
	"errors"
	"fmt"

	common "github.com/lightwebinc/shard-common/frame"
)

const maxPayload = 10 * 1024 * 1024 // 10 MiB, matches protocol spec

var errTooLarge = errors.New("frame: payload exceeds maximum size")

// Version selects the wire format produced by [Encode].
type Version byte

// Version constants. Values match common.FrameVerV1 / FrameVerV2.
const (
	V1 Version = 1
	V2 Version = 2
)

// HeaderSize returns the header size in bytes for the given version.
func HeaderSize(v Version) int {
	switch v {
	case V1:
		return common.HeaderSizeLegacy
	case V2:
		return common.HeaderSize
	default:
		return 0
	}
}

// Encode serialises a frame at the requested version into buf and returns
// the number of bytes written.
func Encode(v Version, f *common.Frame, buf []byte) (int, error) {
	switch v {
	case V2:
		return common.Encode(f, buf)
	case V1:
		return encodeV1(f, buf)
	default:
		return 0, fmt.Errorf("frame: unknown version %d", v)
	}
}

func encodeV1(f *common.Frame, buf []byte) (int, error) {
	if len(f.Payload) > maxPayload {
		return 0, errTooLarge
	}
	total := common.HeaderSizeLegacy + len(f.Payload)
	if len(buf) < total {
		return 0, fmt.Errorf("frame: buffer too small (%d, need %d)", len(buf), total)
	}
	binary.BigEndian.PutUint32(buf[0:4], common.MagicBSV)
	binary.BigEndian.PutUint16(buf[4:6], common.ProtoVer)
	buf[6] = common.FrameVerV1
	buf[7] = 0
	copy(buf[8:40], f.TxID[:])
	binary.BigEndian.PutUint32(buf[40:44], uint32(len(f.Payload)))
	copy(buf[44:], f.Payload)
	return total, nil
}
