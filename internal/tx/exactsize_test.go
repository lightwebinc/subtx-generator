package tx

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/lightwebinc/shard-common/objfmt"
)

// exactSizes returns the size spread exercised per format: the format minimum,
// min+1, the historical trouble spots (256 = old-raw exact fill, 312 = old-raw
// cap, 512 = old default that PADDED, 1400 ≈ MTU-ish), plus the CompactSize
// 1→3-byte boundary band and one size beyond the 3→5-byte boundary.
func exactSizes(floor int) []int {
	return []int{
		floor, floor + 1, 256, 312, 512, 1400,
		floor + 252, floor + 253, floor + 254, floor + 255, // 1→3-byte varint gap band
		floor + 65537, floor + 65538, floor + 65539, floor + 65540, // 3→5-byte varint gap band
	}
}

// TestExactSizeAgainstObjfmt is the authority check: for every requested size
// the payload must be EXACTLY one valid transaction of that size per the real
// shard-common/objfmt walker (the TCP-lane delimiter), with zero trailing
// bytes. This is the class of bug being guarded: a builder that emits a valid
// tx shorter than the payload and pads the rest desynchronises a TCP objfmt
// stream at object 2.
func TestExactSizeAgainstObjfmt(t *testing.T) {
	var seed [32]byte
	seed[0] = 3
	b := New(seed)

	type format struct {
		name  string
		min   int
		ef    bool
		build func([]byte, int) ([]byte, error)
	}
	formats := []format{
		{"brc124", MinRawSize, false, b.Build},
		{"brc128", MinEFSize, true, b.BuildEF},
	}

	for _, f := range formats {
		for _, size := range exactSizes(f.min) {
			if size < f.min {
				continue // 256/312 etc. are always >= both minimums; guard anyway
			}
			out, err := f.build(nil, size)
			if err != nil {
				t.Fatalf("%s size=%d: build: %v", f.name, size, err)
			}
			if len(out) != size {
				t.Fatalf("%s size=%d: emitted %d bytes", f.name, size, len(out))
			}
			n, err := objfmt.TxSize(out)
			if err != nil {
				t.Fatalf("%s size=%d: objfmt.TxSize: %v", f.name, size, err)
			}
			if n != size {
				t.Fatalf("%s size=%d: objfmt.TxSize walked %d bytes — %d bytes of padding outside the tx",
					f.name, size, n, size-n)
			}
			if got := objfmt.IsEF(out); got != f.ef {
				t.Errorf("%s size=%d: IsEF=%v want %v", f.name, size, got, f.ef)
			}
			if _, err := objfmt.TxID(out); err != nil {
				t.Errorf("%s size=%d: objfmt.TxID: %v", f.name, size, err)
			}
		}
	}
}

// TestConcatenatedStreamWalks proves the TCP-lane property end to end: a
// back-to-back stream of N generated payloads (both formats interleaved,
// varied sizes) must split cleanly into exactly N objects with the real
// objfmt.Reader — the same delimiting a consumer SDA performs.
func TestConcatenatedStreamWalks(t *testing.T) {
	var seed [32]byte
	seed[0] = 11
	b := New(seed)

	sizes := []int{75, 76, 200, 256, 312, 512, 1400, 2048, 4096}
	var stream bytes.Buffer
	want := 0
	for round := 0; round < 4; round++ {
		for i, size := range sizes {
			var (
				out []byte
				err error
			)
			if (round+i)%2 == 0 {
				out, err = b.BuildEF(nil, size)
			} else {
				out, err = b.Build(nil, size)
			}
			if err != nil {
				t.Fatalf("round=%d size=%d: %v", round, size, err)
			}
			stream.Write(out)
			want++
		}
	}

	rd := objfmt.NewReader(bytes.NewReader(stream.Bytes()), objfmt.ClassTx)
	got := 0
	for {
		obj, err := rd.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("stream desync after %d objects: %v", got, err)
		}
		if _, err := objfmt.TxID(obj); err != nil {
			t.Fatalf("object %d: objfmt.TxID: %v", got, err)
		}
		got++
	}
	if got != want {
		t.Fatalf("stream split into %d objects, want %d", got, want)
	}
}
