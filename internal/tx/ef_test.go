package tx

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestBuildEFHasMarker(t *testing.T) {
	var seed [32]byte
	seed[0] = 7
	b := New(seed)
	sizes := []int{75, 128, 256, 512, 1024}
	for _, size := range sizes {
		buf := make([]byte, size)
		out, err := b.BuildEF(buf, size)
		if err != nil {
			t.Fatalf("size=%d: %v", size, err)
		}
		if len(out) != size {
			t.Fatalf("size=%d: got len %d", size, len(out))
		}
		if v := binary.LittleEndian.Uint32(out[0:4]); v != 2 {
			t.Errorf("size=%d: version got %d want 2", size, v)
		}
		if !bytes.Equal(out[4:10], []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0xEF}) {
			t.Errorf("size=%d: EF marker missing at offset 4..10: %x", size, out[4:10])
		}
		if out[10] != 1 {
			t.Errorf("size=%d: vin_count got %d want 1", size, out[10])
		}
	}
}

func TestBuildEFBelowMinErrors(t *testing.T) {
	var seed [32]byte
	b := New(seed)
	buf := make([]byte, 200)
	for _, size := range []int{0, 10, MinEFSize - 1} {
		if _, err := b.BuildEF(buf, size); err == nil {
			t.Errorf("BuildEF(%d) succeeded, want error (< MinEFSize %d)", size, MinEFSize)
		}
	}
}

func TestBuildEFDeterministic(t *testing.T) {
	var seed [32]byte
	seed[0] = 99
	b1 := New(seed)
	b2 := New(seed)
	buf1 := make([]byte, 256)
	buf2 := make([]byte, 256)
	o1, err1 := b1.BuildEF(buf1, 256)
	o2, err2 := b2.BuildEF(buf2, 256)
	if err1 != nil || err2 != nil {
		t.Fatalf("BuildEF: %v / %v", err1, err2)
	}
	if !bytes.Equal(o1, o2) {
		t.Error("same seed produced different EF output")
	}
}
