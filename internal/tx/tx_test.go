package tx

import (
	"encoding/binary"
	"testing"
)

func TestBuilderShape(t *testing.T) {
	var seed [32]byte
	seed[0] = 1
	b := New(seed)

	sizes := []int{64, 128, 256, 512, 1024}
	for _, size := range sizes {
		buf := make([]byte, size)
		out, err := b.Build(buf, size)
		if err != nil {
			t.Fatalf("size=%d: %v", size, err)
		}
		if len(out) != size {
			t.Fatalf("size=%d: got len %d", size, len(out))
		}
		if v := binary.LittleEndian.Uint32(out[0:4]); v != 2 {
			t.Errorf("size=%d: version got %d want 2", size, v)
		}
		if out[4] != 1 {
			t.Errorf("size=%d: vin_count got %d want 1", size, out[4])
		}
	}
}

func TestBuilderDeterministic(t *testing.T) {
	var seed [32]byte
	seed[0] = 42
	b1 := New(seed)
	b2 := New(seed)
	buf1 := make([]byte, 256)
	buf2 := make([]byte, 256)
	o1, err1 := b1.Build(buf1, 256)
	o2, err2 := b2.Build(buf2, 256)
	if err1 != nil || err2 != nil {
		t.Fatalf("Build: %v / %v", err1, err2)
	}
	if string(o1) != string(o2) {
		t.Error("same seed produced different output")
	}
}

func TestBuilderBelowMinErrors(t *testing.T) {
	var seed [32]byte
	b := New(seed)
	buf := make([]byte, 64)
	for _, size := range []int{0, 4, 10, MinRawSize - 1} {
		if _, err := b.Build(buf, size); err == nil {
			t.Errorf("Build(%d) succeeded, want error (< MinRawSize %d)", size, MinRawSize)
		}
	}
}

func TestSplitSlackCoversAllTotals(t *testing.T) {
	// Every slack in the canonical-varint boundary neighbourhoods (and a
	// broad low range) must be exactly representable — the class of bug
	// being fixed is a size that silently could not be represented.
	check := func(s int) {
		t.Helper()
		shim, bulk, err := splitSlack(s)
		if err != nil {
			t.Fatalf("splitSlack(%d): %v", s, err)
		}
		got := shim + varintSize(shim) - 1 + bulk + varintSize(bulk) - 1
		if got != s {
			t.Fatalf("splitSlack(%d) = shim %d bulk %d → cost %d", s, shim, bulk, got)
		}
		if shim > 252 && varintSize(shim) != 1 {
			t.Fatalf("splitSlack(%d): shim %d needs multi-byte varint", s, shim)
		}
	}
	for s := 0; s <= 1024; s++ {
		check(s)
	}
	for s := 65530; s <= 65550; s++ {
		check(s)
	}
}
