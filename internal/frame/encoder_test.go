package frame

import (
	"bytes"
	"testing"

	common "github.com/lightwebinc/shard-common/frame"
)

func TestEncodeV2Roundtrip(t *testing.T) {
	f := &common.Frame{Payload: []byte("hello-payload")}
	for i := 0; i < 32; i++ {
		f.TxID[i] = byte(i)
	}
	f.SeqNum = 42
	buf := make([]byte, HeaderSize(V2)+len(f.Payload))
	n, err := Encode(V2, f, buf)
	if err != nil {
		t.Fatal(err)
	}
	got, err := common.Decode(buf[:n])
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != common.FrameVerV2 {
		t.Errorf("version: got %d want brc122", got.Version)
	}
	if got.SeqNum != 42 {
		t.Errorf("SeqNum mismatch: got %d want 42", got.SeqNum)
	}
	if !bytes.Equal(got.Payload, f.Payload) {
		t.Errorf("payload mismatch")
	}
}

func TestEncodeV1Roundtrip(t *testing.T) {
	f := &common.Frame{Payload: []byte("legacy")}
	for i := 0; i < 32; i++ {
		f.TxID[i] = byte(0xAA)
	}
	buf := make([]byte, HeaderSize(V1)+len(f.Payload))
	n, err := Encode(V1, f, buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 44+len(f.Payload) {
		t.Errorf("v1 size wrong: %d", n)
	}
	got, err := common.Decode(buf[:n])
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != common.FrameVerV1 {
		t.Errorf("version: got %d want v1", got.Version)
	}
	if got.SeqNum != 0 || got.HashKey != 0 || got.SubtreeID != [32]byte{} {
		t.Errorf("v1 should zero v2-only fields")
	}
	if !bytes.Equal(got.Payload, f.Payload) {
		t.Errorf("payload mismatch")
	}
}
