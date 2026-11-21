package main

import (
	"strings"
	"testing"
	"time"

	myframe "github.com/lightwebinc/subtx-generator/internal/frame"
)

func TestCheckGapFlags(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fv      myframe.Version
		every   uint64
		size    uint64
		delay   time.Duration
		errPart string // empty = accepted
	}{
		{"no gaps", myframe.V2, 0, 1, 0, ""},
		{"no gaps v1", myframe.V1, 0, 1, 0, ""},
		{"permanent", myframe.V2, 50, 3, 0, ""},
		{"delayed", myframe.V2, 50, 3, 500 * time.Millisecond, ""},
		{"gaps on v1", myframe.V1, 50, 3, 0, "-frame-version 2"},
		{"delayed gaps on v1", myframe.V1, 50, 3, time.Second, "-frame-version 2"},
		{"size zero", myframe.V2, 50, 0, 0, "-seq-gap-size"},
		{"delay without gaps", myframe.V2, 0, 1, time.Second, "-seq-gap-every"},
		{"negative delay", myframe.V2, 50, 1, -time.Second, "negative"},
	} {
		err := checkGapFlags(tc.fv, tc.every, tc.size, tc.delay)
		switch {
		case tc.errPart == "" && err != nil:
			t.Errorf("%s: %v, want accepted", tc.name, err)
		case tc.errPart != "" && err == nil:
			t.Errorf("%s: accepted, want an error containing %q", tc.name, tc.errPart)
		case tc.errPart != "" && !strings.Contains(err.Error(), tc.errPart):
			t.Errorf("%s: error %q, want it to contain %q", tc.name, err, tc.errPart)
		}
	}
}
