package sender

import (
	"net"
	"testing"
)

func TestModeIotaValues(t *testing.T) {
	t.Parallel()
	// Iota ordering is part of the public API (Config.Mode is serialised
	// in operator-facing logs and metrics).
	if ModeUnicast != 0 {
		t.Errorf("ModeUnicast = %d, want 0", ModeUnicast)
	}
	if ModeDirectMulticast != 1 {
		t.Errorf("ModeDirectMulticast = %d, want 1", ModeDirectMulticast)
	}
}

func TestOpenMulticastEgress_BindSource(t *testing.T) {
	t.Parallel()
	// Binding to ::1 must succeed on every Linux host; lo is always
	// present and the loopback address is always assigned to it.
	uc, err := openMulticastEgress(nil, net.ParseIP("::1"))
	if err != nil {
		t.Fatalf("openMulticastEgress(nil, ::1): %v", err)
	}
	defer func() { _ = uc.Close() }()
	la, ok := uc.LocalAddr().(*net.UDPAddr)
	if !ok {
		t.Fatalf("LocalAddr type = %T, want *net.UDPAddr", uc.LocalAddr())
	}
	if !la.IP.Equal(net.ParseIP("::1")) {
		t.Errorf("bound IP = %s, want ::1", la.IP)
	}
}

func TestOpenMulticastEgress_NoBindSource(t *testing.T) {
	t.Parallel()
	uc, err := openMulticastEgress(nil, nil)
	if err != nil {
		t.Fatalf("openMulticastEgress(nil, nil): %v", err)
	}
	defer func() { _ = uc.Close() }()
}
