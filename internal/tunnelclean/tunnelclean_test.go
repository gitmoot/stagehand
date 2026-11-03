package tunnelclean

import (
	"net/netip"
	"testing"
)

// Real `ss` lines from hetzner2 (3 Nov), plus decoys.
const listeners = `LISTEN 0      128      127.0.0.1:43293      0.0.0.0:*    users:(("sshd-session",pid=2890732,fd=12))
LISTEN 0      128      127.0.0.2:43293      0.0.0.0:*    users:(("sshd-session",pid=777,fd=12))
LISTEN 0      128      127.0.0.1:43291      0.0.0.0:*    users:(("sshd-session",pid=2886811,fd=12))
LISTEN 0      4096     127.0.0.1:43292      0.0.0.0:*    users:(("sandboxd",pid=2840640,fd=12))`

const conns = `0      0      100.73.209.5:22 100.106.218.88:46184 users:(("sshd-session",pid=2890732,fd=9),("sshd-session",pid=2890686,fd=9))
0      0      100.73.209.5:22 100.106.218.88:50000 users:(("sshd-session",pid=777,fd=9))
0      0      100.73.209.5:22 100.106.218.88:40000 users:(("sshd-session",pid=2886811,fd=9))`

var (
	listen = netip.MustParseAddrPort("127.0.0.1:43293")
	us     = []netip.Addr{netip.MustParseAddr("100.106.218.88"), netip.MustParseAddr("127.0.0.1")}
)

func TestHoldersMatchExactAddressOnly(t *testing.T) {
	h := Holders(listeners, conns, listen)
	if len(h) != 1 || h[0].PID != 2890732 || h[0].Client.String() != "100.106.218.88:46184" {
		t.Fatalf("holders %+v; want only pid 2890732 (127.0.0.2 and other ports excluded)", h)
	}
}

func TestLiveConnectionFromThisHostIsNotStale(t *testing.T) {
	live := LocalConnections(`0 0 100.106.218.88:46184 100.73.209.5:22 users:(("ssh",pid=1,fd=3))`)
	if s := Stale(Holders(listeners, conns, listen), us, live); len(s) != 0 {
		t.Fatalf("killed a live tunnel: %+v", s)
	}
}

func TestDeadConnectionFromThisHostIsStale(t *testing.T) {
	live := LocalConnections(`0 0 100.106.218.88:50066 100.73.209.5:22 users:(("ssh",pid=1,fd=3))`)
	s := Stale(Holders(listeners, conns, listen), us, live)
	if len(s) != 1 || s[0].PID != 2890732 {
		t.Fatalf("stale %+v; want pid 2890732", s)
	}
}

func TestOtherHostsSessionIsNeverStale(t *testing.T) {
	other := []netip.Addr{netip.MustParseAddr("100.64.0.9")}
	if s := Stale(Holders(listeners, conns, listen), other, nil); len(s) != 0 {
		t.Fatalf("killed another host's session: %+v", s)
	}
}

func TestHolderWithoutVisibleConnectionIsLeftAlone(t *testing.T) {
	h := Holders(listeners, "", listen)
	if len(h) != 0 {
		t.Fatalf("holders %+v; a holder whose client cannot be identified must not be killed", h)
	}
}
