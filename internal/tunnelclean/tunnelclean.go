// Package tunnelclean finds and ends a stale sshd session that still holds a
// reverse-forwarded port on the far host after a network drop, so the tunnel
// can bind it again.
//
// A holder is ended only when all of these hold:
//   - it is an sshd-session listening on exactly the forward's address and port;
//   - its client connection comes from one of THIS host's addresses;
//   - this host has no live TCP connection with that client address and port.
//
// So it never touches another host's tunnel, a live tunnel from this host,
// or a listener on a different address.
package tunnelclean

import (
	"fmt"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
)

// Session is an sshd-session on the far host holding the forwarded port.
type Session struct {
	PID int
	// Server and Client are the ends of its ssh connection, as the far host
	// sees them.
	Server, Client netip.AddrPort
}

var pidRE = regexp.MustCompile(`\("sshd-session",pid=(\d+),`)

// fields splits an `ss -H` line into its two socket addresses and the
// sshd-session pids in its process list.
func fields(line string) (local, peer string, pids []int) {
	var addrs []string
	for _, f := range strings.Fields(line) {
		if strings.HasPrefix(f, "users:") {
			break
		}
		if strings.Contains(f, ":") {
			addrs = append(addrs, f)
		}
	}
	if len(addrs) >= 2 {
		local, peer = addrs[len(addrs)-2], addrs[len(addrs)-1]
	}
	for _, m := range pidRE.FindAllStringSubmatch(line, -1) {
		if n, err := strconv.Atoi(m[1]); err == nil {
			pids = append(pids, n)
		}
	}
	return local, peer, pids
}

// Holders parses the far host's `ss -Hltnp` (listeners) and
// `ss -Htnp state established '( sport = :22 )'` (ssh connections) output
// and returns the sshd-sessions listening on exactly listen, with their
// client connection. A holder whose connection is not listed is left out:
// nothing proves whose it is.
func Holders(listeners, conns string, listen netip.AddrPort) []Session {
	holding := map[int]bool{}
	for _, line := range strings.Split(listeners, "\n") {
		local, _, pids := fields(line)
		ap, err := netip.ParseAddrPort(local)
		if err != nil || ap != listen {
			continue
		}
		for _, p := range pids {
			holding[p] = true
		}
	}
	var out []Session
	seen := map[int]bool{}
	for _, line := range strings.Split(conns, "\n") {
		local, peer, pids := fields(line)
		srv, err1 := netip.ParseAddrPort(local)
		cli, err2 := netip.ParseAddrPort(peer)
		if err1 != nil || err2 != nil {
			continue
		}
		for _, p := range pids {
			if holding[p] && !seen[p] {
				seen[p] = true
				out = append(out, Session{PID: p, Server: srv, Client: cli})
			}
		}
	}
	return out
}

// Stale returns the holders that are this host's dead connections: the
// client address is one of ours, and we have no live connection from that
// address and port to the server.
func Stale(holders []Session, ours []netip.Addr, live []Connection) []Session {
	own := map[netip.Addr]bool{}
	for _, a := range ours {
		own[a.Unmap()] = true
	}
	alive := map[Connection]bool{}
	for _, c := range live {
		alive[Connection{Local: unmap(c.Local), Remote: unmap(c.Remote)}] = true
	}
	var out []Session
	for _, h := range holders {
		cli, srv := unmap(h.Client), unmap(h.Server)
		if !own[cli.Addr()] {
			continue
		}
		if alive[Connection{Local: cli, Remote: srv}] {
			continue
		}
		out = append(out, h)
	}
	return out
}

// Connection is a local TCP connection.
type Connection struct {
	Local, Remote netip.AddrPort
}

// LocalConnections parses this host's `ss -Htn state established` output.
func LocalConnections(out string) []Connection {
	var cs []Connection
	for _, line := range strings.Split(out, "\n") {
		local, peer, _ := fields(line)
		l, err1 := netip.ParseAddrPort(local)
		r, err2 := netip.ParseAddrPort(peer)
		if err1 == nil && err2 == nil {
			cs = append(cs, Connection{Local: l, Remote: r})
		}
	}
	return cs
}

func unmap(ap netip.AddrPort) netip.AddrPort {
	return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())
}

// KillCommand is the far-host command that ends pid, but only if it still
// holds listen at that moment (guarding against pid reuse).
func KillCommand(pid int, listen netip.AddrPort) string {
	return fmt.Sprintf(`ss -Hltnp 'src %s' | grep -q '("sshd-session",pid=%d,' && kill %d`, listen, pid, pid)
}

// QueryCommand is the far-host command whose output Holders parses: the
// listeners section, a separator line, then the ssh connections.
func QueryCommand(listen netip.AddrPort) string {
	return fmt.Sprintf(`ss -Hltnp 'src %s'; echo %s; ss -Htnp state established '( sport = :22 )'`, listen, Separator)
}

// Separator splits QueryCommand's two sections.
const Separator = "--stagehand-separator--"
