package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"net/url"
	"os"
	"os/exec"
	"strings"

	"github.com/gitmoot/stagehand/internal/modelgw"
	"github.com/gitmoot/stagehand/internal/tunnelclean"
)

// gatewayProxy runs `stagehand gateway-proxy`: the credential side of the
// model path, on the host that holds the omp auth-gateway token.
func gatewayProxy(ctx context.Context, args []string, stderr io.Writer) error {
	fs := flag.NewFlagSet("gateway-proxy", flag.ContinueOnError)
	listen := fs.String("listen", "127.0.0.1:43295", "loopback address to serve")
	upstream := fs.String("upstream", "http://127.0.0.1:4011", "omp auth-gateway URL")
	tokenFile := fs.String("token-file", "", "omp auth-gateway token file (0600); default ~/.omp/auth-gateway.token")
	usage := "gateway-proxy [--listen 127.0.0.1:43295] [--upstream http://127.0.0.1:4011] [--token-file FILE]"
	if _, err := parse(fs, args, 0, usage); err != nil {
		return err
	}
	if *tokenFile == "" {
		h, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		*tokenFile = h + "/.omp/auth-gateway.token"
	}
	up, err := url.Parse(*upstream)
	if err != nil || (up.Scheme != "http" && up.Scheme != "https") || up.Host == "" {
		return fmt.Errorf("--upstream %q must be an http(s) URL", *upstream)
	}
	log := slog.New(slog.NewTextHandler(stderr, nil))
	return modelgw.ServeCredentialProxy(ctx, *listen, modelgw.CredentialProxy{TokenFile: *tokenFile, Upstream: up, Log: log})
}

// readSecretFile reads a private (0600) file holding one secret.
func readSecretFile(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if info.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("%s is accessible by group or others; chmod 600 it", path)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	s := strings.TrimSpace(string(raw))
	if s == "" {
		return "", errors.New(path + " is empty")
	}
	return s, nil
}

// tunnelClearStale runs `stagehand tunnel-clear-stale`: before a reverse
// tunnel reconnects, end this host's own dead sshd session that still holds
// the forwarded port on the far host (see package tunnelclean).
func tunnelClearStale(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("tunnel-clear-stale", flag.ContinueOnError)
	host := fs.String("ssh-host", "", "far host, as given to ssh (e.g. root@hetzner2)")
	listen := fs.String("remote-listen", "", "the forward's exact address on the far host, e.g. 127.0.0.1:43293")
	usage := "tunnel-clear-stale --ssh-host HOST --remote-listen ADDR:PORT"
	if _, err := parse(fs, args, 0, usage); err != nil {
		return err
	}
	ap, err := netip.ParseAddrPort(*listen)
	if err != nil || *host == "" {
		return fmt.Errorf("usage: stagehand %s", usage)
	}
	sshArgs := []string{"-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "UpdateHostKeys=no",
		"-o", "IdentityAgent=none", "-o", "ControlMaster=no", "-o", "ControlPath=none", "-o", "ConnectTimeout=10", *host}
	remote := func(cmd string) (string, error) {
		out, err := exec.CommandContext(ctx, "ssh", append(sshArgs, cmd)...).Output()
		return string(out), err
	}
	out, err := remote(tunnelclean.QueryCommand(ap))
	if err != nil {
		return fmt.Errorf("query %s: %w", *host, err)
	}
	listeners, conns, _ := strings.Cut(out, tunnelclean.Separator)
	holders := tunnelclean.Holders(listeners, conns, ap)
	if len(holders) == 0 {
		fmt.Fprintf(stdout, "%s on %s: no sshd session holds it\n", ap, *host)
		return nil
	}
	local, err := exec.CommandContext(ctx, "ss", "-Htn", "state", "established").Output()
	if err != nil {
		return fmt.Errorf("local sockets: %w", err)
	}
	var ours []netip.Addr
	ifaddrs, err := net.InterfaceAddrs()
	if err != nil {
		return err
	}
	for _, a := range ifaddrs {
		if p, err := netip.ParsePrefix(a.String()); err == nil {
			ours = append(ours, p.Addr())
		}
	}
	stale := tunnelclean.Stale(holders, ours, tunnelclean.LocalConnections(string(local)))
	for _, h := range holders {
		isStale := false
		for _, s := range stale {
			isStale = isStale || s.PID == h.PID
		}
		if !isStale {
			fmt.Fprintf(stdout, "%s on %s: held by live or foreign session pid %d (client %s); left alone\n", ap, *host, h.PID, h.Client)
			continue
		}
		if _, err := remote(tunnelclean.KillCommand(h.PID, ap)); err != nil {
			fmt.Fprintf(stderr, "end stale session pid %d: %v\n", h.PID, err)
			continue
		}
		fmt.Fprintf(stdout, "%s on %s: ended stale session pid %d (client %s, gone here)\n", ap, *host, h.PID, h.Client)
	}
	return nil
}
