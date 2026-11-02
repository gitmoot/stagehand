package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"

	"github.com/gitmoot/stagehand/internal/modelgw"
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
