// Package modelgw carries an agent's model calls to omp's auth-gateway
// without the agent ever holding a real credential.
//
//	guest omp ──(per-run token)──▶ RunGateway ──▶ … ──▶ CredentialProxy ──(gateway bearer)──▶ omp auth-gateway
//
// RunGateway runs inside `stagehand serve` next to the sandbox, only while an
// agent stage runs. It accepts nothing but that run's random token, strips
// it, and forwards. CredentialProxy runs on the host that holds the omp
// auth-gateway token (`stagehand gateway-proxy`): it adds the bearer token and
// forwards to the auth-gateway. Between them is a loopback tunnel.
package modelgw

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// credentialHeaders are stripped from every request before it is forwarded.
var credentialHeaders = []string{"Authorization", "X-Api-Key", "Proxy-Authorization", "Cookie"}

// NewToken returns a random 256-bit token.
func NewToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "sh-run-" + hex.EncodeToString(b), nil
}

// presented returns the credential a client sent: x-api-key, or a Bearer
// token.
func presented(r *http.Request) string {
	if k := r.Header.Get("X-Api-Key"); k != "" {
		return k
	}
	if a, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); ok {
		return a
	}
	return ""
}

func equal(a, b string) bool {
	return len(a) == len(b) && subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func newProxy(upstream *url.URL, prepare func(*http.Request)) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(upstream)
			pr.Out.Host = upstream.Host
			for _, h := range credentialHeaders {
				pr.Out.Header.Del(h)
			}
			pr.Out.Header.Del("X-Forwarded-For")
			if prepare != nil {
				prepare(pr.Out)
			}
		},
		// Model responses stream (SSE); flush every write.
		FlushInterval: -1,
	}
}

func checkLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("%s is not a loopback address", addr)
	}
	return nil
}

// RunGateway is a listener that admits exactly one token.
type RunGateway struct {
	token  string
	srv    *http.Server
	ln     net.Listener
	served atomic.Int64
}

// StartRunGateway listens on listen (a loopback address the sandbox can
// reach through its host port) and forwards requests carrying token to
// upstream. Close it when the stage ends.
func StartRunGateway(listen, upstream, token string, log *slog.Logger) (*RunGateway, error) {
	if err := checkLoopback(listen); err != nil {
		return nil, fmt.Errorf("run gateway: %w", err)
	}
	up, err := url.Parse(upstream)
	if err != nil || (up.Scheme != "http" && up.Scheme != "https") || up.Host == "" {
		return nil, fmt.Errorf("run gateway: upstream %q must be an http(s) URL", upstream)
	}
	if len(token) < 32 {
		return nil, errors.New("run gateway: token too short")
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, fmt.Errorf("run gateway: %w (is another agent stage running?)", err)
	}
	g := &RunGateway{token: token, ln: ln}
	proxy := newProxy(up, nil)
	g.srv = &http.Server{
		ReadHeaderTimeout: 10 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !equal(presented(r), g.token) {
				if log != nil {
					log.Warn("run gateway: rejected request", "path", r.URL.Path, "remote", r.RemoteAddr)
				}
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			g.served.Add(1)
			proxy.ServeHTTP(w, r)
		}),
	}
	go func() { _ = g.srv.Serve(ln) }()
	return g, nil
}

// Requests is how many authorized requests were forwarded.
func (g *RunGateway) Requests() int64 { return g.served.Load() }

// Close stops the listener and cuts open streams.
func (g *RunGateway) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := g.srv.Shutdown(ctx); err != nil {
		return g.srv.Close()
	}
	return nil
}

// CredentialProxy adds the auth-gateway bearer token to every request.
type CredentialProxy struct {
	// TokenFile holds the omp auth-gateway token. It is re-read on every
	// request so a rotation needs no restart.
	TokenFile string
	Upstream  *url.URL
	Log       *slog.Logger
}

// tokenKey carries the bearer token from the handler to the proxy's Rewrite.
type tokenKey struct{}

// Handler returns the proxy's HTTP handler.
func (c CredentialProxy) Handler() http.Handler {
	// Rewrite strips every inbound credential, then sets the gateway bearer.
	proxy := newProxy(c.Upstream, func(out *http.Request) {
		out.Header.Set("Authorization", "Bearer "+out.Context().Value(tokenKey{}).(string))
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, err := readToken(c.TokenFile)
		if err != nil {
			if c.Log != nil {
				c.Log.Error("gateway-proxy: token", "err", err)
			}
			http.Error(w, "gateway credential unavailable", http.StatusBadGateway)
			return
		}
		if c.Log != nil {
			c.Log.Info("gateway-proxy", "method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr)
		}
		proxy.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), tokenKey{}, token)))
	})
}

func readToken(path string) (string, error) {
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
	token := strings.TrimSpace(string(raw))
	if token == "" {
		return "", fmt.Errorf("%s is empty", path)
	}
	return token, nil
}

// ServeCredentialProxy runs the proxy on a loopback address until ctx ends.
func ServeCredentialProxy(ctx context.Context, listen string, c CredentialProxy) error {
	if err := checkLoopback(listen); err != nil {
		return fmt.Errorf("gateway-proxy: %w", err)
	}
	if _, err := readToken(c.TokenFile); err != nil {
		return fmt.Errorf("gateway-proxy: %w", err)
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: c.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	if c.Log != nil {
		c.Log.Info("gateway-proxy listening", "addr", ln.Addr().String(), "upstream", c.Upstream.String())
	}
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
