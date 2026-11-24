// Package modelgw carries an agent's model calls to omp's auth-gateway
// without the agent ever holding a real credential.
//
//	guest omp ──(per-attempt token)──▶ Gateway ──▶ … ──▶ CredentialProxy ──(gateway bearer)──▶ omp auth-gateway
//
// The run gateway (Gateway) runs inside the runner next to the sandbox, while
// agent stages run. It accepts only the random tokens of attempts still
// running, strips them, and forwards. CredentialProxy runs on the host that holds the omp
// auth-gateway token (`stagehand gateway-proxy`): it adds the bearer token and
// forwards to the auth-gateway. Between them is a loopback tunnel.
package modelgw

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Identity names the run an agent attempt belongs to. The run gateway sends
// it upstream with every model call of that attempt, as HeaderPipeline and
// HeaderRun, so a proxy further up can route or meter by pipeline
// (gitmoot/stagehand#48).
type Identity struct {
	Pipeline string
	RunID    int64
}

// The run gateway's identity headers. A guest cannot set them: every header
// it sends under identityPrefix is dropped before these are set.
const (
	HeaderPipeline = "X-Stagehand-Pipeline"
	HeaderRun      = "X-Stagehand-Run"
	identityPrefix = "X-Stagehand-"
)

// dropIdentity deletes every X-Stagehand-* key, in any letter case.
func dropIdentity(h http.Header) {
	for k := range h {
		if len(k) >= len(identityPrefix) && strings.EqualFold(k[:len(identityPrefix)], identityPrefix) {
			delete(h, k)
		}
	}
}

// setIdentity drops every X-Stagehand-* header and request trailer the guest
// sent (a trailer would reach upstream too), then sets the session's
// pipeline and run headers.
func setIdentity(out *http.Request, pipeline, run string) {
	dropIdentity(out.Header)
	dropIdentity(out.Trailer)
	out.Header.Set(HeaderPipeline, pipeline)
	out.Header.Set(HeaderRun, run)
}

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

// Gateway is the run gateway: one loopback listener (the sandboxd
// instance's single host port) shared by every agent attempt of this runner.
// Each attempt opens a Session with its own random token; a request is
// forwarded only while its token's session is open. The listener is bound
// while at least one session is open, and released when the last closes.
type Gateway struct {
	listen string
	up     *url.URL
	log    *slog.Logger

	life sync.Mutex // serialises binding and releasing the listener
	srv  *http.Server
	ln   net.Listener
	done chan struct{} // closed when Serve returns

	mu       sync.Mutex
	sessions map[[sha256.Size]byte]*session
}

type session struct {
	proxy  *httputil.ReverseProxy // forwards with this session's identity
	served atomic.Int64
	ctx    context.Context // cancelled when the session closes
	cancel context.CancelFunc
}

// NewGateway validates the addresses; nothing is bound until Open.
func NewGateway(listen, upstream string, log *slog.Logger) (*Gateway, error) {
	if err := checkLoopback(listen); err != nil {
		return nil, fmt.Errorf("run gateway: %w", err)
	}
	up, err := url.Parse(upstream)
	if err != nil || (up.Scheme != "http" && up.Scheme != "https") || up.Host == "" {
		return nil, fmt.Errorf("run gateway: upstream %q must be an http(s) URL", upstream)
	}
	return &Gateway{listen: listen, up: up, log: log, sessions: map[[sha256.Size]byte]*session{}}, nil
}

// Session is one attempt's admission to the gateway.
type Session struct {
	g    *Gateway
	key  [sha256.Size]byte
	s    *session
	once sync.Once
}

// Open admits token until the returned session is closed, binding the
// listener if no other session holds it. Every call it forwards carries id
// upstream. id must be complete: a non-empty Pipeline and a positive RunID;
// Open refuses anything else. Tokens are compared by their SHA-256, so the
// lookup leaks nothing about a valid token's bytes.
func (g *Gateway) Open(token string, id Identity) (*Session, error) {
	if len(token) < 32 {
		return nil, errors.New("run gateway: token too short")
	}
	if id.Pipeline == "" || id.RunID <= 0 {
		return nil, fmt.Errorf("run gateway: incomplete identity %+v", id)
	}
	key := sha256.Sum256([]byte(token))
	g.life.Lock()
	defer g.life.Unlock()
	if g.srv == nil {
		if err := g.bind(); err != nil {
			return nil, err
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	// The identity comes from the session the token opened, never from the
	// guest's request.
	run := strconv.FormatInt(id.RunID, 10)
	proxy := newProxy(g.up, func(out *http.Request) { setIdentity(out, id.Pipeline, run) })
	s := &session{proxy: proxy, ctx: ctx, cancel: cancel}
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, dup := g.sessions[key]; dup {
		cancel()
		return nil, errors.New("run gateway: token already open")
	}
	g.sessions[key] = s
	return &Session{g: g, key: key, s: s}, nil
}

// bind starts the listener; g.life is held.
func (g *Gateway) bind() error {
	ln, err := net.Listen("tcp", g.listen)
	if err != nil {
		// Reservation can't tell who holds the port: keep the competing-
		// listener hint and add the source-port one when it applies.
		hint := "is another runner using this sandboxd instance?"
		// The port net.Listen actually tried, not a fresh resolution of the
		// configured one.
		var oe *net.OpError
		if errors.Is(err, syscall.EADDRINUSE) && errors.As(err, &oe) {
			if a, ok := oe.Addr.(*net.TCPAddr); ok {
				if risk := portRisk(procIPv4, a.Port); risk != "" {
					hint += " Or " + risk
				}
			}
		}
		return fmt.Errorf("run gateway: %w (%s)", err, hint)
	}
	srv := &http.Server{
		ReadHeaderTimeout: 10 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := sha256.Sum256([]byte(presented(r)))
			g.mu.Lock()
			s := g.sessions[key]
			g.mu.Unlock()
			if s == nil {
				if g.log != nil {
					g.log.Warn("run gateway: rejected request", "path", r.URL.Path, "remote", r.RemoteAddr)
				}
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			s.served.Add(1)
			// The call ends when its attempt does, even mid-stream.
			ctx, cancel := context.WithCancel(r.Context())
			defer cancel()
			stop := context.AfterFunc(s.ctx, cancel)
			defer stop()
			s.proxy.ServeHTTP(w, r.WithContext(ctx))
		}),
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(ln)
	}()
	g.srv, g.ln, g.done = srv, ln, done
	return nil
}

// PortRisk explains why the listen port can be taken from under the
// gateway, or returns "" when it cannot (or the kernel settings can't be
// read). The gateway binds only while an agent stage runs; a port inside the
// kernel's ephemeral range and not reserved may meanwhile be handed to any
// local client as its source port, and the next bind fails with EADDRINUSE
// (gitmoot/stagehand#51).
func (g *Gateway) PortRisk() string {
	return listenRisk(procIPv4, g.listen)
}

// procIPv4 holds the kernel's port settings.
const procIPv4 = "/proc/sys/net/ipv4"

// listenRisk resolves listen the way net.Listen does (a service name
// included) and checks its port.
func listenRisk(dir, listen string) string {
	a, err := net.ResolveTCPAddr("tcp", listen)
	if err != nil {
		return ""
	}
	return portRisk(dir, a.Port)
}

func portRisk(dir string, port int) string {
	raw, err := os.ReadFile(filepath.Join(dir, "ip_local_port_range"))
	if err != nil {
		return ""
	}
	var lo, hi int
	if _, err := fmt.Sscan(string(raw), &lo, &hi); err != nil || port < lo || port > hi {
		return ""
	}
	raw, err = os.ReadFile(filepath.Join(dir, "ip_local_reserved_ports"))
	if err != nil {
		return ""
	}
	for _, item := range strings.Split(strings.TrimSpace(string(raw)), ",") {
		a, b, isRange := strings.Cut(item, "-")
		first, err1 := strconv.Atoi(a)
		last, err2 := first, error(nil)
		if isRange {
			last, err2 = strconv.Atoi(b)
		}
		if err1 == nil && err2 == nil && first <= port && port <= last {
			return ""
		}
	}
	return fmt.Sprintf("port %d is in the kernel's ephemeral range %d-%d and not reserved, so a local client may hold it as its source port; add it to net.ipv4.ip_local_reserved_ports", port, lo, hi)
}

// release stops the listener; g.life is held. The port is free when it
// returns. Closing the listener directly (instead of only Shutdown) matters:
// Shutdown does not close a listener that Serve has not registered yet,
// which would leave the port bound.
func (g *Gateway) release() error {
	lnErr := g.ln.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := g.srv.Shutdown(ctx); err != nil {
		_ = g.srv.Close()
	}
	<-g.done
	g.srv, g.ln, g.done = nil, nil, nil
	if lnErr != nil && !errors.Is(lnErr, net.ErrClosed) {
		return lnErr
	}
	return nil
}

// Requests is how many authorized requests this session forwarded.
func (s *Session) Requests() int64 { return s.s.served.Load() }

// Close revokes the token at once, cuts its in-flight requests, and
// releases the listener if this was the last open session. Other sessions
// are unaffected.
func (s *Session) Close() error {
	var err error
	s.once.Do(func() {
		g := s.g
		g.life.Lock()
		defer g.life.Unlock()
		g.mu.Lock()
		delete(g.sessions, s.key)
		last := len(g.sessions) == 0
		g.mu.Unlock()
		s.s.cancel()
		if last {
			err = g.release()
		}
	})
	return err
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
