package modelgw

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type seen struct {
	auth, apiKey, path string
}

// upstream records the credential headers it receives and streams two SSE
// events with a pause between them.
func upstream(t *testing.T, got chan<- seen) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- seen{auth: r.Header.Get("Authorization"), apiKey: r.Header.Get("X-Api-Key"), path: r.URL.Path}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: one\n\n")
		w.(http.Flusher).Flush()
		time.Sleep(300 * time.Millisecond)
		io.WriteString(w, "data: two\n\n")
	}))
	t.Cleanup(srv.Close)
	return srv
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func newGateway(t *testing.T, addr, upstream string) *Gateway {
	t.Helper()
	g, err := NewGateway(addr, upstream, nil)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// post sends one model call as a guest would. Each call gets its own client:
// every agent attempt is a separate VM with its own connections, and a
// shared client could let one guest's cut connection disturb another's.
func post(t *testing.T, addr, token string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest("POST", "http://"+addr+"/v1/messages", strings.NewReader("{}"))
	if token != "" {
		req.Header.Set("X-Api-Key", token)
	}
	guest := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := guest.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestGatewayAdmitsOnlyOpenTokensAndStripsThem(t *testing.T) {
	got := make(chan seen, 4)
	up := upstream(t, got)
	token, _ := NewToken()
	addr := freeAddr(t)
	g := newGateway(t, addr, up.URL)
	sess, err := g.Open(token, Identity{})
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	for _, bad := range []string{"", "wrong", token[:len(token)-1] + "x"} {
		resp := post(t, addr, bad)
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("token %q: status %d, want 401", bad, resp.StatusCode)
		}
	}
	select {
	case s := <-got:
		t.Fatalf("a rejected request reached upstream: %+v", s)
	default:
	}

	resp := post(t, addr, token)
	defer resp.Body.Close()
	s := <-got
	if s.apiKey != "" || s.auth != "" || s.path != "/v1/messages" {
		t.Errorf("upstream saw %+v; the run token must be stripped", s)
	}
	// The first SSE event must arrive before the upstream finishes.
	start := time.Now()
	line, _ := bufio.NewReader(resp.Body).ReadString('\n')
	if line != "data: one\n" || time.Since(start) > 200*time.Millisecond {
		t.Errorf("stream not flushed: %q after %v", line, time.Since(start))
	}
	if sess.Requests() != 1 {
		t.Errorf("served %d", sess.Requests())
	}
}

// Concurrent attempts share the port. Each is admitted by its own token,
// and closing one revokes only that token: the other keeps working, and the
// closed one is refused at once.
func TestGatewaySessionsAreIndependent(t *testing.T) {
	got := make(chan seen, 8)
	up := upstream(t, got)
	addr := freeAddr(t)
	g := newGateway(t, addr, up.URL)
	t1, _ := NewToken()
	t2, _ := NewToken()
	s1, err := g.Open(t1, Identity{})
	if err != nil {
		t.Fatal(err)
	}
	s2, err := g.Open(t2, Identity{})
	if err != nil {
		t.Fatalf("second concurrent session: %v", err)
	}
	defer s2.Close()
	for _, tok := range []string{t1, t2} {
		resp := post(t, addr, tok)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("open token: status %d", resp.StatusCode)
		}
	}
	if s1.Requests() != 1 || s2.Requests() != 1 {
		t.Errorf("per-session counts %d, %d; want 1, 1", s1.Requests(), s2.Requests())
	}
	s1.Close()
	resp := post(t, addr, t1)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("closed session's token: status %d, want 401", resp.StatusCode)
	}
	resp = post(t, addr, t2)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("other session after a close: status %d, want 200", resp.StatusCode)
	}
	if _, err := g.Open(t2, Identity{}); err == nil {
		t.Error("the same token opened twice")
	}
}

// Closing a session cuts its own streaming call, not the other's.
func TestGatewayCloseCutsOnlyItsOwnStreams(t *testing.T) {
	release := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: one\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		io.WriteString(w, "data: two\n\n")
	}))
	t.Cleanup(up.Close)
	t.Cleanup(func() { close(release) })
	addr := freeAddr(t)
	g := newGateway(t, addr, up.URL)
	t1, _ := NewToken()
	t2, _ := NewToken()
	s1, _ := g.Open(t1, Identity{})
	s2, _ := g.Open(t2, Identity{})
	defer s2.Close()
	r1, r2 := post(t, addr, t1), post(t, addr, t2)
	defer r1.Body.Close()
	defer r2.Body.Close()
	b1, b2 := bufio.NewReader(r1.Body), bufio.NewReader(r2.Body)
	// Consume the whole first event ("data: one" and its blank line), so
	// the next byte on each stream is the second event.
	for _, b := range []*bufio.Reader{b1, b2} {
		b.ReadString('\n')
		b.ReadString('\n')
	}
	s1.Close()
	ended := make(chan error, 1)
	go func() { _, err := io.ReadAll(b1); ended <- err }()
	select {
	case <-ended:
	case <-time.After(3 * time.Second):
		t.Fatal("closed session's stream still open")
	}
	// The other stream is still live: it is waiting for its next event.
	alive := make(chan struct{})
	go func() { b2.Peek(1); close(alive) }()
	select {
	case <-alive:
		t.Fatal("the other session's stream ended when one session closed")
	case <-time.After(300 * time.Millisecond):
	}
}

// The port is bound while a session is open and free once the last one
// closes, so another runner (or the next start) can bind it.
func TestGatewayHoldsThePortOnlyWhileSessionsAreOpen(t *testing.T) {
	addr := freeAddr(t)
	g := newGateway(t, addr, "http://127.0.0.1:1")
	t1, _ := NewToken()
	t2, _ := NewToken()
	s1, err := g.Open(t1, Identity{})
	if err != nil {
		t.Fatal(err)
	}
	s2, _ := g.Open(t2, Identity{})
	s1.Close()
	if c, err := net.DialTimeout("tcp", addr, time.Second); err != nil {
		t.Fatal("port released while a session is still open")
	} else {
		c.Close()
	}
	s2.Close()
	if c, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		c.Close()
		t.Error("gateway still listening after the last session closed")
	}
	if _, err := NewGateway("0.0.0.0:0", "http://127.0.0.1:1", nil); err == nil {
		t.Error("non-loopback listen accepted")
	}
	// A port someone else holds is reported, not silently shared.
	other := newGateway(t, addr, "http://127.0.0.1:1")
	held, _ := other.Open(t1, Identity{})
	defer held.Close()
	if _, err := g.Open(t2, Identity{}); err == nil {
		t.Error("two gateways bound one address")
	}
}

func TestCredentialProxyReplacesCredentials(t *testing.T) {
	got := make(chan seen, 2)
	up := upstream(t, got)
	u, _ := url.Parse(up.URL)
	tokenFile := filepath.Join(t.TempDir(), "gw.token")
	os.WriteFile(tokenFile, []byte("real-gateway-token\n"), 0o600)
	srv := httptest.NewServer(CredentialProxy{TokenFile: tokenFile, Upstream: u}.Handler())
	defer srv.Close()

	req, _ := http.NewRequest("POST", srv.URL+"/v1/messages", strings.NewReader("{}"))
	req.Header.Set("X-Api-Key", "guest-supplied")
	req.Header.Set("Authorization", "Bearer guest-supplied")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	s := <-got
	if s.auth != "Bearer real-gateway-token" || s.apiKey != "" {
		t.Errorf("upstream saw %+v", s)
	}

	os.Chmod(tokenFile, 0o644)
	resp, err = http.Post(srv.URL+"/v1/messages", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("world-readable token file: status %d, want 502", resp.StatusCode)
	}
}

// Close must release the port before it returns, even when it races the
// Serve goroutine's start: the next attempt binds the same port at once.
func TestGatewayPortIsFreeRightAfterLastClose(t *testing.T) {
	addr := freeAddr(t)
	g := newGateway(t, addr, "http://127.0.0.1:1")
	for i := range 300 {
		token, _ := NewToken()
		s, err := g.Open(token, Identity{})
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		s.Close()
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			t.Fatalf("iteration %d: port still bound: %v", i, err)
		}
		ln.Close()
	}
}

// Every forwarded call carries the identity of the session its token opened,
// through gateway-proxy, and nothing a guest sent under X-Stagehand-*
// (gitmoot/stagehand#48).
func TestGatewaySendsSessionIdentityNotTheGuests(t *testing.T) {
	got := make(chan http.Header, 4)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.Header.Clone()
	}))
	defer up.Close()
	upURL, _ := url.Parse(up.URL)
	tokenFile := filepath.Join(t.TempDir(), "gw.token")
	os.WriteFile(tokenFile, []byte("real-gateway-token\n"), 0o600)
	cp := httptest.NewServer(CredentialProxy{TokenFile: tokenFile, Upstream: upURL}.Handler())
	defer cp.Close()

	addr := freeAddr(t)
	g := newGateway(t, addr, cp.URL)
	t1, _ := NewToken()
	t2, _ := NewToken()
	s1, err := g.Open(t1, Identity{Pipeline: "dossier-research", RunID: 41})
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Close()
	s2, err := g.Open(t2, Identity{Pipeline: "canary", RunID: 7})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	send := func(token string, forged map[string][]string) http.Header {
		t.Helper()
		req, _ := http.NewRequest("POST", "http://"+addr+"/v1/messages", strings.NewReader("{}"))
		req.Header.Set("X-Api-Key", token)
		for k, v := range forged {
			req.Header[k] = v // as written, not canonicalised
		}
		resp, err := (&http.Client{Transport: &http.Transport{DisableKeepAlives: true}}).Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d", resp.StatusCode)
		}
		return <-got
	}
	stagehand := func(h http.Header) map[string][]string {
		m := map[string][]string{}
		for k, v := range h {
			if strings.HasPrefix(strings.ToLower(k), "x-stagehand-") {
				m[k] = v
			}
		}
		return m
	}
	want := func(pipeline, run string) map[string][]string {
		return map[string][]string{HeaderPipeline: {pipeline}, HeaderRun: {run}}
	}

	// The canary's guest claims to be dossier-research, in several spellings.
	h := send(t2, map[string][]string{
		"X-Stagehand-Pipeline": {"dossier-research", "dossier-research-b"},
		"x-stagehand-run":      {"41"},
		"X-STAGEHAND-EVENT-ID": {"forged"},
		"X-Stagehand-Anything": {"x"},
	})
	if got, w := stagehand(h), want("canary", "7"); !equalHeaders(got, w) {
		t.Errorf("forging guest: upstream saw %v, want %v", got, w)
	}
	if h.Get("Authorization") != "Bearer real-gateway-token" {
		t.Errorf("gateway-proxy credential missing: %q", h.Get("Authorization"))
	}
	if got, w := stagehand(send(t1, nil)), want("dossier-research", "41"); !equalHeaders(got, w) {
		t.Errorf("plain guest: upstream saw %v, want %v", got, w)
	}
}

func equalHeaders(a, b map[string][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if strings.Join(v, "\x00") != strings.Join(b[k], "\x00") {
			return false
		}
	}
	return true
}
