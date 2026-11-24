package modelgw

import (
	"bufio"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
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

// testID is a complete identity for tests that do not look at it.
var testID = Identity{Pipeline: "p", RunID: 1}

// mustOpen opens a session or fails the test at setup.
func mustOpen(t *testing.T, g *Gateway, token string) *Session {
	t.Helper()
	s, err := g.Open(token, testID)
	if err != nil {
		t.Fatal(err)
	}
	return s
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
	sess, err := g.Open(token, testID)
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
	s1, err := g.Open(t1, testID)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := g.Open(t2, testID)
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
	if _, err := g.Open(t2, testID); err == nil {
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
	s1 := mustOpen(t, g, t1)
	s2 := mustOpen(t, g, t2)
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
	s1, err := g.Open(t1, testID)
	if err != nil {
		t.Fatal(err)
	}
	s2 := mustOpen(t, g, t2)
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
	held := mustOpen(t, other, t1)
	defer held.Close()
	if _, err := g.Open(t2, testID); err == nil {
		t.Error("two gateways bound one address")
	}
}

func TestCredentialProxyReplacesCredentials(t *testing.T) {
	got := make(chan seen, 2)
	up := upstream(t, got)
	u, _ := url.Parse(up.URL)
	tokenFile := filepath.Join(t.TempDir(), "gw.token")
	if err := os.WriteFile(tokenFile, []byte("real-gateway-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
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

	if err := os.Chmod(tokenFile, 0o644); err != nil {
		t.Fatal(err)
	}
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
		s, err := g.Open(token, testID)
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
	type call struct{ header, trailer http.Header }
	got := make(chan call, 4)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body) // trailers arrive after the body
		got <- call{r.Header.Clone(), r.Trailer.Clone()}
	}))
	defer up.Close()
	upURL, _ := url.Parse(up.URL)
	tokenFile := filepath.Join(t.TempDir(), "gw.token")
	if err := os.WriteFile(tokenFile, []byte("real-gateway-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
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

	send := func(token string, forged, trailer map[string][]string) call {
		t.Helper()
		req, _ := http.NewRequest("POST", "http://"+addr+"/v1/messages", strings.NewReader("{}"))
		req.Header.Set("X-Api-Key", token)
		for k, v := range forged {
			req.Header[k] = v // as written, not canonicalised
		}
		if trailer != nil {
			req.Body, req.ContentLength = io.NopCloser(strings.NewReader("{}")), -1 // chunked
			req.Trailer = trailer
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

	// The canary's guest claims to be dossier-research, in several spellings
	// and in a request trailer.
	h := send(t2, map[string][]string{
		"X-Stagehand-Pipeline": {"dossier-research", "dossier-research-b"},
		"x-stagehand-run":      {"41"},
		"X-STAGEHAND-EVENT-ID": {"forged"},
		"X-Stagehand-Anything": {"x"},
	}, map[string][]string{"X-Stagehand-Pipeline": {"dossier-research"}, "X-Stagehand-Trailer": {"x"}})
	if got, w := stagehand(h.header), want("canary", "7"); !maps.EqualFunc(got, w, slices.Equal) {
		t.Errorf("forging guest: upstream headers %v, want %v", got, w)
	}
	// A trailer name alone (declared, with no value) is already a leak.
	if got := stagehand(h.trailer); len(got) != 0 {
		t.Errorf("forging guest: upstream trailers %v, want none", got)
	}
	if h.header.Get("Authorization") != "Bearer real-gateway-token" {
		t.Errorf("gateway-proxy credential missing: %q", h.header.Get("Authorization"))
	}
	if got, w := stagehand(send(t1, nil, nil).header), want("dossier-research", "41"); !maps.EqualFunc(got, w, slices.Equal) {
		t.Errorf("plain guest: upstream headers %v, want %v", got, w)
	}
}

// A session without a complete identity would forward unlabelled calls, which
// an upstream router would treat as some default; Open refuses it instead.
func TestGatewayRefusesIncompleteIdentity(t *testing.T) {
	g := newGateway(t, freeAddr(t), "http://127.0.0.1:1")
	for _, id := range []Identity{{}, {Pipeline: "p"}, {RunID: 1}, {Pipeline: "p", RunID: -1}} {
		token, _ := NewToken()
		if s, err := g.Open(token, id); err == nil {
			s.Close()
			t.Errorf("Open accepted %+v", id)
		}
	}
}

// A gateway port the kernel may hand out as a client source port is
// reported, unless it is reserved; anything unreadable reports nothing.
func TestPortRisk(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if r := portRisk(dir, 43293); r != "" {
		t.Errorf("no kernel settings: %q", r)
	}
	write("ip_local_port_range", "32768\t60999\n")
	write("ip_local_reserved_ports", "\n")
	for _, c := range []struct {
		reserved string
		port     int
		risky    bool
	}{
		{"", 43293, true},
		{"", 8797, false},  // below the range
		{"", 61000, false}, // above it
		{"43293,43295", 43293, false},
		{"43295", 43293, true},
		{"8000,43290-43299", 43293, false},
		{"43294-43299", 43293, true},
	} {
		write("ip_local_reserved_ports", c.reserved+"\n")
		if r := portRisk(dir, c.port); (r != "") != c.risky {
			t.Errorf("reserved %q, port %d: %q, want risky=%v", c.reserved, c.port, r, c.risky)
		}
	}
}

// The port is resolved as net.Listen resolves it, service names included.
func TestListenRiskResolvesServiceNames(t *testing.T) {
	dir := t.TempDir()
	for name, body := range map[string]string{"ip_local_port_range": "1\t65535\n", "ip_local_reserved_ports": "\n"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if listenRisk(dir, "127.0.0.1:http") == "" {
		t.Error("127.0.0.1:http (port 80, unreserved, in range) not reported")
	}
}

// A competing listener on an unreserved port gets both explanations: who
// holds the port is unknown, and reserving it would not move a listener.
func TestBindErrorKeepsBothHints(t *testing.T) {
	held, err := net.Listen("tcp", "127.0.0.1:0") // a kernel-chosen, hence ephemeral and unreserved, port
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	g := newGateway(t, held.Addr().String(), "http://127.0.0.1:1")
	if g.PortRisk() == "" {
		t.Skip("kernel port settings unreadable here")
	}
	token, _ := NewToken()
	_, err = g.Open(token, testID)
	if err == nil {
		t.Fatal("bound a held port")
	}
	if msg := err.Error(); !strings.Contains(msg, "another runner") || !strings.Contains(msg, "ip_local_reserved_ports") {
		t.Errorf("error %q: want both the competing-listener and the reservation hint", msg)
	}
}
