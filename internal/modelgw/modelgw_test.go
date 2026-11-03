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

func TestRunGatewayAdmitsOnlyItsTokenAndStripsIt(t *testing.T) {
	got := make(chan seen, 4)
	up := upstream(t, got)
	token, _ := NewToken()
	addr := freeAddr(t)
	g, err := StartRunGateway(addr, up.URL, token, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	for _, bad := range []string{"", "wrong", token[:len(token)-1] + "x"} {
		req, _ := http.NewRequest("POST", "http://"+addr+"/v1/messages", strings.NewReader("{}"))
		if bad != "" {
			req.Header.Set("X-Api-Key", bad)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
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

	req, _ := http.NewRequest("POST", "http://"+addr+"/v1/messages", strings.NewReader("{}"))
	req.Header.Set("X-Api-Key", token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
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
	if g.Requests() != 1 {
		t.Errorf("served %d", g.Requests())
	}
}

func TestRunGatewayClosesWithItsStage(t *testing.T) {
	token, _ := NewToken()
	addr := freeAddr(t)
	g, err := StartRunGateway(addr, "http://127.0.0.1:1", token, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := StartRunGateway(addr, "http://127.0.0.1:1", token, nil); err == nil {
		t.Error("two run gateways on one address")
	}
	g.Close()
	if c, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		c.Close()
		t.Error("gateway still listening after Close")
	}
	if _, err := StartRunGateway("0.0.0.0:0", "http://127.0.0.1:1", token, nil); err == nil {
		t.Error("non-loopback listen accepted")
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
func TestRunGatewayPortIsFreeRightAfterClose(t *testing.T) {
	token, _ := NewToken()
	addr := freeAddr(t)
	for i := range 300 {
		g, err := StartRunGateway(addr, "http://127.0.0.1:1", token, nil)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		g.Close()
	}
}
