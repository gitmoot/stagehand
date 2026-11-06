package trigger

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/gitmoot/stagehand/internal/engine"
	"github.com/gitmoot/stagehand/internal/spec"
	"github.com/gitmoot/stagehand/internal/stage"
	"github.com/gitmoot/stagehand/internal/store"
)

var secret = []byte("test-secret-0123456789abcdef")

func newServer(t *testing.T) (*httptest.Server, *engine.Engine) {
	t.Helper()
	home := t.TempDir()
	st, err := store.Open(context.Background(), filepath.Join(home, "stagehand.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	eng := engine.New(st, home, map[spec.Kind]stage.Executor{}, nil, nil)
	if _, err := eng.AddPipeline(context.Background(), []byte("name: ev\nevents: {max_queued: 2}\nstages:\n  - {id: a, cmd: x}\n"), "/", false, true); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	Handler{Engine: eng, Secret: func() ([]byte, error) { return secret, nil }, MaxBytes: 1024}.Routes(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, eng
}

type call struct {
	pipeline, eventID string
	body              []byte
	ts                int64
	sig               string // "" = sign correctly
	noSig, noTS       bool
}

func do(t *testing.T, srv *httptest.Server, c call) (int, Response) {
	t.Helper()
	if c.pipeline == "" {
		c.pipeline = "ev"
	}
	if c.ts == 0 {
		c.ts = time.Now().Unix()
	}
	req, _ := http.NewRequest("POST", srv.URL+"/v1/pipelines/"+c.pipeline+"/events", bytes.NewReader(c.body))
	if !c.noTS {
		req.Header.Set(HeaderTimestamp, strconv.FormatInt(c.ts, 10))
	}
	if c.eventID != "" {
		req.Header.Set(HeaderEventID, c.eventID)
	}
	if !c.noSig {
		sig := c.sig
		if sig == "" {
			sig = Sign(secret, c.ts, c.pipeline, c.eventID, c.body)
		}
		req.Header.Set(HeaderSignature, sig)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var r Response
	json.Unmarshal(raw, &r)
	return resp.StatusCode, r
}

func TestUnauthenticatedCallsAreRefusedAndStoreNothing(t *testing.T) {
	srv, eng := newServer(t)
	body := []byte(`{"from":"cmo@example.com"}`)
	cases := map[string]call{
		"no signature":         {body: body, noSig: true},
		"no timestamp":         {body: body, noTS: true},
		"wrong secret":         {body: body, sig: Sign([]byte("other"), time.Now().Unix(), "ev", "", body)},
		"signed other body":    {body: body, sig: Sign(secret, time.Now().Unix(), "ev", "", []byte("x"))},
		"signed other pipe":    {body: body, sig: Sign(secret, time.Now().Unix(), "other", "", body)},
		"stale timestamp":      {body: body, ts: time.Now().Add(-10 * time.Minute).Unix()},
		"future timestamp":     {body: body, ts: time.Now().Add(10 * time.Minute).Unix()},
		"signed other eventid": {body: body, eventID: "a", sig: Sign(secret, time.Now().Unix(), "ev", "b", body)},
	}
	for name, c := range cases {
		if code, _ := do(t, srv, c); code != http.StatusUnauthorized {
			t.Errorf("%s: status %d, want 401", name, code)
		}
	}
	if evs, _ := eng.Store.ListEvents(context.Background(), "", "", 50); len(evs) != 0 {
		t.Fatalf("%d events stored from unauthenticated calls", len(evs))
	}
}

func TestSignedCallQueuesAndDuplicateIsAnsweredOnce(t *testing.T) {
	srv, eng := newServer(t)
	code, r := do(t, srv, call{eventID: "msg-1", body: []byte("deck")})
	if code != http.StatusAccepted || !r.Accepted || r.Duplicate || r.Event.PayloadPath != "" {
		t.Fatalf("first: %d %+v", code, r)
	}
	code, r2 := do(t, srv, call{eventID: "msg-1", body: []byte("deck")})
	if code != http.StatusOK || !r2.Duplicate || r2.Event.ID != r.Event.ID {
		t.Fatalf("retry: %d %+v", code, r2)
	}
	if evs, _ := eng.Store.ListEvents(context.Background(), "ev", "", 50); len(evs) != 1 {
		t.Fatalf("%d events", len(evs))
	}
}

func TestQueueFullTooLargeAndUnknown(t *testing.T) {
	srv, _ := newServer(t)
	for i := range 2 {
		if code, _ := do(t, srv, call{body: []byte{byte(i)}}); code != http.StatusAccepted {
			t.Fatalf("event %d: %d", i, code)
		}
	}
	if code, _ := do(t, srv, call{body: []byte("3")}); code != http.StatusTooManyRequests {
		t.Errorf("queue full: %d, want 429", code)
	}
	if code, _ := do(t, srv, call{body: bytes.Repeat([]byte("x"), 2000)}); code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized: %d, want 413", code)
	}
	if code, _ := do(t, srv, call{pipeline: "nope", body: []byte("x")}); code != http.StatusNotFound {
		t.Errorf("unknown pipeline: %d, want 404", code)
	}
	if code, _ := do(t, srv, call{eventID: "has spaces", body: []byte("x")}); code != http.StatusBadRequest {
		t.Errorf("bad event id: %d, want 400", code)
	}
}
