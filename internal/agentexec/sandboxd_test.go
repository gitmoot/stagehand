package agentexec

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gitmoot/stagehand/internal/sandboxd"
	"github.com/gitmoot/stagehand/internal/stage"
)

// fakeSandboxd implements the strict-profile routes stagehand uses and keeps
// a live inventory. Exec of the configured omp path replays stdout lines; it
// can hang until the client goes away.
type fakeSandboxd struct {
	t        *testing.T
	mu       sync.Mutex
	created  []map[string]any
	live     map[string]string // sandbox id -> job_id
	uploads  map[string]string
	execs    []map[string]any
	deleted  []string
	ompLines []string
	ompHang  bool
	ompExit  int
	// deleteFails makes the next N DELETEs answer 503 and keep the VM.
	deleteFails int
	// truncateCreate allocates the VM but cuts the create response.
	truncateCreate bool
}

func (f *fakeSandboxd) liveCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.live)
}

func (f *fakeSandboxd) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /sandboxes", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-Key") != "key" {
			http.Error(w, "no", 401)
			return
		}
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		meta := body["metadata"].(map[string]any)
		f.mu.Lock()
		f.created = append(f.created, body)
		f.live["sandboxd-abc"] = meta["job_id"].(string)
		truncate := f.truncateCreate
		f.mu.Unlock()
		w.WriteHeader(201)
		if truncate {
			w.Write([]byte(`{"sandboxID":"sandboxd-a`))
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"sandboxID": "sandboxd-abc", "envdAccessToken": "tok", "templateID": body["templateID"]})
	})
	mux.HandleFunc("GET /v2/sandboxes", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		out := []map[string]any{}
		for id, job := range f.live {
			out = append(out, map[string]any{"sandboxID": id, "metadata": map[string]string{"job_id": job}})
		}
		f.mu.Unlock()
		json.NewEncoder(w).Encode(out)
	})
	mux.HandleFunc("DELETE /sandboxes/{id}", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.deleted = append(f.deleted, r.PathValue("id"))
		if f.deleteFails > 0 {
			f.deleteFails--
			http.Error(w, "uncertain destroy", http.StatusServiceUnavailable)
			return
		}
		delete(f.live, r.PathValue("id"))
		w.WriteHeader(204)
	})
	mux.HandleFunc("POST /files", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Access-Token") != "tok" || r.Header.Get("E2b-Sandbox-Id") != "sandboxd-abc" {
			http.Error(w, "no", 401)
			return
		}
		data, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.uploads[r.URL.Query().Get("path")] = string(data)
		f.mu.Unlock()
		w.Write([]byte("{}"))
	})
	mux.HandleFunc("POST /process.Process/Start", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var req struct {
			Process map[string]any `json:"process"`
		}
		json.Unmarshal(raw[5:], &req)
		f.mu.Lock()
		f.execs = append(f.execs, req.Process)
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/connect+json")
		frame := func(flag byte, v any) {
			b, _ := json.Marshal(v)
			hdr := make([]byte, 5)
			hdr[0] = flag
			binary.BigEndian.PutUint32(hdr[1:], uint32(len(b)))
			w.Write(hdr)
			w.Write(b)
			w.(http.Flusher).Flush()
		}
		frame(0, map[string]any{"event": map[string]any{"start": map[string]any{"pid": 7}}})
		code := 0
		if req.Process["cmd"] == "/opt/omp" {
			for _, l := range f.ompLines {
				// Split each line across two data events to exercise line joining.
				half := len(l) / 2
				frame(0, map[string]any{"event": map[string]any{"data": map[string]any{"stdout": []byte(l[:half])}}})
				frame(0, map[string]any{"event": map[string]any{"data": map[string]any{"stdout": []byte(l[half:] + "\n")}}})
			}
			if f.ompHang {
				<-r.Context().Done()
				return
			}
			code = f.ompExit
		}
		frame(0, map[string]any{"event": map[string]any{"end": map[string]any{"exitCode": code, "exited": true}}})
		frame(2, map[string]any{})
	})
	return mux
}

func newFake(t *testing.T) (*fakeSandboxd, Sandboxd, string) {
	t.Helper()
	f := &fakeSandboxd{t: t, uploads: map[string]string{}, live: map[string]string{}}
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	be := Sandboxd{
		Client:          &sandboxd.Client{BaseURL: srv.URL, APIKey: "key"},
		Template:        "stagehand-omp-strict",
		GuestOmp:        "/opt/omp",
		MaxTTL:          time.Hour,
		GatewayListen:   addr,
		GuestGatewayURL: "http://10.0.2.2:43293",
		GatewayUpstream: "http://127.0.0.1:1",
	}
	return f, be, addr
}

// recorder captures Track/Release like the engine's pending-cleanup table.
type recorder struct {
	mu      sync.Mutex
	tracked []string
	pending map[string]bool
}

func (r *recorder) attach(job *stage.Job) {
	r.pending = map[string]bool{}
	job.Track = func(res string) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.tracked = append(r.tracked, res)
		r.pending[res] = true
		return nil
	}
	job.Release = func(res string) {
		r.mu.Lock()
		defer r.mu.Unlock()
		delete(r.pending, res)
	}
}

func TestSandboxdRunsOmpInAFreshVMAndDestroysIt(t *testing.T) {
	f, be, _ := newFake(t)
	f.ompLines = []string{msg(`{"ok":true}`, 0.1), end}
	job := newJob(t, 1)
	var rec recorder
	rec.attach(&job)
	res := Executor{Backend: be}.Execute(context.Background(), job)
	if res.Outcome != stage.Succeeded {
		t.Fatalf("%+v", res)
	}
	meta := f.created[0]["metadata"].(map[string]any)
	if len(rec.tracked) != 1 || rec.tracked[0] != "sandboxd-job:"+meta["job_id"].(string) {
		t.Errorf("tracked %v, want the job id recorded", rec.tracked)
	}
	if len(rec.pending) != 0 {
		t.Errorf("pending %v after a confirmed destroy", rec.pending)
	}
	if f.uploads["/home/user/work/rows.json"] != `[{"id":1}]` {
		t.Errorf("uploads %v", f.uploads)
	}
	if f.liveCount() != 0 {
		t.Error("VM left running")
	}
	meta = f.created[0]["metadata"].(map[string]any)
	if !strings.HasPrefix(meta["job_id"].(string), "stagehand-") || meta["attempt"] != "1" || f.created[0]["secure"] != true {
		t.Errorf("create body %v", f.created[0])
	}
	ompExec := f.execs[len(f.execs)-1]
	env := ompExec["envs"].(map[string]any)
	if env["ANTHROPIC_BASE_URL"] != "http://10.0.2.2:43293" || !strings.HasPrefix(env["ANTHROPIC_API_KEY"].(string), "sh-run-") {
		t.Errorf("omp env %v", env)
	}
	for k := range env {
		if strings.Contains(k, "OAUTH") || k == "OPENAI_API_KEY" {
			t.Errorf("unexpected credential variable %s in the VM", k)
		}
	}
}

func TestSandboxdOverBudgetDestroysVM(t *testing.T) {
	f, be, addr := newFake(t)
	f.ompLines = []string{msg("step", 0.6), msg("step", 0.6)}
	f.ompHang = true
	job := newJob(t, 0.5)
	results := make(chan stage.Result, 1)
	go func() { results <- Executor{Backend: be}.Execute(context.Background(), job) }()
	var res stage.Result
	select {
	case res = <-results:
	case <-time.After(10 * time.Second):
		t.Fatal("over-budget run did not stop")
	}
	if res.Outcome != stage.Failed || !strings.Contains(res.Summary, "over budget") {
		t.Fatalf("%+v", res)
	}
	f.mu.Lock()
	deleted := slices.Clone(f.deleted)
	f.mu.Unlock()
	if len(deleted) != 1 {
		t.Errorf("deleted %v, want the VM destroyed", deleted)
	}
	// The run gateway closes with the stage.
	if c, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		c.Close()
		t.Error("run gateway still listening after the stage ended")
	}
}

// A DELETE that sandboxd answers with 503 leaves the VM: the record must stay
// pending, even though the stage succeeded, and a later Cleanup destroys it.
func TestSandboxdUnconfirmedDestroyStaysPending(t *testing.T) {
	f, be, _ := newFake(t)
	f.ompLines = []string{msg(`{"ok":true}`, 0.1), end}
	f.deleteFails = 1
	job := newJob(t, 1)
	var rec recorder
	rec.attach(&job)
	if res := (Executor{Backend: be}).Execute(context.Background(), job); res.Outcome != stage.Succeeded {
		t.Fatalf("%+v", res)
	}
	if f.liveCount() != 1 || len(rec.pending) != 1 {
		t.Fatalf("live %d, pending %v; want the VM alive and its record pending", f.liveCount(), rec.pending)
	}
	done, err := be.Cleanup(context.Background(), rec.tracked[0])
	if err != nil || !done || f.liveCount() != 0 {
		t.Fatalf("retry: done=%v err=%v live=%d", done, err, f.liveCount())
	}
}

// If the create response is lost, the VM still exists; it carries the job id
// recorded before the request, so the attempt's own cleanup finds it.
func TestSandboxdAmbiguousCreateIsCleanedUp(t *testing.T) {
	f, be, _ := newFake(t)
	f.truncateCreate = true
	job := newJob(t, 1)
	var rec recorder
	rec.attach(&job)
	res := Executor{Backend: be}.Execute(context.Background(), job)
	if res.Outcome != stage.Failed {
		t.Fatalf("%+v", res)
	}
	if len(rec.tracked) != 1 {
		t.Fatalf("tracked %v; the job id must be recorded before create", rec.tracked)
	}
	if f.liveCount() != 0 || len(rec.pending) != 0 {
		t.Errorf("live %d, pending %v; want the allocated VM found and destroyed", f.liveCount(), rec.pending)
	}
}

func TestSandboxdCleanupIgnoresOtherResources(t *testing.T) {
	f, be, _ := newFake(t)
	f.live["sandboxd-other"] = "someone-else"
	f.live["sandboxd-old"] = "stagehand-old"
	if done, err := be.Cleanup(context.Background(), "process:123@4"); done || err != nil {
		t.Errorf("process resource: done=%v err=%v", done, err)
	}
	if done, err := be.Cleanup(context.Background(), "sandboxd-job:stagehand-old"); !done || err != nil {
		t.Errorf("done=%v err=%v", done, err)
	}
	if !slices.Equal(f.deleted, []string{"sandboxd-old"}) {
		t.Errorf("deleted %v; must touch only the recorded job's VM", f.deleted)
	}
}
