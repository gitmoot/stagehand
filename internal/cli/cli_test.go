package cli

import (
	"bytes"
	"encoding/json"
	"github.com/gitmoot/stagehand/internal/homelock"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func runCLI(home string, args ...string) (int, string, string) {
	var out, errb bytes.Buffer
	code := Main(append([]string{"--home", home}, args...), &out, &errb)
	return code, out.String(), errb.String()
}

// Two standalone `run` commands at once, no serve: the second must not be
// accepted as "queued" and then stranded. It waits for the first command's
// lock and executes its own run.
func TestConcurrentStandaloneRunsBothExecute(t *testing.T) {
	home := t.TempDir()
	dir := t.TempDir()
	gate := filepath.Join(dir, "gate")
	os.WriteFile(filepath.Join(dir, "a.yaml"), []byte("name: a\nstages:\n  - {id: wait, cmd: 'while [ ! -f "+gate+" ]; do sleep 0.05; done'}\n"), 0o600)
	os.WriteFile(filepath.Join(dir, "b.yaml"), []byte("name: b\nstages:\n  - {id: hi, cmd: 'echo hi'}\n"), 0o600)
	for _, f := range []string{"a.yaml", "b.yaml"} {
		if code, _, errs := runCLI(home, "add", filepath.Join(dir, f)); code != 0 {
			t.Fatal(errs)
		}
	}

	aDone := make(chan int, 1)
	go func() { code, _, _ := runCLI(home, "run", "a"); aDone <- code }()
	// Wait until a holds the lock and runs its stage.
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, out, _ := runCLI(home, "status", "--json")
		if strings.Contains(out, `"active_run_id": 1`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("run a never started")
		}
		time.Sleep(50 * time.Millisecond)
	}
	bDone := make(chan int, 1)
	go func() { code, _, _ := runCLI(home, "run", "b"); bDone <- code }()
	select {
	case code := <-bDone:
		t.Fatalf("run b returned (exit %d) while a held the lock; it must wait and execute", code)
	case <-time.After(500 * time.Millisecond):
	}
	os.WriteFile(gate, nil, 0o600)
	for name, ch := range map[string]chan int{"a": aDone, "b": bDone} {
		select {
		case code := <-ch:
			if code != 0 {
				t.Errorf("run %s exit %d", name, code)
			}
		case <-time.After(20 * time.Second):
			t.Fatalf("run %s did not finish", name)
		}
	}
	_, out, _ := runCLI(home, "runs", "b", "--json")
	var runs []struct{ State string }
	json.Unmarshal([]byte(out), &runs)
	if len(runs) != 1 || runs[0].State != "succeeded" {
		t.Fatalf("b runs %s", out)
	}
}

// trigger --wait returns once its own event's run is done, even while other
// pipelines keep running and newer events keep the queue non-empty.
func TestTriggerWaitReturnsWhenItsOwnRunIsDone(t *testing.T) {
	home := t.TempDir()
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "fast.yaml"), []byte("name: fast\nevents: {max_queued: 5}\nstages:\n  - {id: a, cmd: 'true'}\n"), 0o600)
	os.WriteFile(filepath.Join(dir, "slow.yaml"), []byte("name: slow\nevents: {max_queued: 5}\nstages:\n  - {id: a, cmd: 'sleep 30'}\n"), 0o600)
	for _, f := range []string{"fast.yaml", "slow.yaml"} {
		if code, _, errs := runCLI(home, "add", filepath.Join(dir, f), "--enable"); code != 0 {
			t.Fatal(errs)
		}
	}
	empty := filepath.Join(dir, "empty")
	os.WriteFile(empty, nil, 0o600)
	// A long event in another pipeline is queued first.
	if code, _, errs := runCLI(home, "trigger", "slow", "--payload-file", empty); code != 0 {
		t.Fatal(errs)
	}
	done := make(chan int, 1)
	start := time.Now()
	go func() { code, _, _ := runCLI(home, "trigger", "fast", "--payload-file", empty, "--wait"); done <- code }()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("exit %d", code)
		}
		if time.Since(start) > 15*time.Second {
			t.Errorf("took %v; it waited for the other pipeline", time.Since(start))
		}
	case <-time.After(25 * time.Second):
		t.Fatal("trigger --wait did not return after its own run finished")
	}
}

// A run a usage limit skips is not a failure: `run --wait` exits 75
// (EX_TEMPFAIL), not 1, and says the next slot runs as usual.
func TestRunWaitExitsTempFailWhenAUsageLimitSkipsTheRun(t *testing.T) {
	home := t.TempDir()
	dir := t.TempDir()
	limit429 := `Anthropic stream error (api_error): 429 {"type":"error","error":{"type":"rate_limit_error","message":"rate limited"}}`
	turn, _ := json.Marshal(limit429 + " retry-after-ms=12769000")
	stream := `{"type":"message_end","message":{"role":"assistant","stopReason":"error","errorMessage":` + string(turn) + `,"content":[]}}` + "\n" +
		`{"type":"agent_end","isTerminal":true}` + "\n"
	streamPath := filepath.Join(dir, "stream.jsonl")
	omp := filepath.Join(dir, "omp")
	os.WriteFile(streamPath, []byte(stream), 0o600)
	os.WriteFile(omp, []byte("#!/bin/sh\ncat "+streamPath+"\n"), 0o700)
	os.WriteFile(filepath.Join(home, "config.yaml"), []byte("agent:\n  executor: local\n  allow_unsafe_local: true\n  omp: "+omp+"\n"), 0o600)
	os.WriteFile(filepath.Join(dir, "lim.yaml"), []byte("name: lim\nstages:\n  - {id: r, agent: omp, prompt: x, output: o.json, max_cost: 1, timeout: 1m}\n"), 0o600)
	if code, _, errs := runCLI(home, "add", filepath.Join(dir, "lim.yaml")); code != 0 {
		t.Fatal(errs)
	}
	code, out, errs := runCLI(home, "run", "lim", "--wait")
	if code != 75 {
		t.Fatalf("exit %d, want 75\nstdout: %s\nstderr: %s", code, out, errs)
	}
	if !strings.Contains(errs, "run #1 skipped: usage limit until ") || !strings.Contains(errs, "; the next slot runs as usual") {
		t.Errorf("stderr %q", errs)
	}
	if !strings.Contains(out, "halted at r: usage limit until ") {
		t.Errorf("stdout %q", out)
	}
}

// With serve running, `run --json` and `resume --json` leave the run to serve
// and print JSON, not text, so a caller can parse the run id (#27).
func TestRunAndResumeQueuedPrintJSON(t *testing.T) {
	home := t.TempDir()
	dir := t.TempDir()
	// One pipeline per command: a pipeline has one active run at a time.
	for _, name := range []string{"p", "q"} {
		spec := filepath.Join(dir, name+".yaml")
		os.WriteFile(spec, []byte("name: "+name+"\nstages:\n  - {id: a, cmd: 'false'}\n"), 0o600)
		if code, _, errs := runCLI(home, "add", spec, "--enable"); code != 0 {
			t.Fatal(errs)
		}
	}
	// A failed run to resume later, executed here while nothing holds the lock.
	if code, _, _ := runCLI(home, "run", "p", "--wait"); code != 1 {
		t.Fatalf("setup run exit %d, want 1", code)
	}
	serve, err := homelock.TryAcquire(filepath.Join(home, "stagehand.lock"), homelock.ModeServe)
	if err != nil {
		t.Fatal(err)
	}
	defer serve.Release()
	for _, c := range []struct {
		args     []string
		pipeline string
	}{{[]string{"run", "q", "--json"}, "q"}, {[]string{"resume", "1", "--json"}, "p"}} {
		args := c.args
		code, out, errs := runCLI(home, args...)
		var q struct {
			ID       int64  `json:"id"`
			Pipeline string `json:"pipeline"`
			State    string `json:"state"`
			Executor string `json:"executor"`
		}
		if code != 0 || json.Unmarshal([]byte(out), &q) != nil || q.ID == 0 || q.Pipeline != c.pipeline || q.State != "queued" || q.Executor != "serve" {
			t.Errorf("%v: exit %d, stdout %q (stderr %q); want queued JSON for serve", args, code, out, errs)
		}
	}
}

// --queue never executes here, so a caller with a short timeout cannot be
// held by a long run and leave it parked. Without serve it still queues,
// and says who will start it (#27).
func TestQueueNeverExecutesHere(t *testing.T) {
	home := t.TempDir()
	dir := t.TempDir()
	marker := filepath.Join(dir, "ran")
	spec := filepath.Join(dir, "p.yaml")
	os.WriteFile(spec, []byte("name: p\nstages:\n  - {id: a, cmd: 'touch "+marker+"'}\n"), 0o600)
	if code, _, errs := runCLI(home, "add", spec, "--enable"); code != 0 {
		t.Fatal(errs)
	}
	code, out, errs := runCLI(home, "run", "p", "--queue", "--json")
	var q struct {
		ID       int64  `json:"id"`
		State    string `json:"state"`
		Executor string `json:"executor"`
	}
	if code != 0 || json.Unmarshal([]byte(out), &q) != nil || q.State != "queued" || q.Executor != "none" {
		t.Fatalf("exit %d, stdout %q; want queued JSON with executor none", code, out)
	}
	if !strings.Contains(errs, "no stagehand serve is running") {
		t.Errorf("stderr %q; want a warning that nothing executes it yet", errs)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("--queue executed the run")
	}
	// The next tick runs it.
	if code, _, errs := runCLI(home, "tick"); code != 0 {
		t.Fatal(errs)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Error("the queued run did not run on the next tick")
	}
	if code, _, _ := runCLI(home, "run", "p", "--queue", "--wait"); code == 0 {
		t.Error("--queue with --wait accepted")
	}
}
