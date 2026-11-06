package cli

import (
	"bytes"
	"encoding/json"
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
