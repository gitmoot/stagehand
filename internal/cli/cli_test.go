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
