package agentexec

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gitmoot/stagehand/internal/stage"
)

// fakeOmp writes a shell script standing in for omp.
func fakeOmp(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "omp")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

// omp exits after a complete answer but leaves a child holding stdout: the
// stage must finish when omp does, not at its timeout.
func TestLocalFinishesWhenOmpExitsDespiteChildHoldingStdout(t *testing.T) {
	bin := fakeOmp(t, `printf '%s\n' '`+msg(`{"ok":true}`, 0.1)+`' '`+end+`'
sleep 60 &
exit 0`)
	job := newJob(t, 1)
	be := Local{Omp: bin, Dir: t.TempDir(), Env: []string{"PATH=/usr/bin:/bin"}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := time.Now()
	res := Executor{Backend: be}.Execute(ctx, job)
	if res.Outcome != stage.Succeeded {
		t.Fatalf("%+v", res)
	}
	if d := time.Since(start); d > 8*time.Second {
		t.Errorf("took %v; the stage waited for the child instead of omp", d)
	}
}

func TestLocalNilEnvInheritsNothing(t *testing.T) {
	t.Setenv("STAGEHAND_LOCAL_TEST_SECRET", "must-not-leak")
	bin := fakeOmp(t, `if env | /bin/grep -q STAGEHAND_LOCAL_TEST_SECRET; then a=leaked; else a=clean; fi
printf '{"type":"message_end","message":{"role":"assistant","stopReason":"stop","content":[{"type":"text","text":"{\\"env\\":\\"%s\\"}"}]}}\n' "$a"
printf '%s\n' '`+end+`'`)
	job := newJob(t, 1)
	res := Executor{Backend: Local{Omp: bin, Dir: t.TempDir()}}.Execute(context.Background(), job)
	if res.Outcome != stage.Succeeded {
		t.Fatalf("%+v", res)
	}
	raw, _ := os.ReadFile(filepath.Join(job.WorkDir, "decisions.json"))
	if !strings.Contains(string(raw), `"clean"`) {
		t.Errorf("omp saw the runner's environment: %s", raw)
	}
}

func TestInputNamedLikeOutputIsReadBeforeOutputIsRemoved(t *testing.T) {
	job := newJob(t, 1)
	job.Stage.Inputs = []string{"decisions.json"}
	if err := os.WriteFile(filepath.Join(job.WorkDir, "decisions.json"), []byte(`["upstream"]`), 0o600); err != nil {
		t.Fatal(err)
	}
	be := &fakeBackend{lines: []string{msg(`{"ok":true}`, 0.1), end}}
	if res := (Executor{Backend: be}).Execute(context.Background(), job); res.Outcome != stage.Succeeded {
		t.Fatalf("%+v", res)
	}
	if string(be.got.Files["decisions.json"]) != `["upstream"]` {
		t.Errorf("input lost: %q", be.got.Files["decisions.json"])
	}
}
