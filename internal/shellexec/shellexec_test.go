package shellexec

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gitmoot/stagehand/internal/spec"
	"github.com/gitmoot/stagehand/internal/stage"
)

func job(t *testing.T, cmd string) stage.Job {
	t.Helper()
	dir := t.TempDir()
	return stage.Job{
		Stage:      spec.Stage{ID: "s", Cmd: cmd},
		WorkDir:    dir,
		LogPath:    filepath.Join(dir, "s.log"),
		ResultPath: filepath.Join(dir, "result.json"),
		Env:        []string{"PATH=/usr/bin:/bin", "STAGEHAND_RESULT=" + filepath.Join(dir, "result.json"), "ONLY=me"},
	}
}

func TestExitCodeDecidesOutcome(t *testing.T) {
	res := Executor{}.Execute(context.Background(), job(t, "echo nope >&2; exit 4"))
	if res.Outcome != stage.Failed || res.ExitCode == nil || *res.ExitCode != 4 {
		t.Fatalf("got %+v, want failed exit 4", res)
	}
	if !strings.Contains(res.Summary, "nope") {
		t.Errorf("summary %q does not carry the last log line", res.Summary)
	}
	res = Executor{}.Execute(context.Background(), job(t, "true"))
	if res.Outcome != stage.Succeeded || *res.ExitCode != 0 {
		t.Fatalf("got %+v, want succeeded", res)
	}
}

func TestEnvironmentIsNotInherited(t *testing.T) {
	t.Setenv("STAGEHAND_TEST_PARENT_SECRET", "leak")
	j := job(t, `env > "$PWD/env.txt"`)
	if res := (Executor{}).Execute(context.Background(), j); res.Outcome != stage.Succeeded {
		t.Fatalf("%+v", res)
	}
	raw, _ := os.ReadFile(filepath.Join(j.WorkDir, "env.txt"))
	if strings.Contains(string(raw), "STAGEHAND_TEST_PARENT_SECRET") {
		t.Error("parent environment leaked into the stage")
	}
	if !strings.Contains(string(raw), "ONLY=me") {
		t.Error("stage environment missing")
	}
}

func TestResultFileCanBlock(t *testing.T) {
	j := job(t, `printf '{"decision":"blocked","summary":"waiting","needs":["rows"]}' > "$STAGEHAND_RESULT"`)
	res := Executor{}.Execute(context.Background(), j)
	if res.Outcome != stage.Blocked || res.Summary != "waiting" || len(res.Needs) != 1 {
		t.Fatalf("got %+v, want blocked with needs", res)
	}
}

func TestInvalidResultFileFails(t *testing.T) {
	for _, body := range []string{`not json`, `{"decision":"maybe"}`, `{"decison":"blocked"}`} {
		j := job(t, "cat > \"$STAGEHAND_RESULT\" <<'EOF'\n"+body+"\nEOF")
		if res := (Executor{}).Execute(context.Background(), j); res.Outcome != stage.Failed {
			t.Errorf("result %q: got %+v, want failed", body, res)
		}
	}
}

func TestTimeoutKillsTheWholeGroup(t *testing.T) {
	j := job(t, `sleep 60 & echo $! > "$PWD/child.pid"; wait`)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	res := Executor{}.Execute(ctx, j)
	if res.Outcome != stage.Failed || !strings.Contains(res.Summary, "timed out") {
		t.Fatalf("got %+v, want timed out", res)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("timeout took too long")
	}
	raw, err := os.ReadFile(filepath.Join(j.WorkDir, "child.pid"))
	if err != nil {
		t.Fatal(err)
	}
	pid := 0
	for _, c := range strings.TrimSpace(string(raw)) {
		pid = pid*10 + int(c-'0')
	}
	time.Sleep(100 * time.Millisecond)
	if err := syscall.Kill(pid, 0); err == nil {
		_ = syscall.Kill(pid, syscall.SIGKILL)
		t.Fatalf("background child %d survived the timeout", pid)
	}
}

func TestCleanupKillsRecordedGroupOnly(t *testing.T) {
	tracked := make(chan string, 1)
	j := job(t, "sleep 60")
	j.Track = func(r string) error { tracked <- r; return nil }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan stage.Result)
	go func() { done <- Executor{}.Execute(ctx, j) }()
	var resource string
	select {
	case resource = <-tracked:
	case <-time.After(5 * time.Second):
		t.Fatal("process was not tracked")
	}
	if !strings.HasPrefix(resource, "process:") {
		t.Fatalf("resource %q", resource)
	}
	// A resource whose start time does not match must be left alone.
	pidPart := strings.TrimPrefix(resource, "process:")
	pidText, _, _ := strings.Cut(pidPart, "@")
	if err := (Executor{}).Cleanup(context.Background(), "process:"+pidText+"@1"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		t.Fatal("cleanup killed a group whose start time did not match")
	case <-time.After(200 * time.Millisecond):
	}
	if err := (Executor{}).Cleanup(context.Background(), resource); err != nil {
		t.Fatal(err)
	}
	select {
	case res := <-done:
		if res.Outcome != stage.Failed {
			t.Errorf("got %+v", res)
		}
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("cleanup did not kill the recorded group")
	}
	cancel()
}
