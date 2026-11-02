package procgroup

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Nothing in the script runs until Open: a runner that dies (or fails to
// record the group) before Open leaves no side effects.
func TestGateHoldsCommandUntilOpen(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "ran")
	p, err := Start(Shell("", "touch "+marker))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("command ran before the gate opened")
	}
	p.Abort()
	code, err := p.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if code != GateExit && code != -1 {
		t.Errorf("exit %d, want %d (gate closed) or killed", code, GateExit)
	}
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("command ran after the gate was aborted")
	}
}

func TestOpenRunsCommandWithExactEnv(t *testing.T) {
	t.Setenv("PROCGROUP_TEST_PARENT", "leak")
	dir := t.TempDir()
	out := filepath.Join(dir, "env")
	cmd := Shell("", `env > "`+out+`"`)
	p, err := Start(cmd) // Env nil: must mean empty, not inherited
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Open(); err != nil {
		t.Fatal(err)
	}
	if code, err := p.Finish(); err != nil || code != 0 {
		t.Fatalf("exit %d, %v", code, err)
	}
	raw, _ := os.ReadFile(out)
	if strings.Contains(string(raw), "PROCGROUP_TEST_PARENT") {
		t.Errorf("parent environment inherited: %s", raw)
	}
}

// The runner dying before Open closes the gate's write end: the child must
// exit without running its command.
func TestGateClosedByRunnerDeathExits(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "ran")
	p, err := Start(Shell("", "touch "+marker))
	if err != nil {
		t.Fatal(err)
	}
	p.gate.Close() // what process exit does to the write end
	select {
	case <-p.Exited():
	case <-time.After(5 * time.Second):
		t.Fatal("gated child did not exit when the gate closed")
	}
	code, _ := p.Finish()
	if code != GateExit {
		t.Errorf("exit %d, want %d", code, GateExit)
	}
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("command ran although the gate was never opened")
	}
}

// Finish kills descendants left in the group once the leader exits.
func TestFinishKillsLeftoverDescendants(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "child")
	p, err := Start(Shell("", `sleep 60 & echo $! > "`+pidFile+`"`))
	if err != nil {
		t.Fatal(err)
	}
	p.Open()
	<-p.Exited()
	if _, err := p.Finish(); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(pidFile)
	pid, _ := strconv.Atoi(strings.TrimSpace(string(raw)))
	time.Sleep(100 * time.Millisecond)
	if pid == 0 || syscall.Kill(pid, 0) == nil {
		_ = syscall.Kill(pid, syscall.SIGKILL)
		t.Fatalf("descendant %d survived Finish", pid)
	}
}
