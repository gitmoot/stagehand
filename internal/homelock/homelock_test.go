package homelock

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestHolderReadsWithoutTakingTheLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stagehand.lock")
	if h, err := Holder(path); h != nil || err != nil {
		t.Fatalf("no lock file: %v %v", h, err)
	}
	l, err := TryAcquire(path, ModeServe)
	if err != nil {
		t.Fatal(err)
	}
	h, err := Holder(path)
	if err != nil || h == nil || h.Mode != ModeServe || h.PID != os.Getpid() {
		t.Fatalf("held by serve: %+v %v", h, err)
	}
	l.Release()
	if h, err := Holder(path); h != nil || err != nil {
		t.Fatalf("released: %+v %v", h, err)
	}
}

// A record naming a live process that does not hold the lock (a serve that
// died without releasing, its pid now reused) is not a holder (#40).
func TestHolderIgnoresARecordWhoseProcessDoesNotHoldTheLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stagehand.lock")
	os.WriteFile(path, []byte(fmt.Sprintf("%d serve\n", os.Getpid())), 0o600)
	if h, err := Holder(path); h != nil || err != nil {
		t.Fatalf("a live pid without the lock reported as holder: %+v %v", h, err)
	}
}

// Anything that is not a readable record is unknown, never "nobody", and
// never hangs: a FIFO, a directory, a malformed record, no /proc/locks (#41).
func TestHolderUnknownStates(t *testing.T) {
	dir := t.TempDir()
	fifo := filepath.Join(dir, "fifo")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	sub := filepath.Join(dir, "sub")
	os.Mkdir(sub, 0o700)
	bad := filepath.Join(dir, "bad")
	os.WriteFile(bad, []byte("not-a-pid serve\n"), 0o600)
	for name, path := range map[string]string{"fifo": fifo, "directory": sub, "malformed": bad} {
		done := make(chan error, 1)
		go func() { _, err := Holder(path); done <- err }()
		select {
		case err := <-done:
			if err == nil {
				t.Errorf("%s: no error; who holds it is unknown", name)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("%s: Holder hung", name)
		}
	}
	held := filepath.Join(dir, "held")
	l, err := TryAcquire(held, ModeServe)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Release()
	old := locksFile
	locksFile = filepath.Join(dir, "missing")
	defer func() { locksFile = old }()
	if _, err := Holder(held); err == nil {
		t.Error("no /proc/locks: no error")
	}
}
