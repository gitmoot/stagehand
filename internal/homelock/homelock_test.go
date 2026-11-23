package homelock

import (
	"os"
	"path/filepath"
	"testing"
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
	// A holder that died without releasing is not a holder.
	os.WriteFile(path, []byte("2147483646 serve\n"), 0o600)
	if h, err := Holder(path); h != nil || err != nil {
		t.Fatalf("dead holder: %+v %v", h, err)
	}
	dir := filepath.Join(t.TempDir(), "lockdir")
	os.Mkdir(dir, 0o700)
	if _, err := Holder(dir); err == nil {
		t.Error("a directory at the lock path is unknown, not 'nobody'")
	}
}
