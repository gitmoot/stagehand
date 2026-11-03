// Package homelock is the exclusive lock that makes one process the stage
// executor for a stagehand home. The lock file names the holder's pid and
// mode, so a CLI command can tell a long-lived `serve` (which will execute
// queued runs) from another short-lived command (which will not).
package homelock

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// Modes.
const (
	// ModeServe is a long-lived executor that runs every queued run.
	ModeServe = "serve"
	// ModeCLI is a command executing only its own runs, then exiting.
	ModeCLI = "cli"
)

// ErrHeld is returned when another process holds the lock.
var ErrHeld = errors.New("another stagehand process is executing stages for this home")

// HeldError describes the holder when known.
type HeldError struct {
	PID  int
	Mode string
}

func (e *HeldError) Error() string {
	if e.PID == 0 {
		return ErrHeld.Error()
	}
	return fmt.Sprintf("%s (pid %d, %s)", ErrHeld, e.PID, e.Mode)
}

func (e *HeldError) Unwrap() error { return ErrHeld }

// Lock is a held lock.
type Lock struct{ f *os.File }

// TryAcquire takes the lock at path without waiting and records mode. When
// it is held, the error is a *HeldError.
func TryAcquire(path, mode string) (*Lock, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		defer f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, readHolder(f)
		}
		return nil, err
	}
	_ = f.Truncate(0)
	_, _ = f.WriteAt([]byte(fmt.Sprintf("%d %s\n", os.Getpid(), mode)), 0)
	return &Lock{f: f}, nil
}

func readHolder(f *os.File) *HeldError {
	raw := make([]byte, 64)
	n, _ := f.ReadAt(raw, 0)
	fields := strings.Fields(string(raw[:n]))
	he := &HeldError{}
	if len(fields) > 0 {
		he.PID, _ = strconv.Atoi(fields[0])
	}
	if len(fields) > 1 {
		he.Mode = fields[1]
	}
	return he
}

// Release drops the lock.
func (l *Lock) Release() error {
	if l == nil || l.f == nil {
		return nil
	}
	_ = l.f.Truncate(0)
	err := l.f.Close()
	l.f = nil
	return err
}
