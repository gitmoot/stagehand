// Package homelock is the exclusive lock that makes one process the stage
// executor for a stagehand home.
package homelock

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// ErrHeld is returned when another process holds the lock.
var ErrHeld = errors.New("another stagehand process is executing stages for this home")

// Lock is a held lock.
type Lock struct{ f *os.File }

// TryAcquire takes the lock at path without waiting. On ErrHeld the error
// names the holder's pid when known.
func TryAcquire(path string) (*Lock, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		defer f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			raw := make([]byte, 32)
			n, _ := f.ReadAt(raw, 0)
			if pid, perr := strconv.Atoi(strings.TrimSpace(string(raw[:n]))); perr == nil {
				return nil, fmt.Errorf("%w (pid %d)", ErrHeld, pid)
			}
			return nil, ErrHeld
		}
		return nil, err
	}
	_ = f.Truncate(0)
	_, _ = f.WriteAt([]byte(strconv.Itoa(os.Getpid())+"\n"), 0)
	return &Lock{f: f}, nil
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
