// Package homelock is the exclusive lock that makes one process the stage
// executor for a stagehand home. The lock file names the holder's pid and
// mode, so a CLI command can tell a long-lived `serve` (which will execute
// queued runs) from another short-lived command (which will not).
package homelock

import (
	"errors"
	"fmt"
	"io"
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

// Holder reports who holds the lock at path, without taking it, so it can
// never delay an executor. nil means nobody holds it. The lock file names
// its holder only while held (Release empties it); the record is trusted
// only if /proc/locks shows that very process holding a flock on this very
// file, so a holder that died, or a recycled pid, is not reported. An
// unreadable or malformed record, or a lock path that is not a regular file,
// is an error: who holds it is unknown.
func Holder(path string) (*HeldError, error) {
	// Non-blocking, so a FIFO at the lock path cannot hang the caller.
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", path)
	}
	raw := make([]byte, 64)
	n, err := f.ReadAt(raw, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	fields := strings.Fields(string(raw[:n]))
	if len(fields) == 0 {
		return nil, nil // released
	}
	pid, perr := strconv.Atoi(fields[0])
	if len(fields) != 2 || perr != nil || pid <= 0 {
		return nil, fmt.Errorf("%s: unreadable holder record %q", path, raw[:n])
	}
	sys, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, fmt.Errorf("%s: no device and inode", path)
	}
	held, err := holdsFlock(pid, uint64(sys.Dev), uint64(sys.Ino))
	if err != nil || !held {
		return nil, err
	}
	return &HeldError{PID: pid, Mode: fields[1]}, nil
}

// locksFile is the kernel's table of held file locks (tests replace it).
var locksFile = "/proc/locks"

// holdsFlock reports whether pid holds a flock on the file dev/ino. Lines
// read "1: FLOCK  ADVISORY  WRITE <pid> <major>:<minor>:<inode> 0 EOF" (major
// and minor in hex); a waiter's line has "->" after the number and does not
// count.
func holdsFlock(pid int, dev, ino uint64) (bool, error) {
	raw, err := os.ReadFile(locksFile)
	if err != nil {
		return false, err
	}
	major := (dev>>8)&0xfff | (dev>>32)&^uint64(0xfff)
	minor := dev&0xff | (dev>>12)&^uint64(0xff)
	file := fmt.Sprintf("%02x:%02x:%d", major, minor, ino)
	owner := strconv.Itoa(pid)
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Fields(line)
		if len(f) >= 6 && f[1] == "FLOCK" && f[4] == owner && f[5] == file {
			return true, nil
		}
	}
	return false, nil
}
