// Package procgroup starts a child in its own process group, holds it at a
// gate until the caller has recorded it durably, and stops the whole group
// without ever signalling a group id the kernel may have reused.
//
// The gate: the child is `sh` running gatePrelude first, which blocks reading
// one line from fd 3. Open writes that line; if the runner dies before Open,
// the pipe's write end closes, the read fails and the child exits 125 without
// running anything.
//
// Group safety: a process group id stays reserved while any member is alive,
// including the leader as an unreaped zombie. Group signals are therefore
// sent only before the leader is reaped; WaitLeader observes the leader's
// exit with waitid(WNOWAIT), which leaves it unreaped.
package procgroup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"unsafe"
)

// GatePrelude is prepended to a shell script to hold it at the gate.
const GatePrelude = "IFS= read -r _ <&3 || exit 125\nexec 3<&-\n"

// GateExit is the exit code of a child whose gate was closed without opening.
const GateExit = 125

// Proc is a started, gated process group.
type Proc struct {
	Cmd  *exec.Cmd
	PGID int

	gate     *os.File
	gateOnce sync.Once

	mu     sync.Mutex
	reaped bool
	done   chan struct{} // closed when the leader has exited (unreaped)
}

// Shell returns `sh -c GatePrelude+script` with extra positional args.
func Shell(sh, script string, args ...string) *exec.Cmd {
	if sh == "" {
		sh = "/bin/sh"
	}
	return exec.Command(sh, append([]string{"-c", GatePrelude + script}, args...)...)
}

// Exec returns a gated shell that execs bin with args once opened (the
// leader becomes bin, keeping the same pid and group).
func Exec(sh, bin string, args ...string) *exec.Cmd {
	return Shell(sh, `exec "$0" "$@"`, append([]string{bin}, args...)...)
}

// Start starts cmd (built by Shell or Exec) in a new process group, held at
// the gate. cmd.Env nil is replaced with an empty environment: nothing is
// inherited by accident.
func Start(cmd *exec.Cmd) (*Proc, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	if cmd.Env == nil {
		cmd.Env = []string{}
	}
	cmd.ExtraFiles = []*os.File{r} // fd 3
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		r.Close()
		w.Close()
		return nil, err
	}
	r.Close()
	p := &Proc{Cmd: cmd, PGID: cmd.Process.Pid, gate: w, done: make(chan struct{})}
	go p.watch()
	return p, nil
}

// Open lets the child run its program.
func (p *Proc) Open() error {
	var err error
	p.gateOnce.Do(func() {
		_, err = p.gate.Write([]byte("\n"))
		p.gate.Close()
	})
	return err
}

// Abort closes the gate unopened (the child exits 125) and kills the group.
func (p *Proc) Abort() {
	p.gateOnce.Do(func() { p.gate.Close() })
	p.Signal(syscall.SIGKILL)
}

// Exited is closed when the leader has exited (it is not reaped yet).
func (p *Proc) Exited() <-chan struct{} { return p.done }

func (p *Proc) watch() {
	// An error here (none expected for our own child) just means Exited
	// fires early; Finish still reaps correctly via Cmd.Wait.
	_ = waitNoReap(p.PGID)
	close(p.done)
}

// Signal sends sig to the whole group, unless the leader has been reaped
// (then the id may already belong to someone else).
func (p *Proc) Signal(sig syscall.Signal) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.reaped {
		return
	}
	_ = syscall.Kill(-p.PGID, sig)
}

// Finish kills whatever is left in the group, then reaps the leader. Call it
// after Exited is closed. It returns the leader's exit code.
func (p *Proc) Finish() (int, error) {
	<-p.done
	p.Signal(syscall.SIGKILL)
	p.mu.Lock()
	p.reaped = true // no more group signals from here on
	p.mu.Unlock()
	err := p.Cmd.Wait()
	p.gateOnce.Do(func() { p.gate.Close() })
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		return 0, nil
	case errors.As(err, &exitErr):
		return exitErr.ExitCode(), nil
	default:
		return 0, err
	}
}

// waitNoReap blocks until pid exits, leaving it a zombie.
func waitNoReap(pid int) error {
	const pPID = 1
	var siginfo [16]uint64
	for {
		_, _, errno := syscall.Syscall6(syscall.SYS_WAITID, pPID, uintptr(pid),
			uintptr(unsafe.Pointer(&siginfo[0])), syscall.WEXITED|syscall.WNOWAIT, 0, 0)
		switch errno {
		case 0:
			return nil
		case syscall.EINTR:
			continue
		default:
			return fmt.Errorf("waitid %d: %w", pid, errno)
		}
	}
}

// Record runs track(resource) but stops waiting when ctx ends, so a slow
// durable write cannot hold off cancellation. A nil track records nothing.
// If ctx wins, the write may still land later; the cleanup sweep then finds
// the group gone and forgets it.
func Record(ctx context.Context, track func(string) error, resource string) error {
	if track == nil {
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- track(resource) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
