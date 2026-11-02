package agentexec

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/gitmoot/stagehand/internal/procgroup"
	"github.com/gitmoot/stagehand/internal/shellexec"
)

// localKillGrace is how long omp gets after SIGTERM before SIGKILL.
const localKillGrace = 5 * time.Second

// drainGrace bounds how long stdout is read after omp's group is gone: a
// process that escaped the group could otherwise hold the pipe open forever.
const drainGrace = 2 * time.Second

// Local runs omp as a process on the runner's host.
//
// It is NOT a sandbox: omp's read tool can read any file the runner can, and
// fetch URLs. Use it only for development and tests; production agent stages
// run in sandboxd.
type Local struct {
	// Omp is the omp binary; empty means "omp" on PATH.
	Omp string
	// Dir is where per-run scratch directories are created.
	Dir string
	// Env is omp's complete environment (KEY=VALUE). Nothing else is
	// inherited; nil means an empty environment.
	Env []string
}

// Run copies the input files into a fresh directory and runs omp there in its
// own process group.
func (l Local) Run(ctx context.Context, req Request, onLine func([]byte)) (int, error) {
	bin := l.Omp
	if bin == "" {
		bin = "omp"
	}
	if err := os.MkdirAll(l.Dir, 0o700); err != nil {
		return 0, err
	}
	dir, err := os.MkdirTemp(l.Dir, "omp-")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(dir)
	for name, data := range req.Files {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			return 0, err
		}
	}

	// stdout is a plain pipe the runner owns, so omp's exit is observed from
	// the process itself, not from stdout reaching EOF.
	outR, outW, err := os.Pipe()
	if err != nil {
		return 0, err
	}
	defer outR.Close()
	cmd := procgroup.Exec("", bin, req.Args...)
	cmd.Dir = dir
	cmd.Env = append([]string{}, l.Env...)
	cmd.Stdin = nil // omp reads stdin to EOF in json mode; /dev/null ends it
	cmd.Stdout = outW
	cmd.Stderr = req.Stderr
	p, err := procgroup.Start(cmd)
	outW.Close()
	if err != nil {
		return 0, err
	}
	resource := shellexec.ProcessResource(p.PGID)
	if err := procgroup.Record(ctx, req.Track, resource); err != nil {
		p.Abort()
		_, _ = p.Finish()
		if req.Release != nil {
			req.Release(resource)
		}
		return 0, fmt.Errorf("record process: %w", err)
	}
	if err := p.Open(); err != nil {
		p.Abort()
	}

	readDone := make(chan error, 1)
	go func() {
		sc := bufio.NewScanner(outR)
		sc.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
		for sc.Scan() {
			onLine(sc.Bytes())
		}
		readDone <- sc.Err()
	}()

	select {
	case <-p.Exited():
	case <-ctx.Done():
		p.Signal(syscall.SIGTERM)
		select {
		case <-p.Exited():
		case <-time.After(localKillGrace):
			p.Signal(syscall.SIGKILL)
		}
	}
	// Leader gone: kill what it left in its group, reap it, then drain what
	// it wrote. A writer outside the group cannot hold the stage open.
	code, waitErr := p.Finish()
	if req.Release != nil {
		req.Release(resource)
	}
	var readErr error
	select {
	case readErr = <-readDone:
	case <-time.After(drainGrace):
		outR.Close()
		readErr = <-readDone
		if readErr == nil {
			readErr = fmt.Errorf("omp's stdout stayed open after it exited")
		}
	}
	if waitErr != nil {
		return 0, waitErr
	}
	if readErr != nil && ctx.Err() == nil {
		return code, fmt.Errorf("read omp output: %w", readErr)
	}
	return code, nil
}

// Cleanup stops a process group left by a crashed runner.
func (Local) Cleanup(ctx context.Context, resource string) (bool, error) {
	return shellexec.Executor{}.Cleanup(ctx, resource)
}
