package agentexec

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/gitmoot/stagehand/internal/shellexec"
)

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
	// inherited.
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

	cmd := exec.Command(bin, req.Args...)
	cmd.Dir = dir
	cmd.Env = l.Env
	cmd.Stdin = nil // omp reads stdin to EOF in json mode; /dev/null ends it
	cmd.Stderr = req.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 0, err
	}
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	pgid := cmd.Process.Pid
	if req.Track != nil {
		if err := req.Track(shellexec.ProcessResource(pgid)); err != nil {
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
			_ = cmd.Wait()
			return 0, fmt.Errorf("record process: %w", err)
		}
	}

	stop := context.AfterFunc(ctx, func() {
		_ = syscall.Kill(-pgid, syscall.SIGTERM)
		time.AfterFunc(5*time.Second, func() { _ = syscall.Kill(-pgid, syscall.SIGKILL) })
	})
	defer stop()

	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
	for sc.Scan() {
		onLine(sc.Bytes())
	}
	scanErr := sc.Err()
	waitErr := cmd.Wait()
	_ = syscall.Kill(-pgid, syscall.SIGKILL)

	if scanErr != nil {
		return 0, fmt.Errorf("read omp output: %w", scanErr)
	}
	var exitErr *exec.ExitError
	switch {
	case waitErr == nil:
		return 0, nil
	case errors.As(waitErr, &exitErr):
		return exitErr.ExitCode(), nil
	default:
		return 0, waitErr
	}
}

// Cleanup stops a process group left by a crashed runner.
func (Local) Cleanup(ctx context.Context, resource string) error {
	return shellexec.Executor{}.Cleanup(ctx, resource)
}
