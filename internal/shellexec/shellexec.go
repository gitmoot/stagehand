// Package shellexec runs shell stages: `sh -c <cmd>` in the stage's working
// directory, with an exact environment, in its own process group.
package shellexec

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/gitmoot/stagehand/internal/procgroup"
	"github.com/gitmoot/stagehand/internal/stage"
)

// KillGrace is how long a stage gets after SIGTERM before SIGKILL.
const KillGrace = 10 * time.Second

// Executor runs shell stages.
type Executor struct {
	// Shell is the shell binary; empty means /bin/sh.
	Shell string
}

// ResultFile is the optional JSON a shell stage writes to $STAGEHAND_RESULT.
type ResultFile struct {
	Decision string   `json:"decision"`
	Summary  string   `json:"summary"`
	Needs    []string `json:"needs"`
}

// Execute runs the stage command. The exit code decides the outcome; a
// result file can turn a successful exit into blocked (or failed) and add a
// summary.
func (e Executor) Execute(ctx context.Context, job stage.Job) stage.Result {
	logf, err := os.OpenFile(job.LogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return stage.Result{Outcome: stage.Failed, Summary: "open log: " + err.Error()}
	}
	defer logf.Close()

	dir := job.Stage.Cwd
	if dir == "" {
		dir = job.WorkDir
	}
	cmd := procgroup.Shell(e.Shell, job.Stage.Cmd)
	cmd.Dir = dir
	cmd.Env = job.Env
	cmd.Stdin = nil // /dev/null
	cmd.Stdout = logf
	cmd.Stderr = logf
	// The stage's own process group, held at the gate until it is recorded,
	// so a crash at any point leaves either nothing running or a recorded
	// group recovery can kill.
	p, err := procgroup.Start(cmd)
	if err != nil {
		return stage.Result{Outcome: stage.Failed, Summary: "start: " + err.Error()}
	}
	resource := ProcessResource(p.PGID)
	if err := procgroup.Record(ctx, job.Track, resource); err != nil {
		p.Abort()
		_, _ = p.Finish()
		if job.Release != nil {
			job.Release(resource)
		}
		if ctx.Err() != nil {
			return stage.Result{Outcome: stage.Failed, Summary: contextSummary(ctx)}
		}
		return stage.Result{Outcome: stage.Failed, Summary: "record process: " + err.Error()}
	}
	if err := p.Open(); err != nil {
		p.Abort()
	}

	select {
	case <-p.Exited():
	case <-ctx.Done():
		p.Signal(syscall.SIGTERM)
		select {
		case <-p.Exited():
		case <-time.After(KillGrace):
			p.Signal(syscall.SIGKILL)
		}
	}
	// Finish kills anything the command left in its group (while the leader
	// is still an unreaped zombie, so the group id cannot be reused), then
	// reaps it. Nothing remains to clean up after that.
	code, waitErr := p.Finish()
	if job.Release != nil {
		job.Release(resource)
	}

	if ctx.Err() != nil {
		return stage.Result{Outcome: stage.Failed, Summary: contextSummary(ctx)}
	}
	if waitErr != nil {
		return stage.Result{Outcome: stage.Failed, Summary: "wait: " + waitErr.Error()}
	}
	res := stage.Result{ExitCode: &code}

	rf, rfErr := readResult(job.ResultPath)
	switch {
	case rfErr != nil:
		res.Outcome = stage.Failed
		res.Summary = rfErr.Error()
	case code != 0:
		res.Outcome = stage.Failed
		res.Summary = fmt.Sprintf("exit %d", code)
		if rf != nil && rf.Summary != "" {
			res.Summary += ": " + rf.Summary
		} else if last := lastLine(job.LogPath); last != "" {
			res.Summary += ": " + last
		}
	case rf == nil || rf.Decision == "" || rf.Decision == "succeeded":
		res.Outcome = stage.Succeeded
		if rf != nil {
			res.Summary = rf.Summary
		}
	case rf.Decision == "blocked":
		res.Outcome = stage.Blocked
		res.Summary = rf.Summary
		res.Needs = rf.Needs
	case rf.Decision == "failed":
		res.Outcome = stage.Failed
		res.Summary = rf.Summary
	default:
		res.Outcome = stage.Failed
		res.Summary = fmt.Sprintf("result file: unknown decision %q (use succeeded, blocked or failed)", rf.Decision)
	}
	return res
}

func contextSummary(ctx context.Context) string {
	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) && !errors.Is(cause, context.DeadlineExceeded) {
		return cause.Error()
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "timed out"
	}
	return "stopped"
}

// readResult reads the optional result file. A missing file is not an error.
func readResult(path string) (*ResultFile, error) {
	if path == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("result file: %v", err)
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, nil
	}
	var rf ResultFile
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rf); err != nil {
		return nil, fmt.Errorf("result file is not valid JSON: %v", err)
	}
	return &rf, nil
}

// lastLine returns the last non-empty line of the log, trimmed to 200 bytes.
func lastLine(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	const tail = 4096
	if st, err := f.Stat(); err == nil && st.Size() > tail {
		_, _ = f.Seek(st.Size()-tail, 0)
	}
	var last string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, tail), tail*2)
	for sc.Scan() {
		if l := strings.TrimSpace(sc.Text()); l != "" {
			last = l
		}
	}
	if len(last) > 200 {
		last = last[:200] + "…"
	}
	return last
}
