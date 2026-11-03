// Package agentexec runs omp agent stages. It builds the prompt and omp's
// command line, hands them to a Backend (a local process for development, a
// sandboxd VM in production), watches omp's event stream to enforce the
// stage's usage cap, and writes the stage's output file from the agent's
// final answer. The agent itself never writes anything on the runner's host.
package agentexec

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gitmoot/stagehand/internal/omp"
	"github.com/gitmoot/stagehand/internal/stage"
)

// Request is one omp run for a Backend.
type Request struct {
	// Files are copied into omp's working directory before it starts, by
	// name: the stage's inputs.
	Files map[string][]byte
	// Args are omp's arguments, without the binary.
	Args []string
	// Track and Release are the attempt's resource record (see
	// stage.Job): Track before starting anything outside the runner,
	// Release once it is confirmed gone.
	Track   func(resource string) error
	Release func(resource string)
	// Stderr receives omp's stderr and backend diagnostics.
	Stderr io.Writer
}

// Backend runs omp somewhere and streams its stdout, line by line, to
// onLine. It returns omp's exit code. It must stop omp and release
// everything it created when ctx is done.
type Backend interface {
	Run(ctx context.Context, req Request, onLine func(line []byte)) (exitCode int, err error)
}

// Executor runs agent stages through a Backend.
type Executor struct {
	Backend Backend
	// Now is replaceable for tests.
	Now func() time.Time
}

// errOverBudget is the cancel cause when the usage cap is reached.
type errOverBudget struct{ spent, limit float64 }

func (e errOverBudget) Error() string {
	return fmt.Sprintf("over budget: spent %.4f of max_cost %.4f; the agent was stopped", e.spent, e.limit)
}

// Execute runs one attempt of an agent stage.
func (x Executor) Execute(ctx context.Context, job stage.Job) stage.Result {
	now := time.Now
	if x.Now != nil {
		now = x.Now
	}
	st := job.Stage
	logf, err := os.OpenFile(job.LogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return stage.Result{Outcome: stage.Failed, Summary: "open log: " + err.Error()}
	}
	defer logf.Close()
	fail := func(format string, args ...any) stage.Result {
		msg := fmt.Sprintf(format, args...)
		fmt.Fprintln(logf, "stagehand:", msg)
		return stage.Result{Outcome: stage.Failed, Summary: msg}
	}

	prompt, err := buildPrompt(job)
	if err != nil {
		return fail("%v", err)
	}
	files := map[string][]byte{}
	for _, name := range st.Inputs {
		raw, err := os.ReadFile(filepath.Join(job.WorkDir, name))
		if err != nil {
			return fail("input %s: %v (an earlier stage must write it to $STAGEHAND_WORK)", name, err)
		}
		files[name] = raw
	}
	// Inputs are read first, so removing the output can never destroy one.
	// A stale output from an earlier attempt must never pass for this one.
	outPath := filepath.Join(job.WorkDir, st.Output)
	if err := os.Remove(outPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fail("remove old output: %v", err)
	}
	maxTime := time.Duration(0)
	if deadline, ok := ctx.Deadline(); ok {
		maxTime = omp.MaxTimeFor(deadline, now())
	}
	args, err := omp.Args(st, maxTime, prompt)
	if err != nil {
		return fail("%v", err)
	}

	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var (
		mu     sync.Mutex
		stream omp.Stream
	)
	streamLog := bufio.NewWriter(logf)
	onLine := func(line []byte) {
		mu.Lock()
		defer mu.Unlock()
		streamLog.Write(line)
		streamLog.WriteByte('\n')
		stream.Feed(line)
		if spent := stream.Cost(); spent > st.MaxCost {
			cancel(errOverBudget{spent: spent, limit: st.MaxCost})
		}
	}
	exitCode, runErr := x.Backend.Run(runCtx, Request{Files: files, Args: args, Track: job.Track, Release: job.Release, Stderr: logf}, onLine)

	mu.Lock()
	defer mu.Unlock()
	_ = streamLog.Flush()
	in, out := stream.Tokens()
	res := stage.Result{Cost: stream.Cost(), InputTokens: in, OutputTokens: out}
	if runErr == nil {
		res.ExitCode = &exitCode
	}
	finish := func(outcome stage.Outcome, summary string) stage.Result {
		res.Outcome, res.Summary = outcome, summary
		fmt.Fprintf(logf, "stagehand: %s: %s (cost %.4f, %d assistant messages)\n", outcome, summary, res.Cost, stream.Messages())
		return res
	}

	var over errOverBudget
	if cause := context.Cause(runCtx); errors.As(cause, &over) {
		return finish(stage.Failed, over.Error())
	}
	if ctx.Err() != nil {
		// Timeout or cancel from the engine; the engine's cause wins.
		msg := "stopped"
		if c := context.Cause(ctx); c != nil {
			msg = c.Error()
		}
		return finish(stage.Failed, msg)
	}
	if runErr != nil {
		return finish(stage.Failed, "run omp: "+runErr.Error())
	}
	answer, err := stream.Result()
	if err != nil {
		if exitCode != 0 {
			return finish(stage.Failed, fmt.Sprintf("omp exited %d: %v", exitCode, err))
		}
		return finish(stage.Failed, err.Error())
	}
	if exitCode != 0 {
		return finish(stage.Failed, fmt.Sprintf("omp exited %d", exitCode))
	}
	if res.Cost > st.MaxCost {
		return finish(stage.Failed, errOverBudget{spent: res.Cost, limit: st.MaxCost}.Error())
	}
	doc, err := omp.ExtractJSON(answer)
	if err != nil {
		return finish(stage.Failed, err.Error())
	}
	if err := writeAtomic(outPath, doc); err != nil {
		return finish(stage.Failed, "write output: "+err.Error())
	}
	return finish(stage.Succeeded, fmt.Sprintf("wrote %s (%d bytes)", st.Output, len(doc)))
}

// Cleanup forwards crash recovery to the backend, if it starts anything.
func (x Executor) Cleanup(ctx context.Context, resource string) (bool, error) {
	if c, ok := x.Backend.(stage.Cleaner); ok {
		return c.Cleanup(ctx, resource)
	}
	return false, nil
}

// buildPrompt is the stage prompt plus the runner's contract: which input
// files exist and that the answer must be JSON only.
func buildPrompt(job stage.Job) (string, error) {
	st := job.Stage
	text := st.Prompt
	if st.PromptFile != "" {
		path := st.PromptFile
		if !filepath.IsAbs(path) {
			path = filepath.Join(job.SourceDir, path)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("prompt_file: %v", err)
		}
		text = string(raw)
	}
	var b strings.Builder
	b.WriteString(strings.TrimSpace(text))
	b.WriteString("\n\n---\n")
	if len(st.Inputs) > 0 {
		b.WriteString("Input files, in your current working directory: ")
		b.WriteString(strings.Join(st.Inputs, ", "))
		b.WriteString(". Read them with the read tool.\n")
	}
	b.WriteString("You cannot write files or run commands. Your final message is saved as ")
	b.WriteString(st.Output)
	b.WriteString(": it must be only valid JSON (or one ```json block), with no other text.\n")
	return b.String(), nil
}

func writeAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
