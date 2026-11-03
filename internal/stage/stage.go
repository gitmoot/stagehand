// Package stage is the contract between the engine and the things that run a
// stage (shell commands, omp agents). The engine owns state; an Executor only
// runs one attempt and reports what happened.
package stage

import (
	"context"

	"github.com/gitmoot/stagehand/internal/spec"
)

// Outcome is how an attempt ended, as seen by its executor.
type Outcome string

const (
	Succeeded Outcome = "succeeded"
	Failed    Outcome = "failed"
	// Blocked parks the run without counting as a failure: the stage needs
	// something (a decision, an input) before it can go on.
	Blocked Outcome = "blocked"
)

// Job is one attempt of one stage.
type Job struct {
	RunID    int64
	Pipeline string
	Stage    spec.Stage
	Attempt  int

	// WorkDir is the run's shared work directory (STAGEHAND_WORK). Stages
	// exchange files through it.
	WorkDir string
	// LogPath is where the attempt's output goes.
	LogPath string
	// ResultPath is where a shell stage may write its result JSON
	// (STAGEHAND_RESULT).
	ResultPath string
	// SourceDir is the directory the spec was added from (for prompt_file).
	SourceDir string
	// Env is the complete environment for a shell stage, KEY=VALUE. Nothing
	// else is inherited.
	Env []string

	// Track durably records something the attempt starts outside the runner:
	// before starting it when it can be named in advance (a sandbox job id),
	// otherwise as soon as it exists (a process group). The record stays as
	// a pending cleanup until Release confirms the thing is gone; a crashed
	// or failed cleanup is retried by the engine's cleanup sweep. The value
	// is executor-defined, e.g. "process:..." or "sandboxd-job:...".
	Track func(resource string) error
	// Release forgets a tracked resource once the executor has confirmed it
	// no longer exists.
	Release func(resource string)
}

// Result is what an executor reports. Cost and tokens are reported even on
// failure.
type Result struct {
	Outcome      Outcome
	ExitCode     *int
	Summary      string
	Needs        []string
	Cost         float64
	InputTokens  int64
	OutputTokens int64
}

// Executor runs one attempt. It must return when ctx is done, after stopping
// everything it started. The ctx deadline is the stage time limit.
type Executor interface {
	Execute(ctx context.Context, job Job) Result
}

// Cleaner is implemented by executors that start things outside the runner.
// The engine offers every pending cleanup that no running attempt owns to
// every Cleaner. done reports that the resource is confirmed gone (or never
// existed) and can be forgotten; false with a nil error means "not mine" or
// "not confirmable yet, ask again later".
type Cleaner interface {
	Cleanup(ctx context.Context, resource string) (done bool, err error)
}

// Limiter is implemented by executors that can run only a limited number of
// attempts at once (a sandboxd instance has one host port). The engine keeps
// further ready stages pending until a slot frees. Zero means no limit.
type Limiter interface {
	MaxConcurrent() int
}
