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

	// Track durably records what the attempt started outside the runner (a
	// process group, a sandbox) before it does any work, so a crashed runner
	// can stop it later. The value is executor-defined, e.g. "process:..." or
	// "sandbox:...".
	Track func(resource string) error
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
// After a crash the engine offers every interrupted attempt's resource to
// every Cleaner; each stops the ones it recognises and ignores the rest.
type Cleaner interface {
	Cleanup(ctx context.Context, resource string) error
}
