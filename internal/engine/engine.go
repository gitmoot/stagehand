// Package engine drives pipelines: it registers specs, creates runs, starts
// ready stages, folds their results, parks runs on a blocked or failed stage,
// resumes and cancels them, and fires schedules.
//
// Only the process that holds the home lock calls Recover and Loop. Other
// processes may call AddPipeline, SetEnabled, StartRun, Resume and Cancel:
// those only write rows, and the lock holder picks them up.
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gitmoot/stagehand/internal/spec"
	"github.com/gitmoot/stagehand/internal/stage"
	"github.com/gitmoot/stagehand/internal/store"
)

// DefaultPoll is how often the loop re-reads the store when nothing wakes it.
const DefaultPoll = 5 * time.Second

var (
	errCancelled = errors.New("cancelled by operator")
	errStopping  = errors.New("interrupted: the runner stopped while this stage ran")
)

// Engine runs pipelines against a store.
type Engine struct {
	Store *store.Store
	// Home is the state directory; run files live in Home/runs/<id>.
	Home string
	// Executors run stages by kind.
	Executors map[spec.Kind]stage.Executor
	// BaseEnv is given to every shell stage (KEY=VALUE), before STAGEHAND_*
	// and the stage's env_keys. Keep it to non-secret basics such as PATH.
	BaseEnv []string
	// Now and Jitter are replaceable for tests.
	Now    func() time.Time
	Jitter func(max time.Duration) time.Duration
	Log    *slog.Logger
	Poll   time.Duration

	mu       sync.Mutex
	inflight map[flightKey]context.CancelCauseFunc
	// liveResources are tracked by attempts still running in this process;
	// the cleanup sweep leaves them alone.
	liveResources map[string]bool
	// running counts attempts in flight per stage kind, for executors with
	// a concurrency limit (stage.Limiter).
	running map[spec.Kind]int
	// stopping is set once Loop is shutting down; advance then only folds
	// results and never starts a stage.
	stopping atomic.Bool
	// beforeEventCommit, if set (tests only), runs after an intake has
	// placed its payload file and before its event commits.
	beforeEventCommit func(payloadPath string)
	wake              chan struct{}
	wg                sync.WaitGroup
}

type flightKey struct {
	run   int64
	stage string
}

// New returns an engine with defaults filled in.
func New(st *store.Store, home string, executors map[spec.Kind]stage.Executor, baseEnv []string, log *slog.Logger) *Engine {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Engine{
		Store:         st,
		Home:          home,
		Executors:     executors,
		BaseEnv:       baseEnv,
		Now:           time.Now,
		Jitter:        defaultJitter,
		Log:           log,
		Poll:          DefaultPoll,
		inflight:      map[flightKey]context.CancelCauseFunc{},
		liveResources: map[string]bool{},
		running:       map[spec.Kind]int{},
		wake:          make(chan struct{}, 1),
	}
}

func defaultJitter(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	return rand.N(max)
}

func (e *Engine) now() time.Time { return e.Now().UTC() }

func (e *Engine) signal() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

// RunDir is the directory holding a run's work, logs and result files.
func (e *Engine) RunDir(runID int64) string {
	return filepath.Join(e.Home, "runs", fmt.Sprint(runID))
}

// ---- pipelines ----

// AddPipeline validates and registers a spec. With replace it overwrites an
// existing pipeline of the same name (runs already created keep their
// snapshot). With enable it also enables the pipeline.
func (e *Engine) AddPipeline(ctx context.Context, raw []byte, sourceDir string, replace, enable bool) (store.Pipeline, error) {
	sp, err := spec.Load(raw)
	if err != nil {
		return store.Pipeline{}, err
	}
	now := e.now()
	existing, err := e.Store.GetPipeline(ctx, sp.Name)
	switch {
	case errors.Is(err, store.ErrNotFound):
		p := store.Pipeline{Name: sp.Name, SpecYAML: raw, SpecHash: spec.Hash(raw), SourceDir: sourceDir, CreatedAt: now, UpdatedAt: now}
		if enable {
			p.Enabled = true
			p.NextDueAt = firstDue(sp.Schedule, now, e.Jitter)
		}
		return p, e.Store.InsertPipeline(ctx, p)
	case err != nil:
		return store.Pipeline{}, err
	case !replace:
		return store.Pipeline{}, fmt.Errorf("pipeline %q %w (use --replace to update it)", sp.Name, store.ErrExists)
	}
	old, _ := spec.Load(existing.SpecYAML)
	p := existing
	p.SpecYAML, p.SpecHash, p.SourceDir, p.UpdatedAt = raw, spec.Hash(raw), sourceDir, now
	if enable && !p.Enabled {
		p.Enabled = true
		p.NextDueAt = firstDue(sp.Schedule, now, e.Jitter)
	} else if p.Enabled && !sameSchedule(old.Schedule, sp.Schedule) {
		p.NextDueAt = rescheduled(sp.Schedule, now, e.Jitter)
	}
	return p, e.Store.UpdatePipeline(ctx, p)
}

// SetEnabled turns a pipeline's schedule and event intake on or off; a
// disabled pipeline refuses new events and leaves queued ones waiting until
// it is enabled again. Enabling a schedule
// without `at` makes it due at once; with `at` it is due at the next slot.
func (e *Engine) SetEnabled(ctx context.Context, name string, enabled bool) (store.Pipeline, error) {
	p, err := e.Store.GetPipeline(ctx, name)
	if err != nil {
		return p, err
	}
	sp, err := spec.Load(p.SpecYAML)
	if err != nil {
		return p, err
	}
	if p.Enabled == enabled {
		return p, nil
	}
	p.Enabled = enabled
	p.UpdatedAt = e.now()
	p.NextDueAt = nil
	if enabled {
		p.NextDueAt = firstDue(sp.Schedule, p.UpdatedAt, e.Jitter)
	}
	return p, e.Store.UpdatePipeline(ctx, p)
}

// RemovePipeline deletes a pipeline that has no running run. Its runs stay.
func (e *Engine) RemovePipeline(ctx context.Context, name string) error {
	active, err := e.Store.HasActiveRun(ctx, name)
	if err != nil {
		return err
	}
	if active {
		return fmt.Errorf("pipeline %q has a running run; cancel it first", name)
	}
	return e.Store.DeletePipeline(ctx, name)
}

// ---- runs ----

// StartRun creates a run of the pipeline's current spec. It does not execute
// anything; the lock holder's Loop does.
func (e *Engine) StartRun(ctx context.Context, name, trigger string) (int64, error) {
	p, err := e.Store.GetPipeline(ctx, name)
	if err != nil {
		return 0, err
	}
	sp, err := spec.Load(p.SpecYAML)
	if err != nil {
		return 0, fmt.Errorf("stored spec for %q: %w", name, err)
	}
	return e.createRun(ctx, p, sp, trigger)
}

func (e *Engine) createRun(ctx context.Context, p store.Pipeline, sp spec.Spec, trigger string) (int64, error) {
	ids := make([]string, len(sp.Stages))
	for i, st := range sp.Stages {
		ids[i] = st.ID
	}
	id, err := e.Store.CreateRun(ctx, store.Run{
		Pipeline: p.Name, Trigger: trigger, SpecYAML: p.SpecYAML, SpecHash: p.SpecHash,
		SourceDir: p.SourceDir, CreatedAt: e.now(),
	}, ids)
	if err == nil {
		e.Log.Info("run created", "pipeline", p.Name, "run", id, "trigger", trigger)
		e.signal()
	}
	return id, err
}

// Resume re-runs a parked run from a stage: that stage, everything that
// depends on it, and every stage that has not succeeded go back to pending.
// from defaults to the stage the run halted on.
func (e *Engine) Resume(ctx context.Context, runID int64, from string) error {
	run, err := e.Store.GetRun(ctx, runID)
	if err != nil {
		return err
	}
	sp, err := spec.Load(run.SpecYAML)
	if err != nil {
		return fmt.Errorf("run %d spec snapshot: %w", runID, err)
	}
	if from == "" {
		from = run.HaltStage
	}
	if from == "" {
		return fmt.Errorf("run %d has no halted stage; pass --from <stage>", runID)
	}
	if _, ok := sp.Stage(from); !ok {
		return fmt.Errorf("run %d has no stage %q", runID, from)
	}
	stages, err := e.Store.ListStages(ctx, runID)
	if err != nil {
		return err
	}
	reset := map[string]bool{}
	for _, id := range sp.Dependents(from) {
		reset[id] = true
	}
	for _, st := range stages {
		if st.State != store.StageSucceeded {
			reset[st.StageID] = true
		}
	}
	var ids []string
	for _, st := range stages {
		if reset[st.StageID] {
			ids = append(ids, st.StageID)
		}
	}
	if err := e.Store.ResumeRun(ctx, runID, ids); err != nil {
		return err
	}
	e.Log.Info("run resumed", "run", runID, "from", from, "stages", ids)
	e.signal()
	return nil
}

// Cancel asks for a running run to stop. The lock holder kills its running
// stages and marks the run cancelled.
func (e *Engine) Cancel(ctx context.Context, runID int64) error {
	if err := e.Store.RequestCancel(ctx, runID); err != nil {
		return err
	}
	e.signal()
	return nil
}

// ---- execution (lock holder only) ----

// CleanupInterval is how often Loop retries pending cleanups.
const CleanupInterval = time.Minute

// Recover finishes attempts a previous process left running: they become
// interrupted (the next advance parks their run). It then sweeps pending
// cleanups, stopping whatever earlier attempts started (process groups,
// sandboxes). Call it once after taking the lock, before Loop.
func (e *Engine) Recover(ctx context.Context) error {
	attempts, err := e.Store.RunningAttempts(ctx)
	if err != nil {
		return err
	}
	for _, a := range attempts {
		now := e.now()
		a.State = store.AttemptInterrupted
		a.Summary = "interrupted: the runner stopped while this stage ran"
		a.FinishedAt = &now
		if err := e.Store.FinishAttempt(ctx, a); err != nil {
			return err
		}
		e.Log.Warn("recovered interrupted attempt", "run", a.RunID, "stage", a.StageID, "attempt", a.Attempt)
	}
	return e.SweepCleanups(ctx)
}

// SweepCleanups offers every pending cleanup that no running attempt owns to
// every Cleaner, and forgets the ones confirmed gone. Failures stay pending
// and are retried on the next sweep.
func (e *Engine) SweepCleanups(ctx context.Context) error {
	pending, err := e.Store.PendingCleanups(ctx)
	if err != nil {
		return err
	}
	for _, p := range pending {
		e.mu.Lock()
		owned := e.liveResources[p.Resource]
		e.mu.Unlock()
		if owned {
			continue
		}
		done := false
		var errs []error
		for kind, ex := range e.Executors {
			c, ok := ex.(stage.Cleaner)
			if !ok {
				continue
			}
			d, err := c.Cleanup(ctx, p.Resource)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", kind, err))
			}
			done = done || d
		}
		switch {
		case done:
			if err := e.Store.ReleaseResource(ctx, p.Resource); err != nil {
				return err
			}
			e.Log.Info("cleaned up", "resource", p.Resource, "run", p.RunID, "stage", p.StageID)
		case len(errs) > 0:
			msg := errors.Join(errs...).Error()
			e.Log.Error("cleanup failed; will retry", "resource", p.Resource, "run", p.RunID, "stage", p.StageID, "tries", p.Tries+1, "err", msg)
			if err := e.Store.NoteCleanupFailure(ctx, p.Resource, msg); err != nil {
				return err
			}
		}
	}
	return nil
}

// LoopOptions selects what Loop does.
type LoopOptions struct {
	// Schedule fires due schedules.
	Schedule bool
	// Only, when non-nil, limits execution to these runs.
	Only map[int64]bool
	// Events starts runs for queued events (implied by Schedule).
	Events bool
	// Pipeline, when set, limits event starts and advancing to this one
	// pipeline.
	Pipeline string
	// Done, when set, ends the loop as soon as it reports true after an
	// advance pass with nothing of the selected work in flight.
	Done func(context.Context) bool
	// UntilIdle returns once no selected run is running (and, with Events,
	// no enabled pipeline has a queued event left).
	UntilIdle bool
}

// Loop advances runs until ctx is done (or, with UntilIdle, until the
// selected runs have settled). It retries pending cleanups every
// CleanupInterval. When ctx ends, running stages are stopped and their runs
// parked as interrupted.
func (e *Engine) Loop(ctx context.Context, opts LoopOptions) error {
	poll := e.Poll
	if poll <= 0 {
		poll = DefaultPoll
	}
	lastSweep := time.Now()
	for {
		// A caller's goal already met (e.g. its run settled while another
		// process held the lock) ends the loop before it dispatches work.
		if opts.Done != nil && opts.Done(ctx) && e.inflightCount() == 0 {
			return nil
		}
		if opts.Schedule {
			if err := e.schedule(ctx); err != nil && ctx.Err() == nil {
				e.Log.Error("schedule", "err", err)
			}
		}
		if opts.Schedule || opts.Events {
			if err := e.startEvents(ctx, opts.Pipeline, opts.Schedule); err != nil && ctx.Err() == nil {
				e.Log.Error("start events", "err", err)
			}
		}
		busy, err := e.advanceAll(ctx, opts.Only, opts.Pipeline)
		if err != nil && ctx.Err() == nil {
			e.Log.Error("advance", "err", err)
		}
		if !busy && (opts.Schedule || opts.Events) && opts.UntilIdle {
			// A run that just settled may leave events waiting behind it.
			if waiting, qerr := e.eventsWaiting(ctx, opts.Pipeline); qerr == nil && waiting {
				busy = true
			}
			// A schedule that yielded to an older event is still due.
			if due, serr := e.scheduleDue(ctx); opts.Schedule && serr == nil && due {
				busy = true
			}
		}
		if time.Since(lastSweep) >= CleanupInterval || (opts.UntilIdle && !busy) {
			if serr := e.SweepCleanups(ctx); serr != nil && ctx.Err() == nil {
				e.Log.Error("cleanup sweep", "err", serr)
			}
			if rerr := e.applyRetention(ctx); rerr != nil && ctx.Err() == nil {
				e.Log.Error("retention", "err", rerr)
			}
			lastSweep = time.Now()
		}
		if opts.Done != nil && err == nil && opts.Done(ctx) && e.inflightCount() == 0 {
			return nil
		}
		if opts.UntilIdle && !busy && err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			e.stopAll(opts.Only)
			return ctx.Err()
		case <-e.wake:
		case <-time.After(poll):
		}
	}
}

// stopAll interrupts every running stage, waits for them, and folds the
// results so their runs are parked rather than left running.
func (e *Engine) stopAll(only map[int64]bool) {
	e.stopping.Store(true)
	defer e.stopping.Store(false)
	e.mu.Lock()
	for _, cancel := range e.inflight {
		cancel(errStopping)
	}
	e.mu.Unlock()
	e.wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := e.advanceAll(ctx, only, ""); err != nil {
		e.Log.Error("final advance", "err", err)
	}
}

// reserve takes an execution slot for a stage of kind, if its executor's
// limit allows one more. Every successful reserve is paired with unreserve.
func (e *Engine) reserve(kind spec.Kind) bool {
	limit := 0
	if l, ok := e.Executors[kind].(stage.Limiter); ok {
		limit = l.MaxConcurrent()
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if limit > 0 && e.running[kind] >= limit {
		return false
	}
	e.running[kind]++
	return true
}

func (e *Engine) unreserve(kind spec.Kind) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.running[kind]--
}

func (e *Engine) isInflight(k flightKey) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.inflight[k]
	return ok
}

func (e *Engine) inflightCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.inflight)
}

// advanceAll advances every selected running run. busy reports whether any
// selected run is still running afterwards.
func (e *Engine) advanceAll(ctx context.Context, only map[int64]bool, pipeline string) (busy bool, err error) {
	runs, err := e.Store.ActiveRuns(ctx)
	if err != nil {
		return e.inflightCount() > 0, err
	}
	var errs []error
	for _, r := range runs {
		if (only != nil && !only[r.ID]) || (pipeline != "" && r.Pipeline != pipeline) {
			continue
		}
		running, err := e.advance(ctx, r)
		if err != nil {
			errs = append(errs, fmt.Errorf("run %d: %w", r.ID, err))
		}
		busy = busy || running
	}
	return busy || e.inflightCount() > 0, errors.Join(errs...)
}
