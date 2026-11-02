package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gitmoot/stagehand/internal/spec"
	"github.com/gitmoot/stagehand/internal/stage"
	"github.com/gitmoot/stagehand/internal/store"
)

// advance moves one running run forward: fold finished attempts, honour a
// cancel request, start ready stages, and settle the run once nothing runs.
// It reports whether the run is still running.
func (e *Engine) advance(ctx context.Context, run store.Run) (bool, error) {
	sp, err := spec.Load(run.SpecYAML)
	if err != nil {
		return false, e.Store.FinishRun(ctx, run.ID, store.RunFailed, "", "stored spec no longer loads: "+err.Error(), e.now())
	}
	stages, err := e.Store.ListStages(ctx, run.ID)
	if err != nil {
		return true, err
	}
	attempts, err := e.Store.ListAttempts(ctx, run.ID)
	if err != nil {
		return true, err
	}
	latest := map[string]store.Attempt{}
	for _, a := range attempts {
		if cur, ok := latest[a.StageID]; !ok || a.Attempt > cur.Attempt {
			latest[a.StageID] = a
		}
	}

	if run.CancelRequested {
		e.mu.Lock()
		for k, cancel := range e.inflight {
			if k.run == run.ID {
				cancel(errCancelled)
			}
		}
		e.mu.Unlock()
	}

	// Fold attempts that finished.
	for i := range stages {
		st := &stages[i]
		if st.State != store.StageRunning {
			continue
		}
		a, ok := latest[st.StageID]
		if !ok || a.Attempt != st.Attempts {
			if err := e.foldStage(ctx, st, store.StageFailed, "no record of the running attempt", nil); err != nil {
				return true, err
			}
			continue
		}
		if a.State == store.AttemptRunning {
			continue // still executing (or waiting for Recover after a crash)
		}
		stg, _ := sp.Stage(st.StageID)
		var state string
		var needs []string
		switch a.State {
		case store.AttemptSucceeded:
			state = store.StageSucceeded
		case store.AttemptBlocked:
			state = store.StageBlocked
			if a.NeedsJSON != "" {
				_ = json.Unmarshal([]byte(a.NeedsJSON), &needs)
			}
		case store.AttemptCancelled:
			state = store.StageCancelled
		case store.AttemptFailed:
			state = store.StageFailed
			if !run.CancelRequested && st.Attempts-st.RetryBase <= stg.Retry {
				state = store.StagePending
				e.Log.Info("stage will retry", "run", run.ID, "stage", st.StageID, "attempt", st.Attempts, "summary", a.Summary)
			}
		default: // interrupted: never retried automatically
			state = store.StageFailed
		}
		if err := e.foldStage(ctx, st, state, a.Summary, needs); err != nil {
			return true, err
		}
	}

	running := 0
	for _, st := range stages {
		if st.State == store.StageRunning {
			running++
		}
	}

	if run.CancelRequested {
		if running > 0 {
			return true, nil
		}
		for i := range stages {
			if stages[i].State == store.StagePending {
				if err := e.foldStage(ctx, &stages[i], store.StageCancelled, "cancelled before it ran", nil); err != nil {
					return true, err
				}
			}
		}
		e.Log.Info("run cancelled", "run", run.ID)
		return false, e.Store.FinishRun(ctx, run.ID, store.RunCancelled, "", errCancelled.Error(), e.now())
	}

	// Start every pending stage whose needs all succeeded.
	state := map[string]string{}
	for _, st := range stages {
		state[st.StageID] = st.State
	}
	for i := range stages {
		st := &stages[i]
		if st.State != store.StagePending {
			continue
		}
		stg, ok := sp.Stage(st.StageID)
		if !ok {
			if err := e.foldStage(ctx, st, store.StageFailed, "stage is not in the run's spec", nil); err != nil {
				return true, err
			}
			continue
		}
		ready := true
		for _, n := range stg.Needs {
			if state[n] != store.StageSucceeded {
				ready = false
				break
			}
		}
		if !ready {
			continue
		}
		if err := e.start(ctx, run, sp, stg, st); err != nil {
			return true, err
		}
		state[st.StageID] = store.StageRunning
		running++
	}
	if running > 0 {
		return true, nil
	}

	// Nothing runs and nothing can start: settle.
	allOK := true
	for _, st := range stages {
		if st.State != store.StageSucceeded {
			allOK = false
		}
	}
	if allOK {
		e.Log.Info("run succeeded", "run", run.ID)
		return false, e.Store.FinishRun(ctx, run.ID, store.RunSucceeded, "", "", e.now())
	}
	final := store.RunBlocked
	var haltStage, haltReason string
	for _, st := range stages {
		switch st.State {
		case store.StageFailed, store.StageCancelled:
			final = store.RunFailed
		}
	}
	for _, st := range stages {
		if st.State == store.StageFailed || st.State == store.StageBlocked || st.State == store.StageCancelled {
			haltStage, haltReason = st.StageID, st.State+": "+st.Summary
			break
		}
	}
	for i := range stages {
		if stages[i].State == store.StagePending {
			if err := e.foldStage(ctx, &stages[i], store.StageSkipped, "not run: an upstream stage did not succeed", nil); err != nil {
				return true, err
			}
		}
	}
	e.Log.Warn("run parked", "run", run.ID, "state", final, "stage", haltStage, "reason", haltReason)
	return false, e.Store.FinishRun(ctx, run.ID, final, haltStage, strings.TrimSuffix(haltReason, ": "), e.now())
}

func (e *Engine) foldStage(ctx context.Context, st *store.RunStage, state, summary string, needs []string) error {
	st.State = state
	st.Summary = summary
	st.NeedsJSON = ""
	if len(needs) > 0 {
		raw, _ := json.Marshal(needs)
		st.NeedsJSON = string(raw)
	}
	if state == store.StagePending {
		st.FinishedAt = nil
	} else {
		now := e.now()
		st.FinishedAt = &now
	}
	return e.Store.UpdateStage(ctx, *st)
}

// start records a new attempt for st and launches it.
func (e *Engine) start(ctx context.Context, run store.Run, sp spec.Spec, stg spec.Stage, st *store.RunStage) error {
	now := e.now()
	st.Attempts++
	if st.StartedAt == nil {
		st.StartedAt = &now
	}
	runDir := e.RunDir(run.ID)
	workDir := filepath.Join(runDir, "work")
	logPath := filepath.Join(runDir, "logs", fmt.Sprintf("%s.%d.log", stg.ID, st.Attempts))
	resultPath := filepath.Join(runDir, "results", fmt.Sprintf("%s.%d.json", stg.ID, st.Attempts))
	a := store.Attempt{RunID: run.ID, StageID: stg.ID, Attempt: st.Attempts, State: store.AttemptRunning, LogPath: logPath, StartedAt: now}
	if err := e.Store.StartAttempt(ctx, *st, a); err != nil {
		st.Attempts--
		return err
	}
	st.State = store.StageRunning

	fail := func(summary string) error {
		end := e.now()
		a.State, a.Summary, a.FinishedAt = store.AttemptFailed, summary, &end
		e.Log.Error("stage could not start", "run", run.ID, "stage", stg.ID, "err", summary)
		err := e.Store.FinishAttempt(ctx, a)
		e.signal()
		return err
	}
	for _, d := range []string{workDir, filepath.Dir(logPath), filepath.Dir(resultPath)} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return fail("create run directory: " + err.Error())
		}
	}
	ex := e.Executors[stg.Kind()]
	if ex == nil {
		return fail(fmt.Sprintf("no %s executor is configured on this runner", stg.Kind()))
	}
	var env []string
	if stg.Kind() == spec.KindShell {
		var err error
		if env, err = e.stageEnv(run, sp, stg, st.Attempts, workDir, resultPath); err != nil {
			return fail(err.Error())
		}
	}
	job := stage.Job{
		RunID: run.ID, Pipeline: run.Pipeline, Stage: stg, Attempt: st.Attempts,
		WorkDir: workDir, LogPath: logPath, ResultPath: resultPath, SourceDir: run.SourceDir, Env: env,
		Track: func(resource string) error {
			return e.Store.SetAttemptResource(context.Background(), run.ID, stg.ID, a.Attempt, resource)
		},
	}

	timeout := stg.TimeoutDuration()
	base, cancel := context.WithCancelCause(context.Background())
	jctx, stopTimer := context.WithTimeoutCause(base, timeout, fmt.Errorf("timed out after %s", timeout))
	key := flightKey{run.ID, stg.ID}
	e.mu.Lock()
	e.inflight[key] = cancel
	e.mu.Unlock()
	e.wg.Add(1)
	e.Log.Info("stage started", "run", run.ID, "stage", stg.ID, "attempt", a.Attempt, "kind", stg.Kind())

	go func() {
		defer e.wg.Done()
		res := ex.Execute(jctx, job)
		cause := context.Cause(jctx)
		stopTimer()
		cancel(nil)

		end := e.now()
		a.FinishedAt = &end
		a.ExitCode = res.ExitCode
		a.Cost, a.InputTokens, a.OutputTokens = res.Cost, res.InputTokens, res.OutputTokens
		a.Summary = res.Summary
		switch {
		case errors.Is(cause, errCancelled):
			a.State, a.Summary = store.AttemptCancelled, errCancelled.Error()
		case errors.Is(cause, errStopping):
			a.State, a.Summary = store.AttemptInterrupted, errStopping.Error()
		case res.Outcome == stage.Succeeded:
			a.State = store.AttemptSucceeded
		case res.Outcome == stage.Blocked:
			a.State = store.AttemptBlocked
			if len(res.Needs) > 0 {
				raw, _ := json.Marshal(res.Needs)
				a.NeedsJSON = string(raw)
			}
		default:
			a.State = store.AttemptFailed
			if a.Summary == "" {
				a.Summary = "failed"
			}
		}
		if err := e.Store.FinishAttempt(context.Background(), a); err != nil {
			e.Log.Error("record attempt", "run", run.ID, "stage", stg.ID, "err", err)
		}
		e.Log.Info("stage finished", "run", run.ID, "stage", stg.ID, "attempt", a.Attempt, "state", a.State,
			"exit", exitString(a.ExitCode), "cost", a.Cost, "summary", firstLine(a.Summary))

		e.mu.Lock()
		delete(e.inflight, key)
		e.mu.Unlock()
		e.signal()
	}()
	return nil
}

func exitString(code *int) string {
	if code == nil {
		return "-"
	}
	return fmt.Sprint(*code)
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(s, "\n")
	return line
}

// stageEnv builds a shell stage's complete environment: BaseEnv, the
// STAGEHAND_* variables, then each env_keys entry from env_file (preferred) or
// the spec's env defaults. A listed key found in neither is an error.
func (e *Engine) stageEnv(run store.Run, sp spec.Spec, stg spec.Stage, attempt int, workDir, resultPath string) ([]string, error) {
	vars := map[string]string{}
	var order []string
	set := func(k, v string) {
		if _, ok := vars[k]; !ok {
			order = append(order, k)
		}
		vars[k] = v
	}
	for _, kv := range e.BaseEnv {
		if k, v, ok := strings.Cut(kv, "="); ok {
			set(k, v)
		}
	}
	set("STAGEHAND_PIPELINE", run.Pipeline)
	set("STAGEHAND_RUN_ID", fmt.Sprint(run.ID))
	set("STAGEHAND_STAGE", stg.ID)
	set("STAGEHAND_ATTEMPT", fmt.Sprint(attempt))
	set("STAGEHAND_WORK", workDir)
	set("STAGEHAND_RESULT", resultPath)
	if len(stg.EnvKeys) > 0 {
		var secrets map[string]string
		if sp.EnvFile != "" {
			var err error
			if secrets, err = ReadEnvFile(sp.EnvFile); err != nil {
				return nil, err
			}
		}
		for _, k := range stg.EnvKeys {
			if v, ok := secrets[k]; ok {
				set(k, v)
			} else if v, ok := sp.Env[k]; ok {
				set(k, v)
			} else {
				return nil, fmt.Errorf("env key %s is in neither env_file nor env", k)
			}
		}
	}
	out := make([]string, 0, len(order))
	for _, k := range order {
		out = append(out, k+"="+vars[k])
	}
	return out, nil
}

// ---- schedule ----

func (e *Engine) schedule(ctx context.Context) error {
	pipelines, err := e.Store.ListPipelines(ctx)
	if err != nil {
		return err
	}
	now := e.now()
	var errs []error
	for _, p := range pipelines {
		if !p.Enabled || p.NextDueAt == nil || now.Before(*p.NextDueAt) {
			continue
		}
		sp, err := spec.Load(p.SpecYAML)
		if err != nil || sp.Schedule == nil {
			continue
		}
		active, err := e.Store.HasActiveRun(ctx, p.Name)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if active {
			continue // fire as soon as the active run settles
		}
		if _, err := e.createRun(ctx, p, sp, "schedule"); err != nil && !errors.Is(err, store.ErrActiveRun) {
			errs = append(errs, err)
			continue
		}
		next := NextDue(*sp.Schedule, now, e.Jitter)
		if err := e.Store.SetNextDue(ctx, p.Name, &next); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// NextDue is the due time after a run fired at now: now+interval without
// `at`, else the first anchored slot after now; plus jitter.
func NextDue(sc spec.Schedule, now time.Time, jitter func(time.Duration) time.Duration) time.Time {
	interval, j, at := sc.Durations()
	base := now.Add(interval)
	if !at.IsZero() {
		base = nextSlot(at, interval, now)
	}
	return base.Add(jitter(j)).UTC()
}

// nextSlot is the first at+k*interval strictly after now (at itself if now is
// before it).
func nextSlot(at time.Time, interval time.Duration, now time.Time) time.Time {
	if now.Before(at) {
		return at
	}
	k := now.Sub(at)/interval + 1
	return at.Add(k * interval)
}

// firstDue is the due time when a pipeline is enabled: now without `at` (run
// at once, as Gitmoot does), else the next slot; nil without a schedule.
func firstDue(sc *spec.Schedule, now time.Time, jitter func(time.Duration) time.Duration) *time.Time {
	if sc == nil {
		return nil
	}
	_, j, at := sc.Durations()
	t := now
	if !at.IsZero() {
		t = nextSlot(at, mustInterval(sc), now).Add(jitter(j))
	}
	t = t.UTC()
	return &t
}

// rescheduled is the due time after an enabled pipeline's schedule changed:
// the next interval or slot, never at once.
func rescheduled(sc *spec.Schedule, now time.Time, jitter func(time.Duration) time.Duration) *time.Time {
	if sc == nil {
		return nil
	}
	t := NextDue(*sc, now, jitter)
	return &t
}

func mustInterval(sc *spec.Schedule) time.Duration {
	interval, _, _ := sc.Durations()
	return interval
}

func sameSchedule(a, b *spec.Schedule) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
