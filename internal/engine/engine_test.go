package engine

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gitmoot/stagehand/internal/spec"
	"github.com/gitmoot/stagehand/internal/stage"
	"github.com/gitmoot/stagehand/internal/store"
)

// fakeExec runs stages by id: each call pops the next scripted behaviour for
// the stage (the last one repeats). A behaviour of "hang" waits for ctx.
type fakeExec struct {
	mu      sync.Mutex
	script  map[string][]string
	calls   []string
	cleaned []string
	started chan string
}

func (f *fakeExec) Execute(ctx context.Context, job stage.Job) stage.Result {
	f.mu.Lock()
	id := job.Stage.ID
	f.calls = append(f.calls, id)
	steps := f.script[id]
	step := "ok"
	if len(steps) > 0 {
		step = steps[0]
		if len(steps) > 1 {
			f.script[id] = steps[1:]
		}
	}
	f.mu.Unlock()
	if job.Track != nil {
		_ = job.Track("fake:" + id)
	}
	if f.started != nil {
		f.started <- id
	}
	code := 0
	switch step {
	case "ok":
		return stage.Result{Outcome: stage.Succeeded, ExitCode: &code, Cost: 0.5}
	case "fail":
		code = 1
		return stage.Result{Outcome: stage.Failed, ExitCode: &code, Summary: "boom"}
	case "blocked":
		return stage.Result{Outcome: stage.Blocked, ExitCode: &code, Summary: "needs input", Needs: []string{"owner decision"}}
	case "hang":
		<-ctx.Done()
		return stage.Result{Outcome: stage.Failed, Summary: "stopped"}
	}
	panic("unknown step " + step)
}

func (f *fakeExec) Cleanup(_ context.Context, resource string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cleaned = append(f.cleaned, resource)
	return nil
}

func (f *fakeExec) callCount(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c == id {
			n++
		}
	}
	return n
}

type harness struct {
	t   *testing.T
	eng *Engine
	ex  *fakeExec
	now time.Time
}

func newHarness(t *testing.T, script map[string][]string) *harness {
	t.Helper()
	home := t.TempDir()
	st, err := store.Open(context.Background(), filepath.Join(home, "stagehand.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	ex := &fakeExec{script: script}
	h := &harness{t: t, ex: ex, now: time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)}
	h.eng = New(st, home, map[spec.Kind]stage.Executor{spec.KindShell: ex}, nil, nil)
	h.eng.Now = func() time.Time { return h.now }
	h.eng.Jitter = func(max time.Duration) time.Duration { return max / 2 }
	h.eng.Poll = 10 * time.Millisecond
	return h
}

func (h *harness) add(yaml string, enable bool) {
	h.t.Helper()
	if _, err := h.eng.AddPipeline(context.Background(), []byte(yaml), "/src", true, enable); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) runToEnd(id int64) RunDetail {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.eng.Loop(ctx, LoopOptions{Only: map[int64]bool{id: true}, UntilIdle: true}); err != nil {
		h.t.Fatal(err)
	}
	d, err := h.eng.Run(context.Background(), id)
	if err != nil {
		h.t.Fatal(err)
	}
	return d
}

func (h *harness) start(name string) int64 {
	h.t.Helper()
	id, err := h.eng.StartRun(context.Background(), name, "manual")
	if err != nil {
		h.t.Fatal(err)
	}
	return id
}

func states(d RunDetail) map[string]string {
	out := map[string]string{}
	for _, s := range d.Stages {
		out[s.ID] = s.State
	}
	return out
}

func assertStates(t *testing.T, d RunDetail, want map[string]string) {
	t.Helper()
	got := states(d)
	for id, w := range want {
		if got[id] != w {
			t.Errorf("stage %s: state %q, want %q (all: %v)", id, got[id], w, got)
		}
	}
}

const diamond = `
name: p
stages:
  - {id: a, cmd: x}
  - {id: b, cmd: x, needs: [a]}
  - {id: c, cmd: x, needs: [a]}
  - {id: d, cmd: x, needs: [b, c]}
  - {id: e, cmd: x}
`

func TestRunSucceedsWhenEveryStageSucceeds(t *testing.T) {
	h := newHarness(t, nil)
	h.add(diamond, false)
	d := h.runToEnd(h.start("p"))
	if d.State != store.RunSucceeded {
		t.Fatalf("run state %s, want succeeded", d.State)
	}
	if d.Cost != 2.5 {
		t.Errorf("run cost %v, want 2.5 (5 stages x 0.5)", d.Cost)
	}
	// d must run after both b and c.
	calls := h.ex.calls
	if slices.Index(calls, "d") < max(slices.Index(calls, "b"), slices.Index(calls, "c")) {
		t.Errorf("d ran before its needs: %v", calls)
	}
}

func TestFailedStageParksRunAndSkipsOnlyDependents(t *testing.T) {
	h := newHarness(t, map[string][]string{"b": {"fail"}})
	h.add(diamond, false)
	d := h.runToEnd(h.start("p"))
	if d.State != store.RunFailed || d.HaltStage != "b" {
		t.Fatalf("run %s halted at %q, want failed at b", d.State, d.HaltStage)
	}
	assertStates(t, d, map[string]string{"a": "succeeded", "b": "failed", "c": "succeeded", "d": "skipped", "e": "succeeded"})
	if h.ex.callCount("d") != 0 {
		t.Error("d ran although b failed")
	}
}

func TestRetryReRunsFailedStageWithinBudget(t *testing.T) {
	h := newHarness(t, map[string][]string{"a": {"fail", "fail", "ok"}})
	h.add("name: p\nstages:\n  - {id: a, cmd: x, retry: 2}\n  - {id: b, cmd: x, needs: [a]}\n", false)
	d := h.runToEnd(h.start("p"))
	if d.State != store.RunSucceeded {
		t.Fatalf("run %s, want succeeded after retries", d.State)
	}
	if n := len(d.Stages[0].Attempts); n != 3 {
		t.Errorf("a has %d attempts, want 3", n)
	}

	h2 := newHarness(t, map[string][]string{"a": {"fail"}})
	h2.add("name: p\nstages:\n  - {id: a, cmd: x, retry: 1}\n", false)
	d2 := h2.runToEnd(h2.start("p"))
	if d2.State != store.RunFailed || len(d2.Stages[0].Attempts) != 2 {
		t.Fatalf("exhausted retry: run %s with %d attempts, want failed with 2", d2.State, len(d2.Stages[0].Attempts))
	}
}

func TestResumeGetsAFreshRetryBudget(t *testing.T) {
	h := newHarness(t, map[string][]string{"a": {"fail", "fail", "fail", "ok"}})
	h.add("name: p\nstages:\n  - {id: a, cmd: x, retry: 1}\n", false)
	id := h.start("p")
	if d := h.runToEnd(id); d.State != store.RunFailed {
		t.Fatalf("first pass %s, want failed", d.State)
	}
	if err := h.eng.Resume(context.Background(), id, ""); err != nil {
		t.Fatal(err)
	}
	d := h.runToEnd(id)
	// attempt 3 fails, attempt 4 (the resume's one retry) succeeds.
	if d.State != store.RunSucceeded || len(d.Stages[0].Attempts) != 4 {
		t.Fatalf("after resume: %s with %d attempts, want succeeded with 4", d.State, len(d.Stages[0].Attempts))
	}
}

func TestBlockedStageParksRunWithNeeds(t *testing.T) {
	h := newHarness(t, map[string][]string{"b": {"blocked", "ok"}})
	h.add(diamond, false)
	id := h.start("p")
	d := h.runToEnd(id)
	if d.State != store.RunBlocked || d.HaltStage != "b" {
		t.Fatalf("run %s at %q, want blocked at b", d.State, d.HaltStage)
	}
	for _, s := range d.Stages {
		if s.ID == "b" && !slices.Equal(s.Needs, []string{"owner decision"}) {
			t.Errorf("needs %v", s.Needs)
		}
	}
	if err := h.eng.Resume(context.Background(), id, ""); err != nil {
		t.Fatal(err)
	}
	d = h.runToEnd(id)
	if d.State != store.RunSucceeded {
		t.Fatalf("after resume %s", d.State)
	}
	if h.ex.callCount("a") != 1 || h.ex.callCount("c") != 1 || h.ex.callCount("b") != 2 || h.ex.callCount("d") != 1 {
		t.Errorf("resume re-ran succeeded stages: %v", h.ex.calls)
	}
}

func TestResumeFromEarlierStageRerunsItsDependentsOnly(t *testing.T) {
	h := newHarness(t, map[string][]string{"d": {"fail", "ok"}})
	h.add(diamond, false)
	id := h.start("p")
	h.runToEnd(id)
	if err := h.eng.Resume(context.Background(), id, "b"); err != nil {
		t.Fatal(err)
	}
	if d := h.runToEnd(id); d.State != store.RunSucceeded {
		t.Fatalf("after resume %s", d.State)
	}
	want := map[string]int{"a": 1, "b": 2, "c": 1, "d": 2, "e": 1}
	for id, n := range want {
		if got := h.ex.callCount(id); got != n {
			t.Errorf("stage %s ran %d times, want %d", id, got, n)
		}
	}
}

func TestResumeRefusesRunningOrSucceededRun(t *testing.T) {
	h := newHarness(t, nil)
	h.add(diamond, false)
	id := h.start("p")
	if err := h.eng.Resume(context.Background(), id, "a"); err == nil {
		t.Error("resumed a running run")
	}
	h.runToEnd(id)
	if err := h.eng.Resume(context.Background(), id, "a"); err == nil {
		t.Error("resumed a succeeded run")
	}
}

func TestOneActiveRunPerPipeline(t *testing.T) {
	h := newHarness(t, nil)
	h.add(diamond, false)
	h.start("p")
	if _, err := h.eng.StartRun(context.Background(), "p", "manual"); !errors.Is(err, store.ErrActiveRun) {
		t.Fatalf("second run: %v, want ErrActiveRun", err)
	}
}

func TestCancelStopsRunningStagesAndPendingOnes(t *testing.T) {
	h := newHarness(t, map[string][]string{"a": {"hang"}})
	h.ex.started = make(chan string, 10)
	h.add("name: p\nstages:\n  - {id: a, cmd: x}\n  - {id: b, cmd: x, needs: [a]}\n", false)
	id := h.start("p")
	done := make(chan RunDetail)
	go func() { done <- h.runToEnd(id) }()
	<-h.ex.started
	if err := h.eng.Cancel(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	d := <-done
	if d.State != store.RunCancelled {
		t.Fatalf("run %s, want cancelled", d.State)
	}
	assertStates(t, d, map[string]string{"a": "cancelled", "b": "cancelled"})
}

func TestTimeoutFailsStage(t *testing.T) {
	h := newHarness(t, map[string][]string{"a": {"hang"}})
	h.add("name: p\nstages:\n  - {id: a, cmd: x, timeout: 50ms}\n", false)
	d := h.runToEnd(h.start("p"))
	if d.State != store.RunFailed {
		t.Fatalf("run %s, want failed", d.State)
	}
}

func TestStoppingRunnerParksRunAsInterrupted(t *testing.T) {
	h := newHarness(t, map[string][]string{"a": {"hang"}})
	h.ex.started = make(chan string, 10)
	h.add("name: p\nstages:\n  - {id: a, cmd: x, retry: 3}\n", false)
	id := h.start("p")
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error)
	go func() { errc <- h.eng.Loop(ctx, LoopOptions{}) }()
	<-h.ex.started
	cancel()
	<-errc
	d, _ := h.eng.Run(context.Background(), id)
	if d.State != store.RunFailed || !strings.Contains(d.HaltReason, "interrupted") {
		t.Fatalf("run %s (%s), want failed as interrupted", d.State, d.HaltReason)
	}
	if h.ex.callCount("a") != 1 {
		t.Errorf("interrupted stage was retried: %d calls", h.ex.callCount("a"))
	}
}

func TestRecoverParksCrashedAttemptAndCleansItsResource(t *testing.T) {
	h := newHarness(t, nil)
	h.add("name: p\nstages:\n  - {id: a, cmd: x, retry: 3}\n  - {id: b, cmd: x, needs: [a]}\n", false)
	id := h.start("p")
	ctx := context.Background()
	// Simulate a process that started attempt 1 and crashed.
	stages, _ := h.eng.Store.ListStages(ctx, id)
	st := stages[0]
	st.Attempts = 1
	now := h.now
	st.StartedAt = &now
	if err := h.eng.Store.StartAttempt(ctx, st, store.Attempt{RunID: id, StageID: "a", Attempt: 1, LogPath: "x", StartedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := h.eng.Store.SetAttemptResource(ctx, id, "a", 1, "fake:crashed"); err != nil {
		t.Fatal(err)
	}
	if err := h.eng.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(h.ex.cleaned, []string{"fake:crashed"}) {
		t.Errorf("cleaned %v, want [fake:crashed]", h.ex.cleaned)
	}
	d := h.runToEnd(id)
	if d.State != store.RunFailed || d.HaltStage != "a" || !strings.Contains(d.HaltReason, "interrupted") {
		t.Fatalf("run %s at %s (%s), want failed at a as interrupted", d.State, d.HaltStage, d.HaltReason)
	}
	if h.ex.callCount("a") != 0 {
		t.Error("crashed attempt was retried automatically")
	}
}

func TestRunKeepsItsSpecSnapshot(t *testing.T) {
	h := newHarness(t, nil)
	h.add("name: p\nstages:\n  - {id: a, cmd: x}\n", false)
	id := h.start("p")
	h.add("name: p\nstages:\n  - {id: z, cmd: x}\n", false)
	d := h.runToEnd(id)
	if d.State != store.RunSucceeded || len(d.Stages) != 1 || d.Stages[0].ID != "a" {
		t.Fatalf("run used the new spec: %+v", d.Stages)
	}
}

func TestScheduleFiresWhenDueAndNotWhileActive(t *testing.T) {
	h := newHarness(t, map[string][]string{"a": {"hang"}})
	h.add("name: p\nschedule: {interval: 1h, jitter: 10m}\nstages:\n  - {id: a, cmd: x}\n", true)
	ctx := context.Background()
	if err := h.eng.schedule(ctx); err != nil {
		t.Fatal(err)
	}
	runs, _ := h.eng.Runs(ctx, "p", 10)
	if len(runs) != 1 {
		t.Fatalf("%d runs after first due tick, want 1", len(runs))
	}
	p, _ := h.eng.Store.GetPipeline(ctx, "p")
	if want := h.now.Add(time.Hour + 5*time.Minute); !p.NextDueAt.Equal(want) {
		t.Errorf("next due %v, want %v", p.NextDueAt, want)
	}
	// Due again, but the first run is still running: no new run, due time
	// unchanged so it fires as soon as the run settles.
	h.now = h.now.Add(2 * time.Hour)
	if err := h.eng.schedule(ctx); err != nil {
		t.Fatal(err)
	}
	runs, _ = h.eng.Runs(ctx, "p", 10)
	if len(runs) != 1 {
		t.Fatalf("%d runs while one is active, want 1", len(runs))
	}
	if err := h.eng.Store.FinishRun(ctx, runs[0].ID, store.RunFailed, "a", "x", h.now); err != nil {
		t.Fatal(err)
	}
	if err := h.eng.schedule(ctx); err != nil {
		t.Fatal(err)
	}
	runs, _ = h.eng.Runs(ctx, "p", 10)
	if len(runs) != 2 {
		t.Fatalf("%d runs after the active one settled, want 2 (missed slots collapse into one)", len(runs))
	}
}

func TestDisabledPipelineNeverFires(t *testing.T) {
	h := newHarness(t, nil)
	h.add("name: p\nschedule: {interval: 1h}\nstages:\n  - {id: a, cmd: x}\n", false)
	h.now = h.now.Add(48 * time.Hour)
	if err := h.eng.schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if runs, _ := h.eng.Runs(context.Background(), "p", 10); len(runs) != 0 {
		t.Fatalf("disabled pipeline fired %d runs", len(runs))
	}
}

func TestAnchoredScheduleUsesSlots(t *testing.T) {
	sc := spec.Schedule{Interval: "168h", At: "2026-11-08T03:00:00+01:00"}
	noJitter := func(time.Duration) time.Duration { return 0 }
	at, _ := time.Parse(time.RFC3339, sc.At)
	cases := []struct {
		now  time.Time
		want time.Time
	}{
		{at.Add(-time.Hour), at},
		{at, at.Add(168 * time.Hour)},
		{at.Add(200 * time.Hour), at.Add(336 * time.Hour)},
	}
	for _, c := range cases {
		if got := NextDue(sc, c.now, noJitter); !got.Equal(c.want) {
			t.Errorf("NextDue(%v) = %v, want %v", c.now, got, c.want)
		}
	}
	// Enabling an anchored schedule waits for the next slot instead of firing.
	got := firstDue(&sc, at.Add(-time.Hour), noJitter)
	if !got.Equal(at) {
		t.Errorf("firstDue = %v, want %v", got, at)
	}
}

func TestShellStageEnvIsExact(t *testing.T) {
	dir := t.TempDir()
	envFile := filepath.Join(dir, "secrets.env")
	writeFile(t, envFile, "DB_URL=postgres://x\nUNLISTED=leak\n", 0o600)
	sp, err := spec.Load([]byte(fmt.Sprintf("name: p\nenv_file: %s\nenv: {LIMIT: '50', OTHER: nope}\nstages:\n  - {id: a, cmd: x, env_keys: [DB_URL, LIMIT]}\n", envFile)))
	if err != nil {
		t.Fatal(err)
	}
	e := &Engine{BaseEnv: []string{"PATH=/bin"}}
	env, err := e.stageEnv(store.Run{ID: 7, Pipeline: "p"}, sp, sp.Stages[0], 1, "/w", "/r.json")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, kv := range env {
		k, v, _ := strings.Cut(kv, "=")
		got[k] = v
	}
	for _, k := range []string{"UNLISTED", "OTHER"} {
		if _, ok := got[k]; ok {
			t.Errorf("%s leaked into the stage environment", k)
		}
	}
	if got["DB_URL"] != "postgres://x" || got["LIMIT"] != "50" || got["PATH"] != "/bin" || got["STAGEHAND_WORK"] != "/w" || got["STAGEHAND_RUN_ID"] != "7" {
		t.Errorf("env %v", got)
	}

	sp.Stages[0].EnvKeys = []string{"MISSING"}
	if _, err := e.stageEnv(store.Run{}, sp, sp.Stages[0], 1, "/w", "/r"); err == nil {
		t.Error("missing env key did not fail")
	}
}

func TestEnvFileMustBePrivate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "s.env")
	writeFile(t, path, "A=1\n", 0o644)
	if _, err := ReadEnvFile(path); err == nil {
		t.Error("world-readable env_file accepted")
	}
}
