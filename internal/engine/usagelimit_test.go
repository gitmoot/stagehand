package engine

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gitmoot/stagehand/internal/spec"
	"github.com/gitmoot/stagehand/internal/store"
)

// A limit's length is what the provider asked for, not the time left when
// the result is folded: a 5h limit folded with a minute to go (the runner
// was down meanwhile) still skips the run instead of waiting and retrying.
func TestDelayedFoldOfALongLimitStillSkips(t *testing.T) {
	h := newLimitHarness(t, nil)
	h.add("name: p\nstages:\n"+agentStage("r", ""), false)
	ctx := context.Background()
	id := h.start("p")
	stages, err := h.eng.Store.ListStages(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	st := stages[0]
	st.Attempts = 1
	finished := h.now
	st.StartedAt = &finished
	until := finished.Add(5 * time.Hour)
	a := store.Attempt{RunID: id, StageID: "r", Attempt: 1, LogPath: "x", StartedAt: finished}
	if err := h.eng.Store.StartAttempt(ctx, st, a); err != nil {
		t.Fatal(err)
	}
	a.State, a.FinishedAt, a.LimitedUntil = store.AttemptLimited, &finished, &until
	if err := h.eng.Store.FinishAttempt(ctx, a); err != nil {
		t.Fatal(err)
	}
	h.now = until.Add(-time.Minute)
	d := h.runToEnd(id)
	if d.State != store.RunSkipped || !sameTime(d.LimitedUntil, until) {
		t.Fatalf("run %s until %v; want skipped until %v", d.State, d.LimitedUntil, until)
	}
	if n := h.ex.callCount("r"); n != 0 {
		t.Errorf("r was retried %d times after a 5h limit", n)
	}
}

// The one short wait belongs to an execution, not to the run's history: a
// stage whose run was cancelled during a wait and then resumed waits out a
// fresh short limit again instead of skipping the run.
func TestResumedStageWaitsOutAFreshShortLimit(t *testing.T) {
	h := newLimitHarness(t, map[string][]string{"r": {"limit-short", "limit-short", "ok"}})
	h.add("name: p\nstages:\n"+agentStage("r", ""), false)
	ctx := context.Background()
	id := h.start("p")
	h.untilWaiting(id, "r")
	if err := h.eng.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	if d := h.runToEnd(id); d.State != store.RunCancelled {
		t.Fatalf("run %s, want cancelled", d.State)
	}
	if err := h.eng.Resume(ctx, id, "r"); err != nil {
		t.Fatal(err)
	}
	h.untilWaiting(id, "r")
	h.now = h.now.Add(time.Minute)
	d := h.runToEnd(id)
	if n := len(stageOf(t, d, "r").Attempts); d.State != store.RunSucceeded || n != 3 {
		t.Fatalf("run %s after %d attempts; want succeeded on attempt 3", d.State, n)
	}
}

// A skipped slot and its next due time are one write. While the next due
// time cannot be written, polls record nothing; once it can, the slot is
// recorded exactly once.
func TestSkippedSlotIsRecordedOnceWhenNextDueFails(t *testing.T) {
	h := newLimitHarness(t, map[string][]string{"r": {"limit-long"}})
	h.add("name: p\nschedule: {interval: 1h}\nstages:\n"+agentStage("r", ""), true)
	ctx := context.Background()
	if err := h.eng.schedule(ctx); err != nil {
		t.Fatal(err)
	}
	runs, _ := h.eng.Runs(ctx, "p", 10)
	if len(runs) != 1 {
		t.Fatalf("%d runs after the first slot", len(runs))
	}
	if d := h.runToEnd(runs[0].ID); d.State != store.RunSkipped {
		t.Fatalf("first slot %s, want skipped", d.State)
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(h.eng.Home, "stagehand.db")+"?_pragma=busy_timeout(15000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TRIGGER fail_next_due BEFORE UPDATE OF next_due_at ON pipelines BEGIN SELECT RAISE(ABORT, 'injected'); END`); err != nil {
		t.Fatal(err)
	}
	h.now = h.now.Add(time.Hour)
	for range 2 {
		if err := h.eng.schedule(ctx); err == nil {
			t.Fatal("schedule hid the failed next due write")
		}
	}
	if runs, _ := h.eng.Runs(ctx, "p", 10); len(runs) != 1 {
		t.Fatalf("%d runs while the next due time could not be written; want the first only", len(runs))
	}
	if _, err := db.Exec(`DROP TRIGGER fail_next_due`); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := h.eng.schedule(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if runs, _ := h.eng.Runs(ctx, "p", 10); len(runs) != 2 || runs[0].State != store.RunSkipped {
		t.Fatalf("runs %+v; want the slot recorded once, skipped", runs)
	}
}

// newLimitHarness is a harness whose fake also runs agent stages.
func newLimitHarness(t *testing.T, script map[string][]string) *harness {
	t.Helper()
	h := newHarness(t, script)
	h.eng.Executors[spec.KindAgent] = h.ex
	return h
}

// agentStage is a spec line for an agent stage; extra adds keys.
func agentStage(id, extra string) string {
	return "  - {id: " + id + ", agent: omp, prompt: x, output: o.json, max_cost: 1, timeout: 1m" + extra + "}\n"
}

// untilWaiting executes run id until stageID waits out a usage limit.
func (h *harness) untilWaiting(id int64, stageID string) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	waiting := func(ctx context.Context) bool {
		stages, err := h.eng.Store.ListStages(ctx, id)
		if err != nil {
			return false
		}
		for _, st := range stages {
			if st.StageID == stageID && st.State == store.StagePending && st.LimitedUntil != nil {
				return true
			}
		}
		return false
	}
	if err := h.eng.Loop(ctx, LoopOptions{Only: map[int64]bool{id: true}, Done: waiting}); err != nil {
		h.t.Fatalf("stage %s never waited out a usage limit: %v", stageID, err)
	}
	h.eng.wg.Wait()
}

func (f *fakeExec) callTotal() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func stageOf(t *testing.T, d RunDetail, id string) StageDetail {
	t.Helper()
	for _, s := range d.Stages {
		if s.ID == id {
			return s
		}
	}
	t.Fatalf("run %d has no stage %s", d.ID, id)
	return StageDetail{}
}

func sameTime(got *time.Time, want time.Time) bool { return got != nil && got.Equal(want) }

func limitReason(until time.Time) string {
	return "usage limit until " + until.UTC().Format(time.RFC3339)
}

// A long usage limit on a manual run ends it skipped, not failed: the stage
// and its dependents are skipped, the halt reason names the limit's end, the
// stage's retry budget is not used, and the run cannot be resumed.
func TestLongUsageLimitSkipsTheRun(t *testing.T) {
	h := newLimitHarness(t, map[string][]string{"r": {"limit-long"}})
	h.add("name: p\nstages:\n  - {id: a, cmd: x}\n"+agentStage("r", ", needs: [a], retry: 2")+
		"  - {id: d, cmd: x, needs: [r]}\n  - {id: e, cmd: x}\n", false)
	id := h.start("p")
	d := h.runToEnd(id)
	until := h.now.Add(5 * time.Hour)
	reason := limitReason(until)
	if d.State != store.RunSkipped || d.HaltStage != "r" || d.HaltReason != reason {
		t.Fatalf("run %s at %q (%s); want skipped at r (%s)", d.State, d.HaltStage, d.HaltReason, reason)
	}
	if !sameTime(d.LimitedUntil, until) {
		t.Errorf("run limited_until %v, want %v", d.LimitedUntil, until)
	}
	assertStates(t, d, map[string]string{"a": "succeeded", "r": "skipped", "d": "skipped", "e": "succeeded"})
	r := stageOf(t, d, "r")
	if !sameTime(r.LimitedUntil, until) || len(r.Attempts) != 1 || r.Attempts[0].State != store.AttemptLimited || !sameTime(r.Attempts[0].LimitedUntil, until) {
		t.Errorf("stage r: %+v", r)
	}
	if s := stageOf(t, d, "d"); s.Summary != "not run: "+reason || s.LimitedUntil != nil {
		t.Errorf("stage d: %q, limited_until %v", s.Summary, s.LimitedUntil)
	}
	if h.ex.callCount("r") != 1 || h.ex.callCount("d") != 0 {
		t.Errorf("calls %v; want r once (retry unused) and d never", h.ex.calls)
	}
	ctx := context.Background()
	if err := h.eng.Resume(ctx, id, ""); !errors.Is(err, store.ErrSkippedRun) {
		t.Errorf("engine resume of a skipped run: %v", err)
	}
	if err := h.eng.Store.ResumeRun(ctx, id, []string{"r"}); !errors.Is(err, store.ErrSkippedRun) {
		t.Errorf("store resume of a skipped run: %v", err)
	}
}

// While a usage limit recorded on the pipeline lasts, a scheduled slot is
// recorded as skipped at once and runs nothing, not even its shell stage. A
// manual run still runs, and the first slot after the limit runs as usual.
func TestScheduledSlotsDuringAUsageLimitAreSkippedWithoutRunning(t *testing.T) {
	h := newLimitHarness(t, map[string][]string{"r": {"limit-long", "ok"}})
	h.add("name: p\nschedule: {interval: 1h}\nstages:\n  - {id: a, cmd: x}\n"+agentStage("r", ", needs: [a]"), true)
	ctx := context.Background()
	fire := func() RunSummary {
		t.Helper()
		if err := h.eng.schedule(ctx); err != nil {
			t.Fatal(err)
		}
		runs, err := h.eng.Runs(ctx, "p", 1)
		if err != nil || len(runs) != 1 {
			t.Fatalf("runs %v, %v", runs, err)
		}
		return runs[0]
	}
	limitedUntil := func() *time.Time {
		t.Helper()
		st, err := h.eng.Status(ctx, "test")
		if err != nil {
			t.Fatal(err)
		}
		return st.Pipelines[0].LimitedUntil
	}

	first := fire()
	if d := h.runToEnd(first.ID); d.State != store.RunSkipped {
		t.Fatalf("first slot %s (%s), want skipped", d.State, d.HaltReason)
	}
	until := h.now.Add(5 * time.Hour)
	reason := limitReason(until)
	calls := h.ex.callTotal()

	h.now = h.now.Add(time.Hour)
	slot := fire()
	if slot.ID == first.ID || slot.State != store.RunSkipped || slot.Trigger != "schedule" || slot.HaltReason != reason ||
		!sameTime(slot.LimitedUntil, until) || slot.FinishedAt == nil {
		t.Fatalf("slot during the limit: %+v; want a finished skipped run (%s)", slot, reason)
	}
	if n := h.ex.callTotal(); n != calls {
		t.Fatalf("the skipped slot ran %d stages", n-calls)
	}
	d, err := h.eng.Run(ctx, slot.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range d.Stages {
		if s.State != store.StageSkipped || s.Summary != "not run: "+reason || len(s.Attempts) != 0 {
			t.Errorf("stage %s: %s %q with %d attempts", s.ID, s.State, s.Summary, len(s.Attempts))
		}
	}
	if active, _ := h.eng.Store.HasActiveRun(ctx, "p"); active {
		t.Error("the skipped slot left a running run")
	}
	if p, _ := h.eng.Store.GetPipeline(ctx, "p"); !sameTime(p.NextDueAt, h.now.Add(time.Hour)) {
		t.Errorf("next due %v, want %v", p.NextDueAt, h.now.Add(time.Hour))
	}
	if got := limitedUntil(); !sameTime(got, until) {
		t.Errorf("status limited_until %v, want %v", got, until)
	}

	if d := h.runToEnd(h.start("p")); d.State != store.RunSucceeded {
		t.Fatalf("manual run during the limit: %s, want it run (succeeded)", d.State)
	}
	calls = h.ex.callTotal()

	h.now = until.Add(time.Minute)
	after := fire()
	if after.State != store.RunRunning {
		t.Fatalf("slot after the limit: %s, want running", after.State)
	}
	if d := h.runToEnd(after.ID); d.State != store.RunSucceeded {
		t.Fatalf("slot after the limit: %s", d.State)
	}
	if n := h.ex.callTotal(); n != calls+2 {
		t.Errorf("slot after the limit ran %d stages, want 2", n-calls)
	}
	if got := limitedUntil(); got != nil {
		t.Errorf("status limited_until %v after the limit ended", got)
	}
}

// A short usage limit is waited out: the stage stays pending, holding no
// slot and not running, until the limit ends; then it runs once more
// although retry is 0.
func TestShortUsageLimitWaitsThenRetriesOnce(t *testing.T) {
	h := newLimitHarness(t, map[string][]string{"r": {"limit-short", "ok"}})
	h.add("name: p\nstages:\n"+agentStage("r", ", retry: 0"), false)
	ctx := context.Background()
	id := h.start("p")
	until := h.now.Add(time.Minute)
	h.untilWaiting(id, "r")
	d, err := h.eng.Run(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if r := stageOf(t, d, "r"); d.State != store.RunRunning || !sameTime(r.LimitedUntil, until) {
		t.Fatalf("run %s, stage r %s until %v; want running, pending until %v", d.State, r.State, r.LimitedUntil, until)
	}

	h.now = until.Add(-time.Second)
	busy, err := h.eng.advanceAll(ctx, nil, "")
	h.eng.wg.Wait()
	if err != nil || !busy {
		t.Fatalf("advance before the limit ends: busy=%v err=%v; the waiting run must stay busy", busy, err)
	}
	if n := h.ex.callCount("r"); n != 1 {
		t.Fatalf("r ran %d times before its usage limit ended", n)
	}
	h.eng.mu.Lock()
	slots := h.eng.running[spec.KindAgent]
	h.eng.mu.Unlock()
	if slots != 0 {
		t.Errorf("a waiting stage holds %d agent slots", slots)
	}

	h.now = until
	d = h.runToEnd(id)
	if r := stageOf(t, d, "r"); d.State != store.RunSucceeded || len(r.Attempts) != 2 || r.LimitedUntil != nil {
		t.Fatalf("run %s, r %d attempts, until %v; want succeeded on the second attempt", d.State, len(r.Attempts), r.LimitedUntil)
	}
}

// The retry after a waited-out limit does not use the stage's retry budget:
// with retry 1, a failure after it still gets its one retry.
func TestUsageLimitRetryKeepsTheRetryBudget(t *testing.T) {
	h := newLimitHarness(t, map[string][]string{"r": {"limit-short", "fail", "ok"}})
	h.add("name: p\nstages:\n"+agentStage("r", ", retry: 1"), false)
	id := h.start("p")
	h.untilWaiting(id, "r")
	h.now = h.now.Add(time.Minute)
	d := h.runToEnd(id)
	if n := len(stageOf(t, d, "r").Attempts); d.State != store.RunSucceeded || n != 3 {
		t.Fatalf("run %s after %d attempts; want succeeded on attempt 3 (limited, failed, retried)", d.State, n)
	}
}

// A usage limit on the retry that waited one out is not waited out again,
// whatever retry says: the run is skipped.
func TestSecondUsageLimitSkipsTheRun(t *testing.T) {
	h := newLimitHarness(t, map[string][]string{"r": {"limit-short"}})
	h.add("name: p\nstages:\n"+agentStage("r", ", retry: 3"), false)
	id := h.start("p")
	h.untilWaiting(id, "r")
	h.now = h.now.Add(time.Minute)
	d := h.runToEnd(id)
	until := h.now.Add(time.Minute)
	if d.State != store.RunSkipped || d.HaltStage != "r" || !sameTime(d.LimitedUntil, until) {
		t.Fatalf("run %s at %q until %v; want skipped at r until %v", d.State, d.HaltStage, d.LimitedUntil, until)
	}
	if n := len(stageOf(t, d, "r").Attempts); n != 2 {
		t.Errorf("r has %d attempts, want 2", n)
	}
}

// agent.usage_limit_wait decides what counts as short.
func TestUsageLimitWaitSetting(t *testing.T) {
	h := newLimitHarness(t, map[string][]string{"r": {"limit-short", "ok"}})
	h.eng.UsageLimitWait = 30 * time.Second
	h.add("name: p\nstages:\n"+agentStage("r", ""), false)
	if d := h.runToEnd(h.start("p")); d.State != store.RunSkipped {
		t.Fatalf("a 1m limit with a 30s wait: %s, want skipped", d.State)
	}
}

// An event run is never skipped: its event is not delivered again, so a long
// limit blocks the stage, and the run can be resumed after the limit.
func TestLongUsageLimitBlocksAnEventRun(t *testing.T) {
	h := newLimitHarness(t, map[string][]string{"r": {"limit-long", "ok"}})
	h.add("name: ev\nevents: {max_queued: 3}\nstages:\n"+agentStage("r", ""), true)
	if _, err := h.trigger("k1", "body"); err != nil {
		t.Fatal(err)
	}
	h.drain()
	ctx := context.Background()
	runs, err := h.eng.Runs(ctx, "ev", 10)
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs %v, %v", runs, err)
	}
	until := h.now.Add(5 * time.Hour)
	want := limitReason(until) + "; resume after it"
	if r := runs[0]; r.State != store.RunBlocked || r.HaltStage != "r" || !strings.Contains(r.HaltReason, want) || r.LimitedUntil != nil {
		t.Fatalf("event run %s at %q (%s), limited_until %v; want blocked at r (%s)", r.State, r.HaltStage, r.HaltReason, r.LimitedUntil, want)
	}
	h.now = until.Add(time.Minute)
	if err := h.eng.Resume(ctx, runs[0].ID, ""); err != nil {
		t.Fatal(err)
	}
	if d := h.runToEnd(runs[0].ID); d.State != store.RunSucceeded {
		t.Fatalf("resumed event run %s", d.State)
	}
}

// Cancelling a run while a stage waits out a usage limit cancels it at once.
func TestCancelDuringUsageLimitWait(t *testing.T) {
	h := newLimitHarness(t, map[string][]string{"r": {"limit-short"}})
	h.add("name: p\nstages:\n"+agentStage("r", "")+"  - {id: b, cmd: x, needs: [r]}\n", false)
	id := h.start("p")
	h.untilWaiting(id, "r")
	if err := h.eng.Cancel(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	d := h.runToEnd(id)
	if d.State != store.RunCancelled {
		t.Fatalf("run %s, want cancelled", d.State)
	}
	assertStates(t, d, map[string]string{"r": "cancelled", "b": "cancelled"})
	if s := stageOf(t, d, "r"); s.LimitedUntil != nil {
		t.Errorf("cancelled stage keeps limited_until %v", s.LimitedUntil)
	}
	if n := h.ex.callCount("r"); n != 1 {
		t.Errorf("r ran %d times", n)
	}
}

// A usage limit never hides a stage that needs a person: when another stage
// of the same run blocked, the run ends blocked (and resumable), not skipped.
func TestBlockedStageWinsOverAUsageLimit(t *testing.T) {
	h := newLimitHarness(t, map[string][]string{"r": {"limit-long"}, "b": {"blocked"}})
	h.add("name: p\nstages:\n"+agentStage("r", "")+"  - {id: b, cmd: x}\n", false)
	id := h.start("p")
	d := h.runToEnd(id)
	if d.State != store.RunBlocked {
		t.Fatalf("run %s (%s); want blocked: the blocked stage needs a person", d.State, d.HaltReason)
	}
	assertStates(t, d, map[string]string{"r": "skipped", "b": "blocked"})
	if err := h.eng.Resume(context.Background(), id, "b"); err != nil {
		t.Errorf("the blocked run is not resumable: %v", err)
	}
}
