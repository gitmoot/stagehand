package engine

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gitmoot/stagehand/internal/shellexec"
	"github.com/gitmoot/stagehand/internal/spec"
	"github.com/gitmoot/stagehand/internal/stage"
	"github.com/gitmoot/stagehand/internal/store"
)

const eventPipeline = "name: ev\nevents: {max_queued: 3}\nstages:\n  - {id: a, cmd: x}\n"

func (h *harness) trigger(key, body string) (EventResult, error) {
	h.t.Helper()
	return h.eng.Trigger(context.Background(), EventRequest{Pipeline: "ev", Key: key, Source: SourceCLI, Payload: strings.NewReader(body), MaxBytes: 1 << 20})
}

func (h *harness) drain() {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := h.eng.Loop(ctx, LoopOptions{Events: true, UntilIdle: true}); err != nil {
		h.t.Fatal(err)
	}
}

func TestSameEventIDYieldsOneRun(t *testing.T) {
	h := newHarness(t, nil)
	h.add(eventPipeline, true)
	first, err := h.trigger("deck-42", "one")
	if err != nil || first.Duplicate {
		t.Fatalf("first: %+v %v", first, err)
	}
	second, err := h.trigger("deck-42", "retried body")
	if err != nil || !second.Duplicate || second.Event.ID != first.Event.ID {
		t.Fatalf("retry: %+v %v; want the first event back as a duplicate", second, err)
	}
	h.drain()
	// A retry after the run finished is still a duplicate.
	if third, err := h.trigger("deck-42", "late"); err != nil || !third.Duplicate {
		t.Fatalf("late retry: %+v %v", third, err)
	}
	runs, _ := h.eng.Runs(context.Background(), "ev", 10)
	if len(runs) != 1 || h.ex.callCount("a") != 1 {
		t.Fatalf("%d runs, %d executions; want exactly one", len(runs), h.ex.callCount("a"))
	}
}

// A burst: events beyond max_queued are refused and nothing is stored for
// them; every accepted event runs exactly once, one run at a time.
func TestBurstIsBoundedAndEveryAcceptedEventRunsOnce(t *testing.T) {
	h := newHarness(t, nil)
	h.add(eventPipeline, true)
	var accepted, refused int
	for i := range 10 {
		_, err := h.trigger("", strings.Repeat("x", i))
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, store.ErrQueueFull):
			refused++
		default:
			t.Fatal(err)
		}
	}
	if accepted != 3 || refused != 7 {
		t.Fatalf("accepted %d, refused %d; want 3 and 7", accepted, refused)
	}
	files, _ := os.ReadDir(h.eng.payloadDir())
	if len(files) != 3 {
		t.Errorf("%d payload files; refused events must leave nothing", len(files))
	}
	h.drain()
	runs, _ := h.eng.Runs(context.Background(), "ev", 10)
	if len(runs) != 3 || h.ex.callCount("a") != 3 {
		t.Fatalf("%d runs, %d executions; want 3", len(runs), h.ex.callCount("a"))
	}
	for _, r := range runs {
		if r.State != store.RunSucceeded || r.Trigger != "event" || r.Event == nil {
			t.Errorf("run %+v", r)
		}
	}
	// Queue has room again.
	if _, err := h.trigger("", "after"); err != nil {
		t.Errorf("after draining: %v", err)
	}
}

func TestEventRunsNeverOverlapWithinAPipeline(t *testing.T) {
	h := newHarness(t, nil)
	lim := &limitedExec{}
	h.eng.Executors[spec.KindShell] = &overlapExec{inner: lim}
	h.add(eventPipeline, true)
	for range 3 {
		if _, err := h.trigger("", "x"); err != nil {
			t.Fatal(err)
		}
	}
	h.drain()
	if lim.overlap || lim.ran != 3 {
		t.Fatalf("overlap=%v ran=%d", lim.overlap, lim.ran)
	}
}

// overlapExec turns limitedExec's overlap check on for shell stages without
// its concurrency limit, to prove the one-run-per-pipeline rule alone keeps
// event runs sequential.
type overlapExec struct{ inner *limitedExec }

func (o *overlapExec) Execute(ctx context.Context, job stage.Job) stage.Result {
	return o.inner.Execute(ctx, job)
}

// The payload reaches a real shell stage only as a file: shell
// metacharacters in it are delivered byte for byte and run nothing.
func TestPayloadWithShellMetacharactersIsDataNotCode(t *testing.T) {
	h := newHarness(t, nil)
	h.eng.Executors[spec.KindShell] = shellexec.Executor{}
	h.eng.BaseEnv = []string{"PATH=/usr/bin:/bin"}
	canary := filepath.Join(t.TempDir(), "pwned")
	payload := "Subject: $(touch " + canary + ") `touch " + canary + "` ; touch " + canary + " | && \n\x00'\"\\"
	h.add("name: ev\nevents: {max_queued: 3}\nstages:\n  - {id: copy, cmd: 'cp \"$STAGEHAND_EVENT_FILE\" \"$STAGEHAND_WORK/got\"; echo \"$STAGEHAND_TRIGGER $STAGEHAND_EVENT_ID\" > \"$STAGEHAND_WORK/meta\"'}\n  - {id: after, needs: [copy], cmd: 'false'}\n", true)
	if _, err := h.trigger("mail-1", payload); err != nil {
		t.Fatal(err)
	}
	h.drain()
	if _, err := os.Stat(canary); err == nil {
		t.Fatal("payload content was executed")
	}
	runs, _ := h.eng.Runs(context.Background(), "ev", 1)
	got, err := os.ReadFile(filepath.Join(h.eng.RunDir(runs[0].ID), "work", "got"))
	if err != nil || !bytes.Equal(got, []byte(payload)) {
		t.Fatalf("payload not delivered byte-exact: %q, %v", got, err)
	}
	meta, _ := os.ReadFile(filepath.Join(h.eng.RunDir(runs[0].ID), "work", "meta"))
	if strings.TrimSpace(string(meta)) != "event mail-1" {
		t.Errorf("meta %q", meta)
	}
}

func TestRetentionDeletesOnSuccessAndKeepsParkedRuns(t *testing.T) {
	h := newHarness(t, map[string][]string{"a": {"ok", "fail", "ok"}})
	h.add(eventPipeline, true)
	ok, _ := h.trigger("ok", "secret deck")
	bad, _ := h.trigger("bad", "another secret")
	h.drain()
	ctx := context.Background()

	okEv, _ := h.eng.Store.GetEvent(ctx, ok.Event.ID)
	okRun, _ := h.eng.Store.GetRun(ctx, okEv.RunID)
	if okRun.State != store.RunSucceeded || okRun.FilesDeletedAt == nil || okEv.PayloadDeletedAt == nil {
		t.Fatalf("succeeded run: state %s, files deleted %v, payload deleted %v", okRun.State, okRun.FilesDeletedAt, okEv.PayloadDeletedAt)
	}
	if _, err := os.Stat(okEv.PayloadPath); !os.IsNotExist(err) {
		t.Error("payload of a succeeded run still on disk")
	}
	if _, err := os.Stat(h.eng.RunDir(okRun.ID)); !os.IsNotExist(err) {
		t.Error("run directory of a succeeded run still on disk")
	}

	badEv, _ := h.eng.Store.GetEvent(ctx, bad.Event.ID)
	badRun, _ := h.eng.Store.GetRun(ctx, badEv.RunID)
	if badRun.State != store.RunFailed || badRun.FilesDeletedAt != nil {
		t.Fatalf("failed run: %s, files deleted %v", badRun.State, badRun.FilesDeletedAt)
	}
	if _, err := os.Stat(badEv.PayloadPath); err != nil {
		t.Fatal("payload of a parked run was deleted; it must survive for resume")
	}
	// Resume works and, once it succeeds, the payload goes.
	if err := h.eng.Resume(ctx, badRun.ID, ""); err != nil {
		t.Fatal(err)
	}
	h.runToEnd(badRun.ID)
	// Deleted at once when the run succeeds, not only by the periodic sweep.
	if _, err := os.Stat(badEv.PayloadPath); !os.IsNotExist(err) {
		t.Error("payload kept after the resumed run succeeded")
	}
}

func TestPurgedRunCannotResume(t *testing.T) {
	h := newHarness(t, map[string][]string{"a": {"fail"}})
	h.add(eventPipeline, true)
	res, _ := h.trigger("p", "secret")
	h.drain()
	ctx := context.Background()
	ev, _ := h.eng.Store.GetEvent(ctx, res.Event.ID)
	if err := h.eng.Purge(ctx, ev.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(ev.PayloadPath); !os.IsNotExist(err) {
		t.Error("purge left the payload")
	}
	if err := h.eng.Resume(ctx, ev.RunID, ""); err == nil {
		t.Error("resumed a run whose files were deleted")
	}
}

func TestEventsRefusedWhenNotOptedInOrDisabledOrTooLarge(t *testing.T) {
	h := newHarness(t, nil)
	h.add("name: ev\nstages:\n  - {id: a, cmd: x}\n", true)
	if _, err := h.trigger("", "x"); !errors.Is(err, ErrEventsDisabled) {
		t.Errorf("no events block: %v", err)
	}
	h.add(eventPipeline, false) // replacing keeps the pipeline enabled
	h.eng.SetEnabled(context.Background(), "ev", false)
	if _, err := h.trigger("", "x"); !errors.Is(err, ErrPipelineDisabled) {
		t.Errorf("disabled pipeline: %v", err)
	}
	h.eng.SetEnabled(context.Background(), "ev", true)
	_, err := h.eng.Trigger(context.Background(), EventRequest{Pipeline: "ev", Payload: strings.NewReader("12345"), MaxBytes: 4})
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Errorf("oversized: %v", err)
	}
	if _, err := h.trigger("bad key with spaces", "x"); !errors.Is(err, ErrBadEventKey) {
		t.Errorf("bad key: %v", err)
	}
	if files, _ := os.ReadDir(h.eng.payloadDir()); len(files) != 0 {
		t.Errorf("refused events left %d files", len(files))
	}
}

// Events and a schedule on one pipeline share the one-run-at-a-time rule.
func TestScheduleAndEventsCoexist(t *testing.T) {
	h := newHarness(t, nil)
	h.add("name: ev\nschedule: {interval: 1h}\nevents: {max_queued: 3}\nstages:\n  - {id: a, cmd: x}\n", true)
	if _, err := h.trigger("e1", "x"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := h.eng.Loop(ctx, LoopOptions{Schedule: true, UntilIdle: true}); err != nil {
		t.Fatal(err)
	}
	runs, _ := h.eng.Runs(context.Background(), "ev", 10)
	triggers := map[string]int{}
	for _, r := range runs {
		triggers[r.Trigger]++
	}
	if triggers["schedule"] != 1 || triggers["event"] != 1 {
		t.Fatalf("runs by trigger %v; want one scheduled and one event run", triggers)
	}
}

func TestConcurrentDuplicateTriggersStoreOneEvent(t *testing.T) {
	h := newHarness(t, nil)
	h.add(eventPipeline, true)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := h.trigger("same", "x"); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	evs, _ := h.eng.Store.ListEvents(context.Background(), "ev", "", 50)
	if len(evs) != 1 {
		t.Fatalf("%d events stored for one key", len(evs))
	}
	if files, _ := os.ReadDir(h.eng.payloadDir()); len(files) != 1 {
		t.Errorf("%d payload files for one event", len(files))
	}
}

// The payload goes the moment its run succeeds, not at the next periodic
// sweep (which runs only once a minute under serve).
func TestPayloadDeletedAtTheMomentTheRunSucceeds(t *testing.T) {
	h := newHarness(t, nil)
	h.add(eventPipeline, true)
	res, err := h.trigger("now", "confidential")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := h.eng.startEvents(ctx, "", true); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		busy, err := h.eng.advanceAll(ctx, nil, "")
		if err != nil {
			t.Fatal(err)
		}
		if !busy {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("run did not settle")
		}
		time.Sleep(10 * time.Millisecond)
	}
	ev, _ := h.eng.Store.GetEvent(ctx, res.Event.ID)
	if r, _ := h.eng.Store.GetRun(ctx, ev.RunID); r.State != store.RunSucceeded {
		t.Fatalf("run %s", r.State)
	}
	if _, err := os.Stat(ev.PayloadPath); !os.IsNotExist(err) {
		t.Fatal("payload still on disk right after the run succeeded")
	}
}

// An intake in another process may have placed its payload file and not yet
// committed the event. A sweep at that moment must leave the file alone.
func TestSweepSparesAPayloadWhoseIntakeHasNotCommitted(t *testing.T) {
	h := newHarness(t, nil)
	h.add(eventPipeline, true)
	h.now = time.Now()
	ctx := context.Background()
	// The sweeping process (e.g. serve) has the store open already.
	other, err := store.Open(ctx, filepath.Join(h.eng.Home, "stagehand.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	peer := New(other, h.eng.Home, nil, nil, nil)
	peer.Now = func() time.Time { return h.now }
	var swept bool
	_, _, err = h.eng.Store.InsertEvent(ctx, store.Event{Pipeline: "ev", Key: "k", Source: SourceHTTP, ReceivedAt: h.now,
		PayloadPath: filepath.Join(h.eng.payloadDir(), "placed.payload")}, 3, func() error {
		os.MkdirAll(h.eng.payloadDir(), 0o700)
		if err := os.WriteFile(filepath.Join(h.eng.payloadDir(), "placed.payload"), []byte("deck"), 0o600); err != nil {
			return err
		}
		// The sweep runs in the other process while this intake's
		// transaction is still open.
		swept = true
		return peer.sweepOrphanPayloads(ctx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if !swept {
		t.Fatal("sweep did not run")
	}
	if _, err := os.Stat(filepath.Join(h.eng.payloadDir(), "placed.payload")); err != nil {
		t.Fatal("sweep deleted the payload of an intake that was about to commit")
	}
}

// Purge claims the deletion in the database before touching files, so a
// concurrent resume is refused instead of resuming a run whose files vanish.
func TestPurgeAndResumeAreMutuallyExclusive(t *testing.T) {
	h := newHarness(t, map[string][]string{"a": {"fail"}})
	h.add(eventPipeline, true)
	res, _ := h.trigger("p", "secret")
	h.drain()
	ctx := context.Background()
	ev, _ := h.eng.Store.GetEvent(ctx, res.Event.ID)
	// A claim without the file removal, as if purge stopped right after it.
	if _, err := h.eng.Store.ClaimFilesDeletion(ctx, ev.RunID, h.now); err != nil {
		t.Fatal(err)
	}
	if err := h.eng.Resume(ctx, ev.RunID, ""); err == nil {
		t.Fatal("resume accepted a run already claimed for deletion")
	}
	// And the other order: once resumed (running), it cannot be claimed.
	h2 := newHarness(t, map[string][]string{"a": {"fail", "hang"}})
	h2.add(eventPipeline, true)
	res2, _ := h2.trigger("q", "secret")
	h2.drain()
	ev2, _ := h2.eng.Store.GetEvent(ctx, res2.Event.ID)
	if err := h2.eng.Resume(ctx, ev2.RunID, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := h2.eng.Store.ClaimFilesDeletion(ctx, ev2.RunID, h2.now); !errors.Is(err, store.ErrNotDeletable) {
		t.Fatalf("claimed a running run: %v", err)
	}
	if err := h2.eng.Purge(ctx, ev2.RunID); err == nil {
		t.Fatal("purged a running run")
	}
	if _, err := os.Stat(ev2.PayloadPath); err != nil {
		t.Fatal("payload of the resumed run is gone")
	}
}

// A deletion interrupted after its claim is finished by the next sweep.
func TestInterruptedDeletionIsFinished(t *testing.T) {
	h := newHarness(t, map[string][]string{"a": {"fail"}})
	h.add(eventPipeline, true)
	res, _ := h.trigger("p", "secret")
	h.drain()
	ctx := context.Background()
	ev, _ := h.eng.Store.GetEvent(ctx, res.Event.ID)
	if _, err := h.eng.Store.ClaimFilesDeletion(ctx, ev.RunID, h.now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(ev.PayloadPath); err != nil {
		t.Fatal("setup: payload should still be on disk")
	}
	if err := h.eng.applyRetention(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(ev.PayloadPath); !os.IsNotExist(err) {
		t.Error("claimed payload not removed by the sweep")
	}
	if _, err := os.Stat(h.eng.RunDir(ev.RunID)); !os.IsNotExist(err) {
		t.Error("claimed run directory not removed by the sweep")
	}
}

// Drop commits "dropped" before unlinking: if the unlink never happens the
// event is still not runnable, and the sweep removes the file later.
func TestDroppedEventNeverRunsEvenIfItsFileSurvives(t *testing.T) {
	h := newHarness(t, nil)
	h.add(eventPipeline, true)
	h.eng.SetEnabled(context.Background(), "ev", false)
	h.eng.SetEnabled(context.Background(), "ev", true)
	res, _ := h.trigger("d", "secret")
	ctx := context.Background()
	// Make the unlink fail: replace the payload with a non-empty directory.
	os.Remove(res.Event.PayloadPath)
	os.MkdirAll(filepath.Join(res.Event.PayloadPath, "x"), 0o700)
	_, err := h.eng.DropEvent(ctx, res.Event.ID)
	if err == nil {
		t.Fatal("setup: unlink should have failed")
	}
	ev, _ := h.eng.Store.GetEvent(ctx, res.Event.ID)
	if ev.State != store.EventDropped || ev.PayloadDeletedAt == nil {
		t.Fatalf("event %s after a failed unlink; it must already be dropped", ev.State)
	}
	h.drain()
	if h.ex.callCount("a") != 0 {
		t.Fatal("a dropped event ran")
	}
}

// The reviewer's scenario: a payload whose bytes were written long ago (a
// slow producer that waited before closing) is placed just before a sweep in
// another process, while its event has not committed. Its age must not make
// it an orphan.
func TestSweepSparesAnAgedPayloadWhoseIntakeHasNotCommitted(t *testing.T) {
	h := newHarness(t, nil)
	h.add(eventPipeline, true)
	h.now = time.Now()
	ctx := context.Background()
	other, err := store.Open(ctx, filepath.Join(h.eng.Home, "stagehand.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	peer := New(other, h.eng.Home, nil, nil, nil)
	peer.Now = func() time.Time { return h.now }
	var placed string
	h.eng.beforeEventCommit = func(path string) {
		placed = path
		old := h.now.Add(-2 * time.Hour)
		os.Chtimes(path, old, old)
		if err := peer.sweepOrphanPayloads(ctx); err != nil {
			t.Error(err)
		}
	}
	res, err := h.trigger("aged", "deck bytes")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(placed); err != nil {
		t.Fatalf("an aged payload of an uncommitted intake was swept: %v", err)
	}
	ev, _ := h.eng.Store.GetEvent(ctx, res.Event.ID)
	if ev.State != store.EventQueued || ev.PayloadDeletedAt != nil {
		t.Fatalf("event %+v", ev)
	}
	// Once committed and unlocked, a genuinely unreferenced old file goes.
	stray := filepath.Join(h.eng.payloadDir(), "stray.payload")
	os.WriteFile(stray, []byte("x"), 0o600)
	old := h.now.Add(-2 * time.Hour)
	os.Chtimes(stray, old, old)
	if err := peer.sweepOrphanPayloads(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stray); !os.IsNotExist(err) {
		t.Error("an unreferenced, unlocked old file was not swept")
	}
	if _, err := os.Stat(placed); err != nil {
		t.Error("the committed event's payload was swept")
	}
}

// Retrying purge after an interrupted deletion finishes it, instead of
// reporting success while the confidential files stay.
func TestPurgeRetryFinishesAnInterruptedDeletion(t *testing.T) {
	h := newHarness(t, map[string][]string{"a": {"fail"}})
	h.add(eventPipeline, true)
	res, _ := h.trigger("p", "secret")
	h.drain()
	ctx := context.Background()
	ev, _ := h.eng.Store.GetEvent(ctx, res.Event.ID)
	if _, err := h.eng.Store.ClaimFilesDeletion(ctx, ev.RunID, h.now); err != nil {
		t.Fatal(err)
	}
	if err := h.eng.Purge(ctx, ev.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(ev.PayloadPath); !os.IsNotExist(err) {
		t.Error("purge retry left the payload")
	}
	if _, err := os.Stat(h.eng.RunDir(ev.RunID)); !os.IsNotExist(err) {
		t.Error("purge retry left the run directory")
	}
}

// A schedule that is due again every time its run ends must not starve
// queued events: whichever is older gets the slot.
func TestDueScheduleDoesNotStarveQueuedEvents(t *testing.T) {
	h := newHarness(t, nil)
	// Frozen clock: the 1-minute schedule is due on every pass.
	h.add("name: ev\nschedule: {interval: 1m}\nevents: {max_queued: 3}\nstages:\n  - {id: a, cmd: x}\n", true)
	ctx := context.Background()
	// The schedule has been due since before the event arrived; it goes
	// first, then the event, even though the schedule is due again.
	h.now = h.now.Add(30 * time.Second)
	if _, err := h.trigger("e1", "x"); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		if err := h.eng.schedule(ctx); err != nil {
			t.Fatal(err)
		}
		if err := h.eng.startEvents(ctx, "", true); err != nil {
			t.Fatal(err)
		}
		runs, _ := h.eng.Runs(ctx, "ev", 10)
		h.eng.Store.FinishRun(ctx, runs[0].ID, store.RunSucceeded, "", "", h.now)
		h.now = h.now.Add(2 * time.Minute) // longer than the interval
	}
	evs, _ := h.eng.Store.ListEvents(ctx, "ev", store.EventQueued, 10)
	if len(evs) != 0 {
		t.Fatalf("event starved: %d still queued after 4 slots", len(evs))
	}
	runs, _ := h.eng.Runs(ctx, "ev", 10)
	if runs[len(runs)-1].Trigger != "schedule" || runs[len(runs)-2].Trigger != "event" {
		t.Errorf("order %v; want the older schedule first, then the event", triggers(runs))
	}
}

func triggers(rs []RunSummary) []string {
	var out []string
	for i := len(rs) - 1; i >= 0; i-- {
		out = append(out, rs[i].Trigger)
	}
	return out
}

// The sweep must take its exclusive lock on a writable descriptor (NFS
// emulates flock with fcntl locks, which need one) and must not mistake a
// permanent locking error for an intake in progress.
func TestSweepOpensCandidatesWritable(t *testing.T) {
	h := newHarness(t, nil)
	os.MkdirAll(h.eng.payloadDir(), 0o700)
	ro := filepath.Join(h.eng.payloadDir(), "readonly.payload")
	os.WriteFile(ro, []byte("x"), 0o400)
	old := h.now.Add(-time.Hour)
	os.Chtimes(ro, old, old)
	err := h.eng.sweepOrphanPayloads(context.Background())
	if os.Geteuid() == 0 {
		// root can open a 0400 file read-write; the file is swept.
		if err != nil {
			t.Fatal(err)
		}
		if _, serr := os.Stat(ro); !os.IsNotExist(serr) {
			t.Error("unreferenced old file not swept")
		}
		return
	}
	if err == nil {
		t.Fatal("a file the sweep cannot open writable was silently treated as in use")
	}
}

// An events-only loop (trigger --wait without serve) cannot dispatch
// schedules, so it must not yield an event to a due schedule.
func TestEventsOnlyLoopDoesNotWaitForASchedule(t *testing.T) {
	h := newHarness(t, nil)
	// Enabled with a schedule: due at once (first_due = now).
	h.add("name: ev\nschedule: {interval: 1h}\nevents: {max_queued: 3}\nstages:\n  - {id: a, cmd: x}\n", true)
	h.now = h.now.Add(time.Minute) // the event is newer than the due time
	res, err := h.trigger("e", "x")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.eng.Loop(ctx, LoopOptions{Events: true, UntilIdle: true}); err != nil {
		t.Fatal(err)
	}
	ev, _ := h.eng.Store.GetEvent(context.Background(), res.Event.ID)
	if ev.State != store.EventStarted {
		t.Fatalf("event %s; an events-only loop stalled behind a schedule it cannot run", ev.State)
	}
}

// A loop whose goal is already met dispatches nothing.
func TestLoopWithDoneAlreadyTrueStartsNothing(t *testing.T) {
	h := newHarness(t, nil)
	h.add(eventPipeline, true)
	if _, err := h.trigger("newer", "x"); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := h.eng.Loop(ctx, LoopOptions{Events: true, UntilIdle: true, Done: func(context.Context) bool { return true }}); err != nil {
		t.Fatal(err)
	}
	if n := h.ex.callCount("a"); n != 0 {
		t.Fatalf("started %d attempts although the caller's goal was already met", n)
	}
}
