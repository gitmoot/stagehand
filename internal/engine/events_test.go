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
	if err := h.eng.applyRetention(ctx); err != nil {
		t.Fatal(err)
	}
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
