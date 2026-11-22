package agentexec

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gitmoot/stagehand/internal/spec"
	"github.com/gitmoot/stagehand/internal/stage"
)

// fakeBackend replays lines, one by one, until ctx is cancelled. It records
// what it was given.
type fakeBackend struct {
	lines []string
	hang  bool
	got   Request
	sent  int
}

func (f *fakeBackend) Run(ctx context.Context, req Request, onLine func([]byte)) (int, error) {
	f.got = req
	for _, l := range f.lines {
		if ctx.Err() != nil {
			return -1, nil
		}
		onLine([]byte(l))
		f.sent++
	}
	if f.hang {
		<-ctx.Done()
		return -1, nil
	}
	return 0, nil
}

func msg(text string, cost float64) string {
	quoted, _ := json.Marshal(text)
	return `{"type":"message_end","message":{"role":"assistant","stopReason":"stop","content":[{"type":"text","text":` + string(quoted) +
		`}],"usage":{"input":100,"output":10,"cost":{"total":` + strconv.FormatFloat(cost, 'f', -1, 64) + `}}}}`
}

const end = `{"type":"agent_end","isTerminal":true}`

func newJob(t *testing.T, maxCost float64) stage.Job {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "rows.json"), []byte(`[{"id":1}]`), 0o600); err != nil {
		t.Fatal(err)
	}
	return stage.Job{
		Stage: spec.Stage{ID: "research", Agent: "omp", Prompt: "Decide each row.", Inputs: []string{"rows.json"},
			Output: "decisions.json", MaxCost: maxCost, Timeout: "10m", Tools: []string{"read", "web_search"}},
		WorkDir: dir,
		LogPath: filepath.Join(dir, "research.log"),
	}
}

func TestSuccessWritesOutputFromFinalAnswer(t *testing.T) {
	job := newJob(t, 1)
	be := &fakeBackend{lines: []string{msg("thinking about it", 0.1), msg("```json\n[{\"id\":1,\"basis\":\"stated\"}]\n```", 0.2), end}}
	res := Executor{Backend: be}.Execute(context.Background(), job)
	if res.Outcome != stage.Succeeded {
		t.Fatalf("%+v", res)
	}
	raw, err := os.ReadFile(filepath.Join(job.WorkDir, "decisions.json"))
	if err != nil || string(raw) != "[{\"id\":1,\"basis\":\"stated\"}]\n" {
		t.Fatalf("output %q, %v", raw, err)
	}
	if res.Cost < 0.2999 || res.Cost > 0.3001 || res.InputTokens != 200 || res.OutputTokens != 20 {
		t.Errorf("usage %+v", res)
	}
	if string(be.got.Files["rows.json"]) != `[{"id":1}]` {
		t.Errorf("input not handed to the backend: %v", be.got.Files)
	}
	prompt := be.got.Args[len(be.got.Args)-1]
	if !strings.Contains(prompt, "Decide each row.") || !strings.Contains(prompt, "rows.json") || !strings.Contains(prompt, "decisions.json") {
		t.Errorf("prompt %q", prompt)
	}
	if !strings.Contains(strings.Join(be.got.Args, " "), "--tools=read,web_search") {
		t.Errorf("args %v", be.got.Args)
	}
}

func TestOverBudgetStopsAgentAndWritesNothing(t *testing.T) {
	job := newJob(t, 0.25)
	be := &fakeBackend{lines: []string{msg("step", 0.2), msg("step", 0.2), msg(`{"late":true}`, 0.2), end}, hang: true}
	res := Executor{Backend: be}.Execute(context.Background(), job)
	if res.Outcome != stage.Failed || !strings.Contains(res.Summary, "over budget") {
		t.Fatalf("%+v", res)
	}
	if be.sent != 2 {
		t.Errorf("backend kept streaming after the cap: %d lines", be.sent)
	}
	if _, err := os.Stat(filepath.Join(job.WorkDir, "decisions.json")); !os.IsNotExist(err) {
		t.Error("output written by an over-budget run")
	}
	if res.Cost < 0.3999 {
		t.Errorf("cost so far not reported: %v", res.Cost)
	}
}

func TestNonJSONAnswerFailsAndRemovesStaleOutput(t *testing.T) {
	job := newJob(t, 1)
	stale := filepath.Join(job.WorkDir, "decisions.json")
	if err := os.WriteFile(stale, []byte(`{"from":"attempt 1"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	be := &fakeBackend{lines: []string{msg("I could not decide.", 0.1), end}}
	res := Executor{Backend: be}.Execute(context.Background(), job)
	if res.Outcome != stage.Failed || !strings.Contains(res.Summary, "not JSON") {
		t.Fatalf("%+v", res)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Error("stale output from an earlier attempt survived a failed attempt")
	}
}

func TestFailedTurnFailsEvenWhenOmpExitsZero(t *testing.T) {
	job := newJob(t, 1)
	be := &fakeBackend{lines: []string{`{"type":"message_end","message":{"role":"assistant","stopReason":"error","errorMessage":"429","content":[]}}`, end}}
	res := Executor{Backend: be}.Execute(context.Background(), job)
	if res.Outcome != stage.Failed || !strings.Contains(res.Summary, "429") || res.ExitCode == nil || *res.ExitCode != 0 {
		t.Fatalf("%+v", res)
	}
}

// Run 2718: omp ended on a usage limit far beyond the 5 minutes it waits out
// itself. That is not a failure of the stage: the engine gets Limited and how
// long to wait.
func TestUsageLimitIsLimitedNotFailed(t *testing.T) {
	job := newJob(t, 1)
	limit429 := `Anthropic stream error (api_error): 429 {"type":"error","error":{"type":"rate_limit_error","message":"This request would exceed your rate limit."}}`
	turn, _ := json.Marshal(limit429 + " retry-after-ms=12769000")
	final, _ := json.Marshal("Provider requested 12769000ms wait, exceeds retry.maxDelayMs (300000ms). Original error: " + limit429)
	be := &fakeBackend{lines: []string{
		`{"type":"message_end","message":{"role":"assistant","stopReason":"error","errorMessage":` + string(turn) + `,"content":[]}}`,
		`{"type":"auto_retry_start"}`,
		`{"type":"auto_retry_end","success":false,"finalError":` + string(final) + `}`,
		end,
	}}
	res := Executor{Backend: be}.Execute(context.Background(), job)
	if res.Outcome != stage.Limited || res.RetryAfter != 12769*time.Second {
		t.Fatalf("%+v; want limited for 12769s", res)
	}
	if !strings.HasPrefix(res.Summary, "usage limit (retry after 3h32m49s): ") || !strings.Contains(res.Summary, "rate_limit_error") {
		t.Errorf("summary %q", res.Summary)
	}
}

func TestTimeoutReportsEngineCause(t *testing.T) {
	job := newJob(t, 1)
	ctx, cancel := context.WithTimeoutCause(context.Background(), 50*time.Millisecond, context.DeadlineExceeded)
	defer cancel()
	res := Executor{Backend: &fakeBackend{hang: true}}.Execute(ctx, job)
	if res.Outcome != stage.Failed {
		t.Fatalf("%+v", res)
	}
}

func TestMissingInputFails(t *testing.T) {
	job := newJob(t, 1)
	job.Stage.Inputs = []string{"missing.json"}
	res := Executor{Backend: &fakeBackend{}}.Execute(context.Background(), job)
	if res.Outcome != stage.Failed || !strings.Contains(res.Summary, "missing.json") {
		t.Fatalf("%+v", res)
	}
}
