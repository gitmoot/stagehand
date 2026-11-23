package omp

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gitmoot/stagehand/internal/spec"
)

func feedFile(t *testing.T, path string) *Stream {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var s Stream
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		s.Feed(sc.Bytes())
	}
	return &s
}

func feed(lines ...string) *Stream {
	var s Stream
	for _, l := range lines {
		s.Feed([]byte(l))
	}
	return &s
}

// Real omp/18.10.4 streams recorded on 2 Nov 2026.
func TestRecordedStreams(t *testing.T) {
	s := feedFile(t, "testdata/tooluse-success.jsonl")
	text, err := s.Result()
	if err != nil {
		t.Fatal(err)
	}
	doc, err := ExtractJSON(text)
	if err != nil || string(doc) != `[{"id":1,"answer":"Rome"}]`+"\n" {
		t.Errorf("ExtractJSON = %q, %v", doc, err)
	}
	// Two assistant messages: the tool call (0.02201) and the answer (0.001971).
	if got := s.Cost(); got < 0.02398 || got > 0.02399 {
		t.Errorf("cost %v, want 0.023981", got)
	}
	if s.Messages() != 2 {
		t.Errorf("messages %d, want 2", s.Messages())
	}

	s = feedFile(t, "testdata/plain-success.jsonl")
	if text, err := s.Result(); err != nil || text != `{"ok":true}` {
		t.Errorf("plain: %q, %v", text, err)
	}
	if in, out := s.Tokens(); in != 12383 || out != 9 {
		t.Errorf("tokens %d/%d, want 12383/9", in, out)
	}
}

const (
	agentEnd  = `{"type":"agent_end","isTerminal":true}`
	answerMsg = `{"type":"message_end","message":{"role":"assistant","stopReason":"stop","content":[{"type":"text","text":"{\"a\":1}"}],"usage":{"input":10,"output":5,"cost":{"total":0.5}}}}`
	errorMsg  = `{"type":"message_end","message":{"role":"assistant","stopReason":"error","errorMessage":"429 rate limited","content":[],"usage":{"input":10,"output":0,"cost":{"total":0.1}}}}`
	toolMsg   = `{"type":"message_end","message":{"role":"assistant","stopReason":"toolUse","content":[{"type":"toolCall"}],"usage":{"input":10,"output":5,"cost":{"total":0.2}}}}`
)

func TestStreamOutcomes(t *testing.T) {
	cases := []struct {
		name  string
		lines []string
		want  string // "" = success
	}{
		{"success", []string{answerMsg, agentEnd}, ""},
		{"failed turn though omp exits 0", []string{errorMsg, agentEnd}, "429 rate limited"},
		{"retry recovered", []string{errorMsg, `{"type":"auto_retry_start"}`, `{"type":"auto_retry_end","success":true}`, answerMsg, agentEnd}, ""},
		{"retry gave up", []string{`{"type":"auto_retry_start"}`, `{"type":"auto_retry_end","success":false,"finalError":"quota"}`, answerMsg, agentEnd}, "gave up retrying: quota"},
		{"no agent_end", []string{answerMsg}, "no agent_end"},
		{"non-terminal end", []string{answerMsg, `{"type":"agent_end","isTerminal":false}`}, "continuation"},
		{"retry still open", []string{answerMsg, `{"type":"auto_retry_start"}`, agentEnd}, "middle of a retry"},
		{"cut off on tool call", []string{toolMsg, agentEnd}, "cut off"},
		{"no text", []string{`{"type":"message_end","message":{"role":"assistant","stopReason":"stop","content":[]}}`, agentEnd}, "no answer"},
		{"cut assistant line", []string{answerMsg, `{"type":"message_end","message":{"role":"assistant","conte`, agentEnd}, "unreadable"},
		{"user message ignored", []string{answerMsg, `{"type":"message_end","message":{"role":"user","content":"hi"}}`, agentEnd}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := feed(c.lines...).Result()
			switch {
			case c.want == "" && err != nil:
				t.Fatalf("unexpected error %v", err)
			case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
				t.Fatalf("error %v, want it to mention %q", err, c.want)
			}
		})
	}
}

func TestCostAddsUpEveryAssistantMessageIncludingFailedOnes(t *testing.T) {
	s := feed(errorMsg, toolMsg, answerMsg, agentEnd)
	if got := s.Cost(); got < 0.7999 || got > 0.8001 {
		t.Errorf("cost %v, want 0.8", got)
	}
}

// Run 2718's shape: the provider's 429 named a wait far beyond the 5 minutes
// omp sits out, so omp ended the turn and gave up retrying.
const (
	limit429  = `Anthropic stream error (api_error): 429 {"type":"error","error":{"type":"rate_limit_error","message":"This request would exceed your rate limit."}}`
	limitTurn = limit429 + ` retry-after-ms=12769000`
	// As omp writes it (run 2718): the original error keeps its own wait.
	limitFinal = `Provider requested 12769000ms wait, exceeds retry.maxDelayMs (300000ms). Original error: ` + limitTurn
	// A 503 whose message happens to mention 429 and rate limits.
	overloaded503 = `Anthropic stream error (api_error): 503 {"type":"error","error":{"type":"overloaded_error","message":"upstream shard 429 unavailable, rate_limit hit"}} retry-after-ms=60000`
)

func failedTurn(errorMessage string) string {
	quoted, _ := json.Marshal(errorMessage)
	return `{"type":"message_end","message":{"role":"assistant","stopReason":"error","errorMessage":` + string(quoted) + `,"content":[]}}`
}

// failedTurnStatus is a failed turn on which omp recorded an HTTP status.
func failedTurnStatus(status int, errorMessage string) string {
	quoted, _ := json.Marshal(errorMessage)
	return fmt.Sprintf(`{"type":"message_end","message":{"role":"assistant","stopReason":"error","errorStatus":%d,"errorMessage":%s,"content":[]}}`, status, quoted)
}

func gaveUp(finalError string) string {
	quoted, _ := json.Marshal(finalError)
	return `{"type":"auto_retry_end","success":false,"finalError":` + string(quoted) + `}`
}

func TestUsageLimit(t *testing.T) {
	const retryStart = `{"type":"auto_retry_start"}`
	cases := []struct {
		name  string
		lines []string
		want  time.Duration // 0 = not a usage limit
	}{
		// The production form (run 2718 and the dossier-research runs since).
		{"failed turn names retry-after-ms", []string{failedTurn(limitTurn), agentEnd}, 12769 * time.Second},
		{"omp gave up on the provider's wait", []string{retryStart, gaveUp(limitFinal), agentEnd}, 12769 * time.Second},
		{"run 2718: both", []string{failedTurn(limitTurn), retryStart, gaveUp(limitFinal), agentEnd}, 12769 * time.Second},
		{"gave up with the original error as finalError", []string{failedTurn(limitTurn), retryStart, gaveUp(limitTurn), agentEnd}, 12769 * time.Second},
		{"unwrapped SDK message", []string{failedTurn(`429 {"type":"error","error":{"type":"rate_limit_error","message":"slow down"}} retry-after-ms=600000`), agentEnd}, 10 * time.Minute},
		// omp writes integral milliseconds; any other number form is not its wait.
		{"fractional wait", []string{failedTurn(limit429 + ` retry-after-ms=1500.5`), agentEnd}, 0},
		{"underscored wait", []string{failedTurn(limit429 + ` retry-after-ms=900_000`), agentEnd}, 0},
		{"signed wait", []string{failedTurn(limit429 + ` retry-after-ms=+900000`), agentEnd}, 0},
		{"exponent wait", []string{failedTurn(limit429 + ` retry-after-ms=9e5`), agentEnd}, 0},
		{"recorded HTTP 429", []string{failedTurnStatus(429, limitTurn), agentEnd}, 12769 * time.Second},
		// Not a provider 429.
		{"503 mentioning 429 is no limit", []string{failedTurn(overloaded503), agentEnd}, 0},
		{"529 with 429 { in its message is no limit", []string{failedTurn(`(api_error): 529 {"type":"error","error":{"type":"overloaded_error","message":"shard said 429 {busy}"}} retry-after-ms=60000`), agentEnd}, 0},
		{"statusless overloaded_error whose message says 429 {", []string{failedTurn(`{"type":"error","error":{"type":"overloaded_error","message":"upstream said 429 {busy}"}} retry-after-ms=900000`), agentEnd}, 0},
		{"wrapped overloaded_error whose message says 429 {", []string{failedTurn(`Anthropic stream error (overloaded_error): upstream said 429 {busy} retry-after-ms=900000`), agentEnd}, 0},
		{"typed overload whose message starts with 429", []string{failedTurn(`Anthropic stream error (overloaded_error): 429 upstream busy; retry-after-ms=900000`), agentEnd}, 0},
		{"typed overload carrying a fake 429 body", []string{failedTurn(`Anthropic stream error (overloaded_error): 429 {"type":"error","error":{"type":"rate_limit_error","message":"x"}} retry-after-ms=900000`), agentEnd}, 0},
		{"recorded HTTP 503 with a 429 text", []string{failedTurnStatus(503, limitTurn), agentEnd}, 0},
		{"429 whose body is not rate_limit_error", []string{failedTurn(`429 {"type":"error","error":{"type":"overloaded_error","message":"x"}} retry-after-ms=60000`), agentEnd}, 0},
		{"rate_limit_error type on a 500", []string{failedTurn(`500 {"type":"error","error":{"type":"rate_limit_error","message":"x"}} retry-after-ms=60000`), agentEnd}, 0},
		{"retry-after on another error", []string{failedTurn(`500 api_error retry-after-ms=4290`), agentEnd}, 0},
		// No provider-stated wait.
		{"429 without retry-after", []string{failedTurn(limit429), agentEnd}, 0},
		{"omp's guessed wait is not a provider wait", []string{failedTurn(limit429), retryStart,
			gaveUp(`Provider requested 1800000ms wait, exceeds retry.maxDelayMs (300000ms). Original error: ` + limit429), agentEnd}, 0},
		{"typed usage limit with only omp's guess", []string{failedTurn(`Anthropic stream error (rate_limit_error): You have reached your usage limit.`), retryStart,
			gaveUp(`Provider requested 1800000ms wait, exceeds retry.maxDelayMs (300000ms). Original error: Anthropic stream error (rate_limit_error): You have reached your usage limit.`), agentEnd}, 0},
		{"typed usage limit whose message ends in a wait token", []string{failedTurn(`Anthropic stream error (rate_limit_error): You have reached your usage limit. see retry-after-ms=900000`), agentEnd}, 0},
		{"wait text inside the JSON body is not the wait", []string{failedTurn(`Anthropic stream error (api_error): 429 {"type":"error","error":{"type":"rate_limit_error","message":"see retry-after-ms=900000 docs"}}`), agentEnd}, 0},
		{"anything after the wait", []string{failedTurn(limitTurn + ` (later)`), agentEnd}, 0},
		{"gave up on a 429 without a wait", []string{retryStart, gaveUp(`429 rate_limit_error`), agentEnd}, 0},
		// Only the failure that ended the run counts; nothing carries over.
		{"wait never carries over to a separate request", []string{failedTurn(limitTurn), failedTurn(limit429), agentEnd}, 0},
		{"a terminal retry end without a wait ends it", []string{failedTurn(limitTurn), retryStart, gaveUp(limit429), agentEnd}, 0},
		{"limit, then a terminal 503 turn", []string{failedTurn(limitTurn), retryStart, failedTurn(overloaded503), agentEnd}, 0},
		{"limit, then omp gave up on a 503", []string{failedTurn(limitTurn), retryStart, gaveUp(overloaded503), agentEnd}, 0},
		{"local retry failure after a limit", []string{failedTurn(limitTurn), retryStart, gaveUp(`Retry continuation failed locally: socket closed. Original error: ` + limitTurn), agentEnd}, 0},
		{"recovered", []string{failedTurn(limitTurn), retryStart, `{"type":"auto_retry_end","success":true}`, answerMsg, agentEnd}, 0},
		{"recovered, then another failure", []string{failedTurn(limitTurn), answerMsg, failedTurn("500 overloaded"), agentEnd}, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := feed(c.lines...)
			got, ok := s.UsageLimit()
			if ok != (c.want != 0) || got != c.want {
				t.Fatalf("UsageLimit() = %v, %v; want %v", got, ok, c.want)
			}
			if _, err := s.Result(); c.want != 0 && err == nil {
				t.Fatal("a usage limit must not be a result")
			}
		})
	}
}

func TestExtractJSON(t *testing.T) {
	ok := map[string]string{
		`{"a":1}`:                          `{"a":1}`,
		"  [1,2]  \n":                      `[1,2]`,
		"Here:\n```json\n{\"a\":1}\n```\n": `{"a":1}`,
		"```\n[1]\n```":                    `[1]`,
	}
	for in, want := range ok {
		got, err := ExtractJSON(in)
		if err != nil || string(got) != want+"\n" {
			t.Errorf("ExtractJSON(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	bad := []string{
		"I could not find anything.",
		"```json\n{\"a\":\n```",
		"```json\n{}\n```\nand\n```json\n[]\n```",
		"```json\n{}\n",
	}
	for _, in := range bad {
		if got, err := ExtractJSON(in); err == nil {
			t.Errorf("ExtractJSON(%q) = %q, want error", in, got)
		}
	}
}

func TestArgsAllowOnlyDeclaredReadOnlyTools(t *testing.T) {
	st := spec.Stage{Agent: "omp", Model: "anthropic/claude-opus-5-5", Effort: "high", Tools: []string{"read"}}
	args, err := Args(st, 10*time.Minute, "do it")
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(args, " ")
	for _, want := range []string{"-p", "--mode=json", "--no-session", "--tools=read", "--model=anthropic/claude-opus-5-5", "--thinking=high", "--max-time=600", "-- do it"} {
		if !strings.Contains(joined, want) {
			t.Errorf("args %q lack %q", joined, want)
		}
	}
	if args[len(args)-1] != "do it" || args[len(args)-2] != "--" {
		t.Error("prompt must come after --")
	}
	if _, err := Args(st, 0, strings.Repeat("x", MaxPromptBytes+1)); err == nil {
		t.Error("oversized prompt accepted")
	}
}
