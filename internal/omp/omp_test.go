package omp

import (
	"bufio"
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
