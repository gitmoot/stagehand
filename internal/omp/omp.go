// Package omp builds the headless omp command line for an agent stage and
// reads omp's `--mode json` event stream: usage and cost per assistant
// message, whether the run really succeeded (omp exits 0 even when a turn
// fails), and the final answer.
//
// The success rules are ported from Gitmoot's omp adapter
// (internal/runtime/omp.go, parseOmpStreamJSONWithProvider, at 1b124f60),
// measured against omp/18.10.4.
package omp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/gitmoot/stagehand/internal/spec"
)

// MaxPromptBytes bounds the prompt passed on the command line.
const MaxPromptBytes = 96 * 1024

// MinMaxTime is the smallest --max-time stagehand passes.
const MinMaxTime = 30 * time.Second

// Args returns omp's arguments (without the binary) for a stage. maxTime is
// passed as --max-time so omp can stop and report before the runner kills
// it; prompt goes after "--".
func Args(st spec.Stage, maxTime time.Duration, prompt string) ([]string, error) {
	if len(prompt) > MaxPromptBytes {
		return nil, fmt.Errorf("prompt is %d bytes; the limit is %d (put large data in an input file instead)", len(prompt), MaxPromptBytes)
	}
	args := []string{
		"-p", "--mode=json", "--no-session", "--no-title",
		"--no-skills", "--no-rules", "--no-extensions", "--no-lsp", "--no-pty",
		"--tools=" + strings.Join(st.AgentTools(), ","),
	}
	if st.Model != "" {
		args = append(args, "--model="+st.Model)
	}
	if st.Effort != "" {
		args = append(args, "--thinking="+st.Effort)
	}
	if maxTime > 0 {
		if maxTime < MinMaxTime {
			maxTime = MinMaxTime
		}
		args = append(args, fmt.Sprintf("--max-time=%d", int(maxTime/time.Second)))
	}
	return append(args, "--", prompt), nil
}

// MaxTimeFor is 90% of the time left before deadline, so omp can write its
// final events before the stage is killed.
func MaxTimeFor(deadline time.Time, now time.Time) time.Duration {
	left := deadline.Sub(now)
	if left <= 0 {
		return MinMaxTime
	}
	return time.Duration(float64(left) * 0.9)
}

type event struct {
	Type       string   `json:"type"`
	Message    *message `json:"message"`
	IsTerminal *bool    `json:"isTerminal"`
	Success    *bool    `json:"success"`
	FinalError string   `json:"finalError"`
}

type message struct {
	Role         string          `json:"role"`
	Content      json.RawMessage `json:"content"`
	Usage        *usage          `json:"usage"`
	StopReason   string          `json:"stopReason"`
	ErrorMessage string          `json:"errorMessage"`
	Model        string          `json:"model"`
}

type part struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type usage struct {
	Input      int64 `json:"input"`
	Output     int64 `json:"output"`
	CacheRead  int64 `json:"cacheRead"`
	CacheWrite int64 `json:"cacheWrite"`
	Cost       *struct {
		Total float64 `json:"total"`
	} `json:"cost"`
}

// Stream accumulates omp's event stream, one line at a time.
type Stream struct {
	cost         float64
	inputTokens  int64
	outputTokens int64
	messages     int

	finalText    string
	finalStop    string
	sawText      bool
	firstErr     error
	turnFailed   bool
	gaveUp       bool
	retryPending bool
	agentEnds    int
	lastTerminal bool
	unreadable   error
}

// Feed reads one line of omp's stdout. Lines that are not JSON or not one of
// the event types that decide the outcome are ignored, except a cut-off
// assistant message_end, which makes the run unreadable.
func (s *Stream) Feed(line []byte) {
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return
	}
	var ev event
	if err := json.Unmarshal(line, &ev); err != nil {
		if s.unreadable == nil && bytes.Contains(line, []byte(`"message_end"`)) && bytes.Contains(line, []byte(`"assistant"`)) {
			s.unreadable = fmt.Errorf("omp wrote an unreadable assistant message: %v", err)
		}
		return
	}
	switch ev.Type {
	case "message_end":
		m := ev.Message
		if m == nil || m.Role != "assistant" {
			return
		}
		s.messages++
		if u := m.Usage; u != nil {
			s.inputTokens += u.Input + u.CacheRead + u.CacheWrite
			s.outputTokens += u.Output
			if u.Cost != nil && !math.IsNaN(u.Cost.Total) && u.Cost.Total > 0 {
				s.cost += u.Cost.Total
			}
		}
		parts := decodeParts(m.Content)
		if err := assistantError(m); err != nil {
			if s.firstErr == nil {
				s.firstErr = err
			}
			s.turnFailed = true
		} else if !s.gaveUp && carriesContent(parts) {
			// A later real answer recovers from an earlier failed turn
			// (omp retried or fell back to another model).
			s.turnFailed = false
			s.firstErr = nil
		}
		text := textOf(parts)
		if strings.TrimSpace(text) != "" {
			s.sawText = true
		}
		s.finalText = text
		s.finalStop = m.StopReason
	case "auto_retry_start":
		s.retryPending = true
	case "auto_retry_end":
		s.retryPending = false
		if ev.Success != nil && !*ev.Success {
			s.turnFailed = true
			s.gaveUp = true
			if s.firstErr == nil {
				detail := strings.TrimSpace(ev.FinalError)
				if detail == "" {
					detail = "no finalError reported"
				}
				s.firstErr = fmt.Errorf("omp gave up retrying: %s", detail)
			}
		}
	case "agent_end":
		s.agentEnds++
		s.lastTerminal = ev.IsTerminal == nil || *ev.IsTerminal
	}
}

// Cost is the summed list-price cost of every assistant message so far.
func (s *Stream) Cost() float64 { return s.cost }

// Tokens returns prompt tokens (input plus cache reads and writes) and output
// tokens so far.
func (s *Stream) Tokens() (input, output int64) { return s.inputTokens, s.outputTokens }

// Messages is the number of assistant messages seen.
func (s *Stream) Messages() int { return s.messages }

// Result returns the final answer, or why the run did not produce one.
func (s *Stream) Result() (string, error) {
	switch {
	case s.unreadable != nil:
		return "", s.unreadable
	case s.turnFailed:
		return "", s.firstErr
	case s.agentEnds == 0:
		return "", errors.New("omp stopped without finishing (no agent_end event)")
	case !s.lastTerminal:
		return "", errors.New("omp stopped while scheduling a continuation (retry, fallback or compaction)")
	case s.retryPending:
		return "", errors.New("omp stopped in the middle of a retry")
	case s.finalStop == "length" || s.finalStop == "toolUse":
		return "", fmt.Errorf("omp's final message was cut off (stopReason %q): it never wrote a final answer", s.finalStop)
	case strings.TrimSpace(s.finalText) == "":
		if s.sawText {
			return "", errors.New("omp's final message had no text")
		}
		return "", errors.New("omp produced no answer")
	}
	return s.finalText, nil
}

func decodeParts(raw json.RawMessage) []part {
	var parts []part
	if len(raw) > 0 && raw[0] == '[' {
		_ = json.Unmarshal(raw, &parts)
	}
	return parts
}

func textOf(parts []part) string {
	var b strings.Builder
	for _, p := range parts {
		if p.Type == "text" {
			b.WriteString(p.Text)
		}
	}
	return b.String()
}

func carriesContent(parts []part) bool {
	for _, p := range parts {
		switch p.Type {
		case "toolCall":
			return true
		case "text":
			if strings.TrimSpace(p.Text) != "" {
				return true
			}
		}
	}
	return false
}

func assistantError(m *message) error {
	if m.StopReason != "error" && m.StopReason != "aborted" {
		return nil
	}
	detail := strings.TrimSpace(m.ErrorMessage)
	if detail == "" {
		detail = "no errorMessage reported"
	}
	return fmt.Errorf("omp turn failed (stopReason %s): %s", m.StopReason, detail)
}

// ExtractJSON returns the JSON document in an agent's final answer: either
// the whole answer, or the body of its single fenced block. The bytes are
// returned as written, after validation, with a trailing newline.
func ExtractJSON(answer string) ([]byte, error) {
	trimmed := strings.TrimSpace(answer)
	if json.Valid([]byte(trimmed)) {
		return []byte(trimmed + "\n"), nil
	}
	blocks := fencedBlocks(trimmed)
	switch len(blocks) {
	case 0:
		return nil, errors.New("the final answer is not JSON and has no ```json block")
	case 1:
		body := strings.TrimSpace(blocks[0])
		var v any
		if err := json.Unmarshal([]byte(body), &v); err != nil {
			return nil, fmt.Errorf("the ```json block in the final answer is not valid JSON: %v", err)
		}
		return []byte(body + "\n"), nil
	default:
		return nil, fmt.Errorf("the final answer has %d fenced blocks; expected exactly one JSON block", len(blocks))
	}
}

// fencedBlocks returns the bodies of ``` fenced blocks, whatever their
// language tag, so an answer mixing a JSON block with others is rejected.
func fencedBlocks(s string) []string {
	var out []string
	var body []string
	inBlock := false
	for line := range strings.SplitSeq(s, "\n") {
		fence := strings.HasPrefix(strings.TrimSpace(line), "```")
		switch {
		case !inBlock && fence:
			inBlock, body = true, nil
		case inBlock && strings.TrimSpace(line) == "```":
			inBlock = false
			out = append(out, strings.Join(body, "\n"))
		case inBlock:
			body = append(body, line)
		}
	}
	return out
}
