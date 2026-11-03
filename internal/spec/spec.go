// Package spec defines the stagehand pipeline file: a small DAG of shell and
// agent stages plus an optional interval schedule. A spec is stored verbatim
// with its SHA-256; each run snapshots the bytes it was created from, so a run
// always executes the spec it started with.
//
// The shape is a subset of Gitmoot's `gitmoot pipeline` spec (name, schedule,
// env_file, env, stages, needs, timeout, retry, env_keys, cmd) plus the keys a
// standalone runner needs (cwd, and the omp agent keys).
package spec

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	yaml "gopkg.in/yaml.v3"
)

// Kind names what executes a stage.
type Kind string

const (
	KindShell Kind = "shell"
	KindAgent Kind = "agent"
)

// AgentOmp is the only agent harness in v1.
const AgentOmp = "omp"

// DefaultTimeout bounds a shell stage that sets no timeout: every stage has a
// time limit. Agent stages must set their own.
const DefaultTimeout = time.Hour

// MaxRetry bounds retry so a typo cannot loop a broken stage for hours.
const MaxRetry = 10

// MinInterval stops a schedule from hot-looping.
const MinInterval = time.Minute

// MinAtYear and MaxAtYear bound schedule.at.
const (
	MinAtYear = 2000
	MaxAtYear = 2200
)

// DefaultAgentTools are the omp tools an agent stage gets when tools is unset:
// read (local files inside the sandbox and URL fetch) and web search. No tool
// that writes or executes is ever allowed.
var DefaultAgentTools = []string{"read", "web_search"}

// AllowedAgentTools are the read-only omp tools a stage may list.
var AllowedAgentTools = []string{"read", "web_search", "grep", "glob"}

// AgentEfforts are omp's --thinking levels.
var AgentEfforts = []string{"off", "minimal", "low", "medium", "high", "xhigh", "max", "auto"}

// Spec is one pipeline file.
type Spec struct {
	Name        string            `yaml:"name"`
	Description string            `yaml:"description,omitempty"`
	Schedule    *Schedule         `yaml:"schedule,omitempty"`
	EnvFile     string            `yaml:"env_file,omitempty"`
	Env         map[string]string `yaml:"env,omitempty"`
	Stages      []Stage           `yaml:"stages"`
}

// Schedule fires a run every Interval. Jitter adds a random [0, jitter) delay
// to each due time. At, when set, anchors the due times to At + k*Interval (for
// example a fixed weekday and hour); without it a freshly enabled pipeline runs
// at once and then every Interval. Missed slots collapse into one run.
type Schedule struct {
	Interval string `yaml:"interval" json:"interval"`
	Jitter   string `yaml:"jitter,omitempty" json:"jitter,omitempty"`
	At       string `yaml:"at,omitempty" json:"at,omitempty"`
}

// Stage is one node of the DAG. Exactly one of Cmd (shell) or Agent is set.
type Stage struct {
	ID      string   `yaml:"id"`
	Needs   []string `yaml:"needs,omitempty"`
	Timeout string   `yaml:"timeout,omitempty"`
	Retry   int      `yaml:"retry,omitempty"`
	EnvKeys []string `yaml:"env_keys,omitempty"`

	// Shell stage.
	Cmd string `yaml:"cmd,omitempty"`
	Cwd string `yaml:"cwd,omitempty"`

	// Agent stage.
	Agent      string   `yaml:"agent,omitempty"`
	Model      string   `yaml:"model,omitempty"`
	Effort     string   `yaml:"effort,omitempty"`
	Prompt     string   `yaml:"prompt,omitempty"`
	PromptFile string   `yaml:"prompt_file,omitempty"`
	Inputs     []string `yaml:"inputs,omitempty"`
	Output     string   `yaml:"output,omitempty"`
	Tools      []string `yaml:"tools,omitempty"`
	MaxCost    float64  `yaml:"max_cost,omitempty"`
}

// Kind classifies a validated stage.
func (s Stage) Kind() Kind {
	if s.Agent != "" {
		return KindAgent
	}
	return KindShell
}

// TimeoutDuration is the stage time limit (DefaultTimeout when unset).
func (s Stage) TimeoutDuration() time.Duration {
	if s.Timeout == "" {
		return DefaultTimeout
	}
	d, err := time.ParseDuration(s.Timeout)
	if err != nil || d <= 0 {
		return DefaultTimeout
	}
	return d
}

// AgentTools is the stage's tool list (DefaultAgentTools when unset).
func (s Stage) AgentTools() []string {
	if len(s.Tools) == 0 {
		return slices.Clone(DefaultAgentTools)
	}
	return slices.Clone(s.Tools)
}

// Load parses, normalizes and validates raw YAML. Unknown keys are errors so a
// typo fails at add time instead of being ignored.
func Load(raw []byte) (Spec, error) {
	var s Spec
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&s); err != nil {
		return Spec{}, fmt.Errorf("parse spec: %w", err)
	}
	if err := s.Validate(); err != nil {
		return Spec{}, err
	}
	return s, nil
}

// Hash is the hex SHA-256 of the verbatim spec bytes.
func Hash(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

var (
	nameRE   = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)
	stageRE  = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)
	envKeyRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	fileRE   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
)

// Validate checks the structure. It reads no files and touches no state.
func (s Spec) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if !nameRE.MatchString(s.Name) {
		add("name %q: use 1-64 lowercase letters, digits, '.', '_' or '-', starting with a letter or digit", s.Name)
	}
	if s.EnvFile != "" && !filepath.IsAbs(s.EnvFile) {
		add("env_file %q must be an absolute path", s.EnvFile)
	}
	for k := range s.Env {
		if err := validEnvKey(k); err != nil {
			add("env: %v", err)
		}
	}
	if s.Schedule != nil {
		if err := s.Schedule.validate(); err != nil {
			add("schedule: %v", err)
		}
	}
	if len(s.Stages) == 0 {
		add("stages: at least one stage is required")
	}

	ids := map[string]bool{}
	for i, st := range s.Stages {
		label := fmt.Sprintf("stages[%d]", i)
		if st.ID != "" {
			label = fmt.Sprintf("stage %q", st.ID)
		}
		if !stageRE.MatchString(st.ID) {
			add("%s: id %q: use 1-64 lowercase letters, digits, '_' or '-'", label, st.ID)
		} else if ids[st.ID] {
			add("%s: duplicate id", label)
		}
		ids[st.ID] = true
		for _, e := range st.validate() {
			add("%s: %v", label, e)
		}
	}
	for _, st := range s.Stages {
		seen := map[string]bool{}
		for _, n := range st.Needs {
			switch {
			case n == st.ID:
				add("stage %q: needs itself", st.ID)
			case !ids[n]:
				add("stage %q: needs unknown stage %q", st.ID, n)
			case seen[n]:
				add("stage %q: needs %q twice", st.ID, n)
			}
			seen[n] = true
		}
	}
	if len(errs) == 0 {
		if cycle := s.findCycle(); cycle != nil {
			add("stages: dependency cycle %s", strings.Join(cycle, " -> "))
		}
	}
	return errors.Join(errs...)
}

func (sc Schedule) validate() error {
	interval, err := time.ParseDuration(sc.Interval)
	if err != nil {
		return fmt.Errorf("interval %q: %v", sc.Interval, err)
	}
	if interval < MinInterval {
		return fmt.Errorf("interval %s is shorter than %s", interval, MinInterval)
	}
	if sc.Jitter != "" {
		jitter, err := time.ParseDuration(sc.Jitter)
		if err != nil {
			return fmt.Errorf("jitter %q: %v", sc.Jitter, err)
		}
		if jitter < 0 || jitter >= interval {
			return fmt.Errorf("jitter %s must be at least 0 and less than the interval", jitter)
		}
	}
	if sc.At != "" {
		at, err := time.Parse(time.RFC3339, sc.At)
		if err != nil {
			return fmt.Errorf("at %q: use RFC 3339, e.g. 2026-11-08T03:00:00+01:00", sc.At)
		}
		// Bounded so slot arithmetic stays far from time.Duration's ±292y.
		if at.Year() < MinAtYear || at.Year() > MaxAtYear {
			return fmt.Errorf("at %q: the year must be between %d and %d", sc.At, MinAtYear, MaxAtYear)
		}
	}
	return nil
}

// Durations returns the parsed schedule. Call only on a validated spec.
func (sc Schedule) Durations() (interval, jitter time.Duration, at time.Time) {
	interval, _ = time.ParseDuration(sc.Interval)
	if sc.Jitter != "" {
		jitter, _ = time.ParseDuration(sc.Jitter)
	}
	if sc.At != "" {
		at, _ = time.Parse(time.RFC3339, sc.At)
	}
	return interval, jitter, at
}

func (st Stage) validate() []error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if st.Timeout != "" {
		if d, err := time.ParseDuration(st.Timeout); err != nil || d <= 0 {
			add("timeout %q must be a positive duration such as 10m", st.Timeout)
		}
	}
	if st.Retry < 0 || st.Retry > MaxRetry {
		add("retry %d must be between 0 and %d", st.Retry, MaxRetry)
	}
	for _, k := range st.EnvKeys {
		if err := validEnvKey(k); err != nil {
			add("env_keys: %v", err)
		}
	}

	switch {
	case st.Cmd != "" && st.Agent != "":
		add("set either cmd (shell stage) or agent, not both")
	case st.Cmd == "" && st.Agent == "":
		add("set cmd (shell stage) or agent")
	case st.Cmd != "":
		if st.Cwd != "" && !filepath.IsAbs(st.Cwd) {
			add("cwd %q must be an absolute path", st.Cwd)
		}
		for key, set := range map[string]bool{
			"model": st.Model != "", "effort": st.Effort != "", "prompt": st.Prompt != "",
			"prompt_file": st.PromptFile != "", "inputs": len(st.Inputs) > 0, "output": st.Output != "",
			"tools": len(st.Tools) > 0, "max_cost": st.MaxCost != 0,
		} {
			if set {
				add("%s is only for agent stages", key)
			}
		}
	default:
		errs = append(errs, st.validateAgent()...)
	}
	return errs
}

func (st Stage) validateAgent() []error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if st.Agent != AgentOmp {
		add("agent %q: only %q is supported", st.Agent, AgentOmp)
	}
	if st.Cwd != "" {
		add("cwd is only for shell stages")
	}
	if len(st.EnvKeys) > 0 {
		add("env_keys: agent stages receive no environment or secrets")
	}
	if (st.Prompt == "") == (st.PromptFile == "") {
		add("set exactly one of prompt or prompt_file")
	}
	if st.Output == "" {
		add("output is required: the JSON file the runner writes from the agent's final answer")
	} else if err := validFileName(st.Output); err != nil {
		add("output: %v", err)
	}
	for _, in := range st.Inputs {
		if err := validFileName(in); err != nil {
			add("inputs: %v", err)
		}
		if in == st.Output {
			add("output %q is also an input; the runner replaces output, so give it its own name", in)
		}
	}
	if st.Timeout == "" {
		add("timeout is required for agent stages")
	}
	if st.MaxCost <= 0 {
		add("max_cost is required for agent stages and must be positive")
	}
	if st.Effort != "" && !slices.Contains(AgentEfforts, st.Effort) {
		add("effort %q: use one of %s", st.Effort, strings.Join(AgentEfforts, ", "))
	}
	for _, t := range st.Tools {
		if !slices.Contains(AllowedAgentTools, t) {
			add("tools: %q is not allowed; agent stages may use only %s", t, strings.Join(AllowedAgentTools, ", "))
		}
	}
	return errs
}

func validEnvKey(k string) error {
	if !envKeyRE.MatchString(k) {
		return fmt.Errorf("%q is not a valid variable name", k)
	}
	if strings.HasPrefix(strings.ToUpper(k), "STAGEHAND_") {
		return fmt.Errorf("%q: the STAGEHAND_ prefix is reserved", k)
	}
	return nil
}

// validFileName accepts a plain file name in the run work directory.
func validFileName(name string) error {
	if !fileRE.MatchString(name) || strings.Contains(name, "..") {
		return fmt.Errorf("%q must be a plain file name (letters, digits, '.', '_', '-'), no directories", name)
	}
	return nil
}

// Stage returns the stage with the given id.
func (s Spec) Stage(id string) (Stage, bool) {
	for _, st := range s.Stages {
		if st.ID == id {
			return st, true
		}
	}
	return Stage{}, false
}

// Dependents returns id and every stage that transitively needs it, in spec
// order.
func (s Spec) Dependents(id string) []string {
	set := map[string]bool{id: true}
	for changed := true; changed; {
		changed = false
		for _, st := range s.Stages {
			if set[st.ID] {
				continue
			}
			for _, n := range st.Needs {
				if set[n] {
					set[st.ID] = true
					changed = true
					break
				}
			}
		}
	}
	var out []string
	for _, st := range s.Stages {
		if set[st.ID] {
			out = append(out, st.ID)
		}
	}
	return out
}

func (s Spec) findCycle() []string {
	needs := map[string][]string{}
	for _, st := range s.Stages {
		needs[st.ID] = st.Needs
	}
	const (
		unvisited = iota
		active
		done
	)
	state := map[string]int{}
	var stack []string
	var visit func(string) []string
	visit = func(id string) []string {
		state[id] = active
		stack = append(stack, id)
		for _, n := range needs[id] {
			switch state[n] {
			case active:
				i := slices.Index(stack, n)
				return append(slices.Clone(stack[i:]), n)
			case unvisited:
				if c := visit(n); c != nil {
					return c
				}
			}
		}
		stack = stack[:len(stack)-1]
		state[id] = done
		return nil
	}
	for _, st := range s.Stages {
		if state[st.ID] == unvisited {
			if c := visit(st.ID); c != nil {
				return c
			}
		}
	}
	return nil
}
