package engine

import (
	"context"
	"encoding/json"
	"time"

	"github.com/gitmoot/stagehand/internal/spec"
	"github.com/gitmoot/stagehand/internal/store"
)

// Status is the machine-readable overview served by `status --json` and
// GET /status.
type Status struct {
	GeneratedAt time.Time        `json:"generated_at"`
	Version     string           `json:"version"`
	Pipelines   []PipelineStatus `json:"pipelines"`
}

// PipelineStatus describes one pipeline.
type PipelineStatus struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Enabled     bool           `json:"enabled"`
	Schedule    *spec.Schedule `json:"schedule,omitempty"`
	NextDueAt   *time.Time     `json:"next_due_at"`
	ActiveRunID *int64         `json:"active_run_id"`
	// Events is set when the pipeline accepts events.
	Events       *spec.Events `json:"events,omitempty"`
	QueuedEvents int          `json:"queued_events"`
	LastRun      *RunSummary  `json:"last_run"`
	SpecHash     string       `json:"spec_hash"`
	Error        string       `json:"error,omitempty"`
}

// RunSummary is a run without its stage detail.
type RunSummary struct {
	ID           int64      `json:"id"`
	Pipeline     string     `json:"pipeline"`
	Trigger      string     `json:"trigger"`
	State        string     `json:"state"`
	CreatedAt    time.Time  `json:"created_at"`
	FinishedAt   *time.Time `json:"finished_at"`
	HaltStage    string     `json:"halt_stage,omitempty"`
	HaltReason   string     `json:"halt_reason,omitempty"`
	Cost         float64    `json:"cost"`
	InputTokens  int64      `json:"input_tokens"`
	OutputTokens int64      `json:"output_tokens"`
	// Event is set for event-triggered runs.
	Event *EventRef `json:"event,omitempty"`
	// FilesDeletedAt: the run's files and payload were deleted (retention).
	FilesDeletedAt *time.Time `json:"files_deleted_at,omitempty"`
}

// EventRef identifies the event behind a run. The payload itself is never
// included, only a reference to it.
type EventRef struct {
	ID            int64      `json:"id"`
	Key           string     `json:"key"`
	Source        string     `json:"source"`
	ReceivedAt    time.Time  `json:"received_at"`
	ContentType   string     `json:"content_type,omitempty"`
	PayloadBytes  int64      `json:"payload_bytes"`
	PayloadSHA256 string     `json:"payload_sha256"`
	PayloadPath   string     `json:"payload_path,omitempty"`
	DeletedAt     *time.Time `json:"payload_deleted_at,omitempty"`
}

// EventRefOf describes an event without its payload.
func EventRefOf(ev store.Event) *EventRef {
	ref := &EventRef{ID: ev.ID, Key: ev.Key, Source: ev.Source, ReceivedAt: ev.ReceivedAt, ContentType: ev.ContentType,
		PayloadBytes: ev.PayloadBytes, PayloadSHA256: ev.PayloadSHA256, DeletedAt: ev.PayloadDeletedAt}
	if ev.PayloadDeletedAt == nil {
		ref.PayloadPath = ev.PayloadPath
	}
	return ref
}

// RunDetail is a run with its stages and attempts.
type RunDetail struct {
	RunSummary
	SpecHash        string        `json:"spec_hash"`
	CancelRequested bool          `json:"cancel_requested,omitempty"`
	Stages          []StageDetail `json:"stages"`
}

// StageDetail is one stage of a run.
type StageDetail struct {
	ID         string          `json:"id"`
	Kind       spec.Kind       `json:"kind"`
	State      string          `json:"state"`
	Attempts   []AttemptDetail `json:"attempts"`
	Summary    string          `json:"summary,omitempty"`
	Needs      []string        `json:"needs,omitempty"`
	StartedAt  *time.Time      `json:"started_at"`
	FinishedAt *time.Time      `json:"finished_at"`
}

// AttemptDetail is one attempt of a stage.
type AttemptDetail struct {
	Attempt      int        `json:"attempt"`
	State        string     `json:"state"`
	ExitCode     *int       `json:"exit_code"`
	Cost         float64    `json:"cost"`
	InputTokens  int64      `json:"input_tokens"`
	OutputTokens int64      `json:"output_tokens"`
	LogPath      string     `json:"log_path"`
	Resource     string     `json:"resource,omitempty"`
	Summary      string     `json:"summary,omitempty"`
	StartedAt    time.Time  `json:"started_at"`
	FinishedAt   *time.Time `json:"finished_at"`
}

// Status reports every pipeline with its schedule, next due time and newest
// run.
func (e *Engine) Status(ctx context.Context, version string) (Status, error) {
	pipelines, err := e.Store.ListPipelines(ctx)
	if err != nil {
		return Status{}, err
	}
	out := Status{GeneratedAt: e.now(), Version: version, Pipelines: []PipelineStatus{}}
	for _, p := range pipelines {
		ps := PipelineStatus{Name: p.Name, Enabled: p.Enabled, NextDueAt: p.NextDueAt, SpecHash: p.SpecHash}
		if sp, err := spec.Load(p.SpecYAML); err != nil {
			ps.Error = err.Error()
		} else {
			ps.Description, ps.Schedule, ps.Events = sp.Description, sp.Schedule, sp.Events
		}
		last, err := e.Store.LastRun(ctx, p.Name)
		if err != nil {
			return Status{}, err
		}
		if last != nil {
			sum, err := e.summarize(ctx, *last)
			if err != nil {
				return Status{}, err
			}
			ps.LastRun = &sum
		}
		if ps.QueuedEvents, err = e.Store.QueuedEventCount(ctx, p.Name); err != nil {
			return Status{}, err
		}
		active, err := e.Store.ActiveRunID(ctx, p.Name)
		if err != nil {
			return Status{}, err
		}
		if active != 0 {
			ps.ActiveRunID = &active
		}
		out.Pipelines = append(out.Pipelines, ps)
	}
	return out, nil
}

// Runs lists a pipeline's newest runs.
func (e *Engine) Runs(ctx context.Context, pipeline string, limit int) ([]RunSummary, error) {
	runs, err := e.Store.ListRuns(ctx, pipeline, limit)
	if err != nil {
		return nil, err
	}
	out := []RunSummary{}
	for _, r := range runs {
		s, err := e.summarize(ctx, r)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

func (e *Engine) summarize(ctx context.Context, r store.Run) (RunSummary, error) {
	cost, in, out, err := e.Store.RunCost(ctx, r.ID)
	if err != nil {
		return RunSummary{}, err
	}
	sum := RunSummary{
		ID: r.ID, Pipeline: r.Pipeline, Trigger: r.Trigger, State: r.State, CreatedAt: r.CreatedAt,
		FinishedAt: r.FinishedAt, HaltStage: r.HaltStage, HaltReason: r.HaltReason,
		Cost: cost, InputTokens: in, OutputTokens: out, FilesDeletedAt: r.FilesDeletedAt,
	}
	if r.EventID != 0 {
		ev, err := e.Store.GetEvent(ctx, r.EventID)
		if err != nil {
			return RunSummary{}, err
		}
		sum.Event = EventRefOf(ev)
	}
	return sum, nil
}

// Run returns one run with its stages and attempts.
func (e *Engine) Run(ctx context.Context, id int64) (RunDetail, error) {
	r, err := e.Store.GetRun(ctx, id)
	if err != nil {
		return RunDetail{}, err
	}
	sum, err := e.summarize(ctx, r)
	if err != nil {
		return RunDetail{}, err
	}
	stages, err := e.Store.ListStages(ctx, id)
	if err != nil {
		return RunDetail{}, err
	}
	attempts, err := e.Store.ListAttempts(ctx, id)
	if err != nil {
		return RunDetail{}, err
	}
	sp, _ := spec.Load(r.SpecYAML)
	byStage := map[string][]AttemptDetail{}
	for _, a := range attempts {
		byStage[a.StageID] = append(byStage[a.StageID], AttemptDetail{
			Attempt: a.Attempt, State: a.State, ExitCode: a.ExitCode, Cost: a.Cost,
			InputTokens: a.InputTokens, OutputTokens: a.OutputTokens, LogPath: a.LogPath,
			Resource: a.Resource, Summary: a.Summary, StartedAt: a.StartedAt, FinishedAt: a.FinishedAt,
		})
	}
	d := RunDetail{RunSummary: sum, SpecHash: r.SpecHash, CancelRequested: r.CancelRequested, Stages: []StageDetail{}}
	for _, st := range stages {
		sd := StageDetail{ID: st.StageID, State: st.State, Summary: st.Summary, StartedAt: st.StartedAt, FinishedAt: st.FinishedAt,
			Attempts: byStage[st.StageID]}
		if sd.Attempts == nil {
			sd.Attempts = []AttemptDetail{}
		}
		if stg, ok := sp.Stage(st.StageID); ok {
			sd.Kind = stg.Kind()
		}
		if st.NeedsJSON != "" {
			_ = json.Unmarshal([]byte(st.NeedsJSON), &sd.Needs)
		}
		d.Stages = append(d.Stages, sd)
	}
	return d, nil
}
