// Package store keeps stagehand's state in one SQLite file (pure-Go driver, no
// cgo): registered pipelines, runs with their spec snapshot, per-stage state
// and every stage attempt.
//
// Several processes may open the store at once (the `serve` loop plus CLI
// commands). Only the process holding the home lock executes stages; others
// only create runs, request cancellation or resume parked runs, each in a
// single transaction.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Run states.
const (
	RunRunning   = "running"
	RunSucceeded = "succeeded"
	RunBlocked   = "blocked"
	RunFailed    = "failed"
	RunCancelled = "cancelled"
)

// Stage states.
const (
	StagePending   = "pending"
	StageRunning   = "running"
	StageSucceeded = "succeeded"
	StageBlocked   = "blocked"
	StageFailed    = "failed"
	StageSkipped   = "skipped"
	StageCancelled = "cancelled"
)

// Attempt states. Interrupted means the runner stopped or crashed while the
// attempt ran; it counts as a failure that parks the run without retry.
const (
	AttemptRunning     = "running"
	AttemptSucceeded   = "succeeded"
	AttemptBlocked     = "blocked"
	AttemptFailed      = "failed"
	AttemptCancelled   = "cancelled"
	AttemptInterrupted = "interrupted"
)

var (
	// ErrNotFound is returned when a pipeline or run does not exist.
	ErrNotFound = errors.New("not found")
	// ErrExists is returned when adding a pipeline whose name is taken.
	ErrExists = errors.New("already exists")
	// ErrActiveRun is returned when a pipeline already has a running run.
	ErrActiveRun = errors.New("pipeline already has an active run")
)

// Store is a handle on the state database.
type Store struct {
	db *sql.DB
}

// Open opens (creating if needed) the database at path and applies
// migrations.
func Open(ctx context.Context, path string) (*Store, error) {
	dsn := "file:" + path + "?_txlock=immediate" +
		"&_pragma=busy_timeout(15000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// One connection: SQLite serializes writers anyway, and a single
	// connection keeps goroutines in this process from deadlocking each other.
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.migrate(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate %s: %w", path, err)
	}
	return s, nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

var migrations = []string{
	`CREATE TABLE pipelines (
		name        TEXT PRIMARY KEY,
		spec_yaml   BLOB NOT NULL,
		spec_hash   TEXT NOT NULL,
		source_dir  TEXT NOT NULL,
		enabled     INTEGER NOT NULL DEFAULT 0,
		next_due_at TEXT,
		created_at  TEXT NOT NULL,
		updated_at  TEXT NOT NULL
	);
	CREATE TABLE runs (
		id               INTEGER PRIMARY KEY AUTOINCREMENT,
		pipeline         TEXT NOT NULL,
		trigger          TEXT NOT NULL,
		spec_yaml        BLOB NOT NULL,
		spec_hash        TEXT NOT NULL,
		source_dir       TEXT NOT NULL,
		state            TEXT NOT NULL,
		halt_stage       TEXT NOT NULL DEFAULT '',
		halt_reason      TEXT NOT NULL DEFAULT '',
		cancel_requested INTEGER NOT NULL DEFAULT 0,
		created_at       TEXT NOT NULL,
		finished_at      TEXT
	);
	CREATE INDEX runs_by_pipeline ON runs(pipeline, id);
	CREATE UNIQUE INDEX runs_one_active ON runs(pipeline) WHERE state = 'running';
	CREATE TABLE run_stages (
		run_id      INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
		stage_id    TEXT NOT NULL,
		position    INTEGER NOT NULL,
		state       TEXT NOT NULL,
		attempts    INTEGER NOT NULL DEFAULT 0,
		retry_base  INTEGER NOT NULL DEFAULT 0,
		summary     TEXT NOT NULL DEFAULT '',
		needs_json  TEXT NOT NULL DEFAULT '',
		started_at  TEXT,
		finished_at TEXT,
		PRIMARY KEY (run_id, stage_id)
	);
	CREATE TABLE stage_attempts (
		run_id        INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
		stage_id      TEXT NOT NULL,
		attempt       INTEGER NOT NULL,
		state         TEXT NOT NULL,
		exit_code     INTEGER,
		cost          REAL NOT NULL DEFAULT 0,
		input_tokens  INTEGER NOT NULL DEFAULT 0,
		output_tokens INTEGER NOT NULL DEFAULT 0,
		log_path      TEXT NOT NULL,
		resource      TEXT NOT NULL DEFAULT '',
		summary       TEXT NOT NULL DEFAULT '',
		needs_json    TEXT NOT NULL DEFAULT '',
		started_at    TEXT NOT NULL,
		finished_at   TEXT,
		PRIMARY KEY (run_id, stage_id, attempt)
	);
	CREATE INDEX stage_attempts_state ON stage_attempts(state);`,
	// 2: cleanup ledger (review round 1).
	`CREATE TABLE pending_cleanups (
		resource    TEXT PRIMARY KEY,
		run_id      INTEGER NOT NULL,
		stage_id    TEXT NOT NULL,
		attempt     INTEGER NOT NULL,
		recorded_at TEXT NOT NULL,
		tries       INTEGER NOT NULL DEFAULT 0,
		last_error  TEXT NOT NULL DEFAULT ''
	);`,
}

func (s *Store) migrate(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var version int
	if err := tx.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return err
	}
	if version > len(migrations) {
		return fmt.Errorf("database schema version %d is newer than this binary supports (%d)", version, len(migrations))
	}
	for i := version; i < len(migrations); i++ {
		if _, err := tx.ExecContext(ctx, migrations[i]); err != nil {
			return fmt.Errorf("migration %d: %w", i+1, err)
		}
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`PRAGMA user_version = %d`, len(migrations))); err != nil {
		return err
	}
	return tx.Commit()
}

// ---- time helpers ----

func fmtTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func fmtTimePtr(t *time.Time) any {
	if t == nil || t.IsZero() {
		return nil
	}
	return fmtTime(*t)
}

func parseTimePtr(ns sql.NullString) (*time.Time, error) {
	if !ns.Valid || ns.String == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339Nano, ns.String)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func parseTime(s string) (time.Time, error) { return time.Parse(time.RFC3339Nano, s) }

// ---- pipelines ----

// Pipeline is a registered pipeline.
type Pipeline struct {
	Name      string
	SpecYAML  []byte
	SpecHash  string
	SourceDir string
	Enabled   bool
	NextDueAt *time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
}

const pipelineCols = `name, spec_yaml, spec_hash, source_dir, enabled, next_due_at, created_at, updated_at`

func scanPipeline(row interface{ Scan(...any) error }) (Pipeline, error) {
	var p Pipeline
	var next sql.NullString
	var created, updated string
	if err := row.Scan(&p.Name, &p.SpecYAML, &p.SpecHash, &p.SourceDir, &p.Enabled, &next, &created, &updated); err != nil {
		return Pipeline{}, err
	}
	var err error
	if p.NextDueAt, err = parseTimePtr(next); err != nil {
		return Pipeline{}, err
	}
	if p.CreatedAt, err = parseTime(created); err != nil {
		return Pipeline{}, err
	}
	if p.UpdatedAt, err = parseTime(updated); err != nil {
		return Pipeline{}, err
	}
	return p, nil
}

// InsertPipeline registers a new pipeline. It fails with ErrExists if the
// name is taken.
func (s *Store) InsertPipeline(ctx context.Context, p Pipeline) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO pipelines (`+pipelineCols+`) VALUES (?,?,?,?,?,?,?,?)`,
		p.Name, p.SpecYAML, p.SpecHash, p.SourceDir, p.Enabled, fmtTimePtr(p.NextDueAt), fmtTime(p.CreatedAt), fmtTime(p.UpdatedAt))
	if err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed") {
		return fmt.Errorf("pipeline %q %w", p.Name, ErrExists)
	}
	return err
}

// UpdatePipeline rewrites every mutable column of an existing pipeline.
func (s *Store) UpdatePipeline(ctx context.Context, p Pipeline) error {
	res, err := s.db.ExecContext(ctx, `UPDATE pipelines SET spec_yaml=?, spec_hash=?, source_dir=?, enabled=?, next_due_at=?, updated_at=? WHERE name=?`,
		p.SpecYAML, p.SpecHash, p.SourceDir, p.Enabled, fmtTimePtr(p.NextDueAt), fmtTime(p.UpdatedAt), p.Name)
	return expectOne(res, err, "pipeline "+p.Name)
}

// SetNextDue sets only the next due time.
func (s *Store) SetNextDue(ctx context.Context, name string, next *time.Time) error {
	res, err := s.db.ExecContext(ctx, `UPDATE pipelines SET next_due_at=? WHERE name=?`, fmtTimePtr(next), name)
	return expectOne(res, err, "pipeline "+name)
}

// GetPipeline returns one pipeline.
func (s *Store) GetPipeline(ctx context.Context, name string) (Pipeline, error) {
	p, err := scanPipeline(s.db.QueryRowContext(ctx, `SELECT `+pipelineCols+` FROM pipelines WHERE name=?`, name))
	if errors.Is(err, sql.ErrNoRows) {
		return Pipeline{}, fmt.Errorf("pipeline %q %w", name, ErrNotFound)
	}
	return p, err
}

// ListPipelines returns every pipeline ordered by name.
func (s *Store) ListPipelines(ctx context.Context) ([]Pipeline, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+pipelineCols+` FROM pipelines ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Pipeline
	for rows.Next() {
		p, err := scanPipeline(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// DeletePipeline removes a pipeline. Its runs stay as history.
func (s *Store) DeletePipeline(ctx context.Context, name string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM pipelines WHERE name=?`, name)
	return expectOne(res, err, "pipeline "+name)
}

// ---- runs ----

// Run is one execution of a pipeline, pinned to the spec it was created from.
type Run struct {
	ID              int64
	Pipeline        string
	Trigger         string
	SpecYAML        []byte
	SpecHash        string
	SourceDir       string
	State           string
	HaltStage       string
	HaltReason      string
	CancelRequested bool
	CreatedAt       time.Time
	FinishedAt      *time.Time
}

// RunStage is the state of one stage within a run.
type RunStage struct {
	RunID    int64
	StageID  string
	Position int
	State    string
	Attempts int
	// RetryBase is Attempts at the last resume; the retry budget counts
	// attempts after it.
	RetryBase  int
	Summary    string
	NeedsJSON  string
	StartedAt  *time.Time
	FinishedAt *time.Time
}

// Attempt is one execution of one stage.
type Attempt struct {
	RunID        int64
	StageID      string
	Attempt      int
	State        string
	ExitCode     *int
	Cost         float64
	InputTokens  int64
	OutputTokens int64
	LogPath      string
	// Resource names what the attempt started outside this process (a
	// process group or a sandbox), so recovery can stop it after a crash.
	Resource string
	Summary  string
	// NeedsJSON is the JSON list a blocked attempt says it needs.
	NeedsJSON  string
	StartedAt  time.Time
	FinishedAt *time.Time
}

const runCols = `id, pipeline, trigger, spec_yaml, spec_hash, source_dir, state, halt_stage, halt_reason, cancel_requested, created_at, finished_at`

func scanRun(row interface{ Scan(...any) error }) (Run, error) {
	var r Run
	var created string
	var finished sql.NullString
	if err := row.Scan(&r.ID, &r.Pipeline, &r.Trigger, &r.SpecYAML, &r.SpecHash, &r.SourceDir, &r.State,
		&r.HaltStage, &r.HaltReason, &r.CancelRequested, &created, &finished); err != nil {
		return Run{}, err
	}
	var err error
	if r.CreatedAt, err = parseTime(created); err != nil {
		return Run{}, err
	}
	if r.FinishedAt, err = parseTimePtr(finished); err != nil {
		return Run{}, err
	}
	return r, nil
}

// CreateRun inserts a running run and one pending row per stage, atomically.
// It fails with ErrActiveRun if the pipeline already has a running run.
func (s *Store) CreateRun(ctx context.Context, r Run, stageIDs []string) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `INSERT INTO runs (pipeline, trigger, spec_yaml, spec_hash, source_dir, state, created_at) VALUES (?,?,?,?,?,?,?)`,
		r.Pipeline, r.Trigger, r.SpecYAML, r.SpecHash, r.SourceDir, RunRunning, fmtTime(r.CreatedAt))
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return 0, fmt.Errorf("pipeline %q: %w", r.Pipeline, ErrActiveRun)
		}
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	for i, sid := range stageIDs {
		if _, err := tx.ExecContext(ctx, `INSERT INTO run_stages (run_id, stage_id, position, state) VALUES (?,?,?,?)`, id, sid, i, StagePending); err != nil {
			return 0, err
		}
	}
	return id, tx.Commit()
}

// GetRun returns one run.
func (s *Store) GetRun(ctx context.Context, id int64) (Run, error) {
	r, err := scanRun(s.db.QueryRowContext(ctx, `SELECT `+runCols+` FROM runs WHERE id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Run{}, fmt.Errorf("run %d %w", id, ErrNotFound)
	}
	return r, err
}

func (s *Store) queryRuns(ctx context.Context, where string, args ...any) ([]Run, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+runCols+` FROM runs `+where, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ActiveRuns returns every running run, oldest first.
func (s *Store) ActiveRuns(ctx context.Context) ([]Run, error) {
	return s.queryRuns(ctx, `WHERE state=? ORDER BY id`, RunRunning)
}

// ListRuns returns a pipeline's runs, newest first.
func (s *Store) ListRuns(ctx context.Context, pipeline string, limit int) ([]Run, error) {
	if limit <= 0 {
		limit = 20
	}
	return s.queryRuns(ctx, `WHERE pipeline=? ORDER BY id DESC LIMIT ?`, pipeline, limit)
}

// LastRun returns the newest run of a pipeline, if any.
func (s *Store) LastRun(ctx context.Context, pipeline string) (*Run, error) {
	runs, err := s.ListRuns(ctx, pipeline, 1)
	if err != nil || len(runs) == 0 {
		return nil, err
	}
	return &runs[0], nil
}

// HasActiveRun reports whether the pipeline has a running run.
func (s *Store) HasActiveRun(ctx context.Context, pipeline string) (bool, error) {
	id, err := s.ActiveRunID(ctx, pipeline)
	return id != 0, err
}

// ActiveRunID is the id of the pipeline's running run, or 0. It may be older
// than the newest run (a resumed run keeps its id).
func (s *Store) ActiveRunID(ctx context.Context, pipeline string) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx, `SELECT id FROM runs WHERE pipeline=? AND state=?`, pipeline, RunRunning).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return id, err
}

// FinishRun records a run's terminal state.
func (s *Store) FinishRun(ctx context.Context, id int64, state, haltStage, haltReason string, at time.Time) error {
	res, err := s.db.ExecContext(ctx, `UPDATE runs SET state=?, halt_stage=?, halt_reason=?, finished_at=? WHERE id=? AND state=?`,
		state, haltStage, haltReason, fmtTime(at), id, RunRunning)
	return expectOne(res, err, fmt.Sprintf("running run %d", id))
}

// RequestCancel flags a running run for cancellation. The executing process
// stops its stages; if none is executing, the next one to take the lock does.
func (s *Store) RequestCancel(ctx context.Context, id int64) error {
	res, err := s.db.ExecContext(ctx, `UPDATE runs SET cancel_requested=1 WHERE id=? AND state=?`, id, RunRunning)
	return expectOne(res, err, fmt.Sprintf("running run %d", id))
}

// ResumeRun moves a parked run back to running and resets the given stages to
// pending, in one transaction. It fails with ErrActiveRun if another run of
// the pipeline is running.
func (s *Store) ResumeRun(ctx context.Context, id int64, reset []string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var pipeline, state string
	if err := tx.QueryRowContext(ctx, `SELECT pipeline, state FROM runs WHERE id=?`, id).Scan(&pipeline, &state); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("run %d %w", id, ErrNotFound)
		}
		return err
	}
	switch state {
	case RunBlocked, RunFailed, RunCancelled:
	default:
		return fmt.Errorf("run %d is %s; only a blocked, failed or cancelled run can be resumed", id, state)
	}
	_, err = tx.ExecContext(ctx, `UPDATE runs SET state=?, halt_stage='', halt_reason='', cancel_requested=0, finished_at=NULL WHERE id=?`, RunRunning, id)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return fmt.Errorf("pipeline %q: %w", pipeline, ErrActiveRun)
		}
		return err
	}
	for _, sid := range reset {
		if _, err := tx.ExecContext(ctx, `UPDATE run_stages SET state=?, retry_base=attempts, summary='', needs_json='', finished_at=NULL WHERE run_id=? AND stage_id=?`,
			StagePending, id, sid); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ---- stages ----

const stageCols = `run_id, stage_id, position, state, attempts, retry_base, summary, needs_json, started_at, finished_at`

// ListStages returns a run's stages in spec order.
func (s *Store) ListStages(ctx context.Context, runID int64) ([]RunStage, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+stageCols+` FROM run_stages WHERE run_id=? ORDER BY position`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RunStage
	for rows.Next() {
		var st RunStage
		var started, finished sql.NullString
		if err := rows.Scan(&st.RunID, &st.StageID, &st.Position, &st.State, &st.Attempts, &st.RetryBase, &st.Summary, &st.NeedsJSON, &started, &finished); err != nil {
			return nil, err
		}
		if st.StartedAt, err = parseTimePtr(started); err != nil {
			return nil, err
		}
		if st.FinishedAt, err = parseTimePtr(finished); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// UpdateStage writes a stage row.
func (s *Store) UpdateStage(ctx context.Context, st RunStage) error {
	res, err := s.db.ExecContext(ctx, `UPDATE run_stages SET state=?, attempts=?, summary=?, needs_json=?, started_at=?, finished_at=? WHERE run_id=? AND stage_id=?`,
		st.State, st.Attempts, st.Summary, st.NeedsJSON, fmtTimePtr(st.StartedAt), fmtTimePtr(st.FinishedAt), st.RunID, st.StageID)
	return expectOne(res, err, fmt.Sprintf("stage %d/%s", st.RunID, st.StageID))
}

// ---- attempts ----

const attemptCols = `run_id, stage_id, attempt, state, exit_code, cost, input_tokens, output_tokens, log_path, resource, summary, needs_json, started_at, finished_at`

func (s *Store) queryAttempts(ctx context.Context, where string, args ...any) ([]Attempt, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+attemptCols+` FROM stage_attempts `+where, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Attempt
	for rows.Next() {
		var a Attempt
		var exit sql.NullInt64
		var started string
		var finished sql.NullString
		if err := rows.Scan(&a.RunID, &a.StageID, &a.Attempt, &a.State, &exit, &a.Cost, &a.InputTokens, &a.OutputTokens,
			&a.LogPath, &a.Resource, &a.Summary, &a.NeedsJSON, &started, &finished); err != nil {
			return nil, err
		}
		if exit.Valid {
			v := int(exit.Int64)
			a.ExitCode = &v
		}
		if a.StartedAt, err = parseTime(started); err != nil {
			return nil, err
		}
		if a.FinishedAt, err = parseTimePtr(finished); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// StartAttempt records a new running attempt and moves its stage to running,
// atomically.
func (s *Store) StartAttempt(ctx context.Context, st RunStage, a Attempt) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO stage_attempts (run_id, stage_id, attempt, state, log_path, started_at) VALUES (?,?,?,?,?,?)`,
		a.RunID, a.StageID, a.Attempt, AttemptRunning, a.LogPath, fmtTime(a.StartedAt)); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `UPDATE run_stages SET state=?, attempts=?, summary='', needs_json='', started_at=?, finished_at=NULL WHERE run_id=? AND stage_id=?`,
		StageRunning, st.Attempts, fmtTimePtr(st.StartedAt), st.RunID, st.StageID)
	if err := expectOne(res, err, "stage "+st.StageID); err != nil {
		return err
	}
	return tx.Commit()
}

// TrackResource records what an attempt starts outside the runner (a process
// group, a sandbox) on the attempt row and as a pending cleanup, atomically.
// The pending cleanup stays until ReleaseResource.
func (s *Store) TrackResource(ctx context.Context, runID int64, stageID string, attempt int, resource string, at time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE stage_attempts SET resource=? WHERE run_id=? AND stage_id=? AND attempt=?`, resource, runID, stageID, attempt)
	if err := expectOne(res, err, "attempt"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO pending_cleanups (resource, run_id, stage_id, attempt, recorded_at) VALUES (?,?,?,?,?)
		ON CONFLICT(resource) DO NOTHING`, resource, runID, stageID, attempt, fmtTime(at)); err != nil {
		return err
	}
	return tx.Commit()
}

// ReleaseResource forgets a pending cleanup.
func (s *Store) ReleaseResource(ctx context.Context, resource string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM pending_cleanups WHERE resource=?`, resource)
	return err
}

// PendingCleanup is a resource not yet confirmed gone.
type PendingCleanup struct {
	Resource   string
	RunID      int64
	StageID    string
	Attempt    int
	RecordedAt time.Time
	Tries      int
	LastError  string
}

// PendingCleanups lists resources not yet confirmed gone, oldest first.
func (s *Store) PendingCleanups(ctx context.Context) ([]PendingCleanup, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT resource, run_id, stage_id, attempt, recorded_at, tries, last_error FROM pending_cleanups ORDER BY recorded_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PendingCleanup
	for rows.Next() {
		var p PendingCleanup
		var at string
		if err := rows.Scan(&p.Resource, &p.RunID, &p.StageID, &p.Attempt, &at, &p.Tries, &p.LastError); err != nil {
			return nil, err
		}
		if p.RecordedAt, err = parseTime(at); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// NoteCleanupFailure records a failed cleanup try.
func (s *Store) NoteCleanupFailure(ctx context.Context, resource, msg string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE pending_cleanups SET tries=tries+1, last_error=? WHERE resource=?`, msg, resource)
	return err
}

// FinishAttempt records an attempt's outcome. It only changes a running
// attempt, so a late writer cannot overwrite a recovered one.
func (s *Store) FinishAttempt(ctx context.Context, a Attempt) error {
	var exit any
	if a.ExitCode != nil {
		exit = *a.ExitCode
	}
	res, err := s.db.ExecContext(ctx, `UPDATE stage_attempts SET state=?, exit_code=?, cost=?, input_tokens=?, output_tokens=?, summary=?, needs_json=?, finished_at=?
		WHERE run_id=? AND stage_id=? AND attempt=? AND state=?`,
		a.State, exit, a.Cost, a.InputTokens, a.OutputTokens, a.Summary, a.NeedsJSON, fmtTimePtr(a.FinishedAt), a.RunID, a.StageID, a.Attempt, AttemptRunning)
	return expectOne(res, err, fmt.Sprintf("running attempt %d/%s/%d", a.RunID, a.StageID, a.Attempt))
}

// ListAttempts returns a run's attempts ordered by stage position and attempt.
func (s *Store) ListAttempts(ctx context.Context, runID int64) ([]Attempt, error) {
	return s.queryAttempts(ctx, `WHERE run_id=? ORDER BY stage_id, attempt`, runID)
}

// RunningAttempts returns every attempt still marked running.
func (s *Store) RunningAttempts(ctx context.Context) ([]Attempt, error) {
	return s.queryAttempts(ctx, `WHERE state=? ORDER BY run_id, stage_id, attempt`, AttemptRunning)
}

// RunCost sums the cost and tokens of a run's attempts.
func (s *Store) RunCost(ctx context.Context, runID int64) (cost float64, in, out int64, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(cost),0), COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0) FROM stage_attempts WHERE run_id=?`, runID).
		Scan(&cost, &in, &out)
	return cost, in, out, err
}

func expectOne(res sql.Result, err error, what string) error {
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%s %w", what, ErrNotFound)
	}
	return nil
}
