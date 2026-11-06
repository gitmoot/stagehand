package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// Event states.
const (
	// EventQueued is waiting for its pipeline to be free.
	EventQueued = "queued"
	// EventStarted has a run (see RunID).
	EventStarted = "started"
	// EventDropped was removed by an operator before it ran.
	EventDropped = "dropped"
)

var (
	// ErrQueueFull is returned when a pipeline already has its maximum of
	// queued events; nothing was stored.
	ErrQueueFull = errors.New("event queue is full")
)

// Event is one accepted trigger of a pipeline.
type Event struct {
	ID       int64
	Pipeline string
	// Key is the caller's idempotency key (or a generated one). One key per
	// pipeline yields one event.
	Key              string
	State            string
	Source           string
	ReceivedAt       time.Time
	ContentType      string
	PayloadPath      string
	PayloadBytes     int64
	PayloadSHA256    string
	PayloadDeletedAt *time.Time
	RunID            int64
}

const eventCols = `id, pipeline, event_key, state, source, received_at, content_type, payload_path, payload_bytes, payload_sha256, payload_deleted_at, COALESCE(run_id, 0)`

func scanEvent(row interface{ Scan(...any) error }) (Event, error) {
	var e Event
	var received string
	var deleted sql.NullString
	if err := row.Scan(&e.ID, &e.Pipeline, &e.Key, &e.State, &e.Source, &received, &e.ContentType,
		&e.PayloadPath, &e.PayloadBytes, &e.PayloadSHA256, &deleted, &e.RunID); err != nil {
		return Event{}, err
	}
	var err error
	if e.ReceivedAt, err = parseTime(received); err != nil {
		return Event{}, err
	}
	if e.PayloadDeletedAt, err = parseTimePtr(deleted); err != nil {
		return Event{}, err
	}
	return e, nil
}

// InsertEvent stores an event unless one with the same pipeline and key
// exists (duplicate=true, the existing event is returned) or the pipeline
// already has maxQueued queued events (ErrQueueFull). place is called inside
// the transaction, after the checks and before commit, to move the payload
// file into place; if it fails nothing is stored.
func (s *Store) InsertEvent(ctx context.Context, e Event, maxQueued int, place func() error) (Event, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Event{}, false, err
	}
	defer tx.Rollback()
	existing, err := scanEvent(tx.QueryRowContext(ctx, `SELECT `+eventCols+` FROM events WHERE pipeline=? AND event_key=?`, e.Pipeline, e.Key))
	switch {
	case err == nil:
		return existing, true, nil
	case !errors.Is(err, sql.ErrNoRows):
		return Event{}, false, err
	}
	var queued int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE pipeline=? AND state=?`, e.Pipeline, EventQueued).Scan(&queued); err != nil {
		return Event{}, false, err
	}
	if queued >= maxQueued {
		return Event{}, false, fmt.Errorf("pipeline %q has %d queued events: %w", e.Pipeline, queued, ErrQueueFull)
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO events (pipeline, event_key, state, source, received_at, content_type, payload_path, payload_bytes, payload_sha256)
		VALUES (?,?,?,?,?,?,?,?,?)`, e.Pipeline, e.Key, EventQueued, e.Source, fmtTime(e.ReceivedAt), e.ContentType, e.PayloadPath, e.PayloadBytes, e.PayloadSHA256)
	if err != nil {
		return Event{}, false, err
	}
	if e.ID, err = res.LastInsertId(); err != nil {
		return Event{}, false, err
	}
	if place != nil {
		if err := place(); err != nil {
			return Event{}, false, fmt.Errorf("store payload: %w", err)
		}
	}
	e.State = EventQueued
	return e, false, tx.Commit()
}

// GetEvent returns one event.
func (s *Store) GetEvent(ctx context.Context, id int64) (Event, error) {
	e, err := scanEvent(s.db.QueryRowContext(ctx, `SELECT `+eventCols+` FROM events WHERE id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Event{}, fmt.Errorf("event %d %w", id, ErrNotFound)
	}
	return e, err
}

// FindEvent returns a pipeline's event by key.
func (s *Store) FindEvent(ctx context.Context, pipeline, key string) (Event, error) {
	e, err := scanEvent(s.db.QueryRowContext(ctx, `SELECT `+eventCols+` FROM events WHERE pipeline=? AND event_key=?`, pipeline, key))
	if errors.Is(err, sql.ErrNoRows) {
		return Event{}, fmt.Errorf("event %q of %q %w", key, pipeline, ErrNotFound)
	}
	return e, err
}

// ListEvents returns events, newest first, optionally for one pipeline
// and/or one state.
func (s *Store) ListEvents(ctx context.Context, pipeline, state string, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = 50
	}
	q := `SELECT ` + eventCols + ` FROM events WHERE 1=1`
	var args []any
	if pipeline != "" {
		q += ` AND pipeline=?`
		args = append(args, pipeline)
	}
	if state != "" {
		q += ` AND state=?`
		args = append(args, state)
	}
	q += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// QueuedEventCount is how many events of a pipeline wait.
func (s *Store) QueuedEventCount(ctx context.Context, pipeline string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE pipeline=? AND state=?`, pipeline, EventQueued).Scan(&n)
	return n, err
}

// OldestQueuedEvent is the next event a pipeline should run, if any.
func (s *Store) OldestQueuedEvent(ctx context.Context, pipeline string) (*Event, error) {
	e, err := scanEvent(s.db.QueryRowContext(ctx, `SELECT `+eventCols+` FROM events WHERE pipeline=? AND state=? ORDER BY id LIMIT 1`, pipeline, EventQueued))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &e, nil
}

// CreateEventRun creates a run for a queued event and marks the event
// started, atomically. It fails with ErrActiveRun if the pipeline has a
// running run, and with ErrNotFound if the event is no longer queued.
func (s *Store) CreateEventRun(ctx context.Context, r Run, stageIDs []string, eventID int64) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `INSERT INTO runs (pipeline, trigger, spec_yaml, spec_hash, source_dir, state, created_at, event_id) VALUES (?,?,?,?,?,?,?,?)`,
		r.Pipeline, r.Trigger, r.SpecYAML, r.SpecHash, r.SourceDir, RunRunning, fmtTime(r.CreatedAt), eventID)
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
	upd, err := tx.ExecContext(ctx, `UPDATE events SET state=?, run_id=? WHERE id=? AND state=?`, EventStarted, id, eventID, EventQueued)
	if err := expectOne(upd, err, fmt.Sprintf("queued event %d", eventID)); err != nil {
		return 0, err
	}
	return id, tx.Commit()
}

// DropEvent removes a queued event's payload and marks it dropped.
func (s *Store) DropEvent(ctx context.Context, id int64, at time.Time) (Event, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Event{}, err
	}
	defer tx.Rollback()
	e, err := scanEvent(tx.QueryRowContext(ctx, `SELECT `+eventCols+` FROM events WHERE id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Event{}, fmt.Errorf("event %d %w", id, ErrNotFound)
	}
	if err != nil {
		return Event{}, err
	}
	if e.State != EventQueued {
		return Event{}, fmt.Errorf("event %d is %s; only a queued event can be dropped (use `purge <run-id>` for a started one)", id, e.State)
	}
	if err := removePayload(e.PayloadPath); err != nil {
		return Event{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE events SET state=?, payload_deleted_at=? WHERE id=?`, EventDropped, fmtTime(at), id); err != nil {
		return Event{}, err
	}
	e.State = EventDropped
	return e, tx.Commit()
}

// RetentionDue lists succeeded event runs whose files are not deleted yet.
func (s *Store) RetentionDue(ctx context.Context) ([]Run, error) {
	return s.queryRuns(ctx, `WHERE event_id IS NOT NULL AND state=? AND files_deleted_at IS NULL ORDER BY id`, RunSucceeded)
}

// MarkFilesDeleted records that a run's directory and its event's payload
// are gone.
func (s *Store) MarkFilesDeleted(ctx context.Context, runID, eventID int64, at time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET files_deleted_at=? WHERE id=?`, fmtTime(at), runID); err != nil {
		return err
	}
	if eventID != 0 {
		if _, err := tx.ExecContext(ctx, `UPDATE events SET payload_deleted_at=COALESCE(payload_deleted_at, ?) WHERE id=?`, fmtTime(at), eventID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// LivePayloadPaths returns every payload path not yet deleted, for the
// orphan sweep.
func (s *Store) LivePayloadPaths(ctx context.Context) (map[string]bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT payload_path FROM events WHERE payload_deleted_at IS NULL AND payload_path != ''`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		out[p] = true
	}
	return out, rows.Err()
}

func removePayload(path string) error {
	if path == "" {
		return nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
