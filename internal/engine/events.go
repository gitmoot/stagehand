package engine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"os"
	"path/filepath"
	"regexp"
	"syscall"
	"time"

	"github.com/gitmoot/stagehand/internal/spec"
	"github.com/gitmoot/stagehand/internal/store"
)

// Event sources.
const (
	SourceHTTP = "http"
	SourceCLI  = "cli"
)

var (
	// ErrEventsDisabled: the pipeline has no `events:` block.
	ErrEventsDisabled = errors.New("pipeline does not accept events (add `events: {max_queued: N}` to its spec)")
	// ErrPipelineDisabled: the pipeline is disabled.
	ErrPipelineDisabled = errors.New("pipeline is disabled")
	// ErrPayloadTooLarge: the payload exceeds the host limit.
	ErrPayloadTooLarge = errors.New("payload too large")
	// ErrBadEventKey: the caller's event id has an invalid form.
	ErrBadEventKey = errors.New("event id must be 1-128 characters from A-Z a-z 0-9 . _ : -")
)

var eventKeyRE = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// ValidEventKey reports whether key may be used as an event id.
func ValidEventKey(key string) bool { return eventKeyRE.MatchString(key) }

// EventRequest is one trigger.
type EventRequest struct {
	Pipeline string
	// Key is the caller's idempotency key; empty means "always new".
	Key         string
	ContentType string
	Source      string
	Payload     io.Reader
	// MaxBytes bounds the payload.
	MaxBytes int64
}

// EventResult reports what Trigger did.
type EventResult struct {
	Event     store.Event
	Duplicate bool
}

func (e *Engine) payloadDir() string { return filepath.Join(e.Home, "events") }

// Trigger accepts an event: it writes the payload to a private file, then
// records the event, unless one with the same key exists (Duplicate) or the
// pipeline's queue is full (store.ErrQueueFull). Nothing is kept on refusal.
func (e *Engine) Trigger(ctx context.Context, req EventRequest) (EventResult, error) {
	p, err := e.Store.GetPipeline(ctx, req.Pipeline)
	if err != nil {
		return EventResult{}, err
	}
	sp, err := spec.Load(p.SpecYAML)
	if err != nil {
		return EventResult{}, fmt.Errorf("stored spec for %q: %w", req.Pipeline, err)
	}
	if sp.Events == nil {
		return EventResult{}, ErrEventsDisabled
	}
	if !p.Enabled {
		return EventResult{}, fmt.Errorf("%q: %w", req.Pipeline, ErrPipelineDisabled)
	}
	key := req.Key
	if key == "" {
		key = "auto-" + randomHex(12)
	} else if !ValidEventKey(key) {
		return EventResult{}, ErrBadEventKey
	}
	// A duplicate is answered before reading the body, so a retried webhook
	// costs nothing.
	if existing, err := e.Store.FindEvent(ctx, req.Pipeline, key); err == nil {
		return EventResult{Event: existing, Duplicate: true}, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return EventResult{}, err
	}

	if err := os.MkdirAll(e.payloadDir(), 0o700); err != nil {
		return EventResult{}, err
	}
	tmp, err := os.CreateTemp(e.payloadDir(), ".incoming-*")
	if err != nil {
		return EventResult{}, err
	}
	// The intake holds an exclusive lock on its payload file from creation
	// until its event is committed (or abandoned). The lock follows the
	// inode through the rename, and the orphan sweep skips any file it
	// cannot lock, so a file that belongs to an intake in progress is never
	// swept, however old its contents.
	if err := syscall.Flock(int(tmp.Fd()), syscall.LOCK_EX); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return EventResult{}, fmt.Errorf("lock payload file: %w", err)
	}
	defer tmp.Close()           // releases the lock, after the commit below
	defer os.Remove(tmp.Name()) // no-op once renamed
	h := sha256.New()
	limit := req.MaxBytes
	if limit <= 0 {
		limit = 64 << 20
	}
	n, err := io.Copy(io.MultiWriter(tmp, h), io.LimitReader(req.Payload, limit+1))
	if err == nil {
		err = tmp.Sync()
	}
	if err != nil {
		return EventResult{}, fmt.Errorf("read payload: %w", err)
	}
	if n > limit {
		return EventResult{}, fmt.Errorf("%w: more than %d bytes", ErrPayloadTooLarge, limit)
	}

	final := filepath.Join(e.payloadDir(), randomHex(16)+".payload")
	ev := store.Event{
		Pipeline: req.Pipeline, Key: key, Source: req.Source, ReceivedAt: e.now(),
		ContentType: sanitizeContentType(req.ContentType), PayloadPath: final,
		PayloadBytes: n, PayloadSHA256: hex.EncodeToString(h.Sum(nil)),
	}
	stored, dup, err := e.Store.InsertEvent(ctx, ev, sp.Events.MaxQueued, func() error {
		if err := os.Rename(tmp.Name(), final); err != nil {
			return err
		}
		if e.beforeEventCommit != nil {
			e.beforeEventCommit(final)
		}
		return nil
	})
	if err != nil {
		os.Remove(final)
		return EventResult{}, err
	}
	if dup {
		// Lost a race with an identical event: keep the first one.
		return EventResult{Event: stored, Duplicate: true}, nil
	}
	e.Log.Info("event accepted", "pipeline", req.Pipeline, "event", stored.ID, "key", key, "bytes", n, "source", req.Source)
	e.signal()
	return EventResult{Event: stored}, nil
}

// startEvents starts the oldest queued event of every enabled pipeline that
// has no running run.
func (e *Engine) startEvents(ctx context.Context, only string, yieldToSchedule bool) error {
	pipelines, err := e.Store.ListPipelines(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, p := range pipelines {
		if !p.Enabled || (only != "" && p.Name != only) {
			continue
		}
		ev, err := e.Store.OldestQueuedEvent(ctx, p.Name)
		if err != nil || ev == nil {
			if err != nil {
				errs = append(errs, err)
			}
			continue
		}
		if active, err := e.Store.HasActiveRun(ctx, p.Name); err != nil || active {
			continue
		}
		sp, err := spec.Load(p.SpecYAML)
		if err != nil {
			continue
		}
		// Yield to an older due schedule only when this loop also runs
		// schedules; otherwise nothing would ever dispatch it.
		if yieldToSchedule && sp.Schedule != nil && p.NextDueAt != nil && !e.now().Before(*p.NextDueAt) && !eventBeforeSchedule(ev, p) {
			continue // the due schedule is older; it goes first
		}
		ids := make([]string, len(sp.Stages))
		for i, st := range sp.Stages {
			ids[i] = st.ID
		}
		id, err := e.Store.CreateEventRun(ctx, store.Run{
			Pipeline: p.Name, Trigger: "event", SpecYAML: p.SpecYAML, SpecHash: p.SpecHash,
			SourceDir: p.SourceDir, CreatedAt: e.now(),
		}, ids, ev.ID)
		switch {
		case errors.Is(err, store.ErrActiveRun), errors.Is(err, store.ErrNotFound):
			continue // lost a race; the next pass retries
		case err != nil:
			errs = append(errs, err)
			continue
		}
		e.Log.Info("run created", "pipeline", p.Name, "run", id, "trigger", "event", "event", ev.ID, "key", ev.Key)
	}
	return errors.Join(errs...)
}

// applyRetention deletes the run directory and payload of every succeeded
// event run (owner decision, 6 Nov: confidential payloads do not outlive a
// successful run), and sweeps payload files no event refers to.
func (e *Engine) applyRetention(ctx context.Context) error {
	var errs []error
	runs, err := e.Store.RetentionDue(ctx)
	if err != nil {
		return err
	}
	for _, r := range runs {
		if err := e.deleteRunFiles(ctx, r.ID); err != nil && !errors.Is(err, store.ErrNotDeletable) {
			errs = append(errs, err)
		}
	}
	// Finish deletions interrupted between the claim and the unlinks.
	claimed, err := e.Store.DeletedRuns(ctx)
	if err != nil {
		return err
	}
	for _, r := range claimed {
		if err := e.removeRunFiles(ctx, r); err != nil {
			errs = append(errs, err)
		}
	}
	if err := e.sweepOrphanPayloads(ctx); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// deleteRunFiles claims a finished run's files for deletion (atomically
// against resume), then removes them.
func (e *Engine) deleteRunFiles(ctx context.Context, runID int64) error {
	r, err := e.Store.ClaimFilesDeletion(ctx, runID, e.now())
	if err != nil {
		return err
	}
	if err := e.removeRunFiles(ctx, r); err != nil {
		return err
	}
	e.Log.Info("deleted run files and payload", "run", r.ID, "event", r.EventID)
	return nil
}

// removeRunFiles removes a claimed run's directory and payload. It is
// idempotent, so an interrupted deletion can simply be repeated.
func (e *Engine) removeRunFiles(ctx context.Context, r store.Run) error {
	if err := os.RemoveAll(e.RunDir(r.ID)); err != nil {
		return fmt.Errorf("run %d: delete files: %w", r.ID, err)
	}
	if r.EventID != 0 {
		ev, err := e.Store.GetEvent(ctx, r.EventID)
		if err != nil {
			return err
		}
		if err := os.Remove(ev.PayloadPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("run %d: delete payload: %w", r.ID, err)
		}
	}
	return nil
}

// sweepOrphanPayloads removes payload files that no live event references:
// leftovers of an intake interrupted before its commit, or of a drop or
// deletion interrupted after its commit.
//
// It never races an intake: each intake holds an exclusive lock on its file
// until its event is committed, and the sweep only removes a file whose lock
// it holds itself, checking the committed live set after taking the lock. A
// file whose lock it cannot take belongs to an intake in progress. Files
// modified within orphanAge are skipped as well, which covers the instant
// between an intake creating its file and locking it.
func (e *Engine) sweepOrphanPayloads(ctx context.Context) error {
	entries, err := os.ReadDir(e.payloadDir())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, ent := range entries {
		info, err := ent.Info()
		if err != nil || !info.Mode().IsRegular() || e.now().Sub(info.ModTime()) < orphanAge {
			continue
		}
		path := filepath.Join(e.payloadDir(), ent.Name())
		if err := e.sweepOne(ctx, path); err != nil {
			return err
		}
	}
	return nil
}

// sweepOne removes path if it is unlocked and no live event references it.
func (e *Engine) sweepOne(ctx context.Context, path string) error {
	// Read-write: on NFS, Linux emulates flock with fcntl locks, and an
	// exclusive one needs a writable descriptor.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil // gone already
	}
	if err != nil {
		return fmt.Errorf("sweep %s: %w", path, err)
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil // an intake holds it
		}
		return fmt.Errorf("sweep %s: lock: %w", path, err)
	}
	// With the lock held, no intake can still be committing this file:
	// whatever the live set says now is final for it.
	live, err := e.Store.LivePayloadPaths(ctx)
	if err != nil {
		return err
	}
	if live[path] {
		return nil
	}
	if err := os.Remove(path); err == nil {
		e.Log.Warn("removed orphaned payload", "file", path)
	}
	return nil
}

// orphanAge is how old an unfinished intake file must be before the sweep
// removes it.
const orphanAge = 15 * time.Minute

// Purge deletes the files and payload of a finished event run that will not
// be resumed. It refuses a running run.
func (e *Engine) Purge(ctx context.Context, runID int64) error {
	r, err := e.Store.GetRun(ctx, runID)
	if err != nil {
		return err
	}
	if r.State == store.RunRunning {
		return fmt.Errorf("run %d is running; cancel it first", runID)
	}
	if r.FilesDeletedAt != nil {
		// Claimed earlier; removal may not have completed. It is idempotent.
		return e.removeRunFiles(ctx, r)
	}
	return e.deleteRunFiles(ctx, runID)
}

// DropEvent discards a queued event and its payload.
func (e *Engine) DropEvent(ctx context.Context, id int64) (store.Event, error) {
	return e.Store.DropEvent(ctx, id, e.now())
}

// sanitizeContentType keeps a well-formed media type (without parameters)
// and drops anything else.
func sanitizeContentType(ct string) string {
	mt, _, err := mime.ParseMediaType(ct)
	if err != nil || len(mt) > 100 {
		return ""
	}
	return mt
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// eventsWaiting reports whether any enabled pipeline has a queued event.
func (e *Engine) eventsWaiting(ctx context.Context, only string) (bool, error) {
	pipelines, err := e.Store.ListPipelines(ctx)
	if err != nil {
		return false, err
	}
	for _, p := range pipelines {
		if !p.Enabled || (only != "" && p.Name != only) {
			continue
		}
		n, err := e.Store.QueuedEventCount(ctx, p.Name)
		if err != nil {
			return false, err
		}
		if n > 0 {
			return true, nil
		}
	}
	return false, nil
}

// eventBeforeSchedule reports whether a pipeline's oldest queued event
// arrived before its schedule came due. Whichever is older takes the
// pipeline's single run slot, so neither source can starve the other.
func eventBeforeSchedule(ev *store.Event, p store.Pipeline) bool {
	if ev == nil {
		return false
	}
	if p.NextDueAt == nil {
		return true
	}
	return !ev.ReceivedAt.After(*p.NextDueAt)
}

// scheduleDue reports whether an enabled, scheduled pipeline is due now.
func (e *Engine) scheduleDue(ctx context.Context) (bool, error) {
	pipelines, err := e.Store.ListPipelines(ctx)
	if err != nil {
		return false, err
	}
	now := e.now()
	for _, p := range pipelines {
		if p.Enabled && p.NextDueAt != nil && !now.Before(*p.NextDueAt) {
			sp, err := spec.Load(p.SpecYAML)
			if err == nil && sp.Schedule != nil {
				return true, nil
			}
		}
	}
	return false, nil
}
