package store

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A home created by an earlier binary must gain every later table when a
// newer binary opens it. The version-1 schema is frozen as shipped, so
// editing an already-applied migration (instead of adding one) fails here.
func TestOpenUpgradesAnExistingVersion1Home(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stagehand.db")
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	v1, err := os.ReadFile("testdata/schema-v1.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(v1)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`PRAGMA user_version = 1`); err != nil {
		t.Fatal(err)
	}
	db.Close()

	s, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.PendingCleanups(context.Background()); err != nil {
		t.Fatalf("pending cleanups on an upgraded home: %v", err)
	}
}

// Two intakes of the same event can both pass the engine's early duplicate
// check (slow request bodies overlap). The insert itself must then answer
// "duplicate" for the second, without placing its payload.
func TestInsertEventIsIdempotentInsideTheTransaction(t *testing.T) {
	s, err := Open(context.Background(), filepath.Join(t.TempDir(), "stagehand.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ev := Event{Pipeline: "p", Key: "k", Source: "http", ReceivedAt: time.Now(), PayloadPath: "/a"}
	placed := 0
	place := func() error { placed++; return nil }
	first, dup, err := s.InsertEvent(context.Background(), ev, 5, place)
	if err != nil || dup {
		t.Fatalf("first: dup=%v err=%v", dup, err)
	}
	ev.PayloadPath = "/b"
	second, dup, err := s.InsertEvent(context.Background(), ev, 5, place)
	if err != nil || !dup || second.ID != first.ID || second.PayloadPath != "/a" {
		t.Fatalf("second: %+v dup=%v err=%v; want the first event as a duplicate", second, dup, err)
	}
	if placed != 1 {
		t.Errorf("payload placed %d times", placed)
	}
}

// A slot a usage limit skips is recorded while another run of the pipeline
// is running: it never exists as running itself, and it moves the
// pipeline's next due time. The pipeline's limit is the latest end as a
// time, even where RFC 3339 text sorts it first (a trimmed fraction:
// "11:00:00.5Z" < "11:00:00Z").
func TestSkippedRunsAndPipelineLimitedUntil(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "stagehand.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	base := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	if err := s.InsertPipeline(ctx, Pipeline{Name: "p", SpecYAML: []byte("x"), SpecHash: "h", SourceDir: "/", CreatedAt: base, UpdatedAt: base}); err != nil {
		t.Fatal(err)
	}
	r := Run{Pipeline: "p", Trigger: "schedule", SpecYAML: []byte("x"), SpecHash: "h", SourceDir: "/", CreatedAt: base}
	if _, err := s.CreateRun(ctx, r, []string{"a"}); err != nil {
		t.Fatal(err)
	}
	later := base.Add(time.Hour + 500*time.Millisecond)
	earlier := base.Add(time.Hour)
	for i, until := range []time.Time{later, earlier} {
		next := base.Add(time.Duration(i+1) * time.Hour)
		id, err := s.CreateSkippedRun(ctx, r, []string{"a"}, until, next)
		if err != nil {
			t.Fatalf("skipped run beside a running one: %v", err)
		}
		got, err := s.GetRun(ctx, id)
		if err != nil || got.State != RunSkipped || got.HaltReason != UsageLimitReason(until) || got.LimitedUntil == nil ||
			!got.LimitedUntil.Equal(until) || got.FinishedAt == nil {
			t.Fatalf("skipped run %+v, %v", got, err)
		}
		if p, err := s.GetPipeline(ctx, "p"); err != nil || p.NextDueAt == nil || !p.NextDueAt.Equal(next) {
			t.Fatalf("next due %v, %v; want %v", p.NextDueAt, err, next)
		}
	}
	if until, err := s.PipelineLimitedUntil(ctx, "p"); err != nil || until == nil || !until.Equal(later) {
		t.Errorf("pipeline limited until %v, %v; want %v", until, err, later)
	}
	if until, err := s.PipelineLimitedUntil(ctx, "other"); err != nil || until != nil {
		t.Errorf("pipeline without limits: %v, %v", until, err)
	}
}

// The skipped run and the next due time are one write: when the next due
// time cannot be written, no skipped run is left behind (or the next poll
// would record the same slot again).
func TestSkippedRunIsNotRecordedWhenNextDueFails(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "stagehand.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	base := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	r := Run{Pipeline: "gone", Trigger: "schedule", SpecYAML: []byte("x"), SpecHash: "h", SourceDir: "/", CreatedAt: base}
	if _, err := s.CreateSkippedRun(ctx, r, []string{"a"}, base.Add(time.Hour), base.Add(time.Hour)); err == nil {
		t.Fatal("recorded a skipped slot for a pipeline whose next due time cannot be written")
	}
	if runs, err := s.ListRuns(ctx, "gone", 10); err != nil || len(runs) != 0 {
		t.Fatalf("runs %+v, %v; want none", runs, err)
	}
}
