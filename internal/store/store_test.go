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
