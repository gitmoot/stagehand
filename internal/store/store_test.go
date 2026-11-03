package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

// A home created by an earlier binary (schema version 1) must gain every
// later table when a newer binary opens it.
func TestOpenUpgradesAnExistingVersion1Home(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stagehand.db")
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(migrations[0]); err != nil {
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
