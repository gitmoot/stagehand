package store

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
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
