package main

import (
	"database/sql"
	"os"
	"testing"
)

func TestFirebird5Profiler(t *testing.T) {
	dsn := os.Getenv("FIREBIRD_TEST_DSN")
	if dsn == "" {
		t.Skip("requires isolated Firebird 5 fixture")
	}
	db, err := sql.Open("firebirdsql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, err := profile(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows != 2 || r.Requests < 1 || r.ProfileID <= 0 {
		t.Fatal(r)
	}
	t.Logf("%+v", r)
}
