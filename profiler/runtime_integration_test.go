package profiler

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"
)

func TestCleanupStaleKeepsOtherProfilerSessions(t *testing.T) {
	dsn := os.Getenv("FIREBIRD_TEST_DSN")
	if dsn == "" {
		t.Skip("requires isolated Firebird 5 fixture")
	}
	db, err := sql.Open("firebirdsql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := t.Context()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var ownID, otherID int64
	if err := conn.QueryRowContext(ctx, `SELECT RDB$PROFILER.START_SESSION('firebirdotel/integration/stale') FROM RDB$DATABASE`).Scan(&ownID); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, `EXECUTE PROCEDURE RDB$PROFILER.FINISH_SESSION(TRUE)`); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRowContext(ctx, `SELECT RDB$PROFILER.START_SESSION('unrelated session') FROM RDB$DATABASE`).Scan(&otherID); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, `EXECUTE PROCEDURE RDB$PROFILER.FINISH_SESSION(TRUE)`); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(context.Background(), `DELETE FROM PLG$PROF_SESSIONS WHERE PROFILE_ID = ?`, otherID)
	runtime, err := New(dsn, "integration")
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if err := runtime.CleanupStale(ctx, time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM PLG$PROF_SESSIONS WHERE PROFILE_ID IN (?, ?)`, ownID, otherID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("got %d sessions, expected only unrelated session", count)
	}
}
