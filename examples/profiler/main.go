// A manual Firebird 5 profiling scope. Requires testdata/firebird5/schema.sql.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/nakagami/firebirdsql"
	"os"
	"time"
)

type report struct {
	Source, Correlation string
	ProfileID           int64
	Rows, Requests      int
}

func profile(ctx context.Context, db *sql.DB) (result report, err error) {
	result.Source = "profiler"
	result.Correlation = "scoped"
	conn, err := db.Conn(ctx)
	if err != nil {
		return result, err
	}
	defer conn.Close()
	// All commands run on this pinned connection, outside an old snapshot transaction.
	err = conn.QueryRowContext(ctx, `SELECT RDB$PROFILER.START_SESSION('firebirdotel synthetic example', NULL, NULL, NULL, 'DETAILED_REQUESTS') FROM RDB$DATABASE`).Scan(&result.ProfileID)
	if err != nil {
		return result, err
	}
	active := true
	defer func() {
		if active {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, cleanupErr := conn.ExecContext(cleanup, `EXECUTE PROCEDURE RDB$PROFILER.FINISH_SESSION(TRUE)`)
			err = errors.Join(err, cleanupErr)
		}
	}()
	rows, err := conn.QueryContext(ctx, `SELECT N FROM OTEL_REPORT`)
	if err != nil {
		return result, err
	}
	for rows.Next() {
		var n int
		if err = rows.Scan(&n); err != nil {
			break
		}
		result.Rows++
	}
	err = errors.Join(err, rows.Err(), rows.Close())
	if err != nil {
		return result, err
	}
	// Finish only after EOF/Close. Flush commits autonomously; the subsequent query
	// must not use an earlier snapshot transaction or it cannot see these records.
	if _, err = conn.ExecContext(ctx, `EXECUTE PROCEDURE RDB$PROFILER.FINISH_SESSION(TRUE)`); err != nil {
		return result, err
	}
	active = false
	err = conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM PLG$PROF_REQUESTS WHERE PROFILE_ID = ?`, result.ProfileID).Scan(&result.Requests)
	return result, err
}
func main() {
	db, err := sql.Open("firebirdsql", os.Getenv("FIREBIRD_TEST_DSN"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "open failed")
		os.Exit(1)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r, err := profile(ctx, db)
	if err != nil {
		fmt.Fprintln(os.Stderr, "profiling failed (check permissions and the synthetic fixture)")
		os.Exit(1)
	}
	fmt.Printf("source=%s correlation=%s session=%d rows=%d detailed_requests=%d\n", r.Source, r.Correlation, r.ProfileID, r.Rows, r.Requests)
}
