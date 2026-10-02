package profiler

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestCheckSupportedRejectsReadOnlyReplica(t *testing.T) {
	for _, tc := range []struct {
		name        string
		readOnly    int64
		replicaMode int64
		wantError   bool
	}{
		{name: "primary", readOnly: 0, replicaMode: 0},
		{name: "read only replica", readOnly: 0, replicaMode: 1, wantError: true},
		{name: "read write replica", readOnly: 0, replicaMode: 2},
		{name: "read only database", readOnly: 1, replicaMode: 0, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectQuery("SELECT MON\\$READ_ONLY, MON\\$REPLICA_MODE FROM MON\\$DATABASE").
				WillReturnRows(sqlmock.NewRows([]string{"MON$READ_ONLY", "MON$REPLICA_MODE"}).AddRow(tc.readOnly, tc.replicaMode))
			err = (&Runtime{db: db}).CheckSupported(context.Background())
			if got := errors.Is(err, ErrReadOnlyDatabase); got != tc.wantError {
				t.Fatalf("CheckSupported error = %v, want read-only error %t", err, tc.wantError)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSafeAccessPathPreservesMultilineIndexPlan(t *testing.T) {
	path := "-> Table \"OBJ$CONTRACT_PERSONAL_DETAIL\" Access By ID\n    -> Index \"PK_OBJ$CONTRACT_PERSONAL_DETAIL\" Full Scan\n        -> Bitmap"
	want := "-> Table \"OBJ$CONTRACT_PERSONAL_DETAIL\" Access By ID -> Index \"PK_OBJ$CONTRACT_PERSONAL_DETAIL\" Full Scan -> Bitmap"
	if got := safeAccessPath(path); got != want {
		t.Fatalf("access path = %q, want %q", got, want)
	}
	if got := safeAccessPath("SQL 'sensitive'"); got != "" {
		t.Fatalf("unsafe access path exported: %q", got)
	}
	if got := safeAccessPath(strings.Repeat("A", 513)); got != "" {
		t.Fatalf("oversized access path exported")
	}
}
