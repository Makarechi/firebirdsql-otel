package profiler

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
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

func TestReportExportsTopSourcesAndBoundsDetail(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	columns := []string{"STATEMENT_ID", "REQUEST_ID", "CURSOR_ID", "RECORD_SOURCE_ID", "PARENT_RECORD_SOURCE_ID", "LEVEL", "ACCESS_PATH", "OPEN_COUNTER", "FETCH_COUNTER", "OPEN_TOTAL_ELAPSED_TIME", "FETCH_TOTAL_ELAPSED_TIME"}
	rows := sqlmock.NewRows(columns)
	for i := 0; i < 65; i++ {
		rows.AddRow(1, 1, 1, i+1, nil, 1, "-> Index \"IDX_CONTRACT\" Range Scan", 1, i+1, int64((65-i)*1000000), int64(1000000))
	}
	mock.ExpectQuery("FROM PLG\\$PROF_RECORD_SOURCE_STATS").WithArgs(int64(17)).WillReturnRows(rows)
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer tp.Shutdown(context.Background())
	_, span := tp.Tracer("test").Start(context.Background(), "profile")
	if err := (&Runtime{db: db}).report(context.Background(), 17, span); err != nil {
		t.Fatal(err)
	}
	span.End()
	spans := recorder.Ended()
	if len(spans) != 1 || len(spans[0].Events()) != 64 {
		t.Fatalf("got %d events", len(spans[0].Events()))
	}
	attrs := make(map[string]attribute.Value)
	for _, kv := range spans[0].Attributes() {
		attrs[string(kv.Key)] = kv.Value
	}
	if attrs["firebird.profiler.record_sources"].AsInt64() != 65 || !attrs["firebird.profiler.truncated"].AsBool() {
		t.Fatalf("report bound missing: %v", attrs)
	}
	if attrs["firebird.profiler.top.01.ms"].AsFloat64() != 66 || attrs["firebird.profiler.top.05.fetch_count"].AsInt64() != 5 {
		t.Fatalf("top source summary wrong: %v", attrs)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
