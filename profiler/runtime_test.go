package profiler

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestProfilerDurationBucketsResolveMillisecondCosts(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(context.Background())
	runtime := &Runtime{name: "primary"}
	runtime.initMetrics(provider)
	runtime.recordStage("cleanup", 40*time.Millisecond, nil)
	var got metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &got); err != nil {
		t.Fatal(err)
	}
	for _, scope := range got.ScopeMetrics {
		for _, value := range scope.Metrics {
			if value.Name != "firebird.profiler.stage.duration" {
				continue
			}
			points := value.Data.(metricdata.Histogram[float64]).DataPoints
			if len(points) != 1 || len(points[0].Bounds) < 6 || points[0].Bounds[5] != .05 {
				t.Fatalf("unexpected profiler histogram boundaries: %+v", points)
			}
			return
		}
	}
	t.Fatal("profiler duration histogram missing")
}

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

func TestDeleteMeasuresConnectionAndExecutionSeparately(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectExec("DELETE FROM PLG\\$PROF_SESSIONS WHERE PROFILE_ID").WithArgs(int64(17)).WillReturnResult(sqlmock.NewResult(0, 1))
	acquire, execute, acquireErr, executeErr := (&Runtime{db: db}).delete(context.Background(), 17)
	if acquireErr != nil || executeErr != nil || acquire < 0 || execute <= 0 {
		t.Fatalf("delete phases: acquire=%s execute=%s errors=%v/%v", acquire, execute, acquireErr, executeErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCleanupStaleCountsRemainingProfiles(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(context.Background())
	runtime := &Runtime{db: db, name: "primary"}
	runtime.initMetrics(provider)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM RDB\\$RELATIONS").WillReturnRows(sqlmock.NewRows([]string{"COUNT"}).AddRow(1))
	mock.ExpectExec("DELETE FROM PLG\\$PROF_SESSIONS WHERE DESCRIPTION").WithArgs("firebirdotel/primary/", sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM PLG\\$PROF_SESSIONS WHERE DESCRIPTION").WithArgs("firebirdotel/primary/", sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"COUNT"}).AddRow(1))
	if err := runtime.CleanupStale(context.Background(), time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	var got metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &got); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, scope := range got.ScopeMetrics {
		for _, value := range scope.Metrics {
			if value.Name != "firebird.profiler.stale.remaining" {
				continue
			}
			points := value.Data.(metricdata.Gauge[int64]).DataPoints
			if len(points) != 1 || points[0].Value != 1 {
				t.Fatalf("remaining profiles = %+v", points)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("remaining-profile gauge missing")
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
