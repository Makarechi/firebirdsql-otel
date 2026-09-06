package firebirdotel

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"testing"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
)

type rejectingProvider struct{ metric.MeterProvider }

func (rejectingProvider) Meter(string, ...metric.MeterOption) metric.Meter {
	return rejectingMeter{noop.NewMeterProvider().Meter("test")}
}

type rejectingMeter struct{ metric.Meter }

func (rejectingMeter) Float64Histogram(string, ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	return nil, errors.New("refused histogram")
}

type ownedConnectorDriver struct {
	mockDriver
	c driver.Connector
}

func (d ownedConnectorDriver) OpenConnector(string) (driver.Connector, error) { return d.c, nil }
func TestOwnedConnectorClosedAfterInstrumentationFailure(t *testing.T) {
	cleanupErr := errors.New("cleanup failure")
	for _, closeErr := range []error{nil, cleanupErr} {
		raw := &cleanupConnector{err: closeErr}
		cfg := SafeConfig()
		cfg.MeterProvider = rejectingProvider{}
		db, err := OpenWithDriverConfig(ownedConnectorDriver{c: raw}, "unused", cfg)
		if db != nil || !errors.Is(err, ErrInstrumentation) || raw.calls != 1 {
			t.Fatal("owned connector leaked", err, raw.calls)
		}
		if closeErr != nil && !errors.Is(err, closeErr) {
			t.Fatal("cleanup error omitted", err)
		}
		if closeErr == nil && err != ErrInstrumentation {
			t.Fatal("original initialization error changed", err)
		}
		raw.calls = 0
		if _, err := OpenDBWithConfig(raw, cfg); !errors.Is(err, ErrInstrumentation) || raw.calls != 0 {
			t.Fatal("closed caller-owned connector on failure", err, raw.calls)
		}
	}
}
func TestCompatibilityExplicitDriverPreservesDSN(t *testing.T) {
	cfg, rec, _ := setupTelemetry(t, CompatibilityConfig())
	db, err := OpenWithDriverConfig(mockDriver{}, "user:password@db.example:3057/alias", cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(t.Context(), "select 1"); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, s := range rec.Ended() {
		if containsAttribute(s.Attributes(), "server.address", "db.example") && containsAttribute(s.Attributes(), "server.port", int64(3057)) {
			found = true
		}
	}
	if !found {
		t.Fatal("compatibility network attributes omitted", rec.Ended())
	}
}

type errorStatement struct {
	mockStmt
	err error
}

func (s errorStatement) ExecContext(context.Context, []driver.NamedValue) (driver.Result, error) {
	return nil, s.err
}
func (s errorStatement) QueryContext(context.Context, []driver.NamedValue) (driver.Rows, error) {
	return nil, s.err
}

type executionErrorConn struct{ scriptConn }

func (c *executionErrorConn) PrepareContext(context.Context, string) (driver.Stmt, error) {
	return errorStatement{err: c.err}, nil
}
func TestErrSkipMatchesDatabaseSQLFallback(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		for _, method := range []string{"exec", "query"} {
			for _, wrapped := range []bool{false, true} {
				t.Run(fmt.Sprintf("prepared=%v/%s/wrapped=%v", prepared, method, wrapped), func(t *testing.T) {
					original := error(driver.ErrSkip)
					if wrapped {
						original = fmt.Errorf("opaque: %w", original)
					}
					cfg, rec, reader := setupTelemetry(t, SafeConfig())
					// Exact connection ErrSkip falls back to a successful mock statement.
					var conn driver.Conn = &scriptConn{err: original}
					if prepared {
						conn = &executionErrorConn{scriptConn{err: original}}
					}
					db, err := OpenDBWithConfig(scriptConnector{conn}, cfg)
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()
					if prepared {
						stmt, e := db.PrepareContext(t.Context(), "select 1")
						if e != nil {
							t.Fatal(e)
						}
						defer stmt.Close()
						if method == "exec" {
							_, err = stmt.ExecContext(t.Context())
						} else {
							_, err = stmt.QueryContext(t.Context())
						}
					} else if method == "exec" {
						_, err = db.ExecContext(t.Context(), "select 1")
					} else {
						rows, e := db.QueryContext(t.Context(), "select 1")
						err = e
						if rows != nil {
							_ = rows.Close()
						}
					}
					wantFailure := prepared || wrapped
					if wantFailure && err != original || !wantFailure && err != nil {
						t.Fatal("caller/fallback behavior changed", err)
					}
					spans := rec.Ended()
					if len(spans) != 1 {
						t.Fatal("missing or duplicate span", len(spans))
					}
					if (spans[0].Status().Code == codes.Error) != wantFailure {
						t.Fatal("wrong span outcome", spans[0].Status())
					}
					var data metricdata.ResourceMetrics
					if err := reader.Collect(t.Context(), &data); err != nil {
						t.Fatal(err)
					}
					var count uint64
					for _, scope := range data.ScopeMetrics {
						for _, m := range scope.Metrics {
							if h, ok := m.Data.(metricdata.Histogram[float64]); ok {
								for _, point := range h.DataPoints {
									if v, ok := point.Attributes.Value("firebird.client.method"); ok && v.AsString() == method {
										count += point.Count
									}
								}
							}
						}
					}
					if count != 1 {
						t.Fatal("missing or duplicate duration", count)
					}
				})
			}
		}
	}
}

type wrappedEOFSetRows struct {
	scriptRows
	err error
}

func (*wrappedEOFSetRows) HasNextResultSet() bool { return true }
func (r *wrappedEOFSetRows) NextResultSet() error { return r.err }
func TestWrappedEOFRemainsCursorError(t *testing.T) {
	for _, resultSet := range []bool{false, true} {
		original := fmt.Errorf("opaque: %w", io.EOF)
		var raw driver.Rows = &scriptRows{err: original}
		if resultSet {
			raw = &wrappedEOFSetRows{err: original}
		}
		cfg, rec, _ := setupTelemetry(t, DiagnosticConfig())
		db, err := OpenDBWithConfig(scriptConnector{&scriptConn{rows: func() driver.Rows { return raw }}}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		rows, err := db.QueryContext(t.Context(), "select 1")
		if err != nil {
			t.Fatal(err)
		}
		if rows.Next() {
			t.Fatal("unexpected row")
		}
		if resultSet && rows.NextResultSet() {
			t.Fatal("unexpected result set")
		}
		if rows.Err() != original {
			t.Fatal("caller error identity changed", rows.Err())
		}
		_ = rows.Close()
		found := false
		for _, s := range rec.Ended() {
			if s.SpanKind() == trace.SpanKindInternal {
				found = true
				if s.Status().Code != codes.Error || containsAttribute(s.Attributes(), "firebird.rows.eof", true) {
					t.Fatal("wrapped EOF treated as exhaustion", s.Attributes(), s.Status())
				}
			}
		}
		if !found {
			t.Fatal("missing cursor span")
		}
	}
}
