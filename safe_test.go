package firebirdotel

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/nakagami/firebirdsql"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

const canary = "SECRET_CANARY_742"

type scriptConnector struct{ conn driver.Conn }

func (s scriptConnector) Driver() driver.Driver                        { return mockDriver{} }
func (s scriptConnector) Connect(context.Context) (driver.Conn, error) { return s.conn, nil }

type scriptConn struct {
	mockConn
	err     error
	rows    func() driver.Rows
	mu      sync.Mutex
	queries []string
	args    [][]driver.NamedValue
}

func (s *scriptConn) ExecContext(ctx context.Context, q string, a []driver.NamedValue) (driver.Result, error) {
	s.mu.Lock()
	s.queries = append(s.queries, q)
	s.args = append(s.args, append([]driver.NamedValue(nil), a...))
	s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	return driver.RowsAffected(3), nil
}
func (s *scriptConn) QueryContext(ctx context.Context, q string, a []driver.NamedValue) (driver.Rows, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.rows != nil {
		return s.rows(), nil
	}
	return &scriptRows{remaining: 2}, nil
}

type scriptRows struct {
	remaining, nextCalls, closes int
	err                          error
}

func (r *scriptRows) Columns() []string { return []string{"id"} }
func (r *scriptRows) Close() error      { r.closes++; return nil }
func (r *scriptRows) Next(v []driver.Value) error {
	r.nextCalls++
	if r.remaining > 0 {
		r.remaining--
		v[0] = int64(r.remaining)
		return nil
	}
	if r.err != nil {
		return r.err
	}
	return io.EOF
}
func setupTelemetry(t *testing.T, c Config) (Config, *tracetest.SpanRecorder, *sdkmetric.ManualReader) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	c.TracerProvider = tp
	c.MeterProvider = mp
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()); _ = mp.Shutdown(context.Background()) })
	return c, rec, reader
}
func TestSafePrivacyErrorsAndIdentity(t *testing.T) {
	for _, mode := range []string{"", "database", "database/dup"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("OTEL_SEMCONV_STABILITY_OPT_IN", mode)
			cfg, rec, reader := setupTelemetry(t, SafeConfig())
			cfg.SpanAttributes = []attribute.KeyValue{attribute.String("request.only", "span")}
			cfg.MetricAttributes = []attribute.KeyValue{attribute.String("pool", "primary")}
			fb := &firebirdsql.FbError{Message: canary, Params: [][]string{{canary}}, SQLCode: -803, SQLState: "23505", GDSCodes: []int{335544665}}
			original := fmt.Errorf("%s: %w", canary, fb)
			raw := &scriptConn{err: original}
			db, err := OpenDBWithConfig(scriptConnector{raw}, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx, parent := cfg.TracerProvider.Tracer("app").Start(t.Context(), "request")
			q := "execute procedure pay('" + canary + "', ?)"
			_, err = db.ExecContext(ctx, q, canary)
			if err != original {
				t.Fatalf("error identity lost: %v", err)
			}
			var got *firebirdsql.FbError
			if !errors.As(err, &got) || got != fb {
				t.Fatal("As lost")
			}
			parent.End()
			spans := rec.Ended()
			if len(spans) != 2 {
				t.Fatalf("got %d spans", len(spans))
			}
			s := spans[0]
			if s.Name() != "EXECUTE PROCEDURE PAY" || s.Parent().SpanID() != parent.SpanContext().SpanID() || s.Status().Code != codes.Error {
				t.Fatalf("wrong span %s %+v", s.Name(), s.Status())
			}
			if !containsAttribute(s.Attributes(), "firebird.error.sqlcode", int64(-803)) {
				t.Fatal(s.Attributes())
			}
			if raw.queries[0] != q || raw.args[0][0].Value != canary {
				t.Fatal("mutated query")
			}
			for _, span := range spans {
				if strings.Contains(fmt.Sprint(span.Name(), span.Attributes(), span.Events(), span.Status()), canary) {
					t.Fatal("secret leaked")
				}
			}
			var m metricdata.ResourceMetrics
			if err := reader.Collect(t.Context(), &m); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(fmt.Sprint(m), canary) || strings.Contains(fmt.Sprint(m), "request.only") {
				t.Fatal("metric leak")
			}
		})
	}
}
func TestErrorClasses(t *testing.T) {
	for _, tt := range []struct {
		err  error
		kind string
	}{{nil, "ok"}, {io.EOF, "ok"}, {driver.ErrSkip, "fallback"}, {context.Canceled, "cancelled"}, {context.DeadlineExceeded, "deadline"}, {driver.ErrBadConn, "connection"}, {errors.ErrUnsupported, "unsupported"}, {&net.DNSError{Err: canary}, "network"}, {&firebirdsql.FbError{Message: canary}, "server"}, {errors.New(canary), "unknown"}} {
		if got := outcome(tt.err); got != tt.kind {
			t.Fatal(got, tt.kind)
		}
		if strings.Contains(fmt.Sprint(ErrorAttributes(tt.err)), canary) {
			t.Fatal("leak")
		}
	}
}
func TestFilteredAndUnsampledNeverChangeParent(t *testing.T) {
	for _, unsampled := range []bool{false, true} {
		cfg, rec, _ := setupTelemetry(t, DiagnosticConfig())
		if unsampled {
			cfg.TracerProvider = sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()))
		} else {
			cfg.Client.Filter = func(context.Context, Operation) bool { return false }
		}
		db, err := OpenDBWithConfig(scriptConnector{&scriptConn{err: errors.New(canary)}}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		parentTP := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
		ctx, parent := parentTP.Tracer("parent").Start(t.Context(), "HTTP")
		_, _ = db.ExecContext(ctx, "select 1")
		parent.End()
		_ = db.Close()
		if len(rec.Ended()) != 1 || rec.Ended()[0].Status().Code != codes.Unset || len(rec.Ended()[0].Events()) != 0 {
			t.Fatal("changed parent")
		}
		_ = parentTP.Shutdown(t.Context())
	}
}
func TestRowsLifetime(t *testing.T) {
	for _, tc := range []struct {
		name                string
		early               bool
		err                 error
		delivered, attempts int64
	}{{"eof", false, nil, 2, 3}, {"early", true, nil, 1, 1}, {"error", false, context.Canceled, 2, 3}} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, rec, _ := setupTelemetry(t, DiagnosticConfig())
			r := &scriptRows{remaining: 2, err: tc.err}
			db, err := OpenDBWithConfig(scriptConnector{&scriptConn{rows: func() driver.Rows { return r }}}, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			rows, err := db.QueryContext(t.Context(), "select * from report(?)", 42)
			if err != nil {
				t.Fatal(err)
			}
			var n int
			for rows.Next() {
				if err := rows.Scan(&n); err != nil {
					t.Fatal(err)
				}
				if tc.early {
					break
				}
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			_ = rows.Close()
			if r.closes != 1 || int64(r.nextCalls) != tc.attempts {
				t.Fatal("hidden reads/double close", r)
			}
			var query, cursor sdktrace.ReadOnlySpan
			for _, s := range rec.Ended() {
				if s.SpanKind() == trace.SpanKindInternal {
					cursor = s
				}
				if s.Name() == "SELECT REPORT" {
					query = s
				}
			}
			if cursor == nil || query == nil || cursor.Parent().SpanID() != query.SpanContext().SpanID() {
				t.Fatal("cursor parent")
			}
			if !containsAttribute(cursor.Attributes(), "firebird.rows.delivered", tc.delivered) || !containsAttribute(cursor.Attributes(), "firebird.rows.next_attempts", tc.attempts) {
				t.Fatal(cursor.Attributes())
			}
			if tc.err != nil && !errors.Is(rows.Err(), tc.err) {
				t.Fatal("lost rows error")
			}
		})
	}
}
func TestConcurrentPreparedReuseAndTransactions(t *testing.T) {
	cfg, rec, _ := setupTelemetry(t, SafeConfig())
	db, err := OpenDBWithConfig(mockConnector{}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stmt, err := db.PrepareContext(t.Context(), "execute procedure work(?)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, parent := cfg.TracerProvider.Tracer("app").Start(t.Context(), "request")
			defer parent.End()
			if _, err := stmt.ExecContext(ctx, 42); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	for _, s := range rec.Ended() {
		if s.Name() == "EXECUTE PROCEDURE WORK" && !s.Parent().IsValid() {
			t.Fatal("prepared reuse lost parent")
		}
	}
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = tx.Commit()
	tx, err = db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback()
}
func TestCapabilitiesAndRaw(t *testing.T) {
	cfg, _, _ := setupTelemetry(t, DiagnosticConfig())
	wrapped, err := WrapConnector(mockConnector{}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	c, err := wrapped.Connect(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	raw := &mockConn{}
	interfaces := []any{(*driver.ConnBeginTx)(nil), (*driver.ConnPrepareContext)(nil), (*driver.Queryer)(nil), (*driver.QueryerContext)(nil), (*driver.Execer)(nil), (*driver.ExecerContext)(nil), (*driver.Pinger)(nil), (*driver.NamedValueChecker)(nil), (*driver.Validator)(nil), (*driver.SessionResetter)(nil)}
	for _, v := range interfaces {
		typ := reflect.TypeOf(v).Elem()
		if reflect.TypeOf(raw).Implements(typ) != reflect.TypeOf(c).Implements(typ) {
			t.Fatal("wrong capability", typ)
		}
	}
	if _, err := WrapConnector(wrapped, cfg); err == nil {
		t.Fatal("accepted nested wrapper")
	}
	db, err := OpenDBWithConfig(mockConnector{}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := Raw(conn, func(v any) error {
		if _, ok := v.(*mockConn); !ok {
			t.Fatalf("not native: %T", v)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPoolRegistrationUsesOnlyMetricDimensions(t *testing.T) {
	cfg, _, reader := setupTelemetry(t, SafeConfig())
	cfg.SpanAttributes = []attribute.KeyValue{attribute.String("private.span", canary)}
	cfg.MetricAttributes = []attribute.KeyValue{attribute.String("pool", "primary")}
	db, err := OpenDBWithConfig(mockConnector{}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(7)
	reg, err := RegisterDBStatsMetricsWithConfig(db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	var data metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(fmt.Sprint(data), canary) || !strings.Contains(fmt.Sprint(data), "primary") {
		t.Fatal("dimensions mixed")
	}
	if err := reg.Unregister(); err != nil {
		t.Fatal(err)
	}
	data = metricdata.ResourceMetrics{}
	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(fmt.Sprint(data), "db.sql.connection") {
		t.Fatal("callback retained")
	}
}
func TestErrSkipUsesSQLFallback(t *testing.T) {
	cfg, rec, _ := setupTelemetry(t, SafeConfig())
	db, err := OpenDBWithConfig(scriptConnector{&scriptConn{err: driver.ErrSkip}}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(t.Context(), "execute procedure WORK(?)", 42); err != nil {
		t.Fatal(err)
	}
	if len(rec.Ended()) != 1 || rec.Ended()[0].Name() != "EXECUTE PROCEDURE WORK" || rec.Ended()[0].Status().Code == codes.Error {
		t.Fatal("fallback double span or false error", rec.Ended())
	}
}

type richConn struct {
	mockConn
	checked, resets int
}

func (c *richConn) CheckNamedValue(v *driver.NamedValue) error { c.checked++; return nil }
func (c *richConn) ResetSession(context.Context) error         { c.resets++; return nil }
func (c *richConn) IsValid() bool                              { return true }
func TestOptionalConnForwarding(t *testing.T) {
	cfg, _, _ := setupTelemetry(t, SafeConfig())
	raw := &richConn{}
	db, err := OpenDBWithConfig(scriptConnector{raw}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 2; i++ {
		if _, err := db.ExecContext(t.Context(), "execute procedure P(?)", struct{ N int }{42}); err != nil {
			t.Fatal("named value check not forwarded", err)
		}
	}
	if raw.checked != 2 || raw.resets < 1 {
		t.Fatal(raw.checked, raw.resets)
	}
}

type multiRows struct {
	scriptRows
	set int
}

func (r *multiRows) HasNextResultSet() bool { return r.set == 0 }
func (r *multiRows) NextResultSet() error {
	if r.set != 0 {
		return io.EOF
	}
	r.set++
	r.remaining = 1
	return nil
}
func (r *multiRows) ColumnTypeDatabaseTypeName(int) string             { return "INTEGER" }
func (r *multiRows) ColumnTypeLength(int) (int64, bool)                { return 4, true }
func (r *multiRows) ColumnTypeNullable(int) (bool, bool)               { return false, true }
func (r *multiRows) ColumnTypePrecisionScale(int) (int64, int64, bool) { return 10, 0, true }
func (r *multiRows) ColumnTypeScanType(int) reflect.Type               { return reflect.TypeOf(int64(0)) }
func TestOptionalRowsAndNextResultSet(t *testing.T) {
	cfg, rec, _ := setupTelemetry(t, DiagnosticConfig())
	raw := &multiRows{scriptRows: scriptRows{remaining: 1}}
	db, err := OpenDBWithConfig(scriptConnector{&scriptConn{rows: func() driver.Rows { return raw }}}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.QueryContext(t.Context(), "select * from REPORT(?)", 1)
	if err != nil {
		t.Fatal(err)
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	typ := types[0]
	if typ.DatabaseTypeName() != "INTEGER" || typ.ScanType() != reflect.TypeOf(int64(0)) {
		t.Fatal("column type lost")
	}
	if n, ok := typ.Length(); !ok || n != 4 {
		t.Fatal("length lost")
	}
	if n, ok := typ.Nullable(); !ok || n {
		t.Fatal("nullable lost")
	}
	if p, s, ok := typ.DecimalSize(); !ok || p != 10 || s != 0 {
		t.Fatal("precision lost")
	}
	for set := 0; set < 2; set++ {
		for rows.Next() {
			var n int
			if err := rows.Scan(&n); err != nil {
				t.Fatal(err)
			}
		}
		if set == 0 && !rows.NextResultSet() {
			t.Fatal("lost second result set")
		}
	}
	_ = rows.Close()
	if raw.closes != 1 || raw.nextCalls != 4 {
		t.Fatal("changed result lifecycle", raw)
	}
	count := 0
	for _, s := range rec.Ended() {
		if s.SpanKind() == trace.SpanKindInternal {
			count++
			if !containsAttribute(s.Attributes(), "firebird.rows.delivered", int64(2)) {
				t.Fatal(s.Attributes())
			}
		}
	}
	if count != 1 {
		t.Fatal("cursor ended twice", count)
	}
}
