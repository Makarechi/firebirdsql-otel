package firebirdotel

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"slices"
	"sync/atomic"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

var mockDriverID uint64

func TestOpenWithDriverCreatesSpans(t *testing.T) {
	driverName := registerMockDriver(t)
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown tracer provider: %v", err)
		}
	})

	db, err := OpenWithDriver(
		driverName,
		"sysdba:masterkey@localhost:3050/tmp/test.fdb",
		WithTracerProvider(tp),
		WithSpanOptions(SpanOptions{Ping: true}),
	)
	if err != nil {
		t.Fatalf("open instrumented db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "execute procedure recalculate_invoice(?)", 42); err != nil {
		t.Fatalf("exec procedure: %v", err)
	}

	rows, err := db.QueryContext(ctx, "select * from procedure_report(?)", 42)
	if err != nil {
		t.Fatalf("query procedure: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close rows: %v", err)
	}

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	spans := exporter.GetSpans()
	if len(spans) < 3 {
		t.Fatalf("expected at least 3 spans, got %d", len(spans))
	}

	if !hasSpanName(spans, "sql.conn.exec") {
		t.Fatalf("expected sql.conn.exec span, got %v", spanNames(spans))
	}
	if !hasSpanName(spans, "sql.conn.query") {
		t.Fatalf("expected sql.conn.query span, got %v", spanNames(spans))
	}
	if !hasSpanName(spans, "sql.conn.ping") {
		t.Fatalf("expected sql.conn.ping span, got %v", spanNames(spans))
	}
	if !hasSpanAttribute(spans, "db.system.name", "firebirdsql") {
		t.Fatalf("expected db.system.name=firebirdsql attribute")
	}
}

func TestRegisterWithDriverCreatesReusableDriverName(t *testing.T) {
	driverName := registerMockDriver(t)
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown tracer provider: %v", err)
		}
	})

	instrumentedDriverName, err := RegisterWithDriver(driverName, "", WithTracerProvider(tp))
	if err != nil {
		t.Fatalf("register instrumented driver: %v", err)
	}

	db, err := sql.Open(instrumentedDriverName, "sysdba:masterkey@localhost/tmp/test.fdb")
	if err != nil {
		t.Fatalf("open registered driver: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.ExecContext(context.Background(), "execute procedure do_work"); err != nil {
		t.Fatalf("exec procedure: %v", err)
	}

	spans := exporter.GetSpans()
	if !hasSpanName(spans, "sql.conn.exec") {
		t.Fatalf("expected sql.conn.exec span, got %v", spanNames(spans))
	}
}

func TestRegisterDBStatsMetrics(t *testing.T) {
	driverName := registerMockDriver(t)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		if err := mp.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown meter provider: %v", err)
		}
	})

	db, err := OpenWithDriver(
		driverName,
		"sysdba:masterkey@localhost:3050/tmp/test.fdb",
		WithMeterProvider(mp),
	)
	if err != nil {
		t.Fatalf("open instrumented db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(7)

	reg, err := RegisterDBStatsMetrics(
		db,
		"sysdba:masterkey@localhost:3050/tmp/test.fdb",
		WithMeterProvider(mp),
	)
	if err != nil {
		t.Fatalf("register db stats metrics: %v", err)
	}
	t.Cleanup(func() {
		if err := reg.Unregister(); err != nil {
			t.Fatalf("unregister db stats metrics: %v", err)
		}
	})

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}

	names := metricNames(rm)
	if !slices.Contains(names, "db.sql.connection.max_open") {
		t.Fatalf("expected db.sql.connection.max_open metric, got %v", names)
	}
}

func TestDefaultAttributes(t *testing.T) {
	attrs := DefaultAttributes("sysdba:masterkey@localhost:3050/tmp/test.fdb")

	if !containsAttribute(attrs, "db.system.name", "firebirdsql") {
		t.Fatalf("expected db.system.name=firebirdsql in %v", attrs)
	}
	if !containsAttribute(attrs, "server.address", "localhost") {
		t.Fatalf("expected server.address=localhost in %v", attrs)
	}
	if !containsAttribute(attrs, "server.port", int64(3050)) {
		t.Fatalf("expected server.port=3050 in %v", attrs)
	}
}

func registerMockDriver(t *testing.T) string {
	t.Helper()

	id := atomic.AddUint64(&mockDriverID, 1)
	name := fmt.Sprintf("firebirdotel-mock-%d", id)
	sql.Register(name, mockDriver{})
	return name
}

func hasSpanName(spans tracetest.SpanStubs, name string) bool {
	return slices.ContainsFunc(spans, func(span tracetest.SpanStub) bool {
		return span.Name == name
	})
}

func spanNames(spans tracetest.SpanStubs) []string {
	names := make([]string, 0, len(spans))
	for _, span := range spans {
		names = append(names, span.Name)
	}
	return names
}

func hasSpanAttribute(spans tracetest.SpanStubs, key string, value any) bool {
	return slices.ContainsFunc(spans, func(span tracetest.SpanStub) bool {
		return containsAttribute(span.Attributes, key, value)
	})
}

func containsAttribute(attrs []attribute.KeyValue, key string, value any) bool {
	return slices.ContainsFunc(attrs, func(attr attribute.KeyValue) bool {
		return string(attr.Key) == key && attr.Value.AsInterface() == value
	})
}

func metricNames(rm metricdata.ResourceMetrics) []string {
	var names []string
	for _, scope := range rm.ScopeMetrics {
		for _, metric := range scope.Metrics {
			names = append(names, metric.Name)
		}
	}
	return names
}

type mockDriver struct{}

func (mockDriver) Open(name string) (driver.Conn, error) {
	return &mockConn{}, nil
}

func (mockDriver) OpenConnector(name string) (driver.Connector, error) {
	return mockConnector{}, nil
}

type mockConnector struct{}

func (mockConnector) Connect(context.Context) (driver.Conn, error) {
	return &mockConn{}, nil
}

func (mockConnector) Driver() driver.Driver {
	return mockDriver{}
}

type mockConn struct{}

func (*mockConn) Prepare(query string) (driver.Stmt, error) {
	return mockStmt{}, nil
}

func (*mockConn) Close() error {
	return nil
}

func (*mockConn) Begin() (driver.Tx, error) {
	return mockTx{}, nil
}

func (*mockConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return mockTx{}, nil
}

func (*mockConn) PrepareContext(context.Context, string) (driver.Stmt, error) {
	return mockStmt{}, nil
}

func (*mockConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}

func (*mockConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &mockRows{}, nil
}

func (*mockConn) Ping(context.Context) error {
	return nil
}

type mockStmt struct{}

func (mockStmt) Close() error {
	return nil
}

func (mockStmt) NumInput() int {
	return -1
}

func (mockStmt) Exec([]driver.Value) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}

func (mockStmt) Query([]driver.Value) (driver.Rows, error) {
	return &mockRows{}, nil
}

func (mockStmt) ExecContext(context.Context, []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}

func (mockStmt) QueryContext(context.Context, []driver.NamedValue) (driver.Rows, error) {
	return &mockRows{}, nil
}

type mockTx struct{}

func (mockTx) Commit() error {
	return nil
}

func (mockTx) Rollback() error {
	return nil
}

type mockRows struct{}

func (*mockRows) Columns() []string {
	return []string{"ok"}
}

func (*mockRows) Close() error {
	return nil
}

func (*mockRows) Next([]driver.Value) error {
	return io.EOF
}
