package firebirdotel

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/nakagami/firebirdsql"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestRegisterWithConfigDefaultsAndValidation(t *testing.T) {
	before := sql.Drivers()
	for _, c := range []Config{{Profile: "invalid"}, CompatibilityConfig(), {MeterProvider: rejectingProvider{}}} {
		if name, err := RegisterWithConfig(c); err == nil || name != "" {
			t.Fatalf("registered invalid configuration: %q, %v", name, err)
		}
	}
	if !reflect.DeepEqual(before, sql.Drivers()) {
		t.Fatal("failed initialization consumed a global registration")
	}
	name, err := RegisterWithConfig(Config{})
	if err != nil {
		t.Fatal(err)
	}
	// Opening is still lazy; the application owns the ordinary database/sql pool.
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(7)
	if db.Stats().MaxOpenConnections != 7 || db.Stats().OpenConnections != 0 {
		t.Fatal("registration changed pool ownership or opened a connection")
	}
	if err := db.PingContext(t.Context()); !errors.Is(err, firebirdsql.ErrDsnUserUnknown) {
		t.Fatal("native connection error changed", err)
	}
	if _, err := db.Driver().Open(""); !errors.Is(err, firebirdsql.ErrDsnUserUnknown) {
		t.Fatal("Driver.Open changed native error", err)
	}
	if _, err := OpenWithDriverConfig(db.Driver(), "unused", Config{}); err == nil {
		t.Fatal("allowed double instrumentation of registered driver")
	}
	if _, err := WrapConnector(&dsnConnector{d: db.Driver()}, Config{}); err == nil {
		t.Fatal("allowed double instrumentation through a connector adapter")
	}
}

func TestRegisteredDriverSharesConfigurationWithoutMixingPools(t *testing.T) {
	cfg, rec, _ := setupTelemetry(t, Config{})
	cfg.Connection.ParseDSNNetwork = true
	cfg.SpanAttributes = []attribute.KeyValue{attribute.String("label", "original")}
	normalized, err := normalizeConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	tel, err := newTelemetry(normalized)
	if err != nil {
		t.Fatal(err)
	}
	name, err := registerSafeDriver(&registeredSafeDriver{native: mockDriver{}, t: tel})
	if err != nil {
		t.Fatal(err)
	}
	cfg.SpanAttributes[0] = attribute.String("label", canary)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dsn := fmt.Sprintf("%s:password@host%d.example:3050/private-path", canary, i)
			db, err := sql.Open(name, dsn)
			if err != nil {
				t.Error(err)
				return
			}
			defer db.Close()
			query := fmt.Sprintf("execute procedure P%d('%s')", i, canary)
			if _, err := db.ExecContext(t.Context(), query); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if len(rec.Ended()) != 8 {
		t.Fatal("missing or duplicate spans", len(rec.Ended()))
	}
	for _, s := range rec.Ended() {
		if strings.Contains(fmt.Sprint(s.Name(), s.Attributes(), s.Events(), s.Status()), canary) {
			t.Fatal("secret or mutable caller configuration leaked")
		}
		attrs := attribute.NewSet(s.Attributes()...)
		proc, _ := attrs.Value("db.stored_procedure.name")
		host, _ := attrs.Value("server.address")
		want := "host" + strings.TrimPrefix(proc.AsString(), "P") + ".example"
		if host.AsString() != want {
			t.Fatal("mixed connection attributes", host.AsString(), want)
		}
	}
}

func TestRegisterWithConfigConcurrentNames(t *testing.T) {
	names := make(chan string, 8)
	var wg sync.WaitGroup
	for i := 0; i < cap(names); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name, err := RegisterWithConfig(Config{})
			if err != nil {
				t.Error(err)
				return
			}
			names <- name
		}()
	}
	wg.Wait()
	close(names)
	seen := make(map[string]bool)
	for name := range names {
		if seen[name] {
			t.Fatal("duplicate driver name", name)
		}
		seen[name] = true
	}
	if len(seen) != cap(names) {
		t.Fatal("missing registrations", len(seen))
	}
}

func TestFirebird5RegisteredDriver(t *testing.T) {
	dsn := integrationDSN(t)
	cfg, rec, reader := setupTelemetry(t, Config{})
	name, err := RegisterWithConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open(name, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	ctx, parent := cfg.TracerProvider.Tracer("application").Start(t.Context(), "request")
	defer parent.End()
	var value string
	if err := db.QueryRowContext(ctx, "select '"+canary+"' from rdb$database").Scan(&value); err != nil || value != canary {
		t.Fatal("instrumentation changed SQL result", value, err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.PrepareContext(ctx, "select cast(? as integer) from rdb$database")
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := stmt.QueryRowContext(ctx, 42).Scan(&n); err != nil || n != 42 {
		t.Fatal("prepared query changed", n, err)
	}
	if err := stmt.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := Raw(conn, func(v any) error {
		if _, ok := v.(driver.Pinger); !ok || reflect.TypeOf(v).Elem().PkgPath() != "github.com/nakagami/firebirdsql" {
			return fmt.Errorf("unexpected native connection %T", v)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	err = db.QueryRowContext(ctx, "select cast('"+canary+"' as integer) from rdb$database").Scan(&n)
	var fbErr *firebirdsql.FbError
	if !errors.As(err, &fbErr) {
		t.Fatal("lost original native error", err)
	}
	for _, span := range rec.Ended() {
		if span.Parent().SpanID() != parent.SpanContext().SpanID() || strings.Contains(fmt.Sprint(span.Name(), span.Attributes(), span.Events(), span.Status()), canary) {
			t.Fatal("lost parent or leaked a secret", span.Name())
		}
	}
	if len(rec.Ended()) != 5 {
		t.Fatal("unexpected spans", len(rec.Ended()))
	}
	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &metrics); err != nil {
		t.Fatal(err)
	}
	var operations uint64
	for _, scope := range metrics.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == "db.client.operation.duration" {
				for _, p := range m.Data.(metricdata.Histogram[float64]).DataPoints {
					operations += p.Count
				}
			}
		}
	}
	if operations < 5 || strings.Contains(fmt.Sprint(metrics), canary) {
		t.Fatal("missing operation metrics or leaked secret")
	}
}
