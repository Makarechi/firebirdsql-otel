package firebirdotel

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	servertrace "github.com/Makarechi/firebirdsql-otel/trace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	otrace "go.opentelemetry.io/otel/trace"
)

func TestFirebird5ServerSpans(t *testing.T) {
	dsn := integrationDSN(t)
	binary := os.Getenv("FIREBIRD_TRACE_BINARY")
	if binary == "" {
		t.Skip("requires trace worker")
	}
	u, err := url.Parse("firebird://" + strings.TrimPrefix(dsn, "firebird://"))
	if err != nil {
		t.Fatal(err)
	}
	password, _ := u.User.Password()
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer tp.Shutdown(context.Background())
	runtime, err := servertrace.NewSpans(servertrace.SpanConfig{TracerProvider: tp})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if err := runtime.Start(ctx, servertrace.Config{Executable: binary, Address: u.Host, User: u.User.Username(), Password: password, Database: u.Path, Name: "firebirdotel-nested-spans"}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		stop, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := runtime.Shutdown(stop); err != nil {
			t.Error(err)
		}
	}()
	c := SafeConfig()
	c.TracerProvider = tp
	c.ServerTrace = runtime
	db, err := OpenWithConfig(dsn, c)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	tracer := tp.Tracer("application")
	parents := make(map[otrace.TraceID]string)
	prepared, err := db.PrepareContext(ctx, "execute procedure OTEL_OUTER(?)")
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	for _, kind := range []string{"direct", "prepared", "prepared_again", "selectable"} {
		request, span := tracer.Start(ctx, kind)
		parents[span.SpanContext().TraceID()] = kind
		switch kind {
		case "direct":
			_, err = db.ExecContext(request, "execute procedure OTEL_OUTER(?)", 7)
		case "prepared", "prepared_again":
			_, err = prepared.ExecContext(request, 8)
		case "selectable":
			var rows *sql.Rows
			rows, err = db.QueryContext(request, "select N from OTEL_REPORT")
			if err == nil {
				for rows.Next() {
					var n int
					if err = rows.Scan(&n); err != nil {
						break
					}
				}
				if err == nil {
					err = rows.Err()
				}
				rows.Close()
			}
		}
		span.End()
		if err != nil {
			t.Fatal(kind, err)
		}
	}
	var wg sync.WaitGroup
	errors := make(chan error, 8)
	for i := 0; i < 8; i++ {
		request, span := tracer.Start(ctx, fmt.Sprintf("concurrent-%d", i))
		parents[span.SpanContext().TraceID()] = fmt.Sprintf("concurrent-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer span.End()
			_, err := db.ExecContext(request, "execute procedure OTEL_RECURSE(?)", 3)
			errors <- err
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	// QueryRow closes before client EOF. No following query may be needed to flush
	// the final server execution into telemetry during otherwise idle traffic.
	request, last := tracer.Start(ctx, "query_row")
	parents[last.SpanContext().TraceID()] = "query_row"
	var value int
	if err := db.QueryRowContext(request, "select N from OTEL_REPORT").Scan(&value); err != nil || value != 1 {
		t.Fatal("QueryRow", value, err)
	}
	last.End()
	for {
		found := make(map[string]map[string]bool)
		counts := make(map[string]int)
		all := make(map[otrace.SpanID]sdktrace.ReadOnlySpan)
		snapshot := recorder.Ended()
		for _, span := range snapshot {
			all[span.SpanContext().SpanID()] = span
		}
		for _, span := range snapshot {
			if span.InstrumentationScope().Name != "github.com/Makarechi/firebirdsql-otel/trace" {
				continue
			}
			kind, ok := parents[span.SpanContext().TraceID()]
			if !ok {
				t.Fatal("server span attached to unrelated trace")
			}
			if found[kind] == nil {
				found[kind] = make(map[string]bool)
			}
			found[kind][span.Name()] = true
			parent, ok := all[span.Parent().SpanID()]
			if !ok {
				t.Fatal("missing actual parent", span.Name())
			}
			if span.Name() == "OTEL_NESTED_A" && parent.Name() != "OTEL_OUTER" {
				t.Fatal("wrong nested parent", parent.Name())
			}
			if span.Name() == "OTEL_RECURSE" {
				counts[kind]++
			}
			if span.Name() == "OTEL_NESTED_B" {
				t.Fatal("unexecuted branch exported")
			}
		}
		concurrent := true
		for i := 0; i < 8; i++ {
			if counts[fmt.Sprintf("concurrent-%d", i)] != 4 {
				concurrent = false
			}
		}
		if concurrent && found["direct"]["OTEL_NESTED_A"] && found["direct"]["OTEL_DOUBLE"] && found["direct"]["OTEL_A_CHANGED"] && found["prepared"]["OTEL_NESTED_A"] && found["prepared_again"]["OTEL_NESTED_A"] && found["selectable"]["OTEL_REPORT"] && found["query_row"]["OTEL_REPORT"] {
			t.Logf("server spans by request: %+v", found)
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("missing server spans: %+v; error: %v", found, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}
