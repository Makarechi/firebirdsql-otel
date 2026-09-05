package firebirdotel

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/nakagami/firebirdsql"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type closingRows struct {
	scriptRows
	closeError error
	closed     chan struct{}
}

func (r *closingRows) Close() error {
	r.closes++
	if r.closed != nil {
		close(r.closed)
	}
	return r.closeError
}
func TestCursorCancellationOnAutomaticClose(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancelled", true: "deadline"}[deadline], func(t *testing.T) {
			cfg, rec, _ := setupTelemetry(t, DiagnosticConfig())
			raw := &closingRows{scriptRows: scriptRows{remaining: 2}, closed: make(chan struct{})}
			db, err := OpenDBWithConfig(scriptConnector{&scriptConn{rows: func() driver.Rows { return raw }}}, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx, cancel := context.WithCancel(t.Context())
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(t.Context(), 100*time.Millisecond)
			}
			defer cancel()
			rows, err := db.QueryContext(ctx, "select * from report")
			if err != nil {
				t.Fatal(err)
			}
			if !deadline {
				cancel()
			}
			select {
			case <-raw.closed:
			case <-time.After(5 * time.Second):
				t.Fatal("database/sql did not close cancelled rows")
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			if !errors.Is(rows.Err(), ctx.Err()) {
				t.Fatal("caller error lost", rows.Err())
			}
			cursors := 0
			for _, s := range rec.Ended() {
				if s.SpanKind() == trace.SpanKindInternal {
					cursors++
					if s.Status().Code != codes.Error || !containsAttribute(s.Attributes(), "error.type", outcome(ctx.Err())) || containsAttribute(s.Attributes(), "firebird.rows.eof", true) {
						t.Fatal("cancellation not recorded", s.Attributes(), s.Status())
					}
				}
			}
			if cursors != 1 || raw.closes != 1 || raw.nextCalls != 0 {
				t.Fatal("wrong lifecycle", cursors, raw.closes, raw.nextCalls)
			}
		})
	}
}
func TestCursorEOFFinalizesAfterDeferredCloseError(t *testing.T) {
	cfg, rec, _ := setupTelemetry(t, DiagnosticConfig())
	original := &firebirdsql.FbError{Message: canary, SQLCode: -901, SQLState: "HY000"}
	raw := &closingRows{closeError: original}
	db, err := OpenDBWithConfig(scriptConnector{&scriptConn{rows: func() driver.Rows { return raw }}}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.QueryContext(t.Context(), "select * from report")
	if err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Fatal("unexpected row")
	}
	if rows.Err() != original {
		t.Fatal("deferred error changed", rows.Err())
	}
	_ = rows.Close()
	found := 0
	for _, s := range rec.Ended() {
		if s.SpanKind() == trace.SpanKindInternal {
			found++
			if s.Status().Code != codes.Error || !containsAttribute(s.Attributes(), "firebird.rows.eof", true) || !containsAttribute(s.Attributes(), "firebird.error.sqlcode", int64(-901)) || strings.Contains(s.Status().Description, canary) {
				t.Fatal("deferred error omitted", s.Attributes(), s.Status())
			}
		}
	}
	if found != 1 || raw.closes != 1 || raw.nextCalls != 1 {
		t.Fatal("wrong cursor lifecycle")
	}
}
func TestNonRecordingCursorReturnsRawRows(t *testing.T) {
	cfg := DiagnosticConfig()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()))
	defer tp.Shutdown(t.Context())
	cfg.TracerProvider = tp
	cfg, err := normalizeConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	tel, err := newTelemetry(cfg)
	if err != nil {
		t.Fatal(err)
	}
	raw := &scriptRows{remaining: 1000}
	got, err := tel.queryResult(tel.start(t.Context(), "query", tel.describe("select * from report")), raw, nil)
	if err != nil || got != raw {
		t.Fatal("non-recording cursor still wrapped")
	}
}
func TestEOFFailsNonRowOperations(t *testing.T) {
	cfg, rec, _ := setupTelemetry(t, SafeConfig())
	db, err := OpenDBWithConfig(scriptConnector{&scriptConn{err: io.EOF}}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(t.Context(), "execute procedure work"); err != io.EOF {
		t.Fatal(err)
	}
	if _, err := db.QueryContext(t.Context(), "select * from report"); err != io.EOF {
		t.Fatal(err)
	}
	if len(rec.Ended()) != 2 {
		t.Fatal("wrong span count")
	}
	for _, s := range rec.Ended() {
		if s.Status().Code != codes.Error || !containsAttribute(s.Attributes(), "error.type", "connection") {
			t.Fatal("EOF was successful", s.Status(), s.Attributes())
		}
	}
}
