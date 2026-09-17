package firebirdotel

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	otrace "go.opentelemetry.io/otel/trace"
)

type markerConn struct {
	markerErr, businessErr               error
	markers, businessCalls, markerCloses int
}

func (*markerConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c *markerConn) PrepareContext(_ context.Context, q string) (driver.Stmt, error) {
	return &markerStmt{conn: c, query: q}, nil
}

type markerStmt struct {
	conn  *markerConn
	query string
}

func (s *markerStmt) Close() error                             { s.conn.markerCloses++; return nil }
func (*markerStmt) NumInput() int                              { return 0 }
func (*markerStmt) Exec([]driver.Value) (driver.Result, error) { return nil, driver.ErrSkip }
func (s *markerStmt) Query([]driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), nil)
}
func (s *markerStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.conn.QueryContext(ctx, s.query, args)
}
func (*markerConn) Close() error              { return nil }
func (*markerConn) Begin() (driver.Tx, error) { return nil, driver.ErrSkip }
func (c *markerConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	c.businessCalls++
	return driver.RowsAffected(1), c.businessErr
}
func (c *markerConn) QueryContext(_ context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if !strings.HasPrefix(q, scopeSQL) || !strings.HasSuffix(q, "*/") || len(args) != 0 {
		return nil, errors.New("unexpected diagnostic SQL")
	}
	c.markers++
	if c.markerErr != nil {
		return nil, c.markerErr
	}
	return &markerRows{}, nil
}

type markerRows struct{ read bool }

func (*markerRows) Columns() []string { return []string{"result"} }
func (*markerRows) Close() error      { return nil }
func (r *markerRows) Next(v []driver.Value) error {
	if r.read {
		return io.EOF
	}
	r.read = true
	v[0] = int64(1)
	return nil
}

type markerTrace struct{}

func (markerTrace) Register(parent otrace.SpanContext) string {
	if parent.IsValid() && !parent.IsSampled() {
		return ""
	}
	return "0123456789abcdef0123456789abcdef"
}
func (markerTrace) Bind(string, otrace.SpanContext) {}
func (markerTrace) Discard(string)                  {}

type trackingMarkerTrace struct {
	completed, bound, discarded int
}

func (*trackingMarkerTrace) Register(otrace.SpanContext) string {
	return "0123456789abcdef0123456789abcdef"
}
func (t *trackingMarkerTrace) MarkerComplete(string)           { t.completed++ }
func (t *trackingMarkerTrace) Bind(string, otrace.SpanContext) { t.bound++ }
func (t *trackingMarkerTrace) Discard(string)                  { t.discarded++ }

func TestServerMarkersPreserveClientBehavior(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer tp.Shutdown(context.Background())
	server := markerTrace{}
	for _, mode := range []string{"enabled", "disabled", "filtered", "unsampled", "marker_failure", "fallback", "overlapping_cursor"} {
		t.Run(mode, func(t *testing.T) {
			c := SafeConfig()
			c.TracerProvider = tp
			c.ServerTrace = server
			wantMarkers := 1
			if mode == "disabled" {
				c.ServerTrace = nil
				wantMarkers = 0
			}
			if mode == "filtered" {
				c.Client.Filter = func(context.Context, Operation) bool { return false }
				wantMarkers = 0
			}
			parent, _ := otrace.TraceIDFromHex("11111111111111111111111111111111")
			parentSpan, _ := otrace.SpanIDFromHex("2222222222222222")
			flags := otrace.FlagsSampled
			if mode == "unsampled" {
				flags = 0
				wantMarkers = 0
			}
			ctx := otrace.ContextWithSpanContext(context.Background(), otrace.NewSpanContext(otrace.SpanContextConfig{TraceID: parent, SpanID: parentSpan, TraceFlags: flags}))
			businessErr := errors.New("business failure")
			raw := &markerConn{businessErr: businessErr}
			if mode == "marker_failure" {
				raw.markerErr = errors.New("SECRET_CANARY")
			}
			if mode == "fallback" {
				raw.businessErr = driver.ErrSkip
			}
			tel, err := newTelemetry(c)
			if err != nil {
				t.Fatal(err)
			}
			conn := &connState{raw: raw, t: tel}
			if mode == "overlapping_cursor" {
				conn.serverRows = 1
				conn.serverToken = server.Register(otrace.SpanContextFromContext(ctx))
				wantMarkers = 0
			}
			before := len(recorder.Ended())
			_, err = conn.ExecContext(ctx, "execute procedure P(?)", []driver.NamedValue{{Ordinal: 1, Value: 1}})
			if err != raw.businessErr || raw.businessCalls != 1 || raw.markers != wantMarkers || raw.markerCloses != wantMarkers {
				t.Fatalf("behavior changed: err=%v calls=%d markers=%d", err, raw.businessCalls, raw.markers)
			}
			if mode == "fallback" && len(recorder.Ended()) != before {
				t.Fatal("ErrSkip exported duplicate client span")
			}
		})
	}
}

func TestServerScopeUsesMarkerCompletionAndDiscardsFailedCalls(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprint(failed), func(t *testing.T) {
			recorder := tracetest.NewSpanRecorder()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
			defer tp.Shutdown(context.Background())
			server := &trackingMarkerTrace{}
			c := SafeConfig()
			c.TracerProvider = tp
			c.ServerTrace = server
			tel, err := newTelemetry(c)
			if err != nil {
				t.Fatal(err)
			}
			var businessErr error
			if failed {
				businessErr = errors.New("business failure")
			}
			conn := &connState{raw: &markerConn{businessErr: businessErr}, t: tel}
			_, err = conn.ExecContext(context.Background(), "execute procedure P(?)", []driver.NamedValue{{Ordinal: 1, Value: 1}})
			if err != businessErr || server.completed != 1 {
				t.Fatal("marker completion was not recorded", err, server)
			}
			if failed && (server.discarded != 1 || server.bound != 0) {
				t.Fatal("failed business call retained a server scope", server)
			}
			if !failed && (server.bound != 1 || server.discarded != 0) {
				t.Fatal("successful business call was not bound", server)
			}
		})
	}
}
