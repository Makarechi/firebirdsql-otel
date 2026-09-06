package firebirdotel

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Makarechi/firebirdsql-otel/internal/otelsqlfirebird"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type transactionRowsConn struct{ scriptConn }
type transactionRowsStmt struct {
	mockStmt
	rows func() driver.Rows
}

func (c *transactionRowsConn) PrepareContext(context.Context, string) (driver.Stmt, error) {
	return transactionRowsStmt{rows: c.rows}, nil
}
func (s transactionRowsStmt) QueryContext(context.Context, []driver.NamedValue) (driver.Rows, error) {
	return s.rows(), nil
}

func TestTransactionCancellationReachesCursor(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		for _, deadline := range []bool{false, true} {
			cfg, rec, _ := setupTelemetry(t, DiagnosticConfig())
			raw := &closingRows{scriptRows: scriptRows{remaining: 2}, closed: make(chan struct{})}
			db, err := OpenDBWithConfig(scriptConnector{&transactionRowsConn{scriptConn{rows: func() driver.Rows { return raw }}}}, cfg)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(t.Context(), 100*time.Millisecond)
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			var rows *sql.Rows
			if prepared {
				stmt, e := tx.PrepareContext(t.Context(), "select * from report")
				if e != nil {
					t.Fatal(e)
				}
				defer stmt.Close()
				rows, err = stmt.QueryContext(t.Context())
			} else {
				rows, err = tx.QueryContext(t.Context(), "select * from report")
			}
			if err != nil {
				t.Fatal(err)
			}
			if !deadline {
				cancel()
			}
			select {
			case <-raw.closed:
			case <-time.After(5 * time.Second):
				t.Fatal("transaction cancellation did not close rows")
			}
			_ = rows.Close()
			if !errors.Is(rows.Err(), ctx.Err()) {
				t.Fatal("caller cancellation changed", rows.Err())
			}
			count := 0
			for _, span := range rec.Ended() {
				if span.SpanKind() == trace.SpanKindInternal {
					count++
					if span.Status().Code != codes.Error || !containsAttribute(span.Attributes(), "firebird.rows.outcome", outcome(ctx.Err())) {
						t.Fatal("transaction cancellation omitted", span.Attributes())
					}
				}
			}
			if count != 1 {
				t.Fatal("wrong cursor count", count)
			}
			cancel()
			_ = tx.Rollback()
			_ = db.Close()
		}
	}
}

func TestTransactionCompletionRetainsValuesWithoutCancellation(t *testing.T) {
	type key struct{}
	for _, method := range []string{"commit", "rollback"} {
		cfg, rec, _ := setupTelemetry(t, SafeConfig())
		calls := 0
		cfg.Client.Filter = func(ctx context.Context, op Operation) bool {
			if op.Method == method {
				calls++
				if ctx.Err() != nil || ctx.Value(key{}) != "kept" {
					t.Error("completion lost live values", ctx.Err())
				}
			}
			return ctx.Err() == nil
		}
		normalized, err := normalizeConfig(cfg)
		if err != nil {
			t.Fatal(err)
		}
		tel, err := newTelemetry(normalized)
		if err != nil {
			t.Fatal(err)
		}
		ctx, parent := cfg.TracerProvider.Tracer("test").Start(context.WithValue(t.Context(), key{}, "kept"), "parent")
		ctx, cancel := context.WithCancel(ctx)
		conn := &connState{raw: &mockConn{}, t: tel}
		tx, err := conn.BeginTx(ctx, driver.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		if method == "commit" {
			err = tx.Commit()
		} else {
			err = tx.Rollback()
		}
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, span := range rec.Ended() {
			if span.Name() == method {
				found = true
				if span.Parent().SpanID() != parent.SpanContext().SpanID() {
					t.Fatal("lost parent")
				}
			}
		}
		if calls != 1 || !found || conn.transactionContext() != nil {
			t.Fatal("completion not observed or context retained")
		}
		parent.End()
	}
}

type oneShotRows struct {
	multiRows
	probes int
}

func (r *oneShotRows) HasNextResultSet() bool { r.probes++; return r.probes == 1 }
func TestResultSetProbeIsForwardedOnce(t *testing.T) {
	cfg, rec, _ := setupTelemetry(t, DiagnosticConfig())
	raw := &oneShotRows{multiRows: multiRows{scriptRows: scriptRows{remaining: 1}}}
	db, err := OpenDBWithConfig(scriptConnector{&scriptConn{rows: func() driver.Rows { return raw }}}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.QueryContext(t.Context(), "select * from report")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for set := 0; set < 2; set++ {
		n := 0
		for rows.Next() {
			n++
		}
		if n != 1 {
			t.Fatal("result set lost", set, n)
		}
		if set == 0 && !rows.NextResultSet() {
			t.Fatal("next result set hidden", rows.Err())
		}
	}
	if raw.probes != 2 {
		t.Fatal("extra result set probe", raw.probes)
	}
	for _, span := range rec.Ended() {
		if span.SpanKind() == trace.SpanKindInternal && !containsAttribute(span.Attributes(), "firebird.rows.eof", true) {
			t.Fatal("final EOF missing")
		}
	}
}

type namedFixtureConnector struct{ mockConnector }

func (namedFixtureConnector) Driver() driver.Driver {
	return otelsqlfirebird.Driver{Driver: mockDriver{}}
}
func TestCoincidentalOtelsqlPackageNameIsAccepted(t *testing.T) {
	db, err := OpenDBWithConfig(namedFixtureConnector{}, SafeConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(t.Context(), "select 1"); err != nil {
		t.Fatal(err)
	}
}

func TestNormalTransactionCompletionWithOpenRows(t *testing.T) {
	for _, method := range []string{"commit", "rollback", "close"} {
		for _, prepared := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/prepared=%v", method, prepared), func(t *testing.T) {
				cfg, rec, _ := setupTelemetry(t, DiagnosticConfig())
				raw := &closingRows{scriptRows: scriptRows{remaining: 2}, closed: make(chan struct{})}
				db, err := OpenDBWithConfig(scriptConnector{&transactionRowsConn{scriptConn{rows: func() driver.Rows { return raw }}}}, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				var rows *sql.Rows
				if prepared {
					stmt, e := tx.PrepareContext(t.Context(), "select * from report")
					if e != nil {
						t.Fatal(e)
					}
					defer stmt.Close()
					rows, err = stmt.QueryContext(t.Context())
				} else {
					rows, err = tx.QueryContext(t.Context(), "select * from report")
				}
				if err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() {
					switch method {
					case "commit":
						done <- tx.Commit()
					case "rollback":
						done <- tx.Rollback()
					default:
						done <- rows.Close()
					}
				}()
				select {
				case err = <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("completion blocked on open rows")
				}
				if ctx.Err() != nil {
					t.Fatal("original BeginTx context cancelled")
				}
				if method == "close" {
					if rows.Err() != nil {
						t.Fatal(rows.Err())
					}
				} else if !errors.Is(rows.Err(), context.Canceled) {
					t.Fatal("expected database/sql private transaction cancellation", rows.Err())
				}
				count := 0
				for _, span := range rec.Ended() {
					if span.SpanKind() == trace.SpanKindInternal {
						count++
						if !containsAttribute(span.Attributes(), "firebird.rows.outcome", "transaction_close_unknown") || span.Status().Code == codes.Error {
							t.Fatal("invented driver closure cause", span.Attributes(), span.Status())
						}
					}
				}
				if count != 1 || raw.closes != 1 || raw.nextCalls != 0 {
					t.Fatal("incorrect cursor lifecycle", count, raw.closes, raw.nextCalls)
				}
			})
		}
	}
}
