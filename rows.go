package firebirdotel

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type rowsBase interface{ driver.Rows }
type rowsState struct {
	raw                 driver.Rows
	span                trace.Span
	start               time.Time
	mu                  sync.Mutex
	once, closeOnce     sync.Once
	delivered, attempts int64
	first               time.Duration
	eof                 bool
	closeErr            error
}

func (t *telemetry) queryResult(op operation, r driver.Rows, err error) (driver.Rows, error) {
	sc := t.finish(op, err, nil)
	if err != nil {
		return r, err
	}
	if r == nil {
		return nil, nil
	}
	if !t.c.Client.Rows || !op.enabled || !sc.IsValid() {
		return r, nil
	}
	// Never retain the operation context (which may contain a request body or application state).
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	_, span := t.tracer.Start(ctx, op.d.Summary+" consumption", trace.WithSpanKind(trace.SpanKindInternal), trace.WithAttributes(attribute.String("firebird.source", "client"), attribute.String("firebird.correlation", "exact"), attribute.String("firebird.duration.kind", "consumption_lifetime")))
	return wrapRows(&rowsState{raw: r, span: span, start: time.Now()}), nil
}
func (r *rowsState) Columns() []string { return r.raw.Columns() }
func (r *rowsState) Next(dest []driver.Value) error {
	err := r.raw.Next(dest)
	r.mu.Lock()
	r.attempts++
	if err == nil {
		r.delivered++
		if r.delivered == 1 {
			r.first = time.Since(r.start)
		}
	}
	r.mu.Unlock()
	if errors.Is(err, io.EOF) {
		if n, ok := r.raw.(driver.RowsNextResultSet); !ok || !n.HasNextResultSet() {
			r.finish("eof", nil)
		}
	} else if err != nil {
		r.finish(outcome(err), err)
	}
	return err
}
func (r *rowsState) Close() error {
	r.closeOnce.Do(func() {
		r.closeErr = r.raw.Close()
		kind := "early_close"
		if r.closeErr != nil {
			kind = outcome(r.closeErr)
		}
		r.finish(kind, r.closeErr)
	})
	return r.closeErr
}
func (r *rowsState) finish(reason string, err error) {
	r.once.Do(func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.eof = reason == "eof"
		r.span.SetAttributes(attribute.Int64("firebird.rows.delivered", r.delivered), attribute.Int64("firebird.rows.next_attempts", r.attempts), attribute.Bool("firebird.rows.eof", r.eof), attribute.String("firebird.rows.outcome", reason), attribute.Float64("firebird.rows.consumption_seconds", time.Since(r.start).Seconds()))
		if r.delivered > 0 {
			r.span.SetAttributes(attribute.Float64("firebird.rows.first_delivery_seconds", r.first.Seconds()))
		}
		if err != nil {
			r.span.SetAttributes(ErrorAttributes(err)...)
			r.span.SetStatus(codes.Error, outcome(err))
		}
		r.span.End()
	})
}
func (r *rowsState) HasNextResultSet() bool {
	return r.raw.(driver.RowsNextResultSet).HasNextResultSet()
}
func (r *rowsState) NextResultSet() error {
	err := r.raw.(driver.RowsNextResultSet).NextResultSet()
	if errors.Is(err, io.EOF) {
		r.finish("eof", nil)
	} else if err != nil {
		r.finish(outcome(err), err)
	}
	return err
}
func (r *rowsState) ColumnTypeDatabaseTypeName(i int) string {
	return r.raw.(driver.RowsColumnTypeDatabaseTypeName).ColumnTypeDatabaseTypeName(i)
}
func (r *rowsState) ColumnTypeLength(i int) (int64, bool) {
	return r.raw.(driver.RowsColumnTypeLength).ColumnTypeLength(i)
}
func (r *rowsState) ColumnTypeNullable(i int) (bool, bool) {
	return r.raw.(driver.RowsColumnTypeNullable).ColumnTypeNullable(i)
}
func (r *rowsState) ColumnTypePrecisionScale(i int) (int64, int64, bool) {
	return r.raw.(driver.RowsColumnTypePrecisionScale).ColumnTypePrecisionScale(i)
}
func (r *rowsState) ColumnTypeScanType(i int) reflect.Type {
	return r.raw.(driver.RowsColumnTypeScanType).ColumnTypeScanType(i)
}
