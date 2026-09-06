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
	inTransaction       bool
	closeErr, nextErr   error
	cancellations       [2]rowCancellation
}

type rowCancellation struct {
	done <-chan struct{}
	err  <-chan error
	stop func() bool
}

func (t *telemetry) queryResult(op operation, r driver.Rows, err error, txContexts ...context.Context) (driver.Rows, error) {
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
	// Parentage retains only SpanContext. The stoppable cancellation subscription below
	// lives only until Close and transfers the canonical cancellation error to minimal row state.
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	_, span := t.tracer.Start(ctx, op.d.Summary+" consumption", trace.WithSpanKind(trace.SpanKindInternal), trace.WithAttributes(attribute.String("firebird.source", "client"), attribute.String("firebird.correlation", "exact"), attribute.String("firebird.duration.kind", "consumption_lifetime")))
	if !span.IsRecording() {
		span.End()
		return r, nil
	}
	state := &rowsState{raw: r, span: span, start: time.Now()}
	sources := [2]context.Context{op.ctx, nil}
	if len(txContexts) > 0 {
		sources[1] = txContexts[0]
		state.inTransaction = txContexts[0] != nil
	}
	for i, source := range sources {
		if source == nil || source.Done() == nil {
			continue
		}
		signal := make(chan error, 1)
		state.cancellations[i] = rowCancellation{source.Done(), signal, context.AfterFunc(source, func() { signal <- source.Err() })}
	}

	return wrapRows(state), nil
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
		if _, ok := r.raw.(driver.RowsNextResultSet); !ok {
			r.mu.Lock()
			r.eof = true
			r.mu.Unlock()
		}
	} else if err != nil {
		r.mu.Lock()
		r.nextErr = err
		r.mu.Unlock()
	}
	return err
}
func (r *rowsState) Close() error {
	r.closeOnce.Do(func() {
		r.closeErr = r.raw.Close()
		// database/sql may supply a deferred error only from Close after Next returned EOF.
		// Keep the original Close result for the caller; choose the observed telemetry outcome separately.
		r.mu.Lock()
		observed, eof := r.nextErr, r.eof
		r.mu.Unlock()
		for i, cancellation := range r.cancellations {
			if !eof && cancellation.done != nil {
				select {
				case <-cancellation.done:
					// Wait for the canonical error if database/sql closed before AfterFunc ran.
					if cancelled := <-cancellation.err; observed == nil {
						observed = cancelled
					}
				default:
				}
			}
			if cancellation.stop != nil {
				cancellation.stop()
			}
			r.cancellations[i] = rowCancellation{}
		}

		if observed == nil {
			observed = r.closeErr
		}
		kind := "early_close"
		// database/sql closes rows before invoking driver Commit/Rollback and does
		// not expose its private transaction cancellation context to the driver.
		// An explicit Close and that automatic Close are indistinguishable here.
		if r.inTransaction {
			kind = "transaction_close_unknown"
		}
		if eof {
			kind = "eof"
		}
		if observed != nil {
			kind = outcome(observed)
		}
		r.finish(kind, observed)
	})
	return r.closeErr
}
func (r *rowsState) finish(reason string, err error) {
	r.once.Do(func() {
		r.mu.Lock()
		defer r.mu.Unlock()
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
	next := r.raw.(driver.RowsNextResultSet).HasNextResultSet()
	if !next {
		r.mu.Lock()
		r.eof = true
		r.mu.Unlock()
	}
	return next
}
func (r *rowsState) NextResultSet() error {
	err := r.raw.(driver.RowsNextResultSet).NextResultSet()
	if errors.Is(err, io.EOF) {
		r.mu.Lock()
		r.eof = true
		r.mu.Unlock()
	} else if err != nil {
		r.mu.Lock()
		r.nextErr = err
		r.mu.Unlock()
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
