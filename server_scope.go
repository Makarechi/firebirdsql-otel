package firebirdotel

import (
	"database/sql/driver"
	"io"

	"go.opentelemetry.io/otel/trace"
)

// The diagnostic marker has a fixed SQL shape and a random, non-sensitive comment.
// It never changes application SQL, bind values, or session/transaction variables.
const scopeSQL = "SELECT 1 FROM RDB$DATABASE /*firebirdotel_scope:"

func (c *connState) serverScope(op *operation) {
	s := c.t.c.ServerTrace
	if s == nil {
		return
	}
	c.txMu.Lock()
	if c.serverRows > 0 {
		// A suspended selectable procedure and another operation on one attachment
		// cannot be ordered reliably by text Trace. Invalidate, never guess.
		s.Discard(c.serverToken)
		c.txMu.Unlock()
		return
	}
	c.txMu.Unlock()
	if !op.enabled {
		return
	}
	p, ok := c.raw.(driver.ConnPrepareContext)
	if !ok {
		return
	}
	token := s.Register(trace.SpanContextFromContext(op.ctx))
	if token == "" {
		return
	}
	stmt, err := p.PrepareContext(op.ctx, scopeSQL+token+"*/")
	if err != nil || stmt == nil {
		s.Discard(token)
		return
	}
	q, ok := stmt.(driver.StmtQueryContext)
	if !ok {
		_ = stmt.Close()
		s.Discard(token)
		return
	}
	r, err := q.QueryContext(op.ctx, nil)
	if err == nil && r != nil {
		values := make([]driver.Value, len(r.Columns()))
		err = r.Next(values)
		if err == nil {
			err = r.Next(values)
			if err == io.EOF {
				err = nil
			}
		}
		closeErr := r.Close()
		if err == nil {
			err = closeErr
		}
	} else if err == nil {
		_ = stmt.Close()
		s.Discard(token)
		return
	} else if r != nil {
		_ = r.Close()
	}
	closeErr := stmt.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		s.Discard(token)
		return
	}
	op.serverToken = token
	c.txMu.Lock()
	c.serverToken = token
	c.txMu.Unlock()
}

func (c *connState) queryResult(op operation, r driver.Rows, err error) (driver.Rows, error) {
	var release func()
	if err == nil && r != nil && c.t.c.ServerTrace != nil {
		c.txMu.Lock()
		c.serverRows++
		c.txMu.Unlock()
		release = func() { c.txMu.Lock(); c.serverRows--; c.txMu.Unlock() }
	}
	return c.t.queryResultWithClose(op, r, err, release, c.transactionContext())
}
