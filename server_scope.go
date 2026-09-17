package firebirdotel

import (
	"crypto/sha256"
	"database/sql/driver"
	"io"

	"go.opentelemetry.io/otel/trace"
)

// The diagnostic marker has a fixed SQL shape and a random, non-sensitive comment.
// It never changes application SQL, bind values, or session/transaction variables.
const scopeSQL = "SELECT 1 FROM RDB$DATABASE /*firebirdotel_scope:"

type markerCompletion interface {
	MarkerComplete(string)
}

func (c *connState) serverScope(op *operation) {
	s := c.t.c.ServerTrace
	if s == nil {
		return
	}
	c.txMu.Lock()
	staleFallback := c.fallbackToken
	c.fallbackToken = ""
	if c.serverRows > 0 {
		// A suspended selectable procedure and another operation on one attachment
		// cannot be ordered reliably by text Trace. Invalidate, never guess.
		current := c.serverToken
		s.Discard(current)
		c.txMu.Unlock()
		if staleFallback != "" && staleFallback != current {
			s.Discard(staleFallback)
		}
		return
	}
	c.txMu.Unlock()
	if staleFallback != "" {
		s.Discard(staleFallback)
	}
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
	if completed, ok := s.(markerCompletion); ok {
		completed.MarkerComplete(token)
	}
	op.serverToken = token
	c.txMu.Lock()
	c.serverToken = token
	c.txMu.Unlock()
}

func (c *connState) preserveFallbackToken(op *operation, err error, query string) {
	if err != driver.ErrSkip || op.serverToken == "" {
		return
	}
	token := op.serverToken
	c.txMu.Lock()
	stale := c.fallbackToken
	c.fallbackToken = token
	c.fallbackQuery = sha256.Sum256([]byte(query))
	op.serverToken = ""
	c.txMu.Unlock()
	if stale != "" && stale != token {
		c.t.c.ServerTrace.Discard(stale)
	}
}

func (c *connState) takeFallbackToken(query string) string {
	if c.t.c.ServerTrace == nil {
		return ""
	}
	c.txMu.Lock()
	token := c.fallbackToken
	match := token != "" && c.fallbackQuery == sha256.Sum256([]byte(query))
	c.fallbackToken = ""
	c.fallbackQuery = [32]byte{}
	c.txMu.Unlock()
	if token != "" && !match {
		c.t.c.ServerTrace.Discard(token)
		return ""
	}
	return token
}

func (c *connState) discardFallbackToken() {
	if c.t.c.ServerTrace == nil {
		return
	}
	c.txMu.Lock()
	token := c.fallbackToken
	c.fallbackToken = ""
	c.fallbackQuery = [32]byte{}
	c.txMu.Unlock()
	if token != "" {
		c.t.c.ServerTrace.Discard(token)
	}
}

func (s *stmtState) serverScope(op *operation) {
	s.serverMu.Lock()
	token := s.serverToken
	s.serverToken = ""
	s.serverMu.Unlock()
	if token != "" {
		if op.enabled {
			op.serverToken = token
		} else {
			s.t.c.ServerTrace.Discard(token)
		}
		return
	}
	s.conn.serverScope(op)
}

func (s *stmtState) discardServerToken() {
	s.serverMu.Lock()
	token := s.serverToken
	s.serverToken = ""
	s.serverMu.Unlock()
	if token != "" && s.t.c.ServerTrace != nil {
		s.t.c.ServerTrace.Discard(token)
	}
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
