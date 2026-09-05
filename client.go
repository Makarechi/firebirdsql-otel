package firebirdotel

import (
	"context"
	"database/sql/driver"
	"sync"
)

//go:generate python3 internal/generate/capabilities.py

type connBase interface {
	driver.Conn
	Unwrap() driver.Conn
}
type connState struct {
	raw   driver.Conn
	t     *telemetry
	txMu  sync.Mutex
	txCtx context.Context
}

func (c *connState) Unwrap() driver.Conn { return c.raw }
func (c *connState) Close() error        { return c.raw.Close() }
func (c *connState) Prepare(q string) (driver.Stmt, error) {
	return c.prepare(context.Background(), q, false)
}
func (c *connState) PrepareContext(ctx context.Context, q string) (driver.Stmt, error) {
	return c.prepare(ctx, q, true)
}
func (c *connState) prepare(ctx context.Context, q string, withCtx bool) (driver.Stmt, error) {
	d := c.t.describe(q)
	op := c.t.start(ctx, "prepare", d)
	var s driver.Stmt
	var err error
	if withCtx {
		s, err = c.raw.(driver.ConnPrepareContext).PrepareContext(ctx, q)
	} else {
		s, err = c.raw.Prepare(q)
	}
	c.t.finish(op, err, nil)
	if err != nil {
		return nil, err
	}
	return wrapStmt(&stmtState{raw: s, t: c.t, d: d, conn: c}), nil
}
func (c *connState) Begin() (driver.Tx, error) {
	return c.begin(context.Background(), driver.TxOptions{}, false)
}
func (c *connState) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.begin(ctx, opts, true)
}
func (c *connState) begin(ctx context.Context, opts driver.TxOptions, withCtx bool) (driver.Tx, error) {
	op := c.t.start(ctx, "begin", description{})
	var tx driver.Tx
	var err error
	if withCtx {
		tx, err = c.raw.(driver.ConnBeginTx).BeginTx(ctx, opts)
	} else {
		tx, err = c.raw.Begin()
	}
	c.t.finish(op, err, nil)
	if err != nil {
		return nil, err
	}
	c.txMu.Lock()
	c.txCtx = ctx
	c.txMu.Unlock()
	return &txState{raw: tx, t: c.t, ctx: context.WithoutCancel(ctx), conn: c}, nil
}
func (c *connState) Exec(q string, args []driver.Value) (driver.Result, error) {
	op := c.t.start(context.Background(), "exec", c.t.describe(q))
	r, err := c.raw.(driver.Execer).Exec(q, args)
	c.t.finish(op, err, resultAttributes(r))
	return r, err
}
func (c *connState) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	op := c.t.start(ctx, "exec", c.t.describe(q))
	r, err := c.raw.(driver.ExecerContext).ExecContext(ctx, q, args)
	c.t.finish(op, err, resultAttributes(r))
	return r, err
}
func (c *connState) Query(q string, args []driver.Value) (driver.Rows, error) {
	op := c.t.start(context.Background(), "query", c.t.describe(q))
	r, err := c.raw.(driver.Queryer).Query(q, args)
	return c.t.queryResult(op, r, err, c.transactionContext())
}
func (c *connState) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	op := c.t.start(ctx, "query", c.t.describe(q))
	r, err := c.raw.(driver.QueryerContext).QueryContext(ctx, q, args)
	return c.t.queryResult(op, r, err, c.transactionContext())
}
func (c *connState) Ping(ctx context.Context) error {
	op := c.t.start(ctx, "ping", description{})
	err := c.raw.(driver.Pinger).Ping(ctx)
	c.t.finish(op, err, nil)
	return err
}
func (c *connState) ResetSession(ctx context.Context) error {
	op := c.t.start(ctx, "reset", description{})
	err := c.raw.(driver.SessionResetter).ResetSession(ctx)
	c.t.finish(op, err, nil)
	return err
}
func (c *connState) IsValid() bool { return c.raw.(driver.Validator).IsValid() }
func (c *connState) CheckNamedValue(v *driver.NamedValue) error {
	return c.raw.(driver.NamedValueChecker).CheckNamedValue(v)
}

type stmtBase interface{ driver.Stmt }
type stmtState struct {
	raw  driver.Stmt
	t    *telemetry
	d    description
	conn *connState
}

func (s *stmtState) Close() error  { return s.raw.Close() }
func (s *stmtState) NumInput() int { return s.raw.NumInput() }
func (s *stmtState) Exec(args []driver.Value) (driver.Result, error) {
	op := s.t.start(context.Background(), "exec", s.d)
	r, err := s.raw.Exec(args)
	s.t.finish(op, err, resultAttributes(r))
	return r, err
}
func (s *stmtState) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	op := s.t.start(ctx, "exec", s.d)
	r, err := s.raw.(driver.StmtExecContext).ExecContext(ctx, args)
	s.t.finish(op, err, resultAttributes(r))
	return r, err
}
func (s *stmtState) Query(args []driver.Value) (driver.Rows, error) {
	op := s.t.start(context.Background(), "query", s.d)
	r, err := s.raw.Query(args)
	return s.t.queryResult(op, r, err, s.conn.transactionContext())
}
func (s *stmtState) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	op := s.t.start(ctx, "query", s.d)
	r, err := s.raw.(driver.StmtQueryContext).QueryContext(ctx, args)
	return s.t.queryResult(op, r, err, s.conn.transactionContext())
}
func (s *stmtState) ColumnConverter(i int) driver.ValueConverter {
	return s.raw.(driver.ColumnConverter).ColumnConverter(i)
}
func (s *stmtState) CheckNamedValue(v *driver.NamedValue) error {
	return s.raw.(driver.NamedValueChecker).CheckNamedValue(v)
}

type txState struct {
	raw  driver.Tx
	t    *telemetry
	ctx  context.Context
	conn *connState
}

func (tx *txState) Commit() error {
	defer tx.clearContext()
	op := tx.t.start(tx.ctx, "commit", description{})
	err := tx.raw.Commit()
	tx.t.finish(op, err, nil)
	return err
}
func (tx *txState) Rollback() error {
	defer tx.clearContext()
	op := tx.t.start(tx.ctx, "rollback", description{})
	err := tx.raw.Rollback()
	tx.t.finish(op, err, nil)
	return err
}

func (c *connState) transactionContext() context.Context {
	if c == nil {
		return nil
	}
	c.txMu.Lock()
	defer c.txMu.Unlock()
	return c.txCtx
}
func (tx *txState) clearContext() {
	if tx.conn != nil {
		tx.conn.txMu.Lock()
		tx.conn.txCtx = nil
		tx.conn.txMu.Unlock()
	}
	tx.ctx = nil
}
