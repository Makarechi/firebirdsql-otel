package firebirdotel

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"
)

type onceDriver struct{ calls int }

func (d *onceDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("unexpected legacy open")
}
func (d *onceDriver) OpenConnector(string) (driver.Connector, error) {
	d.calls++
	if d.calls > 1 {
		return nil, errors.New("factory invoked twice")
	}
	return mockConnector{}, nil
}
func TestDriverConnectorFactoryRunsOnce(t *testing.T) {
	d := &onceDriver{}
	db, err := OpenWithDriverConfig(d, "unused", SafeConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if d.calls != 1 {
		t.Fatal("factory not called once", d.calls)
	}
	if _, err := db.ExecContext(t.Context(), "select 1"); err != nil {
		t.Fatal(err)
	}
	if d.calls != 1 {
		t.Fatal("factory called again")
	}
	// Guards the native-only registered driver lookup in OpenWithConfig.
	native, err := sql.Open(DriverName, "")
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	if _, ok := native.Driver().(driver.DriverContext); ok {
		t.Fatal("native driver now needs an explicit connector API")
	}
}

type lazyResult struct{ calls int }

func (r *lazyResult) LastInsertId() (int64, error) { return 0, nil }
func (r *lazyResult) RowsAffected() (int64, error) { r.calls++; return int64(r.calls), nil }

type lazyConn struct {
	mockConn
	result *lazyResult
}

func (c *lazyConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return c.result, nil
}
func TestExecutionDoesNotProbeLazyResult(t *testing.T) {
	for _, filtered := range []bool{false, true} {
		cfg, _, _ := setupTelemetry(t, SafeConfig())
		if filtered {
			cfg.Client.Filter = func(context.Context, Operation) bool { return false }
		}
		raw := &lazyResult{}
		db, err := OpenDBWithConfig(scriptConnector{&lazyConn{result: raw}}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		result, err := db.ExecContext(t.Context(), "update T set N=1")
		if err != nil {
			t.Fatal(err)
		}
		if raw.calls != 0 {
			t.Fatal("telemetry probed RowsAffected")
		}
		n, err := result.RowsAffected()
		if err != nil || n != 1 || raw.calls != 1 {
			t.Fatal("application observed altered result", n, err)
		}
		_ = db.Close()
	}
}

type cleanupConnector struct {
	mockConnector
	calls int
	err   error
}

func (c *cleanupConnector) Close() error { c.calls++; return c.err }
func TestConnectorCloseCapabilityAndError(t *testing.T) {
	original := errors.New("cleanup failure")
	raw := &cleanupConnector{err: original}
	wrapped, err := WrapConnector(raw, SafeConfig())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := wrapped.(io.Closer); !ok {
		t.Fatal("lost closer capability")
	}
	if _, err := WrapConnector(wrapped, SafeConfig()); err == nil {
		t.Fatal("accepted closing wrapper twice")
	}
	db := sql.OpenDB(wrapped)
	if err := db.Close(); err != original || raw.calls != 1 {
		t.Fatal("connector cleanup lost", err, raw.calls)
	}
	_ = db.Close()
	if raw.calls != 1 {
		t.Fatal("connector closed twice")
	}
	plain, err := WrapConnector(mockConnector{}, SafeConfig())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := plain.(io.Closer); ok {
		t.Fatal("invented closer capability")
	}
}
