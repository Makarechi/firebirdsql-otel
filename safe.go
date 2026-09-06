package firebirdotel

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"io"
	"reflect"
)

// OpenWithConfig opts into safe instrumentation. The zero Config is safe_client.
// SQL descriptions use client dialect 3, as does the pinned Firebird driver.
func OpenWithConfig(dsn string, c Config) (*sql.DB, error) {
	normalized, err := normalizeConfig(c)
	if err != nil {
		return nil, err
	}
	if normalized.Profile == Compatibility {
		return Open(dsn, compatibilityOptions(normalized)...)
	}
	// The pinned native driver has only Driver.Open, so this lookup constructs no connector.
	raw, err := sql.Open(DriverName, dsn)
	if err != nil {
		return nil, err
	}
	native := raw.Driver()
	_ = raw.Close()
	return OpenWithDriverConfig(native, dsn, normalized)
}

// OpenWithDriverConfig accepts an explicit driver using client dialect 3.
// DriverContext factories run exactly once; named compatibility APIs are unchanged.
func OpenWithDriverConfig(d driver.Driver, dsn string, c Config) (*sql.DB, error) {
	c, err := normalizeConfig(c)
	if err != nil {
		return nil, err
	}
	if d == nil {
		return nil, errors.New("firebirdotel: nil driver")
	}
	var connector driver.Connector
	if dc, ok := d.(driver.DriverContext); ok {
		connector, err = dc.OpenConnector(dsn)
	} else {
		connector = &dsnConnector{d, dsn}
	}
	if err != nil {
		return nil, err
	}
	if c.Profile == Compatibility {
		return OpenDBWithDSN(connector, dsn, compatibilityOptions(c)...), nil
	}
	c.Connection = resolveConnection(dsn, c.Connection)
	db, err := OpenDBWithConfig(connector, c)
	if err != nil {
		// This constructor owns the connector until a database accepts ownership.
		if closer, ok := connector.(io.Closer); ok {
			if closeErr := closer.Close(); closeErr != nil {
				err = errors.Join(err, closeErr)
			}
		}
	}
	return db, err
}

// OpenDBWithConfig takes an uninstrumented, client-dialect-3 connector.
// Call Raw to access native connections.
func OpenDBWithConfig(connector driver.Connector, c Config) (*sql.DB, error) {
	c, err := normalizeConfig(c)
	if err != nil {
		return nil, err
	}
	if connector == nil {
		return nil, errors.New("firebirdotel: nil connector")
	}
	if c.Profile == Compatibility {
		return OpenDB(connector, compatibilityOptions(c)...), nil
	}
	wrapped, err := WrapConnector(connector, c)
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(wrapped), nil
}
func compatibilityOptions(c Config) []Option {
	out := append([]Option(nil), c.OTelOptions...)
	if c.TracerProvider != nil {
		out = append(out, WithTracerProvider(c.TracerProvider))
	}
	if c.MeterProvider != nil {
		out = append(out, WithMeterProvider(c.MeterProvider))
	}
	return out
}

// WrapConnector requires client dialect 3, preserves optional driver interfaces
// and does not create global registrations.
func WrapConnector(connector driver.Connector, c Config) (driver.Connector, error) {
	c, err := normalizeConfig(c)
	if err != nil {
		return nil, err
	}
	if c.Profile == Compatibility {
		return nil, errors.New("firebirdotel: use OpenDBWithConfig for compatibility")
	}
	if connector == nil {
		return nil, errors.New("firebirdotel: nil connector")
	}
	if _, ok := connector.(interface{ firebirdInstrumentedConnector() }); ok {
		return nil, errors.New("firebirdotel: connector is already instrumented")
	}
	// otelsql has no public unwrapping contract. Reject its known wrappers rather than double wrapping them.
	t := reflect.TypeOf(connector.Driver())
	if t != nil {
		if t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		if t.PkgPath() == "github.com/XSAM/otelsql" {
			return nil, errors.New("firebirdotel: expected an uninstrumented driver")
		}
	}
	tel, err := newTelemetry(c)
	if err != nil {
		return nil, err
	}
	wrapped := &safeConnector{connector, tel}
	if closer, ok := connector.(io.Closer); ok {
		return &closingSafeConnector{safeConnector: wrapped, Closer: closer}, nil
	}
	return wrapped, nil
}

// Raw calls fn with the underlying connection only while database/sql holds it.
// The connection must not be retained after fn returns; the same contract as sql.Conn.Raw applies.
func Raw(conn *sql.Conn, fn func(any) error) error {
	return conn.Raw(func(v any) error {
		if c, ok := v.(interface{ Unwrap() driver.Conn }); ok {
			return fn(c.Unwrap())
		}
		return fn(v)
	})
}

type dsnConnector struct {
	d   driver.Driver
	dsn string
}

func (c *dsnConnector) Driver() driver.Driver { return c.d }
func (c *dsnConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.d.Open(c.dsn)
}

type safeConnector struct {
	driver.Connector
	t *telemetry
}

func (c *safeConnector) Connect(ctx context.Context) (driver.Conn, error) {
	op := c.t.start(ctx, "connect", description{})
	conn, err := c.Connector.Connect(ctx)
	c.t.finish(op, err, nil)
	if err != nil {
		return nil, err
	}
	return wrapConn(&connState{raw: conn, t: c.t}), nil
}

// RegisterDBStatsMetricsWithConfig registers pool statistics using metric dimensions only.
// Keep the registration and call Unregister before closing the providers.
func RegisterDBStatsMetricsWithConfig(db *sql.DB, c Config) (metric.Registration, error) {
	c, err := normalizeConfig(c)
	if err != nil {
		return nil, err
	}
	if c.Profile == Compatibility {
		return RegisterDBStatsMetrics(db, "", compatibilityOptions(c)...)
	}
	attrs := append([]attribute.KeyValue{attribute.String("db.system.name", "firebirdsql")}, c.MetricAttributes...)
	opts := []Option{WithAttributes(attrs...)}
	if c.MeterProvider != nil {
		opts = append(opts, WithMeterProvider(c.MeterProvider))
	}
	return RegisterDBStatsMetrics(db, "", opts...)
}

func (*safeConnector) firebirdInstrumentedConnector() {}

type closingSafeConnector struct {
	*safeConnector
	io.Closer
}
