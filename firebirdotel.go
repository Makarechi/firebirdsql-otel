// Package firebirdotel provides OpenTelemetry instrumentation helpers for the
// github.com/nakagami/firebirdsql database/sql driver.
package firebirdotel

import (
	"database/sql"
	"database/sql/driver"

	"github.com/XSAM/otelsql"
	_ "github.com/nakagami/firebirdsql"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	// DriverName is the standard driver name registered by github.com/nakagami/firebirdsql.
	DriverName = "firebirdsql"

	// CreateDBDriverName is the create-database driver name registered by github.com/nakagami/firebirdsql.
	CreateDBDriverName = "firebirdsql_createdb"
)

type (
	// Option configures OpenTelemetry behavior.
	Option = otelsql.Option

	// SpanOptions controls which database/sql operations create spans.
	SpanOptions = otelsql.SpanOptions

	// Method is the database/sql operation name passed to span name formatters.
	Method = otelsql.Method

	// SpanNameFormatter formats span names.
	SpanNameFormatter = otelsql.SpanNameFormatter

	// AttributesGetter returns span attributes for a database/sql operation.
	AttributesGetter = otelsql.AttributesGetter

	// InstrumentAttributesGetter returns metric attributes for a successful operation.
	InstrumentAttributesGetter = otelsql.InstrumentAttributesGetter

	// InstrumentErrorAttributesGetter returns metric attributes for a failed operation.
	InstrumentErrorAttributesGetter = otelsql.InstrumentErrorAttributesGetter
)

// Open opens an instrumented Firebird database handle.
func Open(dsn string, options ...Option) (*sql.DB, error) {
	return OpenWithDriver(DriverName, dsn, options...)
}

// OpenCreateDB opens an instrumented Firebird create-database handle.
func OpenCreateDB(dsn string, options ...Option) (*sql.DB, error) {
	return OpenWithDriver(CreateDBDriverName, dsn, options...)
}

// OpenWithDriver opens an instrumented database handle using a registered driver.
//
// This is useful for tests and for applications that register a custom Firebird
// driver name.
func OpenWithDriver(driverName, dsn string, options ...Option) (*sql.DB, error) {
	return otelsql.Open(driverName, dsn, withDefaultOptions(dsn, options)...)
}

// OpenDB wraps a driver connector in OpenTelemetry instrumentation.
func OpenDB(connector driver.Connector, options ...Option) *sql.DB {
	return otelsql.OpenDB(connector, withDefaultOptions("", options)...)
}

// OpenDBWithDSN wraps a driver connector and also derives network attributes from dsn.
func OpenDBWithDSN(connector driver.Connector, dsn string, options ...Option) *sql.DB {
	return otelsql.OpenDB(connector, withDefaultOptions(dsn, options)...)
}

// Register registers an instrumented wrapper around the Firebird driver and
// returns the generated driver name.
//
// Prefer Open when each sql.DB handle has a known DSN. Register is useful for
// frameworks that need a driver name before calling sql.Open themselves.
func Register(options ...Option) (string, error) {
	return RegisterWithDriver(DriverName, "", options...)
}

// RegisterCreateDB registers an instrumented wrapper around the create-database driver.
func RegisterCreateDB(options ...Option) (string, error) {
	return RegisterWithDriver(CreateDBDriverName, "", options...)
}

// RegisterWithDriver registers an instrumented wrapper around driverName.
func RegisterWithDriver(driverName, dsn string, options ...Option) (string, error) {
	return otelsql.Register(driverName, withDefaultOptions(dsn, options)...)
}

// RegisterDBStatsMetrics registers database/sql pool statistics metrics for db.
func RegisterDBStatsMetrics(db *sql.DB, dsn string, options ...Option) (metric.Registration, error) {
	return otelsql.RegisterDBStatsMetrics(db, withDefaultOptions(dsn, options)...)
}

// DefaultAttributes returns Firebird database semantic attributes plus best-effort
// network attributes parsed from dsn.
func DefaultAttributes(dsn string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{semconv.DBSystemNameFirebirdsql}
	if dsn == "" {
		return attrs
	}
	return append(attrs, otelsql.AttributesFromDSN(dsn)...)
}

// WithDefaultAttributes adds Firebird database semantic attributes and best-effort
// network attributes parsed from dsn.
func WithDefaultAttributes(dsn string) Option {
	return otelsql.WithAttributes(DefaultAttributes(dsn)...)
}

// AttributesFromDSN returns best-effort network attributes parsed from dsn.
func AttributesFromDSN(dsn string) []attribute.KeyValue {
	return otelsql.AttributesFromDSN(dsn)
}

// WithAttributes adds static attributes to spans and metrics.
func WithAttributes(attributes ...attribute.KeyValue) Option {
	return otelsql.WithAttributes(attributes...)
}

// WithAttributesGetter configures per-operation span attributes.
func WithAttributesGetter(getter AttributesGetter) Option {
	return otelsql.WithAttributesGetter(getter)
}

// WithTracerProvider configures the tracer provider used by spans.
func WithTracerProvider(provider trace.TracerProvider) Option {
	return otelsql.WithTracerProvider(provider)
}

// WithMeterProvider configures the meter provider used by metrics.
func WithMeterProvider(provider metric.MeterProvider) Option {
	return otelsql.WithMeterProvider(provider)
}

// WithSpanNameFormatter configures span names.
func WithSpanNameFormatter(formatter SpanNameFormatter) Option {
	return otelsql.WithSpanNameFormatter(formatter)
}

// WithSpanOptions configures which database/sql operations create spans.
func WithSpanOptions(options SpanOptions) Option {
	return otelsql.WithSpanOptions(options)
}

// WithSQLCommenter enables SQLCommenter trace context injection.
func WithSQLCommenter(enabled bool) Option {
	return otelsql.WithSQLCommenter(enabled)
}

// WithInstrumentAttributesGetter configures per-operation metric attributes.
func WithInstrumentAttributesGetter(getter InstrumentAttributesGetter) Option {
	return otelsql.WithInstrumentAttributesGetter(getter)
}

// WithInstrumentErrorAttributesGetter configures per-operation metric error attributes.
func WithInstrumentErrorAttributesGetter(getter InstrumentErrorAttributesGetter) Option {
	return otelsql.WithInstrumentErrorAttributesGetter(getter)
}

func withDefaultOptions(dsn string, options []Option) []Option {
	out := make([]Option, 0, len(options)+1)
	out = append(out, WithDefaultAttributes(dsn))
	out = append(out, options...)
	return out
}
