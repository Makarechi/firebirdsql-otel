package firebirdotel

import (
	"context"
	"errors"

	"github.com/Makarechi/firebirdsql-otel/internal/sqltext"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type Profile string

const (
	Compatibility Profile = "compatibility"
	SafeClient    Profile = "safe_client"
	Diagnostic    Profile = "diagnostic"
)

type SQLMode string

const (
	SQLOff         SQLMode = "off"
	SQLSanitized   SQLMode = "sanitized"
	SQLRawExplicit SQLMode = "raw_explicit"
)

type SQLPolicy struct {
	Mode                          SQLMode
	MaxInputBytes, MaxOutputBytes int
}
type ConnectionAttributes struct {
	// Namespace is an explicitly chosen logical alias, never inferred from a file path.
	Namespace string
	Host      string
	Port      int
	// ParseDSNNetwork enables conservative host/port extraction. No username/path is exported.
	ParseDSNNetwork bool
}
type ClientDiagnosticsConfig struct {
	Rows                          bool
	Connect, Prepare, Reset, Ping bool
	// Filter receives a bounded, sanitized description, before execution. It cannot select by duration.
	Filter func(context.Context, Operation) bool
}
type Operation struct{ Method, Name, Summary, Procedure string }
type Config struct {
	Profile        Profile
	SQL            SQLPolicy
	Connection     ConnectionAttributes
	Client         ClientDiagnosticsConfig
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
	// SpanAttributes are never copied to metric dimensions. Caller-supplied attributes are trusted.
	SpanAttributes []attribute.KeyValue
	// MetricAttributes must be a bounded, low-cardinality vocabulary chosen by the application.
	MetricAttributes []attribute.KeyValue
	// OTelOptions are accepted only by Compatibility. Safe modes reject opaque callbacks/options.
	OTelOptions []Option
}

func SafeConfig() Config { return Config{Profile: SafeClient, SQL: SQLPolicy{Mode: SQLSanitized}} }
func DiagnosticConfig() Config {
	c := SafeConfig()
	c.Profile = Diagnostic
	c.Client = ClientDiagnosticsConfig{Rows: true, Connect: true, Prepare: true, Reset: true, Ping: true}
	return c
}
func normalizeConfig(c Config) (Config, error) {
	if c.Profile == "" {
		c.Profile = SafeClient
	}
	if c.Profile != Compatibility && c.Profile != SafeClient && c.Profile != Diagnostic {
		return c, errors.New("firebirdotel: invalid profile")
	}
	if c.Profile != Compatibility && len(c.OTelOptions) > 0 {
		return c, errors.New("firebirdotel: OTelOptions are only supported in compatibility mode; use explicit Config fields")
	}
	if c.SQL.Mode == "" {
		c.SQL.Mode = SQLSanitized
	}
	if c.SQL.Mode != SQLOff && c.SQL.Mode != SQLSanitized && c.SQL.Mode != SQLRawExplicit {
		return c, errors.New("firebirdotel: invalid SQL mode")
	}
	if c.SQL.MaxInputBytes == 0 {
		c.SQL.MaxInputBytes = sqltext.MaxInput
	}
	if c.SQL.MaxOutputBytes == 0 {
		c.SQL.MaxOutputBytes = sqltext.MaxOutput
	}
	if c.SQL.MaxInputBytes < 1 || c.SQL.MaxInputBytes > sqltext.MaxInput || c.SQL.MaxOutputBytes < 1 || c.SQL.MaxOutputBytes > sqltext.MaxOutput {
		return c, errors.New("firebirdotel: SQL limits exceed hard bounds")
	}
	if c.Connection.Port < 0 || c.Connection.Port > 65535 {
		return c, errors.New("firebirdotel: invalid port")
	}
	if len(c.Connection.Namespace) > 256 || len(c.Connection.Host) > 253 || len(c.SpanAttributes) > 32 || len(c.MetricAttributes) > 16 {
		return c, errors.New("firebirdotel: attribute limits exceeded")
	}
	c.SpanAttributes = append([]attribute.KeyValue(nil), c.SpanAttributes...)
	c.MetricAttributes = append([]attribute.KeyValue(nil), c.MetricAttributes...)
	return c, nil
}

type OperationHint struct{ Procedure string }
type hintKey struct{}

// WithOperationHint adds a trusted object name, never SQL or argument values.
// Invalid identifiers are ignored. The executed SQL is unchanged.
func WithOperationHint(ctx context.Context, h OperationHint) context.Context {
	if !sqltext.Identifier(h.Procedure) {
		return ctx
	}
	return context.WithValue(ctx, hintKey{}, h)
}
