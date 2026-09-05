package firebirdotel

import (
	"context"
	"database/sql/driver"
	"errors"
	"time"

	"github.com/Makarechi/firebirdsql-otel/internal/sqltext"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type description = sqltext.Description

const instrumentationName = "github.com/Makarechi/firebirdsql-otel"

type telemetry struct {
	c                      Config
	tracer                 trace.Tracer
	duration               metric.Float64Histogram
	spanAttrs, metricAttrs []attribute.KeyValue
}
type operation struct {
	ctx     context.Context
	start   time.Time
	method  string
	d       description
	enabled bool
	t       *telemetry
}

func newTelemetry(c Config) (*telemetry, error) {
	tp := c.TracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	mp := c.MeterProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	h, err := mp.Meter(instrumentationName).Float64Histogram("db.client.operation.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, ErrInstrumentation
	}
	return &telemetry{c: c, tracer: tp.Tracer(instrumentationName), duration: h, spanAttrs: append(connectionAttributes("", c.Connection), c.SpanAttributes...), metricAttrs: append([]attribute.KeyValue{attribute.String("db.system.name", "firebirdsql")}, c.MetricAttributes...)}, nil
}
func (t *telemetry) describe(q string) description {
	d := sqltext.Analyze(q, t.c.SQL.MaxInputBytes, t.c.SQL.MaxOutputBytes)
	switch t.c.SQL.Mode {
	case SQLOff:
		d.Text = ""
	case SQLRawExplicit:
		if len(q) <= t.c.SQL.MaxOutputBytes {
			d.Text = q
		} else {
			d.Text = ""
		}
	}
	return d
}
func (t *telemetry) start(ctx context.Context, method string, d description) operation {
	if ctx == nil {
		ctx = context.Background()
	}
	if h, ok := ctx.Value(hintKey{}).(OperationHint); ok && d.Operation == "SELECT" {
		d.Procedure = h.Procedure
		d.Summary = "SELECT " + h.Procedure
	}
	if d.Operation == "" {
		d.Operation = method
		d.Summary = method
	}
	enabled := true
	switch method {
	case "connect":
		enabled = t.c.Client.Connect
	case "prepare":
		enabled = t.c.Client.Prepare
	case "reset":
		enabled = t.c.Client.Reset
	case "ping":
		enabled = t.c.Client.Ping
	}
	if enabled && t.c.Client.Filter != nil {
		enabled = t.c.Client.Filter(ctx, Operation{Method: method, Name: d.Operation, Summary: d.Summary, Procedure: d.Procedure})
	}
	return operation{ctx: ctx, start: time.Now(), method: method, d: d, enabled: enabled, t: t}
}
func (t *telemetry) finish(op operation, err error, extra []attribute.KeyValue) trace.SpanContext {
	if errors.Is(err, driver.ErrSkip) {
		return trace.SpanContext{}
	}
	end := time.Now()
	kind := outcome(err)
	ma := append([]attribute.KeyValue(nil), t.metricAttrs...)
	ma = append(ma, attribute.String("db.operation.name", op.d.Operation), attribute.String("firebird.client.method", op.method))
	if kind != "ok" {
		ma = append(ma, attribute.String("error.type", kind))
	}
	t.duration.Record(op.ctx, end.Sub(op.start).Seconds(), metric.WithAttributes(ma...))
	if !op.enabled {
		return trace.SpanContext{}
	}
	a := append([]attribute.KeyValue(nil), t.spanAttrs...)
	a = append(a, attribute.String("db.operation.name", op.d.Operation), attribute.String("db.query.summary", op.d.Summary), attribute.String("firebird.client.method", op.method), attribute.String("firebird.source", "client"), attribute.String("firebird.correlation", "exact"))
	if op.d.Text != "" {
		a = append(a, attribute.String("db.query.text", op.d.Text))
	}
	if op.d.Fingerprint != "" {
		a = append(a, attribute.String("firebird.query.fingerprint", op.d.Fingerprint))
	}
	if op.d.Procedure != "" {
		a = append(a, attribute.String("db.stored_procedure.name", op.d.Procedure))
	}
	if op.d.Collection != "" {
		a = append(a, attribute.String("db.collection.name", op.d.Collection))
	}
	a = append(a, extra...)
	a = append(a, ErrorAttributes(err)...)
	_, span := t.tracer.Start(op.ctx, op.d.Summary, trace.WithSpanKind(trace.SpanKindClient), trace.WithTimestamp(op.start), trace.WithAttributes(a...))
	if kind != "ok" {
		span.SetStatus(codes.Error, kind)
	}
	sc := span.SpanContext()
	span.End(trace.WithTimestamp(end))
	return sc
}
func resultAttributes(r driver.Result) []attribute.KeyValue {
	if r == nil {
		return nil
	}
	// This concrete database/sql type carries its count directly. Never probe an
	// arbitrary Result: its RowsAffected method may be lazy or stateful.
	n, ok := r.(driver.RowsAffected)
	if !ok || n < 0 {
		return nil
	}
	return []attribute.KeyValue{attribute.Int64("firebird.rows.affected", int64(n))}
}
