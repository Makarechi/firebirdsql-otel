package firebirdotel

import (
	"context"
	"database/sql/driver"
	"testing"

	fbprofiler "github.com/Makarechi/firebirdsql-otel/profiler"
	"go.opentelemetry.io/otel/trace"
)

type switchProfiler struct{ starts, finishes int }
type switchProfileSession struct{ profiler *switchProfiler }

func (p *switchProfiler) Start(context.Context, driver.Conn, trace.SpanContext, string) (fbprofiler.SessionHandle, error) {
	p.starts++
	return &switchProfileSession{profiler: p}, nil
}
func (s *switchProfileSession) Finish(trace.SpanContext) error { s.profiler.finishes++; return nil }
func (*switchProfileSession) Cancel() error                    { return nil }

func TestProfilerRequiresExplicitEnableWhileClientTraceStaysOn(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "default_off", true: "enabled"}[enabled], func(t *testing.T) {
			profiler := &switchProfiler{}
			cfg, recorder, _ := setupTelemetry(t, Config{Profiler: ProfilerConfig{Enabled: enabled, Starter: profiler}})
			db, err := OpenDBWithConfig(scriptConnector{&scriptConn{}}, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx, parent := cfg.TracerProvider.Tracer("test").Start(t.Context(), "request")
			if _, err := db.ExecContext(ctx, "SELECT 1 FROM RDB$DATABASE"); err != nil {
				t.Fatal(err)
			}
			parent.End()
			want := 0
			if enabled {
				want = 1
			}
			if profiler.starts != want || profiler.finishes != want {
				t.Fatalf("profiles started=%d finished=%d want=%d", profiler.starts, profiler.finishes, want)
			}
			clientSpans := 0
			for _, span := range recorder.Ended() {
				if containsAttribute(span.Attributes(), "firebird.source", "client") {
					clientSpans++
				}
			}
			if clientSpans != 1 {
				t.Fatalf("client traces=%d, want 1", clientSpans)
			}
		})
	}
	if _, err := normalizeConfig(Config{Profiler: ProfilerConfig{Enabled: true}}); err == nil {
		t.Fatal("enabled profiler without a starter was accepted")
	}
}
