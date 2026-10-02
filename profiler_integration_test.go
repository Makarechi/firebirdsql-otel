package firebirdotel_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"os"
	"sync/atomic"
	"testing"

	firebirdotel "github.com/Makarechi/firebirdsql-otel"
	"github.com/Makarechi/firebirdsql-otel/profiler"
	"github.com/nakagami/firebirdsql"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type countingProfiler struct{ starts, finishes atomic.Int32 }
type countingSession struct{ p *countingProfiler }

func (p *countingProfiler) Start(context.Context, driver.Conn, trace.SpanContext, string) (profiler.SessionHandle, error) {
	p.starts.Add(1)
	return &countingSession{p}, nil
}
func (s *countingSession) Finish(trace.SpanContext) error { s.p.finishes.Add(1); return nil }
func (*countingSession) Cancel() error                    { return nil }

func TestProfilerUsesTraceSamplingWithoutSecondRatio(t *testing.T) {
	dsn := os.Getenv("FIREBIRD_TEST_DSN")
	if dsn == "" {
		t.Skip("requires isolated Firebird 5 fixture")
	}
	for _, sampled := range []bool{false, true} {
		t.Run(map[bool]string{false: "unsampled", true: "sampled"}[sampled], func(t *testing.T) {
			var sampler sdktrace.Sampler = sdktrace.NeverSample()
			if sampled {
				sampler = sdktrace.AlwaysSample()
			}
			tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sampler))
			defer tp.Shutdown(context.Background())
			counter := &countingProfiler{}
			cfg := firebirdotel.SafeConfig()
			cfg.TracerProvider = tp
			cfg.ServerProfiler = counter
			db, err := firebirdotel.OpenWithConfig(dsn, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx, root := tp.Tracer("integration").Start(t.Context(), "request")
			var n int
			if err := db.QueryRowContext(ctx, `SELECT FIRST 1 ID FROM OTEL_A WHERE ID = ?`, 1).Scan(&n); err != nil {
				t.Fatal(err)
			}
			root.End()
			want := int32(0)
			if sampled {
				want = 1
			}
			if counter.starts.Load() != want || counter.finishes.Load() != want {
				t.Fatalf("starts=%d finishes=%d want=%d", counter.starts.Load(), counter.finishes.Load(), want)
			}
		})
	}
}

func TestSampledProfilerAutoCleansAfterSQL(t *testing.T) {
	dsn := os.Getenv("FIREBIRD_TEST_DSN")
	if dsn == "" {
		t.Skip("requires isolated Firebird 5 fixture")
	}
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer tp.Shutdown(context.Background())
	runtime, err := profiler.New(dsn, "integration", profiler.WithTracerProvider(tp))
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	cfg := firebirdotel.SafeConfig()
	cfg.TracerProvider = tp
	cfg.ServerProfiler = runtime
	db, err := firebirdotel.OpenWithConfig(dsn, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, root := tp.Tracer("integration").Start(t.Context(), "sampled")
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: firebirdsql.LevelReadCommittedNoWait})
	if err != nil {
		t.Fatal(err)
	}
	var id int
	if err := tx.QueryRowContext(ctx, `SELECT FIRST 1 ID FROM OTEL_A WHERE ID = ? ORDER BY ID DESC`, 1).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	root.End()
	if id != 1 {
		t.Fatal(id)
	}
	profiles, events := 0, 0
	for _, span := range recorder.Ended() {
		if span.InstrumentationScope().Name != "github.com/Makarechi/firebirdsql-otel/profiler" {
			continue
		}
		profiles++
		events += len(span.Events())
	}
	if profiles != 1 || events == 0 {
		t.Fatalf("profiles=%d events=%d", profiles, events)
	}
	raw, err := sql.Open("firebirdsql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var leftover int
	if err := raw.QueryRowContext(ctx, `SELECT COUNT(*) FROM PLG$PROF_SESSIONS WHERE DESCRIPTION STARTING WITH 'firebirdotel/integration/'`).Scan(&leftover); err != nil {
		t.Fatal(err)
	}
	if leftover != 0 {
		t.Fatalf("profiles not deleted: %d", leftover)
	}
}
