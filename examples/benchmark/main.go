// Run only against testdata/firebird5. Measures a local driver workload, not production SLAs.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	firebirdotel "github.com/Makarechi/firebirdsql-otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"io"
	"net"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type exporter struct{ bytes, spans atomic.Int64 }

func (e *exporter) ExportSpans(_ context.Context, ss []sdktrace.ReadOnlySpan) error {
	for _, s := range ss {
		b, err := json.Marshal(struct {
			Name       string
			Attributes []attribute.KeyValue
			Events     []sdktrace.Event
			Status     sdktrace.Status
		}{s.Name(), s.Attributes(), s.Events(), s.Status()})
		if err != nil {
			return err
		}
		e.bytes.Add(int64(len(b)))
		e.spans.Add(1)
	}
	return nil
}
func (*exporter) Shutdown(context.Context) error { return nil }

type proxy struct {
	listener                        net.Listener
	turns, clientBytes, serverBytes atomic.Int64
}

func startProxy(target string) (*proxy, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &proxy{listener: l}
	go func() {
		for {
			client, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				server, err := net.Dial("tcp", target)
				if err != nil {
					client.Close()
					return
				}
				defer client.Close()
				defer server.Close()
				var mu sync.Mutex
				last := 0
				done := make(chan struct{})
				copyDirection := func(dst, src net.Conn, direction int) {
					buf := make([]byte, 32768)
					for {
						n, err := src.Read(buf)
						if n > 0 {
							mu.Lock()
							if direction == 1 {
								p.clientBytes.Add(int64(n))
								if last != 1 {
									p.turns.Add(1)
								}
							} else {
								p.serverBytes.Add(int64(n))
							}
							last = direction
							mu.Unlock()
							if _, werr := dst.Write(buf[:n]); werr != nil {
								return
							}
						}
						if err != nil {
							return
						}
					}
				}
				go func() { copyDirection(server, client, 1); server.Close(); close(done) }()
				copyDirection(client, server, 2)
				client.Close()
				<-done
			}()
		}
	}()
	return p, nil
}
func run() error {
	mode := flag.String("mode", "safe", "plain, compatibility, safe, diagnostic, metadata-cached, metadata-cold, monitoring or trace")
	n := flag.Int("n", 1000, "iterations")
	flag.Parse()
	if *n < 1 || *n > 100000 {
		return fmt.Errorf("invalid iteration count")
	}
	dsn := os.Getenv("FIREBIRD_TEST_DSN")
	if dsn == "" {
		return fmt.Errorf("FIREBIRD_TEST_DSN required")
	}
	u, err := url.Parse("firebird://" + strings.TrimPrefix(dsn, "firebird://"))
	if err != nil {
		return err
	}
	target := u.Host
	if u.Port() == "" {
		target = net.JoinHostPort(u.Hostname(), "3050")
	}
	p, err := startProxy(target)
	if err != nil {
		return err
	}
	defer p.listener.Close()
	u.Host = p.listener.Addr().String()
	dsn = u.String()
	ex := &exporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(ex))
	defer tp.Shutdown(context.Background())
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer mp.Shutdown(context.Background())
	var db *sql.DB
	switch *mode {
	case "plain":
		db, err = sql.Open("firebirdsql", dsn)
	case "compatibility":
		db, err = firebirdotel.Open(dsn, firebirdotel.WithTracerProvider(tp), firebirdotel.WithMeterProvider(mp))
	case "safe", "diagnostic", "metadata-cached", "metadata-cold", "monitoring", "trace":
		c := firebirdotel.SafeConfig()
		if *mode == "diagnostic" {
			c = firebirdotel.DiagnosticConfig()
		}
		c.TracerProvider = tp
		c.MeterProvider = mp
		db, err = firebirdotel.OpenWithConfig(dsn, c)
	default:
		return fmt.Errorf("invalid mode")
	}
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	extra, err := newExtras(ctx, *mode, dsn, db)
	if err != nil {
		return err
	}
	defer extra.close()
	query := func() error {
		rows, err := db.QueryContext(ctx, "SELECT N FROM OTEL_REPORT")
		if err != nil {
			return err
		}
		for rows.Next() {
			var n int
			if err := rows.Scan(&n); err != nil {
				rows.Close()
				return err
			}
		}
		if err := errorsJoin(rows.Err(), rows.Close()); err != nil {
			return err
		}
		return extra.post()
	}
	for i := 0; i < 5; i++ {
		if err := query(); err != nil {
			return err
		}
	}
	if err := extra.wait(5); err != nil {
		return err
	}
	warmupCount := 5
	if *mode == "trace" {
		// A procedure finish can precede buffered statement-finish records. Stop and
		// fully drain the warm-up worker before opening a fresh measured stream.
		extra, err = restartTraceAfterWarmup(extra, func() (*extras, error) { return newExtras(ctx, *mode, dsn, db) })
		if err != nil {
			return err
		}
		defer extra.close()
		warmupCount = 0
	}
	extra.bytes.Store(0)
	times := make([]int64, *n)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	turns, cb, sb := p.turns.Load(), p.clientBytes.Load(), p.serverBytes.Load()
	ex.bytes.Store(0)
	ex.spans.Store(0)
	for i := 0; i < *n; i++ {
		start := time.Now()
		if err := query(); err != nil {
			return err
		}
		times[i] = time.Since(start).Nanoseconds()
	}
	if err := extra.wait(*n + warmupCount); err != nil {
		return err
	}
	if err := extra.close(); err != nil {
		return err
	}
	runtime.ReadMemStats(&after)
	turns = p.turns.Load() - turns
	cb = p.clientBytes.Load() - cb
	sb = p.serverBytes.Load() - sb
	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &metrics); err != nil {
		return err
	}
	metricJSON, _ := json.Marshal(metrics)
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	q := func(percent int) float64 { return float64(times[(*n-1)*percent/100]) / 1000 }
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"mode": *mode, "iterations": *n, "p50_us": q(50), "p95_us": q(95), "p99_us": q(99), "allocs_per_op": float64(after.Mallocs-before.Mallocs) / float64(*n), "bytes_per_op": float64(after.TotalAlloc-before.TotalAlloc) / float64(*n), "tcp_request_turns_per_op": float64(turns) / float64(*n), "tcp_client_bytes_per_op": float64(cb) / float64(*n), "tcp_server_bytes_per_op": float64(sb) / float64(*n), "json_span_bytes_per_op": float64(ex.bytes.Load()) / float64(*n), "spans_per_op": float64(ex.spans.Load()) / float64(*n), "metric_snapshot_json_bytes": len(metricJSON), "diagnostic_json_bytes_per_op": float64(extra.bytes.Load()) / float64(*n)})
}
func errorsJoin(a, b error) error {
	if a != nil {
		return a
	}
	return b
}
func main() {
	if err := run(); err != nil {
		_, _ = io.WriteString(os.Stderr, "benchmark failed; check the isolated fixture\n")
		os.Exit(1)
	}
}
