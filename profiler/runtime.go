// Package profiler collects Firebird 5 record-source measurements for sampled
// SQL operations. It owns a separate, uninstrumented connection for reading
// and deleting the flushed profile; business SQL stays on its original connection.
package profiler

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/nakagami/firebirdsql"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const maxSources = 64
const topSources = 5
const cleanupTimeout = 5 * time.Second

var poolName = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)

type Runtime struct {
	db             *sql.DB
	name           string
	tracer         trace.Tracer
	stageDuration  metric.Float64Histogram
	stageCalls     metric.Int64Counter
	staleRemoved   metric.Int64Counter
	staleRemaining metric.Int64Gauge
	meterProvider  metric.MeterProvider
}

type Option func(*Runtime)

func WithTracerProvider(provider trace.TracerProvider) Option {
	return func(r *Runtime) {
		if provider != nil {
			r.tracer = provider.Tracer("github.com/Makarechi/firebirdsql-otel/profiler")
		}
	}
}

// WithMeterProvider directs bounded, low-cardinality profiler metrics to a
// service's meter provider. The default is the global provider.
func WithMeterProvider(provider metric.MeterProvider) Option {
	return func(r *Runtime) {
		if provider != nil {
			r.meterProvider = provider
		}
	}
}

func (r *Runtime) initMetrics(provider metric.MeterProvider) error {
	meter := provider.Meter("github.com/Makarechi/firebirdsql-otel/profiler")
	var err error
	r.stageDuration, err = meter.Float64Histogram("firebird.profiler.stage.duration",
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(.001, .002, .005, .01, .02, .05, .1, .2, .5, 1, 2, 5),
	)
	if err != nil {
		return errors.New("profiler: metric initialization failed")
	}
	r.stageCalls, err = meter.Int64Counter("firebird.profiler.stage.calls")
	if err != nil {
		return errors.New("profiler: metric initialization failed")
	}
	r.staleRemoved, err = meter.Int64Counter("firebird.profiler.stale.removed")
	if err != nil {
		return errors.New("profiler: metric initialization failed")
	}
	r.staleRemaining, err = meter.Int64Gauge("firebird.profiler.stale.remaining")
	if err != nil {
		return errors.New("profiler: metric initialization failed")
	}
	return nil
}

func (r *Runtime) recordStage(stage string, elapsed time.Duration, err error) {
	attrs := []attribute.KeyValue{attribute.String("pool", r.name), attribute.String("stage", stage)}
	if err != nil {
		attrs = append(attrs, attribute.String("outcome", "error"))
	} else {
		attrs = append(attrs, attribute.String("outcome", "ok"))
	}
	ctx := context.Background()
	if r.stageDuration != nil {
		r.stageDuration.Record(ctx, elapsed.Seconds(), metric.WithAttributes(attrs...))
	}
	if r.stageCalls != nil {
		r.stageCalls.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}

// Starter can profile one operation on the connection that executes it.
type Starter interface {
	Start(context.Context, driver.Conn, trace.SpanContext, string) (SessionHandle, error)
}

type SessionHandle interface {
	Finish(trace.SpanContext) error
	Cancel() error
}

// New opens only a diagnostic pool. It performs no SQL or schema changes.
func New(dsn, name string, options ...Option) (*Runtime, error) {
	if dsn == "" || !poolName.MatchString(name) {
		return nil, errors.New("profiler: invalid configuration")
	}
	db, err := sql.Open("firebirdsql", dsn)
	if err != nil {
		return nil, errors.New("profiler: diagnostic pool creation failed")
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	r := &Runtime{db: db, name: name, tracer: otel.Tracer("github.com/Makarechi/firebirdsql-otel/profiler")}
	r.meterProvider = otel.GetMeterProvider()
	for _, option := range options {
		if option != nil {
			option(r)
		}
	}
	if err := r.initMetrics(r.meterProvider); err != nil {
		_ = db.Close()
		return nil, err
	}
	return r, nil
}

func (r *Runtime) Close() error { return r.db.Close() }

// CheckSupported rejects databases where the default Firebird profiler cannot
// persist and delete its PLG$PROF_* rows. Call this before accepting traffic.
func (r *Runtime) CheckSupported(ctx context.Context) error {
	var readOnly, replicaMode int64
	if err := r.db.QueryRowContext(ctx, "SELECT MON$READ_ONLY, MON$REPLICA_MODE FROM MON$DATABASE").Scan(&readOnly, &replicaMode); err != nil {
		return fmt.Errorf("profiler: database mode check failed: %w", err)
	}
	if readOnly == 1 || replicaMode == 1 {
		return ErrReadOnlyDatabase
	}
	return nil
}

var ErrReadOnlyDatabase = errors.New("profiler: database is read-only; profile storage is unavailable")

type Session struct {
	runtime  *Runtime
	conn     driver.Conn
	span     trace.Span
	id       int64
	done     bool
	overhead time.Duration
}

// Start is called on the physical connection immediately before business SQL.
// The caller checks the sampled parent; no additional sampling occurs here.
func (r *Runtime) Start(ctx context.Context, conn driver.Conn, parent trace.SpanContext, summary string) (_ SessionHandle, err error) {
	if !parent.IsValid() || !parent.IsSampled() || conn == nil {
		return nil, errors.New("profiler: invalid sampled connection")
	}
	_, span := r.tracer.Start(ctx, "Firebird profile "+summary, trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.String("firebird.source", "profiler"), attribute.String("db.connection.name", r.name)))
	description := "firebirdotel/" + r.name + "/" + parent.TraceID().String()
	query := "SELECT RDB$PROFILER.START_SESSION('" + description + "') FROM RDB$DATABASE"
	started := time.Now()
	id, err := queryInt64(ctx, conn, query)
	startDuration := time.Since(started)
	r.recordStage("start", startDuration, err)
	span.SetAttributes(attribute.Float64("firebird.profiler.start_ms", float64(startDuration)/float64(time.Millisecond)))
	if err != nil {
		// A lost response can leave a started session on this connection.
		cleanup, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		started = time.Now()
		_, cancelErr := exec(cleanup, conn, "EXECUTE PROCEDURE RDB$PROFILER.CANCEL_SESSION")
		r.recordStage("cancel", time.Since(started), cancelErr)
		cancel()
		span.SetStatus(codes.Error, "start_failed")
		span.End()
		return nil, errors.Join(err, cancelErr)
	}
	span.SetAttributes(attribute.Int64("firebird.profiler.profile_id", id))
	return &Session{runtime: r, conn: conn, span: span, id: id, overhead: startDuration}, nil
}

func (s *Session) Finish(client trace.SpanContext) error {
	if s.done {
		return nil
	}
	s.done = true
	defer s.span.End()
	if client.IsValid() {
		s.span.SetAttributes(attribute.String("firebird.profiler.client_span_id", client.SpanID().String()))
	}
	finishCtx, finishCancel := context.WithTimeout(context.Background(), cleanupTimeout)
	started := time.Now()
	_, finishErr := exec(finishCtx, s.conn, "EXECUTE PROCEDURE RDB$PROFILER.FINISH_SESSION(TRUE)")
	finishDuration := time.Since(started)
	s.overhead += finishDuration
	s.runtime.recordStage("finish", finishDuration, finishErr)
	s.span.SetAttributes(attribute.Float64("firebird.profiler.finish_ms", float64(finishDuration)/float64(time.Millisecond)))
	var cancelErr error
	if finishErr != nil {
		cancelCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		started = time.Now()
		_, cancelErr = exec(cancelCtx, s.conn, "EXECUTE PROCEDURE RDB$PROFILER.CANCEL_SESSION")
		cancelDuration := time.Since(started)
		cancel()
		s.overhead += cancelDuration
		s.runtime.recordStage("cancel", cancelDuration, cancelErr)
		s.span.SetAttributes(attribute.Float64("firebird.profiler.cancel_ms", float64(cancelDuration)/float64(time.Millisecond)))
		s.span.SetStatus(codes.Error, "finish_failed")
	}
	finishCancel()
	var readErr error
	if finishErr == nil {
		readCtx, readCancel := context.WithTimeout(context.Background(), cleanupTimeout)
		started = time.Now()
		readErr = s.runtime.report(readCtx, s.id, s.span)
		reportDuration := time.Since(started)
		s.overhead += reportDuration
		s.runtime.recordStage("report", reportDuration, readErr)
		s.span.SetAttributes(attribute.Float64("firebird.profiler.report_ms", float64(reportDuration)/float64(time.Millisecond)))
		readCancel()
		if readErr != nil {
			s.span.SetStatus(codes.Error, "read_failed")
		}
	}
	deleteCtx, deleteCancel := context.WithTimeout(context.Background(), cleanupTimeout)
	started = time.Now()
	acquireDuration, executeDuration, acquireErr, executeErr := s.runtime.delete(deleteCtx, s.id)
	deleteErr := errors.Join(acquireErr, executeErr)
	cleanupDuration := time.Since(started)
	s.overhead += cleanupDuration
	s.runtime.recordStage("cleanup", cleanupDuration, deleteErr)
	s.runtime.recordStage("cleanup_acquire", acquireDuration, acquireErr)
	if acquireErr == nil {
		s.runtime.recordStage("cleanup_execute", executeDuration, executeErr)
	}
	s.span.SetAttributes(
		attribute.Float64("firebird.profiler.cleanup_ms", float64(cleanupDuration)/float64(time.Millisecond)),
		attribute.Float64("firebird.profiler.cleanup.acquire_ms", float64(acquireDuration)/float64(time.Millisecond)),
		attribute.Float64("firebird.profiler.cleanup.execute_ms", float64(executeDuration)/float64(time.Millisecond)),
		attribute.Float64("firebird.profiler.overhead_ms", float64(s.overhead)/float64(time.Millisecond)),
	)
	deleteCancel()
	if deleteErr != nil {
		s.span.SetStatus(codes.Error, "cleanup_failed")
	} else {
		s.span.SetAttributes(attribute.Bool("firebird.profiler.cleaned", true))
	}
	if err := errors.Join(finishErr, cancelErr, readErr, deleteErr); err != nil {
		for _, stage := range []struct {
			name string
			err  error
		}{{"finish", finishErr}, {"cancel", cancelErr}, {"read", readErr}, {"cleanup", deleteErr}} {
			if stage.err == nil {
				continue
			}
			attrs := []any{slog.String("pool", s.runtime.name), slog.Int64("profile_id", s.id), slog.String("stage", stage.name)}
			var fbErr *firebirdsql.FbError
			if errors.As(stage.err, &fbErr) {
				attrs = append(attrs, slog.String("sqlstate", fbErr.SQLState), slog.Any("gds_codes", fbErr.GDSCodes))
			} else if errors.Is(stage.err, context.DeadlineExceeded) {
				attrs = append(attrs, slog.String("reason", "timeout"))
			} else {
				attrs = append(attrs, slog.String("reason", "driver_or_context"))
			}
			slog.Error("Firebird profiler failed", attrs...)
		}
		return errors.New("profiler: collection or cleanup failed")
	}
	return nil
}

func (s *Session) Cancel() error {
	if s.done {
		return nil
	}
	s.done = true
	defer s.span.End()
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	started := time.Now()
	_, err := exec(ctx, s.conn, "EXECUTE PROCEDURE RDB$PROFILER.CANCEL_SESSION")
	s.runtime.recordStage("cancel", time.Since(started), err)
	s.span.SetAttributes(attribute.Float64("firebird.profiler.cancel_ms", float64(time.Since(started))/float64(time.Millisecond)))
	if err != nil {
		s.span.SetStatus(codes.Error, "cancel_failed")
	}
	return err
}

func queryInt64(ctx context.Context, conn driver.Conn, query string) (int64, error) {
	q, ok := conn.(driver.QueryerContext)
	if !ok {
		return 0, errors.New("profiler: connection cannot query")
	}
	rows, err := q.QueryContext(ctx, query, nil)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	values := make([]driver.Value, 1)
	if err := rows.Next(values); err != nil {
		return 0, err
	}
	switch v := values[0].(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	case string:
		return strconv.ParseInt(v, 10, 64)
	default:
		return 0, errors.New("profiler: unexpected session id")
	}
}

func exec(ctx context.Context, conn driver.Conn, query string) (driver.Result, error) {
	e, ok := conn.(driver.ExecerContext)
	if !ok {
		return nil, errors.New("profiler: connection cannot execute")
	}
	return e.ExecContext(ctx, query, nil)
}

func (r *Runtime) report(ctx context.Context, id int64, span trace.Span) error {
	const query = `SELECT s.STATEMENT_ID, s.REQUEST_ID, s.CURSOR_ID, s.RECORD_SOURCE_ID,
		r.PARENT_RECORD_SOURCE_ID, r.LEVEL, r.ACCESS_PATH,
		s.OPEN_COUNTER, s.FETCH_COUNTER, s.OPEN_TOTAL_ELAPSED_TIME, s.FETCH_TOTAL_ELAPSED_TIME
		FROM PLG$PROF_RECORD_SOURCE_STATS s
		JOIN PLG$PROF_RECORD_SOURCES r ON r.PROFILE_ID = s.PROFILE_ID
			AND r.STATEMENT_ID = s.STATEMENT_ID AND r.CURSOR_ID = s.CURSOR_ID
			AND r.RECORD_SOURCE_ID = s.RECORD_SOURCE_ID
		JOIN PLG$PROF_STATEMENTS p ON p.PROFILE_ID = s.PROFILE_ID AND p.STATEMENT_ID = s.STATEMENT_ID
		WHERE s.PROFILE_ID = ?
			AND (p.SQL_TEXT IS NULL OR (p.SQL_TEXT NOT STARTING WITH 'SELECT RDB$PROFILER.START_SESSION('
				AND p.SQL_TEXT NOT CONTAINING 'firebirdotel_scope:'))
		ORDER BY s.OPEN_TOTAL_ELAPSED_TIME + s.FETCH_TOTAL_ELAPSED_TIME DESC
		ROWS 65`
	rows, err := r.db.QueryContext(ctx, query, id)
	if err != nil {
		return err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var statement, request, cursor, source, level int64
		var parent sql.NullInt64
		var path sql.NullString
		var opens, fetches, openNS, fetchNS int64
		if err := rows.Scan(&statement, &request, &cursor, &source, &parent, &level, &path, &opens, &fetches, &openNS, &fetchNS); err != nil {
			return err
		}
		count++
		if count > maxSources {
			continue
		}
		attrs := []attribute.KeyValue{
			attribute.Int64("statement_id", statement), attribute.Int64("request_id", request),
			attribute.Int64("cursor_id", cursor), attribute.Int64("record_source_id", source),
			attribute.Int64("level", level),
			attribute.Int64("open_count", opens), attribute.Int64("fetch_count", fetches),
			attribute.Float64("open_ms", float64(openNS)/1e6), attribute.Float64("fetch_ms", float64(fetchNS)/1e6),
		}
		if parent.Valid {
			attrs = append(attrs, attribute.Int64("parent_record_source_id", parent.Int64))
		}
		if path.Valid {
			if safe := safeAccessPath(path.String); safe != "" {
				attrs = append(attrs, attribute.String("access_path", safe))
				if count <= topSources && len(safe) <= 255 {
					span.SetAttributes(attribute.String(fmt.Sprintf("firebird.profiler.top.%02d.path", count), safe))
				}
			}
		}
		if count <= topSources {
			span.SetAttributes(
				attribute.Float64(fmt.Sprintf("firebird.profiler.top.%02d.ms", count), float64(openNS+fetchNS)/1e6),
				attribute.Int64(fmt.Sprintf("firebird.profiler.top.%02d.fetch_count", count), fetches),
			)
		}
		span.AddEvent("firebird.profiler.record_source", trace.WithAttributes(attrs...))
	}
	if err := rows.Err(); err != nil {
		return err
	}
	span.SetAttributes(attribute.Int("firebird.profiler.record_sources", count), attribute.Bool("firebird.profiler.truncated", count > maxSources))
	return nil
}

func safeAccessPath(path string) string {
	path = strings.Join(strings.Fields(path), " ")
	if len(path) == 0 || len(path) > 512 {
		return ""
	}
	for _, char := range path {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' {
			continue
		}
		if strings.ContainsRune("_$\" .->():,", char) {
			continue
		}
		return ""
	}
	return path
}

func (r *Runtime) delete(ctx context.Context, id int64) (time.Duration, time.Duration, error, error) {
	started := time.Now()
	conn, err := r.db.Conn(ctx)
	acquire := time.Since(started)
	if err != nil {
		return acquire, 0, err, nil
	}
	defer conn.Close()
	started = time.Now()
	_, err = conn.ExecContext(ctx, `DELETE FROM PLG$PROF_SESSIONS WHERE PROFILE_ID = ?`, id)
	return acquire, time.Since(started), nil, err
}

// CleanupStale removes only this pool's finished profiles left by an interrupted
// export. First use of the profiler creates its tables, so an absent table is fine.
func (r *Runtime) CleanupStale(ctx context.Context, olderThan time.Duration) (err error) {
	started := time.Now()
	defer func() { r.recordStage("stale_cleanup", time.Since(started), err) }()
	if olderThan <= 0 {
		return errors.New("profiler: invalid cleanup age")
	}
	var exists int
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = 'PLG$PROF_SESSIONS'`).Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		return nil
	}
	prefix, cutoff := fmt.Sprintf("firebirdotel/%s/", r.name), time.Now().Add(-olderThan)
	result, err := r.db.ExecContext(ctx, `DELETE FROM PLG$PROF_SESSIONS WHERE DESCRIPTION STARTING WITH ? AND FINISH_TIMESTAMP < ?`, prefix, cutoff)
	if err == nil && r.staleRemoved != nil {
		if count, countErr := result.RowsAffected(); countErr == nil && count > 0 {
			r.staleRemoved.Add(context.Background(), count, metric.WithAttributes(attribute.String("pool", r.name)))
		}
	}
	if err != nil {
		return err
	}
	var remaining int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM PLG$PROF_SESSIONS WHERE DESCRIPTION STARTING WITH ? AND FINISH_TIMESTAMP < ?`, prefix, cutoff).Scan(&remaining); err != nil {
		return err
	}
	if r.staleRemaining != nil {
		r.staleRemaining.Record(context.Background(), remaining, metric.WithAttributes(attribute.String("pool", r.name)))
	}
	return nil
}

var _ io.Closer = (*Runtime)(nil)
var _ Starter = (*Runtime)(nil)
