package trace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	otrace "go.opentelemetry.io/otel/trace"
)

// SpanConfig enables server diagnostics explicitly. One runtime observes one
// database; independent applications only export their own registered scopes.
type SpanConfig struct {
	Collector      Config
	TracerProvider otrace.TracerProvider
	MeterProvider  metric.MeterProvider
	MaxPending     int
	Retention      time.Duration
}

type scope struct {
	parent          otrace.SpanContext
	registered      time.Time
	markerCompleted time.Time
}

// SpanRuntime converts completed server execution trees into child spans.
// Text Trace nesting remains heuristic; no static dependencies become spans.
type SpanRuntime struct {
	c                SpanConfig
	mu               sync.Mutex
	started, running bool
	stopping         bool
	collector        *Runtime
	registrations    map[string]scope
	done             chan struct{}
	doneOnce         sync.Once
	bound            chan struct{}
	err              error
	tracer           otrace.Tracer
	dropped          metric.Int64Counter
}

func NewSpans(c SpanConfig) (*SpanRuntime, error) {
	if c.MaxPending == 0 {
		c.MaxPending = 256
	}
	if c.Retention == 0 {
		c.Retention = 2 * time.Minute
	}
	if c.MaxPending < 1 || c.MaxPending > 4096 || c.Retention < time.Second || c.Retention > 5*time.Minute {
		return nil, errors.New("trace: invalid span bounds")
	}
	tp := c.TracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	mp := c.MeterProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	dropped, err := mp.Meter("github.com/Makarechi/firebirdsql-otel/trace").Int64Counter("firebird.server.trace.dropped")
	if err != nil {
		return nil, errors.New("trace: metric initialization failed")
	}
	return &SpanRuntime{c: c, registrations: make(map[string]scope), done: make(chan struct{}), bound: make(chan struct{}, 1), tracer: tp.Tracer("github.com/Makarechi/firebirdsql-otel/trace"), dropped: dropped}, nil
}

// Start waits for collector readiness. Its context bounds startup, not lifetime.
// Shutdown must be called before shutting down the application's OTel providers.
func (s *SpanRuntime) Start(ctx context.Context, collector ...Config) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(collector) > 1 {
		return errors.New("trace: one collector configuration expected")
	}
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return errors.New("trace: span runtime already started")
	}
	if len(collector) == 1 {
		s.c.Collector = collector[0]
	}
	s.started = true
	s.mu.Unlock()
	r, err := Start(ctx, s.c.Collector)
	s.mu.Lock()
	s.collector = r
	stopped := s.stopping
	s.mu.Unlock()
	if stopped && err == nil {
		err = errors.New("trace: startup interrupted by shutdown")
	}
	if err == nil {
		select {
		case e, ok := <-r.Events():
			if !ok || e.Kind != "lifecycle" || e.Phase != "ready" {
				err = errors.New("trace: collector did not become ready")
			}
		case <-ctx.Done():
			err = ctx.Err()
		}
		if err == nil {
			err = ctx.Err()
		}
	}
	s.mu.Lock()
	if s.stopping && err == nil {
		err = errors.New("trace: startup interrupted by shutdown")
	}
	if err == nil {
		s.running = true
	}
	s.mu.Unlock()
	if err != nil {
		if r != nil {
			stop, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err = errors.Join(err, r.Shutdown(stop))
			cancel()
		}
		s.mu.Lock()
		s.err = err
		s.mu.Unlock()
		s.signalDone()
		return err
	}
	go s.consume(r)
	return nil
}

// Register is used by the instrumented driver before its reserved marker query.
// Unsampled parents are skipped; Bind checks the final client sampling decision.
// Tokens never become span attributes.
func (s *SpanRuntime) Register(parent otrace.SpanContext) string {
	if parent.IsValid() && !parent.IsSampled() {
		return ""
	}
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return ""
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running {
		return ""
	}
	for k, v := range s.registrations {
		if now.Sub(v.registered) > s.c.Retention {
			delete(s.registrations, k)
		}
	}
	if len(s.registrations) >= s.c.MaxPending {
		return ""
	}
	token := hex.EncodeToString(bytes[:])
	s.registrations[token] = scope{registered: now}
	return token
}

// MarkerComplete records the local completion of the marker query. The driver
// calls it immediately before submitting the business statement.
func (s *SpanRuntime) MarkerComplete(token string) {
	s.mu.Lock()
	if v, ok := s.registrations[token]; ok {
		v.markerCompleted = time.Now()
		s.registrations[token] = v
	}
	s.mu.Unlock()
}

// Bind supplies the actual client span after driver execution. This preserves
// database/sql ErrSkip fallback and the client's existing sampling attributes.
func (s *SpanRuntime) Bind(token string, parent otrace.SpanContext) {
	s.mu.Lock()
	if v, ok := s.registrations[token]; ok {
		if parent.IsValid() && parent.IsSampled() {
			v.parent = parent
			s.registrations[token] = v
		} else {
			delete(s.registrations, token)
		}
	}
	s.mu.Unlock()
	select {
	case s.bound <- struct{}{}:
	default:
	}
}

// Discard invalidates failed markers and ambiguous overlapping cursors.
func (s *SpanRuntime) Discard(token string) {
	s.mu.Lock()
	delete(s.registrations, token)
	s.mu.Unlock()
}

func (s *SpanRuntime) lookup(token string) (scope, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.registrations[token]
	if ok && time.Since(v.registered) > s.c.Retention {
		delete(s.registrations, token)
		return scope{}, false
	}
	return v, ok
}

func (s *SpanRuntime) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	r := s.collector
	started := s.started
	s.running = false
	s.stopping = true
	s.mu.Unlock()
	if !started {
		s.signalDone()
		return nil
	}
	var err error
	if r != nil {
		err = r.Shutdown(ctx)
	}
	select {
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		return errors.Join(err, s.err)
	case <-ctx.Done():
		return errors.Join(err, ctx.Err())
	}
}

func (s *SpanRuntime) signalDone() {
	s.doneOnce.Do(func() { close(s.done) })
}

// Wait reports collector termination without stopping it. Unexpected termination
// is an error; diagnostics stop registering scopes while client queries continue.
func (s *SpanRuntime) Wait(ctx context.Context) error {
	select {
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type serverNode struct {
	start, finish Event
	complete      bool
}
type serverTree struct {
	token      string
	scope      scope
	anchor     time.Time
	nodes      []*serverNode
	bySequence map[uint64]*serverNode
	complete   bool
}

func (s *SpanRuntime) consume(r *Runtime) {
	defer s.signalDone()
	pending := make(map[int64]struct {
		token  string
		anchor time.Time
	})
	frames := make(map[uint64]*serverTree)
	trees := make(map[*serverTree]bool)
	active := make(map[uint64]int64)
	activeByAttachment := make(map[int64]int)
	drop := func(tree *serverTree, reason string) {
		for seq := range tree.bySequence {
			delete(frames, seq)
		}
		delete(trees, tree)
		s.Discard(tree.token)
		s.dropped.Add(context.Background(), 1, metric.WithAttributes(attribute.String("reason", reason)))
	}
	invalidate := func(reason string) {
		for tree := range trees {
			drop(tree, reason)
		}
		for _, p := range pending {
			s.Discard(p.token)
		}
		clear(pending)
		clear(active)
		clear(activeByAttachment)
	}
	flush := func() {
		for tree := range trees {
			if sc, ok := s.lookup(tree.token); !ok {
				drop(tree, "expired_or_ambiguous")
			} else if tree.complete && sc.parent.IsValid() {
				tree.scope = sc
				s.exportTree(tree)
				for seq := range tree.bySequence {
					delete(frames, seq)
				}
				delete(trees, tree)
				s.Discard(tree.token)
			}
		}
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.bound:
			flush()
		case <-ticker.C:
			flush()
			for att, v := range pending {
				if _, ok := s.lookup(v.token); !ok {
					delete(pending, att)
				}
			}
		case e, ok := <-r.Events():
			if !ok {
				flush()
				for tree := range trees {
					drop(tree, "stream_end")
				}
				s.mu.Lock()
				s.running = false
				clear(s.registrations)
				s.mu.Unlock()
				err := r.Wait(context.Background())
				s.mu.Lock()
				if err == nil && !s.stopping {
					err = errors.New("trace: collector ended unexpectedly")
				}
				s.err = err
				s.mu.Unlock()
				return
			}
			if e.Kind == "gap" {
				invalidate("gap")
				continue
			}
			if e.ScopeToken != "" {
				if e.Phase != "finish" {
					continue
				}
				if e.Sequence == 0 || e.Incomplete || e.Correlation == "unmatched" {
					s.Discard(e.ScopeToken)
					invalidate("unmatched_marker")
					continue
				}
				delete(pending, e.AttachmentID)
				anchor, err := time.Parse("2006-01-02T15:04:05.999999999", e.Timestamp)
				if _, ok := s.lookup(e.ScopeToken); ok && err == nil && e.AttachmentID > 0 && len(pending) < s.c.MaxPending {
					pending[e.AttachmentID] = struct {
						token  string
						anchor time.Time
					}{e.ScopeToken, anchor}
				}
				continue
			}
			if e.Sequence == 0 {
				if e.Incomplete && (e.Kind == "statement" || e.Kind == "procedure" || e.Kind == "function" || e.Kind == "trigger") {
					invalidate("unmatched")
				}
				continue
			}
			if e.Phase == "start" {
				tree := frames[e.ParentSequence]
				if p, ok := pending[e.AttachmentID]; ok && e.Kind == "statement" && e.ParentSequence == 0 && activeByAttachment[e.AttachmentID] == 0 {
					delete(pending, e.AttachmentID)
					if sc, valid := s.lookup(p.token); valid {
						tree = &serverTree{token: p.token, scope: sc, anchor: p.anchor, bySequence: make(map[uint64]*serverNode)}
						trees[tree] = true
					}
				}
				if e.AttachmentID > 0 {
					active[e.Sequence] = e.AttachmentID
					activeByAttachment[e.AttachmentID]++
				}
				if tree == nil {
					continue
				}
				if _, ok := s.lookup(tree.token); !ok {
					drop(tree, "expired_or_ambiguous")
					continue
				}
				if len(tree.nodes) >= 128 || len(frames) >= 4096 {
					drop(tree, "overflow")
					continue
				}
				n := &serverNode{start: e}
				tree.nodes = append(tree.nodes, n)
				tree.bySequence[e.Sequence] = n
				frames[e.Sequence] = tree
			} else if e.Phase == "finish" {
				if attachmentID, ok := active[e.Sequence]; ok {
					delete(active, e.Sequence)
					activeByAttachment[attachmentID]--
					if activeByAttachment[attachmentID] == 0 {
						delete(activeByAttachment, attachmentID)
					}
				}
				tree := frames[e.Sequence]
				if tree == nil {
					continue
				}
				n := tree.bySequence[e.Sequence]
				n.finish = e
				n.complete = true
				if tree.nodes[0] == n {
					tree.complete = true
					flush()
				}
			}
		}
	}
}

func (s *SpanRuntime) exportTree(tree *serverTree) {
	incomplete := false
	for _, n := range tree.nodes {
		incomplete = incomplete || !n.complete || n.start.Incomplete || n.finish.Incomplete
	}
	parents := make(map[uint64]otrace.SpanContext)
	for _, n := range tree.nodes {
		if !n.complete {
			continue
		}
		start, err := time.Parse("2006-01-02T15:04:05.999999999", n.start.Timestamp)
		end, endErr := time.Parse("2006-01-02T15:04:05.999999999", n.finish.Timestamp)
		if err != nil || endErr != nil || end.Before(start) {
			continue
		}
		parent := tree.scope.parent
		if n != tree.nodes[0] {
			var ok bool
			parent, ok = parents[n.start.ParentSequence]
			if !ok {
				continue
			}
		}
		attrs := []attribute.KeyValue{
			attribute.String("db.system.name", "firebirdsql"), attribute.String("firebird.source", "trace"),
			attribute.String("firebird.correlation", "heuristic"), attribute.String("firebird.parent.mapping", "sql_marker"),
			attribute.String("firebird.clock.alignment", "marker_estimate"), attribute.String("firebird.server.kind", n.start.Kind),
			attribute.Bool("firebird.incomplete", incomplete),
			attribute.Int64("firebird.server.duration_ms", n.finish.DurationMS),
			attribute.Int64("firebird.pages.read", n.finish.Reads), attribute.Int64("firebird.pages.write", n.finish.Writes),
			attribute.Int64("firebird.pages.fetch", n.finish.Fetches), attribute.Int64("firebird.pages.mark", n.finish.Marks),
		}
		if n.start.Kind == "procedure" {
			attrs = append(attrs, attribute.String("db.stored_procedure.name", n.start.Name))
		}
		if n.finish.SQL != "" {
			attrs = append(attrs, attribute.String("db.query.text", n.finish.SQL))
		}
		name := n.start.Name
		if name == "" {
			name = n.start.Kind
		}
		ctx := otrace.ContextWithSpanContext(context.Background(), parent)
		localAnchor := tree.scope.markerCompleted
		if localAnchor.IsZero() {
			localAnchor = tree.scope.registered
		}
		_, span := s.tracer.Start(ctx, name, otrace.WithSpanKind(otrace.SpanKindInternal), otrace.WithTimestamp(localAnchor.Add(start.Sub(tree.anchor))), otrace.WithAttributes(attrs...))
		parents[n.start.Sequence] = span.SpanContext()
		for _, table := range n.finish.Tables {
			span.AddEvent("firebird.table", otrace.WithTimestamp(localAnchor.Add(end.Sub(tree.anchor))), otrace.WithAttributes(attribute.String("db.collection.name", table.Name), attribute.Int64("firebird.rows.read.natural", table.Natural), attribute.Int64("firebird.rows.read.index", table.Index), attribute.Int64("firebird.rows.updated", table.Update), attribute.Int64("firebird.rows.inserted", table.Insert), attribute.Int64("firebird.rows.deleted", table.Delete), attribute.Int64("firebird.rows.backout", table.Backout), attribute.Int64("firebird.rows.purge", table.Purge), attribute.Int64("firebird.rows.expunge", table.Expunge)))
		}
		span.End(otrace.WithTimestamp(localAnchor.Add(end.Sub(tree.anchor))))
	}
}
