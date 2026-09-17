package trace

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	otrace "go.opentelemetry.io/otel/trace"
)

func TestServerSpanParentageAndIsolation(t *testing.T) {
	for _, mode := range []string{"normal", "late_bind", "gap", "discard", "unregistered"} {
		t.Run(mode, func(t *testing.T) {
			recorder := tracetest.NewSpanRecorder()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
			defer tp.Shutdown(context.Background())
			s, err := NewSpans(SpanConfig{TracerProvider: tp})
			if err != nil {
				t.Fatal(err)
			}
			s.running = true
			_, parent := tp.Tracer("application").Start(context.Background(), "client")
			defer parent.End()
			token := s.Register(parent.SpanContext())
			if mode != "late_bind" {
				s.Bind(token, parent.SpanContext())
			}
			if mode == "unregistered" {
				token = strings.Repeat("0", 32)
			}
			r := &Runtime{events: make(chan Event), done: make(chan struct{})}
			go s.consume(r)
			e := Event{Source: "trace", Correlation: "heuristic", Timestamp: "2026-09-08T10:00:00.0000", AttachmentID: 1, TransactionID: 2}
			marker := e
			marker.Kind = "statement"
			marker.Phase = "finish"
			marker.ScopeToken = token
			marker.Sequence = 99
			r.events <- marker
			e.Kind = "statement"
			e.Name = "EXECUTE PROCEDURE OUTER"
			e.Phase = "start"
			e.Sequence = 1
			r.events <- e
			child := e
			child.Kind = "procedure"
			child.Name = "OUTER"
			child.Sequence = 2
			child.ParentSequence = 1
			r.events <- child
			nested := child
			nested.Name = "INNER"
			nested.Sequence = 3
			nested.ParentSequence = 2
			r.events <- nested
			if mode == "gap" {
				r.events <- Event{Kind: "gap", Incomplete: true}
			}
			if mode == "discard" {
				s.Discard(token)
			}
			nested.Phase = "finish"
			nested.Timestamp = "2026-09-08T10:00:00.0020"
			r.events <- nested
			child.Phase = "finish"
			child.Timestamp = "2026-09-08T10:00:00.0030"
			r.events <- child
			e.Phase = "finish"
			e.Timestamp = "2026-09-08T10:00:00.0040"
			r.events <- e
			r.events <- Event{} // Barrier: the root finish has been processed.
			if mode == "late_bind" {
				if len(recorder.Ended()) != 0 {
					t.Fatal("exported before client binding")
				}
				s.Bind(token, parent.SpanContext())
			}
			close(r.done)
			close(r.events)
			<-s.done
			spans := recorder.Ended()
			if mode == "gap" || mode == "discard" || mode == "unregistered" {
				if len(spans) != 0 {
					t.Fatal("ambiguous data exported", spans)
				}
				return
			}
			if len(spans) != 3 {
				t.Fatalf("got %d server spans", len(spans))
			}
			wantParent := parent.SpanContext().SpanID()
			for _, span := range spans {
				if span.Parent().SpanID() != wantParent || span.SpanContext().TraceID() != parent.SpanContext().TraceID() {
					t.Fatal("wrong parent", span.Name())
				}
				if span.EndTime().Before(span.StartTime()) || strings.Contains(fmt.Sprint(span.Attributes()), token) {
					t.Fatal("invalid timing or token leak")
				}
				wantParent = span.SpanContext().SpanID()
			}
		})
	}
}

func TestServerSpanBoundsAndStartup(t *testing.T) {
	target, err := NewSpans(SpanConfig{MaxPending: 1, Retention: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if token := target.Register(otrace.SpanContext{}); token != "" {
		t.Fatal("inactive collector registered scope")
	}
	target.running = true
	one := target.Register(otrace.SpanContext{})
	if len(one) != 32 {
		t.Fatal("missing scope")
	}
	target.MarkerComplete(one)
	marked, ok := target.lookup(one)
	if !ok || marked.markerCompleted.IsZero() || marked.markerCompleted.Before(marked.registered) {
		t.Fatal("marker completion was not retained")
	}
	if target.Register(otrace.SpanContext{}) != "" {
		t.Fatal("unbounded registrations")
	}
	target.Discard(one)
	if target.Register(otrace.SpanContext{}) == "" {
		t.Fatal("discard did not free capacity")
	}
	for _, c := range []SpanConfig{{MaxPending: 4097}, {Retention: time.Nanosecond}} {
		if _, err := NewSpans(c); err == nil {
			t.Fatal("invalid bounds")
		}
	}
	notStarted, _ := NewSpans(SpanConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if notStarted.Start(ctx) == nil || notStarted.started {
		t.Fatal("cancelled startup launched collector")
	}
	if notStarted.Shutdown(context.Background()) != nil {
		t.Fatal("idle shutdown failed")
	}
	if err := notStarted.Wait(t.Context()); err != nil {
		t.Fatal("idle shutdown did not complete waiters", err)
	}
	if notStarted.Start(context.Background(), Config{}) == nil {
		t.Fatal("invalid collector became ready")
	}
	if notStarted.Shutdown(context.Background()) == nil {
		t.Fatal("startup error lost")
	}
}

func TestGapDiscardsPendingMarkerRegistrations(t *testing.T) {
	s, err := NewSpans(SpanConfig{})
	if err != nil {
		t.Fatal(err)
	}
	s.running = true
	token := s.Register(otrace.SpanContext{})
	if token == "" {
		t.Fatal("registration failed")
	}
	r := &Runtime{events: make(chan Event), done: make(chan struct{})}
	go s.consume(r)
	r.events <- Event{Kind: "statement", Phase: "finish", ScopeToken: token, Sequence: 99, Correlation: "heuristic", AttachmentID: 1, Timestamp: "2026-09-17T08:00:00.0000"}
	r.events <- Event{Kind: "gap", Incomplete: true}
	r.events <- Event{} // Barrier: the gap has been handled before this receive.
	if _, ok := s.lookup(token); ok {
		t.Fatal("gap retained a pending marker registration")
	}
	close(r.done)
	close(r.events)
	<-s.done
}

func TestUnmatchedExecutionDiscardsActiveTree(t *testing.T) {
	s, err := NewSpans(SpanConfig{})
	if err != nil {
		t.Fatal(err)
	}
	s.running = true
	token := s.Register(otrace.SpanContext{})
	if token == "" {
		t.Fatal("registration failed")
	}
	r := &Runtime{events: make(chan Event), done: make(chan struct{})}
	go s.consume(r)
	r.events <- Event{Kind: "statement", Phase: "finish", ScopeToken: token, Sequence: 99, Correlation: "heuristic", AttachmentID: 1, Timestamp: "2026-09-17T08:00:00.0000"}
	r.events <- Event{Kind: "statement", Phase: "start", Name: "SELECT T", Sequence: 1, AttachmentID: 1, TransactionID: 2, Timestamp: "2026-09-17T08:00:00.0010"}
	r.events <- Event{Kind: "procedure", Phase: "finish", Incomplete: true, AttachmentID: 1, TransactionID: 2}
	r.events <- Event{} // Barrier: the unmatched event has been handled.
	if _, ok := s.lookup(token); ok {
		t.Fatal("unmatched execution retained an active tree")
	}
	close(r.done)
	close(r.events)
	<-s.done
}

func TestUnmatchedMarkerCannotCreateServerTree(t *testing.T) {
	s, err := NewSpans(SpanConfig{})
	if err != nil {
		t.Fatal(err)
	}
	s.running = true
	token := s.Register(otrace.SpanContext{})
	r := &Runtime{events: make(chan Event), done: make(chan struct{})}
	go s.consume(r)
	r.events <- Event{Kind: "statement", Phase: "finish", ScopeToken: token, AttachmentID: 1, Incomplete: true, Correlation: "unmatched", Timestamp: "2026-09-17T08:00:00.0000"}
	r.events <- Event{Kind: "statement", Phase: "start", Name: "SELECT T", Sequence: 1, AttachmentID: 1, TransactionID: 2, Timestamp: "2026-09-17T08:00:00.0010"}
	r.events <- Event{} // Barrier: the root statement has been handled.
	if _, ok := s.lookup(token); ok {
		t.Fatal("unmatched marker registration was retained")
	}
	close(r.done)
	close(r.events)
	<-s.done
}

func TestPendingMarkerWaitsForRootStatement(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer tp.Shutdown(context.Background())
	s, err := NewSpans(SpanConfig{TracerProvider: tp})
	if err != nil {
		t.Fatal(err)
	}
	s.running = true
	_, parent := tp.Tracer("application").Start(context.Background(), "client")
	defer parent.End()
	token := s.Register(parent.SpanContext())
	s.Bind(token, parent.SpanContext())
	r := &Runtime{events: make(chan Event), done: make(chan struct{})}
	go s.consume(r)
	base := Event{Source: "trace", Correlation: "heuristic", Timestamp: "2026-09-17T08:00:00.0000", AttachmentID: 1, TransactionID: 2}
	marker := base
	marker.Kind, marker.Phase, marker.ScopeToken = "statement", "finish", token
	marker.Sequence = 99
	r.events <- marker
	trigger := base
	trigger.Kind, trigger.Name, trigger.Phase, trigger.Sequence = "trigger", "ON_COMMIT", "start", 1
	r.events <- trigger
	autonomous := base
	autonomous.Kind, autonomous.Name, autonomous.Phase, autonomous.Sequence = "statement", "SELECT AUTONOMOUS", "start", 2
	r.events <- autonomous
	autonomous.Phase, autonomous.Timestamp = "finish", "2026-09-17T08:00:00.0005"
	r.events <- autonomous
	trigger.Phase, trigger.Timestamp = "finish", "2026-09-17T08:00:00.0010"
	r.events <- trigger
	statement := base
	statement.Kind, statement.Name, statement.Phase, statement.Sequence = "statement", "SELECT T", "start", 3
	r.events <- statement
	statement.Phase, statement.Timestamp = "finish", "2026-09-17T08:00:00.0020"
	r.events <- statement
	r.events <- Event{} // Barrier: the statement finish has been processed.
	close(r.done)
	close(r.events)
	<-s.done
	spans := recorder.Ended()
	if len(spans) != 1 || spans[0].Name() != "SELECT T" || spans[0].Parent().SpanID() != parent.SpanContext().SpanID() {
		t.Fatal("marker was consumed before the root statement", spans)
	}
}

func TestServerSpanExportsAllPerformanceCountersAndMarkerTiming(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer tp.Shutdown(context.Background())
	s, err := NewSpans(SpanConfig{TracerProvider: tp})
	if err != nil {
		t.Fatal(err)
	}
	_, parent := tp.Tracer("application").Start(context.Background(), "client")
	defer parent.End()
	serverAnchor := time.Date(2026, 9, 17, 8, 0, 0, 0, time.UTC)
	localAnchor := serverAnchor.Add(time.Second)
	start := serverAnchor.Add(10 * time.Millisecond)
	end := start.Add(5 * time.Millisecond)
	n := &serverNode{
		start:    Event{Kind: "procedure", Name: "P", Sequence: 1, Timestamp: start.Format("2006-01-02T15:04:05.999999999")},
		finish:   Event{Kind: "procedure", Name: "P", Sequence: 1, Timestamp: end.Format("2006-01-02T15:04:05.999999999"), DurationMS: 5, Reads: 1, Writes: 2, Fetches: 3, Marks: 4, Tables: []Table{{Name: "Order Items", Natural: 5, Index: 6, Update: 7, Insert: 8, Delete: 9, Backout: 10, Purge: 11, Expunge: 12}}},
		complete: true,
	}
	s.exportTree(&serverTree{scope: scope{parent: parent.SpanContext(), registered: localAnchor.Add(-time.Second), markerCompleted: localAnchor}, anchor: serverAnchor, nodes: []*serverNode{n}, bySequence: map[uint64]*serverNode{1: n}, complete: true})
	spans := recorder.Ended()
	if len(spans) != 1 || !spans[0].StartTime().Equal(localAnchor.Add(10*time.Millisecond)) || !spans[0].EndTime().Equal(localAnchor.Add(15*time.Millisecond)) {
		t.Fatal("server span was not aligned to marker completion", spans)
	}
	attrs := attributeMap(spans[0].Attributes())
	for key, want := range map[string]int64{"firebird.pages.read": 1, "firebird.pages.write": 2, "firebird.pages.fetch": 3, "firebird.pages.mark": 4} {
		if got := attrs[key].AsInt64(); got != want {
			t.Fatalf("%s=%d want %d", key, got, want)
		}
	}
	events := spans[0].Events()
	if len(events) != 1 {
		t.Fatal("table counters missing", events)
	}
	tableAttrs := attributeMap(events[0].Attributes)
	for key, want := range map[string]int64{"firebird.rows.backout": 10, "firebird.rows.purge": 11, "firebird.rows.expunge": 12} {
		if got := tableAttrs[key].AsInt64(); got != want {
			t.Fatalf("%s=%d want %d", key, got, want)
		}
	}
}

func attributeMap(attrs []attribute.KeyValue) map[string]attribute.Value {
	out := make(map[string]attribute.Value, len(attrs))
	for _, a := range attrs {
		out[string(a.Key)] = a.Value
	}
	return out
}
