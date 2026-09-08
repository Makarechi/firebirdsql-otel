package trace

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

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
	if notStarted.Start(context.Background(), Config{}) == nil {
		t.Fatal("invalid collector became ready")
	}
	if notStarted.Shutdown(context.Background()) == nil {
		t.Fatal("startup error lost")
	}
}
