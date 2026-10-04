package trace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

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

func TestUnmatchedOtherAttachmentPreservesActiveTree(t *testing.T) {
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
	marker.Kind, marker.Phase, marker.ScopeToken, marker.Sequence = "statement", "finish", token, 99
	r.events <- marker
	root := base
	root.Kind, root.Phase, root.Name, root.Sequence = "statement", "start", "SELECT T", 1
	r.events <- root
	r.events <- Event{Kind: "procedure", Phase: "finish", Incomplete: true, Correlation: "unmatched", AttachmentID: 7, TransactionID: 8}
	r.events <- Event{Kind: "statement", Phase: "finish", ScopeToken: strings.Repeat("0", 32), Incomplete: true, Correlation: "unmatched", AttachmentID: 7, TransactionID: 8}
	root.Phase, root.Timestamp = "finish", "2026-09-17T08:00:00.0010"
	r.events <- root
	close(r.done)
	close(r.events)
	<-s.done
	spans := recorder.Ended()
	if len(spans) != 1 || spans[0].Name() != "SELECT T" || spans[0].Parent().SpanID() != parent.SpanContext().SpanID() {
		t.Fatal("unrelated attachment discarded the active tree", spans)
	}
}

func TestLargeServerTreeKeepsRootSpan(t *testing.T) {
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
	marker.Kind, marker.Phase, marker.ScopeToken, marker.Sequence = "statement", "finish", token, 99
	r.events <- marker
	root := base
	root.Kind, root.Phase, root.Name, root.Sequence = "statement", "start", "SELECT VIEW", 1
	r.events <- root
	for i := uint64(2); i <= 202; i++ {
		child := base
		child.Kind, child.Phase, child.Name, child.Sequence, child.ParentSequence = "procedure", "start", "LOOKUP", i, 1
		r.events <- child
		child.Phase = "finish"
		r.events <- child
	}
	root.Phase, root.Timestamp = "finish", "2026-09-17T08:00:00.0100"
	r.events <- root
	close(r.done)
	close(r.events)
	<-s.done
	spans := recorder.Ended()
	if len(spans) != 128 {
		t.Fatalf("got %d spans, want bounded root plus children", len(spans))
	}
	rootSpan := spans[0]
	if rootSpan.Name() != "SELECT VIEW" || rootSpan.Parent().SpanID() != parent.SpanContext().SpanID() || !attributeMap(rootSpan.Attributes())["firebird.incomplete"].AsBool() {
		t.Fatal("large query lost its incomplete parent span", rootSpan)
	}
	attrs := attributeMap(rootSpan.Attributes())
	if attrs["firebird.incomplete.reasons"].AsString() != "collection_limit" || attrs["firebird.server.nodes.exportable"].AsInt64() != 128 {
		t.Fatal("bounded trace does not explain its limits", attrs)
	}
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

func TestServerStatementSpanExportsExecutionPlan(t *testing.T) {
	for _, tc := range []struct {
		name, startPlan, finishPlan, wantPlan string
	}{
		{name: "plan on start", startPlan: "PLAN ( T ORDER IDX_ID )", wantPlan: "PLAN ( T ORDER IDX_ID )"},
		{name: "plan on finish", finishPlan: "PLAN SORT ( T INDEX ( IDX_CONTRACT ) )", wantPlan: "PLAN SORT ( T INDEX ( IDX_CONTRACT ) )"},
		{name: "explained plan", startPlan: "Select Expression\n    -> Filter", wantPlan: "Select Expression\n    -> Filter"},
		{name: "missing plan"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := tracetest.NewSpanRecorder()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
			defer tp.Shutdown(context.Background())
			s, err := NewSpans(SpanConfig{TracerProvider: tp})
			if err != nil {
				t.Fatal(err)
			}
			_, parent := tp.Tracer("application").Start(context.Background(), "client")
			defer parent.End()
			anchor := time.Date(2026, 10, 2, 8, 0, 0, 0, time.UTC)
			start := Event{Kind: "statement", Name: "SELECT T", Plan: tc.startPlan, Sequence: 1, Timestamp: anchor.Add(time.Millisecond).Format("2006-01-02T15:04:05.999999999")}
			finish := Event{Kind: "statement", Name: "SELECT T", Plan: tc.finishPlan, Sequence: 1, Timestamp: anchor.Add(2 * time.Millisecond).Format("2006-01-02T15:04:05.999999999")}
			n := &serverNode{start: start, finish: finish, complete: true}
			s.exportTree(&serverTree{scope: scope{parent: parent.SpanContext(), registered: anchor}, anchor: anchor, nodes: []*serverNode{n}})
			spans := recorder.Ended()
			if len(spans) != 1 {
				t.Fatalf("got %d server spans, want 1", len(spans))
			}
			got, ok := attributeMap(spans[0].Attributes())["firebird.query.plan"]
			if tc.wantPlan == "" {
				if ok {
					t.Fatalf("unexpected plan attribute: %s", got.AsString())
				}
			} else if !ok || got.AsString() != tc.wantPlan {
				t.Fatalf("plan = %q, present = %t; want %q", got.AsString(), ok, tc.wantPlan)
			}
			if tc.wantPlan != "" {
				sum := sha256.Sum256([]byte(normalizePlan(tc.wantPlan)))
				fingerprint := attributeMap(spans[0].Attributes())["firebird.query.plan.fingerprint"].AsString()
				if fingerprint != hex.EncodeToString(sum[:12]) {
					t.Fatalf("plan fingerprint = %s", fingerprint)
				}
			}
		})
	}
}

func TestPlanClassificationAndFingerprintRetainStructure(t *testing.T) {
	plan := "Select Expression\n    -> Table \"SORT_QUEUE\" Full Scan\n    -> Index \"IDX\" Range Scan"
	flags := classifyPlan(plan)
	if flags.sort || !flags.natural || !flags.indexRange || flags.indexFull {
		t.Fatalf("unrelated operations were combined: %+v", flags)
	}
	flags = classifyPlan("Select Expression\n    -> Index \"IDX\" Full Scan")
	if !flags.indexFull || flags.natural {
		t.Fatalf("index full scan was not classified: %+v", flags)
	}
	flags = classifyPlan("PLAN SORT (T NATURAL)")
	if !flags.sort || !flags.natural || flags.indexFull {
		t.Fatalf("classic plan was not classified: %+v", flags)
	}
	if flags := classifyPlan("PLAN MERGE (SORT (T NATURAL), SORT (U NATURAL))"); !flags.sort || !flags.natural {
		t.Fatalf("nested classic sort was missed: %+v", flags)
	}
	if flags := classifyPlan(`PLAN ("NATURAL" INDEX ("SORT_QUEUE"))`); flags.natural || flags.sort {
		t.Fatalf("quoted object name mistaken for an operator: %+v", flags)
	}
	left := normalizePlan("Select Expression\r\n    -> Filter  \r\n        -> Index \"IDX\" Full Scan")
	right := normalizePlan("Select Expression\n        -> Filter\n    -> Index \"IDX\" Full Scan")
	if left == right || left != normalizePlan("Select Expression\n    -> Filter\n        -> Index \"IDX\" Full Scan") {
		t.Fatal("plan hierarchy or line-ending normalization lost")
	}
}

func TestRepeatGroupsKeepPerformanceAndNestedParent(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer tp.Shutdown(context.Background())
	s, err := NewSpans(SpanConfig{TracerProvider: tp, CollapseFastRepeats: true})
	if err != nil {
		t.Fatal(err)
	}
	_, parent := tp.Tracer("application").Start(context.Background(), "client")
	defer parent.End()
	anchor := time.Date(2026, 10, 3, 8, 0, 0, 0, time.UTC)
	stamp := func(ms int) string {
		return anchor.Add(time.Duration(ms) * time.Millisecond).Format("2006-01-02T15:04:05.999999999")
	}
	node := func(seq, parentSeq uint64, kind, name string, at int) *serverNode {
		start := Event{Sequence: seq, ParentSequence: parentSeq, Kind: kind, Name: name, Timestamp: stamp(at)}
		finish := start
		finish.Timestamp = stamp(at + 1)
		return &serverNode{start: start, finish: finish, complete: true}
	}
	root := node(1, 0, "statement", "SELECT", 0)
	root.finish.Timestamp = stamp(10)
	nested := node(2, 1, "procedure", "OUTER", 1)
	nested.finish.Timestamp = stamp(9)
	tree := &serverTree{scope: scope{parent: parent.SpanContext(), registered: anchor}, anchor: anchor,
		nodes: []*serverNode{root, nested, node(3, 2, "procedure", "LOOKUP", 2), node(4, 2, "procedure", "LOOKUP", 3), node(5, 2, "procedure", "LOOKUP", 4), node(6, 2, "procedure", "LOOKUP", 5)}}
	tree.nodes[5].finish.Reads = 1
	s.exportTree(tree)
	spans := recorder.Ended()
	if len(spans) != 3 {
		t.Fatalf("got %d spans, want root, parent and measured call", len(spans))
	}
	if len(spans[0].Events()) != 0 || len(spans[1].Events()) != 1 || spans[1].Events()[0].Name != "firebird.server.repeated_procedure" {
		t.Fatalf("summary not attached to nested parent: %+v", spans)
	}
	if spans[1].Events()[0].Time.After(spans[1].EndTime()) || spans[1].Events()[0].Time.Before(spans[1].StartTime()) {
		t.Fatal("summary event outside parent span")
	}
	if attributeMap(spans[2].Attributes())["firebird.pages.read"].AsInt64() != 1 {
		t.Fatal("performance counters were collapsed")
	}
	tree.omitted = true
	if collapsed, _ := s.repeatedLeaves(tree); len(collapsed) != 0 {
		t.Fatal("truncated tree collapsed calls")
	}
}

func TestRepeatGroupSelectionStableAcrossParents(t *testing.T) {
	s := &SpanRuntime{c: SpanConfig{CollapseFastRepeats: true}}
	stamp := "2026-10-03T08:00:00.000000000"
	root := &serverNode{start: Event{Sequence: 1, Kind: "statement", Timestamp: stamp}, finish: Event{Timestamp: stamp}, complete: true}
	tree := &serverTree{nodes: []*serverNode{root}}
	for parent := uint64(2); parent <= 11; parent++ {
		p := &serverNode{start: Event{Sequence: parent, ParentSequence: 1, Kind: "procedure", Timestamp: stamp}, finish: Event{Timestamp: stamp}, complete: true}
		tree.nodes = append(tree.nodes, p)
		for i := uint64(0); i < 3; i++ {
			seq := parent*10 + i + 100
			tree.nodes = append(tree.nodes, &serverNode{start: Event{Sequence: seq, ParentSequence: parent, Kind: "procedure", Name: "LOOKUP", Timestamp: stamp}, finish: Event{Timestamp: stamp}, complete: true})
		}
	}
	for i := 0; i < 20; i++ {
		_, groups := s.repeatedLeaves(tree)
		if len(groups) != 8 || groups[0].parent != 2 || groups[7].parent != 9 {
			t.Fatalf("unstable repeat selection: %+v", groups)
		}
	}
}

func TestCompactServerTreeRetainsSlowAndNestedCalls(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer tp.Shutdown(context.Background())
	s, err := NewSpans(SpanConfig{TracerProvider: tp, CollapseFastRepeats: true})
	if err != nil {
		t.Fatal(err)
	}
	_, parent := tp.Tracer("application").Start(context.Background(), "client")
	defer parent.End()
	anchor := time.Date(2026, 10, 3, 8, 0, 0, 0, time.UTC)
	stamp := func(ms int) string {
		return anchor.Add(time.Duration(ms) * time.Millisecond).Format("2006-01-02T15:04:05.999999999")
	}
	node := func(seq, parentSeq uint64, kind, name string, startMS, endMS int) *serverNode {
		start := Event{Sequence: seq, ParentSequence: parentSeq, Kind: kind, Name: name, Timestamp: stamp(startMS)}
		finish := start
		finish.Timestamp = stamp(endMS)
		finish.DurationMS = int64(endMS - startMS)
		return &serverNode{start: start, finish: finish, complete: true}
	}
	tree := &serverTree{scope: scope{parent: parent.SpanContext(), registered: anchor}, anchor: anchor, nodes: []*serverNode{
		node(1, 0, "statement", "SELECT T", 0, 10),
		node(2, 1, "procedure", "LOOKUP", 1, 1),
		node(3, 1, "procedure", "LOOKUP", 2, 2),
		node(4, 1, "procedure", "LOOKUP", 3, 3),
		node(5, 1, "procedure", "LOOKUP", 4, 8),
		node(6, 1, "procedure", "NESTED", 5, 6),
		node(7, 6, "procedure", "CHILD", 5, 5),
	}}
	s.exportTree(tree)
	spans := recorder.Ended()
	if len(spans) != 4 {
		t.Fatalf("got %d spans, want root, slow lookup, nested parent and child", len(spans))
	}
	rootAttrs := attributeMap(spans[0].Attributes())
	if rootAttrs["firebird.server.repeated_calls.collapsed"].AsInt64() != 3 || len(spans[0].Events()) != 1 {
		t.Fatalf("repeat summary missing: attrs=%v events=%v", rootAttrs, spans[0].Events())
	}
	if attributeMap(spans[0].Events()[0].Attributes)["call_count"].AsInt64() != 3 {
		t.Fatal("wrong repeated call count")
	}
	if spans[1].Name() != "LOOKUP" || spans[2].Name() != "NESTED" || spans[3].Name() != "CHILD" {
		t.Fatalf("slow or nested detail missing: %s, %s, %s", spans[1].Name(), spans[2].Name(), spans[3].Name())
	}
}

func TestCompactServerTreeCountsLargeRepeatGroup(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer tp.Shutdown(context.Background())
	s, err := NewSpans(SpanConfig{TracerProvider: tp, CollapseFastRepeats: true})
	if err != nil {
		t.Fatal(err)
	}
	_, parent := tp.Tracer("application").Start(context.Background(), "client")
	defer parent.End()
	anchor := time.Date(2026, 10, 3, 8, 0, 0, 0, time.UTC)
	stamp := anchor.Format("2006-01-02T15:04:05.999999999")
	root := &serverNode{start: Event{Kind: "statement", Name: "SELECT T", Sequence: 1, Timestamp: stamp}, finish: Event{Kind: "statement", Sequence: 1, Timestamp: stamp}, complete: true}
	tree := &serverTree{scope: scope{parent: parent.SpanContext(), registered: anchor}, anchor: anchor, nodes: []*serverNode{root}}
	for i := uint64(2); i <= 366; i++ {
		start := Event{Kind: "procedure", Name: "LOOKUP", Sequence: i, ParentSequence: 1, Timestamp: stamp, Incomplete: true}
		tree.nodes = append(tree.nodes, &serverNode{start: start, finish: Event{Timestamp: stamp, Incomplete: true}, complete: true})
	}
	s.exportTree(tree)
	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want one summarized statement", len(spans))
	}
	attrs := attributeMap(spans[0].Attributes())
	if attrs["firebird.server.repeated_calls.collapsed"].AsInt64() != 365 {
		t.Fatal("lost repeat count", attrs)
	}
	if !attrs["firebird.incomplete"].AsBool() {
		t.Fatal("partial repeat detail lost its incomplete marker")
	}
	if attrs["firebird.incomplete.reasons"].AsString() != "source_event" || attrs["firebird.server.nodes.collected"].AsInt64() != 366 || attrs["firebird.server.nodes.exportable"].AsInt64() != 1 {
		t.Fatal("repeat trace does not explain its incompleteness", attrs)
	}
}

func TestPlanPartsFitCloudTraceAndPreserveTree(t *testing.T) {
	plan := "Select Expression\n" + strings.Repeat("    -> Table \"Договор\" Access By ID\n        -> Index \"FK_CONTRACT_DETAIL\" Range Scan (full match)\n", 20)
	parts := planParts(plan)
	if len(parts) < 2 || strings.Join(parts, "") != plan {
		t.Fatalf("plan not preserved across parts: %d parts", len(parts))
	}
	for i, part := range parts {
		if len(part) > 255 || !utf8.ValidString(part) {
			t.Fatalf("part %d exceeds Cloud Trace limit or splits UTF-8: %q", i, part)
		}
	}
}

func TestPlanPartsPreferCompleteSteps(t *testing.T) {
	plan := "Select Expression\n    -> First N Records\n        -> Filter\n            -> Table \"OBJ$CONTRACT_PERSONAL_DETAIL\" Access By ID\n                -> Index \"PK_OBJ$CONTRACT_PERSONAL_DETAIL\" Full Scan\n                    -> Bitmap\n                        -> Index \"FK_OBJ$CONTRACT_PERS_DETAIL_1\" Range Scan (full match)"
	parts := planParts(plan)
	if len(parts) != 2 || !strings.HasSuffix(parts[0], "-> Bitmap\n") || !strings.HasPrefix(parts[1], "                        -> Index ") || strings.Join(parts, "") != plan {
		t.Fatalf("record-source step split unnecessarily: %#v", parts)
	}
}

func attributeMap(attrs []attribute.KeyValue) map[string]attribute.Value {
	out := make(map[string]attribute.Value, len(attrs))
	for _, a := range attrs {
		out[string(a.Key)] = a.Value
	}
	return out
}
