package main

import (
	"errors"
	collector "github.com/Makarechi/firebirdsql-otel/trace"
	"sync/atomic"
	"testing"
)

func TestWarmupRestartDrainsLateRecordsBeforeNewCounters(t *testing.T) {
	// Model a procedure finish already observed, followed by a late statement finish.
	late := make(chan struct{})
	drained := make(chan struct{})
	previous := &extras{}
	go func() { <-late; previous.bytes.Add(123); close(drained) }()
	previous.close = func() error { close(late); <-drained; return nil }
	current, err := restartTraceAfterWarmup(previous, func() (*extras, error) {
		if previous.bytes.Load() != 123 {
			t.Fatal("started measured stream before trailing warm-up record")
		}
		return &extras{}, nil
	})
	if err != nil || current.bytes.Load() != 0 {
		t.Fatal("warm-up telemetry leaked into measurement", err)
	}
}

func TestTraceMeasurementWaitsForProcedureAndOuterStatement(t *testing.T) {
	var procedures, statements atomic.Int64
	if !observeTraceCompletion(collector.Event{Kind: "procedure", Name: "OTEL_REPORT", Phase: "finish"}, &procedures, &statements) {
		t.Fatal("procedure completion was ignored")
	}
	if procedures.Load() != 1 || statements.Load() != 0 {
		t.Fatal("procedure completion also counted the outer statement")
	}
	if !observeTraceCompletion(collector.Event{Kind: "statement", Name: "SELECT OTEL_REPORT", Phase: "finish"}, &procedures, &statements) {
		t.Fatal("outer statement completion was ignored")
	}
	if procedures.Load() != 1 || statements.Load() != 1 {
		t.Fatal("measurement barrier did not observe both records")
	}
}
func TestWarmupRestartStopsOnDrainFailure(t *testing.T) {
	failure := errors.New("collector did not drain")
	_, err := restartTraceAfterWarmup(&extras{close: func() error { return failure }}, func() (*extras, error) { t.Fatal("started despite failed barrier"); return nil, nil })
	if err != failure {
		t.Fatal("drain failure hidden")
	}
}
