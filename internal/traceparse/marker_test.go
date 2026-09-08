package traceparse

import (
	"strings"
	"testing"
)

func TestSQLMarkerAndNativeTrailingSpaces(t *testing.T) {
	token := strings.Repeat("a", 32)
	marker := "SELECT 1 FROM RDB$DATABASE /*firebirdotel_scope:" + token + "*/"
	for _, sql := range []string{marker, marker + " EXTRA", "SELECT '" + marker + "' FROM RDB$DATABASE", "SELECT 1 FROM RDB$DATABASE /*firebirdotel_scope:SECRET_CANARY*/"} {
		wire := record("EXECUTE_STATEMENT_START", "Statement 1:\n---\n"+sql) + record("TRACE_FINI", "")
		wire = strings.ReplaceAll(wire, "\n", " \n")
		p := New()
		events := p.Feed(wire)
		if len(events) != 1 || events[0].AttachmentID != 16 || events[0].TransactionID != 36 {
			t.Fatal("native header scope lost", events)
		}
		want := ""
		if sql == marker {
			want = token
		}
		if events[0].ScopeToken != want || strings.Contains(events[0].SQL, token) || strings.Contains(events[0].SQL, "SECRET_CANARY") {
			t.Fatal("invalid marker or SQL forwarding", events)
		}
	}
}

func TestIdleFlushRequiresFinishedPerformanceRecord(t *testing.T) {
	p := New()
	p.Feed(record("EXECUTE_PROCEDURE_START", "Procedure P:"))
	if len(p.FlushFinished()) != 0 {
		t.Fatal("idle start was finalized")
	}
	p.Feed(record("EXECUTE_PROCEDURE_FINISH", "Procedure P:\n2 ms"))
	events := p.FlushFinished()
	if len(events) != 1 || events[0].Phase != "finish" || events[0].Sequence == 0 || !events[0].Incomplete {
		t.Fatal("missing conservative idle finish", events)
	}
	p = New()
	p.Feed(record("EXECUTE_STATEMENT_FINISH", "Statement 1:\n---\nSELECT '\n2 ms\nFROM RDB$DATABASE"))
	if len(p.FlushFinished()) != 0 {
		t.Fatal("unfinished SQL was flushed as performance")
	}
}
