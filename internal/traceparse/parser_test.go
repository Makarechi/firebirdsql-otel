package traceparse

import (
	"fmt"
	"strings"
	"testing"
)

func record(kind, body string) string {
	return "2026-09-05T20:11:08.5030 (29:0x1) " + kind + " \n\t/db (ATT_16, SECRET_CANARY_USER, UTF8)\n\t(TRA_36, READ_WRITE)\n\n" + body + "\n\n"
}
func TestRealFormatAndPrivacy(t *testing.T) {
	p := New()
	text := record("EXECUTE_STATEMENT_START", "Statement 148:\n-----\nexecute procedure WORK('SECRET_CANARY_SQL')\n\nparam0 = text, SECRET_CANARY_ARG") + record("EXECUTE_PROCEDURE_START", "Procedure WORK:\n\nparam0 = SECRET_CANARY_ARG") + record("EXECUTE_PROCEDURE_FINISH", "Procedure WORK:\n  2 ms, 5 read(s), 20 fetch(es), 3 mark(s)\n\nTable                              Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge\n"+strings.Repeat("*", 112)+"\n"+fmt.Sprintf("%-32s%10s%10s%10s%10d%10s%10s%10s%10s", "MY_TABLE", "", "", "", 1, "", "", "", "")) + record("EXECUTE_STATEMENT_FINISH", "Statement 148:\n-----\nexecute procedure WORK('SECRET_CANARY_SQL')") + record("TRACE_FINI", "SESSION_1 test")
	var events []Event
	for i := 0; i < len(text); i += 7 {
		end := i + 7
		if end > len(text) {
			end = len(text)
		}
		events = append(events, p.Feed(text[i:end])...)
	}
	events = append(events, p.Flush()...)
	if strings.Contains(fmt.Sprint(events), "SECRET_CANARY") {
		t.Fatal("raw trace leaked", events)
	}
	if len(events) != 5 {
		t.Fatal(len(events), events)
	}
	start, proc, finish := events[0], events[1], events[2]
	if proc.ParentSequence != start.Sequence || finish.Sequence != proc.Sequence || finish.Incomplete {
		t.Fatal("pairing", events)
	}
	if finish.DurationMS != 2 || finish.Fetches != 20 || len(finish.Tables) != 1 || finish.Tables[0].Insert != 1 {
		t.Fatal("performance", finish)
	}
	if start.SQL != "EXECUTE PROCEDURE WORK ( ? )" {
		t.Fatal(start)
	}
}
func TestGapAndBounds(t *testing.T) {
	p := New()
	p.Feed(record("EXECUTE_PROCEDURE_START", "Procedure P:"))
	events := p.Feed(strings.Repeat("x", MaxRecord+1) + "\n")
	if len(events) != 1 || events[0].Kind != "gap" {
		t.Fatal(events)
	}
	events = p.Feed(record("EXECUTE_PROCEDURE_FINISH", "Procedure P:") + record("TRACE_FINI", ""))
	if len(events) == 0 || !events[0].Incomplete || events[0].Correlation != "unmatched" {
		t.Fatal(events)
	}
}
func TestRecursionAndTruncation(t *testing.T) {
	p := New()
	events := p.Feed(record("EXECUTE_PROCEDURE_START", "Procedure P:") + record("EXECUTE_PROCEDURE_START", "Procedure P:") + record("EXECUTE_PROCEDURE_FINISH", "Procedure P:") + record("EXECUTE_PROCEDURE_FINISH", "Procedure P:") + record("TRACE_FINI", ""))
	if events[2].Sequence != events[1].Sequence || events[3].Sequence != events[0].Sequence {
		t.Fatal(events)
	}
	p = New()
	events = p.Feed(record("EXECUTE_STATEMENT_START", "Statement 1:\n---\nselect 'SECRET...") + record("TRACE_FINI", ""))
	if events[0].SQL != "" || !events[0].Incomplete {
		t.Fatal(events)
	}
}
func FuzzTraceChunks(f *testing.F) {
	f.Add(record("EXECUTE_PROCEDURE_START", "Procedure P:"))
	f.Add("junk")
	f.Fuzz(func(t *testing.T, s string) {
		p := New()
		events := p.Feed(s)
		events = append(events, p.Flush()...)
		for _, e := range events {
			if len(e.SQL) > 4096 || len(e.Tables) > 64 || len(e.Name) > 300 {
				t.Fatal("unbounded")
			}
		}
	})
}
