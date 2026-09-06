// Package traceparse parses a bounded subset of Firebird 5 text Trace.
// Only sanitized, typed records leave the parser. Unrecognized lines are discarded.
package traceparse

import (
	"github.com/Makarechi/firebirdsql-otel/internal/sqltext"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const MaxRecord = 65536

// MaxSQL leaves 16 KiB of the record envelope for native metadata, plan and counters.
const MaxSQL = 48 * 1024
const MaxScopes = 64
const MaxDepth = 64

type Table struct {
	Name                                                            string
	Natural, Index, Update, Insert, Delete, Backout, Purge, Expunge int64
}
type Event struct {
	Source, Correlation, Kind, Phase, Name, SQL, Plan string
	Timestamp                                         string
	AttachmentID, TransactionID, StatementID          int64
	Sequence, ParentSequence                          uint64
	DurationMS, Reads, Fetches, Marks                 int64
	Tables                                            []Table
	Incomplete                                        bool
}
type frame struct {
	kind, name string
	sequence   uint64
	statement  int64
}
type Parser struct {
	line               string
	skipLine           bool
	current            *Event
	sql                strings.Builder
	collectSQL         bool
	metadataHeader     bool
	planSection        bool
	performanceSection bool
	tableWidth         int
	recordBytes        int
	sequence           uint64
	stacks             map[[2]int64][]frame
	incomplete         bool
}

var header = regexp.MustCompile(`^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+) \([^\r\n]{1,100}\) ([A-Z_ ]{1,80})$`)
var attachment = regexp.MustCompile(`^\t[^\r\n]+ \(ATT_([0-9]+), [^\r\n]*\)$`)
var transaction = regexp.MustCompile(`^\t[ \t]*\(TRA_([0-9]+), [^\r\n]*\)$`)
var statement = regexp.MustCompile(`^Statement ([0-9]+):$`)
var parameter = regexp.MustCompile(`^param[0-9]+ = [^,\r\n]+, "`)
var fetched = regexp.MustCompile(`^[0-9]+ records fetched$`)
var perf = regexp.MustCompile(`([0-9]+) (ms|read\(s\)|fetch\(es\)|mark\(s\))`)

func New() *Parser { return &Parser{stacks: make(map[[2]int64][]frame)} }
func (p *Parser) Gap() Event {
	p.incomplete = true
	clear(p.stacks)
	p.current = nil
	p.sql.Reset()
	p.collectSQL = false
	return Event{Source: "trace", Correlation: "unmatched", Kind: "gap", Incomplete: true}
}

// Feed accepts arbitrary wire chunks; incomplete lines/records are capped at MaxRecord.
func (p *Parser) Feed(chunk string) []Event {
	out := []Event{}
	for len(chunk) > 0 {
		i := strings.IndexByte(chunk, '\n')
		piece := chunk
		if i >= 0 {
			piece = chunk[:i]
		}
		if !p.skipLine {
			if len(p.line)+len(piece) > MaxRecord {
				out = append(out, p.Gap())
				p.line = ""
				p.skipLine = true
			} else {
				p.line += piece
			}
		}
		if i < 0 {
			break
		}
		if !p.skipLine {
			out = append(out, p.consume(strings.TrimSuffix(p.line, "\r"))...)
		}
		p.line = ""
		p.skipLine = false
		chunk = chunk[i+1:]
	}
	return out
}
func (p *Parser) Flush() []Event {
	out := []Event{}
	if p.line != "" && !p.skipLine {
		out = append(out, p.consume(p.line)...)
	}
	p.line = ""
	if e := p.finish(); e != nil {
		e.Incomplete = true
		out = append(out, *e)
	}
	if len(p.stacks) > 0 {
		out = append(out, p.Gap())
	}
	return out
}
func (p *Parser) consume(line string) []Event {
	if p.collectSQL {
		trim := strings.TrimSpace(line)
		boundary := header.MatchString(line) || strings.HasPrefix(trim, "^^^") || parameter.MatchString(line) || fetched.MatchString(trim) || (len(trim) > 0 && trim[0] >= '0' && trim[0] <= '9' && perf.MatchString(trim))
		if !boundary || !sqltext.LexicallyComplete(p.sql.String()) {
			p.recordBytes += len(line) + 1
			if p.recordBytes > MaxRecord || p.sql.Len()+len(line)+1 > MaxRecord {
				return []Event{p.Gap()}
			}
			p.sql.WriteString(line)
			p.sql.WriteByte('\n')
			return nil
		}
		p.collectSQL = false
	}

	if m := header.FindStringSubmatch(line); m != nil {
		out := []Event{}
		if e := p.finish(); e != nil {
			out = append(out, *e)
		}
		kind, phase := "", ""
		switch strings.TrimSpace(m[2]) {
		case "EXECUTE_STATEMENT_START":
			kind, phase = "statement", "start"
		case "EXECUTE_STATEMENT_FINISH":
			kind, phase = "statement", "finish"
		case "EXECUTE_PROCEDURE_START":
			kind, phase = "procedure", "start"
		case "EXECUTE_PROCEDURE_FINISH":
			kind, phase = "procedure", "finish"
		case "EXECUTE_FUNCTION_START":
			kind, phase = "function", "start"
		case "EXECUTE_FUNCTION_FINISH":
			kind, phase = "function", "finish"
		case "EXECUTE_TRIGGER_START":
			kind, phase = "trigger", "start"
		case "EXECUTE_TRIGGER_FINISH":
			kind, phase = "trigger", "finish"
		case "TRACE_INIT", "TRACE_FINI":
			kind, phase = "lifecycle", strings.ToLower(strings.TrimSpace(m[2]))
		default:
			e := p.Gap()
			e.Name = "unsupported_event"
			out = append(out, e)
			return out
		}
		_, err := time.Parse("2006-01-02T15:04:05.999999999", m[1])
		if err != nil {
			e := p.Gap()
			e.Name = "invalid_timestamp"
			out = append(out, e)
			return out
		}
		p.current = &Event{Source: "trace", Correlation: "heuristic", Kind: kind, Phase: phase, Timestamp: m[1], Incomplete: p.incomplete}
		p.recordBytes = 0
		p.tableWidth = 0
		p.metadataHeader = true
		p.planSection = false
		p.performanceSection = false
		return out
	}
	if p.current == nil {
		return nil
	}
	p.recordBytes += len(line) + 1
	if p.recordBytes > MaxRecord {
		return []Event{p.Gap()}
	}
	trim := strings.TrimSpace(line)
	e := p.current
	if m := attachment.FindStringSubmatch(line); p.metadataHeader && !p.collectSQL && e.AttachmentID == 0 && m != nil {
		e.AttachmentID, _ = strconv.ParseInt(m[1], 10, 64)
		return nil
	}
	if m := transaction.FindStringSubmatch(line); p.metadataHeader && !p.collectSQL && e.AttachmentID != 0 && e.TransactionID == 0 && m != nil {
		e.TransactionID, _ = strconv.ParseInt(m[1], 10, 64)
		return nil
	}
	if m := statement.FindStringSubmatch(trim); p.metadataHeader && e.Kind == "statement" && m != nil {
		p.metadataHeader = false
		e.StatementID, _ = strconv.ParseInt(m[1], 10, 64)
		return nil
	}
	for _, label := range []string{"Procedure ", "Function ", "Trigger "} {
		if p.metadataHeader && strings.HasPrefix(trim, label) && strings.EqualFold(strings.TrimSpace(label), e.Kind) {
			p.metadataHeader = false
			name := strings.TrimSuffix(strings.TrimPrefix(trim, label), ":")
			if label == "Trigger " {
				name = triggerName(name)
			}
			if sqltext.Identifier(name) {
				e.Name = name
			} else {
				e.Incomplete = true
			}
			p.collectSQL = false
			return nil
		}
	}
	if strings.HasPrefix(trim, "---") {
		p.collectSQL = true
		p.metadataHeader = false
		return nil
	}
	if strings.HasPrefix(trim, "^^^") {
		p.planSection = true
		return nil
	}
	if trim == "" {
		p.collectSQL = false
		return nil
	}
	if p.planSection && strings.HasPrefix(trim, "PLAN ") {
		d := sqltext.AnalyzeUnknownDialect(trim, 0, 0)
		if d.Valid {
			e.Plan = d.Text
		} else {
			e.Incomplete = true
		}
		p.collectSQL = false
		return nil
	}
	if p.performanceSection && strings.HasPrefix(line, "Table") && strings.Contains(line, "   Natural     Index") {
		p.tableWidth = strings.Index(line, "   Natural")
		p.collectSQL = false
		return nil
	}
	if p.tableWidth >= 32 && len(line) >= p.tableWidth+80 && !strings.HasPrefix(line, "***") {
		name := strings.TrimSpace(line[:p.tableWidth])
		if sqltext.Identifier(name) {
			table := Table{Name: name}
			values := []*int64{&table.Natural, &table.Index, &table.Update, &table.Insert, &table.Delete, &table.Backout, &table.Purge, &table.Expunge}
			valid := true
			for i, v := range values {
				s := strings.TrimSpace(line[p.tableWidth+i*10 : p.tableWidth+(i+1)*10])
				if s != "" {
					n, err := strconv.ParseInt(s, 10, 64)
					if err != nil {
						valid = false
						break
					}
					*v = n
				}
			}
			if valid && len(e.Tables) < 64 {
				e.Tables = append(e.Tables, table)
			} else {
				e.Incomplete = true
			}
		}
		return nil
	}
	if parameter.MatchString(line) || trim == "returns:" || fetched.MatchString(trim) {
		p.collectSQL = false
		return nil
	}
	matches := perf.FindAllStringSubmatch(trim, -1)
	if len(matches) > 0 && trim[0] >= '0' && trim[0] <= '9' {
		p.performanceSection = true
		for _, m := range matches {
			v, _ := strconv.ParseInt(m[1], 10, 64)
			switch strings.TrimSpace(m[2]) {
			case "ms":
				e.DurationMS = v
			case "read(s)":
				e.Reads = v
			case "fetch(es)":
				e.Fetches = v
			case "mark(s)":
				e.Marks = v
			}
		}
		p.collectSQL = false
		return nil
	}
	if p.collectSQL {
		if p.sql.Len()+len(line)+1 > MaxRecord {
			return []Event{p.Gap()}
		}
		p.sql.WriteString(line)
		p.sql.WriteByte('\n')
	}
	return nil
}
func (p *Parser) finish() *Event {
	e := p.current
	if e == nil {
		return nil
	}
	p.current = nil
	if p.sql.Len() > 0 {
		raw := p.sql.String()
		if sqltext.HasTerminalEllipsis(raw) {
			e.Incomplete = true
		} else {
			d := sqltext.AnalyzeUnknownDialect(raw, 0, 0)
			e.SQL = d.Text
			if e.Kind == "statement" {
				e.Name = d.Summary
			}
			if !d.Valid {
				e.Incomplete = true
			}
		}
	}
	p.sql.Reset()
	p.collectSQL = false
	key := [2]int64{e.AttachmentID, e.TransactionID}
	stack := p.stacks[key]
	if e.Kind == "lifecycle" {
		e.Correlation = "unmatched"
		return e
	}
	if e.AttachmentID == 0 || e.TransactionID == 0 {
		e.Incomplete = true
		e.Correlation = "unmatched"
		return e
	}
	if e.Phase == "start" {
		if len(p.stacks) >= MaxScopes && len(stack) == 0 || len(stack) >= MaxDepth {
			p.incomplete = true
			clear(p.stacks)
			stack = nil
			e.Incomplete = true
		}
		p.sequence++
		e.Sequence = p.sequence
		if len(stack) > 0 {
			e.ParentSequence = stack[len(stack)-1].sequence
		}
		p.stacks[key] = append(stack, frame{e.Kind, e.Name, e.Sequence, e.StatementID})
	} else if len(stack) > 0 {
		last := stack[len(stack)-1]
		if last.kind == e.Kind && last.name == e.Name && (e.StatementID == 0 || last.statement == e.StatementID) {
			e.Sequence = last.sequence
			stack = stack[:len(stack)-1]
			if len(stack) > 0 {
				e.ParentSequence = stack[len(stack)-1].sequence
				p.stacks[key] = stack
			} else {
				delete(p.stacks, key)
			}
		} else {
			e.Incomplete = true
			e.Correlation = "unmatched"
			p.incomplete = true
			delete(p.stacks, key)
		}
	} else {
		e.Incomplete = true
		e.Correlation = "unmatched"
		p.incomplete = true
	}
	e.Incomplete = e.Incomplete || p.incomplete
	return e
}

// The relation separator is syntax only outside a quoted trigger identifier.
func triggerName(s string) string {
	quoted := false
	for i := 0; i < len(s); i++ {
		if s[i] == '"' {
			if quoted && i+1 < len(s) && s[i+1] == '"' {
				i++
				continue
			}
			quoted = !quoted
		} else if !quoted && strings.HasPrefix(s[i:], " FOR ") {
			return s[:i]
		}
	}
	return s
}
