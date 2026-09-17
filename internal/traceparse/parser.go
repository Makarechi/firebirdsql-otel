// Package traceparse parses a bounded subset of Firebird 5 text Trace.
// Only sanitized, typed records leave the parser. Unrecognized lines are discarded.
package traceparse

import (
	"github.com/Makarechi/firebirdsql-otel/internal/sqltext"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
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
	// ScopeToken is an opaque instrumentation marker, never SQL or a context value.
	ScopeToken                                        string
	Source, Correlation, Kind, Phase, Name, SQL, Plan string
	Timestamp                                         string
	AttachmentID, TransactionID, StatementID          int64
	Sequence, ParentSequence                          uint64
	DurationMS, Reads, Writes, Fetches, Marks         int64
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
	sqlSeparated       bool
	metadataHeader     bool
	planSection        bool
	performanceSection bool
	tableWidth         int
	recordBytes        int
	sequence           uint64
	stacks             map[[2]int64][]frame
	incomplete         bool
	stackOverflow      bool
}

var header = regexp.MustCompile(`^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+) \([^\r\n]{1,100}\) ([A-Z_ ]{1,80})$`)
var attachment = regexp.MustCompile(`^\t[^\r\n]+ \(ATT_([0-9]+), [^\r\n]*\)[ \t]*$`)
var transaction = regexp.MustCompile(`^\t[ \t]*\(TRA_([0-9]+), [^\r\n]*\)[ \t]*$`)
var scopeMarker = regexp.MustCompile(`^SELECT 1 FROM RDB\$DATABASE /\*firebirdotel_scope:([0-9a-f]{32})\*/$`)
var statement = regexp.MustCompile(`^Statement ([0-9]+):$`)
var parameterPrefix = regexp.MustCompile(`^param[0-9]+ = `)
var fetched = regexp.MustCompile(`^[0-9]+ records fetched$`)
var affected = regexp.MustCompile(`^[0-9]+ records affected$`)
var performanceLine = regexp.MustCompile(`^[0-9]+ ms(?:, [0-9]+ (?:read\(s\)|write\(s\)|fetch\(es\)|mark\(s\)))*$`)
var perf = regexp.MustCompile(`([0-9]+) (ms|read\(s\)|write\(s\)|fetch\(es\)|mark\(s\))`)

func New() *Parser { return &Parser{stacks: make(map[[2]int64][]frame)} }
func (p *Parser) Gap() Event {
	p.incomplete = true
	clear(p.stacks)
	p.current = nil
	p.sql.Reset()
	p.collectSQL = false
	p.sqlSeparated = false
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
		if e.Kind != "lifecycle" || e.Phase != "trace_fini" {
			e.Incomplete = true
		}
		out = append(out, *e)
	}
	if p.stackOverflow {
		p.stackOverflow = false
		out = append(out, p.Gap())
	}
	if len(p.stacks) > 0 {
		out = append(out, p.Gap())
	}
	return out
}

// FlushFinished releases TRACE_INIT or a finished execution during an idle
// stream, without interpreting an unterminated SQL body as a complete record.
// A trailing table section could still arrive, so execution counters are
// conservatively incomplete.
func (p *Parser) FlushFinished() []Event {
	if p.line != "" || p.current == nil || p.collectSQL {
		return nil
	}
	if p.current.Kind == "lifecycle" && p.current.Phase == "trace_init" {
		return []Event{*p.finish()}
	}
	if p.current.Phase != "finish" || !p.performanceSection {
		return nil
	}
	e := p.finish()
	if e == nil {
		return nil
	}
	e.Incomplete = true
	out := []Event{*e}
	if p.stackOverflow {
		p.stackOverflow = false
		out = append(out, p.Gap())
	}
	return out
}
func (p *Parser) consume(line string) []Event {
	if p.collectSQL {
		trim := strings.TrimSpace(line)
		complete := sqltext.LexicallyComplete(p.sql.String())
		mayEnd := complete && sqltext.StatementMayEnd(p.sql.String())
		performanceBoundary := mayEnd && performanceLine.MatchString(trim) && terminalPerformanceOperation(p.sql.String())
		metadataBoundary := mayEnd && (p.sqlSeparated && (parameterMetadata(line) || trim == "returns:") || fetched.MatchString(trim) || affected.MatchString(trim))
		boundary := header.MatchString(line) || strings.HasPrefix(trim, "^^^") || metadataBoundary || performanceBoundary
		if !boundary || !sqltext.LexicallyComplete(p.sql.String()) {
			p.recordBytes += len(line) + 1
			if p.recordBytes > MaxRecord || p.sql.Len()+len(line)+1 > MaxRecord {
				return []Event{p.Gap()}
			}
			p.sql.WriteString(line)
			p.sql.WriteByte('\n')
			p.sqlSeparated = trim == "" && complete
			return nil
		}
		p.collectSQL = false
		p.sqlSeparated = false
	}

	if m := header.FindStringSubmatch(line); m != nil {
		out := []Event{}
		if e := p.finish(); e != nil {
			out = append(out, *e)
		}
		if p.stackOverflow {
			p.stackOverflow = false
			out = append(out, p.Gap())
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
		p.sqlSeparated = false
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
		if d.Valid && d.Text != "" {
			if e.Plan == "" {
				e.Plan = d.Text
			} else if len(e.Plan)+1+len(d.Text) <= sqltext.MaxOutput {
				e.Plan += "\n" + d.Text
			} else {
				e.Incomplete = true
			}
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
		if table, ok := tableCounterRow(line, p.tableWidth); ok && len(e.Tables) < 64 {
			e.Tables = append(e.Tables, table)
		} else {
			e.Incomplete = true
		}
		return nil
	}
	if parameterMetadata(line) || trim == "returns:" || fetched.MatchString(trim) || affected.MatchString(trim) {
		p.collectSQL = false
		return nil
	}
	matches := perf.FindAllStringSubmatch(trim, -1)
	if performanceLine.MatchString(trim) {
		p.performanceSection = true
		for _, m := range matches {
			v, _ := strconv.ParseInt(m[1], 10, 64)
			switch strings.TrimSpace(m[2]) {
			case "ms":
				e.DurationMS = v
			case "read(s)":
				e.Reads = v
			case "write(s)":
				e.Writes = v
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

func tableCounterRow(line string, minimumNameWidth int) (Table, bool) {
	type candidate struct {
		nameLen int
		values  [8]int64
	}
	var candidates []candidate
	var parse func(int, int, [8]int64)
	parse = func(field, end int, values [8]int64) {
		if field < 0 {
			name := strings.TrimSpace(line[:end])
			if end >= minimumNameWidth && metadataName(name) {
				candidates = append(candidates, candidate{end, values})
			}
			return
		}
		if end < 10 {
			return
		}
		fixed := line[end-10 : end]
		trimmed := strings.TrimSpace(fixed)
		if trimmed == "" {
			parse(field-1, end-10, values)
		} else if n, err := strconv.ParseInt(trimmed, 10, 64); err == nil && strings.TrimLeft(fixed, " ") == trimmed {
			next := values
			next[field] = n
			parse(field-1, end-10, next)
		}
		start := end
		for start > 0 && line[start-1] >= '0' && line[start-1] <= '9' {
			start--
		}
		if end-start > 10 {
			if n, err := strconv.ParseInt(line[start:end], 10, 64); err == nil {
				next := values
				next[field] = n
				parse(field-1, start, next)
			}
		}
	}
	parse(7, len(line), [8]int64{})
	if len(candidates) == 0 {
		return Table{}, false
	}
	best := candidates[0]
	for _, c := range candidates[1:] {
		if c.nameLen < best.nameLen {
			best = c
		}
	}
	name := strings.TrimSpace(line[:best.nameLen])
	v := best.values
	return Table{Name: name, Natural: v[0], Index: v[1], Update: v[2], Insert: v[3], Delete: v[4], Backout: v[5], Purge: v[6], Expunge: v[7]}, true
}
func (p *Parser) finish() *Event {
	e := p.current
	if e == nil {
		return nil
	}
	p.current = nil
	if p.sql.Len() > 0 {
		raw := p.sql.String()
		if m := scopeMarker.FindStringSubmatch(strings.TrimSpace(raw)); e.Kind == "statement" && m != nil {
			e.ScopeToken = m[1]
		}
		if sqltext.HasTerminalEllipsis(raw) {
			e.Incomplete = true
		} else {
			d := sqltext.AnalyzeUnknownDialect(raw, 0, 0)
			e.SQL = d.Text
			if e.Kind == "statement" {
				e.Name = d.Summary
			}
			if !d.Valid || strings.TrimSpace(raw) != "" && d.Text == "" {
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
	if e.Kind == "statement" && e.StatementID == 0 {
		e.Incomplete = true
		e.Correlation = "unmatched"
		p.incomplete = true
		delete(p.stacks, key)
		return e
	}
	if e.Phase == "start" {
		if len(p.stacks) >= MaxScopes && len(stack) == 0 || len(stack) >= MaxDepth {
			p.incomplete = true
			clear(p.stacks)
			p.stackOverflow = true
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
		if last.kind == e.Kind && last.name == e.Name && last.statement == e.StatementID {
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

func terminalPerformanceOperation(raw string) bool {
	switch sqltext.LeadingOperation(raw) {
	case "CREATE", "ALTER", "DROP", "RECREATE", "DECLARE", "GRANT", "REVOKE", "COMMENT", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "SET", "EXECUTE BLOCK", "EXECUTE PROCEDURE":
		return true
	default:
		return false
	}
}

func parameterMetadata(line string) bool {
	loc := parameterPrefix.FindStringIndex(line)
	if loc == nil || loc[0] != 0 {
		return false
	}
	rest := line[loc[1]:]
	depth := 0
	separator := -1
	for i, r := range rest {
		switch r {
		case '(':
			depth++
		case ')':
			if depth == 0 {
				return false
			}
			depth--
		case ',':
			if depth == 0 {
				separator = i
			}
		}
		if separator >= 0 {
			break
		}
	}
	typeName := strings.TrimSpace(rest[:max(separator, 0)])
	value := ""
	if separator >= 0 {
		value = strings.TrimSpace(rest[separator+1:])
	}
	if separator < 0 || typeName == "" || len(typeName) > 128 || value == "" || depth != 0 {
		return false
	}
	for _, r := range typeName {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && !unicode.IsSpace(r) && !strings.ContainsRune("_(),", r) {
			return false
		}
	}
	return true
}

func metadataName(s string) bool {
	if s == "" || len(s) > 256 || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
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
