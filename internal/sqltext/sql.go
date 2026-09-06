// Package sqltext implements bounded, fail-closed Firebird SQL descriptions.
// It never changes the statement submitted to the database.
package sqltext

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode"
	"unicode/utf8"
)

const MaxInput = 65536
const MaxOutput = 4096

type Description struct {
	Text, Operation, Summary, Procedure, Collection, Fingerprint string
	Valid                                                        bool
}
type token struct {
	text       string
	identifier bool
}

// Analyze describes SQL submitted using client dialect 3. Double quotes denote identifiers.
func Analyze(sql string, inputLimit, outputLimit int) Description {
	return analyze(sql, inputLimit, outputLimit, true)
}

// AnalyzeUnknownDialect omits the entire description when double-quoted tokens
// could be literals. Trace can contain SQL from other clients whose dialect is unknown.
func AnalyzeUnknownDialect(sql string, inputLimit, outputLimit int) Description {
	return analyze(sql, inputLimit, outputLimit, false)
}

func analyze(sql string, inputLimit, outputLimit int, quotedIdentifiers bool) Description {
	d := Description{Operation: "SQL", Summary: "SQL"}
	if inputLimit <= 0 || inputLimit > MaxInput {
		inputLimit = MaxInput
	}
	if outputLimit <= 0 || outputLimit > MaxOutput {
		outputLimit = MaxOutput
	}
	if len(sql) > inputLimit || !utf8.ValidString(sql) {
		return d
	}
	ts, ok := lex(sql)
	if !ok || len(ts) == 0 {
		return d
	}
	if !quotedIdentifiers {
		for _, t := range ts {
			if t.identifier && strings.HasPrefix(t.text, `"`) {
				return d
			}
		}
	}
	parts := make([]string, len(ts))
	for i, t := range ts {
		parts[i] = t.text
	}
	normalized := strings.Join(parts, " ")
	sum := sha256.Sum256([]byte(normalized))
	d.Fingerprint = hex.EncodeToString(sum[:16])
	d.Valid = true
	// Omit, rather than cut in the middle of a quoted identifier or UTF-8 rune.
	if len(normalized) <= outputLimit {
		d.Text = normalized
	}
	start := 0
	if ts[0].text == "WITH" {
		depth := 0
		start = -1
		for i, t := range ts {
			if t.text == "(" {
				depth++
			}
			if t.text == ")" {
				depth--
			}
			if i > 0 && depth == 0 && isOperation(t.text) {
				start = i
				break
			}
		}
		if start < 0 {
			return d
		}
	}
	op := ts[start].text
	if !isOperation(op) {
		return d
	}
	d.Operation = op
	target := -1
	switch op {
	case "EXECUTE":
		if start+1 < len(ts) && (ts[start+1].text == "PROCEDURE" || ts[start+1].text == "BLOCK") {
			d.Operation += " " + ts[start+1].text
			if ts[start+1].text == "PROCEDURE" {
				target = start + 2
			}
		}
	case "SELECT", "DELETE":
		depth := 0
		for i := start + 1; i < len(ts); i++ {
			if ts[i].text == "(" {
				depth++
			}
			if ts[i].text == ")" {
				depth--
			}
			if depth == 0 && ts[i].text == "FROM" {
				target = i + 1
				break
			}
		}
	case "UPDATE":
		target = start + 1
		if start+3 < len(ts) && ts[start+1].text == "OR" && ts[start+2].text == "INSERT" {
			d.Operation = "UPDATE OR INSERT"
			target = start + 3
			if ts[target].text == "INTO" {
				target++
			}
		}
	case "INSERT", "MERGE":
		if start+1 < len(ts) && ts[start+1].text == "INTO" {
			target = start + 2
		}
	}
	if op == "SELECT" && target >= 0 && target+1 < len(ts) && ts[target].text == "LATERAL" && ts[target+1].text == "(" {
		target = -1 // A derived table is not a selectable procedure.
	}
	name, end := objectName(ts, target)
	d.Summary = d.Operation
	if name != "" {
		d.Summary += " " + name
		if d.Operation == "EXECUTE PROCEDURE" || (op == "SELECT" && end < len(ts) && ts[end].text == "(") {
			d.Procedure = name
		}
		// SELECT may reference multiple tables/CTEs/views: never claim one collection.
		if op == "UPDATE" || op == "INSERT" || op == "DELETE" || op == "MERGE" {
			d.Collection = name
		}
	}
	return d
}
func isOperation(s string) bool {
	switch s {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "EXECUTE", "CREATE", "ALTER", "DROP", "RECREATE", "GRANT", "REVOKE", "COMMENT", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "SET":
		return true
	}
	return false
}
func objectName(ts []token, i int) (string, int) {
	if i < 0 || i >= len(ts) || !ts[i].identifier {
		return "", i
	}
	s := ts[i].text
	i++
	for i+1 < len(ts) && ts[i].text == "." && ts[i+1].identifier {
		s += "." + ts[i+1].text
		i += 2
	}
	if len(s) > 256 {
		return "", i
	}
	return s, i
}

// Identifier validates an explicit application hint; it is not a SQL fragment.
func Identifier(s string) bool {
	if len(s) == 0 || len(s) > 256 || !utf8.ValidString(s) {
		return false
	}
	ts, ok := lex(s)
	if !ok {
		return false
	}
	n, end := objectName(ts, 0)
	return n != "" && end == len(ts) && len(s) <= 256 && strings.EqualFold(s, n)
}
func lex(s string) ([]token, bool) {
	out := make([]token, 0, 32)
	add := func(v string, id bool) { out = append(out, token{v, id}) }
	delimiters := make([]rune, 0, 8)
	for i := 0; i < len(s); {
		if len(out) >= 4096 {
			return nil, false
		}
		r, n := utf8.DecodeRuneInString(s[i:])
		if unicode.IsSpace(r) {
			i += n
			continue
		}
		if strings.HasPrefix(s[i:], "--") {
			j := strings.IndexByte(s[i:], '\n')
			if j < 0 {
				break
			}
			i += j + 1
			continue
		}
		if strings.HasPrefix(s[i:], "/*") {
			i += 2
			level := 1
			for i < len(s) && level > 0 {
				if strings.HasPrefix(s[i:], "/*") {
					level++
					i += 2
				} else if strings.HasPrefix(s[i:], "*/") {
					level--
					i += 2
				} else {
					i++
				}
			}
			if level != 0 {
				return nil, false
			}
			continue
		}
		if (r == 'q' || r == 'Q') && i+2 < len(s) && s[i+1] == '\'' {
			delim, sz := utf8.DecodeRuneInString(s[i+2:])
			if unicode.IsSpace(delim) || delim == '\'' {
				return nil, false
			}
			close := delim
			switch delim {
			case '{':
				close = '}'
			case '[':
				close = ']'
			case '(':
				close = ')'
			case '<':
				close = '>'
			}
			end := strings.Index(s[i+2+sz:], string(close)+"'")
			if end < 0 {
				return nil, false
			}
			i += 2 + sz + end + len(string(close)) + 1
			add("?", false)
			continue
		}
		if r == '\'' || r == '"' || ((r == 'x' || r == 'X') && i+1 < len(s) && s[i+1] == '\'') {
			start := i
			quote := byte(r)
			if r == 'x' || r == 'X' {
				i++
				quote = '\''
			}
			i++
			closed := false
			for i < len(s) {
				if s[i] == quote {
					i++
					if i < len(s) && s[i] == quote {
						i++
						continue
					}
					closed = true
					break
				}
				i++
			}
			if !closed {
				return nil, false
			}
			if quote == '"' {
				add(s[start:i], true)
			} else {
				add("?", false)
			}
			continue
		}
		if r >= '0' && r <= '9' || r == '.' && i+1 < len(s) && s[i+1] >= '0' && s[i+1] <= '9' {
			if i+2 < len(s) && s[i] == '0' && (s[i+1] == 'x' || s[i+1] == 'X') {
				i += 2
				start := i
				for i < len(s) && strings.ContainsRune("0123456789abcdefABCDEF", rune(s[i])) {
					i++
				}
				if i == start {
					return nil, false
				}
			} else {
				for i < len(s) && s[i] >= '0' && s[i] <= '9' {
					i++
				}
				if i < len(s) && s[i] == '.' {
					i++
					for i < len(s) && s[i] >= '0' && s[i] <= '9' {
						i++
					}
				}
				if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
					i++
					if i < len(s) && (s[i] == '+' || s[i] == '-') {
						i++
					}
					start := i
					for i < len(s) && s[i] >= '0' && s[i] <= '9' {
						i++
					}
					if start == i {
						return nil, false
					}
				}
			}
			if i < len(s) {
				rr, _ := utf8.DecodeRuneInString(s[i:])
				if unicode.IsLetter(rr) || rr == '_' {
					return nil, false
				}
			}
			add("?", false)
			continue
		}
		if unicode.IsLetter(r) || r == '_' {
			start := i
			i += n
			for i < len(s) {
				rr, sz := utf8.DecodeRuneInString(s[i:])
				if !unicode.IsLetter(rr) && !unicode.IsDigit(rr) && rr != '_' && rr != '$' {
					break
				}
				i += sz
			}
			word := strings.ToUpper(s[start:i])
			if word == "NULL" || word == "TRUE" || word == "FALSE" || word == "UNKNOWN" {
				add("?", false)
			} else {
				add(word, true)
			}
			continue
		}
		if strings.ContainsRune("()[],.;?=<>!^~+-*/|:%", r) {
			if r == '(' || r == '[' {
				delimiters = append(delimiters, r)
				if len(delimiters) > 128 {
					return nil, false
				}
			}
			if r == ')' || r == ']' {
				if len(delimiters) == 0 {
					return nil, false
				}
				opening := delimiters[len(delimiters)-1]
				if r == ')' && opening != '(' || r == ']' && opening != '[' {
					return nil, false
				}
				delimiters = delimiters[:len(delimiters)-1]
			}
			add(string(r), false)
			i += n
			continue
		}
		return nil, false
	}
	return out, len(delimiters) == 0
}
