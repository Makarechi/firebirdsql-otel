package sqltext

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// LexicallyComplete tracks open lexical constructs independently of sanitizer
// token, nesting and syntax limits. It never builds or exports SQL tokens.
func LexicallyComplete(s string) bool {
	if len(s) > MaxInput || !utf8.ValidString(s) {
		return false
	}
	var delimiters []byte // bounded by the input byte limit
	for i := 0; i < len(s); {
		if strings.HasPrefix(s[i:], "--") {
			n := strings.IndexByte(s[i:], '\n')
			if n < 0 {
				break
			}
			i += n + 1
			continue
		}
		if strings.HasPrefix(s[i:], "/*") {
			i += 2
			depth := 1
			for i < len(s) && depth > 0 {
				if strings.HasPrefix(s[i:], "/*") {
					depth++
					i += 2
				} else if strings.HasPrefix(s[i:], "*/") {
					depth--
					i += 2
				} else {
					i++
				}
			}
			if depth != 0 {
				return false
			}
			continue
		}
		if (s[i] == 'q' || s[i] == 'Q') && i+2 < len(s) && s[i+1] == '\'' {
			delim, size := utf8.DecodeRuneInString(s[i+2:])
			end := delim
			switch delim {
			case '[':
				end = ']'
			case '{':
				end = '}'
			case '(':
				end = ')'
			case '<':
				end = '>'
			}
			start := i + 2 + size
			n := strings.Index(s[start:], string(end)+"'")
			if n < 0 {
				return false
			}
			i = start + n + len(string(end)) + 1
			continue
		}
		if s[i] == '\'' || s[i] == '"' {
			quote := s[i]
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
				return false
			}
			continue
		}
		if s[i] == '(' || s[i] == '[' {
			delimiters = append(delimiters, s[i])
			i++
			continue
		}
		if s[i] == ')' || s[i] == ']' {
			if len(delimiters) == 0 {
				return false
			}
			open := delimiters[len(delimiters)-1]
			if s[i] == ')' && open != '(' || s[i] == ']' && open != '[' {
				return false
			}
			delimiters = delimiters[:len(delimiters)-1]
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		i += size
		if unicode.IsLetter(r) || r == '_' {
			for i < len(s) {
				r, size = utf8.DecodeRuneInString(s[i:])
				if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != '$' {
					break
				}
				i += size
			}
		}
	}
	return len(delimiters) == 0
}

// StatementMayEnd rejects prefixes whose final token requires more SQL. It is
// intentionally conservative and is only used to distinguish Trace metadata
// from legal SQL lines after the statement text.
func StatementMayEnd(s string) bool {
	if !LexicallyComplete(s) {
		return false
	}
	last := ""
	for i := 0; i < len(s); {
		if strings.HasPrefix(s[i:], "--") {
			n := strings.IndexByte(s[i:], '\n')
			if n < 0 {
				break
			}
			i += n + 1
			continue
		}
		if strings.HasPrefix(s[i:], "/*") {
			i += 2
			depth := 1
			for depth > 0 {
				switch {
				case strings.HasPrefix(s[i:], "/*"):
					depth++
					i += 2
				case strings.HasPrefix(s[i:], "*/"):
					depth--
					i += 2
				default:
					i++
				}
			}
			continue
		}
		if (s[i] == 'q' || s[i] == 'Q') && i+2 < len(s) && s[i+1] == '\'' {
			delim, size := utf8.DecodeRuneInString(s[i+2:])
			end := delim
			switch delim {
			case '[':
				end = ']'
			case '{':
				end = '}'
			case '(':
				end = ')'
			case '<':
				end = '>'
			}
			start := i + 2 + size
			n := strings.Index(s[start:], string(end)+"'")
			i = start + n + len(string(end)) + 1
			last = "?"
			continue
		}
		if s[i] == '\'' || s[i] == '"' {
			quote := s[i]
			i++
			for i < len(s) {
				if s[i] == quote {
					i++
					if i < len(s) && s[i] == quote {
						i++
						continue
					}
					break
				}
				i++
			}
			last = "?"
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if unicode.IsSpace(r) {
			i += size
			continue
		}
		if unicode.IsLetter(r) || r == '_' {
			start := i
			i += size
			for i < len(s) {
				r, size = utf8.DecodeRuneInString(s[i:])
				if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != '$' {
					break
				}
				i += size
			}
			last = strings.ToUpper(s[start:i])
			continue
		}
		last = string(r)
		i += size
	}
	switch last {
	case "SELECT", "FROM", "WHERE", "JOIN", "ON", "AS", "BY", "HAVING", "ORDER", "GROUP", "UNION", "ALL", "DISTINCT", "INTERSECT", "EXCEPT", "INTO", "VALUES", "SET", "RETURNING", "WHEN", "THEN", "ELSE", "AND", "OR", "NOT", "=", "<", ">", "!", "^", "~", "+", "-", "*", "/", "|", ":", ",", ".", "(", "[":
		return false
	default:
		return true
	}
}
