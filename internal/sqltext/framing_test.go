package sqltext

import (
	"strings"
	"testing"
)

func TestFramingIndependentOfSanitizerLimits(t *testing.T) {
	for _, sql := range []string{"SELECT " + strings.Repeat("1+", 2200) + "1", "SELECT " + strings.Repeat("(", 129) + "1" + strings.Repeat(")", 129), "SELECT @unsupported"} {
		if Analyze(sql, 0, 0).Valid {
			t.Fatal("fixture must exceed sanitizer support")
		}
		if !LexicallyComplete(sql) {
			t.Fatal("complete input held by sanitizer failure")
		}
		if LexicallyComplete(sql+" /* unclosed") || LexicallyComplete(sql+" 'unclosed") {
			t.Fatal("open secret construct accepted")
		}
	}
	for _, sql := range []string{`SELECT q'{hello '' secret}'`, `SELECT q'[hello]'`, `SELECT "quoted""identifier"`, "SELECT /* outer /* nested */ done */ 1"} {
		if !LexicallyComplete(sql) {
			t.Fatal("closed construct rejected", sql)
		}
	}
}
