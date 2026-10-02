package profiler

import (
	"strings"
	"testing"
)

func TestSafeAccessPathPreservesMultilineIndexPlan(t *testing.T) {
	path := "-> Table \"OBJ$CONTRACT_PERSONAL_DETAIL\" Access By ID\n    -> Index \"PK_OBJ$CONTRACT_PERSONAL_DETAIL\" Full Scan\n        -> Bitmap"
	want := "-> Table \"OBJ$CONTRACT_PERSONAL_DETAIL\" Access By ID -> Index \"PK_OBJ$CONTRACT_PERSONAL_DETAIL\" Full Scan -> Bitmap"
	if got := safeAccessPath(path); got != want {
		t.Fatalf("access path = %q, want %q", got, want)
	}
	if got := safeAccessPath("SQL 'sensitive'"); got != "" {
		t.Fatalf("unsafe access path exported: %q", got)
	}
	if got := safeAccessPath(strings.Repeat("A", 513)); got != "" {
		t.Fatalf("oversized access path exported")
	}
}
