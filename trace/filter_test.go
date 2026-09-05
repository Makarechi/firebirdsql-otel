package trace

import (
	"database/sql"
	"os"
	"strings"
	"testing"
)

func TestDatabaseFilterEncoding(t *testing.T) {
	for _, tt := range []struct{ path, want string }{
		{"/db/tenant_a+1.fdb", `"/db/tenant\_a\+1.fdb"`},
		{`C:\data\tenant_a.fdb`, `"C:\\data\\tenant\_a.fdb"`},
		{"/db/a {b}#c.fdb", `"/db/a \{{b\}}#c.fdb"`},
		{"/db/$(root).fdb", `"/db/$\(root\).fdb"`},
	} {
		got, err := databaseFilter(tt.path)
		if err != nil || got != tt.want {
			t.Fatalf("%q -> %q, %v, want %q", tt.path, got, err, tt.want)
		}
	}
	for _, path := range []string{"", "/db\nother", "/db\rfoo", "/db\x00foo", "/db\"foo", strings.Repeat("a", 1025)} {
		if _, err := databaseFilter(path); err == nil {
			t.Fatal("unsafe config accepted")
		}
	}
}
func TestFirebird5LiteralDatabasePatterns(t *testing.T) {
	dsn := os.Getenv("FIREBIRD_TEST_DSN")
	if dsn == "" {
		t.Skip("requires isolated Firebird")
	}
	db, err := sql.Open("firebirdsql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, tt := range []struct{ path, other string }{
		{"/db/tenant_a.fdb", "/db/tenantXa.fdb"},
		{"/db/tenant+a.fdb", "/db/tenantttta.fdb"},
		{`C:\data\tenant_a.fdb`, `C:data\tenant_a.fdb`},
		{"/db/a {b}#c.fdb", "/db/a b#c.fdb"},
		{"/db/[%](x)?-y.fdb", "/db/somethingxy.fdb"},
	} {
		encoded, err := databaseFilter(tt.path)
		if err != nil {
			t.Fatal(err)
		}
		pattern := strings.NewReplacer("{{", "{", "}}", "}").Replace(strings.Trim(encoded, `"`))
		for _, candidate := range []string{tt.path, tt.other} {
			var got int
			err := db.QueryRowContext(t.Context(), `SELECT CASE WHEN CAST(? AS VARCHAR(1024)) SIMILAR TO ? ESCAPE '\' THEN 1 ELSE 0 END FROM RDB$DATABASE`, candidate, pattern).Scan(&got)
			if err != nil {
				t.Fatal("Firebird rejected escaped pattern", err)
			}
			if (got == 1) != (candidate == tt.path) {
				t.Fatal("pattern crossed database boundary", candidate, pattern)
			}
		}
	}
}
