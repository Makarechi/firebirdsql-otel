package firebirdotel

import (
	"context"
	"github.com/XSAM/otelsql"
	"go.opentelemetry.io/otel/attribute"
	"testing"
)

func TestCompatibilityAliases(t *testing.T) {
	var opts []Option = []otelsql.Option{otelsql.WithAttributes(attribute.String("x", "y"))}
	if len(opts) != 1 {
		t.Fatal("alias")
	}
	var _ SpanOptions = otelsql.SpanOptions{}
	var _ SpanNameFormatter = otelsql.SpanNameFormatter(nil)
}
func TestConfigValidation(t *testing.T) {
	for _, c := range []Config{{Profile: "bad"}, {SQL: SQLPolicy{Mode: "bad"}}, {OTelOptions: []Option{WithSQLCommenter(true)}}, {SQL: SQLPolicy{MaxInputBytes: 65537}}, {Connection: ConnectionAttributes{Port: 65536}}} {
		if _, err := normalizeConfig(c); err == nil {
			t.Fatalf("accepted %+v", c)
		}
	}
	if _, err := normalizeConfig(Config{}); err != nil {
		t.Fatal(err)
	}
}
func TestDSNNetwork(t *testing.T) {
	for _, tt := range []struct {
		dsn, host string
		port      int
		ok        bool
	}{
		{"user:pass@localhost/alias", "localhost", 3050, true},
		{"firebird://user:p%40ss@host:3051/C:/data/my%20db.fdb?charset=UTF8", "host", 3051, true},
		{"user:pass@[::1]:3052/alias", "::1", 3052, true},
		{"user:pass@[2001:db8::1]/alias", "2001:db8::1", 3050, true},
		{"user:pass@localhost/var/db/a b.fdb", "localhost", 3050, true},
		{"user:bad@secret@host/db", "", 0, false},
		{"user:pass@host:bad/db", "", 0, false},
		{"user:pass@host/db#secret", "", 0, false},
	} {
		h, p, ok := dsnNetwork(tt.dsn)
		if h != tt.host || p != tt.port || ok != tt.ok {
			t.Fatalf("%q => %q %d %t", tt.dsn, h, p, ok)
		}
	}
}
func TestExplicitConnectionAndHints(t *testing.T) {
	a := connectionAttributes("SECRET_CANARY", ConnectionAttributes{Host: "db", Port: 3050, Namespace: "billing"})
	if len(a) != 4 {
		t.Fatal(a)
	}
	ctx := context.Background()
	if WithOperationHint(ctx, OperationHint{Procedure: "bad; select 1"}) != ctx {
		t.Fatal("invalid hint")
	}
	if WithOperationHint(ctx, OperationHint{Procedure: `pkg."Проц"`}) == ctx {
		t.Fatal("missing hint")
	}
}
