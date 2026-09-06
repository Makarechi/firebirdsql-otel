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

func TestHintsRejectCommentSuffix(t *testing.T) {
	ctx := context.Background()
	for _, s := range []string{"P --SECRET_CANARY", "P/*SECRET_CANARY*/", "P 'SECRET_CANARY'"} {
		if WithOperationHint(ctx, OperationHint{Procedure: s}) != ctx {
			t.Fatal("SQL fragment accepted as hint", s)
		}
	}
}
func TestHintsRejectCommentsInsideName(t *testing.T) {
	ctx := context.Background()
	for _, s := range []string{"pkg/* SECRET */.proc", "/* SECRET */proc"} {
		if WithOperationHint(ctx, OperationHint{Procedure: s}) != ctx {
			t.Fatal("comment accepted", s)
		}
	}
}
func TestConnectionFieldPrecedence(t *testing.T) {
	for _, tt := range []struct {
		name, dsn   string
		input, want ConnectionAttributes
	}{
		{"explicit host", "user:pass@db:3051/alias", ConnectionAttributes{Host: "alias", ParseDSNNetwork: true}, ConnectionAttributes{Host: "alias", Port: 3051, ParseDSNNetwork: true}},
		{"explicit port", "user:pass@db:3051/alias", ConnectionAttributes{Port: 4000, ParseDSNNetwork: true}, ConnectionAttributes{Host: "db", Port: 4000, ParseDSNNetwork: true}},
		{"both explicit", "user:pass@db:3051/alias", ConnectionAttributes{Host: "alias", Port: 4000, ParseDSNNetwork: true}, ConnectionAttributes{Host: "alias", Port: 4000, ParseDSNNetwork: true}},
		{"default port", "user:pass@db/alias", ConnectionAttributes{Host: "alias", ParseDSNNetwork: true}, ConnectionAttributes{Host: "alias", Port: 3050, ParseDSNNetwork: true}},
		{"ambiguous", "user:bad@secret@db:3051/alias", ConnectionAttributes{Host: "alias", ParseDSNNetwork: true}, ConnectionAttributes{Host: "alias", ParseDSNNetwork: true}},
		{"disabled", "user:pass@db:3051/alias", ConnectionAttributes{Host: "alias"}, ConnectionAttributes{Host: "alias"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveConnection(tt.dsn, tt.input); got != tt.want {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
			attrs := connectionAttributes(tt.dsn, tt.input)
			port := 0
			for _, a := range attrs {
				if a.Key == "server.port" {
					port = int(a.Value.AsInt64())
				}
			}
			if port != tt.want.Port {
				t.Fatalf("exported port %d, want %d", port, tt.want.Port)
			}
		})
	}
}

func TestExplicitPortWithoutHost(t *testing.T) {
	for _, parse := range []bool{false, true} {
		attrs := connectionAttributes("user:bad@secret@host/db", ConnectionAttributes{Port: 4000, ParseDSNNetwork: parse})
		found := false
		for _, a := range attrs {
			if string(a.Key) == "server.port" {
				found = a.Value.AsInt64() == 4000
			}
			if string(a.Key) == "server.address" {
				t.Fatal("ambiguous host exported")
			}
		}
		if !found {
			t.Fatal("explicit port omitted", attrs)
		}
	}
}

func TestConnectionAttributesRequireValidUTF8(t *testing.T) {
	for _, connection := range []ConnectionAttributes{{Host: "db\xff"}, {Namespace: "billing\xff"}} {
		if _, err := normalizeConfig(Config{Connection: connection}); err == nil {
			t.Fatal("invalid UTF-8 accepted")
		}
	}
	if _, err := normalizeConfig(Config{Connection: ConnectionAttributes{Host: "сервер", Namespace: "оплата"}}); err != nil {
		t.Fatal("valid Unicode rejected", err)
	}
}
