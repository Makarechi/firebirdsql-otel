package firebirdotel

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

func integrationDSN(t *testing.T) string {
	t.Helper()
	s := os.Getenv("FIREBIRD_TEST_DSN")
	if s == "" {
		t.Skip("set FIREBIRD_TEST_DSN to the isolated Firebird 5 fixture database")
	}
	return s
}
func TestFirebird5Client(t *testing.T) {
	dsn := integrationDSN(t)
	cfg, rec, _ := setupTelemetry(t, DiagnosticConfig())
	cfg.Connection.Namespace = "synthetic"
	db, err := OpenWithConfig(dsn, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := t.Context()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := Raw(conn, func(native any) error {
		typ := reflect.TypeOf(native)
		if typ.Kind() != reflect.Pointer || typ.Elem().PkgPath() != "github.com/nakagami/firebirdsql" {
			return fmt.Errorf("unexpected native %T", native)
		}
		_, ok := native.(driver.Pinger)
		if !ok {
			t.Fatal("lost native pinger")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "execute procedure OTEL_OUTER(?)", 7); err != nil {
		t.Fatal(err)
	}
	stmt, err := conn.PrepareContext(ctx, "select * from OTEL_REPORT")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	types, err := rows.ColumnTypes()
	if err != nil || len(types) != 1 || types[0].DatabaseTypeName() == "" {
		t.Fatal("lost column types", types, err)
	}
	n := 0
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatal(n)
	}
	_ = rows.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var v int
	err = conn.QueryRowContext(ctx, "select cast('"+canary+"' as integer) from rdb$database").Scan(&v)
	if err == nil {
		t.Fatal("expected server error")
	}
	for _, s := range rec.Ended() {
		if strings.Contains(fmt.Sprint(s.Name(), s.Attributes(), s.Events(), s.Status()), canary) {
			t.Fatal("live secret leaked")
		}
		if strings.Contains(s.Name(), "OTEL_NESTED") || strings.Contains(s.Name(), "OTEL_A_CHANGED") {
			t.Fatal("invented server execution")
		}
	}
}

func TestFirebird5GoldenSnapshot(t *testing.T) {
	dsn := integrationDSN(t)
	cfg, rec, _ := setupTelemetry(t, DiagnosticConfig())
	db, err := OpenWithConfig(dsn, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(t.Context(), "execute procedure OTEL_OUTER(?)", 9); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryContext(t.Context(), "select * from OTEL_REPORT")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var n int
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	var n int
	_ = db.QueryRowContext(t.Context(), "select cast('SECRET_CANARY_742' as integer) from rdb$database").Scan(&n)
	data, err := os.ReadFile("testdata/firebird5/expected-client.json")
	if err != nil {
		t.Fatal(err)
	}
	var expected []struct {
		Name       string
		Attributes map[string]any
	}
	if err := json.Unmarshal(data, &expected); err != nil {
		t.Fatal(err)
	}
	for _, want := range expected {
		found := false
		for _, span := range rec.Ended() {
			if span.Name() != want.Name {
				continue
			}
			matches := true
			for key, value := range want.Attributes {
				present := false
				for _, attr := range span.Attributes() {
					if string(attr.Key) == key && fmt.Sprint(attr.Value.AsInterface()) == fmt.Sprint(value) {
						present = true
					}
				}
				if !present {
					matches = false
				}
			}
			if matches {
				found = true
			}
		}
		if !found {
			for _, got := range rec.Ended() {
				t.Logf("span %s: %v", got.Name(), got.Attributes())
			}
			t.Fatalf("missing golden span %+v", want)
		}
	}
}
