package sqltext

import (
	"strings"
	"testing"
)

func TestGolden(t *testing.T) {
	for _, tt := range []struct{ sql, op, summary, proc, text string }{
		{"-- header\n execute procedure pkg.proc('FROM', 123)", "EXECUTE PROCEDURE", "EXECUTE PROCEDURE PKG.PROC", "PKG.PROC", "EXECUTE PROCEDURE PKG . PROC ( ? , ? )"},
		{`select * from "Звіт"(?)`, "SELECT", `SELECT "Звіт"`, `"Звіт"`, `SELECT * FROM "Звіт" ( ? )`},
		{`select * from obj$contract`, "SELECT", "SELECT OBJ$CONTRACT", "", "SELECT * FROM OBJ$CONTRACT"},
		{`with x as (select 'FROM' from rdb$database) select * from x`, "SELECT", "SELECT X", "", "WITH X AS ( SELECT ? FROM RDB$DATABASE ) SELECT * FROM X"},
		{`update or insert into obj$contract(id) values(0xFF)`, "UPDATE OR INSERT", "UPDATE OR INSERT OBJ$CONTRACT", "", "UPDATE OR INSERT INTO OBJ$CONTRACT ( ID ) VALUES ( ? )"},
		{`merge into t using u on t.id=u.id when matched then update set x=2`, "MERGE", "MERGE T", "", "MERGE INTO T USING U ON T . ID = U . ID WHEN MATCHED THEN UPDATE SET X = ?"},
		{`execute block as begin x = q'{a'b FROM}'; end`, "EXECUTE BLOCK", "EXECUTE BLOCK", "", "EXECUTE BLOCK AS BEGIN X = ? ; END"},
		{`select X'DEAD', _UTF8 'it''s private', -1.25e+3, true from rdb$database`, "SELECT", "SELECT RDB$DATABASE", "", "SELECT ? , _UTF8 ? , - ? , ? FROM RDB$DATABASE"},
		{`/* outer /* inner */ */ create table "a" (id integer)`, "CREATE", "CREATE", "", `CREATE TABLE "a" ( ID INTEGER )`},
	} {
		t.Run(tt.sql, func(t *testing.T) {
			d := Analyze(tt.sql, 0, 0)
			if !d.Valid || d.Operation != tt.op || d.Summary != tt.summary || d.Procedure != tt.proc || d.Text != tt.text {
				t.Fatalf("got %+v", d)
			}
		})
	}
}
func TestFailClosed(t *testing.T) {
	for _, s := range []string{"select 'SECRET_CANARY", "select q'{SECRET_CANARY'", "select /* SECRET_CANARY", "select (1", "select 1abc", "select \x00SECRET_CANARY", string([]byte{0xff}), strings.Repeat("x", MaxInput+1)} {
		d := Analyze(s, 0, 0)
		if d.Valid || d.Text != "" || d.Fingerprint != "" {
			t.Fatalf("unsafe %+v", d)
		}
	}
}
func TestFingerprintAndLimits(t *testing.T) {
	a := Analyze("select 123 from t", 0, 0)
	b := Analyze("SELECT 'secret' FROM T", 0, 0)
	if a.Fingerprint != b.Fingerprint {
		t.Fatal("literal-dependent fingerprint")
	}
	d := Analyze(`select "long identifier" from t`, 0, 10)
	if d.Text != "" {
		t.Fatal("output exceeded bound")
	}
}
func FuzzAnalyze(f *testing.F) {
	for _, s := range []string{"select 1", `execute procedure "x"(?)`, "select q'[a'b]'", "/*bad"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		d := Analyze(s, 0, 0)
		if len(d.Text) > MaxOutput || len(d.Summary) > 300 {
			t.Fatal("unbounded output")
		}
	})
}

func TestUnknownDialectOmitsAmbiguousDescription(t *testing.T) {
	for _, q := range []string{`select "SECRET_CANARY" from rdb$database`, `select * from "SECRET_CANARY"`, `execute procedure "SECRET_CANARY"(?)`, `PLAN ("SECRET_CANARY" NATURAL)`} {
		d := AnalyzeUnknownDialect(q, 0, 0)
		if d.Valid || d.Text != "" || d.Fingerprint != "" || d.Procedure != "" || d.Collection != "" || d.Summary != "SQL" {
			t.Fatalf("ambiguous description exported: %+v", d)
		}
	}
	for _, q := range []string{`select 'a"SECRET_CANARY' from rdb$database`, `select q'{"SECRET_CANARY"}' from rdb$database`, `select 1 /* "SECRET_CANARY" */ from rdb$database`} {
		d := AnalyzeUnknownDialect(q, 0, 0)
		if !d.Valid || strings.Contains(d.Text, "SECRET_CANARY") {
			t.Fatalf("safe literals/comments mishandled: %+v", d)
		}
	}
	d := Analyze(`execute procedure "Quoted"."Проц"(?)`, 0, 0)
	if !d.Valid || d.Procedure != `"Quoted"."Проц"` {
		t.Fatalf("dialect 3 identifiers lost: %+v", d)
	}
}

func TestFirebirdOperatorsAndArraySubscripts(t *testing.T) {
	for _, q := range []string{"SELECT A FROM T WHERE A ^= 12345", "SELECT A FROM T WHERE A ~= 12345", "SELECT VALUES_COL[12345] FROM T", "SELECT VALUES_COL[(12345+1),2] FROM T"} {
		d := Analyze(q, 0, 0)
		if !d.Valid || d.Text == "" || d.Fingerprint == "" || strings.Contains(d.Text, "12345") {
			t.Fatal("valid expression omitted or literal leaked", q, d)
		}
	}
	for _, q := range []string{"SELECT A[1 FROM T", "SELECT A] FROM T", "SELECT A[(1]) FROM T", "SELECT A" + strings.Repeat("[", 129) + "1" + strings.Repeat("]", 129) + " FROM T"} {
		if d := Analyze(q, 0, 0); d.Valid || d.Text != "" {
			t.Fatal("invalid brackets accepted", d)
		}
	}
	for _, q := range []string{"SAVEPOINT nested", "RELEASE SAVEPOINT nested"} {
		d := Analyze(q, 0, 0)
		want := strings.Fields(q)[0]
		if !d.Valid || d.Operation != want || d.Summary != want {
			t.Fatal("transaction operation omitted", d)
		}
	}
}
