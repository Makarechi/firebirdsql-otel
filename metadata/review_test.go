package metadata

import (
	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"testing"
)

func TestQuotedDependenciesAndBroadenedPackageScope(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	reader, err := New(db, Config{Database: "test"})
	if err != nil {
		t.Fatal(err)
	}
	columns := []string{"name", "type", "field", "package"}
	mock.ExpectBegin()
	mock.ExpectQuery("ORDER BY RDB\\$DEPENDED_ON_TYPE, RDB\\$DEPENDED_ON_NAME, RDB\\$PACKAGE_NAME, RDB\\$FIELD_NAME").WithArgs("ROOT", 5).WillReturnRows(sqlmock.NewRows(columns).AddRow(" LEADING   ", 5, " FIELD\t  ", nil))
	mock.ExpectQuery("ORDER BY RDB\\$DEPENDED_ON_TYPE, RDB\\$DEPENDED_ON_NAME, RDB\\$PACKAGE_NAME, RDB\\$FIELD_NAME").WithArgs(" LEADING", 5).WillReturnRows(sqlmock.NewRows(columns).AddRow(" MEMBER   ", 5, nil, " PACKAGE   "))
	mock.ExpectQuery("ORDER BY RDB\\$DEPENDED_ON_TYPE, RDB\\$DEPENDED_ON_NAME, RDB\\$PACKAGE_NAME, RDB\\$FIELD_NAME").WithArgs(" PACKAGE", 19).WillReturnRows(sqlmock.NewRows(columns).AddRow(" TABLE\t   ", 0, nil, nil))
	mock.ExpectQuery("ORDER BY RDB\\$DEPENDED_ON_TYPE, RDB\\$DEPENDED_ON_NAME, RDB\\$PACKAGE_NAME, RDB\\$FIELD_NAME").WithArgs(" TABLE\t", 0).WillReturnRows(sqlmock.NewRows(columns))
	mock.ExpectCommit()
	g, err := reader.Read(t.Context(), Object{Name: "ROOT", Type: 5})
	if err != nil {
		t.Fatal(err)
	}
	if g.Scope != "package_body" || g.Executed != "unknown" || len(g.Edges) != 4 {
		t.Fatal(g)
	}
	if g.Edges[0].To.Name != " LEADING" || g.Edges[0].Field != " FIELD\t" || g.Edges[1].To.Package != " PACKAGE" || g.Edges[3].To.Name != " TABLE\t" {
		t.Fatal("identities changed", g)
	}
	cached, err := reader.Read(t.Context(), g.Root)
	if err != nil || cached.Scope != "package_body" {
		t.Fatal("cached precision lost", cached, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPackagedRootRemainsConnected(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, _ := New(db, Config{Database: "test"})
	root := Object{Name: "MEMBER", Type: 5, Package: "PKG"}
	columns := []string{"name", "type", "field", "package"}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT FIRST").WithArgs("PKG", 19).WillReturnRows(sqlmock.NewRows(columns).AddRow("T", 0, nil, nil))
	mock.ExpectQuery("SELECT FIRST").WithArgs("T", 0).WillReturnRows(sqlmock.NewRows(columns))
	mock.ExpectCommit()
	g, err := r.Read(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	reachable := map[Object]bool{root: true}
	for i := 0; i < len(g.Nodes); i++ {
		for _, edge := range g.Edges {
			if reachable[edge.From] {
				reachable[edge.To] = true
			}
		}
	}
	for _, node := range g.Nodes {
		if !reachable[node] {
			t.Fatal("disconnected graph", g)
		}
	}
	if g.Edges[0].Kind != "package_body_scope" || g.Edges[1].Kind != "dependency" {
		t.Fatal("synthetic scope relation presented as dependency", g)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
