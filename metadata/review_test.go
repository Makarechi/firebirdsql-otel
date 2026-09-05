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
	mock.ExpectQuery("SELECT FIRST").WithArgs("ROOT", 5).WillReturnRows(sqlmock.NewRows(columns).AddRow(" LEADING   ", 5, " FIELD\t  ", nil))
	mock.ExpectQuery("SELECT FIRST").WithArgs(" LEADING", 5).WillReturnRows(sqlmock.NewRows(columns).AddRow(" MEMBER   ", 5, nil, " PACKAGE   "))
	mock.ExpectQuery("SELECT FIRST").WithArgs(" PACKAGE", 19).WillReturnRows(sqlmock.NewRows(columns).AddRow(" TABLE\t   ", 0, nil, nil))
	mock.ExpectQuery("SELECT FIRST").WithArgs(" TABLE\t", 0).WillReturnRows(sqlmock.NewRows(columns))
	mock.ExpectCommit()
	g, err := reader.Read(t.Context(), Object{Name: "ROOT", Type: 5})
	if err != nil {
		t.Fatal(err)
	}
	if g.Scope != "package_body" || g.Executed != "unknown" || len(g.Edges) != 3 {
		t.Fatal(g)
	}
	if g.Edges[0].To.Name != " LEADING" || g.Edges[0].Field != " FIELD\t" || g.Edges[1].To.Package != " PACKAGE" || g.Edges[2].To.Name != " TABLE\t" {
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
