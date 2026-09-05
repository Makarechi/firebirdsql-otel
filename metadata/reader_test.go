package metadata

import (
	"context"
	"errors"
	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"testing"
	"time"
)

func TestBoundsCyclesAndCache(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, err := New(db, Config{Database: "test", MaxNodes: 2, MaxDepth: 3, MaxEntries: 1, TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	root := Object{Name: "A", Type: 5}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT FIRST").WithArgs("A", 5).WillReturnRows(sqlmock.NewRows([]string{"name", "type", "field", "package"}).AddRow("B", 5, nil, nil))
	mock.ExpectQuery("SELECT FIRST").WithArgs("B", 5).WillReturnRows(sqlmock.NewRows([]string{"name", "type", "field", "package"}).AddRow("A", 5, nil, nil).AddRow("C", 5, nil, nil))
	mock.ExpectCommit()
	g, err := r.Read(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if !g.Truncated || len(g.Nodes) != 2 || len(g.Edges) != 2 || g.Executed != "unknown" {
		t.Fatal(g)
	}
	g.Nodes[0].Name = "mutated"
	g, err = r.Read(context.Background(), root)
	if err != nil || g.Nodes[0].Name != "A" {
		t.Fatal("cache mutated")
	}
	if len(r.cache) != 1 {
		t.Fatal("missing cache")
	}
	if err := r.Invalidate("v2"); err != nil {
		t.Fatal(err)
	}
	if len(r.cache) != 0 {
		t.Fatal("invalidation failed")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
func TestExpiredCacheAndQueryFailure(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	r, _ := New(db, Config{Database: "db"})
	root := Object{Name: "A", Type: 5}
	r.cache[key{"db", "", root}] = entry{expires: time.Now().Add(-time.Second)}
	failure := errors.New("metadata unavailable")
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT FIRST").WillReturnError(failure)
	mock.ExpectRollback()
	_, err := r.Read(context.Background(), root)
	if !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
func TestConfigurationBounds(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	for _, c := range []Config{{}, {Database: "db", MaxDepth: 65}, {Database: "db", MaxNodes: 4097}, {Database: "db", MaxEntries: 1025}} {
		if _, err := New(db, c); err == nil {
			t.Fatal("accepted bad limits")
		}
	}
}
