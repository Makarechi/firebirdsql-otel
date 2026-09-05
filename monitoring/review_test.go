package monitoring

import (
	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"strings"
	"testing"
	"time"
)

func TestSnapshotTimeExcludesPoolWaitAndPreservesNames(t *testing.T) {
	var firstMON time.Time
	matcher := sqlmock.QueryMatcherFunc(func(expected, actual string) error {
		if strings.Contains(actual, "FROM MON$ATTACHMENTS") && firstMON.IsZero() {
			firstMON = time.Now()
		}
		return sqlmock.QueryMatcherRegexp.Match(expected, actual)
	})
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	held, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	reader, _ := New(db, 5)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT CURRENT_CONNECTION").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(99))
	mock.ExpectQuery("SELECT MON\\$ATTACHMENT_ID FROM MON\\$ATTACHMENTS").WithArgs(int64(7)).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(7))
	mock.ExpectQuery("FROM MON\\$STATEMENTS").WillReturnRows(sqlmock.NewRows([]string{"id", "att", "tx", "state", "compiled"}).AddRow(1, 7, 2, 1, 3))
	mock.ExpectQuery("FROM MON\\$CALL_STACK").WillReturnRows(sqlmock.NewRows([]string{"id", "stmt", "caller", "name", "pkg", "type", "line", "column"}).AddRow(1, 1, nil, " PROC\t   ", " PKG  ", 5, 1, 1))
	mock.ExpectQuery("FROM MON\\$COMPILED_STATEMENTS").WillReturnRows(sqlmock.NewRows([]string{"id", "name", "pkg", "type"}).AddRow(3, " COMPILED  ", " PKG  ", 5))
	mock.ExpectQuery("FROM MON\\$TABLE_STATS").WillReturnRows(sqlmock.NewRows([]string{"table", "group", "seq", "idx", "ins", "upd", "del"}).AddRow(" TABLE\t  ", 1, 0, 0, 0, 0, 0))
	mock.ExpectCommit()
	type result struct {
		snapshot Snapshot
		err      error
	}
	done := make(chan result, 1)
	go func() { s, err := reader.Read(t.Context(), Scope{AttachmentID: 7}); done <- result{s, err} }()
	deadline := time.Now().Add(5 * time.Second)
	for db.Stats().WaitCount == 0 {
		if time.Now().After(deadline) {
			t.Fatal("reader did not wait for pool")
		}
		time.Sleep(time.Millisecond)
	}
	released := time.Now()
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatal(got.err)
		}
		s := got.snapshot
		if s.CapturedAt.Before(released) || s.CapturedAt.After(firstMON) {
			t.Fatal("timestamp includes pool/setup wait", released, s.CapturedAt, firstMON)
		}
		if s.Calls[0].Name != " PROC\t" || s.Calls[0].Package != " PKG" || s.Compiled[0].Name != " COMPILED" || s.Compiled[0].Package != " PKG" || s.Tables[0].Table != " TABLE\t" {
			t.Fatal("quoted identity changed", s)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reader did not finish")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestInvisibleTargetFailsInsteadOfReturningIdleSnapshot(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, _ := New(db, 5)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT CURRENT_CONNECTION").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(99))
	mock.ExpectQuery("SELECT MON\\$ATTACHMENT_ID FROM MON\\$ATTACHMENTS").WithArgs(int64(7)).WillReturnRows(sqlmock.NewRows([]string{"id"}))
	mock.ExpectRollback()
	s, err := r.Read(t.Context(), Scope{AttachmentID: 7})
	if err != ErrTargetNotVisible || s.Correlation != "unmatched" {
		t.Fatal("invisible target looked idle", s, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
