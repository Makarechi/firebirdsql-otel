package monitoring

import (
	"context"
	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"testing"
)

func TestFreshScopedTransactionsAndLimits(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	r, _ := New(db, 1)
	for i := 0; i < 2; i++ {
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT CURRENT_CONNECTION").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(99))
		mock.ExpectQuery("SELECT MON\\$ATTACHMENT_ID FROM MON\\$ATTACHMENTS").WithArgs(int64(7)).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(7))
		mock.ExpectQuery("FROM MON\\$STATEMENTS S WHERE S.MON\\$ATTACHMENT_ID = \\? AND S.MON\\$STATEMENT_ID = \\?").WithArgs(7, 8).WillReturnRows(sqlmock.NewRows([]string{"id", "att", "tx", "state", "compiled"}).AddRow(8, 7, 1, 1, 2).AddRow(9, 7, 1, 1, 3))
		mock.ExpectQuery("FROM MON\\$CALL_STACK").WithArgs(7, 8).WillReturnRows(sqlmock.NewRows([]string{"id", "stmt", "caller", "name", "pkg", "type", "line", "column"}))
		mock.ExpectQuery("FROM MON\\$COMPILED_STATEMENTS.*UNION SELECT K.MON\\$COMPILED_STATEMENT_ID FROM MON\\$CALL_STACK.*S.MON\\$ATTACHMENT_ID = \\? AND S.MON\\$STATEMENT_ID = \\?").WithArgs(7, 8, 7, 8).WillReturnRows(sqlmock.NewRows([]string{"id", "name", "pkg", "type"}))
		mock.ExpectQuery("FROM MON\\$TABLE_STATS").WithArgs(7, 8).WillReturnRows(sqlmock.NewRows([]string{"table", "group", "seq", "idx", "ins", "upd", "del"}).AddRow("A", 3, 1, 2, 3, 4, 5))
		mock.ExpectCommit()
		s, err := r.Read(context.Background(), Scope{AttachmentID: 7, StatementID: 8})
		if err != nil {
			t.Fatal(err)
		}
		if !s.Truncated || len(s.Statements) != 1 || s.Tables[0].StatGroup != 3 || s.Correlation != "scoped" {
			t.Fatal(s)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
func TestRejectBusinessAttachment(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	r, _ := New(db, 2)
	if _, err := r.Read(context.Background(), Scope{}); err == nil {
		t.Fatal("missing scope")
	}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT CURRENT_CONNECTION").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(7))
	mock.ExpectRollback()
	if _, err := r.Read(context.Background(), Scope{AttachmentID: 7}); err == nil {
		t.Fatal("used business attachment")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
