// Package monitoring reads one scoped MON$ snapshot per owned transaction.
package monitoring

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

type Scope struct {
	AttachmentID int64
	StatementID  int64
}
type Statement struct {
	ID, AttachmentID int64
	TransactionID    sql.NullInt64
	State            int
	CompiledID       sql.NullInt64
}
type Call struct {
	ID, StatementID int64
	CallerID        sql.NullInt64
	Name, Package   string
	ObjectType      sql.NullInt64
	Line, Column    sql.NullInt64
}
type CompiledStatement struct {
	ID            int64
	Name, Package string
	ObjectType    sql.NullInt64
}
type TableStats struct {
	Table                                           string
	StatGroup                                       int
	SeqReads, IndexReads, Inserts, Updates, Deletes int64
}
type Snapshot struct {
	Source, Correlation string
	Scope               Scope
	CapturedAt          time.Time
	Statements          []Statement
	Calls               []Call
	Compiled            []CompiledStatement
	Tables              []TableStats
	Truncated           bool
}
type Reader struct {
	db      *sql.DB
	maxRows int
}

// New requires a dedicated diagnostic pool. Use credentials with the minimum necessary visibility.
func New(db *sql.DB, maxRows int) (*Reader, error) {
	if maxRows == 0 {
		maxRows = 128
	}
	if db == nil || maxRows < 1 || maxRows > 4096 {
		return nil, errors.New("monitoring: invalid pool or row bound")
	}
	return &Reader{db, maxRows}, nil
}

// Read always completes its own transaction. Successive calls obtain fresh MON$ snapshots.
// No SQL text or explained plan BLOB is read into memory. Object identities are retained.
func (r *Reader) Read(ctx context.Context, scope Scope) (Snapshot, error) {
	out := Snapshot{Source: "monitoring", Correlation: "scoped", Scope: scope}
	if scope.AttachmentID <= 0 || scope.StatementID < 0 {
		return out, errors.New("monitoring: explicit attachment scope required")
	}
	conn, err := r.db.Conn(ctx)
	if err != nil {
		return out, err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	var own int64
	if err = tx.QueryRowContext(ctx, `SELECT CURRENT_CONNECTION FROM RDB$DATABASE`).Scan(&own); err != nil {
		return out, err
	}
	if own == scope.AttachmentID {
		return out, errors.New("monitoring: diagnostic attachment must differ from target")
	}
	where := `S.MON$ATTACHMENT_ID = ?`
	args := []any{scope.AttachmentID}
	if scope.StatementID != 0 {
		where += ` AND S.MON$STATEMENT_ID = ?`
		args = append(args, scope.StatementID)
	}
	query := fmt.Sprintf(`SELECT FIRST %d S.MON$STATEMENT_ID,S.MON$ATTACHMENT_ID,S.MON$TRANSACTION_ID,S.MON$STATE,S.MON$COMPILED_STATEMENT_ID FROM MON$STATEMENTS S WHERE %s ORDER BY S.MON$STATEMENT_ID`, r.maxRows+1, where)
	out.CapturedAt = time.Now() // The first MON$ query creates the transaction snapshot.
	err = r.scan(ctx, tx, query, args, &out, func(rows *sql.Rows) error {
		var x Statement
		if err := rows.Scan(&x.ID, &x.AttachmentID, &x.TransactionID, &x.State, &x.CompiledID); err != nil {
			return err
		}
		out.Statements = append(out.Statements, x)
		return nil
	})
	if err != nil {
		return out, err
	}
	query = fmt.Sprintf(`SELECT FIRST %d C.MON$CALL_ID,C.MON$STATEMENT_ID,C.MON$CALLER_ID,C.MON$OBJECT_NAME,C.MON$PACKAGE_NAME,C.MON$OBJECT_TYPE,C.MON$SOURCE_LINE,C.MON$SOURCE_COLUMN FROM MON$CALL_STACK C JOIN MON$STATEMENTS S ON S.MON$STATEMENT_ID=C.MON$STATEMENT_ID WHERE %s ORDER BY C.MON$CALL_ID`, r.maxRows+1, where)
	err = r.scan(ctx, tx, query, args, &out, func(rows *sql.Rows) error {
		var x Call
		var name, pkg sql.NullString
		if err := rows.Scan(&x.ID, &x.StatementID, &x.CallerID, &name, &pkg, &x.ObjectType, &x.Line, &x.Column); err != nil {
			return err
		}
		x.Name = strings.TrimRight(name.String, " ")
		x.Package = strings.TrimRight(pkg.String, " ")
		out.Calls = append(out.Calls, x)
		return nil
	})
	if err != nil {
		return out, err
	}
	query = fmt.Sprintf(`SELECT FIRST %d DISTINCT C.MON$COMPILED_STATEMENT_ID,C.MON$OBJECT_NAME,C.MON$PACKAGE_NAME,C.MON$OBJECT_TYPE FROM MON$COMPILED_STATEMENTS C JOIN MON$STATEMENTS S ON S.MON$COMPILED_STATEMENT_ID=C.MON$COMPILED_STATEMENT_ID WHERE %s ORDER BY C.MON$COMPILED_STATEMENT_ID`, r.maxRows+1, where)
	err = r.scan(ctx, tx, query, args, &out, func(rows *sql.Rows) error {
		var x CompiledStatement
		var name, pkg sql.NullString
		if err := rows.Scan(&x.ID, &name, &pkg, &x.ObjectType); err != nil {
			return err
		}
		x.Name = strings.TrimRight(name.String, " ")
		x.Package = strings.TrimRight(pkg.String, " ")
		out.Compiled = append(out.Compiled, x)
		return nil
	})
	if err != nil {
		return out, err
	}
	var statScope string
	if scope.StatementID > 0 {
		statScope = `T.MON$STAT_GROUP=3 AND T.MON$STAT_ID IN (SELECT S.MON$STAT_ID FROM MON$STATEMENTS S WHERE ` + where + `)`
	} else {
		statScope = `T.MON$STAT_GROUP=1 AND T.MON$STAT_ID IN (SELECT MON$STAT_ID FROM MON$ATTACHMENTS WHERE MON$ATTACHMENT_ID=?)`
	}
	query = fmt.Sprintf(`SELECT FIRST %d T.MON$TABLE_NAME,T.MON$STAT_GROUP,R.MON$RECORD_SEQ_READS,R.MON$RECORD_IDX_READS,R.MON$RECORD_INSERTS,R.MON$RECORD_UPDATES,R.MON$RECORD_DELETES FROM MON$TABLE_STATS T JOIN MON$RECORD_STATS R ON R.MON$STAT_ID=T.MON$RECORD_STAT_ID AND R.MON$STAT_GROUP=T.MON$STAT_GROUP WHERE %s ORDER BY T.MON$TABLE_NAME`, r.maxRows+1, statScope)
	err = r.scan(ctx, tx, query, args, &out, func(rows *sql.Rows) error {
		var x TableStats
		if err := rows.Scan(&x.Table, &x.StatGroup, &x.SeqReads, &x.IndexReads, &x.Inserts, &x.Updates, &x.Deletes); err != nil {
			return err
		}
		x.Table = strings.TrimRight(x.Table, " ")
		out.Tables = append(out.Tables, x)
		return nil
	})
	if err != nil {
		return out, err
	}
	return out, tx.Commit()
}
func (r *Reader) scan(ctx context.Context, tx *sql.Tx, q string, args []any, out *Snapshot, visit func(*sql.Rows) error) error {
	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		if n == r.maxRows {
			out.Truncated = true
			break
		}
		if err := visit(rows); err != nil {
			return err
		}
		n++
	}
	return errors.Join(rows.Err(), rows.Close())
}
