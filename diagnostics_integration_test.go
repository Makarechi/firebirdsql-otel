package firebirdotel

import (
	"context"
	"database/sql"
	"github.com/Makarechi/firebirdsql-otel/metadata"
	"github.com/Makarechi/firebirdsql-otel/monitoring"
	"testing"
	"time"
)

func TestFirebird5MetadataMonitoring(t *testing.T) {
	dsn := integrationDSN(t)
	db, err := sql.Open(DriverName, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	r, err := metadata.New(db, metadata.Config{Database: "synthetic", SchemaVersion: "1"})
	if err != nil {
		t.Fatal(err)
	}
	graph, err := r.Read(ctx, metadata.Object{Name: "OTEL_OUTER", Type: 5})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, n := range graph.Nodes {
		if n.Name == "OTEL_NESTED_B" {
			found = true
		}
	}
	if !found || graph.Executed != "unknown" || graph.Source != "metadata" {
		t.Fatal(graph)
	}
	graph.Nodes[0].Name = "MUTATED"
	again, err := r.Read(ctx, metadata.Object{Name: "OTEL_OUTER", Type: 5})
	if err != nil || again.Nodes[0].Name == "MUTATED" {
		t.Fatal("mutable cache")
	}
	if err := r.Invalidate("2"); err != nil {
		t.Fatal(err)
	}
	for _, obj := range []metadata.Object{{Name: "OTEL_RECURSE", Type: 5}, {Name: "OTEL_PACKAGED_CALL", Type: 5}, {Name: "MEMBER", Package: "OTEL_PACKAGE", Type: 5}, {Name: "OTEL_V", Type: 1}, {Name: "OTEL_A_CHANGED", Type: 2}} {
		g, err := r.Read(ctx, obj)
		if err != nil {
			t.Fatal(err)
		}
		if len(g.Edges) == 0 {
			t.Fatalf("no dependencies for %+v", obj)
		}
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var attachment int64
	if err := conn.QueryRowContext(ctx, "select current_connection from rdb$database").Scan(&attachment); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "execute procedure OTEL_OUTER(?)", 8); err != nil {
		t.Fatal(err)
	}
	diag, err := sql.Open(DriverName, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer diag.Close()
	mon, err := monitoring.New(diag, 64)
	if err != nil {
		t.Fatal(err)
	}
	held, err := conn.PrepareContext(ctx, "select * from OTEL_REPORT")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	heldRows, err := held.QueryContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer heldRows.Close()
	if !heldRows.Next() {
		t.Fatal("missing live row")
	}
	missing, err := mon.Read(ctx, monitoring.Scope{AttachmentID: attachment, StatementID: 9223372036854775807})
	if err != monitoring.ErrStatementNotVisible || missing.Correlation != "unmatched" {
		t.Fatal("missing live statement accepted", missing, err)
	}

	for i := 0; i < 2; i++ {
		snap, err := mon.Read(ctx, monitoring.Scope{AttachmentID: attachment})
		if err != nil {
			t.Fatal(err)
		}
		if snap.Source != "monitoring" || snap.Correlation != "scoped" || snap.Scope.AttachmentID != attachment {
			t.Fatal(snap)
		}
		if len(snap.Statements) == 0 || len(snap.Compiled) == 0 || len(snap.Tables) == 0 {
			t.Fatal("missing scoped live state", snap)
		}
		scoped, err := mon.Read(ctx, monitoring.Scope{AttachmentID: attachment, StatementID: snap.Statements[0].ID})
		if err != nil {
			t.Fatal(err)
		}
		if len(scoped.Statements) != 1 {
			t.Fatal("statement scope not applied", scoped)
		}
		t.Logf("snapshot: %d statements, %d calls, %d compiled, %d tables", len(snap.Statements), len(snap.Calls), len(snap.Compiled), len(snap.Tables))
	}
}

func TestFirebird5CompiledCallStack(t *testing.T) {
	dsn := integrationDSN(t)
	db, err := sql.Open(DriverName, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	locker, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback()
	if _, err := locker.ExecContext(ctx, "update OTEL_A set VAL=VAL where ID=1"); err != nil {
		t.Fatal(err)
	}
	worker, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	var att int64
	if err := worker.QueryRowContext(ctx, "select current_connection from rdb$database").Scan(&att); err != nil {
		t.Fatal(err)
	}
	diag, err := sql.Open(DriverName, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer diag.Close()
	reader, _ := monitoring.New(diag, 64)
	finished := make(chan error, 1)
	go func() { _, err := worker.ExecContext(ctx, "execute procedure OTEL_OUTER(?)", 7); finished <- err }()
	// Always release the row lock before waiting for the driver; context cancellation
	// alone is not a reliable way to interrupt a blocked native wire operation.
	defer func() {
		_ = locker.Rollback()
		select {
		case <-finished:
		case <-time.After(10 * time.Second):
			t.Error("blocked statement did not finish")
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		snap, err := reader.Read(ctx, monitoring.Scope{AttachmentID: att})
		if err != nil {
			t.Fatal(err)
		}
		if len(snap.Calls) > 0 {
			names := map[string]bool{}
			for _, c := range snap.Compiled {
				names[c.Name] = true
			}
			for _, call := range snap.Calls {
				if !names[call.Name] {
					t.Fatalf("call %s missing from compiled snapshot: %+v", call.Name, snap.Compiled)
				}
			}
			if !names["OTEL_NESTED_A"] {
				t.Fatal("fixture did not reach nested PSQL", snap)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no active PSQL call stack")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
