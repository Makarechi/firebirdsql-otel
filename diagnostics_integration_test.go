package firebirdotel

import (
	"context"
	"database/sql"
	"github.com/Makarechi/firebirdsql-otel/metadata"
	"github.com/Makarechi/firebirdsql-otel/monitoring"
	"testing"
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
