package trace

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestRuntimeBoundedShutdown(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix signal helper")
	}
	path := filepath.Join(t.TempDir(), "worker")
	script := "#!/bin/sh\ntrap '' INT\nwhile true; do printf '%s\\n' '{\"Source\":\"trace\",\"Kind\":\"test\"}'; done\n"
	if err := os.WriteFile(path, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	r, err := Start(context.Background(), Config{Executable: path, Address: "localhost", User: "test", Password: "SECRET", Database: "/db", Name: "test", Buffer: 1})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-r.Events():
	case <-time.After(3 * time.Second):
		t.Fatal("no events")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := time.Now()
	err = r.Shutdown(ctx)
	if time.Since(start) > 2*time.Second {
		t.Fatal("unbounded stop")
	}
	if err == nil {
		t.Fatal("forced stop must report cleanup uncertainty")
	}
}
func TestInvalidWorkerInput(t *testing.T) {
	for _, s := range []string{"invalid", `{"Database":"/db\nother"}`} {
		if err := RunWorker(context.Background(), strings.NewReader(s), os.Stdout); err == nil {
			t.Fatal("accepted input")
		}
	}
}
func TestEventEncoding(t *testing.T) {
	b, err := json.Marshal(Event{Source: "trace", Correlation: "unmatched", Kind: "gap", Incomplete: true})
	if err != nil || !strings.Contains(string(b), "gap") {
		t.Fatal(fmt.Sprint(err))
	}
}

func TestFirebird5Trace(t *testing.T) {
	dsn := os.Getenv("FIREBIRD_TEST_DSN")
	binary := os.Getenv("FIREBIRD_TRACE_BINARY")
	if dsn == "" || binary == "" {
		t.Skip("requires isolated Firebird 5 and compiled trace worker")
	}
	u, err := url.Parse("firebird://" + strings.TrimPrefix(dsn, "firebird://"))
	if err != nil {
		t.Fatal(err)
	}
	password, _ := u.User.Password()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	r, err := Start(ctx, Config{Executable: binary, Address: u.Host, User: u.User.Username(), Password: password, Database: u.Path, Name: "firebirdotel-live-test"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		stop, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := r.Shutdown(stop); err != nil {
			t.Error(err)
		}
	}()
	select {
	case e, ok := <-r.Events():
		if !ok || e.Phase != "ready" {
			t.Fatal("worker did not start", e, r.Wait(ctx))
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	db, err := sql.Open("firebirdsql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 3; i++ {
		if _, err := db.ExecContext(ctx, "execute procedure OTEL_OUTER(?)", 7); err != nil {
			t.Fatal(err)
		}
	}
	found := map[string]bool{}
	for !(found["OTEL_OUTER"] && found["OTEL_NESTED_A"] && found["OTEL_DOUBLE"] && found["OTEL_A_CHANGED"]) {
		select {
		case e, ok := <-r.Events():
			if !ok {
				t.Fatal("worker ended", r.Wait(ctx), found)
			}
			t.Logf("event kind=%s phase=%s name=%s incomplete=%v", e.Kind, e.Phase, e.Name, e.Incomplete)
			if e.Source != "trace" || e.Correlation == "exact" {
				t.Fatal("invalid provenance", e)
			}
			if e.Kind == "procedure" && e.Name == "OTEL_NESTED_B" {
				t.Fatal("unexecuted branch emitted")
			}
			if e.Phase == "finish" {
				found[e.Name] = true
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err(), found)
		}
	}
	t.Log("observed actual outer, nested procedure, function and trigger finishes")
}
