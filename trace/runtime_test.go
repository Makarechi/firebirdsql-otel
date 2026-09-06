package trace

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Makarechi/firebirdsql-otel/internal/traceparse"
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
	var aliasValue int
	if err := db.QueryRowContext(ctx, "SELECT\n1 ms\nFROM OTEL_A").Scan(&aliasValue); err != nil || aliasValue != 1 {
		t.Fatal("native expression alias failed", err)
	}
	longSQL := "SELECT 1 /*" + strings.Repeat("x", traceparse.MaxSQL-len("SELECT 1 /**/ FROM RDB$DATABASE")) + "*/ FROM RDB$DATABASE"
	var value int
	if err := db.QueryRowContext(ctx, longSQL).Scan(&value); err != nil || value != 1 {
		t.Fatal("large native SQL failed", err)
	}
	// The parser finalizes a record on the following native header.
	if err := db.QueryRowContext(ctx, "select count(*) from OTEL_A").Scan(&value); err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for !(found["OTEL_OUTER"] && found["OTEL_NESTED_A"] && found["OTEL_DOUBLE"] && found["OTEL_A_CHANGED"] && found["SELECT RDB$DATABASE"] && found["expression alias"]) {
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
				if e.Name == "SELECT RDB$DATABASE" && e.SQL != "SELECT ? FROM RDB$DATABASE" {
					t.Fatal("maximum requested SQL lost framing", e)
				}
				found[e.Name] = true
				if e.SQL == "SELECT ? MS FROM OTEL_A" {
					found["expression alias"] = true
				}
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err(), found)
		}
	}
	t.Log("observed actual outer, nested procedure, function and trigger finishes")
}

func TestEncodedWorkerConfigurationBound(t *testing.T) {
	want := workerConfig{strings.Repeat("<", 512), strings.Repeat(">", 256), strings.Repeat("&", 4096), strings.Repeat("<", 1024), strings.Repeat(">", 128)}
	data, err := json.Marshal(want)
	if err != nil || len(data) <= 8192 {
		t.Fatal("fixture must exceed previous encoded limit", err)
	}
	got, err := decodeWorkerConfig(strings.NewReader(string(data)))
	if err != nil || got != want {
		t.Fatal("accepted raw fields did not round trip", err)
	}
	for _, bad := range []string{strings.Repeat(" ", maxWorkerConfig+1), string(data) + "{}", `{"Password":"` + strings.Repeat("x", 4097) + `"}`} {
		if _, err := decodeWorkerConfig(strings.NewReader(bad)); err == nil {
			t.Fatal("accepted oversized or trailing input")
		}
	}
}

func TestMalformedWorkerRetainsCleanupUncertainty(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix signal helper")
	}
	for _, oversized := range []bool{false, true} {
		t.Run(fmt.Sprint(oversized), func(t *testing.T) {
			payload := "SECRET_CANARY_INVALID_JSON"
			expected := "invalid worker record"
			if oversized {
				payload = strings.Repeat("x", 65537)
				expected = "exceeds bound"
			}
			path := filepath.Join(t.TempDir(), "worker")
			script := "#!/bin/sh\ntrap '' INT\nprintf '%s\\n' '" + payload + "'\nwhile :; do :; done\n"
			if err := os.WriteFile(path, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			r, err := Start(t.Context(), Config{Executable: path, Address: "localhost", User: "test", Database: "/db", Name: "test"})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
			defer cancel()
			err = r.Wait(ctx)
			if err == nil || !strings.Contains(err.Error(), expected) || !strings.Contains(err.Error(), "server session cleanup may be required") || strings.Contains(err.Error(), "SECRET_CANARY") {
				t.Fatal("lost safe parse failure or cleanup uncertainty", err)
			}
		})
	}
}
func TestUnsupportedWindowsCollector(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("verified on Windows CI")
	}
	r, err := Start(t.Context(), Config{Executable: "must-not-run.exe", Address: "localhost", User: "test", Database: "/db", Name: "test"})
	if r != nil || !errors.Is(err, errors.ErrUnsupported) {
		t.Fatal("Windows launch was not rejected", err)
	}
}
