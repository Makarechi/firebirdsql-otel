package trace

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Makarechi/firebirdsql-otel/internal/traceparse"
)

type fakeTraceManager struct {
	session       *fakeTraceSession
	name          string
	config        string
	ctxErr        error
	err           error
	waitForCancel bool
}

func (m *fakeTraceManager) StartWithNameContext(ctx context.Context, name, config string) (traceSession, error) {
	m.name, m.config, m.ctxErr = name, config, ctx.Err()
	if m.waitForCancel {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if m.err != nil {
		return nil, m.err
	}
	return m.session, nil
}

type fakeTraceSession struct {
	chunks       chan string
	closed       chan struct{}
	waitErr      error
	closeErr     error
	closeOnce    sync.Once
	startOnce    sync.Once
	closeStarted chan struct{}
	blockClose   bool
}

func newFakeTraceSession() *fakeTraceSession {
	return &fakeTraceSession{chunks: make(chan string, 8), closed: make(chan struct{})}
}

func (s *fakeTraceSession) WaitStringsContext(ctx context.Context, result chan string) error {
	for {
		select {
		case chunk := <-s.chunks:
			select {
			case result <- chunk:
			case <-ctx.Done():
				return ctx.Err()
			}
		case <-s.closed:
			return s.waitErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *fakeTraceSession) CloseContext(ctx context.Context) error {
	if s.closeStarted != nil {
		s.startOnce.Do(func() { close(s.closeStarted) })
	}
	if s.blockClose {
		<-ctx.Done()
	}
	s.closeOnce.Do(func() { close(s.closed) })
	if s.blockClose {
		return ctx.Err()
	}
	return s.closeErr
}

func useFakeManager(t *testing.T, manager *fakeTraceManager) {
	t.Helper()
	previous := newTraceManager
	newTraceManager = func(string, string, string) (traceManager, error) { return manager, nil }
	t.Cleanup(func() { newTraceManager = previous })
}

func TestRuntimeStartsAndShutsDownInProcess(t *testing.T) {
	session := newFakeTraceSession()
	manager := &fakeTraceManager{session: session}
	useFakeManager(t, manager)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	r, err := Start(ctx, Config{Address: "localhost", User: "test", Password: "SECRET", Database: "/db", Name: "test"})
	if err != nil {
		t.Fatal(err)
	}
	e := <-r.Events()
	if e.Kind != "lifecycle" || e.Phase != "ready" || manager.name != "test" || !strings.Contains(manager.config, `database = "/db"`) {
		t.Fatalf("collector did not become ready with expected config: event=%+v name=%q", e, manager.name)
	}
	if err := r.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-r.Events(); ok {
		t.Fatal("events remained open after shutdown")
	}
}

func TestRuntimeShutdownDoesNotRequireEventConsumer(t *testing.T) {
	session := newFakeTraceSession()
	manager := &fakeTraceManager{session: session}
	useFakeManager(t, manager)
	r, err := Start(t.Context(), Config{Address: "localhost", User: "test", Database: "/db", Name: "test", Buffer: 1})
	if err != nil {
		t.Fatal(err)
	}
	// The ready event fills the only queue slot. This additional parser event
	// blocks delivery until Shutdown switches the runtime to discard mode.
	session.chunks <- "2026-09-17T08:00:00.0000 (1:0x1) UNSUPPORTED_EVENT"
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := r.Shutdown(ctx); err != nil {
		t.Fatal("shutdown depended on draining Events", err)
	}
}

func TestRuntimeStartupAndShutdownRespectContexts(t *testing.T) {
	t.Run("startup", func(t *testing.T) {
		manager := &fakeTraceManager{session: newFakeTraceSession(), waitForCancel: true}
		useFakeManager(t, manager)
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		if r, err := Start(ctx, Config{Address: "localhost", User: "test", Database: "/db", Name: "test"}); r != nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("startup context was not preserved", err)
		}
	})

	t.Run("shutdown", func(t *testing.T) {
		session := newFakeTraceSession()
		session.blockClose = true
		session.closeStarted = make(chan struct{})
		manager := &fakeTraceManager{session: session}
		useFakeManager(t, manager)
		r, err := Start(t.Context(), Config{Address: "localhost", User: "test", Database: "/db", Name: "test"})
		if err != nil {
			t.Fatal(err)
		}
		<-r.Events()
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		if err = r.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("shutdown context was not preserved", err)
		}
		select {
		case <-session.closeStarted:
		default:
			t.Fatal("session cleanup was not attempted")
		}
	})
}

func TestRuntimeReportsSafeStreamAndCleanupErrors(t *testing.T) {
	for _, cleanupFailure := range []bool{false, true} {
		t.Run(fmt.Sprint(cleanupFailure), func(t *testing.T) {
			session := newFakeTraceSession()
			session.waitErr = errors.New("SECRET_STREAM_ERROR")
			if cleanupFailure {
				session.closeErr = errors.New("SECRET_CLOSE_ERROR")
			}
			manager := &fakeTraceManager{session: session}
			useFakeManager(t, manager)
			r, err := Start(t.Context(), Config{Address: "localhost", User: "test", Database: "/db", Name: "test"})
			if err != nil {
				t.Fatal(err)
			}
			<-r.Events()
			session.closeOnce.Do(func() { close(session.closed) })
			err = r.Wait(t.Context())
			if err == nil || strings.Contains(err.Error(), "SECRET") || !strings.Contains(err.Error(), "stream ended") {
				t.Fatal("unsafe or missing stream error", err)
			}
			if cleanupFailure && !strings.Contains(err.Error(), "session cleanup failed") {
				t.Fatal("cleanup failure lost", err)
			}
		})
	}
}

func TestRuntimeReportsUnexpectedCleanStreamEnd(t *testing.T) {
	session := newFakeTraceSession()
	manager := &fakeTraceManager{session: session}
	useFakeManager(t, manager)
	r, err := Start(t.Context(), Config{Address: "localhost", User: "test", Database: "/db", Name: "test"})
	if err != nil {
		t.Fatal(err)
	}
	<-r.Events()
	session.closeOnce.Do(func() { close(session.closed) })
	err = r.Wait(t.Context())
	if err == nil || !strings.Contains(err.Error(), "stream ended unexpectedly") {
		t.Fatal("clean unsolicited stream end was not reported", err)
	}
}

func TestRuntimeConfigurationValidation(t *testing.T) {
	for _, cfg := range []Config{
		{},
		{Address: "localhost", User: "test", Database: "/db", Name: "bad\nname"},
		{Address: "localhost", User: "test", Database: "/bad\npath", Name: "test"},
		{Address: "localhost", User: "test", Database: "/db", Name: "test", Buffer: 257},
		{Address: "localhost", User: strings.Repeat("x", 257), Database: "/db", Name: "test"},
		{Address: "localhost", User: string([]byte{0xff}), Database: "/db", Name: "test"},
		{Address: "localhost", User: "test", Password: string([]byte{0xff}), Database: "/db", Name: "test"},
		{Address: "localhost", User: "test", Database: "/db", Name: string([]byte{0xff})},
	} {
		if r, err := Start(t.Context(), cfg); err == nil || r != nil {
			t.Fatal("accepted invalid collector config")
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
	if dsn == "" {
		t.Skip("requires isolated Firebird 5")
	}
	u, err := url.Parse("firebird://" + strings.TrimPrefix(dsn, "firebird://"))
	if err != nil {
		t.Fatal(err)
	}
	password, _ := u.User.Password()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	r, err := Start(ctx, Config{Address: u.Host, User: u.User.Username(), Password: password, Database: u.Path, Name: "firebirdotel-live-test"})
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
			t.Fatal("collector did not start", e, r.Wait(ctx))
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
	if err := db.QueryRowContext(ctx, "select count(*) from OTEL_A").Scan(&value); err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for !(found["OTEL_OUTER"] && found["OTEL_NESTED_A"] && found["OTEL_DOUBLE"] && found["OTEL_A_CHANGED"] && found["SELECT RDB$DATABASE"] && found["expression alias"]) {
		select {
		case e, ok := <-r.Events():
			if !ok {
				t.Fatal("collector ended", r.Wait(ctx), found)
			}
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
}
