// Package trace provides an experimental in-process Firebird Trace collector.
// It never starts a trace session from an SQL callback.
package trace

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Makarechi/firebirdsql-otel/internal/traceparse"
	"github.com/nakagami/firebirdsql"
)

type Event = traceparse.Event
type Table = traceparse.Table

type Config struct {
	Address, User, Password, Database string
	// Name is an operator-chosen, non-sensitive session name, useful for diagnostics.
	Name   string
	Buffer int
}

type traceSession interface {
	WaitStringsContext(context.Context, chan string) error
	CloseContext(context.Context) error
}

type traceManager interface {
	StartWithNameContext(context.Context, string, string) (traceSession, error)
}

type driverTraceManager struct{ *firebirdsql.TraceManager }

func (m driverTraceManager) StartWithNameContext(ctx context.Context, name, config string) (traceSession, error) {
	return m.TraceManager.StartWithNameContext(ctx, name, config)
}

var newTraceManager = func(address, user, password string) (traceManager, error) {
	m, err := firebirdsql.NewTraceManager(address, user, password, firebirdsql.GetDefaultServiceManagerOptions())
	if err != nil {
		return nil, err
	}
	return driverTraceManager{m}, nil
}

type Runtime struct {
	events      chan Event
	done        chan struct{}
	cancel      context.CancelFunc
	session     traceSession
	closeOnce   sync.Once
	closeDone   chan struct{}
	discard     chan struct{}
	discardOnce sync.Once

	mu       sync.Mutex
	err      error
	stopping bool
}

func Start(ctx context.Context, c Config) (*Runtime, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.Address == "" || c.User == "" || c.Database == "" || c.Name == "" {
		return nil, errors.New("trace: address, user, database and session name required")
	}
	if len(c.Address) > 512 || len(c.User) > 256 || len(c.Password) > 4096 || len(c.Database) > 1024 || len(c.Name) > 128 {
		return nil, errors.New("trace: configuration limits exceeded")
	}
	if !utf8.ValidString(c.Address) || !utf8.ValidString(c.User) || !utf8.ValidString(c.Password) || !utf8.ValidString(c.Database) || !utf8.ValidString(c.Name) {
		return nil, errors.New("trace: invalid configuration encoding")
	}
	if strings.ContainsAny(c.Name, "\r\n") {
		return nil, errors.New("trace: invalid session name")
	}
	filter, err := databaseFilter(c.Database)
	if err != nil {
		return nil, err
	}
	if c.Buffer == 0 {
		c.Buffer = 64
	}
	if c.Buffer < 1 || c.Buffer > 256 {
		return nil, errors.New("trace: invalid queue bound")
	}
	manager, err := newTraceManager(c.Address, c.User, c.Password)
	if err != nil {
		return nil, errors.New("trace: manager creation failed")
	}
	session, err := manager.StartWithNameContext(ctx, c.Name, serverConfig(filter))
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("trace: session start failed")
	}
	runCtx, cancel := context.WithCancel(context.Background())
	r := &Runtime{
		events:    make(chan Event, c.Buffer),
		done:      make(chan struct{}),
		cancel:    cancel,
		session:   session,
		closeDone: make(chan struct{}),
		discard:   make(chan struct{}),
	}
	r.events <- Event{Source: "trace", Correlation: "unmatched", Kind: "lifecycle", Phase: "ready"}
	go r.run(runCtx)
	return r, nil
}

func serverConfig(filter string) string {
	return fmt.Sprintf(`database = %s {
 enabled = true
 log_statement_start = true
 log_statement_finish = true
 log_procedure_start = true
 log_procedure_finish = true
 log_function_start = true
 log_function_finish = true
 log_trigger_start = true
 log_trigger_finish = true
 print_plan = true
 print_perf = true
 time_threshold = 0
 max_sql_length = %d
 max_arg_length = 1
 max_arg_count = 1
}
`, filter, traceparse.MaxSQL)
}

func (r *Runtime) run(ctx context.Context) {
	defer close(r.done)
	defer close(r.events)
	defer r.cancel()

	raw := make(chan string)
	finished := make(chan error, 1)
	go func() {
		finished <- r.session.WaitStringsContext(ctx, raw)
		close(raw)
	}()

	parser := traceparse.New()
	emit := func(events []Event) bool {
		for _, event := range events {
			select {
			case <-r.discard:
				continue
			default:
			}
			select {
			case r.events <- event:
			case <-r.discard:
				continue
			case <-ctx.Done():
				return false
			}
		}
		return true
	}
	idle := time.NewTimer(250 * time.Millisecond)
	defer idle.Stop()
	for {
		select {
		case <-idle.C:
			if !emit(parser.FlushFinished()) {
				r.finish(nil)
				return
			}
			idle.Reset(250 * time.Millisecond)
		case chunk, ok := <-raw:
			if !ok {
				readErr := <-finished
				r.finish(readErr)
				emit(parser.Flush())
				return
			}
			// WaitStringsContext uses Firebird's isc_info_svc_line API and returns
			// one line without its delimiter. Restore that documented delimiter.
			if !emit(parser.Feed(chunk + "\n")) {
				r.finish(nil)
				return
			}
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(250 * time.Millisecond)
		}
	}
}

func (r *Runtime) finish(readErr error) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r.requestClose(cleanupCtx)
	select {
	case <-r.closeDone:
	case <-cleanupCtx.Done():
		r.mu.Lock()
		r.err = errors.Join(r.err, errors.New("trace: session cleanup timed out"))
		r.mu.Unlock()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if readErr != nil && !r.stopping {
		r.err = errors.Join(r.err, errors.New("trace: stream ended with an error"))
	}
}

func (r *Runtime) requestClose(ctx context.Context) {
	r.closeOnce.Do(func() {
		go func() {
			if err := r.session.CloseContext(ctx); err != nil {
				r.mu.Lock()
				r.err = errors.Join(r.err, errors.New("trace: session cleanup failed"))
				r.mu.Unlock()
			}
			close(r.closeDone)
		}()
	})
}

func (r *Runtime) Events() <-chan Event { return r.events }

// Shutdown stops the server-side Trace session, drains its final records and
// releases the service connection. The context bounds the entire cleanup.
func (r *Runtime) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	r.stopping = true
	r.mu.Unlock()
	r.discardOnce.Do(func() { close(r.discard) })
	r.requestClose(ctx)
	select {
	case <-r.done:
		r.mu.Lock()
		defer r.mu.Unlock()
		return errors.Join(r.err, ctx.Err())
	case <-ctx.Done():
		r.cancel()
		return ctx.Err()
	}
}

func (r *Runtime) Wait(ctx context.Context) error {
	select {
	case <-r.done:
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// databaseFilter escapes the SIMILAR TO expression, then its trace-config container.
// Firebird uses backslash for the regex escape and doubled braces in config values.
func databaseFilter(path string) (string, error) {
	if path == "" || len(path) > 1024 || !utf8.ValidString(path) {
		return "", errors.New("trace: invalid database path")
	}
	var pattern strings.Builder
	for _, r := range path {
		if unicode.IsControl(r) || r == '"' {
			return "", errors.New("trace: unsupported database path character")
		}
		if strings.ContainsRune(`[]()|^-+*%_?{}\`, r) {
			pattern.WriteByte('\\')
		}
		pattern.WriteRune(r)
	}
	encoded := strings.NewReplacer("{", "{{", "}", "}}").Replace(pattern.String())
	return `"` + encoded + `"`, nil
}
