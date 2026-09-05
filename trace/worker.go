package trace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/Makarechi/firebirdsql-otel/internal/traceparse"
	"github.com/nakagami/firebirdsql"
)

// RunWorker is the isolated helper entry point, not an in-process production collector.
// The driver lacks cancellable startup/read/stop guarantees. The parent must supervise this process.
func RunWorker(ctx context.Context, input io.Reader, output io.Writer) error {
	var cfg workerConfig
	if err := json.NewDecoder(io.LimitReader(input, 8192)).Decode(&cfg); err != nil {
		return errors.New("trace: invalid worker input")
	}
	// A literal database path avoids trace config/regular expression injection.
	if cfg.Database == "" || strings.ContainsAny(cfg.Database, "\r\n{}\"<>|*?[]()^$%") || strings.ContainsAny(cfg.Name, "\r\n") {
		return errors.New("trace: database filter must be a literal path")
	}
	serverConfig := fmt.Sprintf(`database = %s {
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
 max_sql_length = 65536
 max_arg_length = 1
 max_arg_count = 1
}
`, cfg.Database)
	manager, err := firebirdsql.NewTraceManager(cfg.Address, cfg.User, cfg.Password, firebirdsql.GetDefaultServiceManagerOptions())
	if err != nil {
		return errors.New("trace: manager creation failed")
	}
	session, err := manager.StartWithName(cfg.Name, serverConfig)
	if err != nil {
		return errors.New("trace: session start failed")
	}
	// WaitStrings requires a raw string channel. It is unbuffered and confined to this
	// disposable worker; records are sanitized before the bounded public event queue/IPC.
	raw := make(chan string)
	finished := make(chan error, 1)
	go func() { finished <- session.WaitStrings(raw); close(raw) }()
	parser := traceparse.New()
	encoder := json.NewEncoder(output)
	emit := func(events []Event) error {
		for _, e := range events {
			if err := encoder.Encode(e); err != nil {
				return errors.New("trace: output failed")
			}
		}
		return nil
	}
	if err := emit([]Event{{Source: "trace", Correlation: "unmatched", Kind: "lifecycle", Phase: "ready"}}); err != nil {
		return err
	}
	cancelled := ctx.Done()
	stopped := false
	for {
		select {
		case chunk, ok := <-raw:
			if !ok {
				readErr := <-finished
				if err := emit(parser.Flush()); err != nil {
					return err
				}
				closeErr := session.Close() // Only after WaitStrings has finished; no concurrent conn mutation.
				if readErr != nil || closeErr != nil {
					return errors.New("trace: session ended with an error")
				}
				return nil
			}
			if err := emit(parser.Feed(chunk + "\n")); err != nil {
				return err
			}
		case <-cancelled:
			cancelled = nil
			if !stopped {
				stopped = true
				if err := session.Stop(); err != nil {
					return errors.New("trace: stop failed; inspect server session")
				}
			}
			// Keep consuming until the blocked reader returns; the parent kills on its deadline.
		}
	}
}
