// Package trace provides an experimental, process-isolated Firebird Trace collector.
// It never starts a trace session from an SQL callback.
package trace

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/Makarechi/firebirdsql-otel/internal/traceparse"
)

type Event = traceparse.Event
type Table = traceparse.Table
type Config struct {
	// Executable is the built cmd/firebirdotel-trace binary (an explicit trusted path).
	Executable                        string
	Address, User, Password, Database string
	// Name is an operator-chosen, non-sensitive session name, useful for orphan cleanup.
	Name   string
	Buffer int
}

// JSON may expand each byte to six characters; include framing/key overhead.
const maxWorkerConfig = 6*(512+256+4096+1024+128) + 128

type workerConfig struct{ Address, User, Password, Database, Name string }
type Runtime struct {
	events chan Event
	done   chan struct{}
	cancel context.CancelFunc
	mu     sync.Mutex
	err    error
}

func Start(ctx context.Context, c Config) (*Runtime, error) {
	if c.Executable == "" || c.Address == "" || c.User == "" || c.Database == "" || c.Name == "" {
		return nil, errors.New("trace: executable, address, user, database and session name required")
	}
	if len(c.Address) > 512 || len(c.User) > 256 || len(c.Password) > 4096 || len(c.Database) > 1024 || len(c.Name) > 128 {
		return nil, errors.New("trace: configuration limits exceeded")
	}
	if c.Buffer == 0 {
		c.Buffer = 64
	}
	if c.Buffer < 1 || c.Buffer > 256 {
		return nil, errors.New("trace: invalid queue bound")
	}
	data, err := json.Marshal(workerConfig{c.Address, c.User, c.Password, c.Database, c.Name})
	if err != nil || len(data) > maxWorkerConfig {
		return nil, errors.New("trace: encode configuration failed")
	}
	runCtx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(runCtx, c.Executable, "--worker")
	cmd.Stdin = bytes.NewReader(data) // Credentials never appear in argv or environment.
	cmd.Stderr = io.Discard           // Upstream error strings may contain credentials or raw SQL.
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
	cmd.WaitDelay = time.Second
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, errors.New("trace: output pipe failed")
	}
	if err := cmd.Start(); err != nil {
		cancel()
		return nil, errors.New("trace: worker start failed")
	}
	r := &Runtime{events: make(chan Event, c.Buffer), done: make(chan struct{}), cancel: cancel}
	go func() {
		defer close(r.done)
		defer close(r.events)
		defer cancel()
		scan := bufio.NewScanner(stdout)
		scan.Buffer(make([]byte, 4096), traceparse.MaxRecord)
		var readErr error
		for scan.Scan() {
			var e Event
			if err := json.Unmarshal(scan.Bytes(), &e); err != nil {
				readErr = errors.New("trace: invalid worker record")
				cancel()
				break
			}
			select {
			case r.events <- e:
			case <-runCtx.Done():
				cancel()
				goto done
			}
		}
		if scan.Err() != nil {
			readErr = errors.New("trace: worker record exceeds bound or stream failed")
			cancel()
		}
	done:
		// Continue draining stdout while Wait performs bounded interrupt/kill, even if the consumer stopped.
		drained := make(chan struct{})
		go func() { _, _ = io.Copy(io.Discard, stdout); close(drained) }()
		waitErr := cmd.Wait()
		<-drained
		r.mu.Lock()
		defer r.mu.Unlock()
		if readErr != nil {
			r.err = readErr
		} else if waitErr != nil && (cmd.ProcessState == nil || !cmd.ProcessState.Success()) {
			r.err = errors.New("trace: worker failed; server session cleanup may be required")
		}
	}()
	return r, nil
}
func (r *Runtime) Events() <-chan Event { return r.events }

// Shutdown bounds local process cleanup. A forced kill cannot guarantee server session removal.
// Use the configured Name to inspect/stop orphan sessions before restarting a collector.
func (r *Runtime) Shutdown(ctx context.Context) error {
	r.cancel()
	select {
	case <-r.done:
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.err
	case <-ctx.Done():
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
