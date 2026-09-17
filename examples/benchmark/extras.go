package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/Makarechi/firebirdsql-otel/metadata"
	"github.com/Makarechi/firebirdsql-otel/monitoring"
	collector "github.com/Makarechi/firebirdsql-otel/trace"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type extras struct {
	post  func() error
	wait  func(int) error
	close func() error
	bytes atomic.Int64
}

func newExtras(ctx context.Context, mode, dsn string, business *sql.DB) (*extras, error) {
	e := &extras{post: func() error { return nil }, wait: func(int) error { return nil }, close: func() error { return nil }}
	switch mode {
	case "metadata-cached", "metadata-cold", "monitoring":
		db, err := sql.Open("firebirdsql", dsn)
		if err != nil {
			return nil, err
		}
		db.SetMaxOpenConns(1)
		e.close = db.Close
		if strings.HasPrefix(mode, "metadata") {
			r, err := metadata.New(db, metadata.Config{Database: "synthetic"})
			if err != nil {
				db.Close()
				return nil, err
			}
			e.post = func() error {
				if mode == "metadata-cold" {
					if err := r.Invalidate("benchmark"); err != nil {
						return err
					}
				}
				g, err := r.Read(ctx, metadata.Object{Name: "OTEL_OUTER", Type: 5})
				if err == nil {
					b, _ := json.Marshal(g)
					e.bytes.Add(int64(len(b)))
				}
				return err
			}
		} else {
			var attachment int64
			if err := business.QueryRowContext(ctx, "SELECT CURRENT_CONNECTION FROM RDB$DATABASE").Scan(&attachment); err != nil {
				db.Close()
				return nil, err
			}
			r, err := monitoring.New(db, 128)
			if err != nil {
				db.Close()
				return nil, err
			}
			e.post = func() error {
				s, err := r.Read(ctx, monitoring.Scope{AttachmentID: attachment})
				if err == nil {
					b, _ := json.Marshal(s)
					e.bytes.Add(int64(len(b)))
				}
				return err
			}
		}
	case "trace":
		u, err := url.Parse(dsn)
		if err != nil {
			return nil, err
		}
		password, _ := u.User.Password()
		r, err := collector.Start(ctx, collector.Config{Address: u.Host, User: u.User.Username(), Password: password, Database: u.Path, Name: "firebirdotel-benchmark"})
		if err != nil {
			return nil, err
		}
		stopCollector := func() error {
			stop, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			return r.Shutdown(stop)
		}
		select {
		case event, ok := <-r.Events():
			if !ok || event.Phase != "ready" {
				_ = stopCollector()
				return nil, fmt.Errorf("trace collector not ready")
			}
		case <-ctx.Done():
			_ = stopCollector()
			return nil, ctx.Err()
		}
		var finished atomic.Int64
		changed := make(chan struct{}, 1)
		drained := make(chan struct{})
		initialized := make(chan struct{})
		var initializedOnce sync.Once
		go func() {
			defer close(drained)
			for event := range r.Events() {
				b, _ := json.Marshal(event)
				e.bytes.Add(int64(len(b)))
				if event.Kind == "lifecycle" && event.Phase == "trace_init" {
					initializedOnce.Do(func() { close(initialized) })
				}
				if event.Kind == "procedure" && event.Name == "OTEL_REPORT" && event.Phase == "finish" {
					finished.Add(1)
					select {
					case changed <- struct{}{}:
					default:
					}
				}
			}
		}()
		var once sync.Once
		var closeErr error
		e.close = func() error {
			once.Do(func() {
				closeErr = stopCollector()
				<-drained
			})
			return closeErr
		}
		select {
		case <-initialized:
		case <-drained:
			_ = e.close()
			return nil, fmt.Errorf("trace collector ended before initialization")
		case <-ctx.Done():
			_ = e.close()
			return nil, ctx.Err()
		}
		e.wait = func(count int) error {
			for finished.Load() < int64(count) {
				select {
				case <-changed:
				case <-drained:
					return fmt.Errorf("trace collector ended before catch-up")
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}
	}
	return e, nil
}

func restartTraceAfterWarmup(previous *extras, start func() (*extras, error)) (*extras, error) {
	if err := previous.close(); err != nil {
		return nil, err
	}
	return start()
}
