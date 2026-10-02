package trace

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/nakagami/firebirdsql"
)

// These Services API tags are defined by Firebird's ibase.h. The driver's
// TraceSession only exposes line-at-a-time reads; the same public low-level
// service connection supports isc_info_svc_to_eof buffered reads.
const (
	traceStartAction byte = 22
	traceStopAction  byte = 23
	traceIDTag       byte = 1
	traceNameTag     byte = 2
	traceConfigTag   byte = 3
)

var traceReply = regexp.MustCompile(`^Trace session ID ([0-9]+) (started|stopped)$`)

type bufferedTraceManager struct {
	address, user, password string
	options                 firebirdsql.ServiceManagerOptions
}

func (m bufferedTraceManager) connect(ctx context.Context) (*firebirdsql.ServiceManager, error) {
	return firebirdsql.NewServiceManagerContext(ctx, m.address, m.user, m.password, m.options)
}

func (m bufferedTraceManager) StartWithNameContext(ctx context.Context, name, config string) (_ traceSession, err error) {
	if len(name) > 65535 || len(config) > 65535 {
		return nil, errors.New("trace: session name or configuration too long")
	}
	svc, err := m.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, svc.CloseContext(ctx))
		}
	}()
	spb := firebirdsql.NewXPBWriterFromTag(traceStartAction).
		PutString(traceNameTag, name).
		PutString(traceConfigTag, config)
	if err = svc.ServiceStartContext(ctx, spb.Bytes()); err != nil {
		return nil, err
	}
	reply, _, err := svc.GetStringContext(ctx)
	if err != nil {
		return nil, err
	}
	id, err := parseTraceReply(reply, "started", 0)
	if err != nil {
		return nil, err
	}
	return &bufferedTraceSession{manager: m, svc: svc, id: id}, nil
}

func parseTraceReply(reply, action string, expected int32) (int32, error) {
	match := traceReply.FindStringSubmatch(strings.TrimSpace(reply))
	if len(match) != 3 || match[2] != action {
		return 0, errors.New("trace: unexpected session response")
	}
	id, err := strconv.ParseInt(match[1], 10, 32)
	if err != nil || id <= 0 || expected != 0 && int32(id) != expected {
		return 0, errors.New("trace: invalid session ID in response")
	}
	return int32(id), nil
}

type bufferedTraceSession struct {
	manager   bufferedTraceManager
	svc       *firebirdsql.ServiceManager
	id        int32
	closeOnce sync.Once
	closeErr  error
}

func (s *bufferedTraceSession) WaitChunksContext(ctx context.Context, result chan string) error {
	chunks := make(chan []byte, 8)
	finished := make(chan error, 1)
	go func() {
		finished <- s.svc.WaitBufferContext(ctx, chunks)
		close(chunks)
	}()
	for chunk := range chunks {
		select {
		case result <- string(chunk):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return <-finished
}

func (s *bufferedTraceSession) CloseContext(ctx context.Context) error {
	s.closeOnce.Do(func() {
		control, err := s.manager.connect(ctx)
		if err == nil {
			spb := firebirdsql.NewXPBWriterFromTag(traceStopAction).PutInt32(traceIDTag, s.id)
			err = control.ServiceStartContext(ctx, spb.Bytes())
			if err == nil {
				var reply string
				reply, _, err = control.GetStringContext(ctx)
				if err == nil {
					_, err = parseTraceReply(reply, "stopped", s.id)
				}
			}
			err = errors.Join(err, control.CloseContext(ctx))
		}
		s.closeErr = errors.Join(err, s.svc.CloseContext(ctx))
	})
	return s.closeErr
}
