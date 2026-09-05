package firebirdotel

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"regexp"

	"github.com/nakagami/firebirdsql"
	"go.opentelemetry.io/otel/attribute"
)

// ErrInstrumentation identifies a failure to initialize telemetry, not a database error.
var ErrInstrumentation = errors.New("firebirdotel: instrumentation failure")
var sqlStatePattern = regexp.MustCompile(`^[0-9A-Z]{5}$`)

// ErrorAttributes returns bounded codes, never err.Error(), Message, Params or wrapped messages.
func ErrorAttributes(err error) []attribute.KeyValue {
	kind := outcome(err)
	if kind == "ok" || kind == "fallback" {
		return nil
	}
	a := []attribute.KeyValue{attribute.String("error.type", kind)}
	var fb *firebirdsql.FbError
	if errors.As(err, &fb) && fb != nil {
		a = append(a, attribute.Int64("firebird.error.sqlcode", int64(fb.SQLCode)))
		if sqlStatePattern.MatchString(fb.SQLState) {
			a = append(a, attribute.String("firebird.error.sqlstate", fb.SQLState))
		}
		n := len(fb.GDSCodes)
		if n > 16 {
			n = 16
		}
		codes := append([]int(nil), fb.GDSCodes[:n]...)
		a = append(a, attribute.IntSlice("firebird.error.gds_codes", codes))
	}
	return a
}
func outcome(err error) string {
	switch {
	case err == nil || errors.Is(err, io.EOF):
		return "ok"
	case errors.Is(err, driver.ErrSkip):
		return "fallback"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, ErrInstrumentation):
		return "instrumentation"
	case errors.Is(err, driver.ErrBadConn):
		return "connection"
	case errors.Is(err, errors.ErrUnsupported):
		return "unsupported"
	}
	var fb *firebirdsql.FbError
	if errors.As(err, &fb) {
		return "server"
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return "network"
	}
	return "unknown"
}
