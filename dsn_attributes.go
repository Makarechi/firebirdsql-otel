package firebirdotel

import (
	"net"
	"net/url"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/attribute"
)

func connectionAttributes(dsn string, c ConnectionAttributes) []attribute.KeyValue {
	a := []attribute.KeyValue{attribute.String("db.system.name", "firebirdsql")}
	host, port := c.Host, c.Port
	if c.ParseDSNNetwork && host == "" {
		if h, p, ok := dsnNetwork(dsn); ok {
			host = h
			if port == 0 {
				port = p
			}
		}
	}
	if host != "" {
		a = append(a, attribute.String("server.address", host))
		if port > 0 {
			a = append(a, attribute.Int("server.port", port))
		}
	}
	if c.Namespace != "" {
		a = append(a, attribute.String("db.namespace", c.Namespace))
	}
	return a
}
func dsnNetwork(dsn string) (string, int, bool) {
	if len(dsn) > 8192 {
		return "", 0, false
	}
	if !strings.HasPrefix(dsn, "firebird://") {
		dsn = "firebird://" + dsn
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme != "firebird" || u.User == nil || u.Fragment != "" || u.Opaque != "" || u.Host == "" || u.Path == "" {
		return "", 0, false
	}
	h := u.Hostname()
	if h == "" || len(h) > 253 {
		return "", 0, false
	}
	// Require an unambiguous authority; credentials with reserved characters must be escaped.
	if strings.Count(strings.SplitN(strings.TrimPrefix(dsn, "firebird://"), "/", 2)[0], "@") != 1 {
		return "", 0, false
	}
	if net.ParseIP(h) == nil {
		for _, r := range h {
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '-') {
				return "", 0, false
			}
		}
	}
	p := 3050
	if u.Port() != "" {
		p, err = strconv.Atoi(u.Port())
		if err != nil || p < 1 || p > 65535 {
			return "", 0, false
		}
	}
	if strings.Contains(h, ":") && !strings.HasPrefix(u.Host, "[") {
		return "", 0, false
	}
	return h, p, true
}
