package firebirdotel

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strconv"
	"sync"
)

var safeRegisterMu sync.Mutex

// RegisterWithConfig registers instrumented Firebird and returns a driver name
// for the application's existing sql.Open or framework configuration. It does
// not connect to Firebird, configure a pool, or register pool statistics.
// The zero Config enables safe tracing and operation metrics with global providers.
// Register once at startup per telemetry configuration and reuse the name:
// database/sql registrations last for the lifetime of the process.
// Use Register for the legacy compatibility profile.
func RegisterWithConfig(c Config) (string, error) {
	c, err := normalizeConfig(c)
	if err != nil {
		return "", err
	}
	if c.Profile == Compatibility {
		return "", errors.New("firebirdotel: use Register for compatibility")
	}
	tel, err := newTelemetry(c)
	if err != nil {
		return "", err
	}
	// The pinned driver's registered type only implements Driver.Open. Looking
	// it up through a temporary empty handle performs no network I/O.
	raw, err := sql.Open(DriverName, "")
	if err != nil {
		return "", err
	}
	native := raw.Driver()
	if err := raw.Close(); err != nil {
		return "", err
	}
	return registerSafeDriver(&registeredSafeDriver{native: native, t: tel})
}

func registerSafeDriver(d *registeredSafeDriver) (string, error) {
	safeRegisterMu.Lock()
	defer safeRegisterMu.Unlock()
	used := make(map[string]bool)
	for _, name := range sql.Drivers() {
		used[name] = true
	}
	// Bound process-global registrations, just as the compatibility API does.
	for slot := 0; slot < 1000; slot++ {
		name := DriverName + "-otel-safe-" + strconv.Itoa(slot)
		if !used[name] {
			sql.Register(name, d)
			return name, nil
		}
	}
	return "", errors.New("firebirdotel: safe driver registration limit reached")
}

type registeredSafeDriver struct {
	native driver.Driver
	t      *telemetry
}

func (d *registeredSafeDriver) Open(dsn string) (driver.Conn, error) {
	c, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return c.Connect(context.Background())
}

func (d *registeredSafeDriver) OpenConnector(dsn string) (driver.Connector, error) {
	// A registration can serve several pools. Resolve optional network attributes
	// separately for each DSN without mutating the shared telemetry configuration.
	tel := *d.t
	tel.spanAttrs = append(connectionAttributes(dsn, tel.c.Connection), tel.c.SpanAttributes...)
	base := &dsnConnector{d: d.native, dsn: dsn}
	return &registeredSafeConnector{safeConnector: &safeConnector{Connector: base, t: &tel}, driver: d}, nil
}

func (*registeredSafeDriver) firebirdInstrumentedDriver() {}

type registeredSafeConnector struct {
	*safeConnector
	driver driver.Driver
}

func (c *registeredSafeConnector) Driver() driver.Driver { return c.driver }
