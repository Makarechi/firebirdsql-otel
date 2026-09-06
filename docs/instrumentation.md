# Add instrumentation to an existing database setup

`firebirdsql-otel` instruments the native Firebird driver. Your application or
framework continues to own its DSN, pool settings, startup checks and shutdown.

Initialize your application's OpenTelemetry providers, then register once at
startup and pass the returned driver name to your existing database setup:

```go
driverName, err := firebirdotel.RegisterWithConfig(firebirdotel.Config{})
if err != nil {
    return err
}

// Existing database/sql setup; only the driver name changes.
db, err := sql.Open(driverName, dsn)
if err != nil {
    return err
}
defer db.Close()
// Keep your existing pool settings, startup checks and query calls here.
```

The zero configuration uses global providers and enables safe query spans and
operation duration metrics. SQL literals and comments are sanitized; bind values
and raw error messages are not exported. Creating a registration makes no
Firebird connection. Registrations live until process exit, so reuse the returned
name rather than registering on every request or pool reopen. Separate telemetry
configurations can use separate names; at most 1000 safe names can be registered.

With GORM, pass the name through the Firebird dialector's `DriverName`. Disable
any existing ORM tracing plugin to avoid duplicate spans and independently
configure application/ORM logging: the instrumented driver cannot sanitize logs
produced by another layer. Existing plain SQL users continue to call `sql.Open`.

If your application already constructs a `driver.Connector`, use
`firebirdotel.WrapConnector(connector, firebirdotel.Config{})`, then keep the
application's normal `sql.OpenDB` call. The wrapper must be installed before
creating the working `*sql.DB`: database/sql has no public API to replace an
already-open pool's driver. Do not open a replacement pool and copy its settings.

`OpenWithConfig` and `OpenDBWithConfig` remain optional convenience functions.
The legacy `Open`/`Register` APIs retain their original compatibility behavior;
they do not gain the safe profile's privacy policy.

## Optional telemetry settings

`Config` describes telemetry, not database connection or pool configuration.
Use it only for overrides such as providers, logical database aliases, fixed
pool labels or opt-in diagnostic detail. DSN and pool settings remain in their
existing application configuration.

Pool gauges are optional and separate from the operation metrics already
enabled by registration. They observe the application's existing pool:

```go
registration, err := firebirdotel.RegisterDBStatsMetricsWithConfig(db, firebirdotel.Config{})
if err != nil {
    return err
}
defer registration.Unregister() // before closing the pool and providers
```

The executable [safe example](../examples/safe/main.go) uses only database/sql
and the public instrumentation package.
