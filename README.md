# firebirdsql-otel

OpenTelemetry instrumentation for the Go Firebird driver
[`nakagami/firebirdsql`](https://github.com/nakagami/firebirdsql) **v0.9.20**.
Uses OpenTelemetry Go **v1.44.0** and requires Go 1.25 or later.

## Quick start

Initialize OpenTelemetry in your application as usual. Enable instrumentation
once at startup, then use the returned driver name in your existing database setup:

```go
driverName, err := firebirdotel.Instrument()
if err != nil {
    return err
}

// Your ordinary database/sql setup.
db, err := sql.Open(driverName, dsn)
if err != nil {
    return err
}
defer db.Close()

// Existing queries use the request context.
_, err = db.ExecContext(ctx, "execute procedure BILL_UPDATE(?)", id)
```

Imports: `database/sql` and
`firebirdotel "github.com/Makarechi/firebirdsql-otel"`.
See the [complete example](examples/safe/main.go).

`Instrument()` needs no configuration object. It uses the application's global
OTel providers and adds child spans to the trace in the query context, records
operation duration metrics, and sanitizes SQL before exporting it. Attributes
include `db.system.name=firebirdsql`, `db.operation.name`, and sanitized
`db.query.text`. Bind values and raw error messages are never exported.

Your application keeps its connection settings, pool limits, startup checks and
shutdown. Reuse the returned driver name; registration lasts until process exit.
Frameworks such as GORM can use the same name as their `DriverName`.
Instrumentation is attached before creating `*sql.DB`; database/sql cannot change
the driver of an already-open pool.

## Optional features

- [Frameworks, connectors and pool statistics](docs/instrumentation.md).
- [Custom telemetry settings, privacy and compatibility](docs/client.md).
  Use `RegisterWithConfig` only when you need overrides such as pool labels or
  specific providers. Existing `Open`/`Register` APIs retain their legacy behavior.
- [Server diagnostics](docs/diagnostics.md): metadata, MON$, Trace and Profiler.
  These are separate opt-in tools; ordinary instrumentation never starts them.

## Development

```sh
go test -race ./...
go vet ./...
```

CI also tests against an isolated Firebird 5.0.3 database.
See [validation and measured costs](docs/validation.md),
[design decisions](docs/adr-001-safe-client.md), and
[handoff coverage](docs/handoff-01.md).
