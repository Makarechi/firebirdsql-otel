# firebirdsql-otel

OpenTelemetry instrumentation helpers for the Go Firebird driver
[`github.com/nakagami/firebirdsql`](https://github.com/nakagami/firebirdsql).

This package gives Firebird applications the same ergonomic shape that many
PostgreSQL projects use: open an instrumented `*sql.DB` once, then normal
`ExecContext`, `QueryContext`, `PrepareContext`, transactions, and procedure
calls produce spans automatically.

## Install

```bash
go get github.com/Makarechi/firebirdsql-otel
```

## Usage

```go
package main

import (
	"context"
	"log"

	firebirdotel "github.com/Makarechi/firebirdsql-otel"
)

func main() {
	ctx := context.Background()
	dsn := "sysdba:masterkey@localhost:3050/var/lib/firebird/app.fdb"

	db, err := firebirdotel.Open(dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.ExecContext(ctx, "execute procedure recalculate_invoice(?)", 42)
	if err != nil {
		log.Fatal(err)
	}
}
```

If your framework needs a driver name instead of a ready `*sql.DB`:

```go
driverName, err := firebirdotel.Register()
if err != nil {
	log.Fatal(err)
}

db, err := sql.Open(driverName, dsn)
```

## Metrics

Operation duration metrics are emitted through `otelsql`. You can also register
standard `database/sql` pool statistics:

```go
reg, err := firebirdotel.RegisterDBStatsMetrics(db, dsn)
if err != nil {
	log.Fatal(err)
}
defer reg.Unregister()
```

Pass custom OpenTelemetry providers when your application does not use the
global providers:

```go
db, err := firebirdotel.Open(
	dsn,
	firebirdotel.WithTracerProvider(tracerProvider),
	firebirdotel.WithMeterProvider(meterProvider),
)
```

## What You See

For a stored procedure call from Go, for example:

```sql
execute procedure recalculate_invoice(?)
```

the trace shows the Go application calling Firebird, the elapsed time, the SQL
text when enabled by semantic-convention settings, and any returned error.

The instrumentation does not inspect work inside the Firebird engine. It will
not automatically show which tables, indexes, triggers, or nested procedures
were used inside the stored procedure. That level of detail needs Firebird
server-side tracing or a separate integration with Firebird Trace API output.

## Firebird Service API

The main SQL path is covered by `database/sql` instrumentation. Firebird-specific
administrative APIs such as backup, nbackup, maintenance, user management, trace
sessions, and events are not SQL queries. They use the Firebird Services API or
event protocol, so they need separate wrappers if you want spans around those
operations.

This repository is structured so those wrappers can be added without changing
the SQL instrumentation API.

## Development

Run tests:

```bash
go test ./...
```

The tests use an in-memory mock SQL driver and do not require a running Firebird
server.

