# firebirdsql-otel

Firebird-specific OpenTelemetry instrumentation for `github.com/nakagami/firebirdsql`
**v0.9.20**, using OpenTelemetry Go **v1.44.0**. Requires Go 1.25 or later.

The original otelsql-compatible API remains available. The new Config API provides
sanitized SQL, procedure-aware names, safe error codes/status, and optional cursor
consumption diagnostics. Metadata, MON$, Trace and Profiler are explicit diagnostic
sources, never hidden work performed by ordinary client calls.

## Add instrumentation

Keep your existing connection configuration, pool settings and lifecycle.
After initializing your application's OpenTelemetry providers, register the
instrumented driver once and use its name in your normal database setup:

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

_, err = db.ExecContext(ctx, "execute procedure BILL_UPDATE(?)", id)
```

The zero `Config` enables safe spans and operation duration metrics through the
global providers. No separate instrumentation configuration function is required.
Registration does not connect to Firebird or configure a pool. Reuse the returned
name: database/sql driver registrations live until process exit.

Frameworks can use the same name as their `DriverName`. Applications that already
have a native connector can instead call `WrapConnector` before their existing
`sql.OpenDB`. An already-open `*sql.DB` cannot have its driver replaced.
See [framework integration and optional pool metrics](docs/instrumentation.md)
and the [standalone example](examples/safe/main.go).

`OpenWithConfig` and `OpenDBWithConfig` remain optional convenience functions.
`Config` is for optional telemetry overrides such as providers, logical database
aliases and fixed pool labels; it does not contain DSNs or pool limits.

## Safe client behavior

Safe client SQL descriptions use **client dialect 3**, which the pinned driver sets
explicitly. Custom drivers/connectors supplied to the safe API must also use client
dialect 3. Quoted object names are preserved; dialect 1 support is not added.

The span is named `EXECUTE PROCEDURE BILL_UPDATE`. SQL and arguments sent to the
database are unchanged. Bind values and error messages are never exported by this
mode. `*firebirdsql.FbError` codes and safe error status are recorded on that SQL
operation's span; the original error is returned intact to the caller.

Use `DiagnosticConfig()` to additionally observe connect, prepare, ping, reset
and result consumption. The consumption span is an INTERNAL child of the query
span. Its duration includes application pauses between reads; it is not Firebird
engine execution time. Its aggregate counters distinguish rows delivered, Next
attempts, EOF, early close and errors. No extra Next or SQL is performed. EOF records exhaustion, while final status also
includes deferred Close errors. Query and caller-supplied transaction cancellation are mirrored until Close through
stoppable subscriptions, then released; cursor spans without recording return raw rows
and skip per-row bookkeeping. EOF from non-row operations is a connection error.
When transaction rows close without EOF or an observable error/cancellation, the
outcome is `transaction_close_unknown`: database/sql hides the private cancellation
used by normal Commit/Rollback, so a driver cannot distinguish that automatic closure
from explicit Rows.Close. No cancellation error is invented and caller behavior is unchanged.

`OpenWithDriverConfig` accepts an explicit `driver.Driver`, a DSN and Config; a
`DriverContext` connector factory is called exactly once. The existing named-driver
`OpenWithDriver` compatibility API is unchanged.

Safe instrumentation never calls arbitrary `driver.Result` methods to collect
telemetry. A rows-affected attribute is available only for a concrete cached
`driver.RowsAffected` value; other result implementations are returned untouched.

`OpenDBWithConfig` accepts an uninstrumented `driver.Connector` and returns
`(*sql.DB, error)`. `WrapConnector` is available for framework integration. Known
safe/otelsql wrappers are rejected to prevent double instrumentation. Arbitrary
third-party connectors must also be uninstrumented; there is no universal API to
detect opaque wrappers. A connector's optional Close capability is preserved,
including its cleanup error; connectors without Close do not acquire that capability.
OpenWithDriverConfig closes a newly created connector if instrumentation setup fails;
OpenDBWithConfig leaves a caller-supplied connector owned by the caller on failure.
The compatibility profile of OpenWithDriverConfig retains DSN-derived defaults.

Inside a pinned `*sql.Conn`, use `firebirdotel.Raw(conn, callback)` to access the
native connection. Do not retain it outside the callback. Optional driver, statement
and column metadata interfaces are preserved exactly, including database/sql fallback.
Next-result-set probes are forwarded once. EOF exhaustion uses the exact sentinel,
matching database/sql; wrapped EOF remains an error. ErrSkip is suppressed only for
an exact sentinel on connection Exec/Query fast paths with database/sql fallback.
Prepared calls and wrapped ErrSkip errors retain their spans and duration metrics. Transaction completion telemetry retains
context values and span parentage without retaining cancellation.

## Configuration and privacy

- `SQLSanitized` (default) removes literals and comments before telemetry construction.
  Malformed/unsupported lexical input fails closed. Parsing is bounded at 64 KiB /
  4096 tokens; exported text is bounded at 4096 bytes. Limits can be lowered.
- `SQLOff` omits query text while retaining safe operation/object summaries.
- `SQLRawExplicit` explicitly permits raw query text within the output bound. **It can
  expose secrets contained in SQL.** Bind arguments and error messages remain suppressed.
- `Connection.ParseDSNNetwork` opts into conservative host/port extraction. Credentials,
  usernames and database file paths are not automatically exported. Explicit Host,
  Port and Namespace override inference; ambiguous authorities are omitted. Explicit
  Host and Namespace must contain valid UTF-8.
- `SpanAttributes` and `MetricAttributes` are separate. These attributes, logical aliases,
  and object hints are trusted caller configuration: do not put secrets in them. Keep
  metric dimensions to a small, stable vocabulary.
- `WithOperationHint(ctx, OperationHint{Procedure: "PACKAGE.PROC"})` can disambiguate a
  selectable procedure. A plain `SELECT FROM NAME` is not automatically labelled a
  procedure. Hints must be object identifiers, not SQL fragments or parameter values.
- `Client.Filter` runs before execution with a bounded safe operation description.
  It filters spans, not metrics; use tail sampling to select completed slow operations.
- Opaque `OTelOptions` are rejected in safe profiles because raw callbacks/SQLCommenter
  cannot be made safe by another attributes callback. Use explicit Config fields.

Safe mode consistently emits the modern database attributes regardless of
`OTEL_SEMCONV_STABILITY_OPT_IN`. Compatibility mode retains otelsql's environment-driven
behavior. Operation/object labels never include parameter values or the whole query.
Fingerprinting uses sanitized normalized SQL; there is no long-lived query/DSN cache.

## Compatibility

Existing `Open`, `OpenCreateDB`, `OpenWithDriver`, `OpenDB`, `OpenDBWithDSN`,
`Register*`, `RegisterDBStatsMetrics`, callbacks and aliases keep their signatures
and behavior. Existing otelsql options remain assignable to `Option`.

```go
db, err := firebirdotel.Open(dsn, firebirdotel.WithTracerProvider(tp))
// Equivalent Config routing:
cfg := firebirdotel.CompatibilityConfig(firebirdotel.WithTracerProvider(tp))
db, err = firebirdotel.OpenWithConfig(dsn, cfg)
```

Compatibility mode keeps its original SQL/error recording policy; it does not
inherit the new privacy guarantees. For frameworks requiring a registered driver
name the existing `Register` APIs are unchanged. `OpenWithConfig` does not register
a new global driver name per pool; `RegisterWithConfig` explicitly registers a
reusable name with the safe profile.

## Explicit diagnostic sources

| Source | Entry point | Meaning |
| --- | --- | --- |
| Client | `OpenWithConfig` | Actual external API calls and optional rows consumption |
| Metadata | `metadata.New(...).Read(...)` | Possible dependencies, `executed=unknown`; never execution spans |
| Monitoring | `monitoring.New(...).Read(...)` | Scoped MON$ snapshots from separate transactions |
| Trace | `trace.Start(...)` and `Runtime.Events()` | Observed server start/finish records; correlation is heuristic/unmatched |
| Profiler | `examples/profiler` | Manual Firebird 5 profiling on one pinned connection |

See [diagnostic semantics and lifecycle](docs/diagnostics.md),
[design decision](docs/adr-001-safe-client.md),
[validation and measured costs](docs/validation.md), and
[handoff coverage](docs/handoff-01.md).

## Development

```sh
go test -race ./...
go vet ./...
go generate ./... # regenerate exact optional interface combinations
```

Ordinary tests require no database. The opt-in integration tests use the synthetic
fixture in `testdata/firebird5/schema.sql`. The CI workflow creates an isolated
Firebird 5.0.3 container, installs the fixture, builds the Trace helper, and runs
the full suite. See [validation](docs/validation.md) for local commands.
