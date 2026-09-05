# Diagnostic sources and ownership

## Metadata

Create a reader with a diagnostic `*sql.DB` and a logical database identity:

```go
reader, err := metadata.New(diagnosticDB, metadata.Config{
    Database: "billing", SchemaVersion: "migration-42",
    TTL: time.Minute, MaxEntries: 64, MaxNodes: 128, MaxDepth: 8,
})
graph, err := reader.Read(ctx, metadata.Object{Name: "BILL_UPDATE", Type: 5})
```

Names are exact system-catalog names (normally uppercase for unquoted objects).
The key includes database, schema version, type, name and package. Graphs are copies;
caller mutations do not change cached values. TTL expiry, oldest-entry eviction,
node/edge/depth limits and cycle detection bound memory/work. Call `Invalidate`
after migrations; an in-flight old read cannot repopulate the new schema generation.
Errors are returned to the diagnostic caller; no business callback invokes this reader.

`RDB$PACKAGE_NAME` qualifies the depended-on routine. Dependencies of packaged bodies
are represented under object type 19 (package body). A packaged routine root explicitly
returns `Scope=package_body`; this does not claim routine-level precision. Views,
functions, triggers and other Firebird catalog object types stay distinct.

All graphs say `source=metadata`, `executed=unknown`, `correlation=unmatched`.
Untaken IF branches can be present; dynamic EXECUTE STATEMENT targets can be absent.
Graphs are never turned into execution spans. The reader may conservatively set
`Truncated` at a depth boundary even when that object has no further dependencies.

## MON$ snapshots

Use a separate diagnostic pool with sufficient, minimally scoped visibility:

```go
reader, err := monitoring.New(diagnosticDB, 128)
snapshot, err := reader.Read(ctx, monitoring.Scope{
    AttachmentID: targetAttachment, StatementID: optionalStatement,
})
```

A nonzero attachment is mandatory. The reader rejects the target attachment itself
as a diagnostic connection. Each call owns and completes one transaction containing
queries to MON$STATEMENTS, MON$CALL_STACK, MON$COMPILED_STATEMENTS, MON$TABLE_STATS and
MON$RECORD_STATS. Compiled statement details cover the scoped top-level statements;
the call stack separately describes sampled routines. SQL and plan BLOBs are not read.

The snapshot is fixed from the first MON$ query through transaction completion, even
at read committed isolation. A second Read uses a new transaction. Only this reader's
transaction is completed; never pass a business transaction to it. Queries are SELECT-only
but deliberately use an ordinary transaction: firebirdsql v0.9.20 retains its read-only
transaction setting for subsequent implicit transactions after a read-only BeginTx.
The live integration test verified that catalog reads do not leave that setting in the pool.

Results say `source=monitoring`, `correlation=scoped`, and carry the requested scope.
Attachment table counters aggregate concurrent statements. No per-query attribution
is inferred from their deltas. Short calls may be missed, and an empty result does not
prove inactivity: permissions may limit visibility. Object/attachment identifiers are
not metric dimensions. Each collection has a bounded row count and a truncation flag.

## Experimental Trace collector

Build `go build -o firebirdotel-trace ./cmd/firebirdotel-trace`, then explicitly start:

```go
runtime, err := trace.Start(ctx, trace.Config{
    Executable: "/absolute/path/firebirdotel-trace",
    Address: "localhost:3050", User: diagnosticUser, Password: password,
    Database: "/var/lib/firebird/data/app.fdb", Name: "billing-diagnostic-42",
})
for event := range runtime.Events() {
    // Store/inspect typed, sanitized events in a bounded diagnostic sink.
}
err = runtime.Wait(ctx)
// On early exit: runtime.Shutdown(shutdownContext).
```

The worker uses the existing driver's NewTraceManager, StartWithName and WaitStrings.
It requests start and finish events with time_threshold=0, plans and performance/table
counters. Only a literal database path is accepted, not a general config or pattern.
Statements are sanitized before IPC/event queuing. Procedure/function/trigger names,
page counters and per-table counters are typed; tables are summaries, not timed spans.
Classic PLAN lines are sanitized; other plan forms are omitted conservatively.

Names and table identities are schema metadata, not secret argument values. Client
SQL, bind parameters, connection strings, user/process details and raw error lines
are not forwarded. SQL record staging and lines are bounded at 64 KiB, SQL output
at 4096 bytes, table summaries at 64, nesting at 64, active attachment/transaction
scopes at 64, and the public queue at 1–256 events (64 by default).

The worker's WaitStrings channel is an **unbuffered raw transport handoff** imposed by
the driver API. It exists only in the disposable worker; the public queue and IPC
contain sanitized records. The upstream driver has its own wire buffers outside the
parser's limits. Trace data may already contain sensitive text on the server and in
the encrypted transport before the worker sees it. In Firebird, max_arg_count=0 means
unlimited, not disabled. The supplied config limits argument count/length but still
relies on discarding argument lines; it does not promise server-side redaction.

Matching uses observed ordering within attachment/transaction and statement identity
where available. Correlation remains `heuristic`, never `exact`. Unknown events,
truncation, overflow, mismatched finishes and stream loss invalidate the matching
state and mark subsequent records incomplete. Recursion is stack-based. Sequence IDs
are local to one collector; timestamps retain the server's local text without inventing
a timezone. Neither a fully observed pair nor the presence of parent IDs proves a
complete PSQL execution tree. Do not attach server events as exact HTTP children.

**This is not a production in-process collector.** Upstream startup/read/stop calls
lack cancellable lifecycle guarantees. RunWorker is the helper entry point only;
applications should use the supervised process API. Shutdown sends an interrupt,
allows one second for Stop/read completion/Close, then kills a blocked worker. Tests
cover a nonresponsive worker, a full event queue, and real Firebird session shutdown.
A forced kill reports failure/cleanup uncertainty. It releases local process/socket
resources, but cannot guarantee server-side session removal. Inspect/stop sessions by
the operator-chosen name before restarting after a forced shutdown. There is no
automatic reconnect that silently reconstructs an allegedly complete tree.

## Manual Profiler example

`go run ./examples/profiler` runs against the synthetic fixture using FIREBIRD_TEST_DSN.
It starts RDB$PROFILER with DETAILED_REQUESTS, reads the selectable procedure to EOF,
closes rows, finishes/flushed the session, then reads the snapshot outside an old
snapshot transaction. Every command uses one pinned sql.Conn. The error path attempts
cleanup with a separate bounded context. The example reports only session identity,
row count and detailed request count, with source=profiler/correlation=scoped.

Profiler's default is aggregation; detailed requests can generate substantial data.
Autonomous flush changes snapshot visibility. PLG$ profiler tables can store raw SQL
on the server, so use this example on synthetic data. This example is not a second
Profiler SDK or automatic profiling of production traffic.

## Sources

- [Firebird 5 dependencies](https://www.firebirdsql.org/file/documentation/chunk/en/refdocs/fblangref50/fblangref-appx04-dependencies.html)
- [Firebird 5 monitoring semantics](https://firebirdsql.org/file/documentation/chunk/en/refdocs/fblangref50/fblangref50-appx05-montables.html)
- [Firebird 5.0.3 Trace output implementation](https://github.com/FirebirdSQL/firebird/blob/v5.0.3/src/utilities/ntrace/TracePluginImpl.cpp)
- [Firebird 5.0.3 profiler API](https://github.com/FirebirdSQL/firebird/blob/v5.0.3/doc/sql.extensions/README.profiler.md)
- [Pinned driver Trace lifecycle](https://github.com/nakagami/firebirdsql/blob/v0.9.20/trace_manager.go)
