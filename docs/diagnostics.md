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
are represented under object type 19 (package body). A packaged routine root, or traversal broadened from any dependency to a package body,
returns `Scope=package_body`; this does not claim routine-level precision. Views,
functions, triggers and other Firebird catalog object types stay distinct.
A typed `package_body_scope` edge connects each broadened member to its package body;
catalog edges have kind `dependency`. This keeps a packaged root connected without
claiming that package-wide dependencies belong to that individual routine. Bounded
reads order by type, name, package and field before applying the row limit.

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
a visibility query to MON$ATTACHMENTS followed by queries to MON$STATEMENTS, MON$CALL_STACK, MON$COMPILED_STATEMENTS, MON$TABLE_STATS and
MON$RECORD_STATS. Compiled statement details include IDs referenced by both the scoped top-level
statements and their MON$CALL_STACK entries, deduplicated and bounded together.
This includes compiled procedures, functions and triggers reached by those calls. SQL and plan BLOBs are not read.

`CapturedAt` is taken immediately before the first MON$ query, after pool acquisition
and transaction setup. Catalog and monitoring names lose only right-hand ASCII-space
padding; leading whitespace and other characters remain part of the object identity.

The snapshot is fixed from the first MON$ query through transaction completion, even
at read committed isolation. A second Read uses a new transaction. Only this reader's
transaction is completed; never pass a business transaction to it. Queries are SELECT-only
but deliberately use an ordinary transaction: firebirdsql v0.9.21 retains its read-only
transaction setting for subsequent implicit transactions after a read-only BeginTx.
The live integration test verified that catalog reads do not leave that setting in the pool.

Results say `source=monitoring`, `correlation=scoped`, and carry the requested scope.
Attachment table counters aggregate concurrent statements. No per-query attribution
is inferred from their deltas. Short calls may be missed, and an empty result does not
prove inactivity. An absent or invisible target attachment returns
`ErrTargetNotVisible` with unmatched correlation instead of a successful empty
snapshot; the reader cannot distinguish disconnection from insufficient permissions.
For a nonzero StatementID, the existing scoped MON$STATEMENTS read also validates the
attachment/statement pair. An empty result returns `ErrStatementNotVisible` and
unmatched correlation, without extra queries or fabricated idle-state data. Object/attachment identifiers are
not metric dimensions. Each collection has a bounded row count and a truncation flag.

## Nested server spans

Ordinary client instrumentation observes the outer SQL call. Enable server spans
explicitly to see procedures, functions, triggers and SQL statements actually
reported by Firebird Trace beneath that call. The collector runs in the application;
no helper executable is required.

```go
// fbtrace is github.com/Makarechi/firebirdsql-otel/trace.
server, err := fbtrace.NewSpans(fbtrace.SpanConfig{})
if err != nil { return err }
err = server.Start(startupContext, fbtrace.Config{
    Address: "localhost:3050", User: diagnosticUser, Password: password,
    Database: "/var/lib/firebird/data/app.fdb", Name: "billing-primary-diagnostics",
})
if err != nil { return err }

cfg := firebirdotel.SafeConfig()
cfg.ServerTrace = server
driverName, err := firebirdotel.RegisterWithConfig(cfg)
if err != nil { return err }
// Pass driverName to your existing database constructor. It still owns the pool.
```

`NewSpans` may run before connection configuration is loaded; pass the collector
configuration to `Start` later, before serving database traffic. `Start` waits for
readiness, and its context only bounds startup. On shutdown, stop database traffic,
call `server.Shutdown(shutdownContext)` and close pools, then flush and close the OTel
providers. Both client and server spans default to the application's global providers.
A handle that has not been started adds no marker queries. The collector requires
access to Firebird's Services API and trace permission for the observed attachments.

This opt-in mode runs one extra diagnostic SELECT before an eligible Exec/Query,
including prepared executions, on the same physical connection. It uses a fixed
SQL shape, `SELECT 1 FROM RDB$DATABASE`, with a random opaque comment. The token contains
no trace IDs, business arguments or credentials and is not exported in spans.
Business SQL and bind values are unchanged. Ordinary mode adds no queries. Filtered
calls and calls with an unsampled parent do not set a marker; the actual client span's
sampling decision is checked before server export. Marker failures suppress server
correlation and preserve the business operation's result/error. Session and
transaction variables are untouched; collecting business context-variable values
is not enabled.

Observed output can look like:

```text
application request
  EXECUTE PROCEDURE OTEL_OUTER          (client call)
    EXECUTE PROCEDURE OTEL_OUTER        (server execution)
      OTEL_OUTER
        OTEL_NESTED_A
          OTEL_DOUBLE
          OTEL_A_CHANGED
```

The marker selects the client parent without matching names or timing windows.
Text Trace nesting remains `firebird.correlation=heuristic`; it is not a guarantee
of complete PSQL coverage. This does not produce a span for every SQL instruction
inside PSQL. Table counters become span events, not invented timed table spans.
Server time is aligned to the local marker time and marked
`firebird.clock.alignment=marker_estimate`; native timestamp differences determine
duration, without assuming the server's timezone or clock synchronization.

The runtime buffers bounded trees and exports after the outer server execution
finishes and its client span is known. Defaults: 256 pending operations, two-minute
retention, 128 events per tree and 4096 tracked events globally. `MaxPending` and
`Retention` have hard limits of 4096 and five minutes. Gaps, expiration, overflow,
failed markers, unsampled parents and ambiguous overlapping cursors suppress affected
trees. Missing matched children mark exported trees `firebird.incomplete=true`.
`firebird.server.trace.dropped` counts discarded trees by bounded reason. Concurrent
operations on different connections are independent; overlapping open cursors on one
connection are deliberately not attributed. Unregistered traffic is never exported.
Separate runtimes only export their own tokens, even when observing the same database.
During otherwise idle traffic, the collector releases a parsed finish record after
250 ms without requiring another application query. Such a record is marked
incomplete because additional table counters could still arrive; unfinished SQL
and start records are never finalized by the idle timer.

The collector and server-side privacy/lifecycle boundaries below also apply. The
runtime does not reconnect and pretend that a failed stream stayed complete. Use
`server.Wait(ctx)` to observe unexpected collector termination; ordinary client
queries continue and new marker registrations stop after the collector ends.

## Experimental Trace collector

Start the collector explicitly:

```go
runtime, err := trace.Start(ctx, trace.Config{
    Address: "localhost:3050", User: diagnosticUser, Password: password,
    Database: "/var/lib/firebird/data/app.fdb", Name: "billing-diagnostic-42",
})
for event := range runtime.Events() {
    // Store/inspect typed, sanitized events in a bounded diagnostic sink.
}
err = runtime.Wait(ctx)
// On early exit: runtime.Shutdown(shutdownContext).
```

The collector uses the driver's context-aware Trace lifecycle added in v0.9.21.
It requests start and finish events with time_threshold=0, plans and performance/table
counters. Database paths are escaped as literal SIMILAR TO patterns and then encoded for the
Trace configuration container. Wildcards, quantifiers and backslashes cannot broaden
the selection. Control characters and embedded double quotes are rejected.
Statements are sanitized before public event queuing. Procedure/function/trigger names,
page counters and per-table counters are typed; tables are summaries, not timed spans.
Classic PLAN lines (including JOIN, SORT, HASH and MERGE) are sanitized; other plan
forms are omitted conservatively. Attachment/transaction/statement IDs are parsed
only in the metadata header, before SQL begins; ID-like SQL literal content cannot
change the correlation scope. Blank SQL lines are preserved. PLAN recognition starts
only after the native post-SQL caret separator and outside SQL literals/comments.
Performance-shaped lines cannot end SQL collection. Statement counters are parsed
only after a genuine post-SQL boundary, and must match the complete native counter
line; expression aliases such as `1 ms` remain SQL even with native-looking indentation.
Parameter metadata must match the native numbered, typed parameter-line prefix.
SQL continuation identifiers beginning with `param` remain SQL. Framing tracks
literals/comments/delimiters independently of sanitizer token and syntax limits,
so omitting an unsupported SQL description does not swallow subsequent records.
Table headings are accepted only after a performance record; embedded headers and
table-shaped text inside SQL literals/comments remain sanitizer input.
Unterminated SQL is held conservatively until a size bound or flush marks it incomplete.
The trigger/relation ` FOR ` separator is recognized only outside quoted identifiers,
including doubled-quote escapes. Only terminal ellipses outside complete comments
are treated as truncation markers; ellipses inside literals/comments are sanitized
normally. Unterminated lexical input still fails closed.
Trace can include other applications, so its SQL client dialect is unknown. If the
lexer sees double-quoted tokens outside removed literals/comments, the whole SQL
text and object summary are omitted (generic SQL name), and the event is marked
incomplete. The same conservative rule applies to PLAN text. This does not infer
or add dialect 1 support, and does not alter explicit routine/table identity fields.

Names and table identities are schema metadata, not secret argument values. Client
SQL, bind parameters, connection strings, user/process details and raw error lines
are not forwarded. SQL record staging and lines are bounded at 64 KiB. The collector requests at most
48 KiB of SQL, leaving 16 KiB for record metadata, plans and counters; the complete
record cap still applies to exceptionally large metadata/plan output. SQL output
at 4096 bytes, table summaries at 64, nesting at 64, active attachment/transaction
scopes at 64, and the public queue at 1–256 events (64 by default). Connection and
session configuration fields have explicit size limits and reject invalid database
filter characters.

The driver's WaitStrings channel is an **unbuffered raw transport handoff**. It is
private to the collector; the public queue contains sanitized records. The upstream driver has its own wire buffers outside the
parser's limits. Trace data may already contain sensitive text on the server and in
the encrypted transport before the collector sees it. In Firebird, max_arg_count=0 means
unlimited, not disabled. The supplied config limits argument count/length but still
relies on discarding argument lines; it does not promise server-side redaction.

Matching uses observed ordering within attachment/transaction and statement identity
where available. Correlation remains `heuristic`, never `exact`. Unknown events,
truncation, overflow, mismatched finishes and stream loss invalidate the matching
state and mark subsequent records incomplete. Recursion is stack-based. Sequence IDs
are local to one collector; timestamps retain the server's local text without inventing
a timezone. Neither a fully observed pair nor the presence of parent IDs proves a
complete PSQL execution tree. Do not attach server events as exact HTTP children.
The optional `SpanRuntime` uses a SQL marker for the client parent and retains
heuristic nesting.

The collector is portable across the platforms supported by the driver. Startup,
stream reads, server-side stop and connection cleanup use context-aware v0.9.21 APIs.
`Shutdown` stops the Trace session, drains final records and releases the service
connection within the supplied context. Tests cover cancellation, a bounded event
queue and real Firebird session shutdown. There is no automatic reconnect that
silently reconstructs an allegedly complete tree.

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
- [Pinned driver Trace lifecycle](https://github.com/nakagami/firebirdsql/blob/v0.9.21/trace_manager.go)
