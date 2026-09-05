# Handoff 01 implementation map

Compared with baseline `3cfedff020d45827310cf94c2da18c2191265e37`, the initial dependency
change (`ab4c35e`) only upgraded firebirdsql from 0.9.19 to **0.9.20** and tidied its
transitive dependencies. None of W01–W10 existed at that point. The implementation
below adds them without a driver fork, changes to business procedures or a server plugin.

| Work | Implementation and acceptance evidence |
| --- | --- |
| W01 | `internal/sqltext`, explicit SQL policy; bounded literal/comment removal before telemetry, fail-closed lexing, golden privacy tests in all three semantic modes and fuzz tests |
| W02 | Firebird procedure/package/quoted names, CTE and DML summaries; trusted context hint; SQL passed unchanged; no procedure claim for ambiguous FROM NAME |
| W03 | Conservative opt-in DSN authority parser, explicit namespace, separate span/metric attributes; IPv6, Windows path, credentials and ambiguity tests |
| W04 | Own operation spans, safe FbError codes/status, original errors; no exception text; filter/sampling/parent isolation, concurrent statements and ErrSkip tests |
| W05 | Optional INTERNAL consumption span, first delivery, rows/attempts/EOF/early close/error; exact generated optional interface sets, multi-result forwarding and Raw tests |
| W06 | Individual begin/commit/rollback spans, profile noise flags and pre-execution filter; explicit pool metric registration/unregistration, no background work from Open |
| W07 | Bounded metadata graph with database/object/type/package/schema identity, TTL, eviction/invalidation/cycles; unknown execution and package-body precision documented; mock and live catalog tests |
| W08 | Explicit separate diagnostic pool, owned fresh MON$ transactions with attachment/statement scope; no business transaction commits; table counters and visibility limits documented and tested |
| W09 | Existing native TraceManager in a supervised helper, bounded parser/events/matching, actual procedure/function/trigger/plan/table observations; heuristic correlation and gaps; live Trace and lifecycle tests |
| W10 | Manual pinned-connection Profiler example with DETAILED_REQUESTS, selectable EOF before finish, autonomous flush and fresh read; real Firebird test |

The work is arranged as a dependency upgrade followed by four reviewable layers:
configuration/sanitizer/ADR; client/errors/rows; metadata/MON$ readers; Trace/Profiler,
CI fixtures and final validation documentation. The final layer contains the complete
integration setup; every layer preserves the old public API.

## Intentional limits

- The safe Config API owns spans rather than relying on raw otelsql callbacks. The old
  API retains its behavior; opaque options are accepted only in compatibility mode.
- SQL descriptions are lexical, not a full Firebird compiler. Unsupported/malformed
  lexemes omit text; object names remain schema metadata and explicit attributes are trusted.
- Client durations measure API calls/consumption, not a complete server execution tree.
  Metadata is possible dependency data, MON$ is a sampled snapshot, and Trace matching
  is heuristic. No diagnostic source is automatically attached as an exact HTTP child.
- Trace remains **experimental and process-isolated**. Upstream lacks cancellable
  start/read/stop guarantees. Forced termination bounds local cleanup but can require
  operator removal of a server session. Server/transport Trace and Profiler data can
  already contain sensitive SQL; server-side redaction is not promised.
- Classic Trace PLAN output is supported; unsupported plan text is omitted. Packaged
  metadata can be resolved only to package-body scope. Dynamic dependencies may be absent.
- There is no production collector auto-start, reconnect, migration, or profiler SDK.
  Diagnostic credentials, sampling and lifecycle have explicit owners.

See [validation](validation.md) for tested behaviors and actual cost measurements,
[diagnostics](diagnostics.md) for source-specific contracts and upstream references,
and [the ADR](adr-001-safe-client.md) for span ownership and semantic conventions.
