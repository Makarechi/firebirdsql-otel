# Safe client ownership

The existing functions and otelsql aliases retain their behavior. Config introduces an explicit safe client; opaque otelsql options are accepted only by the compatibility profile. In safe profiles, callers select providers, operation filters and separate span/metric attributes directly. Caller-supplied attributes and object hints are trusted configuration, not automatic data collection.

The safe client will own its operation spans instead of placing another wrapper beneath otelsql. A span is emitted with the measured start/end timestamps after the driver returns. This excludes ErrSkip fallback attempts and avoids changing a parent span when sampling or filtering suppresses a database span. The driver receives the application's context and exact SQL and arguments. No SQL cache, global mutable operation state or diagnostic network request is introduced.

Errors use a bounded classification and Firebird codes, never error strings or parameters. Query spans measure API time; an optional INTERNAL child cursor span measures consumption lifetime, including application pauses. Only a SpanContext is retained for parentage after the query span ends. Transactions have begin/commit/rollback operations, not an invented transaction parent scope.

Optional database/sql interfaces must retain the underlying capability set. Generated interface combinations expose exactly the implemented methods; fallback remains database/sql's responsibility. Raw access has an explicit unwrap helper used only inside sql.Conn.Raw's callback. Wrapping an already-safe connector is rejected to prevent double instrumentation.

Semantic keys are pinned to the database convention vocabulary shipped with OpenTelemetry Go 1.44.0 (semconv/v1.30.0 baseline): db.system.name, db.operation.name, db.query.summary, db.stored_procedure.name, db.collection.name, db.namespace and db.query.text. Safe mode emits this vocabulary regardless of otelsql's legacy environment switch; legacy constructors still follow otelsql.

All readers and collectors are opt-in and independent of business queries. Metadata describes possible dependencies, MON$ describes scoped snapshots, Trace describes observed server events, and Profiler is a manually pinned diagnostic example. None is an inferred complete execution tree.

Client SQL descriptions follow **client dialect 3**, which nakagami/firebirdsql v0.9.20
sets explicitly during statement preparation. A database's stored dialect does not
change that client setting. Custom drivers/connectors passed to the safe API must use
client dialect 3 as well; this API does not negotiate or add dialect 1 support.
Trace may contain SQL from unrelated clients. Without a known client dialect its
parser must use AnalyzeUnknownDialect, omitting text, object summary and fingerprint
when double-quoted tokens are ambiguous. Quotes inside removed literals/comments do
not create ambiguity. Explicit routine identity fields remain schema metadata.
