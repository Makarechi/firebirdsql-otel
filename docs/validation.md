# Validation and measured cost

## Reproduce correctness checks

The complete suite was run on Go 1.25.0 with `-race` against Firebird 5.0.3.
Unit tests also run without a database; integration tests explicitly skip if the
following environment variables are absent. Use a disposable synthetic database:

```sh
docker run --rm -d --name firebirdotel-test \
  -e FIREBIRD_ROOT_PASSWORD=synthetic-test-only -e FIREBIRD_DATABASE=otel.fdb \
  -p 127.0.0.1:3050:3050 firebirdsql/firebird:5.0.3
# Wait for the database to accept connections, then install the fixture.
docker exec -i firebirdotel-test /opt/firebird/bin/isql -b \
  -user SYSDBA -password synthetic-test-only /var/lib/firebird/data/otel.fdb \
  < testdata/firebird5/schema.sql
export FIREBIRD_TEST_DSN='SYSDBA:synthetic-test-only@localhost:3050/var/lib/firebird/data/otel.fdb'
go build -o /tmp/firebirdotel-trace ./cmd/firebirdotel-trace
export FIREBIRD_TRACE_BINARY=/tmp/firebirdotel-trace
go test -race -count=1 ./...
go vet ./...
go mod verify
go test ./internal/sqltext -run '^$' -fuzz FuzzAnalyze -fuzztime 10s -parallel 2
go test ./internal/traceparse -run '^$' -fuzz FuzzTraceChunks -fuzztime 10s -parallel 2
```

CI automates the same fixture/worker setup and uses five-second fuzz runs. Fuzzing
checks panic safety and size invariants; golden canary tests separately check privacy.
The generated capability adapters can be reproduced with `go generate ./...`.

Coverage includes compatibility API/options; every supported semantic-convention
environment setting; SQL/bind/DSN/error canaries; exact error identity and safe status;
filtered/unsampled parent isolation; prepared concurrency; ErrSkip fallback; Rows
EOF/early close/error and multiple result sets; optional interfaces and Raw; pool
metric unregister; bounded caches/cycles/invalidation; fresh, scoped MON$ transactions;
Trace parsing, recursion, gaps, queue saturation and bounded process shutdown. Repeated pool opening/closing checks global registration
and metric callback cleanup; a stalled synchronous exporter checks execution count,
error identity and safe output after release.

The real fixture includes nested and untaken procedure branches, functions, triggers,
views, a selectable procedure, recursion, dynamic SQL and a package. Real client
snapshots are compared to `testdata/firebird5/expected-client.json`. Firebird reports
some errors only during fetch: the failed CAST is on the consumption span, while the
successful query API call stays successful. Trace confirms actual OUTER/NESTED_A,
function and trigger execution without inventing the untaken NESTED_B call. Metadata
can include NESTED_B and explicitly says execution is unknown. Profiler returned two
rows and four detailed request records on the pinned connection.

Review regression checks additionally cover cancellation-driven automatic Close,
deferred errors after EOF, non-row EOF, non-recording cursor bypass, snapshot timing
under pool contention, leading whitespace in catalog identities, package-wide precision,
SQL attempting to spoof Trace metadata, literal database filter matching on Firebird,
classic plan families and late warm-up records. CI uses `otel_review+1.fdb` to exercise
real Trace configuration with pattern metacharacters in the database name. Further
regressions cover single connector-factory invocation, untouched lazy results,
connected typed package scopes, fully ordered bounded dependency reads, invisible
MON$ targets, multiline SQL comments/literals/blank lines, forged headers in literals,
and quoted trigger names containing ` FOR `.

## Measurement method

Measurements below are a local reference, not a production capacity claim. Machine:
Apple M4, macOS; Firebird 5.0.3 amd64 Docker image under emulation. All real-query
rows were remeasured on September 6 with Go 1.25.0 after review corrections; the mock
Exec reference uses Go 1.27.0. The real benchmark
executes `SELECT N FROM OTEL_REPORT`, consumes its two rows to EOF, and uses a synchronous
in-memory JSON span exporter. A local TCP proxy counts bytes and changes from response
to request direction. These are **observed TCP turns**, not a wire-protocol packet
parser or an assertion about all query shapes. Proxy/exporter allocations are included.

```sh
go test -run '^$' -bench BenchmarkClient -benchmem -count=3 .
go build -o /tmp/firebirdotel-benchmark ./examples/benchmark
/usr/bin/time -l /tmp/firebirdotel-benchmark -mode safe -n 1000
# Modes: plain, compatibility, safe, diagnostic,
#        metadata-cached, metadata-cold, monitoring, trace.
```

Set the two environment variables above. Each process warms up five queries. Allocation
and byte deltas cover the measured loop and diagnostic cleanup; startup/warmup are
excluded. CPU/RSS from `time` cover the whole process, including startup, proxy and JSON
export, and do not independently isolate the Trace helper's resident memory. Trace mode waits for warm-up procedure finishes, completely stops/drains that worker,
then starts a fresh worker before resetting counters. This process boundary prevents
late warm-up statement finishes from entering the measured phase. It then waits for
the measured procedure finishes before stopping; measured shutdown/control traffic
is included, while both worker startups are outside the measured loop. Metrics size is
one final JSON snapshot, not bytes per query or an OTLP wire size.

**Allocation columns and JSON fields cover the benchmark process only.** In Trace
mode they exclude the helper's driver, parsing and JSON encoding allocations. The
output explicitly reports `allocation_scope=benchmark_process_only`. These columns
cannot compare complete Trace memory cost against in-process metadata/MON$ costs.

### Mock Exec, no-op providers (three runs)

| Mode | ns/op range | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Plain driver | 111–114 | 80 | 2 |
| Compatibility | 465–493 | 760 | 10 |
| Safe | 1401–1496 | 3744 | 34 |
| Diagnostic | 1447–1462 | 3744 | 34 |

Exec has no cursor, so the two Config profiles have the same allocation count here.
For this small default query, use **48 allocations / 8192 bytes per operation** as a
local regression investigation threshold, not a universal bound or a timing-sensitive
CI assertion. Hard parser/cache/queue limits are enforced independently by tests.

### Real selectable query (1000 iterations per mode)

| Mode | p50 µs | p95 µs | p99 µs | allocs/op | B/op | turns/op | spans/op | span JSON B/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Plain | 945 | 1498 | 2355 | 113.3 | 2559 | 4 | 0 | 0 |
| Compatibility | 1291 | 1878 | 2316 | 164.4 | 8989 | 4 | 2 | 671 |
| Safe | 1262 | 1641 | 2059 | 165.4 | 9461 | 4 | 1 | 730 |
| Diagnostic | 1246 | 1450 | 1815 | 218.4 | 16023 | 4 | 2 | 1532 |

All four modes used 168 client bytes and 304 server bytes per query. The measurements
confirm no diagnostic SQL or hidden fetch in the client profiles for this workload.
Latency differences at this scale include noise and ordering effects; they are not
evidence that instrumentation makes the database faster.

| Mode | CPU user/system s | peak RSS MiB | final metric JSON B |
| --- | ---: | ---: | ---: |
| Plain | 0.06 / 0.21 | 16.6 | 365 |
| Compatibility | 0.09 / 0.30 | 18.9 | 1864 |
| Safe | 0.08 / 0.30 | 19.2 | 1632 |
| Diagnostic | 0.08 / 0.29 | 19.4 | 1637 |

### Explicit diagnostics (200 iterations each, on top of safe client)

| Addition | p50 µs | p95 µs | p99 µs | allocs/op | B/op | turns/op | diagnostic JSON B/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Cached metadata | 1125 | 1296 | 1346 | 170.0 | 12814 | 4.01 | 1650 |
| Cold metadata | 7842 | 8840 | 11620 | 1182.6 | 63254 | 31.03 | 1650 |
| MON$ snapshot | 5154 | 5553 | 7475 | 1343.0 | 63268 | 30.03 | 1795 |
| Trace collector | 957 | 1505 | 2202 | 222.0 | 14617 | 44.06 | 1406 |

| Addition | client/server B/op | CPU user/system s | peak RSS MiB | metric JSON B |
| --- | ---: | ---: | ---: | ---: |
| Cached metadata | 168 / 304 | 0.01 / 0.05 | 17.4 | 1637 |
| Cold metadata | 3132 / 8393 | 0.10 / 0.30 | 20.0 | 1637 |
| MON$ snapshot | 2712 / 9397 | 0.08 / 0.19 | 19.6 | 1638 |
| Trace collector | 1453 / 3477 | 0.15 / 0.39 | 18.7 | 1638 |

Diagnostic traffic includes separate pool/control-session cleanup, hence fractional
turns. Cached metadata is serialized each iteration; cold mode invalidates every time.
MON$ deliberately takes a new snapshot each query for measurement, not as a suggested
production sampling rate. Trace page counters are server observations; its service
polling adds traffic independent of business query count. Profiler is validated as a
manual example, not assigned an unsupported overhead percentage. Production budgets
must be measured with the actual schema, exporter, sampling and server architecture.
