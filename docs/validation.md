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
Trace parsing, recursion, gaps, queue saturation and bounded process shutdown.

The real fixture includes nested and untaken procedure branches, functions, triggers,
views, a selectable procedure, recursion, dynamic SQL and a package. Real client
snapshots are compared to `testdata/firebird5/expected-client.json`. Firebird reports
some errors only during fetch: the failed CAST is on the consumption span, while the
successful query API call stays successful. Trace confirms actual OUTER/NESTED_A,
function and trigger execution without inventing the untaken NESTED_B call. Metadata
can include NESTED_B and explicitly says execution is unknown. Profiler returned two
rows and four detailed request records on the pinned connection.

## Measurement method

Measurements below are a local reference, not a production capacity claim. Machine:
Apple M4, macOS, Go 1.27.0; Firebird 5.0.3 amd64 Docker image under emulation. Correctness
was additionally verified with the module's Go 1.25.0 toolchain. The real benchmark
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
export, and do not independently isolate the Trace helper's resident memory. Trace
mode waits for the measured procedure finish records before stopping. Metrics size is
one final JSON snapshot, not bytes per query or an OTLP wire size.

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
| Plain | 790 | 983 | 1061 | 113.2 | 2549 | 4 | 0 | 0 |
| Compatibility | 804 | 868 | 917 | 182.4 | 9532 | 4 | 2 | 671 |
| Safe | 807 | 871 | 949 | 185.4 | 10922 | 4 | 1 | 730 |
| Diagnostic | 811 | 881 | 935 | 254.6 | 17714 | 4 | 2 | 1532 |

All four modes used 168 client bytes and 304 server bytes per query. The measurements
confirm no diagnostic SQL or hidden fetch in the client profiles for this workload.
Latency differences at this scale include noise and ordering effects; they are not
evidence that instrumentation makes the database faster.

| Mode | CPU user/system s | peak RSS MiB | final metric JSON B |
| --- | ---: | ---: | ---: |
| Plain | 0.05 / 0.15 | 17.4 | 358 |
| Compatibility | 0.06 / 0.14 | 20.1 | 1855 |
| Safe | 0.06 / 0.14 | 20.6 | 1612 |
| Diagnostic | 0.06 / 0.13 | 20.2 | 1622 |

### Explicit diagnostics (200 iterations each, on top of safe client)

| Addition | p50 µs | p95 µs | p99 µs | allocs/op | B/op | turns/op | diagnostic JSON B/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Cached metadata | 1016 | 1241 | 1743 | 190.9 | 14022 | 4.01 | 1470 |
| Cold metadata | 4734 | 5562 | 10654 | 1205.1 | 63927 | 31.03 | 1470 |
| MON$ snapshot | 4469 | 4702 | 5206 | 1244.0 | 62163 | 26.03 | 1795 |
| Trace collector | 1006 | 1125 | 1202 | 211.2 | 16096 | 44.09 | 1435 |

| Addition | client/server B/op | CPU user/system s | peak RSS MiB | metric JSON B |
| --- | ---: | ---: | ---: | ---: |
| Cached metadata | 168 / 304 | 0.03 / 0.06 | 19.1 | 1623 |
| Cold metadata | 2916 / 8393 | 0.08 / 0.19 | 20.8 | 1631 |
| MON$ snapshot | 2444 / 9009 | 0.07 / 0.17 | 20.3 | 1621 |
| Trace collector | 1454 / 3415 | 0.15 / 0.45 | 19.8 | 1631 |

Diagnostic traffic includes separate pool/control-session cleanup, hence fractional
turns. Cached metadata is serialized each iteration; cold mode invalidates every time.
MON$ deliberately takes a new snapshot each query for measurement, not as a suggested
production sampling rate. Trace page counters are server observations; its service
polling adds traffic independent of business query count. Profiler is validated as a
manual example, not assigned an unsupported overhead percentage. Production budgets
must be measured with the actual schema, exporter, sampling and server architecture.
