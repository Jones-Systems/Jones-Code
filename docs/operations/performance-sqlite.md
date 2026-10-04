# Synthetic SQLite diagnostics

## Health reports

Use this tool on a closed synthetic fixture produced and sealed through the
[canonical staging guard](../../scripts/performance-staging/guard.mjs).
Retain the producer's receipt, its independently trusted SHA-256 digest and its
exact repository/source/task/run binding. A copied database or creation marker
does not establish provenance. The tool supplies no live database or snapshot
discovery route.

Run the source directly with the repository's Node 24 runtime:

```sh
node apps/server/scripts/jones-sqlite-health.ts \
  --fixture-root "$FIXTURE_ROOT" \
  --fixture-receipt-sha256 "$TRUSTED_FIXTURE_RECEIPT_SHA256" \
  --fixture-binding-json "$TRUSTED_FIXTURE_BINDING_JSON" \
  < "$FIXTURE_RECEIPT_JSON"
```

Stdin must contain exactly one UTF-8 receipt object followed by EOF. Supply the
expected binding independently as a JSON object containing `repository`,
`sourceRevision`, `taskRef` and `runId`; do not derive either trust input from
an arbitrary receipt. Interactive stdin, missing bindings, SQL, arbitrary
database paths, unknown flags and repeated flags are refused. Receipt bytes
are bounded before retention. The receipt and the complete worker request must
each fit within 48 KiB; a larger valid staging receipt is unavailable to this
transport. No receipt is added to the sealed fixture's manifest.

Defaults report metadata only: runtime SQLite version/source ID, the health
connection's effective settings, main/sidecar sizes and hashes, page/freelist
counts and bounded schema names/types. Connection settings do not establish
the producer's or an installed server's effective durability. Source-file
hashes identify the tool's observed source; source revision and build identity
remain null when unproved.

Counts, btree allocation, full integrity and foreign-key checks are separate,
explicit operations:

```sh
node apps/server/scripts/jones-sqlite-health.ts \
  --fixture-root "$FIXTURE_ROOT" \
  --fixture-receipt-sha256 "$TRUSTED_FIXTURE_RECEIPT_SHA256" \
  --fixture-binding-json "$TRUSTED_FIXTURE_BINDING_JSON" \
  --include counts,allocation,integrity,foreign-keys \
  --deadline-ms 60000 \
  < "$FIXTURE_RECEIPT_JSON"
```

The default `--schema-profile legacy-v1` preserves historical count semantics.
For an independently sealed V2 fixture, explicitly select
`--schema-profile orchestration-v2`; the report records that selection and uses
V2 events, receipts, threads, messages, runs, effect outbox and projection metadata.
Projects and Jones leases retain their shared table names. A profile selects fixed
queries; it does not prove migration, producer compatibility or runtime qualification.
The pinned historical producer remains V1 and cannot establish V2 coverage.

Missing count tables and DBSTAT support are unavailable results. DBSTAT totals
cover btree pages, excluding freelist and other non-btree pages. Integrity
diagnostics omit raw SQLite messages and payloads. An integrity pass does not
imply a foreign-key pass; omitted, interrupted or truncated checks cannot
certify either. Counts and bytes are decimal strings.

The current read-only qualification accepts a closed rollback-journal database
with no WAL/SHM/journal files. A WAL-format header or any present sidecar is
refused as `readonly_layout_unqualified` before SQLite opens it. A native
read-only WAL open can create or update sidecars; immutable mode is not assumed.
The canonical guard verifies identities, hashes and main/WAL/SHM/journal layout
before and after an accepted SQLite open. A changed or unproved layout is
unavailable. The supplied fixture and receipt remain owned by their producer.

For a fixture created with `withClosedSyntheticFixture`, select the producer's
`health-offline-delete` profile and finish the health call inside its consumer
callback. Return `sqliteHealthConsumerOutcome` with that callback's pinned
receipt digest and the health report. The adapter releases the fixture only
when the child has known close/reap and the health supervisor completed cleanup
without retaining a root. An unknown outcome retains it even if the callback
fulfilled. Failed or interrupted reports may release only with proven closure.
The producer's retention error preserves the report at `error.evidence.value`;
do not treat it as permission to remove a retained path.

Working budgets are 5 seconds for metadata and 60 seconds when any expensive
operation is selected; `--deadline-ms` accepts 1–300 seconds in milliseconds.
Final output defaults to 256 KiB (`--max-output-bytes`, 16 KiB–1 MiB). Schema and
allocation rows default to 128 (`--max-records`, maximum 256); integrity/FK
diagnostics default to 64 (`--max-diagnostics`, maximum 128). These are tool
resource bounds, not application performance objectives.

An owned native Node leaf runs synchronous SQLite and fixture verification
under the staging supervisor's hard deadline and combined stdout/stderr bound.
The total budget includes input/setup/verification and reserves 500 ms for
shutdown. The supervisor signals only its captured child, escalates if needed,
and observes close/reap. Partial worker results survive interruption. Raw child
stdout/stderr are absent from the final report. The stderr allowance defaults
to 16 KiB (`--max-stderr-bytes`, maximum 64 KiB); excess is classified after
combined capture, not as an early per-stream cutoff. The explicit child
environment does not establish provider or network containment.

The final JSON envelope includes the child's native outcome separately from
tool/component status and cleanup. Exit codes are 0 completed, 2 refused,
3 partial/interrupted, 1 failed and 4 unknown cleanup. Cleanup removes only the
original supervisor root after every captured child has known close/reap.
Unknown effects retain its exact path for the original owner; there is no
generic cleanup command.

Focused verification uses the TypeScript test with Vitest and the E foundation
tests with Node's runner:

```sh
node_modules/.bin/vp test run apps/server/scripts/jones-sqlite-health.test.ts --no-file-parallelism --maxWorkers=1
node --test --test-concurrency=1 scripts/performance-staging/guard.test.mjs scripts/performance-staging/lifecycle.test.mjs
```

The health tests use small genuinely created/sealed diagnostic fixtures. They
do not qualify production reads, engine transaction benchmarks, migration,
backup/restore, providers, network isolation or packaged runtimes.

## Historical engine benchmark

`apps/server/scripts/jones-sqlite-benchmark.ts` runs bounded, synthetic workloads
through the exact source engine supplied to the canonical performance fixture.
It is a partial diagnostic tool: statement execution, transaction and pure
writer-lock timing remain unfulfilled and deferred until an accepted
instrumentation seam exists. It creates fresh owned roots, uses the qualified
`benchmark-wal` profile, and
does not accept an existing database, SQL, a copied fixture, or a receipt as
permission to write. The fixture receipt returned after an open callback is
historical: its original root has already been closed, sealed and cleaned.

Use a Node runtime supporting native TypeScript execution and `node:sqlite` from
the explicitly bound workspace:

```sh
node apps/server/scripts/jones-sqlite-benchmark.ts
```

With no arguments, or `--help`, this prints metadata and caps without creating a
fixture. Running a workload requires `--run-json` and one complete JSON request.
Set `JONES_PERFORMANCE_SOURCE_PARENT` to your canonical source parent. For
example, select an existing authorized scratch parent and replace the
binding revision with the exact benchmark source revision being qualified:

```sh
node apps/server/scripts/jones-sqlite-benchmark.ts --run-json '{
  "parentPath": "/absolute/authorized/scratch-parent",
  "binding": {
    "repository": "Jones-Systems/Jones-Code",
    "sourceRevision": "EXACT_BENCHMARK_SOURCE_REVISION",
    "taskRef": "spec.jones-performance-portfolio#task.d-bench.001",
    "runId": "synthetic-benchmark-example"
  },
  "databaseSource": {
    "repository": "Jones-Systems/Jones-Code",
    "sourceRevision": "e5a31aceec91484b64315c63dcce80f6e7581604",
    "worktreePath": "/absolute/source-parent/baseline"
  },
  "trials": 1,
  "turns": 8,
  "historyTurns": 3,
  "payloadBytes": 256,
  "arrival": "burst",
  "intervalMs": 20,
  "burstSize": 4,
  "timeoutMs": 30000
}'
```

The source engine is the declared fixture `databaseSource`, not a candidate
engine relabeled as that source. The admitted historical source choices are the exact
`e5a31aceec91484b64315c63dcce80f6e7581604` baseline and
`414bb8da204c3275cd0b76b2ec4d74dfb09a97e4` live-baseline **source checkout** shown
by metadata. This does not open either installation's live data. The E producer
enforces the exact source paths and constructs a new coherent synthetic database
with the real source migrations and engine.

Every field is required. Unknown fields and arguments are refused. Caps are
1–5 independent trials, 1–64 offered turns per trial, 3–256 seed history turns,
1–65536 ASCII payload bytes per user message and assistant delta, arrival
intervals of 0–1000 ms, and burst sizes of 1–64 turns. The whole worker deadline
is 100–120000 ms, including fixture creation, workload, captures and cleanup.
These are harness bounds, not production service objectives. Choose scales and
trial counts only within the current local admission and execution grant.
The combined `(historyTurns + turns) * payloadBytes` must also be at most
4 MiB, bounding the declared seed and workload payload scale together. This
counts the configured payload once per turn; each turn has both a user and
assistant payload, with additional journal and projection copies.

The supervisor uses the E guard and E captured-child lifecycle. It bounds the
encoded request to 24 KiB and aggregate argv to 64 KiB. The E lifecycle bounds
combined stdout/stderr to 80 KiB before accumulation. The supervisor then checks
64 KiB stdout and 16 KiB stderr separately after capture; the stderr check is
not an independent early termination bound. Reports omit raw stderr, SQL and
message/activity payloads. A native child hard deadline can stop synchronous
SQLite; an Effect timeout alone cannot.

Each turn offers four commands: a user turn start, assistant delta, assistant
completion and activity append. Finite steady arrivals offer one turn per
interval; burst arrivals offer `burstSize` turns per interval. A serial harness
drain preserves these commands' order and records its offered backlog. That
backlog describes the harness, not the engine's private queue. One serial
executor does not exercise competing writers or establish saturation. No provider or
network adapter is started by this workload.

After the offered traffic drains, the tool checks an accepted command replay,
a missing-thread rejection and its replay, then one declared synthetic fault.
The fixed temporary trigger aborts the actual activity projection after the
engine's event append. It is removed in `finally`; the same command is retried
once after removal. This measures and proves a harness-injected transient
failure, not a production retry mechanism. Canonical captures must show that
accepted replay, rejected replay and rollback leave journal, receipt,
projection and cursor state unchanged as appropriate. The final capture checks
receipt outcomes, expected event/receipt growth, projection/replay equivalence,
pagination, integrity and foreign keys through the real fixture seams.
Before a trial can complete, its final guard requires full integrity to return
exactly `ok`, zero foreign-key violations, replay/read-model equivalence,
expected event and receipt deltas, unchanged replay and rollback state,
the expected accepted/rejected receipt outcomes, contiguous event streams,
complete receipt/project/thread coupling, and live projector cursors at the journal
head. The supervisor also refuses a worker trial with false consistency flags,
nonzero foreign-key violations, or unequal emitted delta and sequence pairs.

`traffic` counts the four commands per offered turn. `outcomes` also includes
the four protocol operations: accepted replay, rejection, rejected replay and
the retry operation. The failed first retry attempt increments `attempts` and
`failures`, but does not create another offered operation. `accepted` counts
new accepted commands; `fulfilledTerminals` includes the accepted replay.
`rejections` counts both rejection attempts. Failure counts retain the two
rejections and injected projection failure even when the trial completes.

Timing summaries use monotonic milliseconds. `schedulingLag` is scheduled
target to actual arrival-timer callback, once per offered turn; an early
callback is reported as zero lateness. `timing.traffic.harnessQueue` is actual
timer callback to dispatch invocation. It includes earlier commands in the
serial drain, including earlier commands from the same offered turn.
`engineCompletion` is dispatch invocation to the terminal result of
`context.run`, fulfilled or rejected. It includes the engine queue, validation,
persistence, projection and publication. `timing.traffic.arrivalCompletion` is
actual timer callback to that local terminal result. These are local engine
completion boundaries, not browser or WebSocket acknowledgment measurements.

Traffic timing populations contain the four attempted commands per offered
turn. `timing.protocol.engineCompletion` is a separate population containing
the accepted replay, initial rejection, rejected replay, injected fault and
post-removal retry: five attempts, including all three terminal failures.
Protocol attempts have no arrival-timer callback, so no scheduling-lag,
harness-queue or arrival-completion metric is emitted for them.
`traffic.elapsedMs` covers only the offered traffic and its drain.
Pure writer-lock wait, statement execution and transaction intervals are
explicitly unavailable because this fixture API has no observers for those
boundaries. Broad dispatch durations must not be reported as lock hold or SQL
time. Event-loop samples describe the traffic phase; a short run can have zero
delay samples, represented by null values. Canonical-capture durations report
verification overhead separately and are not writer throughput.
The fixed database and WAL file measurements use the genuine context's owned
paths before and after traffic. They report logical file sizes in bytes, not
allocated blocks or payload estimates. A missing sidecar is zero bytes; an
unavailable or nonregular no-follow observation is null. WAL file size can
remain at its high-water size, so its difference is not a WAL frame count or
the amount written. The tool does not checkpoint inside the measured traffic.

The observed runtime includes Node, SQLite, journal mode, synchronous level,
foreign keys, busy timeout, journal limit and other fixture pragmas. The tool
requires observed WAL, synchronous `2`, foreign keys `1`, busy timeout `5000`
and journal size limit `33554432`. A mismatch fails rather than switching modes
or relabeling durability.

Success requires known worker close/reap and known fixture cleanup before the
supervisor disposes its exact root. An explicitly retained fixture, missing or
invalid worker output, unknown child custody, interrupted production, or
unproved cleanup retains the enclosing root and reports its exact path. Known
process exit alone does not override an unknown fixture outcome. Preserve that
evidence for the owning coordinator; this tool supplies no shared cleanup or
live-operation command. Ordinary SIGINT/SIGTERM cancel the captured child.

These synthetic trials do not establish production capacity, headroom, SLOs,
backup/restore qualification or migration downtime. Compare repeated trials
at the same source, workload and observed durability, retaining failure and
variability evidence. Do not divide synthetic writer throughput by historical
mixed read/write traffic or extrapolate live downtime from a synthetic scale.

## Candidate qualification boundaries

The historical benchmark still admits only `e5a31aceec91484b64315c63dcce80f6e7581604`
and `414bb8da204c3275cd0b76b2ec4d74dfb09a97e4`. Its arrival, harness queue,
Engine completion, and protocol populations must remain separate. Those results
are not V2 writer contention or capacity measurements. The seven migration/restore
cases and historical runtime checks retain candidate
`da5f4aee0035beec471b38598eaa2857d1e5155c`; they do not qualify current V2.

Runtime qualification requires `JONES_RUNTIME_BINDING` pointing to a bounded JSON
file with schema `jones-performance-runtime-binding/v1`, canonical `runtimeRoot`
and `executablePath`, exact `nodeVersion`, and `executableSha256`. The executable
is validated before spawn and its actual version is checked inside the child.
Create the runtime root's `evidence` directory explicitly for historical runs.
The historical source checkout uses `JONES_PERFORMANCE_SOURCE_PARENT`; its commit,
tree, lock and selected case hashes remain pinned. Earlier crash evidence is not
superseded by making source paths portable.

For current V2, run `node --test scripts/performance-staging/current-qualification.test.mjs`
with `JONES_CURRENT_QUALIFICATION_REQUEST` pointing to an explicit JSON request:
`candidate` contains `repository`, `worktreePath`, `sourceRevision`, `tree`, and
`lockSha256`; `parentPath`, `binding`, and `policy` use the staging guard contract.
The candidate must be clean and contain the four named V2 synthetic test modules.
This route runs those modules against the bound source under one captured child,
records phase evidence before cleanup, and retains scratch if runner closure is
unknown. The runtime binding is required here too. These checks exercise V2
initialization and storage; they do not grant authority to snapshot installed
state or qualify a compiled/installed runtime.

V2 benchmark qualification remains unavailable: the historical Engine dispatch,
rollback injection, replay assertions and projection tables require a separate
EventSink/ThreadManagementService workload and fresh measurements. Do not widen
the historical source allowlist to present those checks as V2 coverage.
