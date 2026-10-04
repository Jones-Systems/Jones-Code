# Synthetic SQLite benchmark

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
