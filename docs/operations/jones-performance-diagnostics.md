# Jones performance diagnostics

These maintainer tools exercise isolated synthetic databases from a bound source
checkout. They do not qualify an installed application, measure live provider
capacity, or supply a route to discover or copy private databases. Follow
[development test-data guidance](./development.md#test-data) for the source,
destination and lifetime of any separately authorized real-data snapshot.

Use the receiving checkout's supported Node runtime (`26.12.1`) and prepared dependencies.
Historical dependency installation has its own Node `^24.13.1` / pnpm `11.10.0`
binding; it does not select the receiving execution runtime.
Keep request files, runtime bindings and retained reports in task-owned storage
outside the checkout. Source binding requires a clean checkout, so writing an
untracked request into it makes the candidate invalid. Choose disk-backed scratch
with an explicit cleanup owner; do not use shared `/tmp` or live `.t3` state.

## Default checks and metadata

From the repository root, these focused native suites leave their heavy cases
disabled:

```sh
env -u JONES_CURRENT_QUALIFICATION_REQUEST \
  -u JONES_MIGRATION_RESTORE_REQUEST \
  -u JONES_PERFORMANCE_SOURCE_PARENT \
  node --test --test-concurrency=1 \
  scripts/jones/performance/current-qualification.node-test.mjs \
  scripts/jones/performance/migration-restore.node-test.mjs
```

They check request refusal, metadata, closure decisions and the migration case
table and pins. A skipped heavy case is unexecuted qualification. It must not be
reported as a passing migration, restore or benchmark run. Follow the applicable
host's test admission rules before starting checks.

The command-line entry points also expose metadata without starting a workload:

```sh
node scripts/jones/performance/current-qualification.mjs --help
node apps/server/scripts/jones/v2Benchmark.mjs --help
```

Both report unavailable qualification until an explicit request and runtime
binding are supplied. The benchmark's metadata is a description of its bounded
workload, not measured results.

## Bind the source and runtime

[Source helpers](../../scripts/jones/performance/sources.mjs) distinguish two
inputs. `currentDatabaseSource(worktreePath)` binds the receiving Jones Code
checkout by repository, commit, tree, lock SHA-256 and canonical path;
`assertCurrentDatabaseSource` also requires a clean worktree, including untracked
files. Run the tool from that same bound checkout. Another checkout with the same
commit is not an interchangeable candidate.

Historical inputs resolve beneath the canonical absolute
`JONES_PERFORMANCE_SOURCE_PARENT`. The checked-in `qualificationSourcePins` are
the authority for each directory's exact commit, tree and frozen lock. They cover
`baseline`, `live-baseline`, `history`, `lease`, `lease-current`,
`v2-aggregate-77` and `v2-aggregate-91`; do not replace them with a moving branch.
The historical producer uses pinned historical modules by path. It does not
revive V1 orchestration APIs in the receiving source.

Historical checkout preparation is opt-in through
[withPreparedHistoricalSources](../../scripts/jones/performance/prepare-sources.mjs).
Its clone operation does not prepare dependencies or prove import compatibility.
Default checks do not clone historical sources. Installing a dependency alone
does not qualify the producer or its database.

For the two issue #387 cases, use the separate
[historical preparation owner](../../scripts/jones/performance/historical-preparation.mjs)
and its [request types](../../scripts/jones/performance/historical-preparation.d.mts).
It prepares only `history`, `lease-current` and `lease`; the seven-source opt-in
check remains separate. Each invocation owns an absent private disk root outside
the checkout, including its checkouts, store, home, configuration, cache and tmp.
Evidence belongs to a separate private directory. Supply finite byte, entry,
file, cache, combined output, process and invocation deadline bounds, an aggregate
additional-peak estimate, and retention disposition. Every serial producer
rechecks the actual output filesystems for the 30 GiB reserve plus at least
10 GiB additional peak. Monitoring is not a filesystem quota or reservation.

Real historical preparation requires a separately assigned frozen execution run.
Bind canonical historical Node/pnpm paths and digests, and retain individual
configuration-control inspection evidence in the declared evidence custody.
The owner verifies versions and the exact pnpm install help before using its
arguments; missing controls or artifacts fail instead of selecting another tool,
downloading a toolchain or running a blanket rebuild. The frozen install disables
scripts. Historical `prepare` stays unexecuted for this source-import route.
Observed imports must establish that disposition; they are not packaged-build
attestation.

Within `withHistoricalPreparation`'s callback, pass its receipt path and trusted
SHA-256 to
[qualifyHistorical](../../scripts/jones/performance/qualify-historical.mjs).
Keep the original owner alive until the callback returns explicit consumer
closure. The strict entrypoint uses the unchanged source-parent interface,
checks ESM and `createRequire` ownership before allocating databases, then runs
both original cases serially and preserves their full results. Both must return
exactly `status: complete` with every original phase and oracle. Skipped,
unavailable, partial and loader-only results fail acceptance. Metadata is safe
to inspect without executing either case:

```sh
node scripts/jones/performance/qualify-historical.mjs --help
```

The CLI's `--request <JSON-file>` consumes the same preparation receipt and
requires explicit synthetic opt-in, the receiving runtime binding, case options,
the declared evidence directory and a finite evidence-byte bound. A serialized
receipt grants no cleanup capability. Only the original in-memory owner releases
its root, after all producers and consumers close and evidence is durable.
Subreaping handles adopted installer descendants without host-wide process
selection; it is not containment. Unknown family closure, inspection overflow,
replaced paths and external hardlinks retain the exact root. Preserve the
original operation failure and cleanup result separately, and reconcile retained
custody before retrying.

Focused source-only control tests use synthetic processes and files, without
historical fetches, installs or databases:

```sh
node --test --test-concurrency=1 \
  scripts/jones/performance/historical-preparation.node-test.mjs \
  scripts/jones/performance/historical-resolution.node-test.mjs \
  scripts/jones/performance/qualify-historical.node-test.mjs
```

Set `TMPDIR` to short task-owned disk storage before testing. A supervisor-loss
test intentionally retains its exact small root and durable failure evidence;
its diagnostic identifies the retained custody for reconciliation.

Heavy qualification and benchmark runs additionally require
`JONES_RUNTIME_BINDING` to name a canonical JSON file using
`jones-performance-runtime-binding/v1`. The
[runtime reader](../../scripts/jones/performance/runtime-binding.mjs) verifies the
runtime root, executable path and executable SHA-256. Workers compare the observed
executable and Node version with that binding. Populate it from directly observed
runtime identity; do not substitute a requested version or an installation claim.

## Produce and consume a fixture

Use [withOpenSyntheticFixture or withClosedSyntheticFixture](../../scripts/jones/performance/fixtures.mjs)
with an absent child name, source binding and explicit path/size policy. The
wrappers retain their original ownership handles. Policy bounds access; it does
not grant access to protected data.
The default `current-v2` producer uses the receiving persistence loader and V2
event/projection APIs. Select `historical-v1` explicitly when historical evidence
is the intended input.

Choose `health-offline-delete` for a closed fixture intended for read-only health
inspection. It records checkpoint/DELETE maintenance and verifies the closed
header and sidecar state before sealing. `benchmark-wal` preserves observed
production WAL settings. Changing the profile does not convert a V1 fixture into
V2 evidence or establish installed-server durability.

Current receipts use `jones-performance-fixture/v2` and include the producer and
candidate binding; historical receipts retain `/v1`. For a consumer that accepts
only the guard's `/v1` receipt, use `fixtureCustodyReceipt` and pin the digest of
that custody projection separately. Preserve the full producer receipt and its
digest as well. A digest calculated from arbitrary input does not establish
trusted provenance.

Finish consumer reads and close their resources inside the closed-fixture
callback. Return a `jones-performance-fixture-consumer/v1` outcome containing the
callback's full `receiptSha256`, an explicit `release` or `retain` disposition and
the diagnostic value. A raw report is not a release outcome. Missing outcomes,
wrong digests, callback rejection and unknown resource or child closure retain
the root with evidence. Successful wrapper cleanup makes returned paths historical
evidence: the files have been removed. A serialized receipt or retained path is
not a write or cleanup capability; reconcile retained roots with their original
owner rather than issuing a generic deletion command.

## SQLite health

[sqliteHealth.ts](../../apps/server/scripts/jones/sqliteHealth.ts) accepts a sealed
synthetic custody receipt on noninteractive stdin, with its independently trusted
digest and binding passed separately. The default schema profile is
`orchestration-v2`; select `legacy-v1` for a historical V1 fixture. Defaults inspect
metadata only. Counts, allocation, integrity and foreign-key checks require
`--include`.

After the producer has supplied the closed fixture and trusted custody inputs,
this is the metadata-only invocation:

```sh
node apps/server/scripts/jones/sqliteHealth.ts \
  --fixture-root "$FIXTURE_ROOT" \
  --fixture-receipt-sha256 "$TRUSTED_CUSTODY_RECEIPT_SHA256" \
  --fixture-binding-json "$TRUSTED_FIXTURE_BINDING_JSON" \
  --schema-profile orchestration-v2 \
  < "$CUSTODY_RECEIPT_JSON"
```

To request additional checks, add, for example,
`--include counts,integrity,foreign-keys --deadline-ms 60000`. Read component
statuses independently: omitted, unavailable, partial or truncated results do
not certify integrity or foreign keys. Connection settings describe this health
connection, not production durability.

The tool refuses a WAL-format header or any WAL/SHM/journal sidecar before SQLite
opens the fixture. A read-only WAL open can still change sidecars; immutable mode
is not assumed. A successful native backup is therefore not automatically a
read-only-health-qualified fixture. The input remains owned by its producer.
Within a closed-fixture callback, `sqliteHealthConsumerOutcome` converts the full
producer digest and health report into a release only when child close/reap and
supervisor cleanup are known. Report status alone does not prove closure.

## Explicit qualification, workload and restore requests

Prepare a request outside the checkout before using either `--request` entry
point. Current qualification accepts exactly `{ candidate, parentPath }`.
The benchmark additionally requires `workload` with `commands` from 1 to 64,
`payloadBytes` from 1 to 4096 and `intervalMs` from 0 to 100. The supplied parent
must be an existing canonical directory within the permitted task boundary.

```sh
JONES_RUNTIME_BINDING="$RUNTIME_BINDING_JSON" \
  node scripts/jones/performance/current-qualification.mjs \
  --request "$QUALIFICATION_REQUEST_JSON"

JONES_RUNTIME_BINDING="$RUNTIME_BINDING_JSON" \
  node apps/server/scripts/jones/v2Benchmark.mjs \
  --request "$BENCHMARK_REQUEST_JSON"
```

The [V2 workload](../../apps/server/src/jones/performance/v2Workload.ts) exercises
single-thread metadata acceptance, replay, rejection, rollback and retry through
current services. Its timing separates scheduled-arrival lag, harness queue wait
and EventSink completion. It does not measure individual SQL timing, pure lock
wait, contention capacity or provider execution. Preserve the returned evidence
path and inspect runner/database closure and cleanup separately from the workload
outcome. Unknown closure retains scratch for reconciliation by its original owner.

[Migration/restore qualification](../../scripts/jones/performance/migration-restore.mjs)
is opt-in through `JONES_MIGRATION_RESTORE_REQUEST`, which names a JSON file with
schema `jones-performance-migration-restore-request/v2`, an explicit `cases` list
and `options`. Options require `explicitSyntheticRequest: true`, the exact
candidate and matching repository/source binding, original task/run identifiers,
parent path and policy. Runtime binding remains required; historical cases also
need their prepared pinned source parent. The default native suite checks only
the table, pins and refusal paths.

After preparing those inputs for an explicitly requested synthetic run:

```sh
JONES_RUNTIME_BINDING="$RUNTIME_BINDING_JSON" \
  JONES_PERFORMANCE_SOURCE_PARENT="$HISTORICAL_SOURCE_PARENT" \
  JONES_MIGRATION_RESTORE_REQUEST="$RESTORE_REQUEST_JSON" \
  node --test --test-concurrency=1 \
  scripts/jones/performance/migration-restore.node-test.mjs
```

The open cases import historical data under the receiving loader without
redispatching V1 writes. Foreign lookup007 history, including `lease-current`,
and the known foreign V2 prefixes are preserved without feature adoption;
unrecognized names or missing prefixes are refused. Rollback injection uses the
receiving Jones transaction guard. Restore cases compare canonical content after
native backup and reopen under the candidate. Because custody precreates the
destination, this harness copies with native backup before invoking the
initializer; the initializer's absent-destination copy path has separate
[focused coverage](../../apps/server/src/persistence/initializeV2Database.test.ts).
Native backup drains to settlement before cleanup even after cancellation.

The migration report uses `/v2` semantics and retains native-backup `/v1` evidence.
Header and sidecar observations do not grant read-only health qualification. The
`receiving-creation-lookup` case targets `055_OrchestrationV2/RecoveryIndexes` and
checks the receiving lookup while preserving foreign history without adopting
its features. Current qualification includes the receiving lookup and native
bootstrap acceptance oracles; it still requires an explicit candidate and runtime
binding. Default checks do not execute the bound qualification runs. Neither
source coverage nor synthetic qualification establishes installed-runtime
qualification, and current qualification does not qualify historical sources.
