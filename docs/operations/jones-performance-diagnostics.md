# Jones performance diagnostics

These maintainer tools exercise isolated synthetic databases from a bound source
checkout. They do not qualify an installed application, measure live provider
capacity, or supply a route to discover or copy private databases. Follow
[development test-data guidance](./development.md#test-data) for the source,
destination and lifetime of any separately authorized real-data snapshot.

Use the repository's supported Node runtime (`^24.13.1`) and prepared dependencies.
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
It records header and sidecar observations honestly, without granting read-only
health qualification. `successor-138` remains unavailable until the separately
bound `138_JonesThreadCreationLookupIndex` manifest exists. Current qualification
also explicitly holds native acceptance coverage until its dedicated source is
bound. Default checks, historical pins and synthetic reports do not resolve those
holds or establish installed-runtime qualification.
