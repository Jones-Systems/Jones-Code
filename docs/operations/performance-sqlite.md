# Synthetic SQLite health reports

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
