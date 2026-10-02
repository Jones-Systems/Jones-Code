# Native Workstreams provider enrollment

Artifact Type: integration-guide
Artifact ID: integration.jones-workstreams-native-enrollment
Purpose: Describe prepared native enrollment and its separate operator effects.
Governing artifact: spec.jones-workstreams-restoration
Integration owner: Jones Code runtime owner
Consumers: authorized enrollment operators and verifiers
Authority effect: none

This is source preparation. Every real store read, enrollment, credential publication, configuration change, migration, installation and service/thread restart remains held until M Jones approves the exact operation. Neither a CLI flag nor this guide grants that authority. No laptop Jones/T3/Electron executable may be launched under the current owner hold.

## Existing installation qualification

The standalone `workstream provider` commands require an explicitly named existing T3 base directory and native authority directory. Their bounded non-creating reader checks persisted environment identity, current authority, launcher/release qualification, and the invoked package's stamped Jones source/build. Missing, pending, fenced, stale or mismatching qualification fails closed. No command initializes a T3 home, enrolls native authority, runs migrations, generates a signing key or invents a runtime configuration destination.

Persisted qualification is not live IPC or proof of quiescence. Bind the future operation to the actual host, ordinary UID, home/store, qualified executable/build, current authority tuple and observed process state. An absent launcher or release receipt is not permission to substitute development metadata.

## Exact command inputs

Each `plan`, `apply` and `readback` takes `--base-dir`, `--authority-dir`, `--credential-path` and an existing positional request file. The request is the closed `jones-code.workstreams-native-enrollment/v1` object: `enrollment_id`, canonical HTTPS `registry_origin`, full native `context`, and reserved `session` identity/issuance/expiry. Context and enrollment IDs must agree; build and native authority must match trusted qualification. The reserved session uses exactly the dedicated native context, settlement and reconciliation scopes, with a maximum 30-day lifetime.

| Operation  | Additional input                                               | Effect                                                                      |
| ---------- | -------------------------------------------------------------- | --------------------------------------------------------------------------- |
| `plan`     | `--sqlite-sidecar-effects deny                                 | allow`, default `deny`                                                      | Inspect existing enrollment/session records and credential hash; never reserve or issue                                   |
| `readback` | `--expected-credential-sha256 SHA`, plus the same sidecar flag | Verify exact existing records and private-file hash; never reserve or issue |
| `apply`    | `--expected-prior-sha256 absent                                | SHA`                                                                        | Reserve exact records, materialize only that existing session and publish the private credential under its preimage guard |

Retries retain the same closed request, reserved session ID and canonical request digest. Changed bytes conflict; they do not replace authority. Existing session materialization refuses revocation, expiry, widened scopes, changed subjects or issuance metadata. Only apply constructs the adapter that reads the existing fixed signing key. It creates or rotates no key and returns no bearer in public output.

The private writer publishes a bounded same-UID `0600` credential containing only `authorization_header`. It stages only its owned file and uses no-clobber publication. Exact existing bytes are unchanged; different existing bytes are preserved as a conflict. Apply is separate from registry enrollment and runtime configuration.

## SQLite effects and supported runtimes

Strict inspection (`deny`) supports an already quiescent rollback-mode database with no journal, WAL or SHM sidecars. WAL headers or present sidecars are refused before SQL construction. Filesystem metadata alone cannot prove the required quiescence; do not checkpoint, change journal mode, copy a store or stop a service to obtain it under this guide.

Operational inspection (`allow`) is a protected store operation that can create, initialize, size or update WAL/SHM files even though its main database connection is read-only. Its future approval envelope must cover those effects on the exact existing `state.sqlite` and its sidecars. Apply is explicitly mutable and needs its own envelope covering records, private publication and SQLite sidecars. The flag is available only for plan/readback.

The fixed file URI uses `mode=ro&cache=private&vfs=unix` for inspection and `mode=rw&cache=private&vfs=unix` for apply. `mode=rw` refuses a missing main file rather than creating one. Extension loading is disabled; callers cannot supply URI parameters, immutable/nolock options or alternate stores. Actual runtime qualification precedes opening and accepts only these source-qualified pairs:

| Node    | Bundled SQLite |
| ------- | -------------- |
| 24.13.1 | 3.51.2         |
| 24.19.0 | 3.53.3         |
| 24.21.0 | 3.53.4         |

Other pairs are refused. Source qualification is not fixture or installed-runtime verification. Missing schema, hot-journal recovery requirements, lock failures or changing installation/authority qualification produce typed failure; no repair or alternate-store fallback is attempted. Close only the command's connection on failure. Never delete, restore or clean live sidecars.

## Receipt and activation sequence

The public native receipt binds enrollment ID, canonical request digest, session ID, native context, safe state/reason, credential pre/post hashes and independently observed `native_records_reserved` / `native_credential_published` phases. It excludes signing bytes, bearer, configuration contents and database URLs. An unknown commit or publication requires exact readback before another effect; do not resubmit blindly.

A future authorized restoration separately qualifies the native build/store, reserves and publishes its dedicated credential, applies exact control-plane grant/source preimages with the backend enrollment tool, stages both server configurations, then performs authenticated runtime readback. Native success does not establish registry authority or live availability.

Jones transport requires all six canonical activation selectors: `T3_WORKSTREAM_CONTROL_PLANE_URL`, `T3_WORKSTREAM_OWNER_ID`, `T3_WORKSTREAM_PRINCIPAL_ID`, `T3_WORKSTREAM_KEY_ID`, `T3_WORKSTREAM_SIGNING_SECRET`, and `T3_WORKSTREAM_AUTHORIZATION_REVISION`. Their private values and destination must come from the exact enrolled runtime/launcher route. Do not invent them or copy another host's credentials. Absent configuration stays disabled.

List/create metadata remains independent of native reference preparation. Selected threads register in quarantine, verify against qualified native authority, reload current references/placements and only then attach/move. GitHub PR references use their separate configured source and immutable locator. Retry reloads metadata and observes existing command IDs; it never resubmits an unknown effect.

The exact Node 24.21.0/SQLite 3.53.4 pair is additionally source-qualified against the [Node SQLite binding](https://github.com/nodejs/node/blob/v24.21.0/src/node_sqlite.cc) and [bundled SQLite](https://github.com/nodejs/node/blob/v24.21.0/deps/sqlite/sqlite3.c). The URI opening block and relevant WAL/Unix-VFS paths retain the preceding qualified behavior; this does not admit arbitrary Node 24 or SQLite 3.53 releases. Synthetic behavior qualification is recorded with the implementation checks.
