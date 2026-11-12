# SQLite fixtures

Load this reference only when inspecting or seeding local T3 state directly.

## Select the correct database

When `--base-dir` or `--home-dir` is explicit, runtime state lives under `<base-dir>/userdata` and the database path is `<base-dir>/userdata/statev2.sqlite`. The `<base-dir>/dev` state directory is only the fallback for an implicit development home, preventing an ordinary `vp run dev` from touching production state.

The server copies the V1 `state.sqlite` into `statev2.sqlite` only when `statev2.sqlite` is missing. After the first start, edits to `state.sqlite` change nothing.

Start the target runtime once before seeding so all migrations have run. Use an isolated base directory. Stop the server before writes to avoid racing application state or an active projection.

## Use the helper

List tables:

```bash
node apps/server/scripts/t3-sqlite-state.ts query \
  --base-dir <base-dir> \
  --sql "SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name"
```

Inspect current columns before writing a fixture:

```bash
node apps/server/scripts/t3-sqlite-state.ts query \
  --base-dir <base-dir> \
  --sql "PRAGMA table_info(orchestration_v2_projection_threads)"
```

Apply a SQL fixture from a file:

```bash
node apps/server/scripts/t3-sqlite-state.ts exec \
  --base-dir <base-dir> \
  --file /tmp/t3-seed.sql
```

Use one statement per invocation for both `query` and `exec`; the helper wraps writes in a transaction and prints the backup path after a successful mutation. Use a single insert with multiple value rows when a fixture needs several records.

## Seed projection data carefully

Clients read projects from `projection_projects` and everything else from the `orchestration_v2_projection_*` tables: threads, runs, messages, turn items, runtime requests, plans, and provider sessions. The older `projection_thread*` tables hold V1 history, which the server imports once per thread at startup; later edits there do not reach the UI.

Most V2 rows carry a `payload_json` that must decode against the schemas in `packages/contracts/src/orchestrationV2.ts`. The safest start is a row the app wrote itself: create a thread through the UI, copy its rows, and edit them. Keep identifiers unique, timestamps as ISO strings, and related project, thread, and run IDs consistent.

Direct projection writes are appropriate for ephemeral visual states, edge-case counts, long titles, long timelines, and similar UI fixtures. They do not create a coherent event history. Leave the event log (`orchestration_events`) unchanged, and do not use direct projection writes to claim backend business behavior works.

Use the app's commands or APIs for behavior tests. Use `node apps/server/src/bin.ts auth ...` for auth state rather than editing `auth_pairing_links` or `auth_sessions`.

## Empty native Workstreams home

On Linux, use `node apps/server/src/jones/workstreams/testing/nativeHomeFixture.ts
<absolute-home>` from a disposable source copy stamped with the exact candidate's
`jonesSource`. Create `<absolute-home>` as a new, empty, owned `0700` directory
outside `~/.t3`; the helper refuses nonempty, shared or symlinked targets. The
caller owns that directory's lifetime and any partially prepared home after a
failure. Stop all consumers before removing the caller's exact fixture directory.

The helper creates synthetic environment/launcher metadata and a synthetic
receipt, then calls real authority enrollment, registry key/init commands,
SessionStore startup (including its signing key), and source-setup
plan/apply/readback. It saves `source-setup-request.json` and native
config, including `bootstrap.source_session_id` (the issued AuthSession ID,
not an enrollment ID or bearer). This exercises source services; the receipt
attests no packaged executable, installation or launcher process.

Keep the home stopped through setup: standalone plan/readback require a quiesced
rollback-journal database. Start the isolated app only after preparation.
`vp run dev --home-dir <absolute-home> --no-seed` supports renderer inspection;
native mutations additionally require a current launcher IPC parent and server
ownership. The dev runner removes launcher context, so this command alone does
not qualify native assignment acceptance. Do not stamp durable source,
reuse live state, or treat direct SQL rows as business-behavior proof. The adjacent
`nativeHomeFixture.test.ts` checks startup selection and the empty native HTTP
read using the real persisted registry and keys.
