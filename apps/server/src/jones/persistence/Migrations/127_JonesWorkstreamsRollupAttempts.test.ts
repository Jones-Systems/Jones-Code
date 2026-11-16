import { expect, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import nativeMigration from "./125_JonesWorkstreamsNative.ts";
import migration from "./127_JonesWorkstreamsRollupAttempts.ts";

it.effect("127 preserves populated queue fences and rollback while separating retry attempts", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE upstream_sentinel(id TEXT PRIMARY KEY)`;
    yield* sql`INSERT INTO upstream_sentinel VALUES ('preserved')`;
    yield* nativeMigration;
    for (const [id, state, fence, failure] of [
      ["published", "dead_letter", 7, "published"],
      ["failed", "dead_letter", 3, "input_unavailable"],
      ["pending", "pending", 5, null],
      ["running", "running", 2, null],
    ] as const) {
      yield* sql`INSERT INTO jones_workstreams_rollup_queue
        (registry_instance_id,owner_id,workstream_id,manifest_sha256,state,attempt_id,fence,lease_expires_at,failure_code,failed_at,updated_at)
        VALUES ('registry','owner',${id},${"a".repeat(64)},${state},${state === "running" ? "attempt" : null},${fence},
        ${state === "running" ? "2026-10-26T12:01:00.000Z" : null},${failure},${failure ? "2026-10-26T12:00:00.000Z" : null},'2026-10-26T12:00:00.000Z')`;
    }
    const before = yield* sql`SELECT * FROM jones_workstreams_rollup_queue ORDER BY workstream_id`;
    const failed = yield* Effect.result(
      sql.withTransaction(migration.pipe(Effect.andThen(Effect.fail("rollback probe")))),
    );
    expect(failed._tag).toBe("Failure");
    expect(
      (yield* sql<{ name: string }>`PRAGMA table_info(jones_workstreams_rollup_queue)`).some(
        (column) => column.name === "attempt_count",
      ),
    ).toBe(false);
    expect(yield* sql`SELECT * FROM jones_workstreams_rollup_queue ORDER BY workstream_id`).toEqual(
      before,
    );

    yield* sql.withTransaction(migration);
    const after = yield* sql`SELECT * FROM jones_workstreams_rollup_queue ORDER BY workstream_id`;
    expect(after.map(({ attempt_count: _attempts, ...row }) => row)).toEqual(before);
    expect(
      yield* sql`SELECT workstream_id,fence,attempt_count FROM jones_workstreams_rollup_queue ORDER BY workstream_id`,
    ).toEqual([
      { workstream_id: "failed", fence: 3, attempt_count: 3 },
      { workstream_id: "pending", fence: 5, attempt_count: 5 },
      { workstream_id: "published", fence: 7, attempt_count: 0 },
      { workstream_id: "running", fence: 2, attempt_count: 2 },
    ]);
    expect(
      (yield* Effect.result(sql`UPDATE jones_workstreams_rollup_queue SET attempt_count=-1`))._tag,
    ).toBe("Failure");
    expect(
      (yield* Effect.result(
        sql`UPDATE jones_workstreams_rollup_queue SET fence=0 WHERE workstream_id='published'`,
      ))._tag,
    ).toBe("Failure");
    expect(yield* sql`SELECT * FROM upstream_sentinel`).toEqual([{ id: "preserved" }]);
    expect(
      (yield* sql`SELECT count(*) AS count FROM jones_workstreams_rollup_queue`)[0]?.count,
    ).toBe(4);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);
