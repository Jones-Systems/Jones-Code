import { expect, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "./112_JonesClientLaunchRequests.ts";

it.effect("keeps claims immutable, boolean and independent of upstream tables", () =>
  Effect.gen(function* () {
    yield* migration;
    const sql = yield* SqlClient.SqlClient;
    const insert = (flag: number) =>
      sql`INSERT INTO jones_mcp_client_launch_requests VALUES ('command', 'thread', 'digest', 1, ${flag}, 'now')`;
    expect(Exit.isFailure(yield* Effect.exit(insert(2)))).toBe(true);
    yield* insert(1);
    for (const statement of [
      sql`UPDATE jones_mcp_client_launch_requests SET message_requested = 0`,
      sql`DELETE FROM jones_mcp_client_launch_requests`,
      sql`INSERT OR REPLACE INTO jones_mcp_client_launch_requests VALUES ('command', 'other', 'changed', 1, 0, 'later')`,
    ])
      expect(Exit.isFailure(yield* Effect.exit(statement))).toBe(true);
    expect(yield* sql`SELECT message_requested FROM jones_mcp_client_launch_requests`).toEqual([
      { message_requested: 1 },
    ]);
    expect(yield* sql`PRAGMA foreign_key_list(jones_mcp_client_launch_requests)`).toEqual([]);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);
