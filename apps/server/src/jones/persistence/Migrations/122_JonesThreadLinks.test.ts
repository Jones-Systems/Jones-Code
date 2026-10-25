import { expect, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "./122_JonesThreadLinks.ts";

it.effect("keeps sparse links independent, identity immutable and acceptance monotone", () =>
  Effect.gen(function* () {
    yield* migration;
    const sql = yield* SqlClient.SqlClient;
    const id = "tl1:" + "a".repeat(64);
    yield* sql`INSERT INTO jones_thread_links
    (link_id, kind, from_environment_id, from_thread_id, to_environment_id, to_thread_id, basis, asserted_by, reverse_state, acceptance, created_at, updated_at)
    VALUES (${id}, 'continuation', 'here', 'a', 'here', 'b', 'manual', 'user', 'local', 'offered', 'now', 'now')`;
    expect(yield* sql`PRAGMA foreign_key_list(jones_thread_links)`).toEqual([]);
    yield* sql`UPDATE jones_thread_links SET acceptance = 'accepted', accepted_at = 'now' WHERE link_id = ${id}`;
    for (const statement of [
      sql`UPDATE jones_thread_links SET acceptance = 'offered'`,
      sql`UPDATE jones_thread_links SET from_thread_id = 'other'`,
      sql`UPDATE jones_thread_links SET asserted_by = 'system'`,
      sql`UPDATE jones_thread_links SET reverse_state = 'unknown'`,
      sql`DELETE FROM jones_thread_links`,
    ])
      expect(Exit.isFailure(yield* Effect.exit(statement))).toBe(true);
    yield* sql`UPDATE jones_thread_links SET removed_at = 'later'`;
    yield* sql`INSERT OR REPLACE INTO jones_thread_links
    (link_id, kind, from_environment_id, from_thread_id, to_environment_id, to_thread_id, basis, asserted_by, reverse_state, created_at, updated_at)
    VALUES (${id}, 'peer', 'here', 'a', 'here', 'c', 'manual', 'user', 'local', 'later', 'later')`;
    expect((yield* sql`SELECT kind, acceptance, removed_at FROM jones_thread_links`)[0]).toEqual({
      kind: "continuation",
      acceptance: "accepted",
      removed_at: "later",
    });
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);
