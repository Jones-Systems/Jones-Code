import { expect, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import migrate from "../persistence/Migrations/157_JonesRemoteAdmissionReview.ts";
const layer = Layer.effectDiscard(migrate).pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);
it.effect(
  "review migration creates only sparse Jones tables without upstream objects or foreign keys",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const schema = yield* sql<{
        name: string;
        sql: string;
      }>`SELECT name,sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`;
      expect(schema.length).toBe(23);
      for (const table of schema) {
        expect(table.name.startsWith("jones_")).toBe(true);
        expect(table.sql.toUpperCase()).not.toContain("REFERENCES");
      }
      expect(yield* sql`SELECT * FROM jones_remote_admission_review_state`).toEqual([
        { singleton: 1, revision: 0, prune_revision: 0 },
      ]);
      const invalid =
        yield* sql`INSERT INTO jones_remote_admission_access_basis(admission_id,managed_scope_id,kind,grant_id,generation,binding_digest,scope_revision)
    VALUES('a','s','lease','g',1,'digest',1)`.pipe(Effect.exit);
      expect(invalid._tag).toBe("Failure");
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "ordinary observation acknowledgements retain exact session/kind keys with nullable admission",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`INSERT INTO jones_remote_admission_observation_acks VALUES('session-a','root','launch',NULL,'delivered','2026-11-16T00:00:00.000Z')`;
      yield* sql`INSERT INTO jones_remote_admission_observation_acks VALUES('session-b','root','launch','admission','suppressed','2026-11-16T00:00:00.000Z')`;
      yield* sql`INSERT INTO jones_remote_admission_observation_acks VALUES('session-a','root','retry','child','delivered','2026-11-16T00:00:00.000Z')`;
      expect((yield* sql`SELECT * FROM jones_remote_admission_observation_acks`).length).toBe(3);
      expect(
        (yield* sql`INSERT INTO jones_remote_admission_observation_acks VALUES('session-a','root','launch',NULL,'delivered','2026-11-16T00:00:00.000Z')`.pipe(
          Effect.exit,
        ))._tag,
      ).toBe("Failure");
      expect(
        (yield* sql`INSERT INTO jones_remote_admission_observation_acks VALUES('session-a','bad','other',NULL,'delivered','2026-11-16T00:00:00.000Z')`.pipe(
          Effect.exit,
        ))._tag,
      ).toBe("Failure");
    }).pipe(Effect.provide(layer)),
);
