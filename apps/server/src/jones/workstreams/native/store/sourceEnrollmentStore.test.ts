// @effect-diagnostics nodeBuiltinImport:off -- Synthetic file-backed SQLite/WAL evidence stays in the task cache for root delivery.
import * as NodeFS from "node:fs";
import * as NodeCrypto from "node:crypto";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "../../../persistence/Migrations/126_JonesWorkstreamsSourceEnrollments.ts";
import * as Records from "./sourceEnrollmentStore.ts";
import { seed, scope, testLayer } from "./sql.testkit.ts";

const layer = Records.layer.pipe(Layer.provideMerge(testLayer()));
it.effect(
  "encrypted issuance proof roundtrips and rejects identity collisions without mutation",
  () =>
    Effect.gen(function* () {
      yield* seed;
      yield* migration;
      const records = yield* Records.SourceEnrollmentStore,
        sql = yield* SqlClient.SqlClient;
      const record = {
        ...scope,
        enrollment_id: "enrollment",
        session_id: "session",
        request_sha256: "a".repeat(64),
        proof: { protected: "SOURCE-PROOF-SENTINEL" },
      };
      expect(yield* records.read({ session_id: "session" })).toBeNull();
      yield* records.insert(record);
      expect(yield* records.read({ enrollment_id: "enrollment" })).toEqual(record);
      expect(yield* records.read({ session_id: "session" })).toEqual(record);
      expect(
        JSON.stringify(yield* sql`SELECT * FROM jones_workstreams_source_enrollments`),
      ).not.toContain("SOURCE-PROOF-SENTINEL");
      expect((yield* Effect.result(records.insert({ ...record, proof: {} })))._tag).toBe("Failure");
      expect(
        (yield* Effect.result(records.insert({ ...record, enrollment_id: "other" })))._tag,
      ).toBe("Failure");
      expect(yield* records.read({ session_id: "session" })).toEqual(record);
    }).pipe(Effect.provide(layer)),
);

it.effect("owner-bound encryption refuses cross-owner envelope substitution", () =>
  Effect.gen(function* () {
    yield* seed;
    yield* migration;
    const records = yield* Records.SourceEnrollmentStore;
    const original = {
      ...scope,
      enrollment_id: "first",
      session_id: "session1",
      request_sha256: "a".repeat(64),
      proof: { private: "proof" },
    };
    yield* records.insert(original);
    const sql = yield* SqlClient.SqlClient;
    const row = (yield* sql<{
      envelope: string;
    }>`SELECT envelope FROM jones_workstreams_source_enrollments`)[0]!;
    yield* sql`INSERT INTO jones_workstreams_source_enrollments VALUES (${scope.registry_instance_id},'different-owner','second','session2',${"b".repeat(64)},${row.envelope})`;
    expect((yield* Effect.result(records.read({ session_id: "session2" })))._tag).toBe("Failure");
  }).pipe(Effect.provide(layer)),
);

it.effect("encrypted native proof sentinel is absent from file-backed SQLite and WAL", () =>
  Effect.gen(function* () {
    const directory = "node_modules/.cache/nw-source-encryption/" + NodeCrypto.randomUUID();
    NodeFS.mkdirSync(directory, { recursive: true });
    const filename = directory + "/proof.sqlite";
    yield* Effect.gen(function* () {
      yield* seed;
      yield* migration;
      const records = yield* Records.SourceEnrollmentStore,
        sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA journal_mode = WAL`;
      yield* records.insert({
        ...scope,
        enrollment_id: "file-proof",
        session_id: "file-session",
        request_sha256: "a".repeat(64),
        proof: { protected: "NW-SOURCE-ENCRYPTION-SENTINEL" },
      });
      for (const file of [filename, filename + "-wal"]) {
        expect(NodeFS.existsSync(file)).toBe(true);
        expect(
          NodeFS.readFileSync(file).includes(Buffer.from("NW-SOURCE-ENCRYPTION-SENTINEL")),
        ).toBe(false);
      }
    }).pipe(Effect.provide(Records.layer.pipe(Layer.provideMerge(testLayer({ filename })))));
  }),
);
