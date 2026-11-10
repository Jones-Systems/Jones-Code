// @effect-diagnostics nodeBuiltinImport:off -- Synthetic DB/WAL fixtures are retained under the external evidence TMPDIR for root cleanup.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as Db from "./sql.ts";
import * as Test from "./sql.testkit.ts";
import migration from "../../../persistence/Migrations/130_JonesWorkstreamsForwardReceipts.ts";
it.effect(
  "forward request/result envelopes authenticate owner/source/forward/purpose and retain exact bytes",
  () =>
    Effect.gen(function* () {
      yield* Test.seed;
      yield* migration;
      const db = yield* Db.NativeSql;
      const scope = {
        ...Test.scope,
        purpose: "forward_request",
        record_id: JSON.stringify(["source", "forward-000000000001"]),
        revision: 1,
      };
      const raw = ' { "protected-request-sentinel": 1 }\n';
      const envelope = yield* db.sealJson(scope, raw);
      expect(envelope).not.toContain("protected-request-sentinel");
      expect(yield* db.openJson(scope, envelope)).toBe(raw);
      for (const patch of [
        { owner_id: "other" },
        { registry_instance_id: "other" },
        { record_id: JSON.stringify(["other", "forward-000000000001"]) },
        { record_id: JSON.stringify(["source", "other"]) },
        { purpose: "forward_result" },
        { revision: 2 },
      ]) {
        expect((yield* Effect.result(db.openJson({ ...scope, ...patch }, envelope)))._tag).toBe(
          "Failure",
        );
      }
      const sql = yield* SqlClient.SqlClient;
      expect(yield* sql`SELECT * FROM jones_workstreams_forward_receipts`).toHaveLength(0);
    }).pipe(Effect.provide(Test.testLayer())),
);

it.effect(
  "encrypted parent ledger survives SQLite reopen and leaves no sentinel plaintext in DB/WAL",
  () =>
    Effect.gen(function* () {
      const dir = yield* Effect.sync(() =>
        NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "nw-home-forward-ledger-")),
      );
      const filename = NodePath.join(dir, "synthetic.sqlite");
      const scope = {
        ...Test.scope,
        purpose: "forward_request",
        record_id: JSON.stringify(["source", "forward-restart-000001"]),
        revision: 1,
      };
      const request = ' {"protected-ledger-request-sentinel": "exact bytes"}\n';
      const result = '{"protected-ledger-result-sentinel":true}';
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* Test.seed;
          yield* migration;
          const sql = yield* SqlClient.SqlClient,
            db = yield* Db.NativeSql;
          yield* sql.unsafe("PRAGMA journal_mode=WAL");
          const req = yield* db.sealJson(scope, request),
            res = yield* db.sealJson({ ...scope, purpose: "forward_result" }, result);
          yield* sql`INSERT INTO jones_workstreams_forward_receipts ${sql.insert({
            ...Test.scope,
            source_environment_id: "source",
            forward_id: "forward-restart-000001",
            request_sha256: "a".repeat(64),
            identity_sha256: "b".repeat(64),
            wire_sha256: "c".repeat(64),
            state: "accepted",
            request_envelope: req,
            result_envelope: null,
          })}`;
          yield* sql`UPDATE jones_workstreams_forward_receipts SET state='completed',result_envelope=${res}`;
          const paths = [filename, filename + "-wal"].filter((p) => NodeFS.existsSync(p));
          expect(paths).toHaveLength(2);
          for (const path of paths) {
            const bytes = yield* Effect.sync(() => NodeFS.readFileSync(path));
            expect(bytes.includes(Buffer.from("protected-ledger-request-sentinel"))).toBe(false);
            expect(bytes.includes(Buffer.from("protected-ledger-result-sentinel"))).toBe(false);
          }
        }).pipe(Effect.provide(Test.testLayer({ filename }))),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient,
            db = yield* Db.NativeSql;
          const [row] = yield* sql<{
            request_envelope: string;
            result_envelope: string;
            state: string;
          }>`SELECT * FROM jones_workstreams_forward_receipts`;
          expect(row!.state).toBe("completed");
          expect(yield* db.openJson(scope, row!.request_envelope)).toBe(request);
          expect(
            yield* db.openJson({ ...scope, purpose: "forward_result" }, row!.result_envelope),
          ).toBe(result);
        }).pipe(Effect.provide(Test.testLayer({ filename }))),
      );
      // Retain the synthetic directory through parent delivery; root owns cleanup.
    }),
);
