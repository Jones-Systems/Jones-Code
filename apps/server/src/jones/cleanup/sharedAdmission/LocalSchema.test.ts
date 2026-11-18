// @effect-diagnostics nodeBuiltinImport:off -- Owned real databases exercise durable startup rows, copied identities and immutable SQL constraints.
import * as NodeFS from "node:fs";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import * as Local from "./LocalSchema.ts";
import * as Incarnation from "./DatabaseIncarnation.ts";
import {
  makeOwnedFixture,
  makeHeldFixture,
  prepareSyntheticOpen,
} from "./DatabaseIncarnation.fixture.ts";
import * as Inventory from "./EnrollmentInventory.ts";
import { AdmissionError, canonical, digest } from "./Protocol.ts";

describe("immutable local enrollment schema", () => {
  it.effect(
    "UUID is issued once from pre-open binding; migration rerun and exact reads preserve it",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* makeOwnedFixture;
          yield* Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            const token = yield* Incarnation.bindOpen(f.open);
            yield* Local.migration;
            yield* sql`PRAGMA synchronous=NORMAL`;
            const session = yield* Local.beginDurableStartup(token);
            const first = yield* Local.ensureStoreIdentity(token);
            yield* Local.migration;
            assert.deepStrictEqual(yield* Local.ensureStoreIdentity(token), first);
            for (const statement of [
              "UPDATE jones_common_store_identity SET store_id='changed'",
              "DELETE FROM jones_common_store_identity",
              "INSERT OR REPLACE INTO jones_common_store_identity SELECT * FROM jones_common_store_identity",
              "REPLACE INTO jones_common_store_identity SELECT * FROM jones_common_store_identity",
            ])
              assert.isTrue(Exit.isFailure(yield* Effect.exit(sql.unsafe(statement))));
            assert.deepStrictEqual(yield* Local.ensureStoreIdentity(token), first);
            // A missing publication implementation is not a successful boundary.
            const unavailable = yield* Effect.exit(
              session.completePublication.pipe(
                Effect.provideService(Local.PublishedEnrollment, {
                  assertPublished: Effect.fail(
                    new AdmissionError({ reason: "synthetic_publication_not_complete" }),
                  ),
                }),
              ),
            );
            assert.isTrue(Exit.isFailure(unavailable));
            assert.deepStrictEqual(yield* sql`PRAGMA synchronous`, [{ synchronous: 2 }]);
          }).pipe(
            Effect.provide(NodeSqliteClient.layer({ filename: f.dbPath, writerWorker: true })),
          );
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("a copied immutable UUID refuses under the destination's actual physical binding", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const source = yield* makeOwnedFixture;
        const destination = yield* makeHeldFixture;
        yield* Effect.gen(function* () {
          const token = yield* Incarnation.bindOpen(source.open);
          yield* Local.migration;
          yield* Local.ensureStoreIdentity(token);
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: source.dbPath })));
        // Destination has no persistence graph or pre-open token during the copy.
        NodeFS.copyFileSync(source.dbPath, destination.dbPath);
        const destinationOpen = yield* prepareSyntheticOpen(destination.owner, destination.root);
        yield* Effect.gen(function* () {
          const token = yield* Incarnation.bindOpen(destinationOpen.open);
          assert.isTrue(Exit.isFailure(yield* Effect.exit(Local.ensureStoreIdentity(token))));
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: destination.dbPath })));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("sparse manifest/seal commits require FULL/fullfsync and exact immutable retry", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* makeOwnedFixture;
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const token = yield* Incarnation.bindOpen(f.open);
          yield* Local.migration;
          const { storeId } = yield* Local.ensureStoreIdentity(token);
          yield* sql`INSERT INTO jones_common_admission_installation VALUES
          (1,${storeId},${canonical(Incarnation.bindingForSql(token, sql).database)},'journal','synthetic-binding')`;
          const source = {
            domain: "synthetic",
            referenceId: "reference",
            revision: "1",
            sha256: digest("source"),
          };
          const entry: Inventory.InventoryEntry = {
            itemKey: Inventory.itemKey(storeId, source),
            source,
            scopes: [f.infrastructure.scope],
            disposition: "reference",
            owner: null,
            terminalEvidenceSha256: null,
          };
          const entries = [entry];
          const seal = {
            enrollmentId: "enrollment",
            storeId,
            journalId: "journal",
            participantNonce: "nonce",
            inventoryDigest: Inventory.inventoryDigest(entries),
            itemCount: 1,
            schemaContractDigest: digest("schema"),
            coverageContract: "synthetic",
            sourceSnapshotDigest: digest("snapshot"),
          };
          assert.isTrue(Exit.isFailure(yield* Effect.exit(Local.commitEnrollment(seal, entries))));
          yield* sql`PRAGMA synchronous=NORMAL`;
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(sql.withTransaction(Local.commitEnrollment(seal, entries))),
            ),
          );
          assert.deepStrictEqual(yield* sql`SELECT * FROM jones_common_enrollment_seals`, []);
          yield* sql`PRAGMA synchronous=FULL`;
          yield* sql`PRAGMA fullfsync=OFF`;
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(sql.withTransaction(Local.commitEnrollment(seal, entries))),
            ),
          );
          yield* Local.enableDurableWriter(sql);
          yield* sql.withTransaction(Local.commitEnrollment(seal, entries));
          yield* sql.withTransaction(Local.commitEnrollment(seal, entries));
          assert.deepStrictEqual(yield* Local.readEnrollmentSeal(seal.enrollmentId), seal);
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(
                sql.withTransaction(
                  Local.commitEnrollment({ ...seal, participantNonce: "other" }, entries),
                ),
              ),
            ),
          );
          for (const table of [
            "jones_common_enrollment_seals",
            "jones_common_enrollment_manifest",
          ]) {
            for (const statement of [
              `DELETE FROM ${table}`,
              `INSERT OR REPLACE INTO ${table} SELECT * FROM ${table}`,
            ])
              assert.isTrue(Exit.isFailure(yield* Effect.exit(sql.unsafe(statement))));
          }
          assert.deepStrictEqual(yield* Local.readEnrollmentSeal(seal.enrollmentId), seal);
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: f.dbPath, writerWorker: true })));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );
});
