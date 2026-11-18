import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as Bridge from "./LocalBridge.ts";
import * as Local from "./LocalSchema.ts";
import * as Incarnation from "./DatabaseIncarnation.ts";
import * as Journal from "./Journal.ts";
import { makeOwnedFixture } from "./DatabaseIncarnation.fixture.ts";
import { canonical, failure, Participant, storageError } from "./Protocol.ts";

it.effect(
  "pin retry reads exact bytes; SQL UPDATE, DELETE and REPLACE cannot change the installation",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* makeOwnedFixture;
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const token = yield* Incarnation.bindOpen(f.open);
          yield* Bridge.createLocalBridgeSchema;
          const identity = yield* Local.ensureStoreIdentity(token);
          const participant = Participant.of({
            sql,
            identity: {
              storeId: identity.storeId,
              database: identity.binding.database,
              nonce: "synthetic-nonce",
              coverage: "synthetic-real-open",
            },
            checkOutside: Incarnation.validateCheckedOwner(token).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
            ),
            validateLocal: Effect.gen(function* () {
              if (Option.isNone(yield* Effect.serviceOption(sql.transactionService)))
                return yield* failure("fixture_requires_local_transaction");
              const rows = yield* sql<{ store_id: string; physical_binding_json: string }>`
            SELECT store_id,physical_binding_json FROM jones_common_store_identity WHERE singleton=1`;
              if (
                rows[0]?.store_id !== identity.storeId ||
                rows[0]?.physical_binding_json !== canonical(identity.binding)
              )
                return yield* failure("fixture_store_binding_changed");
            }).pipe(Effect.mapError(storageError)),
          });
          const pin = Bridge.pinLocalInstallation.pipe(
            Effect.provideService(Participant, participant),
            Effect.provideService(Journal.JournalConnection, f.bootstrap.connection),
          );
          yield* sql`PRAGMA synchronous=NORMAL`;
          yield* pin;
          const before = yield* sql`SELECT * FROM jones_common_admission_installation`;
          yield* pin;
          for (const statement of [
            "UPDATE jones_common_admission_installation SET journal_id='other'",
            "DELETE FROM jones_common_admission_installation",
            "INSERT OR REPLACE INTO jones_common_admission_installation SELECT * FROM jones_common_admission_installation",
          ])
            assert.isTrue(Exit.isFailure(yield* Effect.exit(sql.unsafe(statement))));
          assert.deepStrictEqual(
            yield* sql`SELECT * FROM jones_common_admission_installation`,
            before,
          );
          assert.lengthOf(before, 1);
          yield* Local.assertDurableWriter(sql);
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: f.dbPath, writerWorker: true })));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
