import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import { SqlError, UnknownError } from "effect/sql/SqlError";
import { runMigrations as runUpstreamMigrations } from "../../../persistence/Migrations.ts";
import { runJonesMigrations } from "../JonesMigrationGuard.ts";
import { jonesMigrationEntries } from "../JonesMigrations.ts";
import upgrade, {
  releasedOwnerTrigger,
  processOwnerTrigger,
} from "./141_JonesOrdinaryCheckoutProcessCustody.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const beforeUpgrade = jonesMigrationEntries.filter(([id]) => id < 148);

it.effect("fresh migration installs store-qualified views and preserves the released ledger", () =>
  Effect.gen(function* () {
    yield* runUpstreamMigrations();
    const sql = yield* SqlClient.SqlClient;
    const [trigger] = yield* sql<{
      readonly sql: string;
    }>`SELECT sql FROM sqlite_master WHERE name='jones_ordinary_checkout_owner_identity'`;
    assert.strictEqual(trigger?.sql, processOwnerTrigger);
    for (const name of ["jones_ordinary_checkout_all_uses", "jones_ordinary_checkout_all_events"]) {
      const columns = yield* sql.unsafe<{ readonly name: string }>(`PRAGMA table_info(${name})`);
      assert.strictEqual(columns[0]?.name, "store");
    }
    const ledger = yield* sql<{
      readonly migration_id: number;
      readonly name: string;
    }>`SELECT migration_id,name FROM jones_sql_migrations WHERE migration_id>=136 ORDER BY migration_id`;
    assert.deepStrictEqual(ledger, [
      { migration_id: 136, name: "WorkspaceProvenance" },
      { migration_id: 137, name: "NativeCreationLeaseRenewal" },
      { migration_id: 138, name: "WorkspaceRetirement" },
      { migration_id: 139, name: "ConversationWorkspace" },
      { migration_id: 140, name: "WorkstreamsNative" },
      { migration_id: 141, name: "WorkstreamsSourceEnrollments" },
      { migration_id: 142, name: "WorkstreamsRollupAttempts" },
      { migration_id: 143, name: "WorkstreamsPeerSessionPins" },
      { migration_id: 144, name: "WorkstreamsMemberBindings" },
      { migration_id: 145, name: "WorkstreamsForwardReceipts" },
      { migration_id: 146, name: "SettlementOriginRetention" },
      { migration_id: 147, name: "NativeExecutionProcessCustody" },
      { migration_id: 148, name: "OrdinaryCheckoutProcessCustody" },
      { migration_id: 149, name: "AuthSessionClientMetadata" },
      { migration_id: 150, name: "WorkspaceRuntimeUses" },
    ]);
    yield* runUpstreamMigrations();
    assert.strictEqual(
      (yield* sql`SELECT migration_id FROM jones_sql_migrations WHERE migration_id=148`).length,
      1,
    );
  }).pipe(Effect.provide(memory)),
);

it.effect(
  "trigger replacement and tracking row roll back with the enclosing migration transaction",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runJonesMigrations(beforeUpgrade);
      const [before] = yield* sql<{
        readonly sql: string;
      }>`SELECT sql FROM sqlite_master WHERE name='jones_ordinary_checkout_owner_identity'`;
      assert.strictEqual(before?.sql, releasedOwnerTrigger);
      const failAfterReplacement = upgrade.pipe(
        Effect.andThen(
          Effect.fail(
            new SqlError({ reason: new UnknownError({ cause: "injected after replacement" }) }),
          ),
        ),
      );
      yield* runJonesMigrations([
        ...beforeUpgrade,
        [148, "OrdinaryCheckoutProcessCustody", failAfterReplacement],
      ]).pipe(Effect.flip);
      const [after] = yield* sql<{
        readonly sql: string;
      }>`SELECT sql FROM sqlite_master WHERE name='jones_ordinary_checkout_owner_identity'`;
      assert.strictEqual(after?.sql, releasedOwnerTrigger);
      assert.strictEqual(
        (yield* sql`SELECT name FROM sqlite_master WHERE name IN ('jones_ordinary_checkout_process_uses','jones_ordinary_checkout_all_events')`)
          .length,
        0,
      );
      assert.strictEqual(
        (yield* sql`SELECT migration_id FROM jones_sql_migrations WHERE migration_id=148`).length,
        0,
      );
      yield* runJonesMigrations([
        ...beforeUpgrade,
        [148, "OrdinaryCheckoutProcessCustody", upgrade],
      ]);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "an unexpected owner trigger definition refuses the upgrade without changing its schema",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runJonesMigrations(beforeUpgrade);
      yield* sql`DROP TRIGGER jones_ordinary_checkout_owner_identity`;
      yield* sql`CREATE TRIGGER jones_ordinary_checkout_owner_identity BEFORE UPDATE ON jones_ordinary_checkout_owners BEGIN SELECT RAISE(ABORT,'unexpected'); END`;
      const [before] = yield* sql<{
        readonly sql: string;
      }>`SELECT sql FROM sqlite_master WHERE name='jones_ordinary_checkout_owner_identity'`;
      yield* runJonesMigrations([
        ...beforeUpgrade,
        [148, "OrdinaryCheckoutProcessCustody", upgrade],
      ]).pipe(Effect.flip);
      const [after] = yield* sql<{
        readonly sql: string;
      }>`SELECT sql FROM sqlite_master WHERE name='jones_ordinary_checkout_owner_identity'`;
      assert.strictEqual(after?.sql, before?.sql);
      assert.strictEqual(
        (yield* sql`SELECT name FROM sqlite_master WHERE name='jones_ordinary_checkout_process_uses'`)
          .length,
        0,
      );
    }).pipe(Effect.provide(memory)),
);

const time = "2026-11-03T10:00:00.000Z";
const seedLegacyHistory = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO jones_ordinary_checkout_owners VALUES('/repo','/repo','{"version":1}',NULL,'lease','thread','birth',1,NULL,${time},${time},${time},0,0,0,'owned')`;
  yield* sql`INSERT INTO jones_ordinary_checkout_admissions(admission_id,command_id,receipt_thread_id,subject_thread_id,command_type,command_digest,origin_kind,checkout_key,lease_id,owner_epoch,target_revision,birth_event_id,birth_sequence,receipt_sequence,basis_event_id,basis_sequence,recorded_at)
    VALUES('admission','command','thread','thread','thread.test',${"a".repeat(64)},'command','/repo','lease',0,0,'birth',1,2,'basis',2,${time})`;
  yield* sql`INSERT INTO jones_ordinary_checkout_uses(operation_id,admission_id,checkout_key,lease_id,owner_epoch,source_kind,effect_id,worker_id,attempt_count,claim_expires_at,producer_id,state,revision,last_event_ordinal,started_at,settled_at)
    VALUES('legacy-operation','admission','/repo','lease',0,'outbox','legacy-effect','legacy-worker',1,${time},NULL,'completed',1,1,${time},${time})`;
  yield* sql`INSERT INTO jones_ordinary_checkout_lifecycle_events VALUES('legacy-operation',1,0,${"b".repeat(64)},'retire','{"legacy":"binding"}','{"legacy":"evidence"}',${time})`;
});
const processRow = (operationId: string, effectId: string) => ({
  operation_id: operationId,
  admission_id: "admission",
  checkout_key: "/repo",
  lease_id: "lease",
  owner_epoch: 0,
  source_kind: "outbox_process_v2",
  effect_id: effectId,
  worker_id: "process-worker",
  attempt_count: 1,
  command_id: "command",
  thread_id: "thread",
  run_id: null,
  claimed_at: time,
  request_digest: "c".repeat(64),
  state: "reserved",
  revision: 0,
  last_event_ordinal: 0,
  started_at: null,
  settled_at: null,
});

it.effect(
  "populated upgrade preserves historical rows and rejects collisions including terminal history in both directions",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runJonesMigrations(beforeUpgrade);
      yield* seedLegacyHistory;
      const oldUses = yield* sql`SELECT * FROM jones_ordinary_checkout_uses`;
      const oldEvents = yield* sql`SELECT * FROM jones_ordinary_checkout_lifecycle_events`;
      const oldLedger = yield* sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`;
      yield* runJonesMigrations([
        ...beforeUpgrade,
        [148, "OrdinaryCheckoutProcessCustody", upgrade],
      ]);
      assert.deepEqual(yield* sql`SELECT * FROM jones_ordinary_checkout_uses`, oldUses);
      assert.deepEqual(
        yield* sql`SELECT * FROM jones_ordinary_checkout_lifecycle_events`,
        oldEvents,
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM jones_sql_migrations WHERE migration_id<148 ORDER BY migration_id`,
        oldLedger,
      );
      for (const row of [
        processRow("legacy-operation", "different-effect"),
        processRow("different-operation", "legacy-effect"),
      ])
        yield* sql`INSERT INTO jones_ordinary_checkout_process_uses ${sql.insert(row)}`.pipe(
          Effect.flip,
        );
      yield* sql`INSERT INTO jones_ordinary_checkout_process_uses ${sql.insert(processRow("process-operation", "process-effect"))}`;
      for (const [id, effect] of [
        ["process-operation", "other-effect"],
        ["other-operation", "process-effect"],
      ])
        yield* sql`INSERT INTO jones_ordinary_checkout_uses(operation_id,admission_id,checkout_key,lease_id,owner_epoch,source_kind,effect_id,worker_id,attempt_count,claim_expires_at,producer_id,state,revision,last_event_ordinal,started_at,settled_at)
        VALUES(${id},'admission','/repo','lease',0,'outbox',${effect},'old-worker',1,${time},NULL,'reserved',0,0,NULL,NULL)`.pipe(
          Effect.flip,
        );
      assert.deepEqual(
        yield* sql`SELECT store,operation_id FROM jones_ordinary_checkout_all_uses ORDER BY store`,
        [
          { store: "legacy", operation_id: "legacy-operation" },
          { store: "process_v2", operation_id: "process-operation" },
        ],
      );
      // Neither physical journal can manufacture a predecessor or owner witness for the other store.
      for (const [table, id] of [
        ["jones_ordinary_checkout_lifecycle_events", "process-operation"],
        ["jones_ordinary_checkout_process_events", "legacy-operation"],
      ])
        yield* sql`INSERT INTO ${sql(table!)} VALUES(${id},2,1,${"d".repeat(64)},'release','{}','{}',${time})`.pipe(
          Effect.flip,
        );
      for (const mutation of [
        sql`UPDATE jones_ordinary_checkout_process_uses SET command_id='foreign',revision=revision+1`,
        sql`INSERT OR REPLACE INTO jones_ordinary_checkout_process_uses SELECT * FROM jones_ordinary_checkout_process_uses`,
        sql`DELETE FROM jones_ordinary_checkout_process_uses`,
        sql`UPDATE jones_ordinary_checkout_owners SET state='released',revision=revision+1`,
        sql`UPDATE jones_ordinary_checkout_owners SET branch='foreign',target_revision=target_revision+1,revision=revision+1`,
        sql`UPDATE jones_ordinary_checkout_owners SET owner_epoch=owner_epoch+1,lease_id='foreign',revision=revision+1`,
      ])
        yield* mutation.pipe(Effect.flip);
      assert.deepEqual(yield* sql`SELECT * FROM jones_ordinary_checkout_uses`, oldUses);
      assert.deepEqual(
        yield* sql`SELECT * FROM jones_ordinary_checkout_lifecycle_events`,
        oldEvents,
      );
      yield* runJonesMigrations([
        ...beforeUpgrade,
        [148, "OrdinaryCheckoutProcessCustody", upgrade],
      ]);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "a conflicting receiving migration allocation is refused before process schema writes",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runJonesMigrations(beforeUpgrade);
      yield* sql`INSERT INTO jones_sql_migrations(migration_id,name) VALUES(148,'UnexpectedReceivingMigration')`;
      yield* runJonesMigrations(jonesMigrationEntries).pipe(Effect.flip);
      assert.deepEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE name='jones_ordinary_checkout_process_uses'`,
        [],
      );
      assert.strictEqual(
        (yield* sql<{
          readonly sql: string;
        }>`SELECT sql FROM sqlite_master WHERE name='jones_ordinary_checkout_owner_identity'`)[0]!
          .sql,
        releasedOwnerTrigger,
      );
    }).pipe(Effect.provide(memory)),
);
