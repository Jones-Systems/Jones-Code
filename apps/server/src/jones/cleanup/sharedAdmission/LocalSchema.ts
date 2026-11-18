// The synchronous Effect UUID constructor keeps the existing factory context and cryptographic UUIDv4 contract.
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Incarnation from "./DatabaseIncarnation.ts";
import * as Inventory from "./EnrollmentInventory.ts";
import { EnrollmentSeal } from "./EnrollmentJournal.ts";
import { AdmissionError, canonical, failure, storageError } from "./Protocol.ts";
const decodeEnrollmentSeal = Schema.decodeUnknownEffect(EnrollmentSeal);
const decodeManifestEntries = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Array(Inventory.InventoryEntry)),
);

/** Root allocates this effect in the Jones loader after reconciling receiving main. */
export const migration = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS jones_common_admission_installation (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1), store_id TEXT NOT NULL,
    database_json TEXT NOT NULL, journal_id TEXT NOT NULL, journal_binding_json TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS jones_common_admission_bridge (
    operation_key TEXT PRIMARY KEY, certificate_json TEXT NOT NULL,
    certificate_sha256 TEXT NOT NULL, terminal_json TEXT, terminal_sha256 TEXT)`;
  yield* sql`CREATE TABLE IF NOT EXISTS jones_common_recreation_bridge (
    operation_key TEXT PRIMARY KEY, certificate_json TEXT NOT NULL, certificate_sha256 TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS jones_common_store_identity (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1), store_id TEXT NOT NULL UNIQUE,
    format_version INTEGER NOT NULL CHECK(format_version=1), physical_binding_json TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS jones_common_enrollment_seals (
    enrollment_id TEXT PRIMARY KEY, store_id TEXT NOT NULL, journal_id TEXT NOT NULL,
    participant_nonce TEXT NOT NULL, inventory_digest TEXT NOT NULL, item_count INTEGER NOT NULL CHECK(item_count>=0),
    schema_contract_digest TEXT NOT NULL, coverage_contract TEXT NOT NULL, source_snapshot_digest TEXT NOT NULL,
    seal_json TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS jones_common_enrollment_manifest (
    enrollment_id TEXT NOT NULL, item_key TEXT NOT NULL,
    entry_json TEXT NOT NULL, entry_sha256 TEXT NOT NULL, PRIMARY KEY(enrollment_id,item_key))`;
  // A BEFORE INSERT guard also covers REPLACE when recursive_triggers is OFF.
  // Service idempotence reads exact bytes first and never issues a second insert.
  for (const [table, key] of [
    ["jones_common_admission_installation", "singleton=NEW.singleton"],
    ["jones_common_store_identity", "singleton=NEW.singleton OR store_id=NEW.store_id"],
    ["jones_common_enrollment_seals", "enrollment_id=NEW.enrollment_id"],
    [
      "jones_common_enrollment_manifest",
      "enrollment_id=NEW.enrollment_id AND item_key=NEW.item_key",
    ],
  ] as const) {
    yield* sql.unsafe(`CREATE TRIGGER IF NOT EXISTS ${table}_immutable_insert BEFORE INSERT ON ${table}
      WHEN EXISTS(SELECT 1 FROM ${table} WHERE ${key}) BEGIN SELECT RAISE(ABORT,'immutable Jones enrollment row'); END`);
    for (const operation of ["UPDATE", "DELETE"] as const)
      yield* sql.unsafe(`CREATE TRIGGER IF NOT EXISTS ${table}_immutable_${operation.toLowerCase()}
        BEFORE ${operation} ON ${table} BEGIN SELECT RAISE(ABORT,'immutable Jones enrollment row'); END`);
  }
});

const outside = Effect.fn("LocalEnrollmentSchema.outside")(function* (sql: SqlClient.SqlClient) {
  if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
    return yield* failure("local_durability_configuration_inside_transaction");
});
/** Plain SqlClient queries use its app writer, never NodeSqliteClient.readOnly. */
export const assertDurableWriter = Effect.fn("LocalEnrollmentSchema.assertDurableWriter")(
  function* (sql: SqlClient.SqlClient) {
    const sync = yield* sql<{ synchronous: number }>`PRAGMA synchronous`.pipe(
      Effect.mapError(storageError),
    );
    const full = yield* sql<{ fullfsync: number }>`PRAGMA fullfsync`.pipe(
      Effect.mapError(storageError),
    );
    if ((sync[0]?.synchronous ?? 0) < 2 || full[0]?.fullfsync !== 1)
      return yield* failure("local_enrollment_writer_not_durable");
  },
);
export const enableDurableWriter = Effect.fn("LocalEnrollmentSchema.enableDurableWriter")(
  function* (sql: SqlClient.SqlClient) {
    yield* outside(sql);
    yield* sql`PRAGMA synchronous = FULL`.pipe(Effect.mapError(storageError));
    yield* sql`PRAGMA fullfsync = ON`.pipe(Effect.mapError(storageError));
    yield* assertDurableWriter(sql);
  },
);

/** Supplied only by root's actual shared-publication readback, outside both databases. */
export class PublishedEnrollment extends Context.Service<
  PublishedEnrollment,
  {
    readonly assertPublished: Effect.Effect<void, AdmissionError>;
  }
>()("t3/jones/cleanup/sharedAdmission/LocalSchema/PublishedEnrollment") {}

/** Begin before UUID/pin/seal writes; failure leaves FULL in place. No finalizer restores NORMAL. */
export const beginDurableStartup = Effect.fn("LocalEnrollmentSchema.beginDurableStartup")(
  function* (token: Incarnation.CheckedDatabaseOwnerToken) {
    const sql = yield* SqlClient.SqlClient;
    yield* outside(sql);
    yield* Incarnation.validateCheckedOwner(token);
    const binding = yield* Effect.try({
      try: () => Incarnation.bindingForSql(token, sql),
      catch: storageError,
    });
    const sync = yield* sql<{ synchronous: number }>`PRAGMA synchronous`.pipe(
      Effect.mapError(storageError),
    );
    const full = yield* sql<{ fullfsync: number }>`PRAGMA fullfsync`.pipe(
      Effect.mapError(storageError),
    );
    const previousSync = sync[0]?.synchronous;
    const previousFull = full[0]?.fullfsync;
    if (
      previousSync === undefined ||
      ![1, 2, 3].includes(previousSync) ||
      previousFull === undefined ||
      ![0, 1].includes(previousFull)
    )
      return yield* failure("local_writer_durability_unavailable");
    yield* enableDurableWriter(sql);
    let completed = false;
    return {
      binding,
      completePublication: Effect.gen(function* () {
        yield* outside(sql);
        if (completed) return yield* failure("local_publication_already_completed");
        const published = yield* PublishedEnrollment;
        yield* published.assertPublished;
        yield* Incarnation.validateCheckedOwner(token).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        );
        yield* assertDurableWriter(sql);
        // An interrupted/unknown restoration is not a second publication attempt.
        completed = true;
        yield* sql
          .unsafe(`PRAGMA synchronous = ${previousSync}`)
          .pipe(Effect.mapError(storageError));
        yield* sql.unsafe(`PRAGMA fullfsync = ${previousFull}`).pipe(Effect.mapError(storageError));
        const restoredSync = yield* sql<{ synchronous: number }>`PRAGMA synchronous`.pipe(
          Effect.mapError(storageError),
        );
        const restoredFull = yield* sql<{ fullfsync: number }>`PRAGMA fullfsync`.pipe(
          Effect.mapError(storageError),
        );
        if (
          restoredSync[0]?.synchronous !== previousSync ||
          restoredFull[0]?.fullfsync !== previousFull
        )
          return yield* failure("local_writer_durability_restore_mismatch");
      }),
    };
  },
);

/** Outside SQL, using only the already-certified immutable physical binding. */
export const ensureStoreIdentity = Effect.fn("LocalEnrollmentSchema.ensureStoreIdentity")(
  function* (token: Incarnation.CheckedDatabaseOwnerToken) {
    const sql = yield* SqlClient.SqlClient;
    yield* outside(sql);
    yield* Incarnation.validateCheckedOwner(token);
    const binding = yield* Effect.try({
      try: () => Incarnation.bindingForSql(token, sql),
      catch: storageError,
    });
    yield* enableDurableWriter(sql);
    return yield* sql
      .withTransaction(
        Effect.gen(function* () {
          yield* assertDurableWriter(sql);
          const rows = yield* sql<{
            store_id: string;
            format_version: number;
            physical_binding_json: string;
          }>`
      SELECT store_id,format_version,physical_binding_json FROM jones_common_store_identity WHERE singleton=1`;
          if (rows.length !== 0) {
            const row = rows[0]!;
            if (
              rows.length !== 1 ||
              row.format_version !== 1 ||
              row.physical_binding_json !== canonical(binding) ||
              !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(
                row.store_id,
              )
            )
              return yield* failure("immutable_store_physical_binding_mismatch");
            return { storeId: row.store_id, binding };
          }
          const storeId = SecureRandom.uuidV4Unsafe();
          yield* sql`INSERT INTO jones_common_store_identity VALUES (1,${storeId},1,${canonical(binding)})`;
          const actual = yield* sql<{ store_id: string; physical_binding_json: string }>`
      SELECT store_id,physical_binding_json FROM jones_common_store_identity WHERE singleton=1`;
          if (
            actual.length !== 1 ||
            actual[0]?.store_id !== storeId ||
            actual[0]?.physical_binding_json !== canonical(binding)
          )
            return yield* failure("immutable_store_identity_readback_mismatch");
          return { storeId, binding };
        }),
      )
      .pipe(Effect.mapError(storageError));
  },
);

const decodeSeal = Schema.decodeUnknownEffect(Schema.fromJsonString(EnrollmentSeal));
/** SQL-only: root calls inside its existing enrollment transaction, under a closed gate. */
export const readEnrollmentSeal = Effect.fn("LocalEnrollmentSchema.readSeal")(function* (
  id: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{
    seal_json: string;
  }>`SELECT seal_json FROM jones_common_enrollment_seals WHERE enrollment_id=${id}`.pipe(
    Effect.mapError(storageError),
  );
  if (rows.length === 0) return null;
  const seal = yield* decodeSeal(rows[0]!.seal_json).pipe(Effect.mapError(storageError));
  if (canonical(seal) !== rows[0]!.seal_json || seal.enrollmentId !== id)
    return yield* failure("local_enrollment_seal_corrupt");
  return seal;
});
/** SQL-only sparse manifest/seal commit; no account FS, shared SQL or source payload copies. */
export const commitEnrollment = Effect.fn("LocalEnrollmentSchema.commitEnrollment")(function* (
  input: EnrollmentSeal,
  rawEntries: readonly Inventory.InventoryEntry[],
) {
  const sql = yield* SqlClient.SqlClient;
  if (Option.isNone(yield* Effect.serviceOption(sql.transactionService)))
    return yield* failure("local_enrollment_seal_requires_transaction");
  yield* assertDurableWriter(sql);
  const seal = yield* decodeEnrollmentSeal(input).pipe(Effect.mapError(storageError));
  // Snapshot caller data before the first asynchronous write.
  const entries = yield* decodeManifestEntries(canonical(rawEntries)).pipe(
    Effect.mapError(storageError),
  );
  yield* Inventory.validateEntries(seal.storeId, entries);
  if (
    seal.itemCount !== entries.length ||
    seal.inventoryDigest !== Inventory.inventoryDigest(entries)
  )
    return yield* failure("local_enrollment_manifest_mismatch");
  const identity = yield* sql<{
    store_id: string;
  }>`SELECT store_id FROM jones_common_store_identity WHERE singleton=1`.pipe(
    Effect.mapError(storageError),
  );
  const pin = yield* sql<{
    store_id: string;
    journal_id: string;
  }>`SELECT store_id,journal_id FROM jones_common_admission_installation WHERE singleton=1`.pipe(
    Effect.mapError(storageError),
  );
  if (
    identity[0]?.store_id !== seal.storeId ||
    pin[0]?.store_id !== seal.storeId ||
    pin[0]?.journal_id !== seal.journalId
  )
    return yield* failure("local_enrollment_identity_or_pin_mismatch");
  const prior = yield* readEnrollmentSeal(seal.enrollmentId);
  if (prior !== null && canonical(prior) !== canonical(seal))
    return yield* failure("immutable_enrollment_seal_changed");
  for (const entry of entries) {
    const rows = yield* sql<{
      entry_json: string;
      entry_sha256: string;
    }>`SELECT entry_json,entry_sha256 FROM jones_common_enrollment_manifest
      WHERE enrollment_id=${seal.enrollmentId} AND item_key=${entry.itemKey}`.pipe(
      Effect.mapError(storageError),
    );
    if (rows.length !== 0) {
      if (
        rows[0]?.entry_json !== canonical(entry) ||
        rows[0]?.entry_sha256 !== Inventory.orderedManifest([entry])[0]?.sha256
      )
        return yield* failure("immutable_enrollment_manifest_changed");
    } else if (prior !== null) return yield* failure("local_enrollment_manifest_missing");
    else
      yield* sql`INSERT INTO jones_common_enrollment_manifest VALUES
      (${seal.enrollmentId},${entry.itemKey},${canonical(entry)},${Inventory.orderedManifest([entry])[0]!.sha256})`.pipe(
        Effect.mapError(storageError),
      );
  }
  const actual = yield* sql<{
    item_key: string;
    entry_sha256: string;
  }>`SELECT item_key,entry_sha256 FROM jones_common_enrollment_manifest
    WHERE enrollment_id=${seal.enrollmentId} ORDER BY item_key`.pipe(Effect.mapError(storageError));
  if (
    canonical(actual.map((row) => ({ itemKey: row.item_key, sha256: row.entry_sha256 }))) !==
    canonical(Inventory.orderedManifest(entries))
  )
    return yield* failure("local_enrollment_manifest_readback_mismatch");
  if (prior === null)
    yield* sql`INSERT INTO jones_common_enrollment_seals VALUES
    (${seal.enrollmentId},${seal.storeId},${seal.journalId},${seal.participantNonce},${seal.inventoryDigest},${seal.itemCount},
     ${seal.schemaContractDigest},${seal.coverageContract},${seal.sourceSnapshotDigest},${canonical(seal)})`.pipe(
      Effect.mapError(storageError),
    );
  if (canonical(yield* readEnrollmentSeal(seal.enrollmentId)) !== canonical(seal))
    return yield* failure("local_enrollment_seal_readback_mismatch");
});
