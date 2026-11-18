// @effect-diagnostics-next-line nodeBuiltinImport:off -- The infrastructure adapter compares the actual SQLite main filename with the retained native directory using the existing context-free host-coordinate contract.
import * as NodePath from "node:path";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Journal from "./Journal.ts";
import * as JournalSchema from "./JournalSchema.ts";
import * as Inventory from "./EnrollmentInventory.ts";
import * as Physical from "./PhysicalScope.ts";
import {
  AdmissionError,
  canonical,
  digest,
  failure,
  Participant,
  ScopeObservation,
  storageError,
  type ParticipantIdentity,
  type PhysicalScope,
  type ReservationBinding,
  Reservation as ReservationSchema,
  Slot as SlotSchema,
  PhysicalScope as PhysicalScopeSchema,
} from "./Protocol.ts";

export const EnrollmentSeal = Schema.Struct({
  enrollmentId: Schema.NonEmptyString,
  storeId: Schema.NonEmptyString,
  journalId: Schema.NonEmptyString,
  participantNonce: Schema.NonEmptyString,
  inventoryDigest: Schema.NonEmptyString,
  itemCount: Schema.Int,
  schemaContractDigest: Schema.NonEmptyString,
  coverageContract: Schema.NonEmptyString,
  sourceSnapshotDigest: Schema.NonEmptyString,
});
export type EnrollmentSeal = typeof EnrollmentSeal.Type;
const decodeOwningSlot = Schema.decodeUnknownEffect(Schema.fromJsonString(SlotSchema));
const decodeStoredReservation = Schema.decodeUnknownEffect(
  Schema.fromJsonString(ReservationSchema),
);
const decodeImportedEntries = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Array(Inventory.InventoryEntry)),
);
const decodePublishedSeal = Schema.decodeUnknownEffect(Schema.fromJsonString(EnrollmentSeal));

export type EnrollmentProgress = {
  readonly enrollmentId: string;
  readonly phase: "draining" | "staging" | "published";
};
const infrastructureBrand: unique symbol = Symbol("InfrastructurePermit");
export type InfrastructurePermit = { readonly [infrastructureBrand]: true };
const permits = new WeakMap<
  InfrastructurePermit,
  {
    scope: PhysicalScope;
    journalId: string;
    alive: Effect.Effect<void, AdmissionError>;
    check: Effect.Effect<void, AdmissionError>;
  }
>();
export const validateInfrastructurePermit = Effect.fn(
  "EnrollmentJournal.validateInfrastructurePermit",
)(function* (permit: InfrastructurePermit, configuredScope: PhysicalScope) {
  const held = permits.get(permit);
  if (held === undefined || canonical(held.scope) !== canonical(configuredScope))
    return yield* failure("infrastructure_permit_not_bound");
  yield* held.check;
});

/** Receives the actual checked local seal reader from Enrollment, never a decoded JSON issuer. */
export class LocalEnrollment extends Context.Service<
  LocalEnrollment,
  {
    readonly assertClosed: Effect.Effect<void, AdmissionError>;
    readonly verifySeal: (seal: EnrollmentSeal) => Effect.Effect<void, AdmissionError>;
    readonly validateTerminalReconciliation: (
      before: Inventory.InventoryEntry,
      terminal: Inventory.InventoryEntry,
    ) => Effect.Effect<void, AdmissionError>;
  }
>()("t3/jones/cleanup/sharedAdmission/EnrollmentJournal/LocalEnrollment") {}
class EnrollmentJournal extends Context.Service<
  EnrollmentJournal,
  {
    readonly beginStore: (
      identity: ParticipantIdentity,
      id: string,
      expectedPreviousNonce?: string | null,
    ) => Effect.Effect<EnrollmentProgress, AdmissionError>;
    readonly importBatch: (
      id: string,
      entries: readonly Inventory.InventoryEntry[],
    ) => Effect.Effect<void, AdmissionError>;
    readonly publish: (id: string, seal: EnrollmentSeal) => Effect.Effect<void, AdmissionError>;
    readonly reconcileTerminal: (
      itemKey: string,
      expectedRevision: number,
      terminal: Inventory.InventoryEntry,
    ) => Effect.Effect<void, AdmissionError>;
  }
>()("t3/jones/cleanup/sharedAdmission/EnrollmentJournal") {}

const decodeScope = Schema.decodeUnknownEffect(Schema.fromJsonString(PhysicalScopeSchema));
const decodeEntryJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Inventory.InventoryEntry));
const destructive = (binding: ReservationBinding) =>
  binding.kind === "removal" || binding.kind === "recreation" || binding.kind === "mutation";

/** Runs only inside the caller's existing shared writer transaction: no local SQL or FS here. */
export const assertAdmission = Effect.fn("EnrollmentJournal.assertAdmission")(function* (
  sql: SqlClient.SqlClient,
  binding: ReservationBinding,
) {
  const ready = yield* sql<{ nonce: string; phase: string }>`SELECT c.nonce,e.phase
    FROM jones_common_store_current c JOIN jones_common_enrollments e ON e.nonce=c.nonce
    WHERE c.store_id=${binding.participant.storeId}`;
  if (ready[0]?.nonce !== binding.participant.nonce || ready[0]?.phase !== "published")
    return yield* failure("participant_enrollment_not_ready");
  if (destructive(binding)) {
    const incomplete =
      yield* sql`SELECT enrollment_id FROM jones_common_enrollments WHERE phase NOT IN ('published','superseded') LIMIT 1`;
    if (incomplete.length !== 0) return yield* failure("enrollment_destructive_barrier");
  }
  const infrastructure = yield* sql<{
    scope_json: string;
    scope_sha256: string;
  }>`SELECT scope_json,scope_sha256 FROM jones_common_infrastructure`;
  for (const row of infrastructure) {
    const scope = yield* decodeScope(row.scope_json).pipe(Effect.mapError(storageError));
    if (digest(scope) !== row.scope_sha256 || canonical(scope) !== row.scope_json)
      return yield* failure("infrastructure_record_corrupt");
    if (binding.scopes.some((candidate) => Physical.overlaps(candidate, scope)))
      return yield* failure("infrastructure_scope_conflict");
  }
  let owningSlot: typeof SlotSchema.Type | null = null;
  if (binding.slot !== null) {
    const slots = yield* sql<{
      record_json: string;
      record_sha256: string;
    }>`SELECT record_json,record_sha256 FROM jones_common_slots WHERE slot_id=${binding.slot.slotId}`;
    if (slots[0] !== undefined) {
      owningSlot = yield* decodeOwningSlot(slots[0].record_json).pipe(
        Effect.mapError(storageError),
      );
      if (
        digest(owningSlot) !== slots[0].record_sha256 ||
        canonical(owningSlot) !== slots[0].record_json
      )
        return yield* failure("enrollment_owning_slot_corrupt");
    }
  }
  const rows = yield* sql<{
    entry_json: string;
    entry_sha256: string;
  }>`SELECT entry_json,entry_sha256 FROM jones_common_enrollment_items`;
  for (const row of rows) {
    const entry = yield* decodeEntryJson(row.entry_json).pipe(Effect.mapError(storageError));
    if (canonical(entry) !== row.entry_json || digest(entry) !== row.entry_sha256)
      return yield* failure("enrollment_item_corrupt");
    if (entry.disposition === "validated_terminal") continue;
    if (entry.scopes.length === 0) {
      if (destructive(binding)) return yield* failure("enrollment_unlocatable_unknown");
      continue;
    }
    if (!binding.scopes.some((a) => entry.scopes.some((b) => Physical.overlaps(a, b)))) continue;
    const owningReference =
      entry.disposition === "reference" &&
      binding.slot !== null &&
      owningSlot !== null &&
      owningSlot.slotId === binding.slot.slotId &&
      entry.owner !== null &&
      canonical(entry.owner) === canonical(binding.slot.owner) &&
      canonical(entry.owner) === canonical(owningSlot.owner) &&
      canonical(binding.owner) === canonical(owningSlot.owner) &&
      entry.scopes.length === 1 &&
      canonical(entry.scopes[0]) === canonical(owningSlot.scope);
    if (
      (destructive(binding) && !owningReference) ||
      (!destructive(binding) && entry.disposition !== "reference")
    )
      return yield* failure("enrollment_history_conflict");
  }
});

const writer = Effect.fn("EnrollmentJournal.writer")(function* <A, E, R>(
  connection: Journal.JournalConnection["Service"],
  effect: Effect.Effect<A, E, R>,
): Effect.fn.Return<A, AdmissionError, R> {
  if (Option.isSome(yield* Effect.serviceOption(connection.sql.transactionService)))
    return yield* failure("enrollment_inside_shared_transaction");
  yield* connection.revalidate;
  const sql = connection.sql;
  return yield* sql
    .withTransaction(
      Effect.gen(function* () {
        yield* sql`UPDATE jones_common_admission_schema SET version=version WHERE singleton=1`;
        const rows = yield* sql<{
          binding_json: string;
        }>`SELECT binding_json FROM jones_common_admission_schema WHERE singleton=1`;
        const sync = yield* sql<{ synchronous: number }>`PRAGMA synchronous`;
        if (
          rows[0]?.binding_json !== canonical(connection.binding) ||
          (sync[0]?.synchronous ?? 0) < 2
        )
          return yield* failure("enrollment_journal_binding_or_durability");
        return yield* effect;
      }),
    )
    .pipe(Effect.mapError(storageError));
});

/** Bootstrap composition only; the returned permit protects infrastructure and grants no work authority. */
export const makeInfrastructure = Effect.gen(function* () {
  const connection = yield* Journal.JournalConnection;
  const lifetime = yield* Effect.scope;
  const observations = Context.get(yield* Layer.build(Physical.layer), ScopeObservation);
  const alive = Effect.gen(function* () {
    const ambient = yield* Effect.serviceOption(SqlClient.SqlClient);
    const local = yield* Effect.serviceOption(Participant);
    if (
      (Option.isSome(ambient) &&
        Option.isSome(yield* Effect.serviceOption(ambient.value.transactionService))) ||
      (Option.isSome(local) &&
        Option.isSome(yield* Effect.serviceOption(local.value.sql.transactionService)))
    )
      return yield* failure("infrastructure_inside_local_transaction");
    if (lifetime.state._tag === "Closed") return yield* failure("bootstrap_scope_closed");
    yield* connection.revalidate;
  });
  const issue = (scope: PhysicalScope): InfrastructurePermit => {
    const permit: InfrastructurePermit = Object.freeze({ [infrastructureBrand]: true as const });
    permits.set(permit, {
      scope,
      journalId: connection.binding.journalId,
      alive,
      check: Effect.gen(function* () {
        yield* alive;
        yield* observations.revalidate(scope);
      }),
    });
    return permit;
  };
  return {
    bindStoreInfrastructure: Effect.fn("EnrollmentJournal.bindStoreInfrastructure")(function* (
      permit: InfrastructurePermit,
    ) {
      const participant = yield* Participant;
      const held = permits.get(permit);
      if (
        held === undefined ||
        held.journalId !== connection.binding.journalId ||
        held.scope.unresolved.length !== 0
      )
        return yield* failure("infrastructure_permit_not_bound");
      if (
        participant.sql === connection.sql ||
        Option.isSome(yield* Effect.serviceOption(participant.sql.transactionService))
      )
        return yield* failure("infrastructure_binding_inside_local_transaction");
      yield* held.check;
      yield* participant.checkOutside;
      const databases = yield* participant.sql<{
        name: string;
        file: string;
      }>`PRAGMA database_list`.pipe(Effect.mapError(storageError));
      const main = databases.filter((database) => database.name === "main");
      const file = main[0]?.file;
      if (
        main.length !== 1 ||
        file === undefined ||
        !NodePath.isAbsolute(file) ||
        NodePath.dirname(file) !== held.scope.canonicalPath
      )
        return yield* failure("infrastructure_actual_database_path_mismatch");
      const sql = connection.sql;
      yield* writer(
        connection,
        Effect.gen(function* () {
          const rows = yield* sql<{
            database_json: string;
            scope_key: string;
            database_path: string;
          }>`SELECT database_json,scope_key,database_path FROM jones_common_store_infrastructure WHERE store_id=${participant.identity.storeId}`;
          if (
            rows[0] !== undefined &&
            (rows[0].database_json !== canonical(participant.identity.database) ||
              rows[0].scope_key !== digest(held.scope) ||
              rows[0].database_path !== file)
          )
            return yield* failure("store_infrastructure_binding_changed");
          yield* sql`INSERT OR IGNORE INTO jones_common_store_infrastructure VALUES (${participant.identity.storeId},${canonical(participant.identity.database)},${digest(held.scope)},${file})`;
        }),
      );
    }),
    observeCreatedInfrastructure: Effect.fn("EnrollmentJournal.observeCreatedInfrastructure")(
      function* (permit: InfrastructurePermit) {
        const held = permits.get(permit);
        if (held === undefined || held.journalId !== connection.binding.journalId)
          return yield* failure("infrastructure_permit_not_bound");
        yield* held.alive;
        const created = yield* observations.capture(held.scope.requestedPath);
        if (
          created.canonicalPath !== held.scope.canonicalPath ||
          created.unresolved.length !== 0 ||
          !held.scope.ancestors.every((old) =>
            created.ancestors.some((now) => canonical(old) === canonical(now)),
          )
        )
          return yield* failure("infrastructure_created_scope_changed");
        const sql = connection.sql;
        yield* writer(
          connection,
          Effect.gen(function* () {
            const before = yield* sql<{
              scope_json: string;
            }>`SELECT scope_json FROM jones_common_infrastructure WHERE scope_key=${digest(held.scope)}`;
            if (before[0]?.scope_json !== canonical(held.scope))
              return yield* failure("infrastructure_scope_cas");
            yield* sql`INSERT OR IGNORE INTO jones_common_infrastructure VALUES (${digest(created)},${canonical(created)},${digest(created)})`;
            if (digest(created) !== digest(held.scope))
              yield* sql`DELETE FROM jones_common_infrastructure WHERE scope_key=${digest(held.scope)}`;
          }),
        );
        permits.delete(permit);
        return { permit: issue(created), scope: created };
      },
    ),
    beginInfrastructure: Effect.fn("EnrollmentJournal.beginInfrastructure")(function* (
      scope: PhysicalScope,
    ) {
      if (lifetime.state._tag === "Closed") return yield* failure("bootstrap_scope_closed");
      const ambient = yield* Effect.serviceOption(SqlClient.SqlClient);
      if (
        Option.isSome(ambient) &&
        Option.isSome(yield* Effect.serviceOption(ambient.value.transactionService))
      )
        return yield* failure("infrastructure_inside_local_transaction");
      yield* observations.revalidate(scope);
      const sql = connection.sql;
      yield* writer(
        connection,
        Effect.gen(function* () {
          const retained = yield* sql<{
            scope_key: string;
            scope_json: string;
            scope_sha256: string;
          }>`SELECT scope_key,scope_json,scope_sha256 FROM jones_common_infrastructure`;
          for (const row of retained) {
            const previous = yield* decodeScope(row.scope_json).pipe(Effect.mapError(storageError));
            if (
              canonical(previous) !== row.scope_json ||
              digest(previous) !== row.scope_sha256 ||
              row.scope_key !== row.scope_sha256
            )
              return yield* failure("infrastructure_record_corrupt");
            if (canonical(previous) === canonical(scope)) return;
            // Restart may have lost its private permit after creating the protected
            // directory. The retained wider exclusion still proves this narrowing;
            // no timeout, absent-row inference or arbitrary JSON issues the permit.
            if (
              previous.canonicalPath === scope.canonicalPath &&
              previous.requestedPath === scope.requestedPath &&
              previous.unresolved.length !== 0 &&
              scope.unresolved.length === 0 &&
              previous.ancestors.every((old) =>
                scope.ancestors.some((now) => canonical(old) === canonical(now)),
              )
            ) {
              yield* sql`INSERT OR IGNORE INTO jones_common_infrastructure VALUES (${digest(scope)},${canonical(scope)},${digest(scope)})`;
              yield* sql`DELETE FROM jones_common_infrastructure WHERE scope_key=${row.scope_key}`;
              return;
            }
          }
          const reservations = yield* sql<{
            record_json: string;
            record_sha256: string;
          }>`SELECT record_json,record_sha256 FROM jones_common_reservations WHERE phase <> 'terminal'`;
          for (const row of reservations) {
            const reservation = yield* decodeStoredReservation(row.record_json).pipe(
              Effect.mapError(storageError),
            );
            if (
              digest(reservation) !== row.record_sha256 ||
              canonical(reservation) !== row.record_json ||
              digest(reservation.binding) !== reservation.bindingSha256
            )
              return yield* failure("infrastructure_existing_reservation_corrupt");
            if (reservation.binding.scopes.some((other) => Physical.overlaps(other, scope)))
              return yield* failure("infrastructure_existing_writer_conflict");
          }
          const items = yield* sql<{
            entry_json: string;
            entry_sha256: string;
          }>`SELECT entry_json,entry_sha256 FROM jones_common_enrollment_items`;
          for (const row of items) {
            const entry = yield* decodeEntryJson(row.entry_json).pipe(
              Effect.mapError(storageError),
            );
            if (digest(entry) !== row.entry_sha256 || canonical(entry) !== row.entry_json)
              return yield* failure("infrastructure_existing_history_corrupt");
            if (
              entry.disposition !== "validated_terminal" &&
              (entry.scopes.length === 0 ||
                entry.scopes.some((other) => Physical.overlaps(other, scope)))
            )
              return yield* failure("infrastructure_existing_history_conflict");
          }
          yield* sql`INSERT OR IGNORE INTO jones_common_infrastructure VALUES (${digest(scope)},${canonical(scope)},${digest(scope)})`;
        }),
      );
      return issue(scope);
    }),
  };
});

const decodeReservationJson = decodeStoredReservation;
export const make = Effect.gen(function* () {
  const connection = yield* Journal.JournalConnection;
  const participant = yield* Participant;
  const local = yield* LocalEnrollment;
  const lifetime = yield* Effect.scope;
  const sql = connection.sql;
  if (sql === participant.sql) return yield* failure("enrollment_common_is_local_sql");
  const active = new Set<string>();
  const outside = Effect.gen(function* () {
    if (lifetime.state._tag === "Closed") return yield* failure("enrollment_scope_closed");
    for (const client of [sql, participant.sql])
      if (Option.isSome(yield* Effect.serviceOption(client.transactionService)))
        return yield* failure("enrollment_cross_sql_transaction");
    yield* participant.checkOutside;
    yield* local.assertClosed;
  });
  const draining = Effect.gen(function* () {
    const rows = yield* sql<{
      record_json: string;
    }>`SELECT record_json FROM jones_common_reservations WHERE phase IN ('entered','unknown')`;
    for (const row of rows) {
      const record = yield* decodeReservationJson(row.record_json).pipe(
        Effect.mapError(storageError),
      );
      if (destructive(record.binding)) return true;
    }
    return false;
  });
  const current = Effect.fn("EnrollmentJournal.current")(function* (id: string) {
    if (!active.has(id)) return yield* failure("enrollment_no_live_staging_operation");
    const rows = yield* sql<{
      identity_json: string;
      phase: string;
      revision: number;
      seal_json: string | null;
    }>`
      SELECT identity_json,phase,revision,seal_json FROM jones_common_enrollments WHERE enrollment_id=${id}`;
    const head = yield* sql<{
      nonce: string;
    }>`SELECT nonce FROM jones_common_store_current WHERE store_id=${participant.identity.storeId}`;
    if (
      rows.length !== 1 ||
      rows[0]?.identity_json !== canonical(participant.identity) ||
      head[0]?.nonce !== participant.identity.nonce
    )
      return yield* failure("enrollment_stale_incarnation");
    return rows[0]!;
  });
  return EnrollmentJournal.of({
    beginStore: Effect.fn("EnrollmentJournal.beginStore")(function* (
      identity,
      id,
      expectedPreviousNonce = null,
    ) {
      yield* outside;
      if (canonical(identity) !== canonical(participant.identity) || !id)
        return yield* failure("enrollment_identity_mismatch");
      const progress = yield* writer(
        connection,
        Effect.gen(function* () {
          const infrastructure = yield* sql<{
            database_json: string;
          }>`SELECT b.database_json FROM jones_common_store_infrastructure b
          JOIN jones_common_infrastructure i ON i.scope_key=b.scope_key WHERE b.store_id=${identity.storeId}`;
          if (infrastructure[0]?.database_json !== canonical(identity.database))
            return yield* failure("store_infrastructure_not_bound");
          const stored = yield* sql<{
            database_json: string;
          }>`SELECT database_json FROM jones_common_participants WHERE store_id=${identity.storeId}`;
          if (stored[0] !== undefined && stored[0].database_json !== canonical(identity.database))
            return yield* failure("store_uuid_physical_database_collision");
          const duplicate = yield* sql<{
            store_id: string;
          }>`SELECT store_id FROM jones_common_participants WHERE database_json=${canonical(identity.database)} AND store_id <> ${identity.storeId}`;
          if (duplicate.length !== 0) return yield* failure("physical_database_multiple_store_ids");
          const old = yield* sql<{
            nonce: string;
          }>`SELECT nonce FROM jones_common_store_current WHERE store_id=${identity.storeId}`;
          const existing = yield* sql<{
            identity_json: string;
            phase: EnrollmentProgress["phase"];
          }>`SELECT identity_json,phase FROM jones_common_enrollments WHERE enrollment_id=${id}`;
          if (existing[0] !== undefined) {
            if (
              existing[0].identity_json !== canonical(identity) ||
              old[0]?.nonce !== identity.nonce ||
              !active.has(id)
            )
              return yield* failure("enrollment_id_replay_or_collision");
            return { enrollmentId: id, phase: existing[0].phase };
          }
          if (
            (old[0]?.nonce ?? null) !== expectedPreviousNonce ||
            expectedPreviousNonce === identity.nonce
          )
            return yield* failure("enrollment_nonce_cas");
          const nonces =
            yield* sql`SELECT nonce FROM jones_common_incarnations WHERE nonce=${identity.nonce}`;
          // A legacy descriptive CommonJournal.enroll may have registered this exact identity.
          const legacy = yield* sql<{
            identity_json: string;
          }>`SELECT identity_json FROM jones_common_incarnations WHERE nonce=${identity.nonce}`;
          if (nonces.length !== 0 && legacy[0]?.identity_json !== canonical(identity))
            return yield* failure("participant_nonce_collision");
          const phase = (yield* draining) ? ("draining" as const) : ("staging" as const);
          yield* sql`INSERT OR IGNORE INTO jones_common_participants VALUES (${identity.storeId},${canonical(identity.database)})`;
          yield* sql`INSERT OR IGNORE INTO jones_common_incarnations VALUES (${identity.nonce},${canonical(identity)})`;
          yield* sql`INSERT INTO jones_common_enrollments VALUES (${id},${identity.storeId},${identity.nonce},${canonical(identity)},${phase},0,NULL)`;
          yield* sql`INSERT INTO jones_common_store_current VALUES (${identity.storeId},${identity.nonce}) ON CONFLICT(store_id) DO UPDATE SET nonce=excluded.nonce`;
          return { enrollmentId: id, phase };
        }),
      );
      active.add(id);
      return progress;
    }),
    importBatch: Effect.fn("EnrollmentJournal.importBatch")(function* (id, entries) {
      yield* outside;
      if (entries.length > 256) return yield* failure("enrollment_batch_too_large");
      // Decode/copy before acquiring writer lock so caller mutations cannot alter a staged batch.
      const copied = yield* decodeImportedEntries(canonical(entries)).pipe(
        Effect.mapError(storageError),
      );
      yield* Inventory.validateEntries(participant.identity.storeId, copied);
      yield* writer(
        connection,
        Effect.gen(function* () {
          const row = yield* current(id);
          if (row.phase === "published") return yield* failure("enrollment_already_published");
          if (yield* draining) return yield* failure("enrollment_draining_entered_destructive");
          for (const entry of copied) {
            const old = yield* sql<{
              entry_json: string;
            }>`SELECT entry_json FROM jones_common_enrollment_items WHERE store_id=${participant.identity.storeId} AND item_key=${entry.itemKey}`;
            if (old[0] !== undefined && old[0].entry_json !== canonical(entry))
              return yield* failure("enrollment_item_revision_collision");
            yield* sql`INSERT OR IGNORE INTO jones_common_enrollment_items (store_id,item_key,entry_json,entry_sha256) VALUES (${participant.identity.storeId},${entry.itemKey},${canonical(entry)},${digest(entry)})`;
            yield* sql`INSERT OR IGNORE INTO jones_common_enrollment_manifest VALUES (${id},${entry.itemKey},${digest(entry)})`;
          }
          yield* sql`UPDATE jones_common_enrollments SET phase='staging',revision=revision+1 WHERE enrollment_id=${id}`;
        }),
      );
    }),
    reconcileTerminal: Effect.fn("EnrollmentJournal.reconcileTerminal")(
      function* (key, expectedRevision, rawTerminal) {
        yield* outside;
        const terminal = yield* Inventory.decodeEntry(rawTerminal).pipe(
          Effect.mapError(storageError),
        );
        yield* Inventory.validateEntries(participant.identity.storeId, [terminal]);
        if (terminal.disposition !== "validated_terminal" || terminal.itemKey !== key)
          return yield* failure("enrollment_terminal_reconciliation_binding");
        const beforeRows = yield* writer(
          connection,
          Effect.gen(function* () {
            const head = yield* sql<{
              nonce: string;
            }>`SELECT nonce FROM jones_common_store_current WHERE store_id=${participant.identity.storeId}`;
            if (active.size === 0 || head[0]?.nonce !== participant.identity.nonce)
              return yield* failure("enrollment_no_current_reconciliation_operation");
            return yield* sql<{ entry_json: string; reconciliation_revision: number }>`
          SELECT entry_json,reconciliation_revision FROM jones_common_enrollment_items WHERE store_id=${participant.identity.storeId} AND item_key=${key}`;
          }),
        );
        const beforeRow = beforeRows[0];
        if (beforeRow === undefined || beforeRow.reconciliation_revision !== expectedRevision)
          return yield* failure("enrollment_reconciliation_revision");
        const before = yield* decodeEntryJson(beforeRow.entry_json).pipe(
          Effect.mapError(storageError),
        );
        if (
          canonical(before.source) !== canonical(terminal.source) ||
          canonical(before.scopes) !== canonical(terminal.scopes) ||
          canonical(before.owner) !== canonical(terminal.owner)
        )
          return yield* failure("enrollment_reconciliation_source_changed");
        yield* participant.sql
          .withTransaction(
            Effect.gen(function* () {
              yield* participant.validateLocal;
              yield* local.validateTerminalReconciliation(before, terminal);
            }),
          )
          .pipe(Effect.mapError(storageError));
        yield* outside;
        yield* writer(
          connection,
          Effect.gen(function* () {
            const rows = yield* sql<{
              entry_json: string;
              reconciliation_revision: number;
            }>`SELECT entry_json,reconciliation_revision FROM jones_common_enrollment_items WHERE store_id=${participant.identity.storeId} AND item_key=${key}`;
            if (
              rows[0]?.entry_json !== beforeRow.entry_json ||
              rows[0]?.reconciliation_revision !== expectedRevision
            )
              return yield* failure("enrollment_reconciliation_cas");
            yield* sql`UPDATE jones_common_enrollment_items SET entry_json=${canonical(terminal)},entry_sha256=${digest(terminal)},reconciliation_revision=reconciliation_revision+1
          WHERE store_id=${participant.identity.storeId} AND item_key=${key} AND reconciliation_revision=${expectedRevision}`;
          }),
        );
      },
    ),
    publish: Effect.fn("EnrollmentJournal.publish")(function* (id, rawSeal) {
      yield* outside;
      const seal = yield* decodePublishedSeal(canonical(rawSeal)).pipe(
        Effect.mapError(storageError),
      );
      if (
        !active.has(id) ||
        seal.enrollmentId !== id ||
        seal.storeId !== participant.identity.storeId ||
        seal.participantNonce !== participant.identity.nonce ||
        seal.journalId !== connection.binding.journalId ||
        seal.schemaContractDigest !== JournalSchema.contractDigest ||
        seal.itemCount < 0 ||
        seal.coverageContract !== participant.identity.coverage
      )
        return yield* failure("enrollment_seal_binding");
      // This is the private operation's actual local seal/source read, outside shared SQL.
      yield* local.verifySeal(seal);
      yield* outside;
      yield* writer(
        connection,
        Effect.gen(function* () {
          const row = yield* current(id);
          if (row.phase === "published") {
            if (row.seal_json !== canonical(seal))
              return yield* failure("enrollment_published_seal_collision");
            return;
          }
          if (yield* draining) return yield* failure("enrollment_draining_entered_destructive");
          const rows = yield* sql<{
            item_key: string;
            entry_sha256: string;
            entry_json: string;
            actual_sha256: string;
          }>`
          SELECT m.item_key,m.entry_sha256,i.entry_json,i.entry_sha256 AS actual_sha256
          FROM jones_common_enrollment_manifest m LEFT JOIN jones_common_enrollment_items i
          ON i.store_id=${participant.identity.storeId} AND i.item_key=m.item_key WHERE m.enrollment_id=${id}`;
          const manifest = [];
          for (const entry of rows) {
            const decoded = yield* decodeEntryJson(entry.entry_json).pipe(
              Effect.mapError(storageError),
            );
            if (
              entry.entry_sha256 !== entry.actual_sha256 ||
              digest(decoded) !== entry.entry_sha256 ||
              decoded.itemKey !== entry.item_key
            )
              return yield* failure("enrollment_manifest_item_changed");
            manifest.push({ itemKey: entry.item_key, sha256: entry.entry_sha256 });
          }
          manifest.sort((a, b) => (a.itemKey < b.itemKey ? -1 : a.itemKey > b.itemKey ? 1 : 0));
          if (manifest.length !== seal.itemCount || digest(manifest) !== seal.inventoryDigest)
            return yield* failure("enrollment_manifest_incomplete");
          yield* sql`UPDATE jones_common_enrollments SET phase='superseded',revision=revision+1 WHERE store_id=${participant.identity.storeId} AND enrollment_id <> ${id} AND phase <> 'published'`;
          yield* sql`UPDATE jones_common_enrollments SET phase='published',seal_json=${canonical(seal)},revision=revision+1 WHERE enrollment_id=${id} AND revision=${row.revision}`;
        }),
      );
    }),
  });
});
