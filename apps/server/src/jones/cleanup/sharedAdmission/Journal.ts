// @effect-diagnostics nodeBuiltinImport:off -- Fixed account coordinates are independent of application and environment overrides.
import * as NodePath from "node:path";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import {
  AdmissionError,
  canonical,
  CommonJournal,
  digest,
  failure,
  operationKey,
  Participant,
  PhysicalScope,
  Reservation,
  Slot,
  TerminalEvidence,
  storageError,
  type LocalCertificate,
  type ParticipantIdentity,
  type ReservationBinding,
} from "./Protocol.ts";
import * as JournalSchema from "./JournalSchema.ts";
import * as EnrollmentJournal from "./EnrollmentJournal.ts";
import { overlaps } from "./PhysicalScope.ts";

export const JournalBinding = Schema.Struct({
  journalId: Schema.NonEmptyString,
  schema: Schema.Literal(1),
  uid: Schema.NonEmptyString,
  canonicalPath: Schema.NonEmptyString,
  file: Schema.Struct({
    device: Schema.NonEmptyString,
    inode: Schema.NonEmptyString,
    birth: Schema.NonEmptyString,
  }),
  parent: PhysicalScope,
});
export type JournalBinding = typeof JournalBinding.Type;

/** The host adapter must obtain home/UID from the native account service, not
 * HOME, XDG, T3 home, baseDir or caller configuration. No path override exists. */
export const fixedAccountJournalPath = (account: {
  readonly home: string;
  readonly uid: string;
}): string => {
  if (
    !NodePath.isAbsolute(account.home) ||
    NodePath.normalize(account.home) !== account.home ||
    !account.uid
  )
    throw new AdmissionError({ reason: "native_account_location_unavailable" });
  return NodePath.join(account.home, ".local/state/jones-code/workspace-admission-v1.sqlite");
};

/** NativeJournal constructs this scoped adapter from a previously enrolled immutable
 * local pin. No default layer opens, initializes or enrolls an account journal. */
export class ProductionJournalAdapter extends Context.Service<
  ProductionJournalAdapter,
  {
    readonly connectFixedAccountJournal: Effect.Effect<
      JournalConnection["Service"],
      AdmissionError
    >;
  }
>()("t3/jones/cleanup/sharedAdmission/Journal/ProductionJournalAdapter") {}

/** Tests supply synthetic paths/connections here. Production composition must
 * obtain this value exclusively from ProductionJournalAdapter. */
export class JournalConnection extends Context.Service<
  JournalConnection,
  {
    readonly sql: SqlClient.SqlClient;
    readonly binding: JournalBinding;
    readonly revalidate: Effect.Effect<void, AdmissionError>;
  }
>()("t3/jones/cleanup/sharedAdmission/Journal/JournalConnection") {}

/** Sidecar-only schema/ledger, never the application's Jones migration ledger. */
export const initializeJournal = Effect.gen(function* () {
  const connection = yield* JournalConnection;
  const participant = yield* Participant;
  const sql = connection.sql;
  for (const client of [sql, participant.sql])
    if (Option.isSome(yield* Effect.serviceOption(client.transactionService)))
      return yield* failure("journal_initialization_inside_transaction");
  yield* participant.checkOutside;
  yield* connection.revalidate;
  // FULL is required for a reservation to mean durable-before-local-commit.
  // Configure only this explicitly supplied sidecar connection, outside SQL.
  yield* sql`PRAGMA synchronous = FULL`.pipe(Effect.mapError(storageError));
  yield* sql
    .withTransaction(JournalSchema.create(sql, canonical(connection.binding)))
    .pipe(Effect.mapError(storageError));
});

const decodeReservation = Schema.decodeUnknownEffect(Schema.fromJsonString(Reservation));
const decodeReservationValue = Schema.decodeUnknownEffect(Reservation);
const decodeTerminalEvidence = Schema.decodeUnknownEffect(TerminalEvidence);
const decodeSlot = Schema.decodeUnknownEffect(Schema.fromJsonString(Slot));
type Stored = { record_json: string; record_sha256: string };
const validateStored = Effect.fn("CommonJournal.validateStored")(function* <A, E>(
  row: Stored,
  decode: (json: string) => Effect.Effect<A, E>,
): Effect.fn.Return<A, AdmissionError> {
  const value = yield* decode(row.record_json).pipe(Effect.mapError(storageError));
  if (canonical(value) !== row.record_json || digest(value) !== row.record_sha256)
    return yield* failure("journal_record_corrupt");
  return value;
});

/** @public Persistence authority only; the Protocol owns live execution rights. */
export const layer = Layer.effect(
  CommonJournal,
  Effect.gen(function* () {
    const connection = yield* JournalConnection;
    const participant = yield* Participant;
    const sql = connection.sql;
    if (sql === participant.sql) return yield* failure("common_journal_is_application_database");
    const outside = Effect.gen(function* () {
      for (const client of [sql, participant.sql])
        if (Option.isSome(yield* Effect.serviceOption(client.transactionService)))
          return yield* failure("journal_called_inside_transaction");
      yield* participant.checkOutside;
      yield* connection.revalidate;
    });
    const transaction = Effect.fn("CommonJournal.transaction")(function* <A, E, R>(
      effect: Effect.Effect<A, E, R>,
    ): Effect.fn.Return<A, AdmissionError, R> {
      yield* outside;
      return yield* sql
        .withTransaction(
          Effect.gen(function* () {
            // First statement takes the SQLite writer lock even on a deferred client.
            // A process-local mutex is neither necessary nor sufficient for this fence.
            yield* sql`UPDATE jones_common_admission_schema SET version=version WHERE singleton=1`;
            const rows = yield* sql<{
              version: number;
              binding_json: string;
            }>`SELECT version,binding_json
        FROM jones_common_admission_schema WHERE singleton=1`;
            if (
              rows.length !== 1 ||
              rows[0]?.version !== 1 ||
              rows[0]?.binding_json !== canonical(connection.binding)
            )
              return yield* failure("journal_identity_or_schema_changed");
            const durability = yield* sql<{ synchronous: number }>`PRAGMA synchronous`;
            if ((durability[0]?.synchronous ?? 0) < 2)
              return yield* failure("journal_durability_unavailable");
            return yield* effect;
          }),
        )
        .pipe(Effect.mapError(storageError));
    });
    const read = Effect.fn("CommonJournal.read")(function* (key: string) {
      const rows =
        yield* sql<Stored>`SELECT record_json,record_sha256 FROM jones_common_reservations WHERE operation_key=${key}`;
      if (rows.length === 0) return null;
      const record = yield* validateStored(rows[0]!, decodeReservation);
      if (
        operationKey(record.binding.operation) !== key ||
        digest(record.binding) !== record.bindingSha256
      )
        return yield* failure("reservation_identity_corrupt");
      return record;
    });
    const readSlot = Effect.fn("CommonJournal.readSlot")(function* (slotId: string) {
      const rows =
        yield* sql<Stored>`SELECT record_json,record_sha256 FROM jones_common_slots WHERE slot_id=${slotId}`;
      if (rows.length === 0) return null;
      const slot = yield* validateStored(rows[0]!, decodeSlot);
      if (slot.slotId !== slotId) return yield* failure("slot_identity_corrupt");
      return slot;
    });
    const put = (record: Reservation) => sql`UPDATE jones_common_reservations
    SET phase=${record.phase},record_json=${canonical(record)},record_sha256=${digest(record)}
    WHERE operation_key=${operationKey(record.binding.operation)}`;
    const putSlot = (slot: Slot) => sql`UPDATE jones_common_slots SET
    record_json=${canonical(slot)},record_sha256=${digest(slot)} WHERE slot_id=${slot.slotId}`;
    const checkParticipant = Effect.fn("CommonJournal.checkParticipant")(function* (
      identity: ParticipantIdentity,
    ) {
      if (canonical(identity) !== canonical(participant.identity))
        return yield* failure("participant_not_current_owner");
      const rows = yield* sql<{
        identity_json: string;
      }>`SELECT identity_json FROM jones_common_incarnations WHERE nonce=${identity.nonce}`;
      if (rows[0]?.identity_json !== canonical(identity))
        return yield* failure("participant_not_enrolled");
    });
    const current = Effect.fn("CommonJournal.current")(function* (record: Reservation) {
      yield* checkParticipant(record.binding.participant);
      const stored = yield* read(operationKey(record.binding.operation));
      if (
        stored === null ||
        stored.bindingSha256 !== record.bindingSha256 ||
        canonical(stored) !== canonical(record)
      )
        return yield* failure("reservation_stale_or_changed");
      return stored;
    });
    const certificateMatches = (record: Reservation, certificate: LocalCertificate) =>
      certificate.journalId === connection.binding.journalId &&
      certificate.operationKey === operationKey(record.binding.operation) &&
      certificate.bindingSha256 === record.bindingSha256;
    const boundSlot = Effect.fn("CommonJournal.boundSlot")(function* (record: Reservation) {
      const request = record.binding.slot;
      if (request === null) return null;
      const slot = yield* readSlot(request.slotId);
      if (slot === null || slot.operation !== operationKey(record.binding.operation))
        return yield* failure("slot_operation_mismatch");
      return slot;
    });
    return CommonJournal.of({
      id: connection.binding.journalId,
      sql,
      enroll: Effect.fn("CommonJournal.enroll")((identity) =>
        transaction(
          Effect.gen(function* () {
            if (canonical(identity) !== canonical(participant.identity))
              return yield* failure("enrollment_owner_mismatch");
            const stores = yield* sql<{
              database_json: string;
            }>`SELECT database_json FROM jones_common_participants WHERE store_id=${identity.storeId}`;
            if (stores.length !== 0 && stores[0]?.database_json !== canonical(identity.database))
              return yield* failure("store_uuid_physical_database_collision");
            const nonces = yield* sql<{
              identity_json: string;
            }>`SELECT identity_json FROM jones_common_incarnations WHERE nonce=${identity.nonce}`;
            if (nonces.length !== 0 && nonces[0]?.identity_json !== canonical(identity))
              return yield* failure("participant_nonce_collision");
            yield* sql`INSERT OR IGNORE INTO jones_common_participants VALUES (${identity.storeId},${canonical(identity.database)})`;
            yield* sql`INSERT OR IGNORE INTO jones_common_incarnations VALUES (${identity.nonce},${canonical(identity)})`;
          }),
        ),
      ),
      enrollRoot: Effect.fn("CommonJournal.enrollRoot")((root) =>
        transaction(
          Effect.gen(function* () {
            if (root.unresolved.length !== 0) return yield* failure("root_must_exist");
            const identity = digest(root.ancestors[0].identity);
            const rows = yield* sql<{
              root_id: string;
            }>`SELECT root_id FROM jones_common_roots WHERE identity_sha256=${identity}`;
            if (rows[0] !== undefined) return rows[0].root_id;
            const rootId = digest({ journalId: connection.binding.journalId, identity });
            yield* sql`INSERT INTO jones_common_roots VALUES (${rootId},${identity},${canonical(root)})`;
            return rootId;
          }),
        ),
      ),
      enrollSlot: Effect.fn("CommonJournal.enrollSlot")((slot) =>
        transaction(
          Effect.gen(function* () {
            yield* checkParticipant(participant.identity);
            if (
              slot.owner.storeId !== participant.identity.storeId ||
              slot.phase !== "owned" ||
              slot.revision !== 0 ||
              slot.operation !== null ||
              slot.removalReceipt !== null ||
              slot.successor !== null ||
              slot.bindingCertificate !== null ||
              slot.scope.unresolved.length !== 0 ||
              slot.root.unresolved.length !== 0 ||
              slot.scope.ancestors[1] === undefined ||
              canonical(slot.scope.ancestors[1].identity) !==
                canonical(slot.root.ancestors[0].identity)
            )
              return yield* failure("invalid_initial_slot");
            const root = yield* sql<{
              identity_sha256: string;
            }>`SELECT identity_sha256 FROM jones_common_roots WHERE root_id=${slot.rootId}`;
            if (root[0]?.identity_sha256 !== digest(slot.root.ancestors[0].identity))
              return yield* failure("slot_root_binding");
            const existing = yield* readSlot(slot.slotId);
            if (existing !== null) {
              if (canonical(existing) !== canonical(slot)) return yield* failure("slot_collision");
              return;
            }
            const all =
              yield* sql<Stored>`SELECT record_json,record_sha256 FROM jones_common_slots`;
            for (const row of all) {
              const other = yield* validateStored(row, decodeSlot);
              if (overlaps(other.scope, slot.scope))
                return yield* failure("slot_physical_collision");
            }
            yield* sql`INSERT INTO jones_common_slots VALUES (${slot.slotId},${slot.scope.canonicalPath},${canonical(slot)},${digest(slot)})`;
          }),
        ),
      ),
      read: Effect.fn("CommonJournal.readPublic")((key) => transaction(read(key))),
      readSlot: Effect.fn("CommonJournal.readSlotPublic")((slotId) =>
        transaction(readSlot(slotId)),
      ),
      reserveScopes: Effect.fn("CommonJournal.reserveScopes")((binding: ReservationBinding) =>
        transaction(
          Effect.gen(function* () {
            yield* decodeReservationValue({
              binding,
              bindingSha256: digest(binding),
              phase: "pending",
              revision: 0,
              certificate: null,
              terminal: null,
            }).pipe(Effect.mapError(storageError));
            yield* checkParticipant(binding.participant);
            if (binding.operation.storeId !== participant.identity.storeId)
              return yield* failure("foreign_operation_key");
            yield* EnrollmentJournal.assertAdmission(sql, binding);
            const key = operationKey(binding.operation);
            if ((yield* read(key)) !== null)
              return yield* failure("durable_replay_has_no_live_permit");
            const request = binding.slot;
            let slot: Slot | null = null;
            if (
              (binding.kind === "removal") !== (request?.action === "remove") ||
              (binding.kind === "recreation") !== (request?.action === "recreate")
            )
              return yield* failure("slot_kind_mismatch");
            if (request !== null) {
              slot = yield* readSlot(request.slotId);
              if (
                slot === null ||
                slot.revision !== request.revision ||
                canonical(slot.owner) !== canonical(request.owner) ||
                canonical(binding.owner) !== canonical(slot.owner) ||
                slot.owner.storeId !== binding.operation.storeId ||
                binding.scopes.length !== 1 ||
                binding.scopes[0].canonicalPath !== slot.scope.canonicalPath ||
                (request.action === "remove" &&
                  (slot.phase !== "owned" ||
                    canonical(binding.scopes[0]) !== canonical(slot.scope))) ||
                (request.action === "recreate" &&
                  (slot.phase !== "removed" ||
                    slot.removalReceipt !== request.removalReceipt ||
                    binding.scopes[0].unresolved.length !== 1 ||
                    canonical(binding.scopes[0].ancestors[0].identity) !==
                      canonical(slot.root.ancestors[0].identity)))
              )
                return yield* failure("slot_cas_or_incarnation_mismatch");
            }
            const slots =
              yield* sql<Stored>`SELECT record_json,record_sha256 FROM jones_common_slots`;
            for (const row of slots) {
              const other = yield* validateStored(row, decodeSlot);
              if (
                other.phase !== "owned" &&
                other.slotId !== slot?.slotId &&
                binding.scopes.some((scope) => overlaps(scope, other.scope))
              )
                return yield* failure("destructive_slot_conflict");
            }
            const active =
              yield* sql<Stored>`SELECT record_json,record_sha256 FROM jones_common_reservations WHERE phase <> 'terminal'`;
            for (const row of active) {
              const other = yield* validateStored(row, decodeReservation);
              if (!binding.scopes.some((a) => other.binding.scopes.some((b) => overlaps(a, b))))
                continue;
              // Only the positive owning reference is discounted. Uses, pending
              // acceptances and unknown reference outcomes are never discounted.
              const owningReference =
                slot !== null &&
                other.phase === "accepted" &&
                other.binding.kind === "reference" &&
                canonical(other.binding.owner) === canonical(slot.owner);
              if ((request !== null && !owningReference) || other.binding.slot !== null)
                return yield* failure("overlapping_reservation");
            }
            const record: Reservation = {
              binding,
              bindingSha256: digest(binding),
              phase: "pending",
              revision: 0,
              certificate: null,
              terminal: null,
            };
            yield* sql`INSERT INTO jones_common_reservations VALUES (${key},'pending',${canonical(record)},${digest(record)})`;
            if (slot !== null && request !== null)
              yield* putSlot({
                ...slot,
                revision: slot.revision + 1,
                phase: request.action === "remove" ? "removal_reserved" : "recreation_reserved",
                operation: key,
              });
            return record;
          }),
        ),
      ),
      commitLocalAcceptance: Effect.fn("CommonJournal.commitLocalAcceptance")(
        (record, certificate) =>
          transaction(
            Effect.gen(function* () {
              yield* current(record);
              if (record.phase !== "pending" || !certificateMatches(record, certificate))
                return yield* failure("local_acceptance_certificate_mismatch");
              const next: Reservation = {
                ...record,
                phase: "accepted",
                revision: record.revision + 1,
                certificate,
              };
              yield* put(next);
              return next;
            }),
          ),
      ),
      enterOnce: Effect.fn("CommonJournal.enterOnce")((record) =>
        transaction(
          Effect.gen(function* () {
            yield* current(record);
            if (record.phase !== "accepted" || record.certificate === null)
              return yield* failure("effect_not_accepted_or_already_entered");
            yield* EnrollmentJournal.assertAdmission(sql, record.binding);
            const slot = yield* boundSlot(record);
            if (slot !== null) {
              if (slot.phase !== "removal_reserved" && slot.phase !== "recreation_reserved")
                return yield* failure("slot_entry_cas");
              yield* putSlot({
                ...slot,
                revision: slot.revision + 1,
                phase:
                  slot.phase === "removal_reserved" ? "removal_invoking" : "recreation_invoking",
              });
            }
            const next: Reservation = {
              ...record,
              phase: "entered",
              revision: record.revision + 1,
            };
            yield* put(next);
            return next;
          }),
        ),
      ),
      markUnknown: Effect.fn("CommonJournal.markUnknown")((record) =>
        transaction(
          Effect.gen(function* () {
            yield* current(record);
            if (record.phase !== "entered") return yield* failure("unknown_without_entry");
            yield* put({ ...record, phase: "unknown", revision: record.revision + 1 });
          }),
        ),
      ),
      recordTerminalEvidence: Effect.fn("CommonJournal.recordTerminalEvidence")(
        (original, evidence: TerminalEvidence) =>
          transaction(
            Effect.gen(function* () {
              yield* decodeTerminalEvidence(evidence).pipe(Effect.mapError(storageError));
              yield* checkParticipant(participant.identity);
              if (
                original.binding.participant.storeId !== participant.identity.storeId ||
                canonical(original.binding.participant.database) !==
                  canonical(participant.identity.database)
              )
                return yield* failure("foreign_store_terminal_evidence");
              const record = yield* read(operationKey(original.binding.operation));
              if (
                record === null ||
                record.bindingSha256 !== original.bindingSha256 ||
                !certificateMatches(record, evidence.certificate) ||
                canonical(record.certificate) !== canonical(evidence.certificate)
              )
                return yield* failure("terminal_binding_mismatch");
              if (record.phase === "terminal") {
                if (canonical(record.terminal) !== canonical(evidence))
                  return yield* failure("terminal_collision");
                return record;
              }
              const retiredReference =
                record.phase === "accepted" &&
                record.binding.kind === "reference" &&
                evidence.outcome === "retired";
              if (record.phase !== "entered" && record.phase !== "unknown" && !retiredReference)
                return yield* failure("terminal_without_entry");
              const slot = yield* boundSlot(record);
              if (slot === null && evidence.outcome !== "retired")
                return yield* failure("physical_outcome_without_slot");
              if (slot !== null) {
                if (
                  evidence.outcome === "removed" &&
                  slot.phase === "removal_invoking" &&
                  evidence.absent.canonicalPath === slot.scope.canonicalPath &&
                  evidence.absent.unresolved.length === 1 &&
                  canonical(evidence.absent.ancestors[0].identity) ===
                    canonical(slot.root.ancestors[0].identity)
                ) {
                  yield* putSlot({
                    ...slot,
                    phase: "removed",
                    revision: slot.revision + 1,
                    removalReceipt: digest(evidence),
                  });
                } else if (
                  evidence.outcome === "materialized" &&
                  slot.phase === "recreation_invoking" &&
                  evidence.successor.canonicalPath === slot.scope.canonicalPath &&
                  evidence.successor.unresolved.length === 0 &&
                  evidence.successor.ancestors[1] !== undefined &&
                  canonical(evidence.successor.ancestors[1].identity) ===
                    canonical(slot.root.ancestors[0].identity) &&
                  canonical(evidence.successor.ancestors[0].identity) !==
                    canonical(slot.scope.ancestors[0].identity)
                ) {
                  yield* putSlot({
                    ...slot,
                    phase: "materialized",
                    revision: slot.revision + 1,
                    successor: evidence.successor,
                  });
                } else return yield* failure("slot_terminal_evidence_mismatch");
              }
              const next: Reservation = {
                ...record,
                phase: "terminal",
                revision: record.revision + 1,
                terminal: evidence,
              };
              yield* put(next);
              return next;
            }),
          ),
      ),
      bindRecreation: Effect.fn("CommonJournal.bindRecreation")((certificate) =>
        transaction(
          Effect.gen(function* () {
            const slot = yield* readSlot(certificate.slotId);
            if (
              slot !== null &&
              slot.phase === "owned" &&
              slot.bindingCertificate === digest(certificate)
            )
              return slot;
            if (
              slot === null ||
              slot.phase !== "materialized" ||
              slot.revision !== certificate.slotRevision ||
              slot.successor === null ||
              slot.operation !== certificate.operationKey ||
              certificate.journalId !== connection.binding.journalId ||
              slot.owner.storeId !== participant.identity.storeId ||
              canonical(slot.owner) !== canonical(certificate.ownerBefore) ||
              canonical(slot.scope) !== canonical(certificate.oldScope) ||
              canonical(slot.successor) !== canonical(certificate.successor)
            )
              return yield* failure("recreation_binding_cas");
            const record = yield* read(certificate.operationKey);
            if (
              record === null ||
              record.phase !== "terminal" ||
              record.terminal?.outcome !== "materialized" ||
              !certificateMatches(record, certificate)
            )
              return yield* failure("recreation_binding_certificate");
            // The local owner adapter supplies a distinct consumed-certificate receipt
            // after its guarded old/new inode and target-revision CAS, never a mkdir ACK.
            if (certificate.receiptId === record.certificate?.receiptId)
              return yield* failure("recreation_requires_local_rebind_receipt");
            const next: Slot = {
              ...slot,
              scope: slot.successor,
              successor: null,
              phase: "owned",
              operation: null,
              revision: slot.revision + 1,
              owner: { ...slot.owner, targetRevision: slot.owner.targetRevision + 1 },
              bindingCertificate: digest(certificate),
            };
            yield* putSlot(next);
            return next;
          }),
        ),
      ),
    });
  }),
);
