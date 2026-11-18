// The synchronous Effect UUID constructor keeps the existing factory context and cryptographic UUIDv4 contract.
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Journal from "./Journal.ts";
import * as JournalSchema from "./JournalSchema.ts";
import * as Inventory from "./EnrollmentInventory.ts";
import * as Shared from "./EnrollmentJournal.ts";
import { AdmissionError, canonical, failure, Participant, storageError } from "./Protocol.ts";

/** Root's Jones-owned immutable local schema/actual SqlClient adapter. Methods are SQL-only.
 * commit records the sparse manifest and seal atomically; read compares the actual retained row.
 * No implementation here invents a migration ID, identity, custody proof or default coverage. */
export class EnrollmentStore extends Context.Service<
  EnrollmentStore,
  {
    readonly commit: (
      seal: Shared.EnrollmentSeal,
      entries: readonly Inventory.InventoryEntry[],
    ) => Effect.Effect<void, AdmissionError>;
    readonly validateTerminalReconciliation: (
      before: Inventory.InventoryEntry,
      terminal: Inventory.InventoryEntry,
    ) => Effect.Effect<void, AdmissionError>;
    readonly read: (
      enrollmentId: string,
    ) => Effect.Effect<Shared.EnrollmentSeal | null, AdmissionError>;
  }
>()("t3/jones/cleanup/sharedAdmission/Enrollment/EnrollmentStore") {}
class EnrollmentGate extends Context.Service<
  EnrollmentGate,
  {
    readonly assertLocal: Effect.Effect<void, AdmissionError>;
    readonly assertOutside: Effect.Effect<void, AdmissionError>;
  }
>()("t3/jones/cleanup/sharedAdmission/Enrollment/EnrollmentGate") {}
class Enrollment extends Context.Service<
  Enrollment,
  {
    readonly prepareAndPublish: Effect.Effect<void, AdmissionError>;
    readonly participant: Participant["Service"];
    readonly gate: EnrollmentGate["Service"];
  }
>()("t3/jones/cleanup/sharedAdmission/Enrollment") {}

/** Construct inertly under the original checked owner. Root calls prepareAndPublish only
 * after selected-trial commit; all managed consumers receive this gated participant. */
export const make = Effect.fn("Enrollment.make")(function* (
  expectedPreviousNonce: string | null = null,
) {
  const participant = yield* Participant;
  const connection = yield* Journal.JournalConnection;
  const inventory = yield* Inventory.EnrollmentInventory;
  const store = yield* EnrollmentStore;
  const lifetime = yield* Effect.scope;
  const sql = participant.sql;
  if (sql === connection.sql) return yield* failure("enrollment_common_is_local_sql");
  let phase: "closed" | "preparing" | "ready" | "revoked" = "closed";
  let captured: Effect.Success<typeof inventory.capture> | null = null;
  let issuedSeal: Shared.EnrollmentSeal | null = null;
  const id = SecureRandom.uuidV4Unsafe();
  const assertOutside = Effect.gen(function* () {
    if (lifetime.state._tag === "Closed" || phase === "revoked")
      return yield* failure("enrollment_gate_revoked");
    for (const client of [sql, connection.sql])
      if (Option.isSome(yield* Effect.serviceOption(client.transactionService)))
        return yield* failure("enrollment_called_inside_transaction");
    yield* participant.checkOutside;
    yield* connection.revalidate;
  });
  const assertClosed = Effect.gen(function* () {
    yield* assertOutside;
    if (phase !== "preparing") return yield* failure("enrollment_not_in_private_staging");
  });
  const verifySeal = Effect.fn("Enrollment.verifyActualLocalSeal")(function* (
    seal: Shared.EnrollmentSeal,
  ) {
    yield* assertClosed;
    if (issuedSeal === null || captured === null || canonical(seal) !== canonical(issuedSeal))
      return yield* failure("enrollment_seal_not_issued_by_operation");
    yield* captured.revalidate;
    const actual = yield* sql
      .withTransaction(
        Effect.gen(function* () {
          yield* participant.validateLocal;
          return yield* store.read(id);
        }),
      )
      .pipe(Effect.mapError(storageError));
    if (canonical(actual) !== canonical(issuedSeal))
      return yield* failure("enrollment_local_seal_changed");
  });
  const shared = yield* Shared.make.pipe(
    Effect.provideService(Shared.LocalEnrollment, {
      assertClosed,
      verifySeal,
      validateTerminalReconciliation: store.validateTerminalReconciliation,
    }),
  );
  const gate = EnrollmentGate.of({
    assertOutside: Effect.gen(function* () {
      yield* assertOutside;
      if (phase !== "ready") return yield* failure("enrollment_gate_closed");
    }),
    assertLocal: Effect.gen(function* () {
      if (
        phase !== "ready" ||
        lifetime.state._tag === "Closed" ||
        Option.isNone(yield* Effect.serviceOption(sql.transactionService)) ||
        Option.isSome(yield* Effect.serviceOption(connection.sql.transactionService))
      )
        return yield* failure("enrollment_gate_closed_or_wrong_sql");
      yield* participant.validateLocal;
    }),
  });
  return Enrollment.of({
    gate,
    participant: Participant.of({
      ...participant,
      checkOutside: gate.assertOutside,
      validateLocal: gate.assertLocal,
    }),
    prepareAndPublish: Effect.gen(function* () {
      if (phase !== "closed") return yield* failure("enrollment_operation_one_shot");
      phase = "preparing";
      yield* assertOutside;
      const sync = yield* sql<{ synchronous: number }>`PRAGMA synchronous`;
      const full = yield* sql<{ fullfsync: number }>`PRAGMA fullfsync`;
      const checkpoint = yield* sql<{ checkpoint_fullfsync: number }>`PRAGMA checkpoint_fullfsync`;
      if (
        (sync[0]?.synchronous ?? 0) < 2 ||
        full[0]?.fullfsync !== 1 ||
        checkpoint[0]?.checkpoint_fullfsync !== 1
      )
        return yield* failure("enrollment_local_full_required");
      const progress = yield* shared.beginStore(participant.identity, id, expectedPreviousNonce);
      if (progress.phase === "draining")
        return yield* failure("enrollment_draining_entered_destructive");
      captured = yield* inventory.capture;
      for (let offset = 0; offset < captured.entries.length; offset += 256)
        yield* shared.importBatch(id, captured.entries.slice(offset, offset + 256));
      yield* captured.revalidate;
      const seal: Shared.EnrollmentSeal = {
        enrollmentId: id,
        storeId: participant.identity.storeId,
        journalId: connection.binding.journalId,
        participantNonce: participant.identity.nonce,
        inventoryDigest: captured.inventoryDigest,
        itemCount: captured.entries.length,
        schemaContractDigest: JournalSchema.contractDigest,
        coverageContract: captured.coverageContract,
        sourceSnapshotDigest: captured.sourceSnapshotDigest,
      };
      const entries = captured.entries;
      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* participant.validateLocal;
            yield* store.commit(seal, entries);
            const actual = yield* store.read(id);
            if (canonical(actual) !== canonical(seal))
              return yield* failure("enrollment_local_seal_commit_mismatch");
          }),
        )
        .pipe(Effect.mapError(storageError));
      issuedSeal = seal;
      yield* shared.publish(id, seal);
      yield* assertOutside;
      phase = "ready";
    }).pipe(
      Effect.mapError(storageError),
      Effect.tapError(() =>
        Effect.sync(() => {
          phase = "revoked";
        }),
      ),
    ),
  });
});
