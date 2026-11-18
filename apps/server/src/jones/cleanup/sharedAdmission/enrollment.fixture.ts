// @effect-diagnostics-next-line nodeBuiltinImport:off -- This synthetic native SQLite fixture keeps its synchronous main-filename coordinate check and existing service context.
import * as NodePath from "node:path";
// Synthetic foundation stores have exactly one managed domain: LocalBridge rows.
// Its existing durable shared reservations already represent every historical commitment.
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Enrollment from "./Enrollment.ts";
import * as Inventory from "./EnrollmentInventory.ts";
import * as Shared from "./EnrollmentJournal.ts";
import * as Journal from "./Journal.ts";
import {
  canonical,
  digest,
  failure,
  Participant,
  Reservation,
  ScopeObservation,
  storageError,
} from "./Protocol.ts";
const decodeCertificateJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const decodeExistingReservation = Schema.decodeUnknownEffect(Schema.fromJsonString(Reservation));
const decodeSyntheticSeal = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Shared.EnrollmentSeal),
);

export const enrollSyntheticFoundation = Effect.gen(function* () {
  const participant = yield* Participant;
  const common = (yield* Journal.JournalConnection).sql;
  const sql = participant.sql;
  const observations = yield* ScopeObservation;
  const infrastructure = yield* Shared.makeInfrastructure;
  const databases = yield* sql<{ name: string; file: string }>`PRAGMA database_list`;
  const main = databases.find((database) => database.name === "main");
  if (main === undefined) return yield* failure("synthetic_database_unavailable");
  const scope = yield* observations.capture(NodePath.dirname(main.file));
  const permit = yield* infrastructure.beginInfrastructure(scope);
  yield* infrastructure.bindStoreInfrastructure(permit);
  yield* sql`PRAGMA synchronous=FULL`;
  yield* sql`PRAGMA fullfsync=ON`;
  yield* sql`PRAGMA checkpoint_fullfsync=ON`;
  yield* sql`CREATE TABLE IF NOT EXISTS jones_fixture_enrollment_seals (id TEXT PRIMARY KEY, seal_json TEXT NOT NULL)`;
  const read = sql<{ operation_key: string; certificate_json: string; certificate_sha256: string }>`
    SELECT operation_key,certificate_json,certificate_sha256 FROM jones_common_admission_bridge ORDER BY operation_key`;
  const inventory = yield* Inventory.makeForSynthetic.pipe(
    Effect.provideService(Inventory.InventoryReaders, {
      coverageContract: participant.identity.coverage,
      requiredDomains: ["synthetic-local-bridge"],
      readers: [
        {
          domain: "synthetic-local-bridge",
          readLocal: Effect.gen(function* () {
            const records = yield* read.pipe(Effect.mapError(storageError));
            return { records, snapshotDigest: digest(records) };
          }),
          locate: Effect.fn("SyntheticEnrollment.checkAlreadyRepresented")(function* (records) {
            for (const raw of records) {
              const row = raw as {
                operation_key: string;
                certificate_json: string;
                certificate_sha256: string;
              };
              const certificate: unknown = decodeCertificateJson(row.certificate_json);
              if (digest(certificate) !== row.certificate_sha256)
                return yield* failure("synthetic_bridge_corrupt");
              const shared = yield* common<{
                record_json: string;
              }>`SELECT record_json FROM jones_common_reservations WHERE operation_key=${row.operation_key}`.pipe(
                Effect.mapError(storageError),
              );
              if (shared[0] === undefined)
                return yield* failure("synthetic_bridge_not_represented");
              const reservation = yield* decodeExistingReservation(shared[0].record_json).pipe(
                Effect.mapError(storageError),
              );
              if (
                typeof certificate !== "object" ||
                certificate === null ||
                !("bindingSha256" in certificate) ||
                certificate.bindingSha256 !== reservation.bindingSha256
              )
                return yield* failure("synthetic_shared_binding_mismatch");
            }
            return [];
          }),
          revalidateLocal: Effect.fn("SyntheticEnrollment.revalidate")(function* (snapshot) {
            if (digest(yield* read.pipe(Effect.mapError(storageError))) !== snapshot)
              return yield* failure("synthetic_bridge_snapshot_changed");
          }),
        },
      ],
    }),
  );
  const previous = yield* common<{
    nonce: string;
  }>`SELECT nonce FROM jones_common_store_current WHERE store_id=${participant.identity.storeId}`;
  const enrollment = yield* Enrollment.make(previous[0]?.nonce ?? null).pipe(
    Effect.provideService(Inventory.EnrollmentInventory, inventory),
    Effect.provideService(Enrollment.EnrollmentStore, {
      validateTerminalReconciliation: () => failure("synthetic_no_imported_reconciliation"),
      commit: Effect.fn("SyntheticEnrollment.commit")((seal: Shared.EnrollmentSeal) =>
        sql`INSERT INTO jones_fixture_enrollment_seals VALUES (${seal.enrollmentId},${canonical(seal)})`.pipe(
          Effect.asVoid,
          Effect.mapError(storageError),
        ),
      ),
      read: Effect.fn("SyntheticEnrollment.read")(function* (id) {
        const rows = yield* sql<{
          seal_json: string;
        }>`SELECT seal_json FROM jones_fixture_enrollment_seals WHERE id=${id}`;
        return rows[0] === undefined ? null : yield* decodeSyntheticSeal(rows[0].seal_json);
      }, Effect.mapError(storageError)),
    }),
  );
  yield* enrollment.prepareAndPublish.pipe(
    Effect.catchTags({
      SharedAdmissionError: (error) =>
        error.reason === "enrollment_draining_entered_destructive"
          ? Effect.void
          : Effect.fail(error),
    }),
  );
});
