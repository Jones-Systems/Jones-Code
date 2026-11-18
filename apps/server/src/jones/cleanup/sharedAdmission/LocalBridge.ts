import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import {
  canonical,
  digest,
  failure,
  LocalBridge,
  Participant,
  storageError,
  type LocalCertificate,
  type TerminalEvidence,
  type RecreationBindingCertificate,
  AdmissionError,
} from "./Protocol.ts";
import * as Context from "effect/Context";
import * as SqlClient from "effect/sql/SqlClient";
import { isSqlError } from "effect/sql/SqlError";
import { JournalConnection } from "./Journal.ts";
import * as LocalSchema from "./LocalSchema.ts";

/** Supplied by the actual ordinary/native/runtime owner. Validate retained
 * producer/physical-completion evidence with SQL-only guards here. A status
 * string is insufficient, and these callbacks must never perform filesystem I/O. */
export class LocalCustody extends Context.Service<
  LocalCustody,
  {
    readonly validateTerminal: (evidence: TerminalEvidence) => Effect.Effect<void, AdmissionError>;
    readonly validateRecreation: (
      certificate: RecreationBindingCertificate,
    ) => Effect.Effect<void, AdmissionError>;
  }
>()("t3/jones/cleanup/sharedAdmission/LocalBridge/LocalCustody") {}

/** DDL for root's future Jones loader allocation and synthetic fixtures only.
 * No app migration is allocated, and no upstream schema or FK is introduced. */
export const createLocalBridgeSchema = LocalSchema.migration;

/** Called only by enrollment after the Jones loader created the local schema.
 * Existing pins are immutable; replacing a sidecar never repairs a broken pin. */
export const pinLocalInstallation = Effect.gen(function* () {
  const participant = yield* Participant;
  const connection = yield* JournalConnection;
  const sql = participant.sql;
  for (const client of [sql, connection.sql])
    if (Option.isSome(yield* Effect.serviceOption(client.transactionService)))
      return yield* failure("installation_pin_inside_transaction");
  yield* participant.checkOutside;
  yield* connection.revalidate;
  yield* LocalSchema.enableDurableWriter(sql);
  yield* sql
    .withTransaction(
      Effect.gen(function* () {
        yield* participant.validateLocal;
        yield* LocalSchema.assertDurableWriter(sql);
        const rows = yield* sql<{
          store_id: string;
          database_json: string;
          journal_id: string;
          journal_binding_json: string;
        }>`
      SELECT store_id,database_json,journal_id,journal_binding_json FROM jones_common_admission_installation WHERE singleton=1`;
        if (
          rows.length !== 0 &&
          (rows[0]?.store_id !== participant.identity.storeId ||
            rows[0]?.database_json !== canonical(participant.identity.database) ||
            rows[0]?.journal_id !== connection.binding.journalId ||
            rows[0]?.journal_binding_json !== canonical(connection.binding))
        )
          return yield* failure("local_installation_pin_changed");
        if (rows.length === 0)
          yield* sql`INSERT INTO jones_common_admission_installation VALUES
      (1,${participant.identity.storeId},${canonical(participant.identity.database)},${connection.binding.journalId},${canonical(connection.binding)})`;
        const actual = yield* sql<{
          store_id: string;
          database_json: string;
          journal_id: string;
          journal_binding_json: string;
        }>`
          SELECT store_id,database_json,journal_id,journal_binding_json FROM jones_common_admission_installation WHERE singleton=1`;
        if (
          actual.length !== 1 ||
          actual[0]?.store_id !== participant.identity.storeId ||
          actual[0]?.database_json !== canonical(participant.identity.database) ||
          actual[0]?.journal_id !== connection.binding.journalId ||
          actual[0]?.journal_binding_json !== canonical(connection.binding)
        )
          return yield* failure("local_installation_pin_readback_mismatch");
      }),
    )
    .pipe(Effect.mapError(storageError));
});

/** @public No fallback custody validator or production schema mutation. */
export const layer = Layer.effect(
  LocalBridge,
  Effect.gen(function* () {
    const participant = yield* Participant;
    const custody = yield* LocalCustody;
    const connection = yield* JournalConnection;
    const sql = participant.sql;
    const validateRow = Effect.fn("CommonLocalBridge.validateRow")(function* (
      certificate: LocalCertificate,
    ) {
      yield* participant.validateLocal;
      const rows = yield* sql<{ certificate_json: string; certificate_sha256: string }>`
      SELECT certificate_json,certificate_sha256 FROM jones_common_admission_bridge
      WHERE operation_key=${certificate.operationKey}`;
      if (
        rows.length !== 1 ||
        rows[0]?.certificate_json !== canonical(certificate) ||
        rows[0]?.certificate_sha256 !== digest(certificate)
      )
        return yield* failure("local_bridge_mismatch");
    });
    const outside = Effect.gen(function* () {
      for (const client of [sql, connection.sql])
        if (Option.isSome(yield* Effect.serviceOption(client.transactionService)))
          return yield* failure("bridge_transaction_already_open");
    });
    return LocalBridge.of({
      validateInstallation: Effect.gen(function* () {
        yield* outside;
        const rows = yield* sql<{
          store_id: string;
          database_json: string;
          journal_id: string;
          journal_binding_json: string;
        }>`
        SELECT store_id,database_json,journal_id,journal_binding_json FROM jones_common_admission_installation WHERE singleton=1`;
        if (
          rows.length !== 1 ||
          rows[0]?.store_id !== participant.identity.storeId ||
          rows[0]?.database_json !== canonical(participant.identity.database) ||
          rows[0]?.journal_id !== connection.binding.journalId ||
          rows[0]?.journal_binding_json !== canonical(connection.binding)
        )
          return yield* failure("local_installation_pin_changed");
      }).pipe(Effect.mapError(storageError)),
      accept: Effect.fn("CommonLocalBridge.accept")(function* <A, E, R>(
        certificate: LocalCertificate,
        write: Effect.Effect<A, E, R>,
      ): Effect.fn.Return<A, E | AdmissionError, R> {
        yield* outside;
        return yield* sql
          .withTransaction(
            Effect.gen(function* () {
              yield* participant.validateLocal;
              const prior = yield* sql`SELECT operation_key FROM jones_common_admission_bridge
            WHERE operation_key=${certificate.operationKey}`;
              if (prior.length !== 0)
                return yield* failure("local_bridge_replay_no_live_authority");
              yield* sql`INSERT INTO jones_common_admission_bridge
            (operation_key,certificate_json,certificate_sha256) VALUES
            (${certificate.operationKey},${canonical(certificate)},${digest(certificate)})`;
              return yield* write;
            }).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
          )
          .pipe(Effect.mapError((cause) => (isSqlError(cause) ? storageError(cause) : cause)));
      }),
      validate: Effect.fn("CommonLocalBridge.validate")(function* (certificate) {
        yield* outside;
        yield* sql.withTransaction(validateRow(certificate)).pipe(Effect.mapError(storageError));
      }),
      recordTerminal: Effect.fn("CommonLocalBridge.recordTerminal")(function* (evidence) {
        yield* outside;
        yield* sql
          .withTransaction(
            Effect.gen(function* () {
              yield* validateRow(evidence.certificate);
              yield* custody.validateTerminal(evidence);
              const rows = yield* sql<{ terminal_json: string | null }>`SELECT terminal_json
          FROM jones_common_admission_bridge WHERE operation_key=${evidence.certificate.operationKey}`;
              const prior = rows[0]?.terminal_json;
              if (prior !== null && prior !== canonical(evidence))
                return yield* failure("local_terminal_collision");
              yield* sql`UPDATE jones_common_admission_bridge SET terminal_json=${canonical(evidence)},
          terminal_sha256=${digest(evidence)} WHERE operation_key=${evidence.certificate.operationKey}`;
            }),
          )
          .pipe(Effect.mapError(storageError));
      }),
      commitRecreationBinding: Effect.fn("CommonLocalBridge.commitRecreationBinding")(function* <
        A,
        E,
        R,
      >(
        certificate: RecreationBindingCertificate,
        write: Effect.Effect<A, E, R>,
      ): Effect.fn.Return<A, E | AdmissionError, R> {
        yield* outside;
        return yield* sql
          .withTransaction(
            Effect.gen(function* () {
              yield* participant.validateLocal;
              yield* custody.validateRecreation(certificate);
              const prior =
                yield* sql`SELECT operation_key FROM jones_common_recreation_bridge WHERE operation_key=${certificate.operationKey}`;
              if (prior.length !== 0)
                return yield* failure("recreation_local_certificate_already_consumed");
              yield* sql`INSERT INTO jones_common_recreation_bridge VALUES
            (${certificate.operationKey},${canonical(certificate)},${digest(certificate)})`;
              // The owning ordinary adapter performs its exact old/new witness CAS
              // in this same local transaction, under its existing guards.
              return yield* write;
            }).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
          )
          .pipe(Effect.mapError((cause) => (isSqlError(cause) ? storageError(cause) : cause)));
      }),
      validateRecreationBinding: Effect.fn("CommonLocalBridge.validateRecreationBinding")(
        function* (certificate) {
          yield* outside;
          yield* sql
            .withTransaction(
              Effect.gen(function* () {
                yield* participant.validateLocal;
                const rows = yield* sql<{ certificate_json: string; certificate_sha256: string }>`
          SELECT certificate_json,certificate_sha256 FROM jones_common_recreation_bridge WHERE operation_key=${certificate.operationKey}`;
                if (
                  rows.length !== 1 ||
                  rows[0]?.certificate_json !== canonical(certificate) ||
                  rows[0]?.certificate_sha256 !== digest(certificate)
                )
                  return yield* failure("recreation_local_consumed_certificate_missing");
              }),
            )
            .pipe(Effect.mapError(storageError));
        },
      ),
    });
  }),
);
