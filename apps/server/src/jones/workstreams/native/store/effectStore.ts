import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import { requireNativeRecoveryReceipt } from "../domain/externalIntents.ts";
import type { ExternalIntent, RegistryExternalResult } from "../domain/state.ts";
import { canonicalJson } from "../domain/validation.ts";
import * as ReceiptStore from "./receiptStore.ts";
import * as HistoryStore from "./historyStore.ts";
import * as NativeSql from "./sql.ts";

export interface Reservation {
  readonly command_id: string;
  readonly principal_id: string;
  readonly native_reference_id: string;
  readonly effect: "verify_reference" | "request_native_t3_settlement" | "refresh_linked_pr";
  readonly server_generation: number;
  readonly admitted_registry_version: number;
  readonly intent: unknown;
}
const decodeRequest = Schema.decodeSync(Schema.fromJsonString(Schema.Unknown));
type Failure = SqlError | NativeSql.StoreError | NativeSql.CryptoError;
export class EffectStore extends Context.Service<
  EffectStore,
  {
    readonly reserve: (
      scope: NativeSql.OwnerScope,
      receipt: ReceiptStore.ReceiptRecord,
      reservation: Reservation,
    ) => Effect.Effect<void, Failure>;
    readonly active: (
      scope: NativeSql.OwnerScope,
    ) => Effect.Effect<readonly Reservation[], Failure>;
    readonly saveResult: (
      scope: NativeSql.OwnerScope,
      commandId: string,
      result: unknown,
    ) => Effect.Effect<void, Failure>;
    readonly result: (
      scope: NativeSql.OwnerScope,
      commandId: string,
    ) => Effect.Effect<unknown | null, Failure>;
    readonly complete: (
      scope: NativeSql.OwnerScope,
      receipt: ReceiptStore.ReceiptRecord,
      registryVersion: number,
      history: readonly HistoryStore.HistoryEvent[],
    ) => Effect.Effect<void, Failure>;
    readonly completeRetainedNativeSettlement: (
      scope: NativeSql.OwnerScope,
      receipt: ReceiptStore.ReceiptRecord,
      expected: { readonly server_generation: number; readonly registry_version: number },
      history: readonly HistoryStore.HistoryEvent[],
    ) => Effect.Effect<void, Failure>;
  }
>()("t3/jones/workstreams/native/store/effectStore") {}
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const db = yield* NativeSql.NativeSql;
  const receipts = yield* ReceiptStore.ReceiptStore;
  const historyStore = yield* HistoryStore.HistoryStore;
  const active = Effect.fn("EffectStore.active")((scope: NativeSql.OwnerScope) =>
    db.transaction(
      scope,
      Effect.gen(function* () {
        const rows = yield* sql<
          Omit<Reservation, "intent"> & { envelope: string }
        >`SELECT * FROM jones_workstreams_external_reservations WHERE registry_instance_id = ${scope.registry_instance_id} AND owner_id = ${scope.owner_id} AND state = 'active' ORDER BY command_id COLLATE BINARY`;
        const result: Reservation[] = [];
        for (const row of rows) {
          const intent = yield* db.openJson(
            {
              ...scope,
              purpose: "external_intent",
              record_id: row.command_id,
              revision: row.admitted_registry_version,
            },
            row.envelope,
          );
          result.push({
            command_id: row.command_id,
            principal_id: row.principal_id,
            native_reference_id: row.native_reference_id,
            effect: row.effect,
            server_generation: row.server_generation,
            admitted_registry_version: row.admitted_registry_version,
            intent,
          });
        }
        return result;
      }),
    ),
  );
  const reserve = Effect.fn("EffectStore.reserve")(
    (scope: NativeSql.OwnerScope, receipt: ReceiptStore.ReceiptRecord, reservation: Reservation) =>
      db.transaction(
        scope,
        Effect.gen(function* () {
          if (
            receipt.receipt.command_id !== reservation.command_id ||
            receipt.receipt.actor.principal_id !== reservation.principal_id ||
            receipt.receipt.server_generation !== reservation.server_generation ||
            !["pending", "unresolved"].includes(receipt.receipt.state)
          )
            return yield* new NativeSql.StoreError({ reason: "invalid_payload" });
          const existing = (yield* active(scope)).find(
            (item) => item.command_id === reservation.command_id,
          );
          if (existing) {
            if (NativeSql.jsonText(existing) !== NativeSql.jsonText(reservation))
              return yield* new NativeSql.StoreError({ reason: "idempotency_conflict" });
            yield* receipts.save(scope, receipt, null);
            return;
          }
          const context = yield* sql<{
            server_generation: number;
            registry_version: number;
            state: string;
          }>`SELECT server_generation,registry_version,state FROM jones_workstreams_registries WHERE registry_instance_id = ${scope.registry_instance_id} AND owner_id = ${scope.owner_id}`;
          if (
            context[0]?.state !== "active" ||
            context[0].server_generation !== reservation.server_generation ||
            context[0].registry_version !== reservation.admitted_registry_version
          )
            return yield* new NativeSql.StoreError({ reason: "conflict" });
          const saved = yield* receipts.save(scope, receipt, null);
          if (!["pending", "unresolved"].includes(saved.receipt.state))
            return yield* new NativeSql.StoreError({ reason: "conflict" });
          const envelope = yield* db.sealJson(
            {
              ...scope,
              purpose: "external_intent",
              record_id: reservation.command_id,
              revision: reservation.admitted_registry_version,
            },
            reservation.intent,
          );
          const { intent: _intent, ...row } = reservation;
          yield* sql`INSERT INTO jones_workstreams_external_reservations ${sql.insert({ ...scope, ...row, state: "active", envelope })}`;
        }),
      ),
  );
  const result = Effect.fn("EffectStore.result")((scope: NativeSql.OwnerScope, commandId: string) =>
    db.transaction(
      scope,
      Effect.gen(function* () {
        const rows = yield* sql<{
          envelope: string;
          admitted_registry_version: number;
        }>`SELECT r.envelope,s.admitted_registry_version FROM jones_workstreams_external_results r JOIN jones_workstreams_external_reservations s ON s.registry_instance_id = r.registry_instance_id AND s.owner_id = r.owner_id AND s.command_id = r.command_id WHERE r.registry_instance_id = ${scope.registry_instance_id} AND r.owner_id = ${scope.owner_id} AND r.command_id = ${commandId}`;
        return rows[0]
          ? yield* db.openJson(
              {
                ...scope,
                purpose: "external_result",
                record_id: commandId,
                revision: rows[0].admitted_registry_version,
              },
              rows[0].envelope,
            )
          : null;
      }),
    ),
  );
  const saveResult = Effect.fn("EffectStore.saveResult")(
    (scope: NativeSql.OwnerScope, commandId: string, value: unknown) =>
      db.transaction(
        scope,
        Effect.gen(function* () {
          if (value === null || value === undefined)
            return yield* new NativeSql.StoreError({ reason: "invalid_payload" });
          const old = yield* result(scope, commandId);
          if (old !== null) {
            if (NativeSql.jsonText(old) !== NativeSql.jsonText(value))
              return yield* new NativeSql.StoreError({ reason: "idempotency_conflict" });
            return;
          }
          const reservation = (yield* active(scope)).find((item) => item.command_id === commandId);
          if (!reservation) return yield* new NativeSql.StoreError({ reason: "not_found" });
          const envelope = yield* db.sealJson(
            {
              ...scope,
              purpose: "external_result",
              record_id: commandId,
              revision: reservation.admitted_registry_version,
            },
            value,
          );
          yield* sql`INSERT INTO jones_workstreams_external_results ${sql.insert({ ...scope, command_id: commandId, envelope })}`;
        }),
      ),
  );
  const writeCompletion = Effect.fn("EffectStore.writeCompletion")(function* (
    scope: NativeSql.OwnerScope,
    receipt: ReceiptStore.ReceiptRecord,
    registryVersion: number,
    events: readonly HistoryStore.HistoryEvent[],
  ) {
    yield* receipts.save(scope, receipt, registryVersion);
    for (const event of events) yield* historyStore.append(scope, event);
    yield* sql`UPDATE jones_workstreams_external_reservations SET state = 'released' WHERE registry_instance_id = ${scope.registry_instance_id} AND owner_id = ${scope.owner_id} AND command_id = ${receipt.receipt.command_id}`;
  });
  const complete = Effect.fn("EffectStore.complete")(
    (
      scope: NativeSql.OwnerScope,
      receipt: ReceiptStore.ReceiptRecord,
      registryVersion: number,
      events: readonly HistoryStore.HistoryEvent[],
    ) =>
      db.transaction(
        scope,
        Effect.gen(function* () {
          if (receipt.receipt.state !== "committed" && receipt.receipt.state !== "rejected")
            return yield* new NativeSql.StoreError({ reason: "invalid_payload" });
          const reservation = (yield* active(scope)).find(
            (item) => item.command_id === receipt.receipt.command_id,
          );
          if (!reservation || (yield* result(scope, reservation.command_id)) === null)
            return yield* new NativeSql.StoreError({ reason: "not_found" });
          const contexts = yield* sql<{
            server_generation: number;
            registry_version: number;
          }>`SELECT server_generation,registry_version FROM jones_workstreams_registries WHERE registry_instance_id = ${scope.registry_instance_id} AND owner_id = ${scope.owner_id}`;
          const context = contexts[0];
          if (
            !context ||
            context.server_generation !== reservation.server_generation ||
            context.registry_version !== registryVersion ||
            (receipt.receipt.state === "committed" &&
              registryVersion !==
                reservation.admitted_registry_version + (receipt.receipt.changed ? 1 : 0))
          )
            return yield* new NativeSql.StoreError({ reason: "conflict" });
          if (
            events.length === 0 ||
            events.some(
              (event) =>
                event.command_id !== reservation.command_id ||
                event.registry_version !== registryVersion,
            )
          )
            return yield* new NativeSql.StoreError({ reason: "invalid_payload" });
          yield* writeCompletion(scope, receipt, registryVersion, events);
        }),
      ),
  );
  const completeRetainedNativeSettlement = Effect.fn(
    "EffectStore.completeRetainedNativeSettlement",
  )(
    (
      scope: NativeSql.OwnerScope,
      receipt: ReceiptStore.ReceiptRecord,
      expected: { readonly server_generation: number; readonly registry_version: number },
      events: readonly HistoryStore.HistoryEvent[],
    ) =>
      db.transaction(
        scope,
        Effect.gen(function* () {
          const value = receipt.receipt;
          if (value.state !== "committed")
            return yield* new NativeSql.StoreError({ reason: "invalid_payload" });
          const reservation = (yield* active(scope)).find(
            (item) => item.command_id === value.command_id,
          );
          const retained = yield* result(scope, value.command_id);
          const prior = yield* receipts.read(scope, value.command_id);
          if (!reservation || retained === null || !prior)
            return yield* new NativeSql.StoreError({ reason: "not_found" });
          const contexts = yield* sql<{
            server_generation: number;
            registry_version: number;
            state: string;
          }>`SELECT server_generation,registry_version,state FROM jones_workstreams_registries WHERE registry_instance_id = ${scope.registry_instance_id} AND owner_id = ${scope.owner_id}`;
          const context = contexts[0];
          if (
            !context ||
            context.state !== "active" ||
            context.server_generation !== expected.server_generation ||
            context.server_generation !== reservation.server_generation ||
            !Number.isSafeInteger(expected.registry_version) ||
            expected.registry_version < reservation.admitted_registry_version ||
            context.registry_version !== expected.registry_version + (value.changed ? 1 : 0) ||
            value.registry_version !== context.registry_version
          )
            return yield* new NativeSql.StoreError({ reason: "conflict" });
          yield* Effect.try({
            try: () => {
              const intent = reservation.intent as ExternalIntent;
              if (
                reservation.effect !== "request_native_t3_settlement" ||
                intent.kind !== "native_t3_settlement" ||
                intent.owner_id !== scope.owner_id ||
                intent.command.command_id !== reservation.command_id ||
                intent.actor.principal_id !== reservation.principal_id ||
                intent.authorization.server_generation !== reservation.server_generation ||
                prior.receipt.server_generation !== reservation.server_generation ||
                intent.reference.native_reference_id !== reservation.native_reference_id ||
                intent.admitted_registry_version !== reservation.admitted_registry_version ||
                intent.command.expected_registry_version !==
                  reservation.admitted_registry_version ||
                prior.origin !== "native" ||
                receipt.origin !== prior.origin ||
                receipt.request_body !== prior.request_body ||
                canonicalJson(decodeRequest(prior.request_body)) !== canonicalJson(intent.command)
              )
                throw new NativeSql.StoreError({ reason: "invalid_payload" });
              requireNativeRecoveryReceipt(
                prior.receipt,
                intent,
                retained as RegistryExternalResult,
                value,
                { ...context, owner_id: scope.owner_id },
              );
              const event = events[0];
              if (
                events.length !== 1 ||
                !event ||
                event.command_id !== value.command_id ||
                event.registry_version !== value.registry_version ||
                event.occurred_at !== value.completed_at ||
                event.workstream_ids.length !== 0 ||
                canonicalJson(event.value) !==
                  canonicalJson({
                    event_id: event.event_id,
                    command_id: value.command_id,
                    actor: value.actor,
                    operation: value.operation,
                    occurred_at: value.completed_at,
                    registry_version: value.registry_version,
                    changed: value.changed,
                    workstream_versions: value.effects.workstream_versions,
                    native_reference_id: value.effects.native_reference_id,
                    membership_ids: value.effects.membership_ids,
                    declaration_id: value.effects.declaration_id,
                    declaration_revision: value.effects.declaration_revision,
                    edge_id: value.effects.edge_id,
                    lifecycle_declaration: value.effects.lifecycle_declaration,
                  })
              )
                throw new NativeSql.StoreError({ reason: "invalid_payload" });
            },
            catch: () => new NativeSql.StoreError({ reason: "invalid_payload" }),
          });
          yield* writeCompletion(scope, receipt, value.registry_version, events);
        }),
      ),
  );
  return EffectStore.of({
    reserve,
    active,
    saveResult,
    result,
    complete,
    completeRetainedNativeSettlement,
  });
});
/** @public Canonical service layer; provider calls remain outside store transactions. */
export const layer = Layer.effect(EffectStore, make);
