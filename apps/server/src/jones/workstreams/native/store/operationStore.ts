// @effect-diagnostics nodeBuiltinImport:off -- SHA-256 binds exact accepted operation bytes across restart.
import { canonicalWorkstreamsSnapshotJson } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as NodeCrypto from "node:crypto";
import { WorkstreamOperationOrigin } from "@t3tools/contracts/jones/workstreamOperations";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import * as BindingStore from "./bindingStore.ts";
import * as NativeSql from "./sql.ts";
import * as Callers from "../runtime/recordedCaller.ts";

export type OperationState =
  | "accepted"
  | "dispatching"
  | "in_progress"
  | "reconciling"
  | "committed"
  | "already_satisfied"
  | "conflict"
  | "rejected"
  | "unavailable";
export interface OperationInput {
  readonly operation_id: string;
  readonly kind: "set_primary" | "create_workstream" | "command" | "appearance";
  readonly thread_environment_id: string | null;
  readonly thread_id: string | null;
  readonly origin: WorkstreamOperationOrigin;
  readonly destination: Omit<BindingStore.Binding, "state">;
  readonly child_command_ids: readonly string[];
  readonly request_body: string;
  readonly accepted_at: string;
  readonly recorded_caller?: Callers.RecordedCallerV1 | null;
}
export interface Operation extends OperationInput {
  readonly request_sha256: string;
  readonly state: OperationState;
  readonly updated_at: string;
  readonly next_poll_at: string | null;
  readonly result_body: string | null;
}
type Failure = SqlError | NativeSql.StoreError | NativeSql.CryptoError;
const decodeOrigin = Schema.decodeUnknownEffect(WorkstreamOperationOrigin);
const decodeOriginJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(WorkstreamOperationOrigin),
);
const decodeChildren = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Array(Schema.String)),
);
export class OperationStore extends Context.Service<
  OperationStore,
  {
    readonly read: (id: string) => Effect.Effect<Operation | null, Failure>;
    readonly accept: (operation: OperationInput) => Effect.Effect<Operation, Failure>;
    readonly advance: (
      id: string,
      expectedState: OperationState,
      nextState: OperationState,
      updatedAt: string,
      nextPollAt: string | null,
      resultBody?: string,
    ) => Effect.Effect<Operation, Failure>;
    readonly open: () => Effect.Effect<readonly Operation[], Failure>;
  }
>()("t3/jones/workstreams/native/store/operationStore") {}
interface Row {
  readonly operation_id: string;
  readonly kind: OperationInput["kind"];
  readonly thread_environment_id: string | null;
  readonly thread_id: string | null;
  readonly origin: string;
  readonly request_sha256: string;
  readonly dest_registry_instance_id: string;
  readonly dest_owner_id: string;
  readonly dest_home_environment_id: string;
  readonly dest_home_generation: number;
  readonly binding_version: number;
  readonly state: OperationState;
  readonly child_command_ids: string;
  readonly accepted_at: string;
  readonly updated_at: string;
  readonly next_poll_at: string | null;
  readonly request_envelope: string;
  readonly result_envelope: string | null;
}
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const db = yield* NativeSql.NativeSql;
  const bindings = yield* BindingStore.BindingStore;
  const decodeRow = Effect.fn("OperationStore.decodeRow")(function* (row: Row) {
    const scope = {
      registry_instance_id: row.dest_registry_instance_id,
      owner_id: row.dest_owner_id,
      key_scope: "local" as const,
    };
    const payload = yield* db.openJson(
      { ...scope, purpose: "operation_request", record_id: row.operation_id, revision: 1 },
      row.request_envelope,
    );
    if (
      typeof payload !== "object" ||
      payload === null ||
      !("request_body" in payload) ||
      typeof payload.request_body !== "string" ||
      !("principal_id" in payload) ||
      typeof payload.principal_id !== "string" ||
      NodeCrypto.createHash("sha256").update(payload.request_body).digest("hex") !==
        row.request_sha256
    )
      return yield* new NativeSql.StoreError({ reason: "invalid_payload" });
    const versioned = "version" in payload;
    if (
      (versioned && payload.version !== 2) ||
      Object.keys(payload).some(
        (key) =>
          !(
            versioned
              ? ["version", "request_body", "principal_id", "recorded_caller"]
              : ["request_body", "principal_id"]
          ).includes(key),
      )
    )
      return yield* new NativeSql.StoreError({ reason: "invalid_payload" });
    const recordedCaller =
      versioned && "recorded_caller" in payload && payload.recorded_caller !== null
        ? yield* Callers.decodeRecordedCaller(payload.recorded_caller).pipe(
            Effect.mapError(() => new NativeSql.StoreError({ reason: "invalid_payload" })),
          )
        : null;
    const origin = yield* decodeOriginJson(row.origin).pipe(
      Effect.mapError(() => new NativeSql.StoreError({ reason: "invalid_payload" })),
    );
    const childIds = yield* decodeChildren(row.child_command_ids).pipe(
      Effect.mapError(() => new NativeSql.StoreError({ reason: "invalid_payload" })),
    );
    if (!Array.isArray(childIds) || !childIds.every((id): id is string => typeof id === "string"))
      return yield* new NativeSql.StoreError({ reason: "invalid_payload" });
    const result = row.result_envelope
      ? yield* db.openJson(
          { ...scope, purpose: "operation_result", record_id: row.operation_id, revision: 1 },
          row.result_envelope,
        )
      : null;
    if (result !== null && typeof result !== "string")
      return yield* new NativeSql.StoreError({ reason: "invalid_payload" });
    if (
      recordedCaller &&
      (Object.entries(recordedCaller.destination).some(
        ([key, value]) =>
          value !==
          {
            registry_instance_id: row.dest_registry_instance_id,
            owner_id: row.dest_owner_id,
            home_environment_id: row.dest_home_environment_id,
            home_generation: row.dest_home_generation,
            principal_id: payload.principal_id,
            binding_version: row.binding_version,
          }[key as keyof typeof recordedCaller.destination],
      ) ||
        canonicalWorkstreamsSnapshotJson(recordedCaller.origin) !==
          canonicalWorkstreamsSnapshotJson(origin))
    )
      return yield* new NativeSql.StoreError({ reason: "invalid_payload" });
    return {
      operation_id: row.operation_id,
      kind: row.kind,
      thread_environment_id: row.thread_environment_id,
      thread_id: row.thread_id,
      origin,
      destination: {
        registry_instance_id: row.dest_registry_instance_id,
        owner_id: row.dest_owner_id,
        home_environment_id: row.dest_home_environment_id,
        home_generation: row.dest_home_generation,
        binding_version: row.binding_version,
        principal_id: payload.principal_id,
      },
      child_command_ids: childIds,
      request_body: payload.request_body,
      recorded_caller: recordedCaller,
      accepted_at: row.accepted_at,
      request_sha256: row.request_sha256,
      state: row.state,
      updated_at: row.updated_at,
      next_poll_at: row.next_poll_at,
      result_body: result,
    } satisfies Operation;
  });
  const read = Effect.fn("OperationStore.read")((id: string) =>
    sql.withTransaction(
      Effect.gen(function* () {
        const rows =
          yield* sql<Row>`SELECT * FROM jones_workstreams_operations WHERE operation_id = ${id}`;
        return rows[0]
          ? yield* db.transaction(
              {
                registry_instance_id: rows[0].dest_registry_instance_id,
                owner_id: rows[0].dest_owner_id,
              },
              decodeRow(rows[0]),
            )
          : null;
      }),
    ),
  );
  const accept = Effect.fn("OperationStore.accept")((operation: OperationInput) =>
    db.transaction(
      operation.destination,
      Effect.gen(function* () {
        const old = yield* read(operation.operation_id);
        if (old) {
          if (
            old.request_body !== operation.request_body ||
            [
              "registry_instance_id",
              "owner_id",
              "home_environment_id",
              "home_generation",
              "principal_id",
              "binding_version",
            ].some(
              (key) =>
                old.destination[key as keyof typeof old.destination] !==
                operation.destination[key as keyof typeof operation.destination],
            ) ||
            !Callers.sameRecordedCaller(old.recorded_caller, operation.recorded_caller) ||
            old.kind !== operation.kind ||
            old.thread_environment_id !== operation.thread_environment_id ||
            old.thread_id !== operation.thread_id ||
            canonicalWorkstreamsSnapshotJson(old.origin) !==
              canonicalWorkstreamsSnapshotJson(operation.origin) ||
            canonicalWorkstreamsSnapshotJson(old.child_command_ids) !==
              canonicalWorkstreamsSnapshotJson(operation.child_command_ids)
          )
            return yield* new NativeSql.StoreError({ reason: "idempotency_conflict" });
          return old;
        }
        if (operation.recorded_caller) {
          const record = yield* Callers.decodeRecordedCaller(operation.recorded_caller).pipe(
            Effect.mapError(() => new NativeSql.StoreError({ reason: "invalid_payload" })),
          );
          if (
            canonicalWorkstreamsSnapshotJson(record.destination) !==
              canonicalWorkstreamsSnapshotJson(
                Object.fromEntries(
                  Object.keys(record.destination).map((key) => [
                    key,
                    operation.destination[key as keyof typeof operation.destination],
                  ]),
                ),
              ) ||
            canonicalWorkstreamsSnapshotJson(record.origin) !==
              canonicalWorkstreamsSnapshotJson(operation.origin)
          )
            return yield* new NativeSql.StoreError({ reason: "invalid_payload" });
        }
        yield* bindings.assertDestination(operation.destination);
        const origin = yield* decodeOrigin(operation.origin).pipe(
          Effect.mapError(() => new NativeSql.StoreError({ reason: "invalid_payload" })),
        );
        const requestSha256 = NodeCrypto.createHash("sha256")
          .update(operation.request_body)
          .digest("hex");
        const requestEnvelope = yield* db.sealJson(
          {
            ...operation.destination,
            key_scope: "local",
            purpose: "operation_request",
            record_id: operation.operation_id,
            revision: 1,
          },
          {
            version: 2,
            recorded_caller: operation.recorded_caller ?? null,
            request_body: operation.request_body,
            principal_id: operation.destination.principal_id,
          },
        );
        yield* sql`INSERT INTO jones_workstreams_operations ${sql.insert({ operation_id: operation.operation_id, kind: operation.kind, thread_environment_id: operation.thread_environment_id, thread_id: operation.thread_id, origin: canonicalWorkstreamsSnapshotJson(origin), request_sha256: requestSha256, dest_registry_instance_id: operation.destination.registry_instance_id, dest_owner_id: operation.destination.owner_id, dest_home_environment_id: operation.destination.home_environment_id, dest_home_generation: operation.destination.home_generation, binding_version: operation.destination.binding_version, state: "accepted", child_command_ids: canonicalWorkstreamsSnapshotJson(operation.child_command_ids), accepted_at: operation.accepted_at, updated_at: operation.accepted_at, next_poll_at: null, request_envelope: requestEnvelope, result_envelope: null })}`;
        return {
          ...operation,
          recorded_caller: operation.recorded_caller ?? null,
          state: "accepted" as const,
          request_sha256: requestSha256,
          updated_at: operation.accepted_at,
          next_poll_at: null,
          result_body: null,
        };
      }),
    ),
  );
  const advance = Effect.fn("OperationStore.advance")(
    (
      id: string,
      expectedState: OperationState,
      nextState: OperationState,
      updatedAt: string,
      nextPollAt: string | null,
      resultBody?: string,
    ) =>
      sql.withTransaction(
        Effect.gen(function* () {
          const old = yield* read(id);
          if (!old) return yield* new NativeSql.StoreError({ reason: "not_found" });
          if (old.state !== expectedState)
            return yield* new NativeSql.StoreError({ reason: "conflict" });
          return yield* db.transaction(
            old.destination,
            Effect.gen(function* () {
              if (nextState === "dispatching") yield* bindings.assertDestination(old.destination);
              const resultEnvelope =
                resultBody === undefined
                  ? null
                  : yield* db.sealJson(
                      {
                        ...old.destination,
                        key_scope: "local",
                        purpose: "operation_result",
                        record_id: id,
                        revision: 1,
                      },
                      resultBody,
                    );
              yield* sql`UPDATE jones_workstreams_operations SET state = ${nextState}, updated_at = ${updatedAt}, next_poll_at = ${nextPollAt}, result_envelope = coalesce(${resultEnvelope},result_envelope) WHERE operation_id = ${id} AND state = ${expectedState}`;
              const next = yield* read(id);
              if (!next) return yield* new NativeSql.StoreError({ reason: "not_found" });
              return next;
            }),
          );
        }),
      ),
  );
  const open = Effect.fn("OperationStore.open")(() =>
    sql.withTransaction(
      Effect.gen(function* () {
        const rows =
          yield* sql<Row>`SELECT * FROM jones_workstreams_operations WHERE state IN ('accepted','dispatching','in_progress','reconciling') ORDER BY accepted_at,operation_id COLLATE BINARY`;
        const result: Operation[] = [];
        for (const row of rows) result.push(yield* decodeRow(row));
        return result;
      }),
    ),
  );
  return OperationStore.of({ read, accept, advance, open });
});
/** @public Canonical service layer; open operations have no expiry and never retarget. */
export const layer = Layer.effect(OperationStore, make);
