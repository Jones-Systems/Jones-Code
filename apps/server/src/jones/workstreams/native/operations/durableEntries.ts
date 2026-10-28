import { WorkstreamCommand } from "@t3tools/contracts";
import {
  WorkstreamOperationRequest,
  WorkstreamOperationResult,
} from "@t3tools/contracts/jones/workstreamOperations";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Callers from "../runtime/recordedCaller.ts";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { canonicalJson } from "../../operations/encoding.ts";
import * as BindingStore from "../store/bindingStore.ts";
import * as OperationStore from "../store/operationStore.ts";

const PendingChild = Schema.Struct({
  command: WorkstreamCommand,
  body: Schema.String,
  digest: Schema.String,
  kind: Schema.Literals(["mem", "cre", "reg", "ver"]),
  ownerId: Schema.String,
  principalId: Schema.String,
});
export const Checkpoint = Schema.Struct({
  version: Schema.Literal(1),
  result: WorkstreamOperationResult,
  epoch: Schema.optional(Schema.Number),
  firstAbsent: Schema.optional(Schema.Number),
  regAbsent: Schema.Boolean,
  unknown: Schema.optional(PendingChild),
});
export type Checkpoint = typeof Checkpoint.Type;
export interface DurableEntry {
  readonly operation: OperationStore.Operation;
  readonly request: WorkstreamOperationRequest;
  readonly checkpoint: Checkpoint | null;
}
export class DurableEntryError extends Schema.TaggedError<DurableEntryError>()(
  "DurableEntryError",
  {
    code: Schema.Literals([
      "unavailable",
      "invalid_checkpoint",
      "operation_id_conflict",
      "thread_operation_in_progress",
    ]),
    open_operation_id: Schema.optional(Schema.String),
  },
) {}
export class DurableEntries extends Context.Service<
  DurableEntries,
  {
    readonly read: (id: string) => Effect.Effect<DurableEntry | null, DurableEntryError>;
    readonly open: () => Effect.Effect<readonly DurableEntry[], DurableEntryError>;
    readonly accept: (
      request: WorkstreamOperationRequest,
      childIds: readonly string[],
      acceptedAt: string,
    ) => Effect.Effect<DurableEntry, DurableEntryError>;
    readonly save: (
      id: string,
      checkpoint: Checkpoint,
      dispatch?: boolean,
    ) => Effect.Effect<OperationStore.Operation, DurableEntryError>;
  }
>()("t3/jones/workstreams/native/operations/durableEntries") {}

const make = Effect.gen(function* () {
  const operations = yield* OperationStore.OperationStore;
  const bindings = yield* BindingStore.BindingStore;
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const decodeRequest = Schema.decodeUnknownEffect(
    Schema.fromJsonString(WorkstreamOperationRequest),
  );
  const decodeCheckpoint = Schema.decodeUnknownEffect(Schema.fromJsonString(Checkpoint));
  const decode = Effect.fn("DurableEntries.decode")(function* (
    operation: OperationStore.Operation,
  ) {
    const request = yield* decodeRequest(operation.request_body).pipe(
      Effect.mapError(() => new DurableEntryError({ code: "invalid_checkpoint" })),
    );
    const checkpoint =
      operation.result_body === null
        ? null
        : yield* decodeCheckpoint(operation.result_body).pipe(
            Effect.mapError(() => new DurableEntryError({ code: "invalid_checkpoint" })),
          );
    const registry = request.kind === "create_workstream" ? request.registry : request.target;
    if (
      request.operation_id !== operation.operation_id ||
      request.kind !== operation.kind ||
      canonicalJson(request) !== operation.request_body ||
      canonicalJson(request.origin) !== canonicalJson(operation.origin) ||
      registry.registry_id !== operation.destination.registry_instance_id ||
      registry.owner_id !== operation.destination.owner_id ||
      (request.kind === "set_primary" &&
        (request.thread.environment_id !== operation.thread_environment_id ||
          request.thread.thread_id !== operation.thread_id)) ||
      (checkpoint &&
        (checkpoint.result.operation_id !== operation.operation_id ||
          checkpoint.result.request_sha256 !== operation.request_sha256 ||
          checkpoint.result.kind !== request.kind ||
          canonicalJson(checkpoint.result.origin) !== canonicalJson(request.origin)))
    )
      return yield* new DurableEntryError({ code: "invalid_checkpoint" });
    if (checkpoint?.unknown) {
      const child = checkpoint.unknown;
      const hash = yield* crypto.digest("SHA-256", new TextEncoder().encode(child.body)).pipe(
        Effect.map(Hex.encode),
        Effect.mapError(() => new DurableEntryError({ code: "unavailable" })),
      );
      if (
        canonicalJson(child.command) !== child.body ||
        hash !== child.digest ||
        !operation.child_command_ids.includes(child.command.command_id) ||
        child.command.expected_server_generation !== operation.destination.home_generation ||
        child.principalId !== operation.destination.principal_id ||
        child.ownerId !== operation.destination.owner_id
      )
        return yield* new DurableEntryError({ code: "invalid_checkpoint" });
    }
    return { operation, request, checkpoint } satisfies DurableEntry;
  });
  const readUsing = Effect.fn("DurableEntries.read")(function* (
    prepared: OperationStore.PreparedOperations,
    id: string,
  ) {
    const row = yield* prepared
      .read(id)
      .pipe(Effect.mapError(() => new DurableEntryError({ code: "unavailable" })));
    return row === null ? null : yield* decode(row);
  });
  const openUsing = Effect.fn("DurableEntries.open")(function* (
    prepared: OperationStore.PreparedOperations,
  ) {
    const rows = yield* prepared
      .open()
      .pipe(Effect.mapError(() => new DurableEntryError({ code: "unavailable" })));
    return yield* Effect.forEach(
      rows.filter((row) => row.kind === "set_primary" || row.kind === "create_workstream"),
      decode,
    );
  });
  const read = (id: string) => readUsing(operations, id);
  const open = () => openUsing(operations);
  const accept = Effect.fn("DurableEntries.accept")(
    (request: WorkstreamOperationRequest, childIds: readonly string[], acceptedAt: string) =>
      operations
        .withPrepared(
          [
            {
              kind: "accept",
              id: request.operation_id,
              destination: {
                registry_instance_id: (request.kind === "create_workstream"
                  ? request.registry
                  : request.target
                ).registry_id,
                owner_id: (request.kind === "create_workstream" ? request.registry : request.target)
                  .owner_id,
              },
            },
            ...(request.kind === "set_primary" ? [{ kind: "open" as const }] : []),
          ],
          (prepared) =>
            sql.withTransaction(
              Effect.gen(function* () {
                // Capture is part of acceptance, including duplicate ownership, never a later checkpoint.
                const callers = yield* Effect.serviceOption(Callers.RecordedCallers);
                const recordedCaller = Option.isSome(callers)
                  ? yield* callers.value
                      .captureCaller()
                      .pipe(Effect.mapError(() => new DurableEntryError({ code: "unavailable" })))
                  : null;
                if (recordedCaller) {
                  const caller = yield* callers.pipe(
                    Option.match({
                      onNone: () => Effect.fail(new DurableEntryError({ code: "unavailable" })),
                      onSome: (value) =>
                        value
                          .readmitRecorded(recordedCaller)
                          .pipe(
                            Effect.mapError(() => new DurableEntryError({ code: "unavailable" })),
                          ),
                    }),
                  );
                  yield* caller
                    .check(
                      "workstreams:write",
                      request.kind === "set_primary" ? request.thread.thread_id : null,
                    )
                    .pipe(Effect.mapError(() => new DurableEntryError({ code: "unavailable" })));
                }
                const body = canonicalJson(request);
                const old = yield* readUsing(prepared, request.operation_id);
                if (old) {
                  if (
                    !Callers.sameRecordedCaller(old.operation.recorded_caller, recordedCaller) ||
                    old.operation.request_body !== body ||
                    canonicalJson(old.operation.child_command_ids) !== canonicalJson(childIds)
                  )
                    return yield* new DurableEntryError({ code: "operation_id_conflict" });
                  return old;
                }
                if (request.kind === "set_primary") {
                  const locked = (yield* openUsing(prepared)).find(
                    (e) =>
                      e.operation.thread_environment_id === request.thread.environment_id &&
                      e.operation.thread_id === request.thread.thread_id,
                  );
                  if (locked)
                    return yield* new DurableEntryError({
                      code: "thread_operation_in_progress",
                      open_operation_id: locked.operation.operation_id,
                    });
                }
                const destination = yield* bindings
                  .read()
                  .pipe(Effect.mapError(() => new DurableEntryError({ code: "unavailable" })));
                const registry =
                  request.kind === "create_workstream" ? request.registry : request.target;
                if (
                  !destination ||
                  destination.state !== "active" ||
                  destination.registry_instance_id !== registry.registry_id ||
                  destination.owner_id !== registry.owner_id
                )
                  return yield* new DurableEntryError({ code: "unavailable" });
                const accepted = yield* prepared
                  .accept({
                    operation_id: request.operation_id,
                    kind: request.kind,
                    thread_environment_id:
                      request.kind === "set_primary" ? request.thread.environment_id : null,
                    thread_id: request.kind === "set_primary" ? request.thread.thread_id : null,
                    origin: request.origin,
                    destination,
                    child_command_ids: childIds,
                    request_body: body,
                    accepted_at: acceptedAt,
                    recorded_caller: recordedCaller,
                  })
                  .pipe(
                    Effect.mapError(() => new DurableEntryError({ code: "unavailable" })),
                    Effect.flatMap(decode),
                  );
                if (Option.isSome(callers) && recordedCaller) {
                  const caller = yield* callers.value
                    .readmitRecorded(recordedCaller)
                    .pipe(Effect.mapError(() => new DurableEntryError({ code: "unavailable" })));
                  yield* caller
                    .check(
                      "workstreams:write",
                      request.kind === "set_primary" ? request.thread.thread_id : null,
                    )
                    .pipe(Effect.mapError(() => new DurableEntryError({ code: "unavailable" })));
                }
                return accepted;
              }),
            ),
        )
        .pipe(
          Effect.mapError((error) =>
            error._tag === "DurableEntryError"
              ? error
              : new DurableEntryError({ code: "unavailable" }),
          ),
        ),
  );
  const save = Effect.fn("DurableEntries.save")(
    (id: string, checkpoint: Checkpoint, dispatch = false) =>
      operations
        .withPrepared([{ kind: "advance", id, resultBodySupplied: true }], (prepared) =>
          sql.withTransaction(
            Effect.gen(function* () {
              let old = yield* prepared.read(id);
              if (!old) return yield* new DurableEntryError({ code: "invalid_checkpoint" });
              const result = checkpoint.result;
              const body = canonicalJson(checkpoint);
              yield* decode({ ...old, result_body: body });
              const pollAt =
                result.poll_after_ms === null
                  ? null
                  : DateTime.formatIso(
                      DateTime.makeUnsafe(Date.parse(result.updated_at) + result.poll_after_ms),
                    );
              if (old.state === "accepted" && result.state === "in_progress" && !dispatch)
                return old;
              if (
                old.state === "accepted" &&
                (dispatch || !["unavailable", "rejected", "conflict"].includes(result.state))
              ) {
                old = yield* prepared.advance(
                  id,
                  "accepted",
                  "dispatching",
                  result.updated_at,
                  pollAt,
                  body,
                );
                if (dispatch) return old;
              }
              if (dispatch) yield* bindings.assertDestination(old.destination);
              const next = dispatch ? "in_progress" : result.state;
              return yield* prepared.advance(id, old.state, next, result.updated_at, pollAt, body);
            }),
          ),
        )
        .pipe(
          Effect.mapError((error) =>
            error._tag === "DurableEntryError"
              ? error
              : new DurableEntryError({ code: "unavailable" }),
          ),
        ),
  );
  return DurableEntries.of({ read, open, accept, save });
});

export const layer = Layer.effect(DurableEntries, make);
