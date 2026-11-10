import {
  WorkstreamOperationRequest,
  type WorkstreamOperationResult,
} from "@t3tools/contracts/jones/workstreamOperations";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Forwarder from "../forwarding/forwarder.ts";
import * as Admission from "../runtime/memberAdmission.ts";
import * as Store from "../store/operationStore.ts";
import { RegistryError } from "../registry/session.ts";
import { projectOperation } from "./memberOperations.ts";

export interface PublicOperation {
  readonly request: WorkstreamOperationRequest;
  readonly result: WorkstreamOperationResult;
}
export class MemberOperationRead extends Context.Service<
  MemberOperationRead,
  {
    readonly read: (id: string) => Effect.Effect<PublicOperation | null, RegistryError>;
    readonly open: () => Effect.Effect<readonly PublicOperation[], RegistryError>;
  }
>()("t3/jones/workstreams/native/operations/memberOperationRead") {}
const decodeRequest = Schema.decodeUnknownEffect(
  Schema.fromJsonString(WorkstreamOperationRequest),
  { onExcessProperty: "error" },
);
const make = Effect.gen(function* () {
  const store = yield* Store.OperationStore;
  const forwarder = yield* Forwarder.MemberForwarder;
  const admission = yield* Admission.MemberAdmission;
  const observe = Effect.fn("MemberOperationRead.observe")(function* (
    row: Store.MemberForwardOperation,
  ) {
    if (row.kind !== "set_primary" && row.kind !== "create_workstream") return null;
    // Normal admission compares this request's actual auth/session/run with the retained original.
    // Retained background admission must never select a public operation reader.
    const admitted = yield* admission.readmitWrite(row.member_forward.recorded_caller);
    yield* admitted.checkCurrent();
    const request = yield* decodeRequest(row.request_body).pipe(
      Effect.mapError(() => new RegistryError({ code: "invalid_response" })),
    );
    if (request.operation_id !== row.operation_id || request.kind !== row.kind)
      return yield* new RegistryError({ code: "invalid_response" });
    const view = yield* forwarder.read(row.operation_id);
    if (view === null) return yield* new RegistryError({ code: "unavailable" });
    const result = yield* projectOperation(request, view);
    yield* admitted.checkCurrent();
    return { request, result } satisfies PublicOperation;
  });
  return MemberOperationRead.of({
    read: Effect.fn("MemberOperationRead.read")(function* (id) {
      const row = yield* store
        .readMemberForward(id)
        .pipe(Effect.mapError(() => new RegistryError({ code: "unavailable" })));
      return row === null ? null : yield* observe(row);
    }),
    open: Effect.fn("MemberOperationRead.open")(function* () {
      const rows = yield* store
        .openMemberForwards()
        .pipe(Effect.mapError(() => new RegistryError({ code: "unavailable" })));
      const visible: PublicOperation[] = [];
      for (const row of rows) {
        // Other callers' operations are filtered before decoding/releasing their public request.
        const allowed = yield* admission.readmitWrite(row.member_forward.recorded_caller).pipe(
          Effect.as(true),
          Effect.catch((error) =>
            error.code === "forbidden" ? Effect.succeed(false) : Effect.fail(error),
          ),
        );
        if (!allowed) continue;
        const value = yield* observe(row);
        if (
          value !== null &&
          (value.result.state === "in_progress" || value.result.state === "reconciling")
        )
          visible.push(value);
        if (visible.length > 100) return yield* new RegistryError({ code: "unavailable" });
      }
      return visible;
    }),
  });
});
export const layer = Layer.effect(MemberOperationRead, make);
