// @effect-diagnostics nodeBuiltinImport:off -- Public-v1 operation digests remain canonical, independently of RAW forwarding intent.
import * as NodeCrypto from "node:crypto";
import type {
  WorkstreamOperationRequest,
  WorkstreamOperationResult,
  WorkstreamOperationSubmitRequest,
} from "@t3tools/contracts/jones/workstreamOperations";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type { WorkstreamsNativeSuppliedAttestation } from "@t3tools/contracts/jones/workstreamsNative";
import {
  WorkstreamOperationRequest as RequestSchema,
  WorkstreamOperationResult as WorkstreamOperationResultSchema,
  type WorkstreamSetPrimaryRequest,
} from "@t3tools/contracts/jones/workstreamOperations";
import { canonicalJson } from "../../operations/encoding.ts";
import { RegistryError } from "../registry/session.ts";
import * as Forwarder from "../forwarding/forwarder.ts";

/** Root binds the genuine source provider to this request-scoped port before mounting set_primary. */
export class MemberOperationEvidence extends Context.Service<
  MemberOperationEvidence,
  {
    readonly capture: (
      request: WorkstreamSetPrimaryRequest,
    ) => Effect.Effect<typeof WorkstreamsNativeSuppliedAttestation.Type, RegistryError>;
  }
>()("t3/jones/workstreams/native/operations/memberOperations/MemberOperationEvidence") {}
const decodeRequestBody = Schema.decodeUnknownEffect(Schema.fromJsonString(RequestSchema));

export class MemberOperations extends Context.Service<
  MemberOperations,
  {
    readonly submit: (
      input: WorkstreamOperationSubmitRequest,
    ) => Effect.Effect<WorkstreamOperationResult, RegistryError>;
    readonly submitBody: (body: string) => Effect.Effect<Forwarder.ForwardView, RegistryError>;
    readonly read: (id: string) => Effect.Effect<Forwarder.ForwardView | null, RegistryError>;
    readonly open: () => Effect.Effect<readonly Forwarder.ForwardView[], RegistryError>;
  }
>()("t3/jones/workstreams/native/operations/memberOperations") {}
const decodeResult = Schema.decodeUnknownEffect(WorkstreamOperationResultSchema, {
  onExcessProperty: "error",
});
/** Project only an actually accepted native intent; null replies never become absence authority. */
export const projectOperation = Effect.fn("MemberOperations.projectOperation")(function* (
  request: WorkstreamOperationRequest,
  view: Forwarder.ForwardView,
) {
  const digest = NodeCrypto.createHash("sha256").update(canonicalJson(request)).digest("hex");
  if (
    view.operation_id !== request.operation_id ||
    (view.result !== null && view.result.kind !== "operation")
  )
    return yield* new RegistryError({ code: "invalid_response" });
  if (view.result?.kind === "operation") {
    const result = yield* decodeResult(view.result.result).pipe(
      Effect.mapError(() => new RegistryError({ code: "invalid_response" })),
    );
    if (
      result.operation_id !== request.operation_id ||
      result.kind !== request.kind ||
      canonicalJson(result.origin) !== canonicalJson(request.origin) ||
      result.request_sha256 !== digest
    )
      return yield* new RegistryError({ code: "invalid_response" });
    if (result.state !== "unavailable") return result;
  }
  if (!["accepted", "dispatching", "in_progress", "reconciling"].includes(view.state))
    return yield* new RegistryError({ code: "invalid_response" });
  return yield* decodeResult({
    contract: "workstream-operations/1.0.0",
    operation_id: request.operation_id,
    kind: request.kind,
    origin: request.origin,
    request_sha256: digest,
    observed: null,
    children: [],
    accepted_at: view.accepted_at,
    updated_at: view.updated_at,
    result: null,
    detail: null,
    state: "reconciling",
    reason:
      request.kind === "create_workstream" ? "create_effect_unknown" : "membership_effect_unknown",
    poll_after_ms: 30_000,
  }).pipe(Effect.mapError(() => new RegistryError({ code: "invalid_response" })));
});
const make = Effect.gen(function* () {
  const forwarder = yield* Forwarder.MemberForwarder;
  const evidence = yield* Effect.serviceOption(MemberOperationEvidence);
  const submitBody = Effect.fn("MemberOperations.submitBody")(function* (body: string) {
    const request = yield* decodeRequestBody(body, { onExcessProperty: "error" }).pipe(
      Effect.mapError(() => new RegistryError({ code: "invalid_request" })),
    );
    // A replay retains its original verification envelope; it never asks the provider for fresh evidence.
    if (
      (yield* forwarder.read(request.operation_id)) !== null ||
      request.kind === "create_workstream"
    )
      return yield* forwarder.submit({ kind: "operation", body });
    if (Option.isNone(evidence)) return yield* new RegistryError({ code: "provider_unconfigured" });
    const proof = yield* evidence.value.capture(request);
    return yield* forwarder.submit({ kind: "operation", body, evidence: proof });
  });
  return MemberOperations.of({
    submitBody,
    read: forwarder.read,
    open: forwarder.open,
    submit: Effect.fn("MemberOperations.submit")(function* (input) {
      if (!Number.isInteger(input.wait_ms) || input.wait_ms < 0 || input.wait_ms > 3000)
        return yield* new RegistryError({ code: "invalid_request" });
      const view = yield* submitBody(canonicalJson(input.request));
      return yield* projectOperation(input.request, view);
    }),
  });
});
export const layer = Layer.effect(MemberOperations, make);
