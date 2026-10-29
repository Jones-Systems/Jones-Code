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
      if (view.result?.kind === "operation" && view.result.result.state !== "unavailable")
        return view.result.result;
      // Existing public clients get a pollable state; raw transport consumers retain the native ForwardView.
      const request: WorkstreamOperationRequest = input.request;
      return {
        contract: "workstream-operations/1.0.0",
        operation_id: request.operation_id,
        kind: request.kind,
        origin: request.origin,
        request_sha256: NodeCrypto.createHash("sha256")
          .update(canonicalJson(request))
          .digest("hex"),
        observed: null,
        children: [],
        accepted_at: view.accepted_at,
        updated_at: view.updated_at,
        result: null,
        detail: null,
        state: "reconciling",
        reason:
          request.kind === "create_workstream"
            ? "create_effect_unknown"
            : "membership_effect_unknown",
        poll_after_ms: 30_000,
      } satisfies WorkstreamOperationResult;
    }),
  });
});
export const layer = Layer.effect(MemberOperations, make);
