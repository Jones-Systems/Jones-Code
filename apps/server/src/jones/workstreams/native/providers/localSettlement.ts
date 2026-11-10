import {
  WorkstreamsNativeSettlementRequest,
  type WorkstreamsNativeSettlementResponseV2,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { NATIVE_PROVIDER_SCOPES } from "../../nativeProvider/enrollment.ts";
import { NativeProviderAttempts } from "../../nativeProvider/attemptRepository.ts";
import { canonicalJson } from "../../operations/encoding.ts";
import { sha256Bytes } from "../../nativeProvider/service.ts";
import {
  NativeProvider,
  ProviderError,
  SourceAdmission,
  type SourceCall,
} from "./localAttestation.ts";

export interface SettlementCall extends SourceCall {
  readonly request: WorkstreamsNativeSettlementRequest;
}

export class SourceSettlement extends Context.Service<
  SourceSettlement,
  {
    readonly settle: (
      input: SettlementCall,
    ) => Effect.Effect<WorkstreamsNativeSettlementResponseV2, ProviderError>;
    readonly lookup: (
      input: SettlementCall,
    ) => Effect.Effect<WorkstreamsNativeSettlementResponseV2, ProviderError>;
  }
>()("t3/jones/workstreams/native/providers/localSettlement/SourceSettlement") {}

const encodeSettlementRequest = Schema.encodeEffect(
  Schema.fromJsonString(WorkstreamsNativeSettlementRequest),
);

const make = Effect.gen(function* () {
  const admission = yield* SourceAdmission;
  const provider = yield* NativeProvider;
  const attempts = yield* NativeProviderAttempts;
  const digest = (request: WorkstreamsNativeSettlementRequest) =>
    encodeSettlementRequest(request).pipe(
      Effect.map(() => sha256Bytes(canonicalJson(request))),
      Effect.mapError((cause) => new ProviderError({ code: "provider_mismatch", cause })),
    );
  const lookup = Effect.fn("SourceSettlement.lookup")(function* (input: SettlementCall) {
    const binding = yield* admission.authorize({
      expected_home: input.expected_home,
      operation: "settle_lookup",
      thread_id: input.request.identity.native_id,
    });
    if (!binding.scopes.includes(NATIVE_PROVIDER_SCOPES.reconciliation))
      return yield* new ProviderError({ code: "not_granted" });
    const recorded = yield* attempts
      .get(input.request)
      .pipe(Effect.mapError((cause) => new ProviderError({ code: "provider_unavailable", cause })));
    if (Option.isNone(recorded))
      return yield* provider.lookup(binding, input.request, yield* digest(input.request));
    const attempt = recorded.value;
    const original = attempt.enrollment;
    if (original.registry_origin !== binding.registry_origin)
      return yield* new ProviderError({ code: "registry_mismatch" });
    if (original.owner_id !== binding.owner_id || original.principal_id !== binding.principal_id)
      return yield* new ProviderError({ code: "not_granted" });
    if (
      original.source_instance_id !== binding.source_instance_id ||
      original.authority_namespace !== binding.authority_namespace ||
      original.store_generation !== binding.store_generation
    )
      return yield* new ProviderError({ code: "generation_mismatch" });
    // Recovery keeps the source's retained enrollment and exact request bytes even after session renewal.
    // The provider still verifies immutable request association and current authority; this path cannot dispatch.
    return yield* provider.lookup(original, input.request, attempt.requestBytesSha256);
  });
  return SourceSettlement.of({
    lookup,
    settle: Effect.fn("SourceSettlement.settle")(function* (input) {
      const previous = yield* lookup(input);
      if (previous.state !== "unknown" || previous.reason !== "attempt_not_found") return previous;
      const binding = yield* admission.authorize({
        expected_home: input.expected_home,
        operation: "settle",
        thread_id: input.request.identity.native_id,
      });
      return yield* provider.settle(binding, input.request, yield* digest(input.request));
    }),
  });
});

export const layer = Layer.effect(SourceSettlement, make);
export const unconfiguredLayer = Layer.succeed(SourceSettlement, {
  settle: () => Effect.fail(new ProviderError({ code: "provider_unconfigured" })),
  lookup: () => Effect.fail(new ProviderError({ code: "provider_unconfigured" })),
});
