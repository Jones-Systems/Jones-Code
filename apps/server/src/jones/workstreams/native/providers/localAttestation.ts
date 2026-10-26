import {
  WORKSTREAMS_T3_PROVIDER_ATTESTATION_TTL_MS,
  WorkstreamsNativeAttestationRequest,
  WorkstreamsNativeAttestationResponseV2,
  type WorkstreamsNativeContextResponseV2,
} from "@t3tools/contracts";
import type { WorkstreamsNativeHomeTarget } from "@t3tools/contracts/jones/workstreamsNative";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import type { NativeProviderEnrollmentBinding } from "../../nativeProvider/enrollment.ts";
import { sha256Bytes, type WorkstreamsNativeProvider } from "../../nativeProvider/service.ts";

export class ProviderError extends Schema.TaggedError<ProviderError>()("WorkstreamsProviderError", {
  code: Schema.Literals([
    "provider_unconfigured",
    "provider_unavailable",
    "provider_mismatch",
    "generation_mismatch",
    "registry_mismatch",
    "not_granted",
    "fenced",
  ]),
  cause: Schema.optionalKey(Schema.Defect()),
}) {
  override get message(): string {
    return `Workstreams provider: ${this.code}.`;
  }
}

export interface SourceCall {
  readonly expected_home: WorkstreamsNativeHomeTarget;
}

// The source adapter must check its singleton binding, authenticated caller and limits.
// It returns the actual source-issued enrollment; adapters never manufacture one.
export class SourceAdmission extends Context.Service<
  SourceAdmission,
  {
    readonly authorize: (
      input: SourceCall & {
        readonly operation: "context" | "attest" | "settle" | "settle_lookup";
        readonly thread_id: string | null;
      },
    ) => Effect.Effect<NativeProviderEnrollmentBinding, ProviderError>;
  }
>()("t3/jones/workstreams/native/providers/localAttestation/SourceAdmission") {}

export class NativeProvider extends Context.Service<
  NativeProvider,
  WorkstreamsNativeProvider<"v2">
>()("t3/jones/workstreams/native/providers/localAttestation/NativeProvider") {}

export class SourceAttestation extends Context.Service<
  SourceAttestation,
  {
    readonly context: (
      input: SourceCall,
    ) => Effect.Effect<WorkstreamsNativeContextResponseV2, ProviderError>;
    readonly attest: (
      input: SourceCall & {
        readonly request: WorkstreamsNativeAttestationRequest;
      },
    ) => Effect.Effect<WorkstreamsNativeAttestationResponseV2, ProviderError>;
  }
>()("t3/jones/workstreams/native/providers/localAttestation/SourceAttestation") {}

const decodeAttestation = Schema.decodeUnknownEffect(WorkstreamsNativeAttestationResponseV2);
const encodeAttestationRequest = Schema.encodeEffect(
  Schema.fromJsonString(WorkstreamsNativeAttestationRequest),
);

export const validateAttestation = Effect.fn("SourceAttestation.validate")(function* (
  request: WorkstreamsNativeAttestationRequest,
  value: unknown,
) {
  const response = yield* decodeAttestation(value).pipe(
    Effect.mapError((cause) => new ProviderError({ code: "provider_mismatch", cause })),
  );
  if (response.state !== "attested") return response;
  const expected = yield* encodeAttestationRequest(request).pipe(Effect.orDie);
  const actual = yield* encodeAttestationRequest(response.request).pipe(Effect.orDie);
  const now = DateTime.toEpochMillis(yield* DateTime.now);
  const at = Date.parse(response.attested_at);
  const expires = Date.parse(response.expires_at);
  const evidence = response.attestation;
  if (
    actual !== expected ||
    evidence.provider !== "t3" ||
    evidence.source_instance_id !== request.identity.source_instance_id ||
    evidence.native_id !== request.identity.native_id ||
    evidence.authority_namespace !== request.expected_authority_namespace ||
    evidence.store_generation !== request.expected_store_generation ||
    at > now ||
    expires <= now ||
    expires <= at ||
    expires - at > WORKSTREAMS_T3_PROVIDER_ATTESTATION_TTL_MS ||
    evidence.evidence_sha256 !==
      sha256Bytes(`${expected}\n${response.attested_at}\n${response.expires_at}`)
  )
    return yield* new ProviderError({ code: "provider_mismatch" });
  return response;
});

const make = Effect.gen(function* () {
  const admission = yield* SourceAdmission;
  const provider = yield* NativeProvider;
  return SourceAttestation.of({
    context: Effect.fn("SourceAttestation.context")(function* (input) {
      const binding = yield* admission.authorize({
        ...input,
        operation: "context",
        thread_id: null,
      });
      return yield* provider.context(binding);
    }),
    attest: Effect.fn("SourceAttestation.attest")(function* (input) {
      const binding = yield* admission.authorize({
        expected_home: input.expected_home,
        operation: "attest",
        thread_id: input.request.identity.native_id,
      });
      return yield* validateAttestation(
        input.request,
        yield* provider.attest(binding, input.request),
      );
    }),
  });
});

export const layer = Layer.effect(SourceAttestation, make);
export const unconfiguredLayer = Layer.succeed(SourceAttestation, {
  context: () => Effect.fail(new ProviderError({ code: "provider_unconfigured" })),
  attest: () => Effect.fail(new ProviderError({ code: "provider_unconfigured" })),
});
