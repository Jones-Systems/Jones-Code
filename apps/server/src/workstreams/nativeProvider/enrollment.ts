import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { WorkstreamsNativeBuild, WORKSTREAMS_T3_PROVIDER_PROTOCOL } from "@t3tools/contracts";

export const NATIVE_PROVIDER_SCOPES = {
  context: "workstreams:native:context",
  settlement: "workstreams:native:settlement",
  reconciliation: "workstreams:native:reconciliation",
} as const;

export const NativeProviderEnrollmentBinding = Schema.Struct({
  owner_id: Schema.String,
  principal_id: Schema.String,
  source_instance_id: Schema.String,
  authority_namespace: Schema.String,
  store_generation: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
  enrollment_id: Schema.String,
  protocol: Schema.Literal(WORKSTREAMS_T3_PROVIDER_PROTOCOL),
  build: WorkstreamsNativeBuild,
  session_id: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(128)),
  registry_origin: Schema.String.check(
    Schema.isMaxLength(2048),
    Schema.isPattern(/^https?:\/\/[^/?#]+$/),
  ),
  scopes: Schema.Array(Schema.Literals(Object.values(NATIVE_PROVIDER_SCOPES))).check(
    Schema.isMaxLength(3),
  ),
});
export type NativeProviderEnrollmentBinding = typeof NativeProviderEnrollmentBinding.Type;

export class NativeProviderBuild extends Context.Service<
  NativeProviderBuild,
  {
    readonly readCurrent: Effect.Effect<Option.Option<WorkstreamsNativeBuild>>;
  }
>()("t3/workstreams/nativeProvider/enrollment/NativeProviderBuild") {}

export class NativeProviderEnrollmentError extends Schema.TaggedError<NativeProviderEnrollmentError>()(
  "NativeProviderEnrollmentError",
  {},
) {}

// Only explicit enrollment may supply this lookup; ordinary browser sessions have no binding.
export class NativeProviderEnrollment extends Context.Service<
  NativeProviderEnrollment,
  {
    readonly getBySessionId: (
      sessionId: string,
    ) => Effect.Effect<
      Option.Option<NativeProviderEnrollmentBinding>,
      NativeProviderEnrollmentError
    >;
  }
>()("t3/workstreams/nativeProvider/enrollment/NativeProviderEnrollment") {}
