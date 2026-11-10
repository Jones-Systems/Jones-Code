import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type { WorkstreamsNativeSettlementRequest } from "@t3tools/contracts";
import type { NativeProviderEnrollmentBinding } from "./enrollment.ts";

export class NativeProviderDispatchRefused extends Schema.TaggedError<NativeProviderDispatchRefused>()(
  "NativeProviderDispatchRefused",
  {},
) {}

/** Request-local original-caller check. Legacy calls retain their existing admission owner. */
export const NativeProviderDispatchAdmission = Context.Reference<
  | ((
      binding: NativeProviderEnrollmentBinding,
      request: WorkstreamsNativeSettlementRequest,
    ) => Effect.Effect<void, NativeProviderDispatchRefused>)
  | undefined
>("t3/jones/workstreams/nativeProvider/dispatchAdmission/NativeProviderDispatchAdmission", {
  defaultValue: () => undefined,
});
