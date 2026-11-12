import type { RunAttemptId, RunId } from "@t3tools/contracts";
import type { ProviderAdapterV2Error } from "@t3tools/provider-core/server/ProviderAdapter";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export interface NativeSessionObservation {
  readonly runId: RunId;
  readonly attemptId: RunAttemptId;
  readonly nativeSessionId: string;
  readonly runtimeGeneration: string;
}

export interface NativeSessionRequirement {
  readonly runId: RunId;
  readonly attemptId: RunAttemptId;
  readonly nativeSessionId: string;
  readonly confirm: (
    input: NativeSessionObservation,
  ) => Effect.Effect<void, ProviderAdapterV2Error>;
}

export const RequiredNativeSession = Context.Reference<NativeSessionRequirement | undefined>(
  "jones/taskContinuation/RequiredNativeSession",
  { defaultValue: () => undefined },
);

export class NativeSessionIdentityRejected extends Schema.TaggedError<NativeSessionIdentityRejected>()(
  "NativeSessionIdentityRejected",
  {
    expected: Schema.String,
    observed: Schema.NullOr(Schema.String),
    reason: Schema.Literals([
      "destination_mismatch",
      "init_mismatch",
      "output_before_init",
      "unobserved_live_query",
    ]),
  },
) {
  override get message(): string {
    return `Claude native session identity was rejected (${this.reason}).`;
  }
}
