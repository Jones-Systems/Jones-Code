import {
  AuthOrchestrationReadScope,
  ThreadId,
  VoiceReviewForbiddenError,
  VoiceReviewUnavailableError,
  type EnvironmentSessionPrincipalShape,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { ServerEnvironmentIdentity } from "../environment/ServerEnvironment.ts";
import * as ThreadMetadataReads from "../jones/orchestration/threadMetadataReads.ts";
import * as PersonalWorkstreams from "../jones/workstreams/personal/service.ts";
import { makeVoiceReviewNativeReadPort, type VoiceReviewNativeIdentity } from "./native.ts";
import type {
  VoiceReviewNativeReadPort,
  VoiceReviewPersonalMembership,
  VoiceReviewThreadIdentity,
} from "./bridge.ts";
import { type VoiceReviewConfig, type VoiceReviewNativeBinding } from "./config.ts";

export interface VoiceReviewCompositionInput {
  readonly binding: VoiceReviewNativeBinding | null;
  readonly reviewConfig: VoiceReviewConfig | null;
  readonly principal: EnvironmentSessionPrincipalShape;
}
export type VoiceReviewCompositionFactory = (
  input: VoiceReviewCompositionInput,
) => Effect.Effect<VoiceReviewNativeReadPort | undefined, VoiceReviewForbiddenError>;

export const makeVoiceReviewComposition = Effect.fn("voiceReview.makeComposition")(function* (
  input: VoiceReviewCompositionInput,
) {
  if (input.binding === null || input.reviewConfig === null) return undefined;
  if (!input.principal.scopes.has(AuthOrchestrationReadScope))
    return yield* new VoiceReviewForbiddenError({});
  const binding = input.binding;
  const identity = yield* ServerEnvironmentIdentity;
  const metadata = yield* ThreadMetadataReads.ThreadMetadataReads;
  const personal = yield* PersonalWorkstreams.PersonalWorkstreams;
  const environmentId = yield* identity.getEnvironmentId;
  if (environmentId !== binding.native_environment_id) return undefined;
  const ids = yield* metadata.listIds().pipe(Effect.orElseSucceed(() => undefined));
  if (ids === undefined) return undefined;
  const identities = ids.map((id): VoiceReviewNativeIdentity => ({
    thread_key: JSON.stringify([binding.registry_host, binding.registry_environment, id]),
    identity: { source_instance_id: environmentId, native_thread_id: id },
  }));
  return makeVoiceReviewNativeReadPort({
    readIdentities: () => identities,
    readThread: Effect.fn("VoiceReviewComposition.readPersonalMembership")(function* (
      request: VoiceReviewThreadIdentity,
    ) {
      const currentEnvironment = yield* identity.getEnvironmentId;
      if (currentEnvironment !== environmentId || request.source_instance_id !== currentEnvironment)
        return yield* new VoiceReviewUnavailableError({});
      const { assignment, workstream } = yield* personal
        .readThread(ThreadId.make(request.native_thread_id))
        .pipe(Effect.mapError(() => new VoiceReviewUnavailableError({})));
      if (workstream === null) return null;
      return {
        workstream_ref: `native:${workstream.id}`,
        workstream_id: workstream.id,
        name: workstream.name,
        environment_id: currentEnvironment,
        thread_id: assignment.threadId,
        revision: assignment.revision,
      } satisfies VoiceReviewPersonalMembership;
    }),
  });
});

export const makeVoiceReviewCompositionFactory = Effect.fn("voiceReview.makeCompositionFactory")(
  function* () {
    const identity = yield* ServerEnvironmentIdentity;
    const metadata = yield* ThreadMetadataReads.ThreadMetadataReads;
    const personal = yield* PersonalWorkstreams.PersonalWorkstreams;
    const factory: VoiceReviewCompositionFactory = (input) =>
      makeVoiceReviewComposition(input).pipe(
        Effect.provideService(ServerEnvironmentIdentity, identity),
        Effect.provideService(ThreadMetadataReads.ThreadMetadataReads, metadata),
        Effect.provideService(PersonalWorkstreams.PersonalWorkstreams, personal),
      );
    return factory;
  },
);
