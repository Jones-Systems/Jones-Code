import {
  AuthOrchestrationReadScope,
  VoiceReviewForbiddenError,
  VoiceReviewUnavailableError,
  type EnvironmentSessionPrincipalShape,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { ServerEnvironmentIdentity } from "../environment/ServerEnvironment.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import { WorkstreamGateway, WorkstreamGatewayError } from "../workstreams/WorkstreamGateway.ts";
import { makeVoiceReviewNativeReadPort, type VoiceReviewNativeIdentity } from "./native.ts";
import type { VoiceReviewNativeReadPort } from "./bridge.ts";
import type { VoiceReviewConfig, VoiceReviewNativeBinding } from "./config.ts";

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
  if (
    !input.reviewConfig.allowed_session_ids.has(input.principal.sessionId) ||
    !input.principal.scopes.has(AuthOrchestrationReadScope)
  )
    return yield* new VoiceReviewForbiddenError({});
  const binding = input.binding;
  const identity = yield* ServerEnvironmentIdentity;
  const projection = yield* ProjectionStore.ProjectionStoreV2;
  const gateway = yield* WorkstreamGateway;
  const readIdentities = Effect.fn("voiceReview.readNativeIdentities")(function* () {
    const environmentId = yield* identity.getEnvironmentId;
    if (environmentId !== binding.native_environment_id)
      return yield* new VoiceReviewUnavailableError({});
    const snapshot = yield* projection.getShellSnapshot();
    return snapshot.threads.map((thread): VoiceReviewNativeIdentity => ({
      thread_key: JSON.stringify([binding.registry_host, binding.registry_environment, thread.id]),
      identity: { source_instance_id: environmentId, native_thread_id: thread.id },
    }));
  });
  const initial = yield* readIdentities().pipe(Effect.catch(() => Effect.succeed(undefined)));
  if (initial === undefined) return undefined;
  return makeVoiceReviewNativeReadPort({
    readIdentities: () => initial,
    gateway: {
      readThreadPlacements: (request) =>
        Effect.gen(function* () {
          const fresh = yield* readIdentities().pipe(
            Effect.mapError(
              () =>
                new WorkstreamGatewayError({
                  reason: "offline",
                  detail: "Native voice binding is unavailable.",
                }),
            ),
          );
          const nativeIds = new Set(fresh.map((record) => record.identity.native_thread_id));
          if (
            request.identities.some(
              (item) =>
                item.source_instance_id !== binding.native_environment_id ||
                !nativeIds.has(item.native_thread_id),
            )
          )
            return yield* new WorkstreamGatewayError({
              reason: "stale",
              detail: "Native voice projection changed.",
            });
          return yield* gateway.readThreadPlacements(request);
        }).pipe(
          Effect.mapError(
            () =>
              new WorkstreamGatewayError({
                reason: "offline",
                detail: "Native voice binding is unavailable.",
              }),
          ),
        ),
    },
  });
});

export const makeVoiceReviewCompositionFactory = Effect.fn("voiceReview.makeCompositionFactory")(
  function* () {
    const identity = yield* ServerEnvironmentIdentity;
    const projection = yield* ProjectionStore.ProjectionStoreV2;
    const gateway = yield* WorkstreamGateway;
    const factory: VoiceReviewCompositionFactory = (input) =>
      makeVoiceReviewComposition(input).pipe(
        Effect.provideService(ServerEnvironmentIdentity, identity),
        Effect.provideService(ProjectionStore.ProjectionStoreV2, projection),
        Effect.provideService(WorkstreamGateway, gateway),
      );
    return factory;
  },
);
