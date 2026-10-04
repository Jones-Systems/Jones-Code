import {
  AuthOrchestrationReadScope,
  EnvironmentAuthenticatedPrincipal,
  T3PlacementIdentity,
  T3PlacementResult,
  t3PlacementIdentityKey,
  VoiceReviewUnavailableError,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { requireEnvironmentScope } from "../auth/http.ts";
import type { WorkstreamGateway } from "../workstreams/WorkstreamGateway.ts";
import type { VoiceReviewNativeReadPort } from "./bridge.ts";

export interface VoiceReviewNativeIdentity {
  readonly thread_key: string;
  readonly identity: typeof T3PlacementIdentity.Type;
}
export interface VoiceReviewNativeComposition {
  readonly gateway: Pick<WorkstreamGateway["Service"], "readThreadPlacements">;
  // The native projection owns this binding; Voice registration or summary fields cannot supply it.
  readonly readIdentities: () => readonly VoiceReviewNativeIdentity[];
  readonly now?: () => number;
}

export const validateVoiceReviewNativePlacementResult = (
  input: typeof T3PlacementResult.Type,
  now: number,
): typeof T3PlacementResult.Type => {
  const result = Schema.decodeUnknownSync(T3PlacementResult)(input);
  const environments = new Map(
    result.trustedEnvironments.map(
      (environment) => [environment.environmentId, environment] as const,
    ),
  );
  if (
    result.readiness !== "ready" ||
    !Number.isFinite(now) ||
    environments.size !== result.trustedEnvironments.length
  )
    throw new VoiceReviewUnavailableError({});
  for (const placement of result.page.items) {
    const trust = environments.get(placement.source_instance_id);
    if (
      trust === undefined ||
      trust.authorityNamespace !== placement.authority_namespace ||
      trust.storeGeneration !== placement.store_generation ||
      Date.parse(placement.attested_at) > now ||
      Date.parse(placement.expires_at) <= now ||
      Date.parse(placement.expires_at) <= Date.parse(placement.attested_at)
    )
      throw new VoiceReviewUnavailableError({});
  }
  return result;
};

const decodeIdentity = Schema.decodeUnknownSync(T3PlacementIdentity);
const readProjection = (read: VoiceReviewNativeComposition["readIdentities"]) => {
  const result = new Map<string, typeof T3PlacementIdentity.Type>();
  const reverse = new Set<string>();
  for (const record of read()) {
    const key: unknown = JSON.parse(record.thread_key);
    if (
      !Array.isArray(key) ||
      key.length !== 3 ||
      !key.every((part) => typeof part === "string" && part.length > 0) ||
      JSON.stringify(key) !== record.thread_key ||
      result.has(record.thread_key)
    )
      throw new VoiceReviewUnavailableError({});
    const identity = decodeIdentity(record.identity);
    const nativeKey = t3PlacementIdentityKey(identity);
    if (reverse.has(nativeKey)) throw new VoiceReviewUnavailableError({});
    reverse.add(nativeKey);
    result.set(record.thread_key, identity);
  }
  return result;
};

export const makeVoiceReviewNativeReadPort = (
  composition: VoiceReviewNativeComposition,
): VoiceReviewNativeReadPort => ({
  identities: (threads) => {
    const projection = readProjection(composition.readIdentities);
    return new Map(
      threads.flatMap((thread) => {
        const identity = projection.get(thread.thread_key);
        return identity === undefined ? [] : [[thread.thread_key, identity] as const];
      }),
    );
  },
  read: (principal, identities) =>
    Effect.runPromise(
      Effect.gen(function* () {
        yield* requireEnvironmentScope(AuthOrchestrationReadScope);
        const projection = readProjection(composition.readIdentities);
        const trusted = new Set(Array.from(projection.values(), t3PlacementIdentityKey));
        const seen = new Set<string>();
        for (const identity of identities) {
          const key = t3PlacementIdentityKey(decodeIdentity(identity));
          if (!trusted.has(key) || seen.has(key)) return yield* new VoiceReviewUnavailableError({});
          seen.add(key);
        }
        const result = yield* composition.gateway.readThreadPlacements({ identities });
        return validateVoiceReviewNativePlacementResult(result, (composition.now ?? Date.now)());
      }).pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal)),
    ),
});
