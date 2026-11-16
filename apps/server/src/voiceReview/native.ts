import {
  AuthOrchestrationReadScope,
  EnvironmentAuthenticatedPrincipal,
  VoiceReviewUnavailableError,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { requireEnvironmentScope } from "../auth/http.ts";
import type {
  VoiceReviewNativeReadPort,
  VoiceReviewPersonalMembership,
  VoiceReviewThreadIdentity,
} from "./bridge.ts";

export interface VoiceReviewNativeIdentity {
  readonly thread_key: string;
  readonly identity: VoiceReviewThreadIdentity;
}
export interface VoiceReviewNativeComposition {
  readonly readThread: (
    identity: VoiceReviewThreadIdentity,
  ) => Effect.Effect<VoiceReviewPersonalMembership | null, VoiceReviewUnavailableError>;
  // Only the local projection and explicit deployment binding supply these identities.
  readonly readIdentities: () => readonly VoiceReviewNativeIdentity[];
}

const identityKey = (identity: VoiceReviewThreadIdentity) =>
  JSON.stringify([identity.source_instance_id, identity.native_thread_id]);
const readProjection = (read: VoiceReviewNativeComposition["readIdentities"]) => {
  const result = new Map<string, VoiceReviewThreadIdentity>();
  const reverse = new Set<string>();
  for (const record of read()) {
    const key: unknown = JSON.parse(record.thread_key);
    const identity = record.identity;
    if (
      !Array.isArray(key) ||
      key.length !== 3 ||
      !key.every((part) => typeof part === "string" && part.length > 0) ||
      JSON.stringify(key) !== record.thread_key ||
      result.has(record.thread_key) ||
      !identity.source_instance_id ||
      !identity.native_thread_id
    )
      throw new VoiceReviewUnavailableError({});
    const nativeKey = identityKey(identity);
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
        const trusted = new Set(Array.from(projection.values(), identityKey));
        const seen = new Set<string>();
        for (const identity of identities) {
          const key = identityKey(identity);
          if (!trusted.has(key) || seen.has(key)) return yield* new VoiceReviewUnavailableError({});
          seen.add(key);
        }
        const memberships = yield* Effect.forEach(identities, (identity) =>
          composition.readThread(identity),
        );
        return memberships.filter((item) => item !== null);
      }).pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal)),
    ),
});
