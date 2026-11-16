import { EnvironmentId, OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Semaphore from "effect/Semaphore";
import * as Review from "@t3tools/contracts/jones/remoteAdmissionReview";
import * as PeerLinks from "../../peer/PeerLinks.ts";
import * as PeerMcpClient from "../../peer/PeerMcpClient.ts";
import type { PeerCallLimits } from "../../peer/PeerMcpClient.ts";
import type { RemoteAdmissionObservation } from "@t3tools/contracts/jones/remoteAdmissionReview";

export interface OriginPeerBinding {
  readonly environmentId: EnvironmentId;
  readonly generation: string;
  /** Receiver-authenticated session identity, never a local request namespace. */
  readonly receiverSessionId: string;
  readonly limits: PeerCallLimits;
  readonly peerLongPoll: boolean;
}
/** Authenticated receiver projection and current-link lookup; no credential administration. */
export class OriginPeerBindings extends Context.Service<OriginPeerBindings, {
  readonly current: (environmentId: EnvironmentId) => Effect.Effect<OriginPeerBinding | null, OrchestratorMcpFailure>;
  readonly relevant: () => Effect.Effect<ReadonlyArray<OriginPeerBinding>, OrchestratorMcpFailure>;
  /** Retry lookup uses the registered observation identity, never client:<session> lookup. */
  readonly ackRetry: (binding: OriginPeerBinding, attemptId: string, observationKey: string, disposition: "delivered" | "suppressed") => Effect.Effect<void, OrchestratorMcpFailure>;
  readonly observeRetry: (binding: OriginPeerBinding, attemptId: string, observationKey: string) => Effect.Effect<RemoteAdmissionObservation, OrchestratorMcpFailure>;
}>()("jones/remoteAdmissionOrigin/OriginPeerBindings") {}

// These protocol operations inspect/register delivery only. They never launch work or
// inherit a full-access coordinator's authority. The launch and retry registration
// retain the original request limits separately in durable intent.
export const protocolLimits: PeerCallLimits = { runtimeMode: "approval-required", interactionMode: "default" };
const make = Effect.gen(function* () {
  const links = yield* PeerLinks.PeerLinks;
  const peers = yield* PeerMcpClient.PeerMcpClient;
  const cache = new Map<string, OriginPeerBinding>();
  const denied = new Map<string, { generation: string; error: OrchestratorMcpFailure }>();
  const gates = new Map<string, Semaphore.Semaphore>();
  const current = Effect.fn("OriginPeerBindings.current")(function* (environmentId: EnvironmentId) {
    let gate = gates.get(environmentId);
    if (!gate) { gate = yield* Semaphore.make(1); gates.set(environmentId, gate); }
    return yield* gate.withPermits(1)(Effect.gen(function* () {
      const generation = yield* links.currentGeneration(environmentId).pipe(Effect.mapError(() => new OrchestratorMcpFailure({code:"orchestration_error",message:"The linked launch generation is unavailable."})));
      if (Option.isNone(generation)) { cache.delete(environmentId); return null; }
      const refusal = denied.get(environmentId);
      if (refusal?.generation === generation.value) return yield* refusal.error;
      denied.delete(environmentId);
      const old = cache.get(environmentId);
      if (old?.generation === generation.value) return old;
      cache.delete(environmentId);
      const status = yield* peers.call({environmentId,expectedGeneration:generation.value,
        tool:"t3_remote_admission_peer_status",arguments:{timeoutMs:0},limits:protocolLimits,success:Review.RemoteAdmissionPeerStatus}).pipe(Effect.tapError((error) => Effect.sync(() => {
          if (error.code === "capability_denied") denied.set(environmentId, {generation:generation.value,error});
        })));
      const latest = yield* links.currentGeneration(environmentId).pipe(Effect.mapError(() => new OrchestratorMcpFailure({code:"orchestration_error",message:"The linked launch generation is unavailable."})));
      if (Option.isNone(latest) || latest.value !== generation.value) return null;
      if (status.receiverEnvironmentId !== environmentId) return yield* new OrchestratorMcpFailure({code:"capability_denied",message:"The linked launch identity did not match its receiver."});
      const binding: OriginPeerBinding = {environmentId,generation:generation.value,receiverSessionId:status.sessionId,
        peerLongPoll:status.peerLongPoll,limits:protocolLimits};
      cache.set(environmentId,binding);
      return binding;
    }));
  });
  const relevant = Effect.fn("OriginPeerBindings.relevant")(function* () {
    const listed = yield* links.list.pipe(Effect.mapError(() => new OrchestratorMcpFailure({code:"orchestration_error",message:"Linked launch peers are unavailable."})));
    const found: OriginPeerBinding[] = [];
    // A failed or older peer must not prevent another peer's durable delivery.
    for (const link of listed) {
      const binding = yield* current(link.environmentId).pipe(Effect.catch(() => Effect.logDebug("Origin peer observation is unavailable for this generation.", { environmentId: link.environmentId }).pipe(Effect.as(null))));
      if (binding) found.push(binding);
    }
    return found;
  });
  const observeRetry = Effect.fn("OriginPeerBindings.observeRetry")(function* (binding: OriginPeerBinding, attemptId: string, observationKey: string) {
    return yield* peers.call({environmentId:binding.environmentId,expectedGeneration:binding.generation,limits:protocolLimits,
      tool:"t3_remote_admission_observe",arguments:{attemptId,observationKey},success:Review.RemoteAdmissionObservation});
  });
  const ackRetry = Effect.fn("OriginPeerBindings.ackRetry")(function* (binding: OriginPeerBinding, attemptId: string, observationKey: string, disposition: "delivered" | "suppressed") {
    yield* peers.call({environmentId:binding.environmentId,expectedGeneration:binding.generation,limits:protocolLimits,
      tool:"t3_remote_admission_ack",arguments:{attemptId,observationKey,disposition},success:Review.RemoteAdmissionAckResult});
  });
  return OriginPeerBindings.of({current,relevant,observeRetry,ackRetry});
});
export const layer = Layer.effect(OriginPeerBindings, make);
