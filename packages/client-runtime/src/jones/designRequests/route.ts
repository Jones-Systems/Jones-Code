import type {
  DesignRequestBinding,
  DesignRequestHeldReason,
} from "@t3tools/contracts/jones/designRequests";
import type { ProviderInteractionMode, RuntimeMode } from "@t3tools/contracts";

export interface PersonalDesignAssignment {
  readonly workstreamId: string | null;
  readonly environmentId: string;
  readonly threadId: string;
  readonly revision: number;
}
export interface PersonalDesignDefinitions {
  readonly items: readonly { readonly workstreamId: string; readonly name: string }[];
  readonly confirmed: boolean;
}
export interface PersonalDesignAssignments {
  readonly items: readonly PersonalDesignAssignment[];
}

import { sha256Hex } from "./packetV1.ts";

/** Registry readiness as the web client observed it; `ready` still requires live, complete data. */
export type DesignRequestRegistryStatus =
  | "unsupported"
  | "unauthenticated"
  | "loading"
  | "error"
  | "ready";

export interface DesignRequestThreadCandidate {
  readonly environmentId: string;
  readonly id: string;
  readonly title: string;
  readonly archivedAt: string | null;
  readonly runtimeMode: RuntimeMode | undefined;
  readonly interactionMode: ProviderInteractionMode | undefined;
}

export interface DesignRequestRouteInput {
  readonly binding: DesignRequestBinding | null;
  readonly registry: DesignRequestRegistryStatus;
  readonly data: PersonalDesignDefinitions | null;
  readonly placements: PersonalDesignAssignments | null;
  /** Only the primary environment's threads can be queued and read back from this client. */
  readonly primaryEnvironmentId: string | null;
  readonly threads: readonly DesignRequestThreadCandidate[];
  readonly now: number;
}

export interface DesignRequestRoutable {
  readonly state: "routable";
  readonly routeToken: string;
  readonly bindingId: string;
  readonly workstream: { readonly id: string; readonly name: string };
  readonly thread: {
    readonly environmentId: string;
    readonly id: string;
    readonly title: string;
    readonly runtimeMode: RuntimeMode;
    readonly interactionMode: ProviderInteractionMode;
  };
  readonly membershipId: string;
}

export interface DesignRequestHeldRoute {
  readonly state: "held";
  readonly reason: DesignRequestHeldReason;
  readonly workstream?: { readonly id: string; readonly name: string };
}

export type DesignRequestResolvedRoute = DesignRequestRoutable | DesignRequestHeldRoute;

/** `sha256(bindingId|workstreamId|membership_id|source_instance_id|native_thread_id)`, 32 hex. */
export function designRequestRouteToken(
  bindingId: string,
  placement: PersonalDesignAssignment,
): string {
  return sha256Hex(
    [
      bindingId,
      placement.workstreamId,
      `personal:${placement.revision}`,
      placement.environmentId,
      placement.threadId,
    ].join("|"),
  ).slice(0, 32);
}

const held = (
  reason: DesignRequestHeldReason,
  workstream?: { readonly id: string; readonly name: string },
): DesignRequestHeldRoute =>
  workstream ? { state: "held", reason, workstream } : { state: "held", reason };

/**
 * Deterministic routing with no meaning-based guessing: the binding names a workstream, which
 * must have exactly one confirmed personal assignment whose
 * thread must be a non-archived thread on the primary environment. Anything else is held.
 */
export function resolveDesignRequestRoute(
  input: DesignRequestRouteInput,
): DesignRequestResolvedRoute {
  const { binding, data, placements } = input;
  if (binding === null) return held("no-binding");
  if (input.registry === "unsupported") return held("runtime-unsupported");
  if (input.registry === "unauthenticated") return held("not-authenticated");
  if (input.registry !== "ready" || data === null || !data.confirmed) return held("registry-stale");
  const metadata = data.items.find((item) => item.workstreamId === binding.workstreamId);
  if (metadata === undefined) return held("workstream-not-found");
  const workstream = { id: metadata.workstreamId, name: metadata.name };
  if (placements === null) return held("registry-stale", workstream);
  const primaries = placements.items.filter((item) => item.workstreamId === binding.workstreamId);
  if (primaries.length === 0) return held("no-primary", workstream);
  if (primaries.length > 1) return held("multiple-primary", workstream);
  const placement = primaries[0]!;
  if (input.primaryEnvironmentId === null || placement.environmentId !== input.primaryEnvironmentId)
    return held("thread-not-local", workstream);
  const thread = input.threads.find(
    (candidate) =>
      candidate.environmentId === placement.environmentId && candidate.id === placement.threadId,
  );
  if (thread === undefined) return held("thread-not-local", workstream);
  if (thread.archivedAt !== null) return held("thread-archived", workstream);
  if (thread.runtimeMode === undefined || thread.interactionMode === undefined)
    return held("thread-config-unknown", workstream);
  return {
    state: "routable",
    routeToken: designRequestRouteToken(binding.bindingId, placement),
    bindingId: binding.bindingId,
    workstream,
    thread: {
      environmentId: thread.environmentId,
      id: thread.id,
      title: thread.title,
      runtimeMode: thread.runtimeMode,
      interactionMode: thread.interactionMode,
    },
    membershipId: `personal:${placement.revision}`,
  };
}
