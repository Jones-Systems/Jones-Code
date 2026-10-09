import {
  DESIGN_REQUEST_MAX_ATTEMPTS,
  type DesignRequestAttachmentData,
  type DesignRequestBinding,
  type DesignRequestDestination,
  type DesignRequestHeldReason,
  type DesignRequestReceiptStatus,
} from "@t3tools/contracts/jones/designRequests";

import type { StartThreadTurnInput } from "../../operations/commands.ts";
import {
  buildDesignRequestMessage,
  designRequestIds,
  designRequestTurnInput,
  parseDesignRequestIds,
  type DesignRequestIds,
} from "./dispatch.ts";
import { PacketV1Error, verifyPacketV1, verifyPacketV1Images, type PacketV1 } from "./packetV1.ts";
import {
  classifyDesignRequestDispatch,
  classifyDesignRequestObservation,
  designRequestDelivery,
  type DesignRequestDispatchOutcome,
  type DesignRequestObservation,
  type DesignRequestObserve,
} from "./reconcile.ts";
import type { DesignRequestResolvedRoute, DesignRequestRoutable } from "./route.ts";

export interface DesignRequestGatewayPorts {
  readonly jonesOrigin: string;
  /** Re-resolve the route from fresh registry and thread state at submit time. */
  readonly resolveRoute: () => Promise<DesignRequestResolvedRoute>;
  readonly observe: DesignRequestObserve;
  /** The shipped `startThreadTurn` queue path. Resolves after the durable command receipt. */
  readonly dispatch: (input: StartThreadTurnInput) => Promise<DesignRequestDispatchOutcome>;
  /** Threads the gallery may reconcile: primary-environment threads this client can read. */
  readonly isReadableThread: (threadId: string) => boolean;
}

export interface DesignRequestSubmission {
  readonly binding: DesignRequestBinding;
  readonly projectKey: string;
  readonly packet: unknown;
  readonly attachmentData: DesignRequestAttachmentData | undefined;
  readonly expectedRouteToken: string | null;
  /** Chosen by the gallery (1..20). Jones derives the IDs from exactly this attempt. */
  readonly attempt: number;
}

export interface DesignRequestReconcileInput {
  readonly threadId: string;
  readonly commandId: string;
  readonly messageId: string;
  readonly packetId: string;
  readonly digest: string;
  readonly workstreamId: string | null;
}

export interface DesignRequestOutcome {
  readonly status: DesignRequestReceiptStatus;
  readonly packetId: string;
  /** Absent only when the submitted packet carries no well-formed digest; no receipt can bind it. */
  readonly digest?: string;
  readonly reason?: string;
  readonly destination?: DesignRequestDestination;
  readonly delivery?: "started" | "queued";
}

const destination = (
  jonesOrigin: string,
  route: DesignRequestRoutable,
  ids: DesignRequestIds,
): DesignRequestDestination => ({
  jonesOrigin,
  workstreamId: route.workstream.id,
  threadId: route.thread.id,
  commandId: ids.commandId,
  messageId: ids.messageId,
});

export const designRequestPacketIdentity = (
  packet: unknown,
): { packetId: string; digest?: string } => {
  const value = packet && typeof packet === "object" ? (packet as Record<string, unknown>) : {};
  const packetId =
    typeof value.packetId === "string" && /^[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}$/.test(value.packetId)
      ? value.packetId
      : "unknown";
  return typeof value.digest === "string" && /^sha256:[a-f0-9]{64}$/.test(value.digest)
    ? { packetId, digest: value.digest }
    : { packetId };
};

/**
 * One gateway per Jones page. Concurrent submits of the same packet attempt share one in-flight
 * result, and every dispatch uses the IDs derived from the gallery's explicit attempt, so a double
 * click or a resend never creates a second message and Jones never picks another attempt.
 */
export function createDesignRequestGateway(ports: DesignRequestGatewayPorts) {
  const inFlight = new Map<string, Promise<DesignRequestOutcome>>();

  const held = (packet: unknown, reason: DesignRequestHeldReason): DesignRequestOutcome => ({
    status: "held",
    ...designRequestPacketIdentity(packet),
    reason,
  });

  async function deliver(
    packet: PacketV1,
    submission: DesignRequestSubmission,
    images: ReturnType<typeof verifyPacketV1Images>,
  ): Promise<DesignRequestOutcome> {
    const identity = { packetId: packet.packetId, digest: packet.digest };
    const route = await ports.resolveRoute();
    if (route.state === "held") return { status: "held", ...identity, reason: route.reason };
    if (
      submission.expectedRouteToken !== null &&
      route.routeToken !== submission.expectedRouteToken
    )
      return { status: "held", ...identity, reason: "route-changed" };
    const text = buildDesignRequestMessage(packet, {
      projectKey: submission.projectKey,
      workstreamName: route.workstream.name,
    });
    if (text === null) return { status: "held", ...identity, reason: "too-large" };
    const ids = designRequestIds(packet.packetId, packet.digest, submission.attempt);
    const bound = destination(ports.jonesOrigin, route, ids);
    // Observe this exact attempt before dispatching: accepted replays, rejected stays rejected,
    // and an unavailable readback leaves the effect unknown without sending anything.
    let before: DesignRequestObservation;
    try {
      before = await ports.observe({ threadId: route.thread.id, ...ids });
    } catch {
      return { status: "unknown", ...identity, reason: "readback-unavailable", destination: bound };
    }
    if (before.commandStatus === "accepted")
      return {
        status: "accepted",
        ...identity,
        destination: bound,
        delivery: designRequestDelivery(before),
      };
    if (before.commandStatus === "rejected")
      return { status: "rejected", ...identity, reason: "rejected" };
    let outcome: DesignRequestDispatchOutcome;
    try {
      outcome = await ports.dispatch(designRequestTurnInput(route, ids, text, images));
    } catch (cause) {
      outcome = { kind: "failed", message: cause instanceof Error ? cause.message : String(cause) };
    }
    let observation = null;
    try {
      observation = await ports.observe({ threadId: route.thread.id, ...ids });
    } catch {
      observation = null;
    }
    const result = classifyDesignRequestDispatch(outcome, observation);
    return {
      ...identity,
      ...result,
      ...(result.status === "accepted" || result.status === "unknown"
        ? { destination: bound }
        : {}),
    };
  }

  return {
    /** Verify, resolve the route again, then queue. Nothing is sent when any check fails. */
    async submit(submission: DesignRequestSubmission): Promise<DesignRequestOutcome> {
      if (submission.projectKey !== submission.binding.projectKey)
        return held(submission.packet, "no-binding");
      if (
        !Number.isSafeInteger(submission.attempt) ||
        submission.attempt < 1 ||
        submission.attempt > DESIGN_REQUEST_MAX_ATTEMPTS
      )
        return held(submission.packet, "packet-invalid");
      let packet: PacketV1;
      let images: ReturnType<typeof verifyPacketV1Images>;
      try {
        packet = verifyPacketV1(submission.packet);
        images = verifyPacketV1Images(packet, submission.attachmentData);
      } catch (cause) {
        if (cause instanceof PacketV1Error) return held(submission.packet, cause.reason);
        throw cause;
      }
      const key = `${packet.packetId}|${packet.digest}|${submission.attempt}`;
      const pending = inFlight.get(key);
      if (pending !== undefined) return pending;
      const identity = { packetId: packet.packetId, digest: packet.digest };
      // A failure before dispatch (for example a registry read) sends nothing, but the gallery
      // still reconciles before resending because the outcome is not authoritative.
      const run = deliver(packet, submission, images)
        .catch((): DesignRequestOutcome => ({
          status: "unknown",
          ...identity,
          reason: "gateway-error",
        }))
        .finally(() => inFlight.delete(key));
      inFlight.set(key, run);
      return run;
    },

    /**
     * Read back one gallery-namespaced command on a readable thread; never dispatches. The IDs must
     * derive from the bound packet ID and digest, and every receipt echoes that full digest, so a
     * fresh Jones page (which never saw the submit) still answers with a bound receipt.
     */
    async reconcile(input: DesignRequestReconcileInput): Promise<DesignRequestOutcome> {
      const identity = { packetId: input.packetId, digest: input.digest };
      const parsed = parseDesignRequestIds(input.commandId, input.messageId);
      if (
        parsed === null ||
        parsed.packetId !== input.packetId ||
        parsed.digestPrefix !== input.digest.slice(7, 39) ||
        parsed.attempt > DESIGN_REQUEST_MAX_ATTEMPTS
      )
        return { status: "held", ...identity, reason: "packet-invalid" };
      if (!ports.isReadableThread(input.threadId))
        return { status: "held", ...identity, reason: "thread-not-local" };
      let observation;
      try {
        observation = await ports.observe(input);
      } catch {
        return { status: "unknown", ...identity, reason: "readback-unavailable" };
      }
      const result = classifyDesignRequestObservation(observation);
      if (result.status !== "accepted") return { ...identity, ...result };
      // Accepted needs the complete bound destination; without the paired workstream it stays unknown.
      if (input.workstreamId === null)
        return { status: "unknown", ...identity, reason: "no-binding" };
      return {
        ...identity,
        ...result,
        destination: {
          jonesOrigin: ports.jonesOrigin,
          workstreamId: input.workstreamId,
          threadId: input.threadId,
          commandId: input.commandId,
          messageId: input.messageId,
        },
      };
    },
  };
}

export type DesignRequestGateway = ReturnType<typeof createDesignRequestGateway>;
