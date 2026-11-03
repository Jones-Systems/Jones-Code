import {
  readPreparedThreadTurnDeliveryEvidence,
  threadTurnDeliveryStatus,
} from "@t3tools/client-runtime/jones/deliveryEvidence";
import type { PreparedThreadTurn } from "@t3tools/client-runtime/operations";
import { ORCHESTRATION_V2_WS_METHODS } from "@t3tools/contracts";
import {
  initialMessageCommandId,
  type DispatchDeliveryEvidence,
  type LaunchDeliveryEvidence,
} from "@t3tools/contracts/jones/deliveryEvidence";

import type { QueuedThreadMessage } from "../../state/thread-outbox-model";
import { queuedThreadTurnMatches } from "./outboxDeliveryContract";

type DeliveryEvidence = DispatchDeliveryEvidence | LaunchDeliveryEvidence;

// Acceptance fences recovery immediately, even while its durable write is pending or failing.
// Weak ownership keeps these facts only as long as the queued frozen request is retained.
const observations = new WeakMap<PreparedThreadTurn, DeliveryEvidence>();

function contentKey(message: QueuedThreadMessage): string {
  const { issuance: _, ...content } = message;
  return JSON.stringify(content, (_key, value: unknown) =>
    typeof value === "object" && value !== null && !Array.isArray(value)
      ? Object.fromEntries(
          Object.entries(value).sort(([left], [right]) =>
            left < right ? -1 : left > right ? 1 : 0,
          ),
        )
      : value,
  );
}

export function withIssuedQueuedThreadTurn(
  message: QueuedThreadMessage,
  prepared: PreparedThreadTurn,
): QueuedThreadMessage {
  if (message.issuance?.state !== "unissued" || !queuedThreadTurnMatches(message, prepared)) {
    throw new Error("Only a never-issued queued message can capture a matching prepared request.");
  }
  return { ...message, issuance: { state: "issued", prepared, contentKey: contentKey(message) } };
}

function mergeEvidence(
  previous: DeliveryEvidence | undefined,
  next: DeliveryEvidence,
): DeliveryEvidence {
  if (previous === undefined || previous.kind !== next.kind) return next;
  if (previous.kind === "dispatch" && next.kind === "dispatch") {
    return previous.status === "accepted" || next.status === "unknown" ? previous : next;
  }
  if (previous.kind === "launch" && next.kind === "launch") {
    return {
      ...next,
      creation:
        previous.creation.status === "accepted" || next.creation.status === "unknown"
          ? previous.creation
          : next.creation,
      initialMessage:
        previous.initialMessage.status === "accepted" || next.initialMessage.status === "unknown"
          ? previous.initialMessage
          : next.initialMessage,
    };
  }
  return next;
}

export function queuedThreadDeliveryEvidence(
  message: QueuedThreadMessage,
): DeliveryEvidence | undefined {
  const issuance = message.issuance;
  if (issuance?.state !== "issued") return undefined;
  const stored =
    issuance.evidence === undefined
      ? undefined
      : readPreparedThreadTurnDeliveryEvidence(
          { deliveryEvidence: issuance.evidence },
          issuance.prepared,
        );
  const observed = observations.get(issuance.prepared);
  return observed === undefined ? stored : mergeEvidence(stored, observed);
}

export function observeQueuedThreadDelivery(
  message: QueuedThreadMessage,
  evidence: DeliveryEvidence,
): QueuedThreadMessage {
  const issuance = message.issuance;
  if (issuance?.state !== "issued") throw new Error("Missing issued request.");
  const matched = readPreparedThreadTurnDeliveryEvidence(
    { deliveryEvidence: evidence },
    issuance.prepared,
  );
  const merged = mergeEvidence(queuedThreadDeliveryEvidence(message), matched);
  observations.set(issuance.prepared, merged);
  return { ...message, issuance: { ...issuance, evidence: merged } };
}

export function acceptedQueuedThreadDelivery(message: QueuedThreadMessage): DeliveryEvidence {
  const issuance = message.issuance;
  if (issuance?.state !== "issued") throw new Error("Missing issued request.");
  const prepared = issuance.prepared;
  return prepared.method === ORCHESTRATION_V2_WS_METHODS.launchThread
    ? {
        kind: "launch",
        commandId: prepared.payload.commandId,
        projectId: prepared.payload.projectId,
        creation: { status: "accepted", threadId: prepared.payload.threadId },
        initialMessage: {
          status: "accepted",
          commandId: initialMessageCommandId(prepared.payload.commandId),
          messageId: prepared.payload.initialMessage.messageId,
        },
      }
    : {
        kind: "dispatch",
        commandId: prepared.payload.commandId,
        commandType: prepared.payload.type,
        threadId: prepared.payload.threadId,
        status: "accepted",
      };
}

export function queuedThreadDeliveryStatus(message: QueuedThreadMessage) {
  const evidence = queuedThreadDeliveryEvidence(message);
  return evidence === undefined ? "unknown" : threadTurnDeliveryStatus(evidence);
}

export function canRestoreQueuedThreadMessage(message: QueuedThreadMessage): boolean {
  return (
    message.issuance?.state === "unissued" || queuedThreadDeliveryStatus(message) === "rejected"
  );
}

export function queuedThreadProvenTarget(message: QueuedThreadMessage) {
  const evidence = queuedThreadDeliveryEvidence(message);
  return evidence?.kind === "launch" && evidence.creation.status === "accepted"
    ? evidence.creation.threadId
    : undefined;
}

export function queuedThreadContentChanged(message: QueuedThreadMessage): boolean {
  return (
    message.issuance?.state === "issued" && message.issuance.contentKey !== contentKey(message)
  );
}

// Pending-task editors predate issuance metadata. They may save new content, but cannot
// replace a request that could already have committed. That content is recovered for a new send.
export function preserveQueuedThreadIssuance(
  previous: QueuedThreadMessage | undefined,
  next: QueuedThreadMessage,
): QueuedThreadMessage {
  if (previous !== undefined && previous.issuance === undefined) {
    if (next.issuance === undefined) return next;
    const { issuance: _, ...legacy } = next;
    return legacy;
  }
  if (previous?.issuance?.state === "unissued" && next.issuance === undefined) {
    return { ...next, issuance: previous.issuance };
  }
  if (previous?.issuance?.state !== "issued") return next;
  const nextEvidence = next.issuance?.state === "issued" ? next.issuance.evidence : undefined;
  const retained = {
    ...next,
    environmentId: previous.environmentId,
    threadId: previous.threadId,
    messageId: previous.messageId,
    commandId: previous.commandId,
    ...(previous.creation === undefined
      ? {}
      : {
          creation: {
            ...(next.creation ?? previous.creation),
            projectId: previous.creation.projectId,
          },
        }),
    issuance: previous.issuance,
  };
  return nextEvidence === undefined
    ? retained
    : observeQueuedThreadDelivery(retained, nextEvidence);
}

export function queuedThreadRecoveryCreation(message: QueuedThreadMessage) {
  return queuedThreadProvenTarget(message) === undefined ? message.creation : undefined;
}

interface DeliveryQueue {
  readonly current: () => ReadonlyArray<QueuedThreadMessage>;
  readonly revisionOf: (id: QueuedThreadMessage["messageId"]) => number;
  readonly confirmQueued: (message: QueuedThreadMessage) => Promise<boolean>;
  readonly update: (message: QueuedThreadMessage, revision: number) => Promise<boolean>;
}

export async function deliverQueuedThreadTurn(input: {
  readonly message: QueuedThreadMessage;
  readonly revision: number;
  readonly prepared?: PreparedThreadTurn;
  readonly queue: DeliveryQueue;
  readonly canDispatch: () => boolean;
  readonly dispatch: (prepared: PreparedThreadTurn) => Promise<unknown>;
}): Promise<{
  readonly message: QueuedThreadMessage;
  readonly revision: number;
  readonly status: "accepted" | "rejected" | "unknown";
  readonly error?: unknown;
} | null> {
  const { queue } = input;
  let message = input.message;
  if (!(await queue.confirmQueued(message))) return null;
  if (message.issuance?.state === "unissued") {
    if (input.prepared === undefined || !input.canDispatch()) return null;
    const issued = withIssuedQueuedThreadTurn(message, input.prepared);
    // The atomic file write must settle successfully before any command RPC.
    if (!(await queue.update(issued, input.revision))) return null;
    const current = queue.current().find((candidate) => candidate.messageId === message.messageId);
    if (current === undefined) return null;
    message = current;
  }
  const issuance = message.issuance;
  if (issuance?.state !== "issued" || !input.canDispatch()) return null;
  let error: unknown;
  let evidence = queuedThreadDeliveryEvidence(message);
  if (evidence === undefined || threadTurnDeliveryStatus(evidence) === "unknown") {
    try {
      await input.dispatch(issuance.prepared);
      evidence = acceptedQueuedThreadDelivery(message);
    } catch (cause) {
      error = cause;
      evidence = readPreparedThreadTurnDeliveryEvidence(cause, issuance.prepared);
    }
  }
  // Publish the observation before awaiting storage. Stale failure/editor work cannot
  // restore an accepted request, even if persistence is temporarily unavailable.
  observeQueuedThreadDelivery(message, evidence);
  const current = queue.current().find((candidate) => candidate.messageId === message.messageId);
  if (current?.issuance?.state !== "issued" || current.issuance.prepared !== issuance.prepared)
    return null;
  const updated = observeQueuedThreadDelivery(current, evidence);
  if (!(await queue.update(updated, queue.revisionOf(current.messageId)))) return null;
  const persisted = queue.current().find((candidate) => candidate.messageId === current.messageId);
  return persisted === undefined
    ? null
    : {
        message: persisted,
        revision: queue.revisionOf(persisted.messageId),
        status: queuedThreadDeliveryStatus(persisted),
        ...(error === undefined ? {} : { error }),
      };
}
