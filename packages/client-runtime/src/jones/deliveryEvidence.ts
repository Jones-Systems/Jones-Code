import {
  DispatchDeliveryEvidence,
  LaunchDeliveryEvidence,
  initialMessageCommandId,
  type CommandDeliveryStatus,
} from "@t3tools/contracts/jones/deliveryEvidence";
import {
  ORCHESTRATION_V2_WS_METHODS,
  type OrchestrationV2ThreadLaunchInput,
} from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import type { PreparedThreadTurn } from "../operations/commands.ts";

const decodeDispatchEvidence = Schema.decodeUnknownOption(DispatchDeliveryEvidence);
const decodeLaunchEvidence = Schema.decodeUnknownOption(LaunchDeliveryEvidence);

// Read only normalized service facts. Neither a V2 error tag nor its message
// proves whether a command committed before its response was lost.
function errorEvidence(error: unknown): unknown {
  try {
    return typeof error === "object" && error !== null && "deliveryEvidence" in error
      ? error.deliveryEvidence
      : undefined;
  } catch {
    return undefined;
  }
}

export function readDispatchDeliveryEvidence(
  error: unknown,
  request: Pick<DispatchDeliveryEvidence, "commandId" | "commandType" | "threadId">,
): DispatchDeliveryEvidence {
  const unknown: DispatchDeliveryEvidence = { kind: "dispatch", ...request, status: "unknown" };
  try {
    const decoded = decodeDispatchEvidence(errorEvidence(error));
    if (Option.isNone(decoded)) return unknown;
    const evidence = decoded.value;
    return evidence.commandId === request.commandId &&
      evidence.commandType === request.commandType &&
      evidence.threadId === request.threadId
      ? evidence
      : unknown;
  } catch {
    return unknown;
  }
}

type LaunchRequest = Pick<
  OrchestrationV2ThreadLaunchInput,
  "commandId" | "projectId" | "threadId" | "initialMessage"
>;

export function readLaunchDeliveryEvidence(
  error: unknown,
  request: LaunchRequest,
): LaunchDeliveryEvidence {
  const initialMessage: LaunchDeliveryEvidence["initialMessage"] =
    request.initialMessage === undefined
      ? { status: "not-requested" }
      : {
          status: "unknown",
          commandId: initialMessageCommandId(request.commandId),
          ...(request.initialMessage.messageId === undefined
            ? {}
            : { messageId: request.initialMessage.messageId }),
        };
  const unknown: LaunchDeliveryEvidence = {
    kind: "launch",
    commandId: request.commandId,
    projectId: request.projectId,
    creation: { status: "unknown" },
    initialMessage,
  };
  try {
    const decoded = decodeLaunchEvidence(errorEvidence(error));
    if (Option.isNone(decoded)) return unknown;
    const evidence = decoded.value;
    if (
      evidence.commandId !== request.commandId ||
      evidence.projectId !== request.projectId ||
      (evidence.creation.status === "accepted" &&
        request.threadId !== undefined &&
        evidence.creation.threadId !== request.threadId)
    ) {
      return unknown;
    }
    const observedMessage = evidence.initialMessage;
    const matchesMessage =
      request.initialMessage === undefined
        ? observedMessage.status === "not-requested"
        : observedMessage.status !== "not-requested" &&
          (request.initialMessage.messageId === undefined ||
            observedMessage.messageId === request.initialMessage.messageId);
    return matchesMessage ? evidence : { ...evidence, initialMessage };
  } catch {
    return unknown;
  }
}

export function readPreparedThreadTurnDeliveryEvidence(
  error: unknown,
  prepared: PreparedThreadTurn,
): DispatchDeliveryEvidence | LaunchDeliveryEvidence {
  return prepared.method === ORCHESTRATION_V2_WS_METHODS.launchThread
    ? readLaunchDeliveryEvidence(error, prepared.payload)
    : readDispatchDeliveryEvidence(error, {
        commandId: prepared.payload.commandId,
        commandType: prepared.payload.type,
        threadId: prepared.payload.threadId,
      });
}

export function threadTurnDeliveryStatus(
  evidence: DispatchDeliveryEvidence | LaunchDeliveryEvidence,
): CommandDeliveryStatus {
  if (evidence.kind === "dispatch") return evidence.status;
  if (evidence.creation.status !== "accepted") return evidence.creation.status;
  return evidence.initialMessage.status === "not-requested"
    ? "accepted"
    : evidence.initialMessage.status;
}
