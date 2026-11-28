import {
  ORCHESTRATION_V2_WS_METHODS,
  type OrchestrationCommandObservation,
} from "@t3tools/contracts";
import type { PreparedThreadTurn } from "@t3tools/client-runtime/operations";
import { initialMessageCommandId } from "@t3tools/contracts/jones/deliveryEvidence";
import { msg } from "gt-react-native";

import { m } from "../../i18n/messages";
import type { QueuedThreadMessage } from "../../state/thread-outbox-model";
import { queuedThreadDeliveryStatus } from "../threadConfiguration/outboxDelivery";

export const AUTO_ROUTING_OUTBOX_VERSION = 5;

export function preparedUsesAutomaticAccounts(prepared: PreparedThreadTurn | undefined): boolean {
  return (
    prepared?.method === ORCHESTRATION_V2_WS_METHODS.launchThread &&
    prepared.payload.accountRouting?.mode === "auto"
  );
}

export function automaticLaunchNeedsReconciliation(message: QueuedThreadMessage): boolean {
  return (
    message.issuance?.state === "issued" &&
    preparedUsesAutomaticAccounts(message.issuance.prepared) &&
    queuedThreadDeliveryStatus(message) !== "rejected"
  );
}

export function automaticAccountWorkspaceError(input: {
  readonly mode: "local" | "worktree";
  readonly pullRequestReference?: string | undefined;
  readonly scratch?: boolean;
}): string | undefined {
  if (input.mode === "local" && input.pullRequestReference === undefined && !input.scratch)
    return undefined;
  return m(
    msg(
      "Automatic account selection needs an existing project folder or worktree. Choose a specific account to create a workspace.",
    ),
  );
}

export function automaticAccountHoldReason(issued: boolean): string {
  return issued
    ? m(
        msg(
          "Automatic account selection is unavailable on this server. Reconnect to a server version that supports it to verify this task's original launch before retrying.",
        ),
      )
    : m(
        msg(
          "This task is queued for automatic account selection. Update the destination server to a version that supports it, or delete the queued task and start again with a specific account.",
        ),
      );
}

type LaunchPayload = Extract<
  PreparedThreadTurn,
  { method: typeof ORCHESTRATION_V2_WS_METHODS.launchThread }
>["payload"];

export function acceptedAutoLaunchSequence(
  payload: LaunchPayload,
  observation: OrchestrationCommandObservation,
): number | undefined {
  return observation.threadId === payload.threadId &&
    observation.messageId === payload.initialMessage.messageId &&
    (observation.commandId === payload.commandId ||
      observation.commandId === initialMessageCommandId(payload.commandId)) &&
    observation.commandStatus === "accepted" &&
    (observation.correlation === "exact" || observation.correlation === "pending")
    ? (observation.acceptedSequence ?? undefined)
    : undefined;
}
