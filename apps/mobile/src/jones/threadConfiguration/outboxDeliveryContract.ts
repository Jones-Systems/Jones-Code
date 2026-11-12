import { PreparedThreadTurnJson } from "@t3tools/client-runtime/jones/preparedThreadTurn";
import type { PreparedThreadTurn } from "@t3tools/client-runtime/operations";
import {
  ORCHESTRATION_V2_WS_METHODS,
  type CommandId,
  type MessageId,
  type ProjectId,
  type ThreadId,
} from "@t3tools/contracts";
import {
  DispatchDeliveryEvidence,
  LaunchDeliveryEvidence,
} from "@t3tools/contracts/jones/deliveryEvidence";
import * as Schema from "effect/Schema";

export const QueuedThreadIssuance = Schema.Union([
  Schema.Struct({ state: Schema.Literal("unissued") }),
  Schema.Struct({
    state: Schema.Literal("issued"),
    prepared: PreparedThreadTurnJson,
    contentKey: Schema.String,
    evidence: Schema.optional(Schema.Union([DispatchDeliveryEvidence, LaunchDeliveryEvidence])),
  }),
]);
export type QueuedThreadIssuance = typeof QueuedThreadIssuance.Type;

interface QueuedThreadTurnIdentity {
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly messageId: MessageId;
  readonly creation?: { readonly projectId: ProjectId };
}

export function queuedThreadTurnMatches(
  message: QueuedThreadTurnIdentity,
  prepared: PreparedThreadTurn,
): boolean {
  const payload = prepared.payload;
  return (
    payload.commandId === message.commandId &&
    payload.threadId === message.threadId &&
    (prepared.method === ORCHESTRATION_V2_WS_METHODS.launchThread
      ? prepared.payload.projectId === message.creation?.projectId &&
        prepared.payload.initialMessage?.messageId === message.messageId
      : prepared.payload.messageId === message.messageId)
  );
}
