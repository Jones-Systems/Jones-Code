import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  CommandId,
  MessageId,
  ProjectId,
  ThreadId,
  TrimmedNonEmptyString,
} from "../baseSchemas.ts";
import { exactStruct } from "./exactStruct.ts";

export const CommandDeliveryStatus = Schema.Literals(["accepted", "rejected", "unknown"]);
export type CommandDeliveryStatus = typeof CommandDeliveryStatus.Type;

export const DispatchDeliveryEvidence = exactStruct({
  kind: Schema.Literal("dispatch"),
  commandId: CommandId,
  commandType: TrimmedNonEmptyString,
  threadId: Schema.NullOr(ThreadId),
  status: CommandDeliveryStatus,
});
export type DispatchDeliveryEvidence = typeof DispatchDeliveryEvidence.Type;

export function initialMessageCommandId(commandId: CommandId): CommandId {
  return CommandId.make(`${commandId}:initial-message`);
}

const LaunchCreationEvidence = Schema.Union([
  exactStruct({ status: Schema.Literal("accepted"), threadId: ThreadId }),
  exactStruct({ status: Schema.Literals(["rejected", "unknown"]) }),
]);

const LaunchInitialMessageEvidence = Schema.Union([
  exactStruct({ status: Schema.Literal("not-requested") }),
  exactStruct({
    status: Schema.Literal("accepted"),
    commandId: CommandId,
    messageId: MessageId,
  }),
  exactStruct({
    status: Schema.Literals(["rejected", "unknown"]),
    commandId: CommandId,
    messageId: Schema.optionalKey(MessageId),
  }),
]);

export const LaunchDeliveryEvidence = exactStruct({
  kind: Schema.Literal("launch"),
  commandId: CommandId,
  projectId: ProjectId,
  creation: LaunchCreationEvidence,
  initialMessage: LaunchInitialMessageEvidence,
}).check(
  Schema.makeFilter(
    (evidence) =>
      (evidence.initialMessage.status !== "accepted" || evidence.creation.status === "accepted") &&
      (evidence.initialMessage.status === "not-requested" ||
        evidence.initialMessage.commandId === initialMessageCommandId(evidence.commandId)),
  ),
);
export type LaunchDeliveryEvidence = typeof LaunchDeliveryEvidence.Type;

// Malformed optional evidence must not make an otherwise readable legacy error
// undecodable or turn a transport/projection failure into permission to resend.
export const OptionalDispatchDeliveryEvidence = Schema.optionalKey(
  Schema.UndefinedOr(DispatchDeliveryEvidence).pipe(
    Schema.catchDecoding(() => Effect.succeedSome(undefined)),
  ),
);
export const OptionalLaunchDeliveryEvidence = Schema.optionalKey(
  Schema.UndefinedOr(LaunchDeliveryEvidence).pipe(
    Schema.catchDecoding(() => Effect.succeedSome(undefined)),
  ),
);
