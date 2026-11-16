import * as Schema from "effect/Schema";
import { CommandId, IsoDateTime, MessageId, PositiveInt, RunId, ThreadId } from "../baseSchemas.ts";
import {
  PROVIDER_SEND_TURN_MAX_ATTACHMENTS,
  PROVIDER_SEND_TURN_MAX_INPUT_CHARS,
  PROVIDER_SEND_TURN_MAX_TOTAL_IMAGE_BYTES,
} from "../chatAttachment.ts";

export const QUEUED_STEER_BATCH_LIMITS = {
  maxInputChars: PROVIDER_SEND_TURN_MAX_INPUT_CHARS,
  maxAttachments: PROVIDER_SEND_TURN_MAX_ATTACHMENTS,
  maxImageBytes: PROVIDER_SEND_TURN_MAX_TOTAL_IMAGE_BYTES,
  maxItems: 100,
} as const;
export const QueuedSteerBatchCapability = Schema.Struct({
  maxInputChars: PositiveInt,
  maxAttachments: PositiveInt,
  maxImageBytes: PositiveInt,
  maxItems: PositiveInt,
});
export const QueuedSteerBatchCommand = Schema.Struct({
  type: Schema.Literal("queued-message.promote-batch-to-steer"),
  commandId: CommandId,
  threadId: ThreadId,
  targetRunId: RunId,
  items: Schema.Array(
    Schema.Struct({
      queuedRunId: RunId,
      messageId: MessageId,
      messageUpdatedAt: IsoDateTime,
    }),
  ).check(Schema.isMinLength(1), Schema.isMaxLength(QUEUED_STEER_BATCH_LIMITS.maxItems)),
});
