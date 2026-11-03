import type { EnvironmentThreadState } from "@t3tools/client-runtime/state/threads";
import {
  ChatAttachmentId,
  ChatFileAttachment,
  ChatImageAttachment,
  OrchestrationMessageContext,
  type EnvironmentId,
} from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import {
  serializeComposerMessageForServer,
  uploadedComposerContext,
} from "../../lib/composerContext";
import type { QueuedThreadMessage } from "../../state/thread-outbox-model";

const KnownAttachments = Schema.Array(Schema.Union([ChatFileAttachment, ChatImageAttachment]));
const knownAttachments = Schema.is(KnownAttachments);
const sameAttachments = Schema.toEquivalence(KnownAttachments);
const sameContext = Schema.toEquivalence(Schema.UndefinedOr(OrchestrationMessageContext));
const uploadedAttachmentId = Schema.decodeUnknownOption(ChatAttachmentId);

export function legacyQueuedThreadMessageAccepted(
  message: QueuedThreadMessage,
  environmentId: EnvironmentId,
  state: EnvironmentThreadState,
): boolean {
  if (message.issuance !== undefined || message.creation !== undefined ||
    environmentId !== message.environmentId || state.status !== "live" ||
    Option.isSome(state.error) || Option.isNone(state.data)) return false;
  const projection = state.data.value;
  if (projection.thread.id !== message.threadId) return false;
  const accepted = projection.messages.find((candidate) =>
    candidate.id === message.messageId && candidate.threadId === message.threadId &&
    candidate.role === "user" && candidate.runId !== null && !candidate.streaming);
  if (accepted === undefined || !projection.runs.some((run) =>
    run.id === accepted.runId && run.threadId === message.threadId)) return false;

  const attachments: Array<ChatFileAttachment | ChatImageAttachment> = [];
  for (const attachment of message.attachments) {
    if (attachment.uploadEnvironmentId !== message.environmentId ||
      attachment.uploadedAttachmentId === undefined) return false;
    const decodedId = uploadedAttachmentId(attachment.uploadedAttachmentId);
    if (Option.isNone(decodedId)) return false;
    const id = decodedId.value;
    attachments.push(attachment.type === "file" ? {
      type: "file", id, name: attachment.name, mimeType: attachment.mimeType,
      sizeBytes: attachment.sizeBytes,
      ...(attachment.source === undefined ? {} : { source: attachment.source }),
    } : {
      type: "image", id, name: attachment.name, mimeType: attachment.mimeType,
      sizeBytes: attachment.sizeBytes,
      ...(attachment.source === undefined ? {} : { source: attachment.source }),
    });
  }
  if (!knownAttachments(accepted.attachments) || !sameAttachments(attachments, accepted.attachments)) return false;
  const context = uploadedComposerContext(message.context, message.attachments, attachments);
  // Either recorded wire representation can prove acceptance; today's capabilities cannot select the old one.
  return [true, false].some((inline) => {
    const wire = serializeComposerMessageForServer(message.text, context, inline);
    return wire.text === accepted.text && sameContext(wire.context, accepted.context);
  });
}
