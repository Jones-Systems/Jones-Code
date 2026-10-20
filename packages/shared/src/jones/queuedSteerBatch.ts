import {
  QUEUED_STEER_BATCH_LIMITS,
  isProviderSendTurnSupportedImageMimeType,
  type ChatAttachment,
  type OrchestrationMessageContext,
  type ThreadId,
} from "@t3tools/contracts";
import { projectComposerContextForProvider } from "../composerContextReferences.ts";

export interface QueuedSteerBatchMessage {
  readonly text: string;
  readonly attachments: ReadonlyArray<ChatAttachment>;
  readonly context?: OrchestrationMessageContext | undefined;
  readonly senderThreadId?: ThreadId | undefined;
  readonly createdBy: string;
  readonly creationSource: string;
}
export interface QueuedSteerBatchLimits {
  readonly maxInputChars: number;
  readonly maxAttachments: number;
  readonly maxImageBytes: number;
  readonly maxItems: number;
}
export interface QueuedSteerBatchCandidate {
  readonly message: QueuedSteerBatchMessage;
  readonly eligible: boolean;
}

export function formatQueuedSteerBatch(messages: ReadonlyArray<QueuedSteerBatchMessage>) {
  const text = messages
    .map((message, index) => {
      const projected = projectComposerContextForProvider({
        text: message.text,
        records: message.context?.records ?? [],
      });
      // A sole slash input retains its leading slash for the provider's skill parser.
      if (messages.length === 1 && message.text.trimStart().startsWith("/")) return projected;
      const sender =
        message.senderThreadId === undefined
          ? message.createdBy
          : `thread ${message.senderThreadId}`;
      return `[Queued message ${index + 1}/${messages.length}; sender: ${sender}; source: ${message.creationSource}]\n${projected}`;
    })
    .join("\n\n");
  return { text, attachments: messages.flatMap((message) => message.attachments) };
}

export function planQueuedSteerBatch(
  candidates: ReadonlyArray<QueuedSteerBatchCandidate>,
  limits: QueuedSteerBatchLimits = QUEUED_STEER_BATCH_LIMITS,
) {
  let count = 0;
  let reason: "complete" | "ineligible" | "batch_over_budget" | "first_member_over_budget" =
    "complete";
  let payload = { text: "", attachments: [] as Array<ChatAttachment> };
  const messages: Array<QueuedSteerBatchMessage> = [];
  for (const candidate of candidates) {
    if (
      !candidate.eligible ||
      (count > 0 &&
        (candidate.message.text.trimStart().startsWith("/") ||
          messages[0]!.text.trimStart().startsWith("/")))
    ) {
      reason = "ineligible";
      break;
    }
    const next = formatQueuedSteerBatch([...messages, candidate.message]);
    const imageBytes = next.attachments.reduce(
      (sum, attachment) =>
        sum +
        (attachment.type === "image" ||
        isProviderSendTurnSupportedImageMimeType(attachment.mimeType)
          ? attachment.sizeBytes
          : 0),
      0,
    );
    if (
      count + 1 > limits.maxItems ||
      next.text.length > limits.maxInputChars ||
      next.attachments.length > limits.maxAttachments ||
      imageBytes > limits.maxImageBytes
    ) {
      reason = count === 0 ? "first_member_over_budget" : "batch_over_budget";
      break;
    }
    messages.push(candidate.message);
    payload = next;
    count += 1;
  }
  return { count, remainingCount: candidates.length - count, reason, ...payload };
}
