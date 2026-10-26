import {
  deriveThreadTitleSeed,
  type StartThreadTurnInput,
} from "@t3tools/client-runtime/operations";

import type { PreparedTurnAttachments } from "../../lib/attachmentUpload";
import {
  serializeComposerMessageForServer,
  uploadedComposerContext,
} from "../../lib/composerContext";
import type { QueuedThreadMessage, ThreadSettingsSnapshot } from "../../state/thread-outbox-model";

export function buildQueuedFollowUpStartTurnInput(input: {
  readonly message: QueuedThreadMessage;
  readonly settings: ThreadSettingsSnapshot;
  readonly attachments: PreparedTurnAttachments["attachments"];
  readonly inlineMessageContext: boolean;
}): StartThreadTurnInput {
  const { message, settings } = input;
  return {
    commandId: message.commandId,
    creationSource: "mobile",
    threadId: message.threadId,
    message: {
      messageId: message.messageId,
      role: "user",
      ...serializeComposerMessageForServer(
        message.text,
        uploadedComposerContext(message.context, message.attachments, input.attachments),
        input.inlineMessageContext,
      ),
      attachments: input.attachments,
    },
    modelSelection: settings.modelSelection,
    ...(message.modelSelectionExplicit === undefined
      ? {}
      : { modelSelectionExplicit: message.modelSelectionExplicit }),
    titleSeed: deriveThreadTitleSeed({ text: message.text, attachments: message.attachments }),
    runtimeMode: settings.runtimeMode,
    interactionMode: settings.interactionMode,
    createdAt: message.createdAt,
    dispatchMode: message.dispatchMode ?? "start",
  };
}
