import { buildTemporaryWorktreeBranchName } from "@t3tools/shared/git";
import { randomHex } from "../../lib/uuid";
import { buildProjectThreadStartTurnInput } from "../../lib/projectThreadStartTurn";
import {
  serializeComposerMessageForServer,
  uploadedComposerContext,
} from "../../lib/composerContext";
import type { PreparedTurnAttachments } from "../../lib/attachmentUpload";
import type {
  QueuedThreadMessage,
  QueuedThreadCreation,
  ThreadSettingsSnapshot,
} from "../../state/thread-outbox-model";

export function buildQueuedThreadCreationStartTurnInput(input: {
  readonly message: QueuedThreadMessage;
  readonly creation: QueuedThreadCreation;
  readonly projectCwd: string;
  readonly attachments: PreparedTurnAttachments["attachments"];
  readonly settings: ThreadSettingsSnapshot;
  readonly serverResolvesWorktreeBase: boolean;
  readonly inlineMessageContext?: boolean;
}) {
  const { message, creation, settings } = input;
  return buildProjectThreadStartTurnInput({
    projectId: creation.projectId,
    projectCwd: input.projectCwd,
    threadId: message.threadId,
    commandId: message.commandId,
    messageId: message.messageId,
    createdAt: message.createdAt,
    titleText: message.text,
    ...serializeComposerMessageForServer(
      message.text.trim(),
      uploadedComposerContext(message.context, message.attachments, input.attachments),
      input.inlineMessageContext === true,
    ),
    uploadedAttachments: input.attachments,
    modelSelection: settings.modelSelection,
    runtimeMode: settings.runtimeMode,
    interactionMode: settings.interactionMode,
    workspaceMode: creation.workspaceMode,
    branch: creation.branch,
    worktreePath: creation.worktreePath,
    startFromOrigin: creation.startFromOrigin ?? false,
    serverResolvesWorktreeBase: input.serverResolvesWorktreeBase,
    worktreeBranchName: buildTemporaryWorktreeBranchName(randomHex),
  });
}
