import { buildQueuedFollowUpStartTurnInput } from "../jones/threadConfiguration/queuedFollowUp";
import { acknowledgedThreadMessagesAtom } from "./acknowledged-thread-messages";
import {
  CommandId,
  ComposerContextId,
  EnvironmentId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import { formatComposerContextReference } from "@t3tools/shared/composerContextReferences";
import { upgradeLegacyContextMessage } from "@t3tools/shared/composerContextLegacy";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import type { PreparedTurnAttachments } from "../lib/attachmentUpload";

const harness = vi.hoisted(() => ({
  canOperate: true,
  manager: null as unknown as ReturnType<
    typeof import("./thread-outbox-manager").createThreadOutboxManager
  >,
  removePersistedFile: vi.fn(async () => undefined),
  removeOutboxMessage: vi.fn(async (_message: QueuedThreadMessage) => undefined),
  prepareTurnAttachments: vi.fn<typeof import("../lib/attachmentUpload").prepareTurnAttachments>(),
  draftFile: (() => {
    let document = "";
    let writeError: Error | null = null;
    return {
      setDocument(value: unknown) {
        document = JSON.stringify(value);
      },
      setWriteError(error: Error | null) {
        writeError = error;
      },
      Directory: class {
        create() {}
      },
      File: class {
        exists = true;
        parentDirectory = null;

        create() {}

        moveSync() {}

        async text() {
          return document;
        }

        write(value: string) {
          if (writeError) {
            throw writeError;
          }
          document = value;
        }
      },
    };
  })(),
}));

vi.mock("react-native", () => ({ Alert: { alert: vi.fn() } }));

vi.mock("expo-file-system", () => ({
  Directory: harness.draftFile.Directory,
  File: harness.draftFile.File,
  Paths: { document: "/documents" },
}));

vi.mock("../lib/composerImages", () => ({
  removePersistedComposerAttachmentFile: harness.removePersistedFile,
}));

vi.mock("../lib/uuid", () => ({
  uuidv4: () => "00000000-0000-4000-8000-000000000000",
  randomHex: () => "abcd",
}));

vi.mock("../lib/attachmentUpload", () => ({
  prepareTurnAttachments: harness.prepareTurnAttachments,
}));

vi.mock("./entities", () => ({
  useProjects: () => [],
  useServerConfigs: () => new Map(),
  useThreadShells: () => [],
}));

vi.mock("./server", async () => {
  const { Atom } = await import("effect/reactivity");
  return { serverEnvironment: { configValueAtom: Atom.family(() => Atom.make(null)) } };
});

vi.mock("./git", () => ({ gitEnvironment: {} }));

vi.mock("./threads", () => ({
  threadEnvironment: {},
}));

vi.mock("./use-atom-command", () => ({
  useAtomCommand: () => async () => undefined,
}));

vi.mock("./session", () => ({
  readEnvironmentScope: () => harness.canOperate,
  useEnvironmentsWithScope: () => new Set(),
}));

vi.mock("./use-thread-outbox", async () => {
  const { Atom } = await import("effect/reactivity");
  return {
    editingQueuedMessageIdsAtom: Atom.make<Record<string, boolean>>({}).pipe(Atom.keepAlive),
    useThreadOutboxMessages: () => ({}),
    useThreadOutboxShellStatuses: () => new Map(),
  };
});

vi.mock("./use-remote-environment-registry", () => ({
  useRemoteConnectionStatus: () => ({ connectedEnvironments: [] }),
}));

vi.mock("./thread-outbox", async () => {
  const { createThreadOutboxManager } = await import("./thread-outbox-manager");
  const { appAtomRegistry } = await import("./atom-registry");
  harness.manager = createThreadOutboxManager({
    registry: appAtomRegistry,
    storage: {
      load: async () => ({ messages: [], errors: [] }),
      write: async () => undefined,
      remove: (message) => harness.removeOutboxMessage(message),
    },
  });
  const manager = harness.manager;
  return {
    threadOutboxManager: manager,
    flushThreadOutbox: async () => undefined,
    confirmThreadOutboxMessageQueued: (message: never) => manager.confirmQueued(message),
    updateThreadOutboxMessage: (message: never, expectedRevision?: number) =>
      manager.update(message, expectedRevision),
    threadOutboxRevision: (messageId: never) => manager.revisionOf(messageId),
  };
});

import { appAtomRegistry } from "./atom-registry";
import { pendingThreadCreationOutcomesAtom } from "./pending-thread-creation";
import {
  decodeQueuedThreadMessage,
  encodeQueuedThreadMessage,
  resolveQueuedThreadSettings,
  type QueuedThreadMessage,
} from "./thread-outbox-model";
import { clearThreadComposerError, threadComposerErrorsAtom } from "./thread-composer-error";
import * as composerDrafts from "./use-composer-drafts";
import { recoverFailedThreadDraft } from "./recover-failed-thread-draft";
import { editingQueuedMessageIdsAtom } from "./use-thread-outbox";
import {
  buildQueuedThreadCreationStartTurnInput,
  completeQueuedMessageDelivery,
  prepareQueuedMessageAttachments,
  recoverEditedCreationAfterDelivery,
  removeAcknowledgedExistingThreadMessage,
  restoreRejectedQueuedMessage,
} from "./use-thread-outbox-drain";

function queuedMessage(input: {
  readonly messageId: string;
  readonly text: string;
  readonly fileUri?: string;
}): QueuedThreadMessage {
  return {
    environmentId: EnvironmentId.make("environment-1"),
    threadId: ThreadId.make("thread-1"),
    messageId: MessageId.make(input.messageId),
    commandId: CommandId.make(`command-${input.messageId}`),
    text: input.text,
    attachments: input.fileUri
      ? [
          {
            id: `file-${input.messageId}`,
            type: "file",
            name: "report.pdf",
            mimeType: "application/pdf",
            sizeBytes: 42,
            fileUri: input.fileUri,
          },
        ]
      : [],
    createdAt: "2026-08-24T12:00:00.000Z",
  };
}

function withReusedFileUpload(
  message: QueuedThreadMessage,
  attachmentId: string,
): QueuedThreadMessage {
  return {
    ...message,
    attachments: message.attachments.map((attachment) =>
      attachment.type === "file"
        ? {
            ...attachment,
            uploadedAttachmentId: attachmentId,
            uploadEnvironmentId: message.environmentId,
          }
        : attachment,
    ),
  };
}

function remainingMessages(): ReadonlyArray<QueuedThreadMessage> {
  return Object.values(appAtomRegistry.get(harness.manager.queuedMessagesByThreadKeyAtom)).flat();
}

beforeEach(() => {
  appAtomRegistry.set(acknowledgedThreadMessagesAtom, []);
  harness.canOperate = true;
  harness.draftFile.setDocument({ schemaVersion: 1, drafts: {} });
});

afterEach(() => {
  appAtomRegistry.set(harness.manager.queuedMessagesByThreadKeyAtom, {});
  appAtomRegistry.set(composerDrafts.composerDraftsAtom, {});
  appAtomRegistry.set(composerDrafts.composerCloudDraftsAtom, { accountId: null, signedOut: {} });
  appAtomRegistry.set(editingQueuedMessageIdsAtom, {});
  appAtomRegistry.set(pendingThreadCreationOutcomesAtom, {});
  appAtomRegistry.set(threadComposerErrorsAtom, {});
  harness.draftFile.setWriteError(null);
  harness.removePersistedFile.mockClear();
  harness.removeOutboxMessage.mockClear();
  harness.prepareTurnAttachments.mockReset();
});

describe("queued follow-up model selection", () => {
  it.each([false, true, undefined])(
    "forwards persisted selection intent (%s) with stable replay identity and message content",
    (modelSelectionExplicit) => {
      const captured = { instanceId: ProviderInstanceId.make("before"), model: "old-model" };
      const current = {
        instanceId: ProviderInstanceId.make("after"),
        model: "new-model",
        options: [{ id: "reasoningEffort", value: "high" }],
      };
      const message = decodeQueuedThreadMessage(
        encodeQueuedThreadMessage({
          ...queuedMessage({
            messageId: "follow-up",
            text: "send this",
            fileUri: "file:///notes.pdf",
          }),
          modelSelection: captured,
          ...(modelSelectionExplicit === undefined ? {} : { modelSelectionExplicit }),
          runtimeMode: "approval-required",
          interactionMode: "plan",
          dispatchMode: "queue",
          context: {
            version: 1,
            records: [
              {
                version: 1,
                kind: "terminal",
                contextId: ComposerContextId.make("build-output"),
                label: "Build",
                terminalId: "main",
                terminalLabel: "Terminal",
                lineStart: 1,
                lineEnd: 1,
                text: "failed build",
              },
            ],
          },
        }),
      );
      const attachments: PreparedTurnAttachments["attachments"] = [
        {
          type: "file",
          id: "uploaded-file",
          name: "notes.pdf",
          mimeType: "application/pdf",
          sizeBytes: 42,
        },
      ];
      const settings = resolveQueuedThreadSettings(message, {
        modelSelection: current,
        runtimeMode: "full-access",
        interactionMode: "default",
      });
      const input = buildQueuedFollowUpStartTurnInput({
        message,
        settings,
        attachments,
        inlineMessageContext: true,
      });
      expect(input).toMatchObject({
        commandId: message.commandId,
        threadId: message.threadId,
        createdAt: message.createdAt,
        dispatchMode: "queue",
        modelSelection: modelSelectionExplicit === false ? current : captured,
        runtimeMode: "approval-required",
        interactionMode: "plan",
        message: {
          messageId: message.messageId,
          text: message.text,
          attachments,
          context: message.context,
        },
      });
      if (modelSelectionExplicit === undefined) {
        expect(input).not.toHaveProperty("modelSelectionExplicit");
      } else {
        expect(input.modelSelectionExplicit).toBe(modelSelectionExplicit);
      }
    },
  );

  it.each([false, true, undefined])(
    "restores rejected selection intent (%s) without pinning inherited snapshots",
    async (modelSelectionExplicit) => {
      const message: QueuedThreadMessage = {
        ...queuedMessage({ messageId: "rejected-selection", text: "retry me" }),
        modelSelection: { instanceId: ProviderInstanceId.make("before"), model: "old-model" },
        ...(modelSelectionExplicit === undefined ? {} : { modelSelectionExplicit }),
      };
      await harness.manager.enqueue(message);
      await expect(restoreRejectedQueuedMessage(message, "rejected")).resolves.toBe("restored");
      const draft = composerDrafts.getComposerDraftSnapshot(
        `${message.environmentId}:${message.threadId}`,
      );
      expect(draft.text).toBe(message.text);
      expect(draft.modelSelection).toEqual(
        modelSelectionExplicit === false ? undefined : message.modelSelection,
      );
    },
  );

  it("preserves a later draft pick while restoring an inherited rejected message", async () => {
    const message: QueuedThreadMessage = {
      ...queuedMessage({ messageId: "later-pick", text: "retry me" }),
      modelSelection: { instanceId: ProviderInstanceId.make("before"), model: "old-model" },
      modelSelectionExplicit: false,
    };
    const later = { instanceId: ProviderInstanceId.make("later"), model: "later-model" };
    const key = `${message.environmentId}:${message.threadId}`;
    composerDrafts.updateComposerDraftSettings(key, { modelSelection: later });
    await harness.manager.enqueue(message);
    await expect(restoreRejectedQueuedMessage(message, "rejected")).resolves.toBe("restored");
    expect(composerDrafts.getComposerDraftSnapshot(key).modelSelection).toEqual(later);
  });
});

describe("queued worktree creation command", () => {
  it.each([true, false])(
    "preserves queued context and uploaded attachments at delivery (%s)",
    (inlineMessageContext) => {
      const terminal = {
        version: 1 as const,
        kind: "terminal" as const,
        contextId: ComposerContextId.make("build-output"),
        label: "Build output",
        terminalId: "main",
        terminalLabel: "Terminal",
        lineStart: 4,
        lineEnd: 5,
        text: "build failed\nretry",
      };
      const image = {
        version: 1 as const,
        kind: "image" as const,
        contextId: ComposerContextId.make("screenshot"),
        label: "Screenshot",
        attachmentId: "draft-photo",
        name: "shot.png",
        mimeType: "image/png",
        sizeBytes: 123,
      };
      const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.6-sol" };
      const uploadedAttachment = {
        type: "image" as const,
        id: "server-photo",
        name: image.name,
        mimeType: image.mimeType,
        sizeBytes: image.sizeBytes,
      };
      const context = { version: 1 as const, records: [terminal, image] };
      const creation = {
        projectId: ProjectId.make("project"),
        workspaceMode: "worktree" as const,
        branch: null,
        worktreePath: null,
      };
      const message = decodeQueuedThreadMessage(
        encodeQueuedThreadMessage({
          ...queuedMessage({
            messageId: "context-worktree",
            text: context.records.map(formatComposerContextReference).join(" "),
          }),
          modelSelection,
          creation,
          context,
          attachments: [
            {
              ...uploadedAttachment,
              id: image.attachmentId,
              fileUri: "file:///draft-photo.png",
              previewUri: "file:///draft-photo.png",
            },
          ],
        }),
      );
      const input = buildQueuedThreadCreationStartTurnInput({
        message,
        creation,
        projectCwd: "/current/workspace",
        attachments: [uploadedAttachment],
        settings: { modelSelection, runtimeMode: "full-access", interactionMode: "default" },
        serverResolvesWorktreeBase: true,
        inlineMessageContext,
      });

      expect(input.message.attachments).toEqual([uploadedAttachment]);
      if (inlineMessageContext) {
        expect(input.message.text).toBe(message.text);
        expect(input.message.context?.records).toEqual([
          terminal,
          { ...image, attachmentId: uploadedAttachment.id },
        ]);
      } else {
        expect(input.message).not.toHaveProperty("context");
        expect(input.message.text).not.toContain("t3-context://");
        expect(
          upgradeLegacyContextMessage(input.message.text).records.find(
            (record) => record.kind === "terminal",
          ),
        ).toMatchObject({
          text: terminal.text,
          lineStart: terminal.lineStart,
          lineEnd: terminal.lineEnd,
        });
      }
      expect(message.context).toEqual(context);
    },
  );

  it.each([true, false])(
    "delivers persisted automatic intent with current server support (%s)",
    (serverResolvesWorktreeBase) => {
      const message = decodeQueuedThreadMessage(
        encodeQueuedThreadMessage({
          ...queuedMessage({ messageId: "automatic-worktree", text: "  queued task  " }),
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.6-sol" },
          creation: {
            projectId: ProjectId.make("project"),
            workspaceMode: "worktree",
            branch: null,
            worktreePath: null,
            startFromOrigin: true,
          },
        }),
      );
      const input = buildQueuedThreadCreationStartTurnInput({
        message,
        creation: message.creation!,
        projectCwd: "/current/workspace",
        attachments: [],
        settings: {
          modelSelection: message.modelSelection!,
          runtimeMode: "full-access",
          interactionMode: "default",
        },
        serverResolvesWorktreeBase,
      });

      expect(input.bootstrap.prepareWorktree).toMatchObject({
        projectCwd: "/current/workspace",
        startFromOrigin: true,
      });
      expect(input.bootstrap.prepareWorktree).not.toHaveProperty("baseBranch");
      expect(input.bootstrap.createThread.branch).toBeNull();
      expect(input.serverResolvesWorktreeBase).toBe(serverResolvesWorktreeBase);
      expect(input.commandId).toBe(message.commandId);
      expect(input.threadId).toBe(message.threadId);
      expect(input.message.messageId).toBe(message.messageId);
      expect(input.createdAt).toBe(message.createdAt);
      expect(input.message.text).toBe("queued task");
    },
  );

  it("keeps a persisted explicit base and origin setting when delivered to an older server", () => {
    const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.6-sol" };
    const message = decodeQueuedThreadMessage(
      encodeQueuedThreadMessage({
        ...queuedMessage({ messageId: "explicit-worktree", text: "queued task" }),
        modelSelection,
        creation: {
          projectId: ProjectId.make("project"),
          workspaceMode: "worktree",
          branch: "upstream/release",
          worktreePath: null,
          startFromOrigin: false,
        },
      }),
    );
    const input = buildQueuedThreadCreationStartTurnInput({
      message,
      creation: message.creation!,
      projectCwd: "/current/workspace",
      attachments: [],
      settings: { modelSelection, runtimeMode: "full-access", interactionMode: "default" },
      serverResolvesWorktreeBase: false,
    });

    expect(input.bootstrap.prepareWorktree).toMatchObject({ baseBranch: "upstream/release" });
    expect(input.bootstrap.prepareWorktree).not.toHaveProperty("startFromOrigin");
    expect(input.bootstrap.createThread.branch).toBe("upstream/release");
  });
});

describe("thread outbox attachment preparation", () => {
  it("keeps a denied message queued without starting its attachment upload", async () => {
    const message = queuedMessage({
      messageId: "denied-queued-message",
      text: "Keep this queued",
      fileUri: "file:///documents/keep.pdf",
    });
    await harness.manager.enqueue(message);
    harness.prepareTurnAttachments.mockResolvedValueOnce({
      status: "ready",
      attachments: [],
      draftAttachments: message.attachments,
      pendingAttachmentIds: [],
    });
    harness.canOperate = false;
    await expect(prepareQueuedMessageAttachments(message)).resolves.toEqual({
      status: "abandoned",
    });
    expect(harness.prepareTurnAttachments).not.toHaveBeenCalled();
    expect(remainingMessages()).toEqual([message]);
  });

  it("abandons reused uploads when an editor saves changed text during verification", async () => {
    const message = withReusedFileUpload(
      queuedMessage({
        messageId: "message-reused-upload-race",
        text: "original text",
        fileUri: "file:///documents/t3-composer-attachments/reused.pdf",
      }),
      "pending-reused-upload",
    );
    const preparationStarted = Promise.withResolvers<void>();
    const preparationBarrier = Promise.withResolvers<PreparedTurnAttachments>();
    harness.prepareTurnAttachments.mockImplementationOnce(async () => {
      preparationStarted.resolve();
      return preparationBarrier.promise;
    });
    await harness.manager.enqueue(message);
    appAtomRegistry.set(editingQueuedMessageIdsAtom, { [message.messageId]: true });

    const preparation = prepareQueuedMessageAttachments(message);
    await preparationStarted.promise;
    const edited = { ...message, text: "saved editor text" };
    await harness.manager.update(edited);
    appAtomRegistry.set(editingQueuedMessageIdsAtom, {});
    preparationBarrier.resolve({
      status: "ready",
      attachments: [],
      draftAttachments: message.attachments,
      pendingAttachmentIds: ["pending-reused-upload"],
    });

    await expect(preparation).resolves.toEqual({ status: "abandoned" });
    expect(remainingMessages()).toEqual([edited]);
  });

  it("uses the known next revision after persisting uploaded references", async () => {
    const message = queuedMessage({
      messageId: "message-new-upload-revision",
      text: "upload this file",
      fileUri: "file:///documents/t3-composer-attachments/new.pdf",
    });
    const uploadedAttachments = message.attachments.map((attachment) =>
      attachment.type === "file"
        ? {
            ...attachment,
            uploadedAttachmentId: "pending-new-upload",
            uploadEnvironmentId: message.environmentId,
          }
        : attachment,
    );
    harness.prepareTurnAttachments.mockImplementationOnce(async (input) => {
      expect(await input.persistUploadedReferences?.(uploadedAttachments)).toBe("persisted");
      return {
        status: "ready",
        attachments: [],
        draftAttachments: uploadedAttachments,
        pendingAttachmentIds: ["pending-new-upload"],
      };
    });
    await harness.manager.enqueue(message);
    const revision = harness.manager.revisionOf(message.messageId);

    const result = await prepareQueuedMessageAttachments(message);

    expect(result).toMatchObject({
      status: "ready",
      persistedMessage: { attachments: uploadedAttachments },
      deliveryRevision: revision + 1,
    });
    expect(harness.manager.revisionOf(message.messageId)).toBe(revision + 1);
  });

  it("does not prepare a payload that was already replaced", async () => {
    const message = queuedMessage({ messageId: "message-stale-before-upload", text: "old" });
    await harness.manager.enqueue(message);
    const edited = { ...message, text: "new" };
    await harness.manager.update(edited);

    await expect(prepareQueuedMessageAttachments(message)).resolves.toEqual({
      status: "abandoned",
    });
    expect(harness.prepareTurnAttachments).not.toHaveBeenCalled();
    expect(remainingMessages()).toEqual([edited]);
  });
});

describe("thread outbox drain delivery cleanup", () => {
  it("removes an acknowledged outbox item even when the sign-out archive write fails", async () => {
    const message = queuedMessage({ messageId: "archive-write-failure", text: "Delivered" });
    await harness.manager.enqueue(message);
    await composerDrafts.archiveCloudComposerDrafts("account-a", new Set([message.environmentId]));
    harness.draftFile.setWriteError(new Error("Draft storage unavailable"));

    await expect(
      completeQueuedMessageDelivery(message, harness.manager.revisionOf(message.messageId)),
    ).resolves.toBe("removed");
    expect(remainingMessages()).toEqual([]);

    harness.draftFile.setWriteError(null);
    await composerDrafts.flushComposerDrafts();
    appAtomRegistry.set(composerDrafts.composerCloudDraftsAtom, { accountId: null, signedOut: {} });
    composerDrafts.resetComposerDraftsLoadState();
    await composerDrafts.restoreCloudComposerDrafts("account-a");
    expect(remainingMessages()).toEqual([]);
  });

  it.each([false, true])(
    "does not restore a message delivered after the sign-out snapshot (outbox already cleared: %s)",
    async (cleared) => {
      const message = queuedMessage({
        messageId: "delivered-during-sign-out",
        text: "Already delivered",
      });
      await harness.manager.enqueue(message);
      const deliveryRevision = harness.manager.revisionOf(message.messageId);
      await composerDrafts.archiveCloudComposerDrafts(
        "account-a",
        new Set([message.environmentId]),
      );
      expect(
        appAtomRegistry.get(composerDrafts.composerCloudDraftsAtom).signedOut["account-a"]
          ?.queuedMessages,
      ).toEqual([message]);

      if (cleared) await harness.manager.clearEnvironment(message.environmentId);
      await expect(completeQueuedMessageDelivery(message, deliveryRevision)).resolves.toBe(
        cleared ? "edited" : "removed",
      );

      appAtomRegistry.set(composerDrafts.composerCloudDraftsAtom, {
        accountId: null,
        signedOut: {},
      });
      composerDrafts.resetComposerDraftsLoadState();
      await composerDrafts.restoreCloudComposerDrafts("account-a");
      expect(remainingMessages()).toEqual([]);
    },
  );

  it("preserves an archived edit when an older payload finishes delivery", async () => {
    const message = queuedMessage({ messageId: "edited-during-sign-out", text: "Original" });
    await harness.manager.enqueue(message);
    const deliveryRevision = harness.manager.revisionOf(message.messageId);
    const edited = { ...message, text: "Keep this edit" };
    await harness.manager.update(edited);
    await composerDrafts.archiveCloudComposerDrafts("account-a", new Set([message.environmentId]));
    await harness.manager.clearEnvironment(message.environmentId);
    await expect(completeQueuedMessageDelivery(message, deliveryRevision)).resolves.toBe("edited");
    await composerDrafts.restoreCloudComposerDrafts("account-a");
    expect(remainingMessages()).toEqual([edited]);
  });

  it("preserves an archived selection-intent edit when an older payload finishes delivery", async () => {
    const message: QueuedThreadMessage = {
      ...queuedMessage({ messageId: "intent-edited-during-sign-out", text: "Original" }),
      modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.5" },
      modelSelectionExplicit: false,
    };
    await harness.manager.enqueue(message);
    const deliveryRevision = harness.manager.revisionOf(message.messageId);
    const edited = { ...message, modelSelectionExplicit: true };
    await harness.manager.update(edited);
    await composerDrafts.archiveCloudComposerDrafts("account-a", new Set([message.environmentId]));
    await harness.manager.clearEnvironment(message.environmentId);
    await expect(completeQueuedMessageDelivery(message, deliveryRevision)).resolves.toBe("edited");
    await composerDrafts.restoreCloudComposerDrafts("account-a");
    expect(remainingMessages()).toEqual([edited]);
  });

  it("retries only cleanup after an acknowledged send removal fails", async () => {
    const message = queuedMessage({ messageId: "message-acknowledged", text: "delivered" });
    const acknowledged = new Set([message.messageId]);
    harness.removeOutboxMessage.mockRejectedValueOnce(new Error("storage unavailable"));
    await harness.manager.enqueue(message);

    await expect(removeAcknowledgedExistingThreadMessage(message, acknowledged)).resolves.toBe(
      false,
    );
    expect(remainingMessages()).toEqual([message]);
    expect(acknowledged).toEqual(new Set([message.messageId]));

    await expect(removeAcknowledgedExistingThreadMessage(message, acknowledged)).resolves.toBe(
      true,
    );
    expect(remainingMessages()).toEqual([]);
    expect(acknowledged).toEqual(new Set());
  });

  it("keeps an edited message and its files when delivery cleanup loses the revision race", async () => {
    const message = queuedMessage({
      messageId: "message-edited",
      text: "original",
      fileUri: "file:///documents/t3-composer-attachments/report.pdf",
    });
    await harness.manager.enqueue(message);
    const deliveryRevision = harness.manager.revisionOf(message.messageId);
    const edited = { ...message, text: "edited while the turn delivered" };
    await harness.manager.update(edited);

    await expect(completeQueuedMessageDelivery(message, deliveryRevision)).resolves.toBe("edited");

    expect(remainingMessages()).toEqual([edited]);
    expect(harness.removePersistedFile).not.toHaveBeenCalled();
  });

  it("removes the delivered message when no edit was accepted", async () => {
    const message = queuedMessage({ messageId: "message-clean", text: "hello" });
    await harness.manager.enqueue(message);
    const deliveryRevision = harness.manager.revisionOf(message.messageId);

    await expect(completeQueuedMessageDelivery(message, deliveryRevision)).resolves.toBe("removed");

    expect(remainingMessages()).toEqual([]);
    expect(appAtomRegistry.get(acknowledgedThreadMessagesAtom)).toEqual([message]);
  });

  it("keeps a delivered message when its editor opens during storage removal", async () => {
    const message = queuedMessage({
      messageId: "message-editor-removal-race",
      text: "keep editor changes",
      fileUri: "file:///documents/t3-composer-attachments/editor-race.pdf",
    });
    const removeStarted = Promise.withResolvers<void>();
    const removeBarrier = Promise.withResolvers<void>();
    harness.removeOutboxMessage.mockImplementationOnce(async () => {
      removeStarted.resolve();
      await removeBarrier.promise;
    });
    await harness.manager.enqueue(message);
    const deliveryRevision = harness.manager.revisionOf(message.messageId);

    const cleanup = completeQueuedMessageDelivery(message, deliveryRevision);
    await removeStarted.promise;
    appAtomRegistry.set(editingQueuedMessageIdsAtom, { [message.messageId]: true });
    removeBarrier.resolve();

    await expect(cleanup).resolves.toBe("edited");
    expect(remainingMessages()).toEqual([message]);
    expect(harness.removePersistedFile).not.toHaveBeenCalled();
  });
});

describe("thread outbox delivered creation recovery", () => {
  it("keeps an edit accepted while the older payload is persisted to the draft", async () => {
    const message = queuedMessage({
      messageId: "message-recovery-race",
      text: "original queued text",
      fileUri: "file:///documents/t3-composer-attachments/report.pdf",
    });
    const originalMergeComposerDraftContent = composerDrafts.mergeComposerDraftContent;
    const mergeCompleted = Promise.withResolvers<void>();
    const releaseRecovery = Promise.withResolvers<void>();
    const mergeSpy = vi
      .spyOn(composerDrafts, "mergeComposerDraftContent")
      .mockImplementation(async (draftKey, content) => {
        const result = await originalMergeComposerDraftContent(draftKey, content);
        mergeCompleted.resolve();
        await releaseRecovery.promise;
        return result;
      });

    try {
      await harness.manager.enqueue(message);
      const recovery = recoverEditedCreationAfterDelivery(message);
      await mergeCompleted.promise;

      const newer = { ...message, text: "edited while recovery persisted the draft" };
      await harness.manager.update(newer);

      releaseRecovery.resolve();
      await expect(recovery).resolves.toBe(false);

      expect(remainingMessages()).toEqual([newer]);
      expect(
        composerDrafts.getComposerDraftSnapshot(`${message.environmentId}:${message.threadId}`),
      ).toMatchObject({ text: message.text, attachments: [] });
      expect(harness.removePersistedFile).not.toHaveBeenCalled();
    } finally {
      releaseRecovery.resolve();
      mergeSpy.mockRestore();
    }
  });

  it("leaves recovery to an editor that opens while the draft persists", async () => {
    const message = queuedMessage({
      messageId: "message-recovery-editor",
      text: "recover this text",
      fileUri: "file:///documents/t3-composer-attachments/editor.pdf",
    });
    const originalMergeComposerDraftContent = composerDrafts.mergeComposerDraftContent;
    const mergeCompleted = Promise.withResolvers<void>();
    const releaseRecovery = Promise.withResolvers<void>();
    const mergeSpy = vi
      .spyOn(composerDrafts, "mergeComposerDraftContent")
      .mockImplementation(async (draftKey, content) => {
        const result = await originalMergeComposerDraftContent(draftKey, content);
        mergeCompleted.resolve();
        await releaseRecovery.promise;
        return result;
      });

    try {
      await harness.manager.enqueue(message);
      const recovery = recoverEditedCreationAfterDelivery(message);
      await mergeCompleted.promise;
      appAtomRegistry.set(editingQueuedMessageIdsAtom, { [message.messageId]: true });

      releaseRecovery.resolve();
      await expect(recovery).resolves.toBe(true);

      expect(remainingMessages()).toEqual([message]);
      expect(
        composerDrafts.getComposerDraftSnapshot(`${message.environmentId}:${message.threadId}`),
      ).toMatchObject({ text: message.text, attachments: [] });
      expect(harness.removePersistedFile).not.toHaveBeenCalled();
    } finally {
      releaseRecovery.resolve();
      mergeSpy.mockRestore();
    }
  });

  it("retries a failed removal without duplicating recovered draft content", async () => {
    const message = queuedMessage({
      messageId: "message-recovery-removal",
      text: "recover once",
      fileUri: "file:///documents/t3-composer-attachments/retry.pdf",
    });
    const draftKey = `${message.environmentId}:${message.threadId}`;
    const removeSpy = vi
      .spyOn(harness.manager, "remove")
      .mockRejectedValueOnce(new Error("storage unavailable"));

    try {
      await harness.manager.enqueue(message);

      await expect(recoverEditedCreationAfterDelivery(message)).resolves.toBe(false);
      expect(remainingMessages()).toEqual([message]);

      await expect(recoverEditedCreationAfterDelivery(message)).resolves.toBe(true);

      const draft = composerDrafts.getComposerDraftSnapshot(draftKey);
      expect(draft.text).toBe(message.text);
      expect(draft.attachments).toEqual(message.attachments);
      expect(remainingMessages()).toEqual([]);
      expect(harness.removePersistedFile).not.toHaveBeenCalled();
    } finally {
      removeSpy.mockRestore();
    }
  });

  it("keeps the queue entry when the recovered draft cannot persist", async () => {
    const message = queuedMessage({
      messageId: "message-recovery-persistence",
      text: "recover after persistence returns",
    });
    await harness.manager.enqueue(message);
    harness.draftFile.setWriteError(new Error("disk full"));

    await expect(recoverEditedCreationAfterDelivery(message)).resolves.toBe(false);

    expect(remainingMessages()).toEqual([message]);
  });
});

describe("thread outbox recovery rollback", () => {
  it("reopens a rejected task with setup edits and every attachment, even above the send cap", async () => {
    const message = queuedMessage({ messageId: "failed-setup", text: "Original prompt" });
    const sourceKey = `${message.environmentId}:${message.threadId}`;
    const targetKey = "new-task:restored-failed-setup";
    const files = Array.from(
      { length: 10 },
      (_, index) =>
        queuedMessage({
          messageId: `attachment-${index}`,
          text: "",
          fileUri: `file:///file-${index}`,
        }).attachments[0]!,
    );
    appAtomRegistry.set(composerDrafts.composerDraftsAtom, {
      [targetKey]: { text: message.text, attachments: files.slice(0, 8) },
      [sourceKey]: { text: "Please include tests", attachments: files.slice(8) },
    });
    await recoverFailedThreadDraft(message);
    expect(composerDrafts.getComposerDraftSnapshot(targetKey)).toMatchObject({
      text: "Original prompt\n\nPlease include tests",
      attachments: files,
    });
    expect(composerDrafts.getComposerDraftSnapshot(sourceKey)).toMatchObject({
      text: "",
      attachments: [],
    });
    await recoverFailedThreadDraft(message);
    expect(composerDrafts.getComposerDraftSnapshot(targetKey).text).toBe(
      "Original prompt\n\nPlease include tests",
    );
  });

  it("keeps setup edits recoverable when saving their recovery draft fails", async () => {
    const message = queuedMessage({ messageId: "failed-save", text: "Original prompt" });
    const sourceKey = `${message.environmentId}:${message.threadId}`;
    appAtomRegistry.set(composerDrafts.composerDraftsAtom, {
      "new-task:restored-failed-save": { text: message.text, attachments: [] },
      [sourceKey]: { text: "Follow-up", attachments: [] },
    });
    harness.draftFile.setWriteError(new Error("disk full"));
    await expect(recoverFailedThreadDraft(message)).rejects.toThrow("Composer draft persistence");
    expect(composerDrafts.getComposerDraftSnapshot(sourceKey).text).toBe("Follow-up");
    harness.draftFile.setWriteError(null);
    await recoverFailedThreadDraft(message);
    expect(composerDrafts.getComposerDraftSnapshot("new-task:restored-failed-save").text).toBe(
      "Original prompt\n\nFollow-up",
    );
  });

  it("restores a rejected new task as its own draft for the project", async () => {
    const message: QueuedThreadMessage = {
      ...queuedMessage({ messageId: "message-creation-restore", text: "new task text" }),
      modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.6-sol" },
      creation: {
        projectId: ProjectId.make("project-1"),
        workspaceMode: "local",
        branch: null,
        worktreePath: null,
      },
    };
    await harness.manager.enqueue(message);

    await expect(restoreRejectedQueuedMessage(message, "rejected by server")).resolves.toBe(
      "restored",
    );

    expect(
      composerDrafts.getComposerDraftSnapshot(`new-task:restored-${message.messageId}`),
    ).toMatchObject({
      text: message.text,
      attachments: message.attachments,
      modelSelection: message.modelSelection,
      project: {
        environmentId: message.environmentId,
        projectId: message.creation!.projectId,
        createdAt: message.createdAt,
      },
    });
    expect(remainingMessages()).toEqual([]);
    expect(appAtomRegistry.get(threadComposerErrorsAtom)).toEqual({});
    expect(
      appAtomRegistry.get(pendingThreadCreationOutcomesAtom)[
        `${message.environmentId}:${message.threadId}`
      ],
    ).toEqual({ kind: "failed", message, reason: "rejected by server" });
  });

  it("does not record a creation outcome for a rejected follow-up message", async () => {
    const message = queuedMessage({ messageId: "message-followup-restore", text: "follow up" });
    await harness.manager.enqueue(message);

    await expect(restoreRejectedQueuedMessage(message, "rejected")).resolves.toBe("restored");

    expect(appAtomRegistry.get(pendingThreadCreationOutcomesAtom)).toEqual({});
    expect(
      appAtomRegistry.get(threadComposerErrorsAtom)[`${message.environmentId}:${message.threadId}`],
    ).toEqual({ message: "rejected", messageId: message.messageId });
  });

  it("leaves no error when the restored text is resent before recovery finishes", async () => {
    const message = queuedMessage({ messageId: "message-resent", text: "resend me" });
    const threadKey = `${message.environmentId}:${message.threadId}`;
    await harness.manager.enqueue(message);
    const unsubscribe = appAtomRegistry.subscribe(composerDrafts.composerDraftsAtom, (drafts) => {
      if (drafts[threadKey]?.text === "resend me") {
        unsubscribe();
        clearThreadComposerError(threadKey);
        void composerDrafts.clearComposerDraftContent(threadKey);
      }
    });

    await expect(restoreRejectedQueuedMessage(message, "rejected")).resolves.toBe("restored");

    expect(appAtomRegistry.get(threadComposerErrorsAtom)).toEqual({});
  });

  it("withdraws the error when an edit makes the recovery back out", async () => {
    const message = queuedMessage({ messageId: "message-edited-mid-recovery", text: "edit me" });
    const threadKey = `${message.environmentId}:${message.threadId}`;
    await harness.manager.enqueue(message);
    const unsubscribe = appAtomRegistry.subscribe(composerDrafts.composerDraftsAtom, (drafts) => {
      if (drafts[threadKey]?.text === "edit me") {
        unsubscribe();
        appAtomRegistry.set(editingQueuedMessageIdsAtom, { [message.messageId]: true });
      }
    });

    await expect(restoreRejectedQueuedMessage(message, "rejected")).resolves.toBe("deferred");

    expect(appAtomRegistry.get(threadComposerErrorsAtom)).toEqual({});
    expect(remainingMessages()).toEqual([message]);
  });

  it("rolls a failed recovery merge back so the retry cannot duplicate the text", async () => {
    const message = queuedMessage({ messageId: "message-restore", text: "queued text" });
    const draftKey = `${message.environmentId}:${message.threadId}`;
    appAtomRegistry.set(composerDrafts.composerDraftsAtom, {
      [draftKey]: { text: "typed offline", attachments: [] },
    });
    await harness.manager.enqueue(message);

    harness.draftFile.setWriteError(new Error("disk full"));
    await expect(restoreRejectedQueuedMessage(message, "too large")).resolves.toBe("retry");

    expect(composerDrafts.getComposerDraftSnapshot(draftKey).text).toBe("typed offline");
    expect(remainingMessages()).toEqual([message]);

    harness.draftFile.setWriteError(null);
    await expect(restoreRejectedQueuedMessage(message, "too large")).resolves.toBe("restored");

    expect(composerDrafts.getComposerDraftSnapshot(draftKey).text).toBe(
      "typed offline\n\nqueued text",
    );
    expect(remainingMessages()).toEqual([]);
    expect(appAtomRegistry.get(threadComposerErrorsAtom)[draftKey]?.message).toBe("too large");
  });
});
