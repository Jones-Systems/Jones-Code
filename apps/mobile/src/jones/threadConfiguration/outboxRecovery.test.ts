import { decodePreparedThreadTurn } from "@t3tools/client-runtime/jones/preparedThreadTurn";
import {
  CommandId, EnvironmentId, MessageId, ORCHESTRATION_V2_WS_METHODS,
  ProjectId, ProviderInstanceId, ThreadId,
} from "@t3tools/contracts";
import { initialMessageCommandId } from "@t3tools/contracts/jones/deliveryEvidence";
import * as Option from "effect/Option";
import { Atom, AsyncResult } from "effect/reactivity";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

const harness = vi.hoisted(() => ({
  document: JSON.stringify({ schemaVersion: 1, drafts: {} }),
  manager: null as unknown as ReturnType<typeof import("../../state/thread-outbox-manager").createThreadOutboxManager>,
  persisted: new Map<string, unknown>(),
  remove: vi.fn(async () => {}),
  write: vi.fn(async () => {}),
}));
vi.mock("react-native", () => ({ Alert: { alert: vi.fn() } }));
vi.mock("expo-file-system", () => ({
  Directory: class { create() {} },
  File: class {
    exists = true;
    parentDirectory = null;
    create() {}
    moveSync() {}
    async text() { return harness.document; }
    write(value: string) { harness.document = value; }
  },
  Paths: { document: "/synthetic-c3-drafts" },
}));
vi.mock("../../lib/composerImages", () => ({ removePersistedComposerAttachmentFile: vi.fn() }));
vi.mock("../../lib/uuid", () => ({ uuidv4: () => "c3-unique-draft", randomHex: () => "c3abcd" }));
vi.mock("../../lib/attachmentUpload", () => ({ prepareTurnAttachments: vi.fn() }));
vi.mock("../../state/entities", () => ({ useProjects: () => [], useServerConfigs: () => new Map(), useThreadShells: () => [] }));
vi.mock("../../state/server", async () => {
  const { Atom } = await import("effect/reactivity");
  return { serverEnvironment: { configValueAtom: Atom.family(() => Atom.make(null)) } };
});
vi.mock("../../state/git", () => ({ gitEnvironment: {} }));
vi.mock("../../state/threads", () => ({ threadEnvironment: {} }));
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => async () => undefined }));
vi.mock("../../state/session", () => ({ readEnvironmentScope: () => true, useEnvironmentsWithScope: () => new Set() }));
vi.mock("../../state/use-remote-environment-registry", () => ({ useRemoteConnectionStatus: () => ({ connectedEnvironments: [] }) }));
vi.mock("../../state/use-thread-outbox", async () => {
  const { Atom } = await import("effect/reactivity");
  return {
    editingQueuedMessageIdsAtom: Atom.make({}).pipe(Atom.keepAlive),
    useThreadOutboxMessages: () => ({}), useThreadOutboxShellStatuses: () => new Map(),
  };
});
vi.mock("../../state/thread-outbox", async () => {
  const { createThreadOutboxManager } = await import("../../state/thread-outbox-manager");
  const { appAtomRegistry } = await import("../../state/atom-registry");
  const { encodeQueuedThreadMessage, decodeQueuedThreadMessage } = await import("../../state/thread-outbox-model");
  const manager = harness.manager = createThreadOutboxManager({
    registry: appAtomRegistry,
    storage: {
      load: async () => ({ messages: Array.from(harness.persisted.values(), decodeQueuedThreadMessage), errors: [] }),
      write: async (message) => { await harness.write(); harness.persisted.set(message.messageId, encodeQueuedThreadMessage(message)); },
      remove: async (message) => { await harness.remove(); harness.persisted.delete(message.messageId); },
    },
  });
  return {
    threadOutboxManager: manager, flushThreadOutbox: async () => {},
    confirmThreadOutboxMessageQueued: manager.confirmQueued,
    updateThreadOutboxMessage: manager.update,
    threadOutboxRevision: manager.revisionOf,
  };
});

import { appAtomRegistry } from "../../state/atom-registry";
import type { QueuedThreadMessage } from "../../state/thread-outbox-model";
import * as drafts from "../../state/use-composer-drafts";
import { pendingThreadCreationOutcomesAtom, resolvePendingThreadCreation } from "../../state/pending-thread-creation";
import { restoredNewTaskDraftKey } from "../../state/new-task-draft-key";
import { threadComposerErrorsAtom } from "../../state/thread-composer-error";
import { editingQueuedMessageIdsAtom } from "../../state/use-thread-outbox";
import { restoreRejectedQueuedMessage } from "../../state/use-thread-outbox-drain";
import { recoverFailedThreadDraft } from "../../state/recover-failed-thread-draft";
import {
  acceptedQueuedThreadDelivery, observeQueuedThreadDelivery, queuedThreadDeliveryStatus,
  withIssuedQueuedThreadTurn,
} from "./outboxDelivery";
import { captureFollowUpModelSelection } from "./modelSelection";

function rejectedCreation(creationAccepted: boolean) {
  const original: QueuedThreadMessage = {
    environmentId: EnvironmentId.make("c3-recovery-env"), threadId: ThreadId.make("c3-recovery-thread"),
    commandId: CommandId.make("c3-recovery-command"), messageId: MessageId.make("c3-recovery-message"),
    text: "Rejected prompt", attachments: [{
      type: "file", id: "c3-file", name: "report.txt", mimeType: "text/plain", sizeBytes: 4,
      fileUri: "file:///synthetic-c3/report.txt",
    }],
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6.1-sol", options: [{ id: "reasoningEffort", value: "max" }] },
    modelSelectionExplicit: true, runtimeMode: "full-access", interactionMode: "default",
    creation: { projectId: ProjectId.make("c3-recovery-project"), workspaceMode: "worktree", branch: null, worktreePath: null },
    issuance: { state: "unissued" }, createdAt: "2026-11-02T12:00:00.000Z",
  };
  const prepared = decodePreparedThreadTurn({
    method: ORCHESTRATION_V2_WS_METHODS.launchThread, payload: {
      commandId: original.commandId, threadId: original.threadId, projectId: original.creation!.projectId,
      title: "Recovery", generateTitle: true, creationSource: "mobile", modelSelection: original.modelSelection,
      runtimeMode: "full-access", interactionMode: "default", workspaceStrategy: { type: "worktree", branch: "frozen-branch" },
      initialMessage: { messageId: original.messageId, text: original.text, attachments: [] },
    },
  });
  return observeQueuedThreadDelivery(withIssuedQueuedThreadTurn(original, prepared), {
    kind: "launch", commandId: original.commandId, projectId: original.creation!.projectId,
    creation: creationAccepted ? { status: "accepted", threadId: original.threadId } : { status: "rejected" },
    initialMessage: { status: creationAccepted ? "rejected" : "unknown", commandId: initialMessageCommandId(original.commandId), messageId: original.messageId },
  });
}
const keyOf = (message: QueuedThreadMessage) => `${message.environmentId}:${message.threadId}`;
const queued = () => Object.values(appAtomRegistry.get(harness.manager.queuedMessagesByThreadKeyAtom)).flat();

beforeEach(async () => {
  harness.document = JSON.stringify({ schemaVersion: 1, drafts: {} });
  await drafts.waitForComposerDraftsLoaded();
  harness.remove.mockReset().mockResolvedValue(undefined);
  harness.write.mockReset().mockResolvedValue(undefined);
});
afterEach(async () => {
  vi.restoreAllMocks();
  await drafts.flushComposerDrafts();
  appAtomRegistry.set(drafts.composerDraftsAtom, {});
  appAtomRegistry.set(drafts.composerCloudDraftsAtom, { accountId: null, signedOut: {} });
  appAtomRegistry.set(harness.manager.queuedMessagesByThreadKeyAtom, {});
  appAtomRegistry.set(editingQueuedMessageIdsAtom, {});
  appAtomRegistry.set(pendingThreadCreationOutcomesAtom, {});
  appAtomRegistry.set(threadComposerErrorsAtom, {});
  harness.persisted.clear();
});

describe("mobile evidence-aware editable recovery", () => {
  it.each([true, false])("restores rejection while retaining the proven target (creation accepted: %s)", async (creationAccepted) => {
    const message = rejectedCreation(creationAccepted);
    await harness.manager.enqueue(message);
    expect(await restoreRejectedQueuedMessage(message, "Refused by admission")).toBe("restored");
    const draftKey = creationAccepted ? keyOf(message) : restoredNewTaskDraftKey(message.messageId);
    const restored = drafts.getComposerDraftSnapshot(draftKey);
    expect(restored.text).toContain(message.text);
    expect(restored.attachments).toEqual(message.attachments);
    expect(restored.modelSelection).toEqual(message.modelSelection);
    expect(queued()).toEqual([]);
    if (creationAccepted) {
      expect(restored.workspaceSelection).toBeUndefined();
      expect(drafts.getComposerDraftSnapshot(restoredNewTaskDraftKey(message.messageId)).text).toBe("");
      expect(resolvePendingThreadCreation({
        threadKey: keyOf(message), pending: { message, outcome: { kind: "failed", message, reason: "Refused" } },
        previous: null, detail: null,
      })).toBeNull();
      // A deliberate corrected send owns new message/command keys in the existing thread.
      const corrected = { ...message, creation: undefined, issuance: { state: "unissued" as const },
        text: "Corrected prompt", commandId: CommandId.make("new-deliberate-command"), messageId: MessageId.make("new-deliberate-message") };
      await harness.manager.enqueue(corrected);
      expect(queued()[0]).toMatchObject({ threadId: message.threadId, commandId: corrected.commandId, messageId: corrected.messageId });
    } else expect(restored.workspaceSelection?.mode).toBe("worktree");
  });

  it("lets late acceptance defeat recovery without erasing newer text, attachments or selection", async () => {
    const message = rejectedCreation(true);
    await harness.manager.enqueue(message);
    const merged = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const merge = drafts.mergeComposerDraftContent;
    vi.spyOn(drafts, "mergeComposerDraftContent").mockImplementation(async (...args) => {
      const result = await merge(...args);
      merged.resolve();
      await release.promise;
      return result;
    });
    const restoring = restoreRejectedQueuedMessage(message, "Stale refusal");
    try {
      await merged.promise;
      drafts.setComposerDraftText(keyOf(message), "Newer unsent content");
      const later = drafts.updateComposerDraftSettings(keyOf(message), {
        modelSelection: { ...message.modelSelection!, model: "newer-model" }, runtimeMode: "approval-required",
      });
      const extra = { ...message.attachments[0]!, id: "newer-file", fileUri: "file:///synthetic-c3/new.txt" };
      drafts.appendComposerDraftAttachments(keyOf(message), [extra]);
      observeQueuedThreadDelivery(message, acceptedQueuedThreadDelivery(message));
      release.resolve();
      expect(await restoring).toBe("deferred");
      const current = drafts.getComposerDraftSnapshot(keyOf(message));
      expect(current.text).toBe("Newer unsent content");
      expect(current.attachments).toEqual([extra]);
      expect(current.modelSelection).toBe(later.modelSelection);
      expect(current.runtimeMode).toBe("approval-required");
      expect(queuedThreadDeliveryStatus(queued()[0]!)).toBe("accepted");
      expect(appAtomRegistry.get(pendingThreadCreationOutcomesAtom)).toEqual({});
    } finally { release.resolve(); await restoring; }
  });

  it("fences a stale removal after an editor save and late acceptance during storage", async () => {
    const message = rejectedCreation(true);
    await harness.manager.enqueue(message);
    const removing = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    harness.remove.mockImplementationOnce(async () => { removing.resolve(); await release.promise; });
    const restoring = restoreRejectedQueuedMessage(message, "Stale refusal");
    try {
      await removing.promise;
      const accepted = observeQueuedThreadDelivery(message, acceptedQueuedThreadDelivery(message));
      const editorSave = harness.manager.enqueue({ ...accepted, text: "Latest saved editor revision" });
      release.resolve();
      expect(await restoring).toBe("deferred");
      await editorSave;
      expect(queued()[0]?.text).toBe("Latest saved editor revision");
      expect(queuedThreadDeliveryStatus(queued()[0]!)).toBe("accepted");
      expect(harness.persisted.get(message.messageId)).toMatchObject({ text: "Latest saved editor revision" });
    } finally { release.resolve(); await restoring; }
  });

  it.each([false, true])("preserves newer source edits or late acceptance in failed-task recovery (accepted: %s)", async (accepted) => {
    const message = rejectedCreation(false);
    drafts.setComposerDraftText(keyOf(message), "Source recovery text");
    const copied = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const merge = drafts.mergeComposerDraftContent;
    vi.spyOn(drafts, "mergeComposerDraftContent").mockImplementation(async (...args) => {
      const result = await merge(...args);
      copied.resolve();
      await release.promise;
      return result;
    });
    const recovering = recoverFailedThreadDraft(message);
    try {
      await copied.promise;
      drafts.setComposerDraftText(keyOf(message), "Newer source text");
      const newer = drafts.updateComposerDraftSettings(keyOf(message), { modelSelection: message.modelSelection });
      if (accepted) observeQueuedThreadDelivery(message, acceptedQueuedThreadDelivery(message));
      release.resolve();
      await recovering;
      expect(drafts.getComposerDraftSnapshot(keyOf(message)).text).toBe("Newer source text");
      expect(drafts.getComposerDraftSnapshot(keyOf(message)).modelSelection).toBe(newer.modelSelection);
      expect(drafts.getComposerDraftSnapshot(restoredNewTaskDraftKey(message.messageId)).text)
        .toBe(accepted ? "" : "Source recovery text");
    } finally { release.resolve(); await recovering; }
  });

  it.each([false, true])("consumes only the durable enqueue's picker identity (equal reselection: %s)", async (reselect) => {
    const message = rejectedCreation(true);
    const key = keyOf(message);
    const pick = message.modelSelection!;
    const saved = { ...pick, model: "concurrent-saved-B" };
    drafts.updateComposerDraftSettings(key, { modelSelection: pick });
    const submitted = drafts.getComposerDraftSnapshot(key).modelSelection;
    const releaseReconciliation = drafts.subscribeComposerDraftModelReconciliation({
      registry: appAtomRegistry,
      threadRefsAtom: Atom.make([{ environmentId: message.environmentId, threadId: message.threadId }]),
      threadShellAtom: () => Atom.make({ modelSelection: pick } as never),
      shellStateAtom: () => Atom.make(AsyncResult.success({ status: "live", snapshot: Option.some({ threads: [{ id: message.threadId, modelSelection: pick }] }) } as never)),
    });
    try {
      expect(drafts.getComposerDraftSnapshot(key).modelSelection).toBe(submitted);
      expect(captureFollowUpModelSelection(submitted, saved)).toEqual({ modelSelection: submitted, modelSelectionExplicit: true });
      const started = Promise.withResolvers<void>();
      const release = Promise.withResolvers<void>();
      harness.write.mockImplementationOnce(async () => { started.resolve(); await release.promise; });
      const enqueuing = harness.manager.enqueue({
        ...message, creation: undefined, issuance: { state: "unissued" },
        ...captureFollowUpModelSelection(submitted, saved),
      });
      drafts.clearComposerDraftContent(key, { deferAttachmentCleanup: true });
      try {
        await started.promise;
        // The exact same catalog object may be returned by an intentional picker action.
        const later = reselect ? drafts.updateComposerDraftSettings(key, { modelSelection: pick }).modelSelection : undefined;
        drafts.setComposerDraftText(key, "Later draft");
        release.resolve();
        await enqueuing;
        drafts.consumeSubmittedComposerModelPick(key, submitted);
        expect(drafts.getComposerDraftSnapshot(key).modelSelection).toBe(later);
        expect(drafts.getComposerDraftSnapshot(key).text).toBe("Later draft");
        expect(queued()[0]?.modelSelection).toEqual(pick);
        expect(queued()[0]?.modelSelectionExplicit).toBe(true);
        if (reselect) expect(later).not.toBe(submitted);
      } finally { release.resolve(); await enqueuing; }
    } finally { releaseReconciliation(); }
  });
});
