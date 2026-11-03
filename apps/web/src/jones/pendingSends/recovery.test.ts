import { afterEach, describe, expect, it, onTestFinished, vi } from "vite-plus/test";
import { AsyncResult } from "effect/reactivity";
import * as Cause from "effect/Cause";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { decodePreparedThreadTurn } from "@t3tools/client-runtime/jones/preparedThreadTurn";
import type { StartThreadTurnInput } from "@t3tools/client-runtime/operations";
import {
  CommandId, EnvironmentId, MessageId, ProviderInstanceId, ThreadId,
  OrchestrationV2DispatchCommandError, type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import { useComposerDraftStore } from "../../composerDraftStore";
import { clearSubmittedComposer } from "../../components/chat/composerSendRecovery";
import { createPendingComposerRecovery } from "./composerRecovery";
import { createPendingSendStore } from "./store";
import { sendPendingTurn } from "./send";

const target = scopeThreadRef(EnvironmentId.make("web-recovery"), ThreadId.make("thread"));
const messageId = MessageId.make("original-message");
const commandId = CommandId.make("original-command");
const selection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6.1-sol" };
const input: StartThreadTurnInput = {
  threadId: target.threadId, runtimeMode: "full-access", interactionMode: "default",
  message: { messageId, role: "user", text: "original draft", attachments: [] },
};
const prepared = decodePreparedThreadTurn({
  method: "orchestration.dispatchCommand",
  payload: {
    type: "message.dispatch", commandId, threadId: target.threadId, messageId,
    text: input.message.text, attachments: [], createdBy: "user", creationSource: "web",
    dispatchMode: { type: "start_immediately" }, runtimeMode: "full-access", interactionMode: "default",
  },
});
const failure = (status?: "accepted" | "rejected") => AsyncResult.failure(Cause.fail(
  new OrchestrationV2DispatchCommandError({
    commandId, commandType: "message.dispatch", message: "Dispatch response failed",
    ...(status ? { deliveryEvidence: { kind: "dispatch" as const, commandId,
      commandType: "message.dispatch", threadId: target.threadId, status } } : {}),
  }),
));
function setup() {
  const store = createPendingSendStore();
  const drafts = useComposerDraftStore.getState();
  drafts.setModelSelection(target, selection, { explicit: true });
  drafts.setPrompt(target, input.message.text);
  const file = new File(["image"], "recovery.png", { type: "image/png" });
  const image = { type: "image" as const, id: "image", name: file.name,
    mimeType: file.type, sizeBytes: file.size, file, previewUrl: URL.createObjectURL(file) };
  onTestFinished(() => URL.revokeObjectURL(image.previewUrl));
  drafts.addImages(target, [image]);
  const attachmentFile = new File(["document"], "recovery.txt", { type: "text/plain" });
  drafts.addFiles(target, [{ type: "file", id: "document", name: attachmentFile.name,
    mimeType: attachmentFile.type, sizeBytes: attachmentFile.size, file: attachmentFile }]);
  const snapshot = drafts.getComposerDraft(target)!;
  const promptRef = { current: snapshot.prompt };
  const owner = {
    routeThreadKey: "original-route", currentRouteThreadKeyRef: { current: "original-route" },
    composerDraftTarget: target, promptRef, isCurrentSend: () => true,
    resetCursor: vi.fn(),
  };
  const cleared = clearSubmittedComposer({ ...owner, expectedDraft: snapshot })!;
  const images = { current: [] as typeof snapshot.images };
  const files = { current: [] as typeof snapshot.files };
  const recovery = createPendingComposerRecovery(() => ({
    ...owner, expectedDraft: cleared.draft, backgroundDraftOpened: false,
    composerImagesRef: images, composerFilesRef: files, composerTerminalContextsRef: { current: [] }, snapshot,
  }));
  const key = store.admit(target.environmentId, target.threadId, {
    id: messageId, role: "user", text: input.message.text, runId: null,
    createdAt: "2026-11-02T20:00:00Z", updatedAt: "2026-11-02T20:00:00Z", streaming: false,
  });
  store.whenRejected(target.environmentId, target.threadId, messageId, recovery.rejected);
  store.whenAccepted(target.environmentId, target.threadId, messageId, recovery.accepted);
  return { store, drafts, key, promptRef, images, files };
}
const projection = { messages: [{ id: messageId }], runs: [], visibleTurnItems: [] } as unknown as OrchestrationV2ThreadProjection;

describe("web receipt recovery through send and draft stores", () => {
  const initialState = useComposerDraftStore.getState();
  afterEach(() => {
    useComposerDraftStore.getState().clearComposerContent(target);
    useComposerDraftStore.setState(initialState, true);
  });

  it.each([false, true])("restores exact rejection and attachments, including rejection after frozen Retry: %s", async (retry) => {
    const { store, drafts, images, files, key } = setup();
    const prepare = vi.fn(async () => AsyncResult.success(prepared));
    const dispatch = vi.fn(async (_value: typeof prepared) => failure("rejected"));
    if (retry) dispatch.mockResolvedValueOnce(failure());
    await sendPendingTurn({ environmentId: target.environmentId, input, store, prepare, dispatch });
    if (retry) {
      expect(store.getSnapshot()[0]).toMatchObject({ delivery: "uncertain", working: true, prepared });
      expect(drafts.getComposerDraft(target)?.prompt).toBe("");
      const originalRetry = store.getSnapshot()[0]!.retry!;
      await originalRetry();
      await originalRetry();
      expect(dispatch).toHaveBeenCalledTimes(2);
    }
    expect(store.getSnapshot()[0]).toMatchObject({ delivery: "failed", working: false, inFlight: false });
    expect(store.getSnapshot()[0]?.retry).toBeUndefined();
    expect(drafts.getComposerDraft(target)?.prompt).toBe(input.message.text);
    expect(images.current).toHaveLength(1);
    expect(images.current[0]?.file).toBeDefined();
    expect(files.current[0]?.file).toBeDefined();
    expect(prepare).toHaveBeenCalledTimes(1);
    expect(dispatch.mock.calls.every(([value]) => value === prepared)).toBe(true);
    store.dismiss(key);
  });

  it.each([false, true])("late acceptance removes only unchanged restored content; newer content: %s", async (newer) => {
    const { store, drafts, key, images, files } = setup();
    await sendPendingTurn({ environmentId: target.environmentId, input, store,
      prepare: async () => AsyncResult.success(prepared), dispatch: async () => failure("rejected") });
    const nextSelection = { ...selection, model: "gpt-6-astra" };
    drafts.setModelSelection(target, nextSelection, { explicit: true });
    if (newer) drafts.setPrompt(target, "newer unsent draft");
    store.reconcile(target.environmentId, target.threadId, projection);
    expect(store.getSnapshot()[0]?.delivery).toBe("accepted");
    expect(drafts.getComposerDraft(target)?.prompt).toBe(newer ? "newer unsent draft" : "");
    expect(images.current).toHaveLength(newer ? 1 : 0);
    expect(files.current).toHaveLength(newer ? 1 : 0);
    expect(drafts.getComposerDraft(target)).toMatchObject({ activeProvider: selection.instanceId,
      modelSelectionExplicit: true, modelSelectionByProvider: { [selection.instanceId]: nextSelection } });
    store.dismiss(key);
  });

  it.each(["accepted", "rejected"] as const)("preserves newer edits through a failed response proving %s", async (status) => {
    const { store, drafts, key } = setup();
    drafts.setPrompt(target, "newer text while waiting");
    await sendPendingTurn({ environmentId: target.environmentId, input, store,
      prepare: async () => AsyncResult.success(prepared), dispatch: async () => failure(status) });
    expect(drafts.getComposerDraft(target)?.prompt).toBe("newer text while waiting");
    store.reconcile(target.environmentId, target.threadId, projection);
    expect(store.getSnapshot()[0]?.delivery).toBe("accepted");
    expect(drafts.getComposerDraft(target)?.prompt).toBe("newer text while waiting");
    store.finish(key, { kind: "dispatch", commandId, commandType: "message.dispatch", threadId: target.threadId, status: "rejected" });
    expect(store.getSnapshot()[0]?.delivery).toBe("accepted");
    expect(drafts.getComposerDraft(target)?.prompt).toBe("newer text while waiting");
    store.dismiss(key);
  });
});
