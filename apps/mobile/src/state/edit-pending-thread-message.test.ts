import { beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { CommandId, EnvironmentId, MessageId, ThreadId } from "@t3tools/contracts";
import type { QueuedThreadMessage } from "./thread-outbox-model";

const state = vi.hoisted(() => ({
  dispatching: null as string | null,
  revision: 1,
  loaded: vi.fn(async () => {}),
  mergePersist: vi.fn(async () => {}),
  held: {} as Record<string, true>,
  draft: { text: "Existing draft", attachments: [] as { id: string }[] },
  confirm: vi.fn(async () => true),
  remove: vi.fn(async () => true),
  flush: vi.fn(async () => {}),
}));
vi.mock("./atom-registry", () => ({
  appAtomRegistry: {
    get: (atom: string) => (atom === "dispatching" ? state.dispatching : state.held),
  },
}));
vi.mock("./use-thread-outbox", () => ({
  dispatchingQueuedMessageIdAtom: "dispatching",
  editingQueuedMessageIdsAtom: "editing",
  holdEditingQueuedMessage: (id: string) => {
    state.held[id] = true;
  },
  releaseEditingQueuedMessage: (id: string) => {
    delete state.held[id];
  },
}));
vi.mock("./thread-outbox", () => ({
  confirmThreadOutboxMessageQueued: state.confirm,
  threadOutboxRevision: () => state.revision,
}));
vi.mock("./thread-outbox-removal", () => ({ removeThreadOutboxMessage: state.remove }));
vi.mock("./use-composer-drafts", () => ({
  waitForComposerDraftsLoaded: state.loaded,
  getComposerDraftSnapshot: () => state.draft,
  mergeComposerDraftContent: async (
    _key: string,
    message: QueuedThreadMessage,
    options?: {
      onMerged?: (draft: typeof state.draft) => void;
    },
  ) => {
    state.draft = {
      text: `${state.draft.text}\n\n${message.text}`,
      attachments: [...state.draft.attachments, ...message.attachments],
    };
    options?.onMerged?.(state.draft);
    await state.mergePersist();
    return { skippedAttachmentCount: 0 };
  },
  updateComposerDraftSettings: (_key: string, settings: object) => {
    state.draft = { ...state.draft, ...settings };
    return state.draft;
  },
  flushComposerDrafts: state.flush,
  undoComposerDraftMerge: async (_key: string, snapshot: typeof state.draft) => {
    state.draft = snapshot;
  },
}));
import { editPendingThreadMessage } from "./edit-pending-thread-message";

const message: QueuedThreadMessage = {
  environmentId: EnvironmentId.make("env"),
  threadId: ThreadId.make("thread"),
  messageId: MessageId.make("message"),
  commandId: CommandId.make("command"),
  text: "Queued task",
  createdAt: "2026-09-06T10:00:00.000Z",
  issuance: { state: "unissued" },
  attachments: [
    {
      id: "file",
      type: "file",
      name: "notes.txt",
      mimeType: "text/plain",
      sizeBytes: 10,
      fileUri: "file:///notes.txt",
    },
  ],
};
beforeEach(() => {
  vi.clearAllMocks();
  state.dispatching = null;
  state.revision = 1;
  state.held = {};
  state.loaded.mockResolvedValue(undefined);
  state.mergePersist.mockResolvedValue(undefined);
  state.draft = { text: "Existing draft", attachments: [] };
  state.confirm.mockResolvedValue(true);
  state.remove.mockResolvedValue(true);
  state.flush.mockResolvedValue(undefined);
});
describe("editing a pending message", () => {
  it("locks delivery and persists text and attachments before removing the queued copy", async () => {
    state.confirm.mockImplementationOnce(async () => {
      expect(state.held[message.messageId]).toBe(true);
      return true;
    });
    state.remove.mockImplementationOnce(async () => {
      expect(state.flush).toHaveBeenCalled();
      expect(state.draft.text).toBe("Existing draft\n\nQueued task");
      expect(state.draft.attachments).toEqual(message.attachments);
      return true;
    });
    expect(await editPendingThreadMessage(message)).toBe(true);
    expect(state.held).toEqual({});
  });
  it("does not reclaim a message already being dispatched", async () => {
    state.dispatching = message.messageId;
    expect(await editPendingThreadMessage(message)).toBe(false);
    expect(state.confirm).not.toHaveBeenCalled();
    expect(state.draft.text).toBe("Existing draft");
  });
  it("rolls back the draft if removing the queued message fails", async () => {
    state.remove.mockRejectedValueOnce(new Error("disk error"));
    await expect(editPendingThreadMessage(message)).rejects.toThrow("disk error");
    expect(state.draft).toEqual({ text: "Existing draft", attachments: [] });
    expect(state.held).toEqual({});
  });
  it("rolls back when a newer queue revision wins", async () => {
    state.remove.mockResolvedValueOnce(false);
    expect(await editPendingThreadMessage(message)).toBe(false);
    expect(state.draft.text).toBe("Existing draft");
  });
  it("prevents a reopened editor from taking delivery while an earlier transfer is pending", async () => {
    let release!: () => void;
    let entered!: () => void;
    const started = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    state.confirm.mockImplementationOnce(async () => {
      entered();
      await gate;
      return true;
    });
    const first = editPendingThreadMessage(message);
    await started;
    expect(await editPendingThreadMessage(message)).toBe(false);
    expect(state.confirm).toHaveBeenCalledTimes(1);
    expect(state.remove).not.toHaveBeenCalled();
    release();
    expect(await first).toBe(true);
    expect(state.remove).toHaveBeenCalledTimes(1);
    expect(state.held).toEqual({});
  });
  it("retains an unrelated queue revision accepted while draft loading is pending", async () => {
    state.loaded.mockImplementationOnce(async () => {
      state.revision++;
    });
    expect(await editPendingThreadMessage(message)).toBe(false);
    expect(state.draft.text).toBe("Existing draft");
    expect(state.remove).not.toHaveBeenCalled();
    expect(state.held).toEqual({});
  });
  it("does not remove the newer queued revision after its draft merge finishes", async () => {
    state.mergePersist.mockImplementationOnce(async () => {
      state.revision++;
    });
    expect(await editPendingThreadMessage(message)).toBe(false);
    expect(state.remove).not.toHaveBeenCalled();
    expect(state.draft.text).toBe("Existing draft");
    expect(state.held).toEqual({});
  });
  it("keeps the queued copy when persisting the draft fails and permits a later explicit edit", async () => {
    state.mergePersist.mockRejectedValueOnce(new Error("draft disk full"));
    await expect(editPendingThreadMessage(message)).rejects.toThrow("draft disk full");
    expect(state.remove).not.toHaveBeenCalled();
    expect(state.draft.text).toBe("Existing draft");
    expect(state.held).toEqual({});
    expect(await editPendingThreadMessage(message)).toBe(true);
    expect(state.remove).toHaveBeenCalledTimes(1);
  });
  it("rechecks delivery ownership after the durable draft flush", async () => {
    state.flush.mockImplementationOnce(async () => {
      state.dispatching = message.messageId;
    });
    expect(await editPendingThreadMessage(message)).toBe(false);
    expect(state.confirm).toHaveBeenCalledTimes(1);
    expect(state.remove).not.toHaveBeenCalled();
    expect(state.draft.text).toBe("Existing draft");
    expect(state.held).toEqual({});
  });
  it("does not reclaim legacy messages with an unknown delivery outcome", async () => {
    const { issuance: _issuance, ...legacy } = message;
    expect(await editPendingThreadMessage(legacy)).toBe(false);
    expect(state.confirm).not.toHaveBeenCalled();
    expect(state.remove).not.toHaveBeenCalled();
  });
});
