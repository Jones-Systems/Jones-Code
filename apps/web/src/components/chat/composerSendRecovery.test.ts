import { scopedThreadKey, scopeThreadRef } from "@t3tools/client-runtime/environment";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { useComposerDraftStore } from "../../composerDraftStore";
import { threadContextRecord } from "../../lib/composerContextRecords";
import { buildFileReviewComment } from "../../reviewCommentContext";
import { clearSubmittedComposer, restoreFailedComposerSend } from "./composerSendRecovery";

const a = scopeThreadRef(EnvironmentId.make("env-a"), ThreadId.make("thread-a"));
const b = scopeThreadRef(EnvironmentId.make("env-a"), ThreadId.make("thread-b"));
const sameIdOtherEnvironment = scopeThreadRef(EnvironmentId.make("env-b"), a.threadId);
const createdAt = "2026-10-04T00:00:00.000Z";

function recovery() {
  return {
    routeThreadKey: scopedThreadKey(a),
    currentRouteThreadKeyRef: { current: scopedThreadKey(a) as string | null },
    composerDraftTarget: a,
    backgroundDraftOpened: false as boolean,
    expectedDraft: null as Parameters<typeof restoreFailedComposerSend>[0]["expectedDraft"],
    isCurrentSend: (): boolean => true,
    promptRef: { current: "" },
    composerImagesRef: { current: [] },
    composerFilesRef: { current: [] },
    composerTerminalContextsRef: { current: [] },
    snapshot: {
      prompt: "original A",
      images: [],
      files: [],
      terminalContexts: [],
      previewAnnotations: [],
      reviewComments: [],
      threadContexts: [],
    },
    resetCursor: vi.fn(),
  } satisfies Parameters<typeof restoreFailedComposerSend>[0];
}

function fullSnapshot(): Parameters<typeof restoreFailedComposerSend>[0]["snapshot"] {
  const file = new File(["test"], "test.txt", { type: "text/plain" });
  const imageFile = new File(["image"], "image.png", { type: "image/png" });
  return {
    prompt: "original A",
    images: [
      {
        type: "image",
        id: "image",
        name: imageFile.name,
        mimeType: imageFile.type,
        sizeBytes: imageFile.size,
        previewUrl: "blob:original",
        file: imageFile,
      },
    ],
    files: [
      {
        type: "file",
        id: "file",
        name: file.name,
        mimeType: file.type,
        sizeBytes: file.size,
        file,
      },
    ],
    terminalContexts: [
      {
        id: "terminal",
        threadId: a.threadId,
        terminalId: "default",
        terminalLabel: "Terminal",
        lineStart: 1,
        lineEnd: 1,
        text: "output",
        createdAt,
      },
    ],
    previewAnnotations: [
      {
        id: "preview",
        pageUrl: "https://example.test",
        pageTitle: null,
        comment: "look",
        elements: [],
        regions: [],
        strokes: [],
        styleChanges: [],
        screenshot: null,
        createdAt,
      },
    ],
    threadContexts: [threadContextRecord(b, "Related work")],
    reviewComments: [
      buildFileReviewComment({
        id: "review",
        filePath: "test.ts",
        startLine: 1,
        endLine: 1,
        text: "look",
        contents: "test",
      }),
    ],
  };
}

function resetStore() {
  useComposerDraftStore.setState({ draftsByThreadKey: {}, draftThreadsByThreadKey: {} });
}

beforeEach(resetStore);
afterEach(() => {
  resetStore();
  vi.restoreAllMocks();
});

describe("composer send recovery ownership", () => {
  it("restores the original same-route prompt and cursor", () => {
    const input = recovery();
    expect(restoreFailedComposerSend(input)).toBe(true);
    expect(input.promptRef.current).toBe("original A");
    expect(useComposerDraftStore.getState().getComposerDraft(a)?.prompt).toBe("original A");
    expect(input.resetCursor).toHaveBeenCalledWith({
      cursor: 10,
      prompt: "original A",
      detectTrigger: true,
    });
  });

  it.each(["", "new B"])("preserves B live state while recovering A with B prompt %j", (prompt) => {
    const input = recovery();
    input.currentRouteThreadKeyRef.current = scopedThreadKey(b);
    input.promptRef.current = prompt;
    useComposerDraftStore.getState().setPrompt(b, prompt);
    expect(restoreFailedComposerSend(input)).toBe(true);
    expect(input.promptRef.current).toBe(prompt);
    expect(useComposerDraftStore.getState().getComposerDraft(b)?.prompt ?? "").toBe(prompt);
    expect(useComposerDraftStore.getState().getComposerDraft(a)?.prompt).toBe("original A");
    expect(input.resetCursor).not.toHaveBeenCalled();
  });

  it.each([b, sameIdOtherEnvironment, null])(
    "clears only the captured store after navigation to %j",
    (route) => {
      const input = recovery();
      const store = useComposerDraftStore.getState();
      store.setPrompt(a, "submitted A");
      store.setPrompt(b, "new B");
      input.currentRouteThreadKeyRef.current = route ? scopedThreadKey(route) : null;
      input.promptRef.current = "new B";
      clearSubmittedComposer({ ...input, expectedDraft: store.getComposerDraft(a) });
      expect(store.getComposerDraft(a)).toBeNull();
      expect(store.getComposerDraft(b)?.prompt).toBe("new B");
      expect(input.promptRef.current).toBe("new B");
      expect(input.resetCursor).not.toHaveBeenCalled();
    },
  );

  it("clears the displayed prompt and cursor on the same route", () => {
    const input = recovery();
    input.promptRef.current = "submitted A";
    useComposerDraftStore.getState().setPrompt(a, input.promptRef.current);
    clearSubmittedComposer({
      ...input,
      expectedDraft: useComposerDraftStore.getState().getComposerDraft(a),
    });
    expect(input.promptRef.current).toBe("");
    expect(input.resetCursor).toHaveBeenCalledOnce();
  });

  it("leaves newer same-route input untouched", () => {
    const input = recovery();
    input.promptRef.current = "newer A";
    useComposerDraftStore.getState().setPrompt(a, "newer A");
    expect(restoreFailedComposerSend(input)).toBe(false);
    expect(input.promptRef.current).toBe("newer A");
    expect(input.resetCursor).not.toHaveBeenCalled();
  });

  it.each([scopedThreadKey(b), scopedThreadKey(sameIdOtherEnvironment), null])(
    "uses A stored content to reject offscreen recovery at %j",
    (route) => {
      const input = recovery();
      input.currentRouteThreadKeyRef.current = route;
      useComposerDraftStore.getState().setPrompt(a, "newer A");
      expect(restoreFailedComposerSend(input)).toBe(false);
      expect(useComposerDraftStore.getState().getComposerDraft(a)?.prompt).toBe("newer A");
      expect(input.promptRef.current).toBe("");
      expect(input.resetCursor).not.toHaveBeenCalled();
    },
  );

  it("treats an opened background draft as offscreen even before the route ref changes", () => {
    const input = recovery();
    input.backgroundDraftOpened = true;
    input.promptRef.current = "fresh composer";
    expect(restoreFailedComposerSend(input)).toBe(true);
    expect(input.promptRef.current).toBe("fresh composer");
    expect(input.resetCursor).not.toHaveBeenCalled();
  });

  it.each([
    "images",
    "files",
    "terminalContexts",
    "previewAnnotations",
    "reviewComments",
    "threadContexts",
  ] as const)("preserves newer A %s during offscreen recovery", (field) => {
    const input = recovery();
    input.currentRouteThreadKeyRef.current = scopedThreadKey(b);
    const snapshot = fullSnapshot();
    const store = useComposerDraftStore.getState();
    store.setPrompt(a, "newer A");
    const draft = store.getComposerDraft(a);
    useComposerDraftStore.setState((state) => ({
      draftsByThreadKey: {
        ...state.draftsByThreadKey,
        [scopedThreadKey(a)]: { ...draft!, prompt: "", [field]: snapshot[field] },
      },
    }));
    const newerDraft = store.getComposerDraft(a);
    expect(restoreFailedComposerSend(input)).toBe(false);
    expect(store.getComposerDraft(a)).toBe(newerDraft);
    expect(input.resetCursor).not.toHaveBeenCalled();
  });

  it("recovers every attachment and context in A without changing populated B refs", () => {
    const snapshot = fullSnapshot();
    vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:retry");
    const input: Parameters<typeof restoreFailedComposerSend>[0] = {
      ...recovery(),
      snapshot,
      currentRouteThreadKeyRef: { current: scopedThreadKey(b) },
      promptRef: { current: "new B" },
      composerImagesRef: { current: snapshot.images },
      composerFilesRef: { current: snapshot.files },
      composerTerminalContextsRef: { current: snapshot.terminalContexts },
    };
    const store = useComposerDraftStore.getState();
    store.setPrompt(b, "new B");
    const bDraft = store.getComposerDraft(b);
    expect(restoreFailedComposerSend(input)).toBe(true);
    const draft = store.getComposerDraft(a)!;
    expect(draft.prompt).toContain("original A");
    expect(draft.images).toEqual([{ ...snapshot.images[0], previewUrl: "blob:retry" }]);
    expect(draft.files).toEqual(snapshot.files);
    expect(draft.terminalContexts).toEqual(snapshot.terminalContexts);
    expect(draft.previewAnnotations).toEqual(snapshot.previewAnnotations);
    expect(draft.reviewComments).toEqual(snapshot.reviewComments);
    expect(draft.threadContexts).toEqual(snapshot.threadContexts);
    expect(input.composerImagesRef.current).toBe(snapshot.images);
    expect(input.composerFilesRef.current).toBe(snapshot.files);
    expect(input.composerTerminalContextsRef.current).toBe(snapshot.terminalContexts);
    expect(input.promptRef.current).toBe("new B");
    expect(store.getComposerDraft(b)).toBe(bDraft);
    expect(input.resetCursor).not.toHaveBeenCalled();
  });
});

function deferredResult() {
  let resolve!: (accepted: boolean) => void;
  const promise = new Promise<boolean>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

describe("delayed composer send results", () => {
  it("delayed acceptance clears A while preserving B's visible prompt and cursor", async () => {
    const input = recovery();
    const store = useComposerDraftStore.getState();
    store.setPrompt(a, "submitted A");
    input.expectedDraft = store.getComposerDraft(a);
    const rpc = deferredResult();
    const completion = rpc.promise.then((accepted) =>
      accepted ? clearSubmittedComposer(input) : null,
    );
    input.currentRouteThreadKeyRef.current = scopedThreadKey(b);
    input.promptRef.current = "next B";
    store.setPrompt(b, "next B");
    rpc.resolve(true);
    expect(await completion).not.toBeNull();
    expect(store.getComposerDraft(a)).toBeNull();
    expect(store.getComposerDraft(b)?.prompt).toBe("next B");
    expect(input.promptRef.current).toBe("next B");
    expect(input.resetCursor).not.toHaveBeenCalled();
  });

  it("delayed acceptance preserves a newer same-thread draft", async () => {
    const input = recovery();
    const store = useComposerDraftStore.getState();
    store.setPrompt(a, "submitted A");
    input.expectedDraft = store.getComposerDraft(a);
    const rpc = deferredResult();
    const completion = rpc.promise.then(() => clearSubmittedComposer(input));
    store.setPrompt(a, "newer A");
    input.promptRef.current = "newer A";
    rpc.resolve(true);
    expect(await completion).toBeNull();
    expect(store.getComposerDraft(a)?.prompt).toBe("newer A");
    expect(input.promptRef.current).toBe("newer A");
    expect(input.resetCursor).not.toHaveBeenCalled();
  });

  it("delayed rejection restores A's complete snapshot without writing B refs", async () => {
    vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:retry");
    const input: Parameters<typeof restoreFailedComposerSend>[0] = {
      ...recovery(),
      snapshot: fullSnapshot(),
    };
    const rpc = deferredResult();
    const completion = rpc.promise.then((accepted) =>
      accepted ? false : restoreFailedComposerSend(input),
    );
    input.currentRouteThreadKeyRef.current = scopedThreadKey(b);
    input.promptRef.current = "next B";
    useComposerDraftStore.getState().setPrompt(b, "next B");
    rpc.resolve(false);
    expect(await completion).toBe(true);
    const draft = useComposerDraftStore.getState().getComposerDraft(a)!;
    expect(draft.files).toEqual(input.snapshot.files);
    expect(draft.images[0]?.file).toBe(input.snapshot.images[0]?.file);
    expect(draft.threadContexts).toEqual(input.snapshot.threadContexts);
    expect(input.promptRef.current).toBe("next B");
    expect(input.composerImagesRef.current).toEqual([]);
    expect(input.resetCursor).not.toHaveBeenCalled();
  });

  it("an older rejection cannot resurrect its prompt after a newer send emptied A", async () => {
    const input = recovery();
    let currentGeneration = 1;
    input.isCurrentSend = () => currentGeneration === 1;
    const rpc = deferredResult();
    const completion = rpc.promise.then(() => restoreFailedComposerSend(input));
    const store = useComposerDraftStore.getState();
    store.setPrompt(a, "second send");
    currentGeneration = 2;
    store.clearComposerContent(a);
    rpc.resolve(false);
    expect(await completion).toBe(false);
    expect(store.getComposerDraft(a)).toBeNull();
    expect(input.promptRef.current).toBe("");
    expect(input.resetCursor).not.toHaveBeenCalled();
  });

  it("recovery respects changed attachment revisions even when the prompt stays empty", () => {
    const input = recovery();
    const store = useComposerDraftStore.getState();
    store.setPrompt(a, "");
    input.expectedDraft = store.getComposerDraft(a);
    store.addFiles(a, fullSnapshot().files);
    expect(restoreFailedComposerSend(input)).toBe(false);
    expect(store.getComposerDraft(a)?.files).toHaveLength(1);
  });
});
