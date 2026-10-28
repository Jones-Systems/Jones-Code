import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import {
  DraftId,
  partializeComposerDraftStoreState,
  useComposerDraftStore,
} from "../../composerDraftStore";
import { threadContextRecord } from "../../lib/composerContextRecords";
import { pendingDraftWork } from "../../components/chat/pendingDraftWork";
import {
  clearQuestionAttachmentDraft,
  useQuestionAttachmentPreparation,
} from "../../questionAttachments";
import { addThreadContextsWithCapture } from "./capture";

const target = DraftId.make("excerpt-test");
const record = () =>
  threadContextRecord(
    { environmentId: EnvironmentId.make("source"), threadId: ThreadId.make("related") },
    "Related",
  );
const excerpt = {
  status: "captured" as const,
  capturedAt: "2026-10-21T00:00:00.000Z",
  sourceSnapshotSequence: 12,
  earlierHistory: "omitted" as const,
  text: "Synthetic bounded history",
};
const result = { threadId: ThreadId.make("related"), excerpt };
function deferred() {
  let resolve!: (value: unknown) => void;
  const promise = new Promise<unknown>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
const read = () => useComposerDraftStore.getState().getComposerDraft(target)?.threadContexts ?? [];
const flush = async () => {
  await vi.advanceTimersByTimeAsync(0);
};

beforeEach(() => {
  vi.useFakeTimers();
  useComposerDraftStore.setState({ draftsByThreadKey: {} });
  useQuestionAttachmentPreparation.setState({ counts: {} });
});
afterEach(async () => {
  await vi.runAllTimersAsync();
  vi.useRealTimers();
});

describe("bounded thread capture lifecycle", () => {
  it("adds identity immediately, blocks send, persists the completed snapshot, and releases", async () => {
    const pending = deferred();
    const source = record();
    const request = vi.fn(() => pending.promise);
    addThreadContextsWithCapture({ target, records: [source], request, isCurrent: () => true });
    expect(read()).toEqual([source]);
    expect(pendingDraftWork.has(target)).toBe(true);
    await flush();
    expect(request).toHaveBeenCalledWith(source);
    pending.resolve(result);
    await flush();
    expect(read()[0]?.excerpt).toEqual(excerpt);
    expect(pendingDraftWork.has(target)).toBe(false);
    const saved = partializeComposerDraftStoreState(useComposerDraftStore.getState());
    expect(JSON.stringify(saved)).toContain("Synthetic bounded history");
    const hydrated = useComposerDraftStore.persist.getOptions().merge!(
      JSON.parse(JSON.stringify(saved)),
      useComposerDraftStore.getInitialState(),
    );
    expect(hydrated.draftsByThreadKey[target]?.threadContexts[0]?.excerpt).toEqual(excerpt);
  });

  it("does not refresh duplicate drops or copied snapshots", async () => {
    const request = vi.fn(async () => result);
    addThreadContextsWithCapture({
      target,
      records: [{ ...record(), excerpt }],
      request,
      isCurrent: () => true,
    });
    addThreadContextsWithCapture({ target, records: [record()], request, isCurrent: () => true });
    await flush();
    expect(request).not.toHaveBeenCalled();
    expect(read()).toHaveLength(1);
    expect(read()[0]?.excerpt).toEqual(excerpt);
  });

  it.each(["removed", "switched", "unmounted"])(
    "does not apply a completion after %s",
    async (change) => {
      const pending = deferred();
      let current = true;
      addThreadContextsWithCapture({
        target,
        records: [record()],
        request: () => pending.promise,
        isCurrent: () => current,
      });
      if (change === "removed") useComposerDraftStore.getState().setThreadContexts(target, []);
      else current = false;
      pending.resolve(result);
      await flush();
      expect(read().some((r) => r.excerpt !== undefined)).toBe(false);
      expect(pendingDraftWork.has(target)).toBe(false);
    },
  );

  it("does not resurrect a removed then re-added chip", async () => {
    const first = deferred();
    const second = deferred();
    addThreadContextsWithCapture({
      target,
      records: [record()],
      request: () => first.promise,
      isCurrent: () => true,
    });
    useComposerDraftStore.getState().setThreadContexts(target, []);
    addThreadContextsWithCapture({
      target,
      records: [record()],
      request: () => second.promise,
      isCurrent: () => true,
    });
    first.resolve(result);
    await flush();
    expect(read()[0]?.excerpt).toBeUndefined();
    expect(pendingDraftWork.has(target)).toBe(true);
    second.resolve(result);
    await flush();
    expect(read()[0]?.excerpt).toEqual(excerpt);
  });

  it("keeps capturing retained chips when another chip is removed", async () => {
    const pending = deferred();
    const retained = record();
    const removed = threadContextRecord(
      { environmentId: EnvironmentId.make("source"), threadId: ThreadId.make("removed") },
      "Removed",
    );
    addThreadContextsWithCapture({
      target,
      records: [retained, { ...removed, excerpt }],
      request: () => pending.promise,
      isCurrent: () => true,
    });
    useComposerDraftStore.getState().setThreadContexts(target, [retained]);
    pending.resolve(result);
    await flush();
    expect(read()).toHaveLength(1);
    expect(read()[0]?.excerpt).toEqual(excerpt);
  });

  it("times out after eight seconds, retains identity, and releases both send guards", async () => {
    addThreadContextsWithCapture({
      target,
      questionTarget: target,
      records: [record()],
      request: () => new Promise(() => {}),
      isCurrent: () => true,
    });
    expect(useQuestionAttachmentPreparation.getState().counts[target]).toBe(1);
    await vi.advanceTimersByTimeAsync(8_000);
    expect(read()[0]?.excerpt).toMatchObject({ status: "unavailable", reason: "timeout" });
    expect(pendingDraftWork.has(target)).toBe(false);
    expect(useQuestionAttachmentPreparation.getState().counts[target]).toBe(0);
  });

  it("an old question completion cannot release a new question capture after clear", async () => {
    const old = deferred();
    const next = deferred();
    addThreadContextsWithCapture({
      target,
      questionTarget: target,
      records: [record()],
      request: () => old.promise,
      isCurrent: () => true,
    });
    clearQuestionAttachmentDraft(target);
    addThreadContextsWithCapture({
      target,
      questionTarget: target,
      records: [record()],
      request: () => next.promise,
      isCurrent: () => true,
    });
    old.resolve(result);
    await flush();
    expect(useQuestionAttachmentPreparation.getState().counts[target]).toBe(1);
    expect(read()[0]?.excerpt).toBeUndefined();
    next.resolve(result);
    await flush();
    expect(useQuestionAttachmentPreparation.getState().counts[target]).toBe(0);
    expect(read()[0]?.excerpt).toEqual(excerpt);
  });
});
