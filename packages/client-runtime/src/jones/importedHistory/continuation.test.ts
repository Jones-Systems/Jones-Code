import { describe, expect, it, vi } from "vite-plus/test";
import { CommandId, ThreadId, EnvironmentId, MessageId, AuthSessionId, type ImportedHistoryOutcome } from "@t3tools/contracts";
import { ImportedHistoryLockUnavailable, createImportedHistoryChoiceController, resolveImportedHistoryOutcome, resolveImportedHistoryReview, encodeImportedHistoryCorrelation, decodeImportedHistoryCorrelation, type ImportedHistoryCorrelation } from "./continuation.ts";
const value: ImportedHistoryCorrelation = {
  environmentId: EnvironmentId.make("environment:test"), draftIdentity: "draft:original",
  commandDigest: "a".repeat(64), deliveryDigest: "b".repeat(64),
  command: { type: "thread.imported-history.start", commandId: CommandId.make("command:test"), threadId: ThreadId.make("thread:test"), reviewedBasis: "c".repeat(64), delivery: { type: "message", command: {
    type: "message.dispatch", commandId: CommandId.make("command:test"), threadId: ThreadId.make("thread:test"), messageId: MessageId.make("message:test"), createdBy: "user", creationSource: "web", text: "Synthetic continuation", attachments: [], dispatchMode: { type: "start_immediately" },
  } } },
};
const outcome = (status: ImportedHistoryOutcome["status"]): ImportedHistoryOutcome => ({
  commandId: value.command.commandId, threadId: value.command.threadId, actorSessionId: AuthSessionId.make("session:test"), commandDigest: value.commandDigest, deliveryDigest: value.deliveryDigest, reviewedBasis: value.command.reviewedBasis, status, runId: null, effectId: null, reason: null,
});
function harness(initial: ImportedHistoryCorrelation | null = null) {
  let saved = initial === null ? null : encodeImportedHistoryCorrelation(initial);
  const start = vi.fn(async () => outcome("accepted"));
  const observe = vi.fn(async () => outcome("unknown"));
  const storage = {
    withLock: async <A>(operation: () => Promise<A>): Promise<A> => operation(),
    read: () => saved === null ? null : decodeImportedHistoryCorrelation(saved),
    reserve: (input: ImportedHistoryCorrelation) => { saved = encodeImportedHistoryCorrelation(input); },
    remove: () => { saved = null; },
  };
  return { storage, start, observe, controller: createImportedHistoryChoiceController(storage, { start, observe }) };
}
describe("imported history reviewed admission", () => {
  it("holds missing and null-basis review", () => {
    expect(resolveImportedHistoryReview(null).status).toBe("unavailable");
    expect(resolveImportedHistoryReview({ status: "available", reviewedBasis: null, reason: null }).status).toBe("unavailable");
  });
  it("rejects mismatched command, thread, basis and canonical digests", () => {
    for (const patch of [{ commandId: CommandId.make("command:other") }, { threadId: ThreadId.make("thread:other") }, { reviewedBasis: "d".repeat(64) }, { commandDigest: "d".repeat(64) }, { deliveryDigest: "d".repeat(64) }]) {
      expect(resolveImportedHistoryOutcome({ ...outcome("accepted"), ...patch }, value).status).toBe("unknown");
    }
  });
  it("shows admission as pending even when effect and run IDs are returned", () => {
    expect(resolveImportedHistoryOutcome({ ...outcome("accepted"), effectId: "effect:test" }, value)).toMatchObject({ status: "pending", intentAccepted: true });
  });
  it("saves and reads back before starting, and duplicate clicks do not submit twice", async () => {
    const h = harness();
    h.start.mockImplementation(async () => { expect(h.storage.read()?.command.commandId).toBe(value.command.commandId); return outcome("accepted"); });
    const first = h.controller.start(value, () => true);
    await h.controller.start(value, () => true);
    await first;
    expect(h.start).toHaveBeenCalledTimes(1);
    expect(h.storage.read()).not.toBeNull();
    await h.controller.start(value, () => true);
    expect(h.start).toHaveBeenCalledTimes(1);
    expect(h.observe).toHaveBeenCalledWith(value.command);
  });
  it("after a lost response and hydration observes the same command only", async () => {
    const h = harness();
    h.start.mockRejectedValueOnce(new Error("lost response"));
    expect((await h.controller.start(value, () => true)).status).toBe("unknown");
    const remount = createImportedHistoryChoiceController(h.storage, { start: h.start, observe: h.observe });
    await remount.observe();
    await remount.start({ ...value, command: { ...value.command, commandId: CommandId.make("command:replacement") } }, () => true);
    expect(h.start).toHaveBeenCalledTimes(1);
    expect(h.observe).toHaveBeenLastCalledWith(value.command);
  });
  it("serializes competing tabs and observes the first durable command without replacing it", async () => {
    const h = harness();
    let tail = Promise.resolve();
    h.storage.withLock = <A>(operation: () => Promise<A>): Promise<A> => {
      const result = tail.then(operation);
      tail = result.then(() => undefined, () => undefined);
      return result;
    };
    let release!: () => void;
    let signalStarted!: () => void;
    const started = new Promise<void>((resolve) => { signalStarted = resolve; });
    const finish = new Promise<void>((resolve) => { release = resolve; });
    h.start.mockImplementation(async () => { signalStarted(); await finish; return outcome("accepted"); });
    const otherTab = createImportedHistoryChoiceController(h.storage, { start: h.start, observe: h.observe });
    const first = h.controller.start(value, () => true);
    await started;
    const replacement = { ...value, draftIdentity: "draft:replacement", command: { ...value.command, commandId: CommandId.make("command:replacement") } };
    const second = otherTab.start(replacement, () => true);
    expect(h.storage.read()?.command.commandId).toBe(value.command.commandId);
    release();
    await Promise.all([first, second]);
    expect(h.start).toHaveBeenCalledTimes(1);
    expect(h.observe).toHaveBeenCalledWith(value.command);
    expect(h.storage.read()?.draftIdentity).toBe(value.draftIdentity);
  });
  it("holds with typed known-no-effect when Web Locks are unavailable", async () => {
    const h = harness();
    const storage = {
      withLock: async () => { throw new ImportedHistoryLockUnavailable(); },
      read: vi.fn(h.storage.read), reserve: vi.fn(h.storage.reserve), remove: vi.fn(h.storage.remove),
    };
    const controller = createImportedHistoryChoiceController(storage, h);
    expect(await controller.start(value, () => true)).toMatchObject({ status: "held", intentAccepted: false, effectOutcome: "known_no_effect" });
    expect(await controller.observe()).toMatchObject({ status: "held", effectOutcome: "known_no_effect" });
    expect(h.start).not.toHaveBeenCalled();
    expect(h.observe).not.toHaveBeenCalled();
    expect(storage.read).not.toHaveBeenCalled();
    expect(storage.reserve).not.toHaveBeenCalled();
  });
  it("storage failure and failed readback submit zero starts", async () => {
    for (const storage of [
      { withLock: async <A>(operation: () => Promise<A>): Promise<A> => operation(), read: () => null, reserve: () => { throw new Error("quota"); }, remove: () => undefined },
      { withLock: async <A>(operation: () => Promise<A>): Promise<A> => operation(), read: () => null, reserve: () => undefined, remove: () => undefined },
    ]) {
      const h = harness();
      await createImportedHistoryChoiceController(storage, h).start(value, () => true);
      expect(h.start).not.toHaveBeenCalled();
    }
  });
  it("changed draft, queue or route invalidates review before admission", async () => {
    const h = harness();
    await h.controller.start(value, () => false);
    expect(h.start).not.toHaveBeenCalled();
    expect(h.storage.read()).toBeNull();
  });
  it("only exact rejection releases the reserved correlation", async () => {
    const h = harness(value);
    h.observe.mockResolvedValueOnce({ ...outcome("rejected"), deliveryDigest: "wrong" });
    await h.controller.observe();
    expect(h.storage.read()).not.toBeNull();
    h.observe.mockResolvedValueOnce(outcome("rejected"));
    await h.controller.observe();
    expect(h.storage.read()).toBeNull();
  });
});
