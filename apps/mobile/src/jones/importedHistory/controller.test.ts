import { describe, expect, it, vi } from "vite-plus/test";
import {
  AuthSessionId,
  CommandId,
  EnvironmentId,
  MessageId,
  RunId,
  ThreadId,
  type ImportedHistoryDelivery,
  type ImportedHistoryOutcome,
} from "@t3tools/contracts";
import {
  decodeImportedHistoryCorrelation,
  encodeImportedHistoryCorrelation,
  type ImportedHistoryCorrelation,
} from "@t3tools/client-runtime/jones/imported-history/continuation";
import { SerializedAsyncQueue } from "../../lib/serialized-async-queue";
import { createMobileImportedHistoryChoice } from "./controller";
const environmentId = EnvironmentId.make("environment:test");
const threadId = ThreadId.make("thread:test");
const delivery: ImportedHistoryDelivery = {
  type: "message",
  command: {
    type: "message.dispatch",
    commandId: CommandId.make("command:test"),
    threadId,
    messageId: MessageId.make("message:test"),
    createdBy: "user",
    creationSource: "mobile",
    text: "Synthetic draft",
    attachments: [],
    dispatchMode: { type: "start_immediately" },
  },
};
function fixture(initial: string | null = null) {
  let saved = initial;
  let current = true;
  const lock = new SerializedAsyncQueue();
  const storage = {
    withLock: <A>(operation: () => Promise<A>) => lock.run(operation),
    read: () => (saved === null ? null : decodeImportedHistoryCorrelation(saved)),
    reserve: (correlation: ImportedHistoryCorrelation) => {
      saved = encodeImportedHistoryCorrelation(correlation);
    },
    remove: () => {
      saved = null;
    },
  };
  const outcome = (status: ImportedHistoryOutcome["status"]): ImportedHistoryOutcome => {
    const value = storage.read()!;
    return {
      commandId: value.command.commandId,
      threadId,
      actorSessionId: AuthSessionId.make("session:test"),
      reviewedBasis: value.command.reviewedBasis,
      commandDigest: value.commandDigest,
      deliveryDigest: value.deliveryDigest,
      status,
      runId: null,
      effectId: null,
      reason: null,
    };
  };
  const ports = {
    environmentId,
    threadId,
    storage,
    isCurrent: () => current,
    allocateCommandId: vi.fn(() => CommandId.make("command:queue")),
    review: vi.fn(async () => ({
      status: "available" as const,
      reviewedBasis: "a".repeat(64),
      reason: null,
    })),
    identity: vi.fn(async () => ({
      commandDigest: "b".repeat(64),
      deliveryDigest: "c".repeat(64),
    })),
    persistReadback: vi.fn(async () => undefined),
    start: vi.fn(async () => outcome("accepted")),
    observe: vi.fn(async (): Promise<ImportedHistoryOutcome | null> => null),
  };
  return {
    ports,
    outcome,
    snapshot: () => saved,
    setCurrent: (value: boolean) => {
      current = value;
    },
    choice: createMobileImportedHistoryChoice(ports),
  };
}
describe("mobile imported history admission", () => {
  it("persists before delivery and keeps accepted admission pending", async () => {
    const h = fixture();
    h.ports.persistReadback.mockImplementation(async () => {
      expect(h.ports.start).not.toHaveBeenCalled();
      expect(h.snapshot()).not.toBeNull();
    });
    await h.choice.review(delivery, "draft:original", () => true);
    await h.choice.start();
    expect(h.ports.start).toHaveBeenCalledTimes(1);
    expect(h.choice.snapshot()).toMatchObject({
      pending: true,
      canStart: false,
      outcome: { status: "pending", intentAccepted: true },
    });
    expect(h.choice.snapshot().notice).toContain("execution is not confirmed");
  });
  it("failed persistence submits zero starts and retains correlation", async () => {
    const h = fixture();
    h.ports.persistReadback.mockRejectedValueOnce(new Error("synthetic write failure"));
    await h.choice.review(delivery, "draft:original", () => true);
    await h.choice.start();
    expect(h.ports.start).not.toHaveBeenCalled();
    expect(h.snapshot()).not.toBeNull();
    expect(h.choice.snapshot().notice).toContain("No start was submitted");
  });
  it("lost response, not found and rehydration observe the original command without a new payload", async () => {
    const h = fixture();
    h.ports.start.mockRejectedValueOnce(new Error("lost response"));
    await h.choice.review(delivery, "draft:original", () => true);
    await h.choice.start();
    const remounted = createMobileImportedHistoryChoice(h.ports);
    await remounted.hydrate();
    await remounted.observe();
    await remounted.start();
    expect(h.ports.start).toHaveBeenCalledTimes(1);
    expect(h.ports.observe).toHaveBeenLastCalledWith(
      expect.objectContaining({ commandId: CommandId.make("command:test"), threadId }),
    );
    expect(remounted.snapshot()).toMatchObject({ pending: true, outcome: { status: "unknown" } });
    expect(h.ports.identity).toHaveBeenCalledTimes(1);
    expect(h.ports.allocateCommandId).not.toHaveBeenCalled();
  });
  it("draft edits and stale environments invalidate reviewed delivery", async () => {
    for (const changedScope of [false, true]) {
      const h = fixture();
      let unchanged = true;
      await h.choice.review(delivery, "draft:original", () => unchanged);
      if (changedScope) h.setCurrent(false);
      else unchanged = false;
      await h.choice.start();
      expect(h.ports.start).not.toHaveBeenCalled();
      expect(h.snapshot()).toBeNull();
    }
  });
  it("an edit during durable persistence cannot deliver or retire replacement text", async () => {
    const h = fixture();
    let unchanged = true;
    await h.choice.review(delivery, "draft:original", () => unchanged);
    h.ports.persistReadback.mockImplementation(async () => {
      unchanged = false;
    });
    await h.choice.start();
    expect(h.ports.start).not.toHaveBeenCalled();
    expect(h.choice.snapshot().pending).toBe(true);
    expect(h.choice.snapshot().notice).toContain("No start was submitted");
  });
  it("late review cannot admit replacement text", async () => {
    const h = fixture();
    let unchanged = true;
    h.ports.review.mockImplementation(async () => {
      unchanged = false;
      return { status: "available", reviewedBasis: "a".repeat(64), reason: null };
    });
    await h.choice.review(delivery, "draft:original", () => unchanged);
    await h.choice.start();
    expect(h.choice.snapshot().canStart).toBe(false);
    expect(h.ports.start).not.toHaveBeenCalled();
  });
  it("queued choice retains original run and message identities and unknown remains pending", async () => {
    const h = fixture();
    const queued = {
      type: "queued_run" as const,
      runId: RunId.make("run:original"),
      messageId: MessageId.make("message:original"),
    };
    await h.choice.review(queued, "queue:original", () => true);
    await h.choice.start();
    expect(h.ports.start).toHaveBeenCalledWith(
      expect.objectContaining({ delivery: queued, commandId: CommandId.make("command:queue") }),
    );
    h.ports.observe.mockImplementation(async () => h.outcome("unknown"));
    await h.choice.observe();
    expect(h.choice.snapshot()).toMatchObject({ pending: true, outcome: { status: "unknown" } });
  });
  it("missing review and failed hydration hold with zero starts", async () => {
    const h = fixture();
    h.ports.review.mockRejectedValueOnce(new Error("missing endpoint"));
    await h.choice.review(delivery, "draft:original", () => true);
    await h.choice.start();
    expect(h.ports.start).not.toHaveBeenCalled();
    const unavailable = createMobileImportedHistoryChoice({
      ...h.ports,
      storage: {
        ...h.ports.storage,
        withLock: async () => {
          throw new Error("missing persistence");
        },
      },
    });
    await unavailable.hydrate();
    await unavailable.start();
    expect(unavailable.snapshot().pending).toBe(true);
    expect(h.ports.start).not.toHaveBeenCalled();
  });
  it("two mounted controllers share exclusive reservation and never replace a pending command", async () => {
    const h = fixture();
    const second = createMobileImportedHistoryChoice(h.ports);
    await Promise.all([
      h.choice.review(delivery, "draft:first", () => true),
      second.review(delivery, "draft:second", () => true),
    ]);
    await Promise.all([h.choice.start(), second.start()]);
    expect(h.ports.start).toHaveBeenCalledTimes(1);
    expect(h.ports.storage.read()?.draftIdentity).toBe("draft:first");
    expect(h.ports.observe).toHaveBeenCalled();
  });
});
