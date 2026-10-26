import { describe, expect, it, vi } from "vite-plus/test";
import type {
  ThreadMonitorInvalidation,
  ThreadMonitorViewChunk,
} from "@t3tools/contracts/jones/desktopThreadMonitor";
import { ThreadMonitorBroker } from "./broker.ts";
import { envelope, peerPayload, publisherPayload } from "./transport.ts";

const lifetime = {
  publisherGeneration: "generation-1",
  environmentId: "environment-1",
  environmentEpoch: 0,
};
const ref = { ...lifetime, threadId: "thread-1" };
const available = { available: true, reason: null };
const row = {
  ...ref,
  title: "Real thread",
  projectLabel: "Project",
  group: "done",
  status: "completed",
  statusLabel: "Done",
  updatedAt: "2026-10-26T07:00:00Z",
  completedAt: "2026-10-26T07:00:00Z",
  completionTimeUnavailable: false,
  pendingChildAttention: null,
  actions: {
    reply: available,
    settle: available,
    respondToApproval: available,
    respondToUserInput: available,
    usePlan: available,
    restore: available,
  },
};
const environment = {
  ...lifetime,
  label: "VPS",
  transport: "connected",
  freshness: "live",
  reason: null,
};
function view(patch: Record<string, unknown> = {}): ThreadMonitorViewChunk {
  const frame = publisherPayload(
    envelope({
      type: "view",
      publisherGeneration: lifetime.publisherGeneration,
      snapshotId: "snapshot-1",
      revision: 1,
      chunkIndex: 0,
      chunkCount: 1,
      complete: true,
      replace: true,
      environments: [environment],
      upserts: [row],
      removed: [],
      counts: { needsYou: 0, working: 0, waiting: 0, done: 1 },
      ...patch,
    }),
  );
  if (frame.type !== "view") throw new Error("Invalid test view");
  return frame;
}
function invalidation(patch: Record<string, unknown> = {}): ThreadMonitorInvalidation {
  const frame = publisherPayload(
    envelope({ ...lifetime, type: "invalidateEnvironment", requestId: "retire-1", ...patch }),
  );
  if (frame.type !== "invalidateEnvironment") throw new Error("Invalid test retirement");
  return frame;
}
const viewState = (revision: number, patch: Record<string, unknown> = {}) => ({
  publisherGeneration: lifetime.publisherGeneration,
  revision,
  collapsed: { needsYou: false, working: true, waiting: false, done: true },
  list: { scrollTop: 120, anchors: [{ key: "section:working", offset: -12 }] },
  detail: { scrollTop: 0, anchors: [] },
  editor: { ref, quick: true, focused: true, selectionStart: 2, selectionEnd: 5 },
  ...patch,
});
function harness() {
  const peer = vi.fn(),
    publisher = vi.fn(),
    recreatePeer = vi.fn(),
    revealMain = vi.fn();
  const timers = new Map<() => void, number>();
  const broker = new ThreadMonitorBroker({
    peer,
    publisher,
    recreatePeer,
    revealMain,
    timer: (callback, milliseconds) => {
      timers.set(callback, milliseconds);
      return () => {
        timers.delete(callback);
      };
    },
  });
  broker.publish({ type: "reset", publisherGeneration: lifetime.publisherGeneration });
  broker.ready();
  peer.mockClear();
  publisher.mockClear();
  return {
    broker,
    peer,
    publisher,
    recreatePeer,
    revealMain,
    timers,
    fire: (milliseconds: number) => {
      for (const [callback, delay] of [...timers])
        if (delay === milliseconds) {
          timers.delete(callback);
          callback();
        }
    },
  };
}

describe("native monitor broker", () => {
  it("commits an entire ordered snapshot before enabling actions and recovers gaps without a partial cohort", () => {
    const h = harness();
    h.broker.publish(view({ chunkCount: 2, complete: false, upserts: [] }));
    expect(h.peer).not.toHaveBeenCalled();
    expect(() =>
      h.broker.intent(
        peerPayload(envelope({ ...ref, type: "open", actionId: "open-1", commandId: "command-1" })),
      ),
    ).toThrow();
    h.broker.publish(view({ chunkIndex: 1, chunkCount: 2, environments: [] }));
    expect(h.peer.mock.calls.map(([frame]) => frame.type)).toEqual(["view", "view"]);
    h.broker.intent(
      peerPayload(envelope({ ...ref, type: "open", actionId: "open-1", commandId: "command-1" })),
    );
    expect(h.publisher).toHaveBeenCalledWith(
      expect.objectContaining({ type: "open", threadId: "thread-1" }),
    );
    h.peer.mockClear();
    h.publisher.mockClear();
    h.broker.publish(view({ snapshotId: "gap", revision: 3, replace: false }));
    expect(h.publisher).toHaveBeenCalledWith({ type: "requestSnapshot" });
    expect(h.peer.mock.calls.map(([frame]) => frame.type)).toEqual(["reset"]);
    expect(() =>
      h.broker.intent(
        peerPayload(
          envelope({
            ...ref,
            type: "send",
            actionId: "send-1",
            commandId: "send-command",
            messageId: "message-1",
            draftRevision: 1,
            text: "Reply",
          }),
        ),
      ),
    ).toThrow();
  });

  it("purges buffered payloads synchronously, requires exact acknowledgment, and rejects every late retired delivery", async () => {
    const h = harness();
    h.broker.publish(view());
    h.broker.intent(
      peerPayload(envelope({ ...ref, type: "selectDetail", selectionId: "selection-1" })),
    );
    h.broker.publish(
      view({ snapshotId: "incomplete", revision: 2, chunkCount: 2, complete: false }),
    );
    const retirement = h.broker.invalidate(invalidation());
    expect(h.broker.accepts(invalidation())).toBe(false);
    expect(h.peer).toHaveBeenLastCalledWith(invalidation());
    expect(() =>
      h.broker.intent(
        peerPayload(
          envelope({ ...lifetime, type: "environmentInvalidated", requestId: "wrong-request" }),
        ),
      ),
    ).toThrow();
    for (const payload of [
      {
        type: "detail",
        detail: {
          ...ref,
          selectionId: "selection-1",
          revision: 1,
          entries: [],
          request: null,
          plan: null,
          unavailableReason: null,
        },
      },
      {
        type: "draft",
        draft: {
          ...ref,
          revision: 3,
          kind: "reply",
          requestId: null,
          text: "private draft",
          answers: {},
        },
      },
      {
        ...ref,
        type: "actionResult",
        actionId: "send-1",
        commandId: "command-1",
        status: "accepted",
        reason: null,
      },
    ])
      expect(() => h.broker.publish(publisherPayload(envelope(payload)))).toThrow();
    expect(() =>
      h.broker.intent(
        peerPayload(
          envelope({
            type: "editDraft",
            draft: {
              ...ref,
              revision: 4,
              kind: "reply",
              requestId: null,
              text: "late old draft",
              answers: {},
            },
          }),
        ),
      ),
    ).toThrow();
    h.broker.intent(
      peerPayload(envelope({ ...lifetime, type: "environmentInvalidated", requestId: "retire-1" })),
    );
    await expect(retirement).resolves.toEqual({
      ...lifetime,
      type: "environmentInvalidated",
      requestId: "retire-1",
    });
    h.peer.mockClear();
    h.broker.publish(view({ revision: 3, snapshotId: "old-late" }));
    expect(h.peer.mock.calls.every(([frame]) => frame.type !== "view")).toBe(true);
    h.broker.publish(
      view({
        revision: 4,
        snapshotId: "new-registration",
        environments: [{ ...environment, environmentEpoch: 1 }],
        upserts: [{ ...row, environmentEpoch: 1 }],
      }),
    );
    expect(h.broker.accepts({ ...invalidation(), environmentEpoch: 1 })).toBe(true);
    expect(h.broker.accepts(invalidation())).toBe(false);
  });

  it("destroys an unresponsive peer before completing retirement and replays surviving publisher data", async () => {
    const h = harness();
    h.broker.publish(view());
    const operation = h.broker.invalidate(invalidation());
    h.fire(5_000);
    expect(h.recreatePeer).toHaveBeenCalledTimes(1);
    await expect(operation).resolves.toEqual({
      ...lifetime,
      type: "environmentInvalidated",
      requestId: "retire-1",
    });
    h.broker.ready();
    expect(h.publisher).toHaveBeenLastCalledWith({ type: "ready" });
    h.broker.publish(
      view({
        revision: 2,
        snapshotId: "remaining",
        environments: [],
        upserts: [],
        counts: { needsYou: 0, working: 0, waiting: 0, done: 0 },
      }),
    );
    expect(h.peer).toHaveBeenLastCalledWith(
      expect.objectContaining({ type: "view", environments: [], upserts: [] }),
    );
  });

  it("replays the same committed revision after peer recreation without retiring environments", () => {
    const h = harness();
    h.broker.publish(view());
    h.broker.peerLost();
    h.broker.ready();
    h.peer.mockClear();
    h.broker.publish(view());
    expect(h.peer).toHaveBeenCalledWith(view());
    expect(h.broker.accepts(invalidation())).toBe(true);
    expect(() =>
      h.broker.publish({ type: "reset", publisherGeneration: "unrelated-document" }),
    ).toThrow();
    h.broker.publisherLost();
    expect(() =>
      h.broker.publish({ type: "reset", publisherGeneration: "next-document" }),
    ).not.toThrow();
  });

  it("keeps unknown command identity, draft version and open routing/reveal order", () => {
    const h = harness();
    h.broker.publish(view());
    const send = peerPayload(
      envelope({
        ...ref,
        type: "send",
        actionId: "send-1",
        commandId: "command-1",
        messageId: "message-1",
        draftRevision: 4,
        text: "Reply",
      }),
    );
    h.broker.intent(send);
    h.broker.intent(send);
    expect(h.publisher.mock.calls.filter(([intent]) => intent.type === "send")).toHaveLength(1);
    h.fire(15_000);
    expect(h.peer).toHaveBeenLastCalledWith(
      expect.objectContaining({
        type: "actionResult",
        status: "unknown",
        actionId: "send-1",
        commandId: "command-1",
        submittedDraftRevision: 4,
      }),
    );
    h.broker.intent(send);
    expect(h.publisher.mock.calls.filter(([intent]) => intent.type === "send")).toHaveLength(1);
    expect(() =>
      h.broker.publish(
        publisherPayload(
          envelope({
            ...ref,
            type: "actionResult",
            actionId: "send-1",
            commandId: "command-1",
            status: "accepted",
            reason: null,
            submittedDraftRevision: 3,
          }),
        ),
      ),
    ).toThrow();
    h.broker.publish(
      publisherPayload(
        envelope({
          ...ref,
          type: "actionResult",
          actionId: "send-1",
          commandId: "wrong-command",
          status: "accepted",
          reason: null,
        }),
      ),
    );
    expect(h.revealMain).not.toHaveBeenCalled();
    h.broker.intent(
      peerPayload(
        envelope({ ...ref, type: "open", actionId: "open-1", commandId: "open-command" }),
      ),
    );
    expect(h.revealMain).not.toHaveBeenCalled();
    const accepted = publisherPayload(
      envelope({
        ...ref,
        type: "actionResult",
        actionId: "open-1",
        commandId: "open-command",
        status: "accepted",
        reason: null,
      }),
    );
    h.broker.publish(accepted);
    h.broker.publish(accepted);
    expect(h.revealMain).toHaveBeenCalledTimes(1);
  });

  it("admits exact restoration after the settled row disappears and refuses unrelated restore IDs", () => {
    const h = harness();
    h.broker.publish(view());
    h.broker.intent(
      peerPayload(
        envelope({
          ...ref,
          type: "settle",
          actionId: "settle-1",
          commandId: "settle-command",
          expectedUpdatedAt: row.updatedAt,
        }),
      ),
    );
    h.broker.publish(
      publisherPayload(
        envelope({
          ...ref,
          type: "actionResult",
          actionId: "settle-1",
          commandId: "settle-command",
          status: "accepted",
          reason: null,
          settleCommandId: "settle-command",
        }),
      ),
    );
    h.broker.publish(
      view({
        revision: 2,
        snapshotId: "settled",
        upserts: [],
        counts: { needsYou: 0, working: 0, waiting: 0, done: 0 },
      }),
    );
    h.broker.intent(
      peerPayload(
        envelope({
          ...ref,
          type: "restore",
          actionId: "restore-1",
          commandId: "restore-command",
          settleCommandId: "settle-command",
        }),
      ),
    );
    expect(h.publisher).toHaveBeenLastCalledWith(
      expect.objectContaining({ type: "restore", settleCommandId: "settle-command" }),
    );
    expect(() =>
      h.broker.intent(
        peerPayload(
          envelope({
            ...ref,
            type: "restore",
            actionId: "restore-2",
            commandId: "restore-other",
            settleCommandId: "not-the-settlement",
          }),
        ),
      ),
    ).toThrow();
  });

  it("forwards main-owned view state with a generation-wide revision fence across peer recreation", () => {
    const h = harness();
    h.broker.publish(view());
    h.broker.intent(peerPayload(envelope({ type: "setViewState", state: viewState(4) })));
    expect(h.publisher).toHaveBeenLastCalledWith({ type: "setViewState", state: viewState(4) });
    const accepted = publisherPayload(envelope({ type: "viewState", state: viewState(4) }));
    h.broker.publish(accepted);
    h.broker.peerLost();
    h.broker.ready();
    h.peer.mockClear();
    h.broker.publish(view());
    h.broker.publish(accepted);
    expect(h.peer).toHaveBeenLastCalledWith(accepted);
    expect(() =>
      h.broker.intent(peerPayload(envelope({ type: "setViewState", state: viewState(4) }))),
    ).toThrow();
    h.peer.mockClear();
    h.broker.publish(publisherPayload(envelope({ type: "viewState", state: viewState(3) })));
    expect(h.peer).not.toHaveBeenCalled();
    h.broker.intent(peerPayload(envelope({ type: "setViewState", state: viewState(5) })));
    expect(h.publisher).toHaveBeenLastCalledWith({ type: "setViewState", state: viewState(5) });
    expect(() =>
      h.broker.publish(
        publisherPayload(
          envelope({ type: "viewState", state: viewState(6, { publisherGeneration: "foreign" }) }),
        ),
      ),
    ).toThrow();
  });

  it("does not restore retired editor identity from late view-state mirrors or intents", async () => {
    const h = harness();
    h.broker.publish(view());
    h.broker.publish(publisherPayload(envelope({ type: "viewState", state: viewState(4) })));
    const retired = h.broker.invalidate(invalidation());
    expect(() =>
      h.broker.intent(peerPayload(envelope({ type: "setViewState", state: viewState(5) }))),
    ).toThrow();
    expect(() =>
      h.broker.publish(publisherPayload(envelope({ type: "viewState", state: viewState(5) }))),
    ).toThrow();
    h.broker.intent(
      peerPayload(envelope({ ...lifetime, type: "environmentInvalidated", requestId: "retire-1" })),
    );
    await retired;
    const sanitized = publisherPayload(
      envelope({
        type: "viewState",
        state: viewState(5, {
          editor: null,
          list: { scrollTop: 0, anchors: [] },
          detail: { scrollTop: 0, anchors: [] },
        }),
      }),
    );
    h.broker.publish(sanitized);
    expect(h.peer).toHaveBeenLastCalledWith(sanitized);
    h.broker.peerLost();
    h.broker.ready();
    h.broker.publish(sanitized);
    expect(h.peer).toHaveBeenLastCalledWith(sanitized);
  });
});
