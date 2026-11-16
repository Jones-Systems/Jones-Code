import { CommandId, EnvironmentId, MessageId, ThreadId } from "@t3tools/contracts";
import type {
  ThreadMonitorAction,
  ThreadMonitorDraft,
  ThreadMonitorViewChunk,
} from "@t3tools/contracts/jones/desktopThreadMonitor";
import { describe, expect, it } from "vite-plus/test";
import { acceptMonitorChunk, MonitorLifetimeFence, monitorDraftKey } from "./lifecycle.ts";
import {
  clearAcceptedMonitorDraft,
  MonitorActionLedger,
  monitorActionUnavailableReason,
  monitorReplyDispatchMode,
} from "./actions.ts";
const environmentId = EnvironmentId.make("e");
const otherEnvironmentId = EnvironmentId.make("other");
const threadId = ThreadId.make("t");
const counts = { needsYou: 0, working: 0, waiting: 0, done: 0 };

describe("monitor lifetime and reliable action identity", () => {
  it("retires all old delivery identities while reconnect and hide retain lifetime", () => {
    const fence = new MonitorLifetimeFence("generation");
    const old = fence.register(environmentId);
    const surviving = fence.register(otherEnvironmentId);
    expect(fence.register(environmentId)).toEqual(old);
    const oldDraftKey = monitorDraftKey({ ...old, threadId });
    expect(fence.retire(environmentId)).toEqual(old);
    expect(fence.accepts(old)).toBe(false);
    const replacement = fence.register(environmentId);
    expect(replacement.environmentEpoch).toBe(old.environmentEpoch + 1);
    expect(monitorDraftKey({ ...replacement, threadId })).not.toBe(oldDraftKey);
    expect(fence.accepts(surviving)).toBe(true);
    expect(fence.accepts(old)).toBe(false);
    expect(fence.accepts(replacement)).toBe(true);
    expect(fence.accepts({ ...replacement, publisherGeneration: "earlier-generation" })).toBe(
      false,
    );
  });
  it("cannot make partial/out-of-order snapshots actionable or resurrect retired rows", () => {
    const fence = new MonitorLifetimeFence("g");
    const lifetime = fence.register(environmentId);
    const first: ThreadMonitorViewChunk = {
      type: "view",
      publisherGeneration: "g",
      snapshotId: "s",
      revision: 1,
      chunkIndex: 0,
      chunkCount: 2,
      complete: false,
      replace: true,
      environments: [
        { ...lifetime, label: "Test", transport: "connected", freshness: "live", reason: null },
      ],
      upserts: [],
      removed: [],
      counts,
    };
    const state = acceptMonitorChunk(null, first, (ref) => fence.accepts(ref));
    expect(state?.complete).toBe(false);
    const last = { ...first, chunkIndex: 1, complete: true };
    expect(acceptMonitorChunk(null, last, (ref) => fence.accepts(ref))).toBeNull();
    expect(acceptMonitorChunk(state, last, (ref) => fence.accepts(ref))?.complete).toBe(true);
    fence.retire(environmentId);
    fence.register(environmentId);
    expect(acceptMonitorChunk(state, last, (ref) => fence.accepts(ref))).toBeNull();
  });
  it("preserves edits made while awaiting durable acceptance and separate decision drafts", () => {
    const draft: ThreadMonitorDraft = {
      publisherGeneration: "g",
      environmentId,
      environmentEpoch: 0,
      threadId,
      revision: 7,
      kind: "reply",
      requestId: null,
      text: "typed after send",
      answers: {},
    };
    expect(clearAcceptedMonitorDraft(draft, 6)).toBe(draft);
    expect(clearAcceptedMonitorDraft(draft, 7)).toMatchObject({ text: "", revision: 8 });
    expect(monitorDraftKey(draft, "request-1")).not.toBe(monitorDraftKey(draft));
  });
  it("never admits unknown operation replay with new delivery attempts", () => {
    const ledger = new MonitorActionLedger();
    const action: ThreadMonitorAction = {
      publisherGeneration: "g",
      environmentId,
      environmentEpoch: 0,
      threadId,
      type: "send",
      actionId: "a",
      commandId: CommandId.make("c"),
      messageId: MessageId.make("m"),
      draftRevision: 0,
      text: "reply",
    };
    expect(ledger.admit(action)).toBe("new");
    expect(ledger.transition("a", "unknown")).toBe(true);
    expect(ledger.admit(action)).toBe("duplicate");
    expect(ledger.admit({ ...action, commandId: CommandId.make("fresh") })).toBe("conflict");
    expect(ledger.transition("a", "accepted")).toBe(true);
    expect(ledger.transition("a", "pending")).toBe(false);
    expect(monitorReplyDispatchMode(true)).toBe("queue");
    expect(monitorReplyDispatchMode(false)).toBe("queue");
    const context = {
      acceptsLifetime: () => true,
      threadExists: true,
      transportConnected: true,
      snapshotLive: true,
      canOperate: true,
      canQueue: false,
      workOrRequestActive: true,
    };
    expect(monitorActionUnavailableReason(action, context)).toMatch(/queue/);
    expect(
      monitorActionUnavailableReason(action, { ...context, acceptsLifetime: () => false }),
    ).toMatch(/lifetime/);
    expect(
      monitorActionUnavailableReason(
        { ...action, type: "open" },
        { ...context, transportConnected: false },
      ),
    ).toBeNull();
  });
});
