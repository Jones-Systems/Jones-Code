import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  DESKTOP_THREAD_MONITOR_PROTOCOL,
  decodeThreadMonitorFrame,
  THREAD_MONITOR_LIMITS,
  ThreadMonitorActionResult,
} from "./desktopThreadMonitor.ts";
const wrap = (payload: unknown) => ({ schemaVersion: DESKTOP_THREAD_MONITOR_PROTOCOL, payload });
const ref = {
  publisherGeneration: "generation-1",
  environmentId: "environment-1",
  environmentEpoch: 0,
  threadId: "thread-1",
};
const action = { ...ref, actionId: "action-1", commandId: "command-1" };

describe("desktop monitor IPC boundary", () => {
  it("keeps legacy results decodable and bounds the opt-in Restore identity extension", () => {
    const legacy = { ...action, type: "actionResult", status: "unknown", reason: null };
    const { restoreIdentity: _, ...legacyFields } = ThreadMonitorActionResult.fields;
    const legacyDecode = Schema.decodeUnknownSync(Schema.Struct(legacyFields), {
      onExcessProperty: "error",
    });
    expect(legacyDecode(legacy)).toEqual(legacy);
    expect(decodeThreadMonitorFrame(wrap(legacy)).payload).toEqual(legacy);
    const extended = {
      ...legacy,
      restoreIdentity: { type: "restore", settleCommandId: "settle-1" },
    };
    expect(decodeThreadMonitorFrame(wrap(extended)).payload).toEqual(extended);
    expect(() => legacyDecode(extended)).toThrow();
    for (const restoreIdentity of [
      { type: "send", settleCommandId: "settle-1" },
      { type: "restore" },
      { type: "restore", settleCommandId: "settle-1", text: "private" },
    ])
      expect(() => decodeThreadMonitorFrame(wrap({ ...legacy, restoreIdentity }))).toThrow();
    for (const payload of [
      { type: "requestSnapshot" },
      { type: "requestSnapshot", restoreIdentityReplay: { publisherGeneration: "generation-1" } },
    ])
      expect(decodeThreadMonitorFrame(wrap(payload)).payload).toEqual(payload);
  });
  it("accepts genuine scoped actions without accepting arbitrary commands", () => {
    for (const payload of [
      { ...action, type: "send", messageId: "message-1", draftRevision: 0, text: "hello" },
      { ...action, type: "respondToApproval", requestId: "request-1", decision: "accept" },
      {
        ...action,
        type: "respondToUserInput",
        requestId: "request-1",
        answers: { a: ["original-value"], b: "custom" },
        draftRevision: 1,
      },
      { ...action, type: "usePlan", planId: "plan-1", messageId: "message-2" },
      { ...action, type: "restore", settleCommandId: "settle-1" },
    ])
      expect(decodeThreadMonitorFrame(wrap(payload)).payload).toEqual(payload);
    expect(() => decodeThreadMonitorFrame(wrap({ ...action, type: "run.interrupt" }))).toThrow();
    expect(() =>
      decodeThreadMonitorFrame(
        wrap({
          ...action,
          type: "respondToApproval",
          requestId: "request-1",
          decision: "acceptAlways",
        }),
      ),
    ).toThrow();
    expect(() =>
      decodeThreadMonitorFrame(
        wrap({ ...action, type: "open", arbitraryUrl: "https://example.test" }),
      ),
    ).toThrow();
  });
  it("requires exact lifetimes and protocol versions", () => {
    const payload = { ...ref, type: "invalidateEnvironment", requestId: "invalidation-1" };
    const { threadId: _, ...control } = payload;
    expect(decodeThreadMonitorFrame(wrap(control)).payload).toEqual(control);
    expect(() => decodeThreadMonitorFrame({ schemaVersion: "future", payload: control })).toThrow();
    expect(() => decodeThreadMonitorFrame(wrap({ ...control, environmentEpoch: -1 }))).toThrow();
    expect(() =>
      decodeThreadMonitorFrame(wrap({ ...control, environmentEpoch: undefined })),
    ).toThrow();
    expect(() =>
      decodeThreadMonitorFrame(wrap({ ...control, text: "private draft in ack" })),
    ).toThrow();
  });
  it("bounds bytes, draft code units and structured payloads independently", () => {
    const send = { ...action, type: "send", messageId: "message-1", draftRevision: 0 };
    expect(() =>
      decodeThreadMonitorFrame(
        wrap({ ...send, text: "x".repeat(THREAD_MONITOR_LIMITS.draftCodeUnits) }),
      ),
    ).not.toThrow();
    expect(() =>
      decodeThreadMonitorFrame(
        wrap({ ...send, text: "x".repeat(THREAD_MONITOR_LIMITS.draftCodeUnits + 1) }),
      ),
    ).toThrow();
    expect(() =>
      decodeThreadMonitorFrame(
        wrap({ ...send, text: "é".repeat(THREAD_MONITOR_LIMITS.frameBytes) }),
      ),
    ).toThrow();
    expect(() =>
      decodeThreadMonitorFrame(
        wrap({
          ...action,
          type: "respondToUserInput",
          requestId: "request-1",
          draftRevision: 0,
          answers: { a: { arbitrary: true } },
        }),
      ),
    ).toThrow();
  });
  it("rejects incomplete and inconsistent chunk markers", () => {
    const chunk = {
      type: "view",
      publisherGeneration: "generation-1",
      snapshotId: "s-1",
      revision: 1,
      chunkIndex: 0,
      chunkCount: 2,
      complete: false,
      replace: true,
      environments: [],
      upserts: [],
      removed: [],
      counts: { needsYou: 0, working: 0, waiting: 0, done: 0 },
    };
    expect(() => decodeThreadMonitorFrame(wrap(chunk))).not.toThrow();
    expect(() => decodeThreadMonitorFrame(wrap({ ...chunk, complete: true }))).toThrow();
    expect(() => decodeThreadMonitorFrame(wrap({ ...chunk, chunkIndex: 2 }))).toThrow();
  });
  it("admits only allowlisted passive theme values and explicit native controls", () => {
    const theme = {
      type: "theme",
      publisherGeneration: "g",
      theme: {
        mode: "dark",
        values: { "--background": "oklch(0.2 0 0)", "--font-sans": "Inter, sans-serif" },
      },
    };
    expect(() => decodeThreadMonitorFrame(wrap(theme))).not.toThrow();
    for (const values of [
      { "--background": "url(https://example.test)" },
      { "--private-setting": "value" },
    ]) {
      expect(() =>
        decodeThreadMonitorFrame(wrap({ ...theme, theme: { mode: "dark", values } })),
      ).toThrow();
    }
    for (const payload of [
      { type: "hide" },
      { type: "requestMore", group: "done" },
      { type: "requestSnapshot" },
    ]) {
      expect(decodeThreadMonitorFrame(wrap(payload)).payload).toEqual(payload);
    }
  });
  it("bounds content-free view state and rejects text or reversed selections", () => {
    const state = {
      publisherGeneration: "generation-1",
      revision: 1,
      collapsed: { needsYou: false, working: true, waiting: false, done: false },
      list: { scrollTop: 1.5, anchors: [{ key: "section:working", offset: -10 }] },
      detail: { scrollTop: 0, anchors: [] },
      editor: { ref, quick: true, focused: true, selectionStart: 0, selectionEnd: 5 },
    };
    for (const type of ["setViewState", "viewState"]) {
      expect(decodeThreadMonitorFrame(wrap({ type, state })).payload).toEqual({ type, state });
    }
    expect(() =>
      decodeThreadMonitorFrame(wrap({ type: "setViewState", state: { ...state, text: "draft" } })),
    ).toThrow();
    expect(() =>
      decodeThreadMonitorFrame(
        wrap({
          type: "setViewState",
          state: { ...state, editor: { ...state.editor, selectionStart: 6 } },
        }),
      ),
    ).toThrow();
    expect(() =>
      decodeThreadMonitorFrame(
        wrap({
          type: "setViewState",
          state: {
            ...state,
            list: { scrollTop: 0, anchors: Array(9).fill({ key: "row", offset: 0 }) },
          },
        }),
      ),
    ).toThrow();
  });
});
