import { describe, expect, it } from "vite-plus/test";
import { envelope, peerPayload, publisherPayload, statePayload } from "./transport.ts";

describe("native monitor ingress direction", () => {
  it("does not confuse a valid protocol payload with sender authority", () => {
    expect(() => publisherPayload(envelope({ type: "hide" }))).toThrow();
    expect(() => peerPayload(envelope({ type: "reset", publisherGeneration: "g" }))).toThrow();
    expect(() => publisherPayload(envelope({ type: "ready" }))).toThrow();
    expect(peerPayload(envelope({ type: "requestMore", group: "waiting" }))).toEqual({
      type: "requestMore",
      group: "waiting",
    });
    expect(() =>
      peerPayload(envelope({ type: "requestMore", group: "all", command: "run" })),
    ).toThrow();
  });
  it("requires actual native activity fields and rejects state-bearing excess properties", () => {
    const state = {
      supported: true,
      enabled: true,
      visible: true,
      mainVisible: false,
      mainFocused: false,
      expanded: false,
      reason: null,
    };
    expect(statePayload(envelope(state))).toEqual(state);
    expect(() => statePayload(envelope({ ...state, credentials: "not allowed" }))).toThrow();
    expect(() => statePayload(envelope({ ...state, mainVisible: undefined }))).toThrow();
  });
  it("accepts frozen view-state directions while rejecting text, foreign payloads and oversized coordinates", () => {
    const state = {
      publisherGeneration: "g",
      revision: 1,
      collapsed: { needsYou: false, working: true, waiting: false, done: false },
      list: { scrollTop: 300, anchors: [{ key: "section:working", offset: -10 }] },
      detail: { scrollTop: 0, anchors: [] },
      editor: null,
    };
    expect(publisherPayload(envelope({ type: "viewState", state }))).toEqual({
      type: "viewState",
      state,
    });
    expect(peerPayload(envelope({ type: "setViewState", state }))).toEqual({
      type: "setViewState",
      state,
    });
    expect(() => publisherPayload(envelope({ type: "setViewState", state }))).toThrow();
    expect(() => peerPayload(envelope({ type: "viewState", state }))).toThrow();
    expect(() =>
      peerPayload(envelope({ type: "setViewState", state: { ...state, draftText: "private" } })),
    ).toThrow();
    expect(() =>
      peerPayload(
        envelope({
          type: "setViewState",
          state: { ...state, list: { scrollTop: Infinity, anchors: [] } },
        }),
      ),
    ).toThrow();
  });
});
