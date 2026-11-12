import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { DesktopBrowserCommand, DesktopBrowserEvent } from "../desktopBrowser.ts";
import {
  CompanionDown,
  CompanionUp,
  DesktopCompanionTicketResponse,
  PREVIEW_COMPANION_HTTP_BASE,
  PREVIEW_COMPANION_PROTOCOL,
  PREVIEW_COMPANION_WS_PATH,
  PREVIEW_STREAM_RENDER_HOST_UNAVAILABLE_CLOSE_CODE,
  PreviewCompanionCapabilities,
  PreviewCompanionDefaultSelectionInput,
  PreviewCompanionHostId,
  PreviewCompanionHostsResponse,
  PreviewCompanionThreadSelectionInput,
  PreviewCompanionThreadSelectionResponse,
  PreviewRenderHostSelection,
} from "./previewCompanion.ts";

const runtimeIdentity = {
  schemaVersion: 1,
  runtimeKind: "electron",
  runtimeInstanceId: "companion-runtime",
  appVersion: "1.0.0",
  buildCommit: null,
};
const capabilities = {
  cdp: true,
  clipboardText: true,
  uploads: false,
  downloads: false,
  recording: false,
};
const tab = { threadId: "thread-1", tabId: "tab-1" };
const selection = { _tag: "companion", hostId: "mini-1" };
const hello = {
  type: "hello",
  protocol: 1,
  hostId: "mini-1",
  label: "Mini",
  platform: "darwin",
  runtimeIdentity,
  capabilities,
};
const welcome = {
  type: "welcome",
  protocol: 1,
  environmentId: "environment-1",
  connectionGeneration: 1,
  heartbeatMs: 15000,
};
// Wire timestamps are Unix epoch milliseconds.
const heartbeat = { type: "heartbeat", sentAt: 1791417600000 };
const chunk = { type: "chunk", id: "message-1", index: 0, final: false, data: '{"type":' };

const events = [
  { type: "attached", ...tab, runtimeIdentity },
  { type: "attached", ...tab },
  { type: "attached", ...tab, runtimeIdentity: null },
  { type: "detached", ...tab },
  { type: "cdp", ...tab, message: '{"id":1,"result":{}}' },
];
const commands = [
  { type: "turn", threadId: tab.threadId, active: true },
  { type: "turn", threadId: tab.threadId, active: false },
  { type: "cdp", ...tab, message: '{"id":1,"method":"Page.enable"}' },
  { type: "release", ...tab },
  { type: "pointer", ...tab, phase: "move", x: 1.5, y: -2 },
  { type: "pointer", ...tab, phase: "click", x: 0, y: 0 },
];

const compiledEncodeSync1 = Schema.encodeSync(CompanionUp);
const compiledDecodeUnknownSync2 = Schema.decodeUnknownSync(CompanionUp);
const compiledEncodeSync3 = Schema.encodeSync(CompanionDown);
const compiledDecodeUnknownSync4 = Schema.decodeUnknownSync(CompanionDown);
const compiledDecodeUnknownSync5 = Schema.decodeUnknownSync(DesktopBrowserEvent);
const compiledDecodeUnknownSync6 = Schema.decodeUnknownSync(DesktopBrowserCommand);
const compiledDecodeUnknownSync7 = Schema.decodeUnknownSync(PreviewRenderHostSelection);
const compiledDecodeUnknownSync8 = Schema.decodeUnknownSync(PreviewCompanionCapabilities);
const compiledDecodeUnknownOption9 = Schema.decodeUnknownOption(CompanionUp);
const compiledDecodeUnknownOption10 = Schema.decodeUnknownOption(CompanionDown);
const compiledDecodeSync11 = Schema.decodeSync(CompanionDown);
const compiledDecodeSync12 = Schema.decodeSync(CompanionUp);

describe("Preview companion wire contracts", () => {
  it("uses additive Jones endpoints and protocol 1", () => {
    expect(PREVIEW_COMPANION_PROTOCOL).toBe(1);
    expect(PREVIEW_COMPANION_HTTP_BASE).toBe("/api/jones/preview-companion");
    expect(PREVIEW_COMPANION_WS_PATH).toBe(`${PREVIEW_COMPANION_HTTP_BASE}/ws`);
    expect(PREVIEW_STREAM_RENDER_HOST_UNAVAILABLE_CLOSE_CODE).toBe(4504);
  });

  it.each([
    hello,
    heartbeat,
    chunk,
    { ...chunk, index: 1, final: true, data: "}" },
    { type: "mounted", ...tab },
    { type: "unmountedAck", ...tab },
    ...events.map((event) => ({ type: "browser", event })),
  ])("round trips uplink $type messages", (wire) => {
    expect(compiledEncodeSync1(compiledDecodeUnknownSync2(wire))).toEqual(wire);
  });

  it.each([
    welcome,
    heartbeat,
    chunk,
    { type: "mount", ...tab },
    { type: "unmount", ...tab },
    { type: "assignments", tabs: [tab] },
    { type: "assignments", tabs: [] },
    ...commands.map((command) => ({ type: "browser", command })),
  ])("round trips downlink $type messages", (wire) => {
    expect(compiledEncodeSync3(compiledDecodeUnknownSync4(wire))).toEqual(wire);
  });

  it("preserves the existing desktop event and command payloads", () => {
    for (const event of events) {
      expect(compiledDecodeUnknownSync2({ type: "browser", event })).toEqual({
        type: "browser",
        event: compiledDecodeUnknownSync5(event),
      });
    }
    for (const command of commands) {
      expect(compiledDecodeUnknownSync4({ type: "browser", command })).toEqual({
        type: "browser",
        command: compiledDecodeUnknownSync6(command),
      });
    }
  });

  it("trims host identities and labels and accepts their exact length limit", () => {
    const decodeId = Schema.decodeUnknownSync(PreviewCompanionHostId);
    expect(decodeId(" Mini_1-a ")).toBe("Mini_1-a");
    expect(decodeId("a".repeat(64))).toBe("a".repeat(64));
    expect(compiledDecodeUnknownSync2({ ...hello, label: ` ${"a".repeat(64)} ` })).toEqual({
      ...hello,
      label: "a".repeat(64),
    });
  });

  it.each(["", " ", "a".repeat(65), "mini.local", "mini/1", "mini 1", "méni"])(
    "rejects invalid host ID %j",
    (hostId) => {
      expect(() => compiledDecodeUnknownSync2({ ...hello, hostId })).toThrow();
      expect(() => compiledDecodeUnknownSync7({ ...selection, hostId })).toThrow();
    },
  );

  it.each(["", " ", "a".repeat(65)])("rejects invalid host label %j", (label) => {
    expect(() => compiledDecodeUnknownSync2({ ...hello, label })).toThrow();
  });

  it("keeps V1 unsupported capabilities disabled and clipboard support explicit", () => {
    expect(
      compiledDecodeUnknownSync8({
        ...capabilities,
        clipboardText: false,
      }),
    ).toEqual({
      ...capabilities,
      clipboardText: false,
    });
    for (const key of ["uploads", "downloads", "recording"]) {
      expect(() => compiledDecodeUnknownSync8({ ...capabilities, [key]: true })).toThrow();
    }
    expect(() => compiledDecodeUnknownSync8({ ...capabilities, cdp: false })).toThrow();
    expect(() =>
      compiledDecodeUnknownSync8({
        ...capabilities,
        clipboardText: undefined,
      }),
    ).toThrow();
  });

  it("rejects unsupported protocols and malformed known messages", () => {
    expect(() => compiledDecodeUnknownSync2({ ...hello, protocol: 2 })).toThrow();
    expect(() => compiledDecodeUnknownSync4({ ...welcome, protocol: 2 })).toThrow();
    expect(() => compiledDecodeUnknownSync2({ ...hello, runtimeIdentity: null })).toThrow();
    expect(() =>
      compiledDecodeUnknownSync2({ type: "browser", event: { type: "attached" } }),
    ).toThrow();
    expect(() =>
      compiledDecodeUnknownSync4({ type: "browser", command: { type: "release" } }),
    ).toThrow();
  });

  it.each(["future-message", "thread.create", "orchestration.dispatch"])(
    "lets transport ignore unknown %s envelopes through Option decoding",
    (type) => {
      expect(Option.isNone(compiledDecodeUnknownOption9({ type }))).toBe(true);
      expect(Option.isNone(compiledDecodeUnknownOption10({ type }))).toBe(true);
    },
  );

  it("bounds envelope identifiers, chunks, timestamps, and generation counters", () => {
    for (const wire of [
      { ...chunk, id: "a".repeat(129) },
      { ...chunk, index: -1 },
      { ...chunk, index: 0.5 },
      { ...chunk, data: "a".repeat(256 * 1024 + 1) },
      { ...heartbeat, sentAt: -1 },
      { ...heartbeat, sentAt: Number.MAX_SAFE_INTEGER + 1 },
    ]) {
      expect(() => compiledDecodeUnknownSync2(wire)).toThrow();
      expect(() => compiledDecodeUnknownSync4(wire)).toThrow();
    }
    expect(() => compiledDecodeUnknownSync4({ ...welcome, heartbeatMs: 0 })).toThrow();
    expect(() => compiledDecodeUnknownSync4({ ...welcome, connectionGeneration: -1 })).toThrow();
    expect(() => compiledDecodeSync11({ type: "mount", ...tab, tabId: "a".repeat(129) })).toThrow();
    expect(() =>
      compiledDecodeSync12({ type: "mounted", ...tab, threadId: "a".repeat(129) }),
    ).toThrow();
  });
});

const compiledEncodeSync13 = Schema.encodeSync(PreviewRenderHostSelection);
const compiledDecodeSync14 = Schema.decodeSync(PreviewRenderHostSelection);
const compiledDecodeSync15 = Schema.decodeSync(DesktopCompanionTicketResponse);
const compiledEncodeSync16 = Schema.encodeSync(DesktopCompanionTicketResponse);
const compiledDecodeUnknownSync17 = Schema.decodeUnknownSync(DesktopCompanionTicketResponse);
const compiledEncodeSync18 = Schema.encodeSync(PreviewCompanionHostsResponse);
const compiledDecodeUnknownSync19 = Schema.decodeUnknownSync(PreviewCompanionHostsResponse);
const compiledEncodeSync20 = Schema.encodeSync(PreviewCompanionDefaultSelectionInput);
const compiledDecodeUnknownSync21 = Schema.decodeUnknownSync(PreviewCompanionDefaultSelectionInput);
const compiledEncodeSync22 = Schema.encodeSync(PreviewCompanionThreadSelectionInput);
const compiledDecodeUnknownSync23 = Schema.decodeUnknownSync(PreviewCompanionThreadSelectionInput);
const compiledDecodeSync24 = Schema.decodeSync(PreviewCompanionThreadSelectionInput);
const compiledEncodeSync25 = Schema.encodeSync(PreviewCompanionThreadSelectionResponse);
const compiledDecodeUnknownSync26 = Schema.decodeUnknownSync(
  PreviewCompanionThreadSelectionResponse,
);

describe("Preview companion HTTP payloads", () => {
  it("preserves explicit-tag constructors and requires selection tags on the wire", () => {
    const server = { _tag: "server" } as const;
    const companion = { _tag: "companion", hostId: "mini-1" } as const;
    for (const wire of [server, companion]) {
      expect(PreviewRenderHostSelection.make(wire)).toEqual(wire);
      expect(compiledEncodeSync13(compiledDecodeSync14(wire))).toEqual(wire);
    }
    expect(() => compiledDecodeUnknownSync7({})).toThrow();
    expect(() => compiledDecodeUnknownSync7({ hostId: "mini-1" })).toThrow();
  });

  it("preserves ready ticket tags through construction and wire round trips", () => {
    const wire = {
      requestId: "request-1",
      environmentId: "environment-1",
      result: { _tag: "ready", url: "ws://companion.invalid/ticket" },
    } as const;
    const decoded = compiledDecodeSync15(wire);
    expect(DesktopCompanionTicketResponse.make(decoded)).toEqual(decoded);
    expect(compiledEncodeSync16(decoded)).toEqual(wire);
    expect(() =>
      compiledDecodeUnknownSync17({
        ...wire,
        result: { url: wire.result.url },
      }),
    ).toThrow();
  });

  it("round trips online and offline host status", () => {
    const host = {
      hostId: "mini-1",
      label: "Mini",
      platform: "darwin",
      online: true,
      lastSeenAt: heartbeat.sentAt,
      runtimeIdentity,
      capabilities,
      connectionGeneration: 1,
    };
    const wire = {
      hosts: [
        host,
        {
          ...host,
          hostId: "mini-2",
          online: false,
          runtimeIdentity: null,
          connectionGeneration: null,
        },
      ],
      environmentDefault: selection,
    };
    expect(compiledEncodeSync18(compiledDecodeUnknownSync19(wire))).toEqual(wire);
  });

  it("allows server or companion defaults, and only thread overrides can be cleared", () => {
    for (const value of [{ _tag: "server" }, selection]) {
      const wire = { selection: value };
      expect(compiledEncodeSync20(compiledDecodeUnknownSync21(wire))).toEqual(wire);
      expect(compiledEncodeSync22(compiledDecodeUnknownSync23(wire))).toEqual(wire);
    }
    expect(compiledDecodeSync24({ selection: null })).toEqual({
      selection: null,
    });
    expect(() => compiledDecodeUnknownSync21({ selection: null })).toThrow();
    expect(() => compiledDecodeUnknownSync23({})).toThrow();
    expect(() => compiledDecodeUnknownSync7({ _tag: "companion" })).toThrow();
  });

  it("reports inherited selection separately from existing tab bindings", () => {
    const wire = {
      selection: null,
      effective: selection,
      tabs: [
        { tabId: "server-tab", hostId: null },
        { tabId: "companion-tab", hostId: "mini-2" },
      ],
    };
    expect(compiledEncodeSync25(compiledDecodeUnknownSync26(wire))).toEqual(wire);
  });
});
