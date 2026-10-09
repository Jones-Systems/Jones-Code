import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { DesktopBrowserCommand, DesktopBrowserEvent } from "../desktopBrowser.ts";
import {
  CompanionDown,
  CompanionUp,
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
  { type: "cdp", ...tab, message: '{"id":1,"method":"Page.enable"}' },
  { type: "release", ...tab },
  { type: "pointer", ...tab, phase: "move", x: 1.5, y: -2 },
  { type: "pointer", ...tab, phase: "click", x: 0, y: 0 },
];

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
    expect(Schema.encodeSync(CompanionUp)(Schema.decodeUnknownSync(CompanionUp)(wire))).toEqual(
      wire,
    );
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
    expect(Schema.encodeSync(CompanionDown)(Schema.decodeUnknownSync(CompanionDown)(wire))).toEqual(
      wire,
    );
  });

  it("preserves the existing desktop event and command payloads", () => {
    for (const event of events) {
      expect(Schema.decodeUnknownSync(CompanionUp)({ type: "browser", event })).toEqual({
        type: "browser",
        event: Schema.decodeUnknownSync(DesktopBrowserEvent)(event),
      });
    }
    for (const command of commands) {
      expect(Schema.decodeUnknownSync(CompanionDown)({ type: "browser", command })).toEqual({
        type: "browser",
        command: Schema.decodeUnknownSync(DesktopBrowserCommand)(command),
      });
    }
  });

  it("trims host identities and labels and accepts their exact length limit", () => {
    const decodeId = Schema.decodeUnknownSync(PreviewCompanionHostId);
    expect(decodeId(" Mini_1-a ")).toBe("Mini_1-a");
    expect(decodeId("a".repeat(64))).toBe("a".repeat(64));
    expect(
      Schema.decodeUnknownSync(CompanionUp)({ ...hello, label: ` ${"a".repeat(64)} ` }),
    ).toEqual({
      ...hello,
      label: "a".repeat(64),
    });
  });

  it.each(["", " ", "a".repeat(65), "mini.local", "mini/1", "mini 1", "méni"])(
    "rejects invalid host ID %j",
    (hostId) => {
      expect(() => Schema.decodeUnknownSync(CompanionUp)({ ...hello, hostId })).toThrow();
      expect(() =>
        Schema.decodeUnknownSync(PreviewRenderHostSelection)({ ...selection, hostId }),
      ).toThrow();
    },
  );

  it.each(["", " ", "a".repeat(65)])("rejects invalid host label %j", (label) => {
    expect(() => Schema.decodeUnknownSync(CompanionUp)({ ...hello, label })).toThrow();
  });

  it("keeps V1 unsupported capabilities disabled and clipboard support explicit", () => {
    expect(
      Schema.decodeUnknownSync(PreviewCompanionCapabilities)({
        ...capabilities,
        clipboardText: false,
      }),
    ).toEqual({
      ...capabilities,
      clipboardText: false,
    });
    for (const key of ["uploads", "downloads", "recording"]) {
      expect(() =>
        Schema.decodeUnknownSync(PreviewCompanionCapabilities)({ ...capabilities, [key]: true }),
      ).toThrow();
    }
    expect(() =>
      Schema.decodeUnknownSync(PreviewCompanionCapabilities)({ ...capabilities, cdp: false }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(PreviewCompanionCapabilities)({
        ...capabilities,
        clipboardText: undefined,
      }),
    ).toThrow();
  });

  it("rejects unsupported protocols and malformed known messages", () => {
    expect(() => Schema.decodeUnknownSync(CompanionUp)({ ...hello, protocol: 2 })).toThrow();
    expect(() => Schema.decodeUnknownSync(CompanionDown)({ ...welcome, protocol: 2 })).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(CompanionUp)({ ...hello, runtimeIdentity: null }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(CompanionUp)({ type: "browser", event: { type: "attached" } }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(CompanionDown)({ type: "browser", command: { type: "release" } }),
    ).toThrow();
  });

  it.each(["future-message", "thread.create", "orchestration.dispatch"])(
    "lets transport ignore unknown %s envelopes through Option decoding",
    (type) => {
      expect(Option.isNone(Schema.decodeUnknownOption(CompanionUp)({ type }))).toBe(true);
      expect(Option.isNone(Schema.decodeUnknownOption(CompanionDown)({ type }))).toBe(true);
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
      expect(() => Schema.decodeUnknownSync(CompanionUp)(wire)).toThrow();
      expect(() => Schema.decodeUnknownSync(CompanionDown)(wire)).toThrow();
    }
    expect(() => Schema.decodeUnknownSync(CompanionDown)({ ...welcome, heartbeatMs: 0 })).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(CompanionDown)({ ...welcome, connectionGeneration: -1 }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(CompanionDown)({ type: "mount", ...tab, tabId: "a".repeat(129) }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(CompanionUp)({ type: "mounted", ...tab, threadId: "a".repeat(129) }),
    ).toThrow();
  });
});

describe("Preview companion HTTP payloads", () => {
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
    expect(
      Schema.encodeSync(PreviewCompanionHostsResponse)(
        Schema.decodeUnknownSync(PreviewCompanionHostsResponse)(wire),
      ),
    ).toEqual(wire);
  });

  it("allows server or companion defaults, and only thread overrides can be cleared", () => {
    for (const value of [{ _tag: "server" }, selection]) {
      const wire = { selection: value };
      expect(
        Schema.encodeSync(PreviewCompanionDefaultSelectionInput)(
          Schema.decodeUnknownSync(PreviewCompanionDefaultSelectionInput)(wire),
        ),
      ).toEqual(wire);
      expect(
        Schema.encodeSync(PreviewCompanionThreadSelectionInput)(
          Schema.decodeUnknownSync(PreviewCompanionThreadSelectionInput)(wire),
        ),
      ).toEqual(wire);
    }
    expect(
      Schema.decodeUnknownSync(PreviewCompanionThreadSelectionInput)({ selection: null }),
    ).toEqual({ selection: null });
    expect(() =>
      Schema.decodeUnknownSync(PreviewCompanionDefaultSelectionInput)({ selection: null }),
    ).toThrow();
    expect(() => Schema.decodeUnknownSync(PreviewCompanionThreadSelectionInput)({})).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(PreviewRenderHostSelection)({ _tag: "companion" }),
    ).toThrow();
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
    expect(
      Schema.encodeSync(PreviewCompanionThreadSelectionResponse)(
        Schema.decodeUnknownSync(PreviewCompanionThreadSelectionResponse)(wire),
      ),
    ).toEqual(wire);
  });
});
