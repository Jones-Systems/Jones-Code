import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  DesktopCompanionConfig,
  type DesktopBrowserCommand,
  type DesktopBrowserEvent,
  type DesktopCompanionState,
  type DesktopCompanionTicketRequest,
} from "@t3tools/contracts";
import { encodeFrames } from "@t3tools/shared/jones/previewCompanionFraming";
import {
  createCompanionUplink,
  isCompanionTicketUrl,
  type CompanionSocket,
} from "./UplinkController.ts";

const config = Schema.decodeUnknownSync(DesktopCompanionConfig)({
  enabled: true,
  environmentId: "env-1",
  hostId: "mini-1",
  label: "Mini",
  browserOnly: true,
});
const url = "wss://example.invalid/api/jones/preview-companion/ws?wsTicket=synthetic";
const tab = { threadId: "thread-1", tabId: "tab-1" };
const welcome = {
  type: "welcome",
  protocol: 1,
  environmentId: "env-1",
  connectionGeneration: 3,
  heartbeatMs: 15000,
};

class FakeSocket implements CompanionSocket {
  bufferedAmount = 0;
  sent: string[] = [];
  closed: number[] = [];
  opens = new Set<() => void>();
  messages = new Set<(frame: unknown) => void>();
  closes = new Set<(code: number) => void>();
  errors = new Set<(status?: number) => void>();
  send(frame: string) {
    this.sent.push(frame);
  }
  close(code: number) {
    this.closed.push(code);
  }
  onOpen(listener: () => void) {
    this.opens.add(listener);
    return () => this.opens.delete(listener);
  }
  onMessage(listener: (frame: unknown) => void) {
    this.messages.add(listener);
    return () => this.messages.delete(listener);
  }
  onClose(listener: (code: number) => void) {
    this.closes.add(listener);
    return () => this.closes.delete(listener);
  }
  onError(listener: (status?: number) => void) {
    this.errors.add(listener);
    return () => this.errors.delete(listener);
  }
  open() {
    for (const listener of [...this.opens]) listener();
  }
  receive(message: unknown) {
    for (const listener of [...this.messages]) listener(JSON.stringify(message));
  }
  disconnect(code = 1006) {
    for (const listener of [...this.closes]) listener(code);
  }
}

function fixture(initial = config, browserOnlyLocked = false) {
  let now = 100000;
  let id = 0;
  const timers = new Map<number, { at: number; task: () => void }>();
  const requests: DesktopCompanionTicketRequest[] = [];
  const states: DesktopCompanionState[] = [];
  const sockets: FakeSocket[] = [];
  const assignments: Array<ReadonlyArray<{ threadId: string; tabId: string }>> = [];
  const commands: Array<{ command: DesktopBrowserCommand; current: () => boolean }> = [];
  const browserListeners: Array<(event: DesktopBrowserEvent) => void> = [];
  let power = 0;
  let unknown = 0;
  let collision = false;
  const controller = createCompanionUplink(initial, {
    browserOnlyLocked,
    runtimeIdentity: {
      schemaVersion: 1,
      runtimeKind: "electron",
      runtimeInstanceId: "runtime-1",
      appVersion: "1.0.0",
      buildCommit: null,
    },
    platform: "darwin",
    now: () => now,
    random: () => 1,
    schedule: (delay, task) => {
      const key = ++id;
      timers.set(key, { at: now + delay, task });
      return () => {
        timers.delete(key);
      };
    },
    connect: () => {
      const socket = new FakeSocket();
      sockets.push(socket);
      return socket;
    },
    requestTicket: (request) => requests.push(request),
    state: (state) => states.push(state),
    replaceAssignments: (keys) => {
      if (collision && keys.length) throw new Error("collision");
      assignments.push(keys);
    },
    command: (command, current) => {
      commands.push({ command, current });
    },
    subscribeBrowser: (listener) => {
      browserListeners.push(listener);
      return () => {};
    },
    preventSuspension: () => {
      power++;
      return () => {
        power--;
      };
    },
    unknownMessage: () => {
      unknown++;
    },
  });
  const tick = (ms: number) => {
    const until = now + ms;
    while (true) {
      const next = [...timers].sort((a, b) => a[1].at - b[1].at)[0];
      if (!next || next[1].at > until) break;
      timers.delete(next[0]);
      now = next[1].at;
      next[1].task();
    }
    now = until;
  };
  const connect = () => {
    controller.setReady(true);
    const request = requests.at(-1)!;
    controller.completeTicket({ ...request, result: { _tag: "ready", url } });
    const socket = sockets.at(-1)!;
    socket.open();
    socket.receive(welcome);
    socket.receive({ type: "assignments", tabs: [tab] });
    return socket;
  };
  return {
    controller,
    requests,
    states,
    sockets,
    assignments,
    commands,
    browserListeners,
    tick,
    connect,
    power: () => power,
    unknown: () => unknown,
    timers: () => timers.size,
    collide: () => {
      collision = true;
    },
  };
}

it("preserves the fixed product role through transport changes and omits it for ordinary desktop", () => {
  const f = fixture(config, true);
  expect(f.controller.getState().browserOnlyLocked).toBe(true);
  f.controller.configure({ ...config, enabled: false, environmentId: null });
  expect(f.controller.getState()).toMatchObject({ browserOnlyLocked: true, status: "disabled" });
  f.controller.configure(config);
  f.connect();
  expect(f.controller.getState().browserOnlyLocked).toBe(true);
  expect(fixture().controller.getState().browserOnlyLocked).toBeUndefined();
});

describe("outbound companion lifecycle", () => {
  it("waits for renderer readiness, binds a fresh ticket, and sends its process identity", () => {
    const f = fixture();
    expect(f.requests).toHaveLength(0);
    const socket = f.connect();
    const hello = JSON.parse(socket.sent[0]!);
    expect(hello).toMatchObject({
      type: "hello",
      hostId: "mini-1",
      protocol: 1,
      runtimeIdentity: { runtimeInstanceId: "runtime-1" },
    });
    expect(f.controller.getState()).toMatchObject({
      status: "online",
      connectionGeneration: 3,
      assignments: [tab],
    });
    expect(f.power()).toBe(1);
    f.controller.dispose();
    expect(f.power()).toBe(0);
    expect(f.timers()).toBe(0);
  });

  it("ignores wrong environment and obsolete ticket replies", () => {
    const f = fixture();
    f.controller.setReady(true);
    const old = f.requests[0]!;
    f.controller.retry();
    f.controller.completeTicket({ ...old, result: { _tag: "ready", url } });
    f.controller.completeTicket({
      ...f.requests.at(-1)!,
      environmentId:
        config.environmentId === null
          ? old.environmentId
          : Schema.decodeUnknownSync(DesktopCompanionConfig)({ ...config, environmentId: "other" })
              .environmentId!,
      result: { _tag: "ready", url },
    });
    expect(f.sockets).toHaveLength(0);
    f.controller.dispose();
  });

  it.each(["auth_required", "unsupported", "unavailable"] as const)(
    "keeps ticket failure %s visible",
    (tag) => {
      const f = fixture();
      f.controller.setReady(true);
      f.controller.completeTicket({ ...f.requests[0]!, result: { _tag: tag } });
      expect(f.controller.getState().status).toBe(tag);
      expect(f.sockets).toHaveLength(0);
      f.tick(tag === "unsupported" ? 299999 : 29999);
      expect(f.requests).toHaveLength(1);
      f.controller.dispose();
    },
  );

  it.each([4401, 4409])("holds native auth or takeover close %i until explicit retry", (code) => {
    const f = fixture();
    f.connect().disconnect(code);
    f.tick(600000);
    expect(f.requests).toHaveLength(1);
    expect(f.controller.getState().status).toBe(code === 4401 ? "auth_required" : "superseded");
    f.controller.retry();
    expect(f.requests).toHaveLength(2);
    f.controller.dispose();
  });

  it.each([404, 4426])("backs unsupported transport %i off for five minutes", (code) => {
    const f = fixture();
    const socket = f.connect();
    if (code === 404) for (const listener of socket.errors) listener(code);
    else socket.disconnect(code);
    f.tick(299999);
    expect(f.requests).toHaveLength(1);
    f.tick(1);
    expect(f.requests).toHaveLength(2);
    f.controller.dispose();
  });

  it("reconnects with one timer and a new ticket, without replaying old commands or browser events", () => {
    const f = fixture();
    const socket = f.connect();
    socket.receive({
      type: "browser",
      command: { type: "cdp", ...tab, message: '{"id":1,"method":"Page.enable"}' },
    });
    expect(f.commands[0]?.current()).toBe(true);
    const staleMessage = [...socket.messages][0]!;
    const staleBrowser = f.browserListeners[0]!;
    socket.disconnect();
    expect(f.controller.getState().assignments).toEqual([]);
    expect(f.commands[0]?.current()).toBe(false);
    expect(f.power()).toBe(0);
    f.tick(999);
    expect(f.requests).toHaveLength(1);
    f.tick(1);
    expect(f.requests).toHaveLength(2);
    const next = f.connect();
    staleMessage(JSON.stringify({ type: "mount", threadId: "stale", tabId: "stale" }));
    staleBrowser({ type: "cdp", ...tab, message: '{"id":1,"result":{}}' });
    expect(f.controller.getState().assignments).toEqual([tab]);
    expect(f.commands).toHaveLength(1);
    expect(next.sent.map((message) => JSON.parse(message).type)).toEqual(["hello"]);
    f.controller.dispose();
  });

  it("sends heartbeats and clears ownership and power after 45 seconds of silence", () => {
    const f = fixture();
    const socket = f.connect();
    f.tick(15000);
    expect(JSON.parse(socket.sent.at(-1)!)).toEqual({ type: "heartbeat", sentAt: 115000 });
    f.tick(30000);
    expect(socket.closed).toContain(4408);
    expect(f.controller.getState().assignments).toEqual([]);
    expect(f.power()).toBe(0);
    f.controller.dispose();
  });

  it("resets liveness only on valid messages and never closes over unknown types", () => {
    const f = fixture();
    const socket = f.connect();
    socket.receive({ type: "new-message" });
    socket.receive({ type: "another-message" });
    expect(f.unknown()).toBe(1);
    expect(socket.closed).toEqual([]);
    f.tick(30000);
    socket.receive({ type: "heartbeat", sentAt: 130000 });
    f.tick(30000);
    expect(f.controller.getState().status).toBe("online");
    f.controller.dispose();
  });

  it("applies assignments before renderer notification and never spreads wire type into tab keys", () => {
    const f = fixture();
    const socket = f.connect();
    socket.receive({ type: "mount", threadId: "thread-2", tabId: "tab-2" });
    expect(f.assignments.at(-1)).toEqual([tab, { threadId: "thread-2", tabId: "tab-2" }]);
    socket.receive({ type: "unmount", ...tab });
    expect(f.controller.getState().assignments).toEqual([{ threadId: "thread-2", tabId: "tab-2" }]);
    socket.receive({ type: "browser", command: { type: "release", ...tab } });
    expect(f.commands).toHaveLength(0);
    f.controller.dispose();
  });

  it("fails closed on an ordinary-tab assignment collision", () => {
    const f = fixture();
    const socket = f.connect();
    f.collide();
    socket.receive({ type: "mount", threadId: "local", tabId: "local" });
    expect(f.controller.getState()).toMatchObject({ status: "unavailable", assignments: [] });
    expect(f.power()).toBe(0);
    f.controller.dispose();
  });

  it("rejects commands before welcome and a welcome for another environment", () => {
    const f = fixture();
    f.controller.setReady(true);
    f.controller.completeTicket({ ...f.requests[0]!, result: { _tag: "ready", url } });
    f.sockets[0]!.receive({ ...welcome, environmentId: "other" });
    expect(f.controller.getState().status).toBe("unavailable");
    expect(f.commands).toHaveLength(0);
    f.controller.dispose();
  });

  it("chunks large UTF-8 browser messages and accepts chunked server commands", () => {
    const f = fixture();
    const socket = f.connect();
    f.browserListeners[0]!({ type: "cdp", ...tab, message: "🦊".repeat(80000) });
    expect(socket.sent.slice(1).every((frame) => JSON.parse(frame).type === "chunk")).toBe(true);
    const frames = encodeFrames(
      { type: "browser", command: { type: "cdp", ...tab, message: "x".repeat(300000) } },
      "server-1",
    );
    for (const frame of frames) for (const listener of socket.messages) listener(frame);
    expect(f.commands).toHaveLength(1);
    f.controller.dispose();
  });

  it("closes over bounded socket backpressure", () => {
    const f = fixture();
    const socket = f.connect();
    socket.bufferedAmount = 32 * 1024 * 1024;
    f.browserListeners[0]!({ type: "attached", ...tab });
    expect(socket.closed).toContain(4413);
    expect(f.controller.getState().status).toBe("unavailable");
    f.controller.dispose();
  });

  it("clears active assignments on renderer loss and blocks browser-only activation until relaunch", () => {
    const f = fixture();
    f.connect();
    f.controller.setReady(false);
    expect(f.controller.getState().assignments).toEqual([]);
    expect(f.power()).toBe(0);
    f.controller.configure(config, true);
    f.controller.setReady(true);
    f.controller.retry();
    expect(f.controller.getState().status).toBe("restart_required");
    expect(f.requests).toHaveLength(1);
    f.controller.dispose();
  });

  it("does not acquire a socket or ticket when disabled", () => {
    const f = fixture({ ...config, enabled: false });
    f.controller.setReady(true);
    f.tick(600000);
    expect(f.requests).toHaveLength(0);
    expect(f.sockets).toHaveLength(0);
    f.controller.dispose();
  });
});

it.each([
  "https://example.invalid/api/jones/preview-companion/ws?wsTicket=ticket",
  "wss://user:secret@example.invalid/api/jones/preview-companion/ws?wsTicket=ticket",
  "wss://example.invalid/ws?wsTicket=ticket",
  "wss://example.invalid/api/jones/preview-companion/ws",
  "wss://example.invalid/api/jones/preview-companion/ws?wsTicket=a&wsTicket=b",
  "wss://example.invalid/api/jones/preview-companion/ws?wsTicket=a&token=secret",
])("rejects non-ticket or generic outbound URL %s", (value) => {
  expect(isCompanionTicketUrl(value)).toBe(false);
});
