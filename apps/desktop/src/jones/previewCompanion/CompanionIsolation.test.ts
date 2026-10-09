// @effect-diagnostics nodeBuiltinImport:off - Synthetic Electron guests and debuggers.
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import * as NodeEvents from "node:events";
import { vi } from "vite-plus/test";
import * as DesktopBrowserHost from "../../preview/DesktopBrowserHost.ts";
import {
  createCompanionAssignments,
  guardCompanionGuest,
  isCompanionGuest,
  localBrowserChannel,
} from "./CompanionIsolation.ts";

const remote = { threadId: "remote-thread", tabId: "remote-tab" };
const local = { threadId: "local-thread", tabId: "local-tab" };
const debuggee = () => {
  const contents = Object.assign(new NodeEvents.EventEmitter(), {
    getURL: () => "https://example.test",
    getTitle: () => "Fixture",
    getUserAgent: () => "Electron",
  });
  const sendCommand = vi.fn(async (_method: string, _params?: object) => ({}));
  const debuggerEvents = Object.assign(new NodeEvents.EventEmitter(), { sendCommand });
  return {
    contents,
    sendCommand,
    tab: {
      webContents: contents as unknown as Electron.WebContents,
      debugger: debuggerEvents as unknown as Electron.Debugger,
    },
  };
};
const readEvents = (stream: Stream.Stream<Uint8Array>, count: number) =>
  stream.pipe(
    Stream.take(count),
    Stream.runCollect,
    Effect.map((lines) =>
      lines.map((line) => JSON.parse(new TextDecoder().decode(line)) as Record<string, unknown>),
    ),
  );
const line = (
  key: DesktopBrowserHost.DesktopBrowserTabKey,
  id: number,
  method = "Runtime.evaluate",
) =>
  JSON.stringify({
    type: "cdp",
    ...key,
    message: JSON.stringify({ id, method, sessionId: "t3-preview-page" }),
  });

describe("companion isolation", () => {
  it("distinguishes active assignment from lifetime ownership, with unambiguous keys", () => {
    const assignments = createCompanionAssignments();
    assignments.replace([remote, { threadId: "a\u0000b", tabId: "c" }]);
    expect(assignments.isAssigned(remote)).toBe(true);
    expect(assignments.isOwned(local)).toBe(false);
    expect(assignments.isOwned({ threadId: "a", tabId: "b\u0000c" })).toBe(false);
    expect(assignments.replace([])).toContainEqual(remote);
    expect(assignments.isAssigned(remote)).toBe(false);
    expect(assignments.isOwned(remote)).toBe(true);
  });

  it("guards top-level, frame and redirect navigation for the guest lifetime", () => {
    const guest = debuggee();
    guardCompanionGuest(guest.tab.webContents);
    guardCompanionGuest(guest.tab.webContents);
    for (const event of ["will-navigate", "will-frame-navigate", "will-redirect"]) {
      expect(guest.contents.listenerCount(event)).toBe(1);
      for (const url of [
        "file:///local/secret",
        "data:text/plain,no",
        "javascript:alert(1)",
        "chrome://settings",
      ]) {
        const preventDefault = vi.fn();
        guest.contents.emit(event, { url, preventDefault });
        expect(preventDefault).toHaveBeenCalledOnce();
      }
      for (const url of ["https://example.test", "http://localhost:3000", "about:blank"]) {
        const preventDefault = vi.fn();
        guest.contents.emit(event, { url, preventDefault });
        expect(preventDefault).not.toHaveBeenCalled();
      }
    }
    expect(isCompanionGuest(guest.tab.webContents)).toBe(true);
    expect(isCompanionGuest(debuggee().tab.webContents)).toBe(false);
    guest.contents.emit("destroyed");
    expect(isCompanionGuest(guest.tab.webContents)).toBe(false);
    expect(guest.contents.listenerCount("will-frame-navigate")).toBe(0);
  });

  it.effect("filters announcements and commands bidirectionally, including local release", () =>
    Effect.gen(function* () {
      const host = yield* DesktopBrowserHost.make;
      const companion = debuggee();
      const ordinary = debuggee();
      host.replaceCompanionAssignments([remote]);
      host.attach(remote, companion.tab);
      host.attach(local, ordinary.tab);
      const localChannel = localBrowserChannel(host);
      expect(yield* readEvents(host.eventsMatching(host.isCompanionAssigned), 1)).toEqual([
        { type: "attached", ...remote },
      ]);
      expect(yield* readEvents(localChannel.desktopBrowserStream, 1)).toEqual([
        { type: "attached", ...local },
      ]);
      yield* localChannel.onDesktopBrowserCommand(line(remote, 1));
      yield* host.handleCommandLine(line(local, 2), host.isCompanionAssigned);
      expect(companion.sendCommand).not.toHaveBeenCalled();
      expect(ordinary.sendCommand).not.toHaveBeenCalled();
      let finishCommand = () => {};
      companion.sendCommand.mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            finishCommand = () => resolve({});
          }),
      );
      const replies = yield* readEvents(host.eventsMatching(host.isCompanionAssigned), 2).pipe(
        Effect.forkScoped,
      );
      yield* Effect.yieldNow;
      yield* host.handleCommandLine(line(remote, 3), host.isCompanionAssigned);
      yield* localChannel.onDesktopBrowserCommand(JSON.stringify({ type: "release", ...remote }));
      // Starting the local FD sink must not reset an in-flight companion relay.
      yield* readEvents(localChannel.desktopBrowserStream, 1);
      finishCommand();
      const [, reply] = yield* Fiber.join(replies);
      expect(reply).toMatchObject({ type: "cdp", ...remote });
      expect(JSON.parse(reply!.message as string)).toMatchObject({ id: 3, result: {} });
      yield* localChannel.onDesktopBrowserCommand(line(local, 4));
      expect(ordinary.sendCommand).toHaveBeenCalledOnce();
    }).pipe(Effect.scoped),
  );

  it.effect("keeps streaming filters active for newly attached tabs", () =>
    Effect.gen(function* () {
      const host = yield* DesktopBrowserHost.make;
      host.replaceCompanionAssignments([remote]);
      const reader = yield* readEvents(host.eventsMatching(host.isCompanionAssigned), 1).pipe(
        Effect.forkScoped,
      );
      yield* Effect.yieldNow;
      host.attach(local, debuggee().tab);
      host.attach(remote, debuggee().tab);
      expect(yield* Fiber.join(reader)).toEqual([{ type: "attached", ...remote }]);
    }).pipe(Effect.scoped),
  );

  it.effect(
    "unassignment detaches, rejects stale reattach and never transfers ownership locally",
    () =>
      Effect.gen(function* () {
        const host = yield* DesktopBrowserHost.make;
        const companion = debuggee();
        host.replaceCompanionAssignments([remote]);
        host.attach(remote, companion.tab);
        host.replaceCompanionAssignments([]);
        host.attach(remote, companion.tab);
        yield* host.handleCommandLine(line(remote, 1));
        expect(companion.sendCommand).not.toHaveBeenCalled();
        expect(host.isCompanionOwned(remote)).toBe(true);
        expect(host.isCompanionAssigned(remote)).toBe(false);
        expect(host.isCompanionGuest(companion.tab.webContents)).toBe(true);
        host.replaceCompanionAssignments([remote]);
        host.attach(remote, companion.tab);
        yield* host.handleCommandLine(line(remote, 2), host.isCompanionAssigned);
        expect(companion.sendCommand).toHaveBeenCalledOnce();
      }),
  );

  it.effect("rejects local-tab assignment collisions atomically", () =>
    Effect.gen(function* () {
      const host = yield* DesktopBrowserHost.make;
      host.attach(local, debuggee().tab);
      expect(() => host.replaceCompanionAssignments([remote, local])).toThrow("collides");
      expect(host.isCompanionAssigned(remote)).toBe(false);
      expect(host.isCompanionOwned(local)).toBe(false);
    }),
  );

  it.effect("cancels companion downloads before relay attach and after assignment removal", () =>
    Effect.gen(function* () {
      const host = yield* DesktopBrowserHost.make;
      const companion = debuggee();
      const ordinary = debuggee();
      const cancel = vi.fn();
      const setSavePath = vi.fn();
      const item = { cancel, setSavePath } as unknown as Electron.DownloadItem;
      host.replaceCompanionAssignments([remote]);
      host.registerCompanionGuest(remote, companion.tab.webContents);
      expect(host.placeDownload(companion.tab.webContents, item)).toBe(true);
      host.attach(remote, companion.tab);
      yield* host.handleCommandLine(
        JSON.stringify({
          type: "cdp",
          ...remote,
          message: JSON.stringify({
            id: 1,
            method: "Browser.setDownloadBehavior",
            params: { behavior: "allow", downloadPath: "/local/downloads" },
          }),
        }),
        host.isCompanionAssigned,
      );
      expect(companion.sendCommand).toHaveBeenCalledWith("Browser.setDownloadBehavior", {
        behavior: "deny",
      });
      host.replaceCompanionAssignments([]);
      expect(host.placeDownload(companion.tab.webContents, item)).toBe(true);
      expect(host.placeDownload(ordinary.tab.webContents, item)).toBe(false);
      expect(cancel).toHaveBeenCalledTimes(2);
      expect(setSavePath).not.toHaveBeenCalled();
    }),
  );
});
