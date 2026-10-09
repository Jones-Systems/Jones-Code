// @effect-diagnostics nodeBuiltinImport:off - Names download files on the shared disk.
/**
 * The desktop end of the desktop browser channel (see `DesktopBrowserEvent` in
 * contracts). The primary backend uses file descriptors; a companion uses a
 * filtered uplink. Each attached tab is reachable only through its `CdpRelay`.
 *
 * A tab is attached once its `<webview>` registers with a key the web app
 * gave it. The preview manager owns the tab's single debugger session and hands
 * it here; the relay shares it.
 */
import {
  DesktopBrowserCommand,
  DesktopBrowserEvent,
  type DesktopBrowserEvent as DesktopBrowserEventType,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as DesktopAppIdentity from "../app/DesktopAppIdentity.ts";
import { companionCdpPolicy } from "../jones/previewCompanion/CompanionCdpPolicy.ts";
import {
  createCompanionAssignments,
  guardCompanionGuest,
  isCompanionGuest,
} from "../jones/previewCompanion/CompanionIsolation.ts";

import { createCdpRelayConnection, type CdpRelayConnection } from "./CdpRelay.ts";

const encodeEvent = Schema.encodeSync(Schema.fromJsonString(DesktopBrowserEvent));
const decodeCommand = Schema.decodeUnknownOption(Schema.fromJsonString(DesktopBrowserCommand));
const lineEncoder = new TextEncoder();

export interface DesktopBrowserTabKey {
  readonly threadId: string;
  readonly tabId: string;
}

/** A tab's debugger, as the preview manager lends it to the relay. */
export interface DesktopBrowserTabDebugger {
  readonly webContents: Electron.WebContents;
  readonly debugger: Electron.Debugger;
}

const keyOf = ({ threadId, tabId }: DesktopBrowserTabKey) => `${threadId}\u0000${tabId}`;

interface AttachedTab {
  readonly key: DesktopBrowserTabKey;
  readonly debuggee: DesktopBrowserTabDebugger;
  relay: CdpRelayConnection | null;
  readonly downloadPolicy: "shared-disk" | "deny";
  /** Where the server wants this tab's downloads; null keeps Electron's own handling. */
  downloadDirectory: string | null;
  /** The guid CDP gave the download that is about to start. */
  pendingDownloadGuid: string | null;
  readonly onMessage: (
    event: Electron.Event,
    method: string,
    params: unknown,
    sessionId: string,
  ) => void;
}

export class DesktopBrowserHost extends Context.Service<
  DesktopBrowserHost,
  {
    /**
     * Newline-delimited events for a backend's browser fd. Each run starts by
     * announcing the tabs already attached, so a restarted backend hears them.
     */
    readonly events: Stream.Stream<Uint8Array>;
    readonly eventsMatching: (
      predicate: (key: DesktopBrowserTabKey) => boolean,
    ) => Stream.Stream<Uint8Array>;
    /** One line from the backend's browser control fd. */
    readonly handleCommandLine: (
      line: string,
      predicate?: (key: DesktopBrowserTabKey) => boolean,
    ) => Effect.Effect<void>;
    readonly replaceCompanionAssignments: (keys: ReadonlyArray<DesktopBrowserTabKey>) => void;
    readonly isCompanionAssigned: (key: DesktopBrowserTabKey) => boolean;
    readonly isCompanionOwned: (key: DesktopBrowserTabKey) => boolean;
    readonly registerCompanionGuest: (
      key: DesktopBrowserTabKey,
      contents: Electron.WebContents,
    ) => void;
    readonly isCompanionGuest: (contents: Electron.WebContents | null) => boolean;
    /** Offers a server tab's `<webview>` to the server. */
    readonly attach: (
      key: DesktopBrowserTabKey,
      debuggee: DesktopBrowserTabDebugger,
      downloadPolicy?: "shared-disk" | "deny",
    ) => void;
    /** Withdraws it: closed, swapped, crashed, or devtools needs the debugger. */
    readonly detach: (key: DesktopBrowserTabKey) => void;
    /** Cancels companion downloads or places local server downloads on the shared disk. */
    readonly placeDownload: (source: Electron.WebContents, item: Electron.DownloadItem) => boolean;
    /** The agent's cursor positions for attached tabs, keyed by their server tab. */
    readonly pointers: Stream.Stream<{
      readonly key: DesktopBrowserTabKey;
      readonly phase: "move" | "click";
      readonly x: number;
      readonly y: number;
    }>;
  }
>()("@t3tools/desktop/preview/DesktopBrowserHost") {}

export const make = Effect.gen(function* () {
  const appIdentity = yield* Effect.serviceOption(DesktopAppIdentity.DesktopAppIdentity);
  const runtimeIdentity = Option.isSome(appIdentity)
    ? yield* appIdentity.value.previewAutomationRuntimeIdentity
    : null;
  const identityFields = runtimeIdentity === null ? {} : { runtimeIdentity };
  const outbox = yield* PubSub.unbounded<DesktopBrowserEventType>();
  const pointers = yield* PubSub.sliding<{
    readonly key: DesktopBrowserTabKey;
    readonly phase: "move" | "click";
    readonly x: number;
    readonly y: number;
  }>(16);
  const runFork = Effect.runForkWith(yield* Effect.context<never>());
  const tabs = new Map<string, AttachedTab>();
  const companions = createCompanionAssignments();
  const emit = (event: DesktopBrowserEventType) => runFork(PubSub.publish(outbox, event));

  const relayFor = (tab: AttachedTab) => {
    if (tab.relay) return tab.relay;
    const { webContents, debugger: debuggee } = tab.debuggee;
    const relay: CdpRelayConnection = createCdpRelayConnection(
      {
        send: (method, params, sessionId) =>
          sessionId === undefined
            ? debuggee.sendCommand(method, params)
            : debuggee.sendCommand(method, params, sessionId),
        targetId: () =>
          debuggee
            .sendCommand("Target.getTargetInfo")
            .then((result: { targetInfo: { targetId: string } }) => result.targetInfo.targetId),
        url: () => webContents.getURL(),
        title: () => webContents.getTitle(),
        userAgent: () => webContents.getUserAgent(),
        setDownloadDirectory: (directory) => {
          tab.downloadDirectory = directory;
        },
      },
      // A released relay's late replies belong to a connection that is gone.
      (message) => {
        if (tab.relay === relay && tabs.get(keyOf(tab.key)) === tab) {
          emit({ type: "cdp", ...tab.key, message });
        }
      },
      tab.downloadPolicy === "deny" ? companionCdpPolicy : undefined,
    );
    tab.relay = relay;
    return relay;
  };

  /**
   * Companion guests cannot save to this host. A local server's Playwright
   * shares the disk and expects the CDP guid as its download filename; without
   * a path Electron would instead open its Save dialog over the app.
   */
  const placeDownload = (source: Electron.WebContents, item: Electron.DownloadItem) => {
    if (isCompanionGuest(source)) {
      item.cancel();
      return true;
    }
    const tab = [...tabs.values()].find(
      (candidate) => candidate.debuggee.webContents === source && candidate.downloadDirectory,
    );
    if (!tab?.downloadDirectory || !tab.pendingDownloadGuid) return false;
    item.setSavePath(NodePath.join(tab.downloadDirectory, tab.pendingDownloadGuid));
    tab.pendingDownloadGuid = null;
    return true;
  };

  const detach = (key: DesktopBrowserTabKey) => {
    const id = keyOf(key);
    const tab = tabs.get(id);
    if (!tab) return;
    tabs.delete(id);
    tab.debuggee.debugger.off("message", tab.onMessage);
    emit({ type: "detached", ...key });
  };

  const registerCompanionGuest = (key: DesktopBrowserTabKey, contents: Electron.WebContents) => {
    if (companions.isOwned(key)) guardCompanionGuest(contents);
  };

  const replaceCompanionAssignments = (keys: ReadonlyArray<DesktopBrowserTabKey>) => {
    for (const key of keys) {
      if (tabs.get(keyOf(key))?.downloadPolicy === "shared-disk") {
        throw new Error("Companion assignment collides with an attached local preview tab.");
      }
    }
    for (const key of companions.replace(keys)) detach(key);
  };

  const attach = (
    key: DesktopBrowserTabKey,
    debuggee: DesktopBrowserTabDebugger,
    downloadPolicy: "shared-disk" | "deny" = "shared-disk",
  ) => {
    if (companions.isOwned(key)) {
      guardCompanionGuest(debuggee.webContents);
      if (!companions.isAssigned(key)) return;
      downloadPolicy = "deny";
    }
    if (downloadPolicy === "deny") guardCompanionGuest(debuggee.webContents);
    const id = keyOf(key);
    if (
      tabs.get(id)?.debuggee.webContents === debuggee.webContents &&
      tabs.get(id)?.downloadPolicy === downloadPolicy
    )
      return;
    detach(key);
    const tab: AttachedTab = {
      key,
      debuggee,
      relay: null,
      downloadPolicy,
      downloadDirectory: null,
      pendingDownloadGuid: null,
      onMessage: (_event, method, params, sessionId) => {
        if (method === "Browser.downloadWillBegin") {
          const guid = (params as { guid?: unknown } | undefined)?.guid;
          tab.pendingDownloadGuid = typeof guid === "string" ? guid : null;
        }
        tab.relay?.event(method, params, sessionId);
      },
    };
    tabs.set(id, tab);
    debuggee.debugger.on("message", tab.onMessage);
    emit({ type: "attached", ...key, ...identityFields });
  };

  const handleCommandLine = (line: string, predicate?: (key: DesktopBrowserTabKey) => boolean) =>
    Effect.sync(() => {
      const command = decodeCommand(line);
      if (Option.isNone(command)) return;
      if (predicate && !predicate(command.value)) return;
      const tab = tabs.get(keyOf(command.value));
      if (!tab) return;
      if (command.value.type === "pointer") {
        const { threadId, tabId, phase, x, y } = command.value;
        runFork(PubSub.publish(pointers, { key: { threadId, tabId }, phase, x, y }));
        return;
      }
      if (command.value.type === "release") {
        // A new server connection starts with a fresh relay and fresh sessions.
        tab.relay = null;
        return;
      }
      relayFor(tab).receive(command.value.message);
    });

  // Read when a backend starts, not when the host is built.
  const announceMatching = (predicate: (key: DesktopBrowserTabKey) => boolean) =>
    Effect.suspend(() =>
      Effect.forEach(
        [...tabs.values()].filter((tab) => predicate(tab.key)),
        (tab) => {
          tab.relay = null;
          return PubSub.publish(outbox, { type: "attached", ...tab.key, ...identityFields });
        },
        { discard: true },
      ),
    );

  const eventsMatching = (predicate: (key: DesktopBrowserTabKey) => boolean) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const subscription = yield* PubSub.subscribe(outbox);
        yield* announceMatching(predicate);
        return Stream.fromSubscription(subscription);
      }),
    ).pipe(
      Stream.filter(predicate),
      Stream.map((event) => lineEncoder.encode(`${encodeEvent(event)}\n`)),
    );

  return DesktopBrowserHost.of({
    pointers: Stream.fromPubSub(pointers),
    // Subscribes before announcing, so no attach falls between the two.
    events: eventsMatching(() => true),
    eventsMatching,
    handleCommandLine,
    replaceCompanionAssignments,
    isCompanionAssigned: companions.isAssigned,
    isCompanionOwned: companions.isOwned,
    registerCompanionGuest,
    isCompanionGuest,
    attach,
    detach,
    placeDownload,
  });
});

export const layer = Layer.effect(DesktopBrowserHost, make);
