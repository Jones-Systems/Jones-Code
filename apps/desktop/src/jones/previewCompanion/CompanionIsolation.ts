import type { DesktopBrowserHost, DesktopBrowserTabKey } from "../../preview/DesktopBrowserHost.ts";
import { isCompanionNavigationAllowed } from "./CompanionCdpPolicy.ts";

const companionGuests = new WeakSet<Electron.WebContents>();

export const isCompanionGuest = (contents: Electron.WebContents | null): boolean =>
  contents !== null && companionGuests.has(contents);

export function guardCompanionGuest(contents: Electron.WebContents): void {
  if (companionGuests.has(contents)) return;
  companionGuests.add(contents);
  const guard = (event: Electron.Event & { readonly url: string }) => {
    if (!isCompanionNavigationAllowed(event.url)) event.preventDefault();
  };
  contents.on("will-navigate", guard);
  contents.on("will-frame-navigate", guard);
  contents.on("will-redirect", guard);
  // Guards live with the guest, even after its assignment or relay is released.
  contents.once("destroyed", () => {
    contents.off("will-navigate", guard);
    contents.off("will-frame-navigate", guard);
    contents.off("will-redirect", guard);
    companionGuests.delete(contents);
  });
}

const keyOf = ({ threadId, tabId }: DesktopBrowserTabKey) => JSON.stringify([threadId, tabId]);

export function createCompanionAssignments() {
  let assigned = new Map<string, DesktopBrowserTabKey>();
  const owned = new Set<string>();
  return {
    isAssigned: (key: DesktopBrowserTabKey) => assigned.has(keyOf(key)),
    // A disconnected or unmounted companion guest must never become local.
    isOwned: (key: DesktopBrowserTabKey) => owned.has(keyOf(key)),
    replace: (keys: ReadonlyArray<DesktopBrowserTabKey>): ReadonlyArray<DesktopBrowserTabKey> => {
      const next = new Map(keys.map((key) => [keyOf(key), { ...key }]));
      const removed = [...assigned].filter(([id]) => !next.has(id)).map(([, key]) => key);
      for (const id of next.keys()) owned.add(id);
      assigned = next;
      return removed;
    },
  };
}

export function localBrowserChannel(host: DesktopBrowserHost["Service"]) {
  const isLocal = (key: DesktopBrowserTabKey) => !host.isCompanionOwned(key);
  return {
    desktopBrowserStream: host.eventsMatching(isLocal),
    onDesktopBrowserCommand: (line: string) => host.handleCommandLine(line, isLocal),
  };
}
