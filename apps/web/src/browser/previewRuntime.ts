import { ThreadId } from "@t3tools/contracts";
import { Atom, AsyncResult } from "effect/reactivity";
import {
  bindingsQuery,
  companionStateAtom,
  readBindingResult,
} from "../jones/previewCompanion/state";
import { nativeCompanionRendering } from "../jones/previewCompanion/inventory";
import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentId, PreviewRuntime, PreviewSessionSnapshot } from "@t3tools/contracts";

import { isElectron } from "~/env";
import { isPreviewSupportedInRuntime } from "~/previewStateStore";
import { appAtomRegistry } from "~/rpc/atomRegistry";
import { primaryEnvironmentIdAtom } from "~/state/primaryEnvironment";
import {
  readEnvironmentSupportsServerBrowser,
  useEnvironmentSupportsServerBrowser,
} from "~/state/entities";

/**
 * Where a tab the user opens should run. The desktop app draws its own
 * server's tabs natively, so those stay agent-drivable at no cost. A remote
 * environment's tabs would stream, so the desktop opens them in its own
 * browser instead: no latency, and it reaches what this computer reaches. The
 * user can move a tab to the environment when only it can reach the page.
 */
export function previewRuntimeFor(environmentId: EnvironmentId): PreviewRuntime | undefined {
  if (!readEnvironmentSupportsServerBrowser(environmentId)) return undefined;
  if (
    isPreviewSupportedInRuntime() &&
    environmentId !== appAtomRegistry.get(primaryEnvironmentIdAtom)
  ) {
    return undefined;
  }
  return "server";
}

/**
 * The other browser a tab can move to, or null when it has none: a desktop
 * tab of a remote environment can move to that environment's browser, and back.
 */
export function alternatePreviewRuntime(
  environmentId: EnvironmentId,
  primaryEnvironmentId: EnvironmentId | null,
  serverBrowser: boolean,
  snapshot: Pick<PreviewSessionSnapshot, "runtime"> | null | undefined,
): PreviewRuntime | null {
  if (!snapshot || !serverBrowser || !isPreviewSupportedInRuntime()) return null;
  if (environmentId === primaryEnvironmentId) return null;
  return snapshot.runtime === "server" ? "desktop" : "server";
}

/** Electron hosts its own browser tabs; other clients need the environment to host them. */
export function isPreviewAvailableFor(environmentId: EnvironmentId): boolean {
  return isPreviewSupportedInRuntime() || readEnvironmentSupportsServerBrowser(environmentId);
}

export function usePreviewAvailable(environmentId: EnvironmentId | null): boolean {
  const serverBrowser = useEnvironmentSupportsServerBrowser(environmentId);
  return isPreviewSupportedInRuntime() || serverBrowser;
}

/**
 * Whether this client draws a server tab with its own `<webview>`. The desktop
 * app obeys immutable host bindings first; the local-server rule remains the
 * fallback only for a server binding or an older server without this endpoint.
 */
export function rendersServerTabNatively(
  environmentId: EnvironmentId,
  primaryEnvironmentId: EnvironmentId | null,
  snapshot:
    | (Pick<PreviewSessionSnapshot, "runtime"> &
        Partial<Pick<PreviewSessionSnapshot, "threadId" | "tabId">>)
    | null
    | undefined,
): boolean {
  if (!isElectron || snapshot?.runtime !== "server") return false;
  if (!snapshot.threadId || !snapshot.tabId) return environmentId === primaryEnvironmentId;
  return nativeCompanionRendering({
    environmentId,
    primaryEnvironmentId,
    snapshot: { ...snapshot, threadId: snapshot.threadId, tabId: snapshot.tabId },
    binding: readBindingResult({ environmentId, threadId: ThreadId.make(snapshot.threadId) }),
    companion: appAtomRegistry.get(companionStateAtom),
  });
}

export function useRendersServerTabNatively(
  environmentId: EnvironmentId,
  snapshot:
    | (Pick<PreviewSessionSnapshot, "runtime"> &
        Partial<Pick<PreviewSessionSnapshot, "threadId" | "tabId">>)
    | null
    | undefined,
): boolean {
  const primaryEnvironmentId = useAtomValue(primaryEnvironmentIdAtom);
  const companion = useAtomValue(companionStateAtom);
  const binding = useAtomValue(
    snapshot?.threadId
      ? bindingsQuery({ environmentId, input: { threadId: ThreadId.make(snapshot.threadId) } })
      : emptyBinding,
  );
  if (!isElectron || !snapshot?.threadId || !snapshot.tabId)
    return rendersServerTabNatively(environmentId, primaryEnvironmentId, snapshot);
  return nativeCompanionRendering({
    environmentId,
    primaryEnvironmentId,
    companion,
    snapshot: { ...snapshot, threadId: snapshot.threadId, tabId: snapshot.tabId },
    binding: binding && AsyncResult.isSuccess(binding) ? binding.value : null,
  });
}
const emptyBinding = Atom.make(null);
