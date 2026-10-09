import { ThreadId } from "@t3tools/contracts";
import { Atom, AsyncResult } from "effect/reactivity";
import {
  bindingsQuery,
  companionStateAtom,
  readBindingResult,
} from "../jones/previewCompanion/state";
import { nativeCompanionRendering } from "../jones/previewCompanion/inventory";
import { appAtomRegistry } from "../rpc/atomRegistry";
import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentId, PreviewRuntime, PreviewSessionSnapshot } from "@t3tools/contracts";

import { isElectron } from "~/env";
import { isPreviewSupportedInRuntime } from "~/previewStateStore";
import { primaryEnvironmentIdAtom } from "~/state/primaryEnvironment";
import {
  readEnvironmentSupportsServerBrowser,
  useEnvironmentSupportsServerBrowser,
} from "~/state/entities";

export function previewRuntimeFor(environmentId: EnvironmentId): PreviewRuntime | undefined {
  return readEnvironmentSupportsServerBrowser(environmentId) ? "server" : undefined;
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
