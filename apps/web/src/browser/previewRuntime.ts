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

export function isPreviewAvailableFor(environmentId: EnvironmentId): boolean {
  return isPreviewSupportedInRuntime() || readEnvironmentSupportsServerBrowser(environmentId);
}

export function usePreviewAvailable(environmentId: EnvironmentId | null): boolean {
  const serverBrowser = useEnvironmentSupportsServerBrowser(environmentId);
  return isPreviewSupportedInRuntime() || serverBrowser;
}

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
