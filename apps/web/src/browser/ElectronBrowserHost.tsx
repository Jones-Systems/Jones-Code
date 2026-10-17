"use client";

import { parseScopedThreadKey } from "@t3tools/client-runtime/environment";
import { AuthPreviewOperateScope, FILL_PREVIEW_VIEWPORT } from "@t3tools/contracts";
import { useAtomValue } from "@effect/atom-react";
import { type ComponentProps, useEffect, useMemo, useRef, useState } from "react";

import { Atom, AsyncResult } from "effect/reactivity";
import { useAssignedCompanionSessions } from "../jones/previewCompanion/sessions";
import { bindingsQuery, companionStateAtom } from "../jones/previewCompanion/state";
import { mergeCompanionSessions } from "../jones/previewCompanion/inventory";
import { primaryEnvironmentIdAtom } from "~/state/primaryEnvironment";

import { isElectron } from "~/env";
import { useClientSettingsHydrated } from "~/hooks/useSettings";
import { useTheme } from "~/hooks/useTheme";
import { useActivePreviewSessions } from "~/previewStateStore";
import { previewEnvironment } from "~/state/preview";
import { useEnvironmentScope } from "~/state/session";
import { useAtomCommand } from "~/state/use-atom-command";

import { readPreviewAnnotationTheme } from "./annotationTheme";
import { useBrowserDefaults } from "./browserDefaults";
import { useBrowserPointerStore } from "./browserPointerStore";
import { HostedBrowserWebview } from "./HostedBrowserWebview";
import { openUrlInPreview } from "./openFileInPreview";
import { rendersServerTabNatively } from "./previewRuntime";
import { previewRuntimeTabId } from "./previewRuntimeTabId";

export function ElectronBrowserHost() {
  const { resolvedTheme } = useTheme();
  const previewByThreadKey = useActivePreviewSessions();
  const primaryEnvironmentId = useAtomValue(primaryEnvironmentIdAtom);
  const assignedSessions = useAssignedCompanionSessions();
  const companion = useAtomValue(companionStateAtom);
  const bindingResults = useAtomValue(
    useMemo(
      () =>
        Atom.make((get) => {
          const results = new Map<string, ReturnType<typeof getBindingValue>>();
          for (const key of Object.keys(previewByThreadKey)) {
            const threadRef = parseScopedThreadKey(key);
            if (threadRef)
              results.set(
                key,
                getBindingValue(
                  get(
                    bindingsQuery({
                      environmentId: threadRef.environmentId,
                      input: { threadId: threadRef.threadId },
                    }),
                  ),
                ),
              );
          }
          return results;
        }),
      [previewByThreadKey],
    ),
  );
  const ordinarySessions = useMemo(
    () =>
      Object.entries(previewByThreadKey).flatMap(([threadKey, previewState]) => {
        const threadRef = parseScopedThreadKey(threadKey);
        return threadRef
          ? Object.values(previewState.sessions)
              .filter(
                (snapshot) =>
                  snapshot.runtime !== "server" ||
                  (bindingResults.get(threadKey)?.tabs.find((tab) => tab.tabId === snapshot.tabId)
                    ?.hostId == null &&
                    rendersServerTabNatively(
                      threadRef.environmentId,
                      primaryEnvironmentId,
                      snapshot,
                    )),
              )
              .map((snapshot) => ({
                threadRef,
                snapshot,
                runtimeTabId: previewRuntimeTabId(
                  threadRef,
                  previewState.serverEpoch,
                  snapshot.tabId,
                ),
                pictureInPicture:
                  previewState.desktopByTabId[snapshot.tabId]?.pictureInPicture ?? false,
                zoomFactor: previewState.desktopByTabId[snapshot.tabId]?.zoomFactor ?? 1,
              }))
          : [];
      }),
    [previewByThreadKey, primaryEnvironmentId, bindingResults, companion],
  );

  const sessions = mergeCompanionSessions(ordinarySessions, assignedSessions);

  useEffect(() => {
    const preview = window.desktopBridge?.preview;
    if (!preview) return;

    let lastSerializedTheme = "";
    const syncTheme = () => {
      const theme = readPreviewAnnotationTheme();
      const serializedTheme = JSON.stringify(theme);
      if (serializedTheme === lastSerializedTheme) return;
      lastSerializedTheme = serializedTheme;
      void preview.setAnnotationTheme(theme).catch(() => {
        lastSerializedTheme = "";
      });
    };
    const frameId = window.requestAnimationFrame(syncTheme);
    const observer = new MutationObserver(syncTheme);
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["class", "style"],
    });
    const headObserver = new MutationObserver(syncTheme);
    headObserver.observe(document.head, {
      childList: true,
      subtree: true,
      characterData: true,
    });
    return () => {
      window.cancelAnimationFrame(frameId);
      observer.disconnect();
      headObserver.disconnect();
    };
  }, [resolvedTheme]);

  useEffect(() => {
    const preview = window.desktopBridge?.preview;
    if (!preview) return;
    return preview.onPointerEvent((event) => {
      useBrowserPointerStore.getState().apply(event);
    });
  }, []);

  const openPreview = useAtomCommand(previewEnvironment.open, { reportFailure: true });
  const sessionByRuntimeTabId = useRef(new Map<string, (typeof sessions)[number]>());
  useEffect(() => {
    sessionByRuntimeTabId.current = new Map(
      sessions.map((session) => [session.runtimeTabId, session]),
    );
  }, [sessions]);
  useEffect(() => {
    const preview = window.desktopBridge?.preview;
    if (!preview) return;
    return preview.onOpenLink(({ tabId, url, background }) => {
      const source = sessionByRuntimeTabId.current.get(tabId);
      if (!source) return;
      void openUrlInPreview({
        threadRef: source.threadRef,
        url,
        openPreview,
        profileId: source.snapshot.profileId,
        background,
      });
    });
  }, [openPreview]);

  if (!isElectron) return null;
  return (
    <div className="contents" data-electron-browser-host>
      {sessions.map(
        ({ threadRef, snapshot, runtimeTabId, pictureInPicture, zoomFactor, companion }) => {
          const url = snapshot.navStatus._tag === "Idle" ? null : snapshot.navStatus.url;
          return (
            <AuthorizedBrowserWebview
              key={runtimeTabId}
              threadRef={threadRef}
              tabId={snapshot.tabId}
              runtimeTabId={runtimeTabId}
              initialUrl={url}
              {...(companion === undefined ? {} : { companion })}
              viewport={snapshot.viewport ?? FILL_PREVIEW_VIEWPORT}
              pictureInPicture={pictureInPicture}
              profileId={snapshot.profileId}
              zoomFactor={zoomFactor}
              serverDriven={snapshot.runtime === "server"}
              {...(snapshot.runtime === "server"
                ? {
                    serverRendering: {
                      colorScheme: snapshot.colorScheme ?? "system",
                      zoomFactor: snapshot.zoomFactor ?? 1,
                    },
                  }
                : {})}
            />
          );
        },
      )}
    </div>
  );
}

function AuthorizedBrowserWebview(props: ComponentProps<typeof HostedBrowserWebview>) {
  const canOperatePreview = useEnvironmentScope(
    props.threadRef.environmentId,
    AuthPreviewOperateScope,
  );
  const profileId = useTabProfileId(props.profileId);
  return canOperatePreview ? <HostedBrowserWebview {...props} profileId={profileId} /> : null;
}

/**
 * It is latched once settings load: Electron fixes the partition when the guest attaches, so a
 * later settings change must not move a live tab.
 */
function useTabProfileId(profileId: string | undefined): string | undefined {
  const hydrated = useClientSettingsHydrated();
  const defaultProfileId = useBrowserDefaults().profileId;
  const [fallback, setFallback] = useState<string | undefined>(undefined);
  if (profileId === undefined && hydrated && fallback === undefined) {
    setFallback(defaultProfileId);
  }
  return profileId ?? fallback;
}

function getBindingValue(value: Atom.Type<ReturnType<typeof bindingsQuery>>) {
  return AsyncResult.isSuccess(value) && value.value.status === "ready" ? value.value.value : null;
}
