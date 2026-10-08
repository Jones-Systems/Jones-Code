"use client";

import { parseScopedThreadKey } from "@t3tools/client-runtime/environment";
import { AuthPreviewOperateScope, FILL_PREVIEW_VIEWPORT } from "@t3tools/contracts";
import { useAtomValue } from "@effect/atom-react";
import { type ComponentProps, useEffect, useMemo } from "react";

import { Atom, AsyncResult } from "effect/reactivity";
import { useAssignedCompanionSessions } from "../jones/previewCompanion/sessions";
import { bindingsQuery, companionStateAtom } from "../jones/previewCompanion/state";
import { mergeCompanionSessions } from "../jones/previewCompanion/inventory";
import { primaryEnvironmentIdAtom } from "~/state/primaryEnvironment";

import { isElectron } from "~/env";
import { useTheme } from "~/hooks/useTheme";
import { useActivePreviewSessions } from "~/previewStateStore";
import { useEnvironmentScope } from "~/state/session";

import { readPreviewAnnotationTheme } from "./annotationTheme";
import { useBrowserPointerStore } from "./browserPointerStore";
import { HostedBrowserWebview } from "./HostedBrowserWebview";
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
        // Companion sessions come only from the assignment inventory below.
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
  return canOperatePreview ? <HostedBrowserWebview {...props} /> : null;
}

function getBindingValue(value: Atom.Type<ReturnType<typeof bindingsQuery>>) {
  return AsyncResult.isSuccess(value) && value.value.status === "ready" ? value.value.value : null;
}
