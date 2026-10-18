import { applyCompanionSessionEvent } from "./snapshot.ts";
import { parseScopedThreadKey, scopedThreadKey } from "@t3tools/client-runtime/environment";
import type { PreviewListResult, ScopedThreadRef } from "@t3tools/contracts";
import { Atom, AsyncResult } from "effect/reactivity";
import { useAtomValue } from "@effect/atom-react";
import { useMemo } from "react";
import { previewEnvironment } from "../../state/preview";
import { previewRuntimeTabId } from "../../browser/previewRuntimeTabId";
import { bindingsQuery, companionStateAtom } from "./state.ts";
import { nativeCompanionRendering, type CompanionMountedSession } from "./inventory.ts";

const threadSessions = Atom.family((key: string) =>
  Atom.make<PreviewListResult | null>((get) => {
    const [threadKey] = JSON.parse(key) as [string, number];
    const threadRef = parseScopedThreadKey(threadKey);
    if (!threadRef) return null;
    const list = previewEnvironment.list({
      environmentId: threadRef.environmentId,
      input: { threadId: threadRef.threadId },
    });
    const events = previewEnvironment.events({ environmentId: threadRef.environmentId, input: {} });
    let current: PreviewListResult | null = null;
    let epoch: string | null = null;
    let revision = -1;
    const retiredEpochs = new Set<string>();
    let live = true;
    get.addFinalizer(() => {
      live = false;
    });
    get.subscribe(events, (result) => {
      if (!AsyncResult.isSuccess(result) || result.value.threadId !== threadRef.threadId) return;
      const event = result.value;
      if (
        retiredEpochs.has(event.serverEpoch) ||
        (epoch === event.serverEpoch && event.revision <= revision)
      )
        return;
      if (epoch !== null && epoch !== event.serverEpoch) retiredEpochs.add(epoch);
      epoch = event.serverEpoch;
      revision = event.revision;
      current = applyCompanionSessionEvent(current, event);
      get.setSelf(current);
      if (!current) get.refresh(list);
    });
    get.subscribe(list, (result) => {
      if (!AsyncResult.isSuccess(result) || result.waiting) return;
      if (
        retiredEpochs.has(result.value.serverEpoch) ||
        (epoch !== null &&
          ((current === null && epoch !== result.value.serverEpoch) ||
            (epoch === result.value.serverEpoch && result.value.revision < revision)))
      ) {
        queueMicrotask(() => {
          if (live) get.refresh(list);
        });
        return;
      }
      if (epoch !== null && epoch !== result.value.serverEpoch) retiredEpochs.add(epoch);
      current = result.value;
      epoch = current.serverEpoch;
      revision = current.revision;
      get.setSelf(current);
    });
    get.mount(events);
    get.mount(list);
    queueMicrotask(() => {
      if (live) get.refresh(list);
    });
    return current;
  }).pipe(Atom.setIdleTTL(0)),
);

export function useAssignedCompanionSessions(): ReadonlyArray<CompanionMountedSession> {
  return useAtomValue(
    useMemo(
      () =>
        Atom.make((get) => {
          const state = get(companionStateAtom);
          const environmentId = state?.config.environmentId;
          if (
            !state ||
            !environmentId ||
            !state.config.enabled ||
            state.status !== "online" ||
            state.connectionGeneration === null
          )
            return [];
          const sessions: CompanionMountedSession[] = [];
          for (const threadId of new Set(state.assignments.map((key) => key.threadId))) {
            const threadRef: ScopedThreadRef = { environmentId, threadId };
            const list = get(
              threadSessions(
                JSON.stringify([scopedThreadKey(threadRef), state.connectionGeneration]),
              ),
            );
            const binding = get(bindingsQuery({ environmentId, input: { threadId } }));
            if (!list || !AsyncResult.isSuccess(binding)) continue;
            for (const snapshot of list.sessions) {
              if (
                !nativeCompanionRendering({
                  environmentId,
                  primaryEnvironmentId: null,
                  snapshot,
                  binding: binding.value,
                  companion: state,
                })
              )
                continue;
              sessions.push({
                threadRef,
                snapshot,
                runtimeTabId: previewRuntimeTabId(threadRef, list.serverEpoch, snapshot.tabId),
                pictureInPicture: false,
                zoomFactor: snapshot.zoomFactor ?? 1,
                companion: true,
              });
            }
          }
          return sessions;
        }),
      [],
    ),
  );
}
