import { useEffect, useRef, useState, useSyncExternalStore } from "react";
import { useAtomValue } from "@effect/atom-react";
import type { ThreadId } from "@t3tools/contracts";
import { buildMonitorCohort, monitorThreadKey } from "@t3tools/client-runtime/jones/thread-monitor";
import {
  seedUserInputDraftAnswers,
  derivePendingThreadRequests,
} from "@t3tools/client-runtime/state/thread-requests";
import {
  type ThreadMonitorPublisherFrame,
  type ThreadMonitorTheme,
} from "@t3tools/contracts/jones/desktopThreadMonitor";
import * as Option from "effect/Option";
import { pendingDecisionRequestIds } from "./pendingDecisionRequestIds";
import type { AppRouter } from "../../router";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import { environmentThreadDetails } from "../../state/threads";
import { monitorStore, refOf } from "./store";
import { monitorInputsAtom, monitorInputFor } from "./inputs";
import { MonitorActionController, monitorPlanUnavailableReason } from "./actions";
import { monitorSnapshot } from "./snapshots";
import { observeMonitorTheme } from "./theme";
import { readEnvironmentSupportsSettlementRestore } from "../settlementRestore/settlementRestore";
import { projectMonitorDetail } from "./detail";
import {
  readMonitorNativeState,
  setMonitorNativeState,
  subscribeMonitorNativeState,
} from "./nativeState";

export function MonitorController({ router }: { readonly router: AppRouter }) {
  return window.desktopBridge?.threadMonitor ? <NativeMonitorPublisher router={router} /> : null;
}
const readMonitorSelection = () => monitorStore.selection;
function NativeMonitorPublisher({ router }: { readonly router: AppRouter }) {
  const inputs = useAtomValue(monitorInputsAtom);
  const selection = useSyncExternalStore(monitorStore.subscribe, readMonitorSelection);
  const native = useSyncExternalStore(subscribeMonitorNativeState, readMonitorNativeState);
  const [now, setNow] = useState(Date.now);
  const transport = useRef<((frame: ThreadMonitorPublisherFrame) => Promise<void>) | null>(null);
  const replay = useRef<(() => void) | null>(null);
  const readPendingKey = () =>
    JSON.stringify(
      inputs.map((input) => [
        input.environment.environmentId,
        [...monitorStore.pendingThreads(input.environment.environmentId)],
      ]),
    );
  const pendingKey = useSyncExternalStore(monitorStore.subscribe, readPendingKey);
  const cohort = (() => {
    const pending = new Map(
      JSON.parse(pendingKey) as ReadonlyArray<readonly [string, ReadonlyArray<ThreadId>]>,
    );
    return buildMonitorCohort(
      inputs.map((input) => ({
        ...input,
        pendingActionThreadIds: new Set(pending.get(input.environment.environmentId) ?? []),
      })),
      now,
    );
  })();
  const expiry = cohort.nextDoneExpiryMs;
  // oxlint-disable-next-line t3code/react-effect-budget -- Publish the rendered cohort to the external monitor store and native peer.
  useEffect(() => {
    monitorStore.setCohort(
      inputs.map((input) => input.environment),
      cohort.rows.map((row) => ({
        ...row,
        actions: {
          ...row.actions,
          restore: {
            available: readEnvironmentSupportsSettlementRestore(row.environmentId),
            reason: readEnvironmentSupportsSettlementRestore(row.environmentId)
              ? null
              : "Server does not support exact settlement restoration",
          },
        },
      })),
      cohort.counts,
    );
    // Draft and view-state notifications retain this compiler-memoized cohort.
  }, [inputs, cohort]);
  // oxlint-disable-next-line t3code/react-effect-budget -- Synchronize the wall clock with expiry timers and native resume events.
  useEffect(() => {
    const refresh = () => setNow(Date.now());
    const minute = setInterval(refresh, 60_000);
    const timer = expiry === null ? null : setTimeout(refresh, Math.max(1, expiry - Date.now()));
    document.addEventListener("visibilitychange", refresh);
    window.addEventListener("focus", refresh);
    window.addEventListener("pageshow", refresh);
    return () => {
      clearInterval(minute);
      if (timer !== null) clearTimeout(timer);
      document.removeEventListener("visibilitychange", refresh);
      window.removeEventListener("focus", refresh);
      window.removeEventListener("pageshow", refresh);
    };
  }, [expiry]);
  // oxlint-disable-next-line t3code/react-effect-budget -- Subscribe to native visibility changes so hidden time is reflected on reveal.
  useEffect(
    () =>
      subscribeMonitorNativeState(() => {
        if (readMonitorNativeState()?.visible) setNow(Date.now());
      }),
    [],
  );
  // oxlint-disable-next-line t3code/react-effect-budget -- Own the native bridge subscriptions and release them with this authenticated renderer.
  useEffect(() => {
    const bridge = window.desktopBridge!.threadMonitor!;
    let alive = true;
    let initialized = false;
    let queue = Promise.resolve();
    let displayRevision = 0;
    let scheduledViewRevision = -1;
    let theme: ThreadMonitorTheme | null = null;
    let scheduled: ReturnType<typeof setTimeout> | null = null;
    const accepts = (frame: ThreadMonitorPublisherFrame) => {
      if (frame.type === "viewState")
        return (
          frame.state.revision === monitorStore.viewState.revision &&
          frame.state.publisherGeneration === monitorStore.fence.publisherGeneration &&
          (frame.state.editor === null || monitorStore.fence.accepts(frame.state.editor.ref))
        );
      if (frame.type === "draft") return monitorStore.fence.accepts(frame.draft);
      if (frame.type === "detail") return monitorStore.fence.accepts(frame.detail);
      if (frame.type === "actionResult") return monitorStore.fence.accepts(frame);
      if (frame.type === "view")
        return [...frame.environments, ...frame.upserts, ...frame.removed].every((ref) =>
          monitorStore.fence.accepts(ref),
        );
      return true;
    };
    const publishBatch = (frames: ReadonlyArray<ThreadMonitorPublisherFrame>) => {
      const next = queue.then(async () => {
        for (const frame of frames) {
          if (alive && accepts(frame)) await bridge.publish(frame);
        }
      });
      queue = next.catch(() => undefined);
      return next;
    };
    const publish = (frame: ThreadMonitorPublisherFrame) => publishBatch([frame]);
    transport.current = publish;
    const viewFrames = () => {
      displayRevision = displayRevision + 1;
      return monitorSnapshot({
        publisherGeneration: monitorStore.fence.publisherGeneration,
        revision: displayRevision,
        environments: monitorStore.environments,
        rows: monitorStore.visibleRows(),
        counts: monitorStore.counts,
      });
    };
    const publishView = async () => {
      if (!alive || !initialized) return;
      await publishBatch(viewFrames());
    };
    const fullReplay = async () => {
      if (!alive || !initialized) return;
      const frames: ThreadMonitorPublisherFrame[] = [...viewFrames()];
      if (theme)
        frames.push({
          type: "theme",
          publisherGeneration: monitorStore.fence.publisherGeneration,
          theme,
        });
      for (const draft of monitorStore.drafts.values()) frames.push({ type: "draft", draft });
      if (monitorStore.detail) frames.push({ type: "detail", detail: monitorStore.detail });
      else if (monitorStore.selection)
        frames.push({
          type: "detail",
          detail: projectMonitorDetail({
            selection: monitorStore.selection,
            projection: null,
            revision: monitorStore.nextDetailRevision(),
            unavailableReason: "Waiting for current thread detail",
            planUnavailableReason: "Waiting for current thread detail",
            childAttention: false,
          }),
        });
      frames.push(...monitorStore.results.values(), {
        type: "viewState",
        state: monitorStore.viewState,
      });
      await publishBatch(frames);
    };
    const scheduleView = () => {
      if (scheduled === null && scheduledViewRevision !== monitorStore.snapshotRevision)
        scheduled = setTimeout(() => {
          scheduled = null;
          scheduledViewRevision = monitorStore.snapshotRevision;
          void publishView().catch(() => undefined);
        }, 16);
    };
    replay.current = () => {
      void fullReplay().catch(() => undefined);
    };
    const actions = new MonitorActionController(router, publish);
    const unsubscribe = bridge.onIntent((intent) => {
      if (intent.type === "ready" || intent.type === "requestSnapshot") {
        void fullReplay().catch(() => undefined);
        return;
      }
      if (intent.type === "setViewState") {
        if (monitorStore.editViewState(intent.state))
          void publish({ type: "viewState", state: monitorStore.viewState }).catch(() => undefined);
        return;
      }
      if (intent.type === "requestMore") {
        monitorStore.more(intent.group);
        return;
      }
      if (intent.type === "editDraft") {
        const thread = monitorInputFor(
          appAtomRegistry.get(monitorInputsAtom),
          intent.draft.environmentId,
        )?.threads.find(
          (thread) =>
            thread.id === intent.draft.threadId &&
            thread.deletedAt === null &&
            thread.archivedAt === null,
        );
        if (thread && monitorStore.edit(intent.draft))
          void publish({ type: "draft", draft: intent.draft }).catch(() => undefined);
        return;
      }
      if (intent.type === "selectDetail") {
        if (
          monitorStore.fence.accepts(intent) &&
          monitorInputFor(
            appAtomRegistry.get(monitorInputsAtom),
            intent.environmentId,
          )?.threads.some(
            (thread) =>
              thread.id === intent.threadId &&
              thread.deletedAt === null &&
              thread.archivedAt === null,
          )
        )
          monitorStore.select({ ...refOf(intent), selectionId: intent.selectionId });
        return;
      }
      if (intent.type === "releaseDetail") {
        if (
          monitorStore.fence.accepts(intent) &&
          monitorStore.selection?.selectionId === intent.selectionId
        )
          monitorStore.select(null);
        return;
      }
      if (
        intent.type === "setExpanded" ||
        intent.type === "hide" ||
        intent.type === "environmentInvalidated"
      )
        return;
      void actions.dispatch(intent).catch(() => undefined);
    });
    monitorStore.bindInvalidator((frame) => bridge.invalidateEnvironment(frame));
    const stopState = bridge.onState(setMonitorNativeState);
    const stopStore = monitorStore.subscribe(scheduleView);
    const stopTheme = observeMonitorTheme((next) => {
      theme = next;
      if (initialized)
        void publish({
          type: "theme",
          publisherGeneration: monitorStore.fence.publisherGeneration,
          theme: next,
        }).catch(() => undefined);
    });
    void publish({ type: "reset", publisherGeneration: monitorStore.fence.publisherGeneration })
      .then(async () => {
        initialized = true;
        setMonitorNativeState(await bridge.getState());
        await fullReplay();
        document.documentElement.dataset.threadMonitorPublisherReady = "true";
      })
      .catch(() => {
        setMonitorNativeState({
          supported: false,
          enabled: false,
          visible: false,
          mainVisible: true,
          mainFocused: false,
          expanded: false,
          reason: "Thread monitor publisher is unavailable",
        });
      });
    return () => {
      delete document.documentElement.dataset.threadMonitorPublisherReady;
      alive = false;
      unsubscribe();
      stopState();
      stopStore();
      stopTheme();
      if (scheduled !== null) clearTimeout(scheduled);
      monitorStore.bindInvalidator(null);
      transport.current = null;
      replay.current = null;
      setMonitorNativeState(null);
    };
  }, [router]);
  // oxlint-disable-next-line t3code/react-effect-budget -- Mount only the selected external thread-detail subscription and fence its cleanup.
  useEffect(() => {
    if (!selection || !native?.visible || !monitorStore.fence.accepts(selection)) return;
    const atom = environmentThreadDetails.stateAtom(selection);
    const unmount = appAtomRegistry.mount(atom);
    let released = false;
    const update = () => {
      if (
        released ||
        !monitorStore.fence.accepts(selection) ||
        monitorStore.selection?.selectionId !== selection.selectionId
      )
        return;
      const state = appAtomRegistry.get(atom);
      const projection = Option.getOrNull(state.data);
      const currentInputs = appAtomRegistry.get(monitorInputsAtom);
      const input = monitorInputFor(currentInputs, selection.environmentId);
      const thread = input?.threads.find((thread) => thread.id === selection.threadId);
      const reason =
        state.status !== "live" || Option.isSome(state.error)
          ? "Waiting for current thread detail"
          : input?.environment.transport !== "connected" || input.environment.freshness !== "live"
            ? "Waiting for current environment data"
            : !input.canOperate
              ? "Operation permission required"
              : !thread || thread.deletedAt !== null
                ? "Thread is unavailable"
                : null;
      const detail = projectMonitorDetail({
        selection,
        projection,
        revision: monitorStore.nextDetailRevision(),
        unavailableReason: reason,
        planUnavailableReason:
          reason ??
          (thread ? monitorPlanUnavailableReason(thread, projection) : "Thread is unavailable"),
        childAttention:
          monitorStore.rows.find((row) => monitorThreadKey(row) === monitorThreadKey(selection))
            ?.pendingChildAttention != null,
      });
      const pendingRequests =
        projection && reason === null ? derivePendingThreadRequests(projection) : null;
      monitorStore.setDetail(
        detail,
        projection && reason === null ? pendingDecisionRequestIds(projection) : undefined,
      );
      if (projection && reason === null) {
        const request = pendingRequests?.userInputs[0];
        if (
          request &&
          !monitorStore.drafts.has(
            JSON.stringify([
              selection.publisherGeneration,
              selection.environmentId,
              selection.environmentEpoch,
              selection.threadId,
              request.requestId,
            ]),
          )
        ) {
          const seeded = seedUserInputDraftAnswers(request.questions, {});
          const answers = Object.fromEntries(
            Object.entries(seeded).flatMap(([id, answer]) =>
              answer.customAnswer === undefined ? [] : [[id, answer.customAnswer]],
            ),
          );
          const draft = {
            ...monitorStore.draft(selection, request.requestId),
            revision: 1,
            answers,
          };
          monitorStore.edit(draft);
          void transport.current?.({ type: "draft", draft }).catch(() => undefined);
        }
      }
      void transport.current?.({ type: "detail", detail }).catch(() => undefined);
    };
    const unsubscribe = appAtomRegistry.subscribe(atom, update);
    const stopInputs = appAtomRegistry.subscribe(monitorInputsAtom, update);
    update();
    return monitorStore.bindDetailRelease(() => {
      if (released) return;
      released = true;
      stopInputs();
      unsubscribe();
      unmount();
    });
  }, [selection, native?.visible]);
  return null;
}
