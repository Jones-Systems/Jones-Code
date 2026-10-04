import { useAtomValue } from "@effect/atom-react";
import { enabledEnvironmentIds } from "@t3tools/client-runtime/state/connections";
import { arrayElementsEqual } from "@t3tools/client-runtime/state/entities";
import {
  createEnvironmentThreadDetailAtoms,
  createEnvironmentThreadShellAtoms,
  createEnvironmentThreadStateAtoms,
  EMPTY_ENVIRONMENT_THREAD_STATE,
  type EnvironmentThreadState,
  createThreadEnvironmentAtoms,
} from "@t3tools/client-runtime/state/threads";
import type {
  EnvironmentId,
  OrchestrationV2OperatingCountsResult,
  OrchestrationV2ShellSnapshot,
  OrchestrationV2ThreadRuntimeObservationResult,
  OrchestrationV2ThreadShell,
  ScopedProjectRef,
  ScopedThreadRef,
  ThreadId,
} from "@t3tools/contracts";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import { scopedThreadKey } from "@t3tools/client-runtime/environment";
import * as Option from "effect/Option";
import { AsyncResult, Atom } from "effect/unstable/reactivity";

import { environmentCatalog } from "../connection/catalog";
import { connectionAtomRuntime } from "../connection/runtime";
import { environmentShell, environmentSnapshotAtom } from "./shell";
import {
  createThreadContinuationAtoms,
  resolveOperatingCounts,
  resolveThreadOperatingState,
  resolveThreadRuntimeObservation,
} from "@t3tools/client-runtime/state/thread-continuation";

export const threadEnvironment = createThreadEnvironmentAtoms(
  connectionAtomRuntime,
  environmentSnapshotAtom,
);
const environmentThreads = createEnvironmentThreadStateAtoms(connectionAtomRuntime);
export const environmentThreadDetails = createEnvironmentThreadDetailAtoms(
  environmentThreads.stateAtom,
);
export const environmentThreadShells = createEnvironmentThreadShellAtoms({
  catalogValueAtom: environmentCatalog.catalogValueAtom,
  snapshotAtom: threadEnvironment.snapshotAtom,
});

export const threadContinuation = createThreadContinuationAtoms(connectionAtomRuntime, {
  threadRefreshAtom: environmentThreadShells.threadShellAtom,
  snapshotAtom: environmentSnapshotAtom,
});

export type ThreadOperatingState = ReturnType<typeof resolveThreadOperatingState>;

export function createThreadOperatingStatesAtom<E>(input: {
  readonly threadsAtom: Atom.Atom<ReadonlyArray<EnvironmentThreadShell>>;
  readonly isCurrentAtom: (environmentId: EnvironmentId) => Atom.Atom<boolean>;
  readonly observationAtom: (
    ref: ScopedThreadRef,
  ) => Atom.Atom<AsyncResult.AsyncResult<OrchestrationV2ThreadRuntimeObservationResult, E>>;
}) {
  return Atom.make((get): ReadonlyMap<string, ThreadOperatingState> => {
    const states = new Map<string, ThreadOperatingState>();
    for (const thread of get(input.threadsAtom)) {
      if (thread.archivedAt !== null) continue;
      const ref = { environmentId: thread.environmentId, threadId: thread.id };
      const current = get(input.isCurrentAtom(thread.environmentId));
      const observation = current
        ? resolveThreadRuntimeObservation(get(input.observationAtom(ref)), thread)
        : { status: "unknown" as const, reason: "Current shell projection is unavailable." };
      states.set(
        scopedThreadKey(ref),
        resolveThreadOperatingState(current ? thread : { ...thread, runtime: null }, observation),
      );
    }
    return states;
  }).pipe(Atom.withLabel("web-thread-operating-states"));
}

const currentShellAtom = Atom.family((environmentId: EnvironmentId) =>
  Atom.map(
    environmentShell.stateValueAtom(environmentId),
    (state) => state.status === "live" && Option.isNone(state.error),
  ),
);
export const threadOperatingStatesAtom = createThreadOperatingStatesAtom({
  threadsAtom: environmentThreadShells.threadShellsAtom,
  isCurrentAtom: currentShellAtom,
  observationAtom: (ref) =>
    threadContinuation.runtimeObservation({
      environmentId: ref.environmentId,
      input: { threadId: ref.threadId },
    }),
});

export function useThreadOperatingStates() {
  return useAtomValue(threadOperatingStatesAtom);
}

type OperatingCountRequest = {
  readonly environmentId: EnvironmentId;
  readonly projectId?: ScopedProjectRef["projectId"];
};

export function createOperatingCountAtom<E>(input: {
  readonly requestsAtom: Atom.Atom<ReadonlyArray<OperatingCountRequest>>;
  readonly isCurrentAtom: (environmentId: EnvironmentId) => Atom.Atom<boolean>;
  readonly snapshotAtom: (
    environmentId: EnvironmentId,
  ) => Atom.Atom<OrchestrationV2ShellSnapshot | null>;
  readonly countsAtom: (
    ref: OperatingCountRequest,
  ) => Atom.Atom<AsyncResult.AsyncResult<OrchestrationV2OperatingCountsResult, E>>;
}) {
  return Atom.make((get): number | null => {
    const requests = get(input.requestsAtom);
    let total = 0;
    for (const ref of requests) {
      if (!get(input.isCurrentAtom(ref.environmentId))) return null;
      const snapshot = get(input.snapshotAtom(ref.environmentId));
      if (snapshot === null) return null;
      const state = resolveOperatingCounts(get(input.countsAtom(ref)), snapshot.snapshotSequence);
      if (state.counts === null) return null;
      total += state.counts.operating;
    }
    return requests.length === 0 ? null : total;
  });
}

const operatingCountAtom = Atom.family((scope: string) => {
  const refs = JSON.parse(scope) as ReadonlyArray<ScopedProjectRef> | null;
  return createOperatingCountAtom({
    requestsAtom: Atom.make(
      (get) =>
        refs ??
        Array.from(
          enabledEnvironmentIds(get(environmentCatalog.catalogValueAtom)),
          (environmentId) => ({ environmentId }),
        ),
    ),
    isCurrentAtom: currentShellAtom,
    snapshotAtom: environmentSnapshotAtom,
    countsAtom: (ref) =>
      threadContinuation.operatingCounts({
        environmentId: ref.environmentId,
        input: ref.projectId === undefined ? {} : { projectId: ref.projectId },
      }),
  }).pipe(Atom.withLabel(`web-operating-count:${scope}`));
});

export function useOperatingCount(refs: ReadonlyArray<ScopedProjectRef> | null = null) {
  const scope =
    refs === null
      ? "null"
      : JSON.stringify(
          [
            ...new Map(refs.map((ref) => [`${ref.environmentId}:${ref.projectId}`, ref])).values(),
          ].sort((a, b) =>
            `${a.environmentId}:${a.projectId}`.localeCompare(`${b.environmentId}:${b.projectId}`),
          ),
        );
  return useAtomValue(operatingCountAtom(scope));
}

const EMPTY_THREAD_STATE_ATOM = Atom.make(AsyncResult.success(EMPTY_ENVIRONMENT_THREAD_STATE)).pipe(
  Atom.withLabel("web-environment-thread:empty"),
);

export function useEnvironmentThread(
  environmentId: EnvironmentId | null,
  threadId: ThreadId | null,
): EnvironmentThreadState {
  const result = useAtomValue(
    environmentId !== null && threadId !== null
      ? environmentThreads.stateAtom(environmentId, threadId)
      : EMPTY_THREAD_STATE_ATOM,
  );
  const state = Option.getOrElse(
    AsyncResult.value(result),
    () => EMPTY_ENVIRONMENT_THREAD_STATE,
  ) as EnvironmentThreadState;
  return state;
}

const isRunning = (status: string) =>
  status === "preparing" || status === "starting" || status === "running";

type KeptThreads = ReadonlyMap<EnvironmentId, ReadonlySet<ThreadId>>;

// True once a thread's own stream no longer needs to stay open: it is in sync
// and shows a settled session, or it cannot progress (deleted or failed). A
// stream that is still loading or reconnecting keeps waiting for the stop.
function isDetailDone<E>(result: AsyncResult.AsyncResult<EnvironmentThreadState, E>): boolean {
  if (!AsyncResult.isSuccess(result)) return true;
  const { status, data, error } = result.value;
  if (status === "deleted" || Option.isSome(error)) return true;
  return (
    status === "live" &&
    !Option.exists(data, (thread) => thread.runs.some((run) => isRunning(run.status)))
  );
}

/**
 * Keeps the thread state atom mounted for each running thread in the listed
 * environments. Mount the result; its value is only bookkeeping.
 *
 * The shell and detail streams are independent, so the shell can report a
 * stop before the detail loads or catches up. A stopped thread stays mounted
 * until its own detail is live and shows the stop too. Then the stream closes
 * and saves the settled state to disk.
 */
export function createRunningThreadKeepAliveAtom<E>(input: {
  readonly environmentIdsAtom: Atom.Atom<ReadonlyArray<EnvironmentId>>;
  readonly threadsAtom: (
    environmentId: EnvironmentId,
  ) => Atom.Atom<ReadonlyArray<Pick<OrchestrationV2ThreadShell, "id" | "status">>>;
  readonly stateAtom: (
    environmentId: EnvironmentId,
    threadId: ThreadId,
  ) => Atom.Atom<AsyncResult.AsyncResult<EnvironmentThreadState, E>>;
}) {
  // Keeps its identity until a thread starts or stops, so ordinary shell
  // updates do not rebuild the keep-alive set.
  const runningThreadIdsAtom = Atom.family((environmentId: EnvironmentId) => {
    let previous: ReadonlyArray<ThreadId> = [];
    return Atom.make((get) => {
      const running = get(input.threadsAtom(environmentId)).flatMap((thread) =>
        isRunning(thread.status) ? [thread.id] : [],
      );
      if (arrayElementsEqual(previous, running)) return previous;
      previous = running;
      return running;
    }).pipe(Atom.withLabel(`web-running-thread-ids:${environmentId}`));
  });

  return Atom.make((get): KeptThreads => {
    const previous = Option.getOrUndefined(get.self<KeptThreads>());
    const kept = new Map<EnvironmentId, ReadonlySet<ThreadId>>();
    // An environment that leaves the list is not visited, so its mounts drop.
    for (const environmentId of get(input.environmentIdsAtom)) {
      const threadIds = new Set(get(runningThreadIdsAtom(environmentId)));
      for (const threadId of previous?.get(environmentId) ?? []) {
        if (threadIds.has(threadId)) continue;
        const stateAtom = input.stateAtom(environmentId, threadId);
        // `once`, not `get`: a dependency on a stopped thread would hold its
        // stream open until some other change rebuilds this atom.
        if (isDetailDone(get.once(stateAtom))) continue;
        threadIds.add(threadId);
        // Rebuild when this detail is done, not on each update.
        get.subscribe(stateAtom, (state) => {
          if (isDetailDone(state)) get.refreshSelf();
        });
      }
      for (const threadId of threadIds) get.mount(input.stateAtom(environmentId, threadId));
      kept.set(environmentId, threadIds);
    }
    return kept;
  }).pipe(Atom.withLabel("web-running-thread-keep-alive"));
}

/** Mounted by `RunningThreadKeepAlive` on desktop, for every enabled environment. */
export const runningThreadKeepAliveAtom = createRunningThreadKeepAliveAtom({
  environmentIdsAtom: Atom.map(environmentCatalog.catalogValueAtom, (catalog) => [
    ...enabledEnvironmentIds(catalog),
  ]),
  threadsAtom: environmentThreadShells.environmentThreadsAtom,
  stateAtom: environmentThreads.stateAtom,
});
