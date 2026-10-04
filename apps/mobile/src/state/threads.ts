import { useAtomRefresh, useAtomValue } from "@effect/atom-react";
import { useFocusEffect } from "@react-navigation/native";
import { useCallback } from "react";
import {
  createThreadContinuationAtoms,
  resolveThreadOperatingState,
  resolveThreadRuntimeObservation,
} from "@t3tools/client-runtime/state/thread-continuation";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import {
  createEnvironmentThreadDetailAtoms,
  createEnvironmentThreadShellAtoms,
  createEnvironmentThreadStateAtoms,
  EMPTY_ENVIRONMENT_THREAD_STATE,
  type EnvironmentThreadState,
  createThreadEnvironmentAtoms,
} from "@t3tools/client-runtime/state/threads";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import * as Option from "effect/Option";
import { AsyncResult, Atom } from "effect/unstable/reactivity";

import { environmentCatalog } from "../connection/catalog";
import { connectionAtomRuntime } from "../connection/runtime";
import { environmentSnapshotAtom, useEnvironmentShellReadiness } from "./shell";

export const threadEnvironment = createThreadEnvironmentAtoms(
  connectionAtomRuntime,
  environmentSnapshotAtom,
);
export const environmentThreads = createEnvironmentThreadStateAtoms(connectionAtomRuntime);
export const environmentThreadDetails = createEnvironmentThreadDetailAtoms(
  environmentThreads.stateAtom,
);
export const environmentThreadShells = createEnvironmentThreadShellAtoms({
  catalogValueAtom: environmentCatalog.catalogValueAtom,
  snapshotAtom: threadEnvironment.snapshotAtom,
});
export const threadContinuation = createThreadContinuationAtoms(connectionAtomRuntime, {
  threadRefreshAtom: (ref) => environmentThreadShells.threadShellAtom(ref),
  snapshotAtom: environmentSnapshotAtom,
});

export function useThreadOperatingState(thread: EnvironmentThreadShell) {
  const shell = useEnvironmentShellReadiness(thread.environmentId);
  const foregroundCurrent = shell.status === "live" && !shell.hasError;
  const atom = threadContinuation.runtimeObservation({
    environmentId: thread.environmentId,
    input: { threadId: thread.id },
  });
  const query = useAtomValue(atom);
  const refresh = useAtomRefresh(atom);
  useFocusEffect(
    useCallback(() => {
      refresh();
    }, [refresh]),
  );
  const observation = foregroundCurrent
    ? resolveThreadRuntimeObservation(query, thread)
    : { status: "unknown" as const, reason: "Current shell projection is unavailable." };
  return {
    observation,
    foregroundCurrent,
    ...resolveThreadOperatingState(
      foregroundCurrent ? thread : { ...thread, runtime: null },
      observation,
    ),
  };
}

const EMPTY_THREAD_STATE_ATOM = Atom.make(AsyncResult.success(EMPTY_ENVIRONMENT_THREAD_STATE)).pipe(
  Atom.withLabel("mobile-environment-thread:empty"),
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
