import {
  CommandId,
  StopCurrentThreadRuntimeInput,
  type ExecutionEnvironmentCapabilities,
  type CurrentRuntimeStopTarget,
  type ScopedThreadRef,
} from "@t3tools/contracts";
import { scopedThreadKey } from "@t3tools/client-runtime/environment";
import {
  restartCapturedCurrentRuntime,
  type CurrentRuntimeRestartOutcome,
} from "@t3tools/client-runtime/operations";
import { useCallback } from "react";
import { randomUUID } from "../lib/utils";
import { threadEnvironment } from "../state/threads";
import { useAtomCommand } from "../state/use-atom-command";
import {
  getLocalStorageItem,
  setLocalStorageItem,
  removeLocalStorageItem,
} from "./useLocalStorage";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
export function currentRuntimeStopPointerStore(key: string) {
  return {
    readPointer: () => getLocalStorageItem(key, StopCurrentThreadRuntimeInput),
    reserve: (input: StopCurrentThreadRuntimeInput) => {
      if (getLocalStorageItem(key, StopCurrentThreadRuntimeInput) !== null)
        throw new Error("A previous stop identity is retained.");
      setLocalStorageItem(key, input, StopCurrentThreadRuntimeInput);
      const saved = getLocalStorageItem(key, StopCurrentThreadRuntimeInput);
      if (saved === null || JSON.stringify(saved) !== JSON.stringify(input))
        throw new Error("Stop identity readback failed.");
    },
    clear: (input: StopCurrentThreadRuntimeInput) => {
      const saved = getLocalStorageItem(key, StopCurrentThreadRuntimeInput);
      if (saved === null || JSON.stringify(saved) !== JSON.stringify(input))
        throw new Error("Stop identity changed.");
      removeLocalStorageItem(key);
    },
  };
}
export function useCurrentRuntimeStop() {
  const capture = useAtomCommand(threadEnvironment.readCurrentRuntimeStopTarget, {
    reportFailure: false,
  });
  const submit = useAtomCommand(threadEnvironment.stopCurrentThreadRuntime, {
    reportFailure: false,
  });
  const observe = useAtomCommand(threadEnvironment.observeCurrentThreadRuntimeStop, {
    reportFailure: false,
  });
  return useCallback(
    async (
      ref: ScopedThreadRef,
      capability: ExecutionEnvironmentCapabilities["currentRuntimeStop"],
      refresh: (target: CurrentRuntimeStopTarget) => Promise<void>,
    ): Promise<CurrentRuntimeRestartOutcome> => {
      const key = `jones.current-runtime-stop.v1:${scopedThreadKey(ref)}`;
      if (typeof navigator === "undefined" || navigator.locks === undefined)
        return {
          status: "unavailable",
          reason: "Safe stop coordination is unavailable in this client. No stop was sent.",
        };
      return navigator.locks.request(key, { ifAvailable: true }, async (lock) => {
        if (lock === null)
          return {
            status: "unknown" as const,
            reason: "Another client action owns the original stop. No new stop was sent.",
          };
        return restartCapturedCurrentRuntime(ref.threadId, capability, {
          ...currentRuntimeStopPointerStore(key),
          commandId: () => CommandId.make(`command:runtime-stop:${randomUUID()}`),
          capture: async () => {
            const result = await capture({
              environmentId: ref.environmentId,
              input: { threadId: ref.threadId },
            });
            if (result._tag === "Failure") throw squashAtomCommandFailure(result);
            return result.value;
          },
          submit: async (input) => {
            const result = await submit({ environmentId: ref.environmentId, input });
            if (result._tag === "Failure") throw squashAtomCommandFailure(result);
            return result.value;
          },
          observe: async (input) => {
            const result = await observe({ environmentId: ref.environmentId, input });
            if (result._tag === "Failure") throw squashAtomCommandFailure(result);
            return result.value;
          },
          refresh,
        });
      });
    },
    [capture, submit, observe],
  );
}
