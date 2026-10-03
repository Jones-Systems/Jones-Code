import type {
  OrchestrationV2CurrentThreadRuntimeTarget,
  OrchestrationV2StopCurrentThreadRuntimeInput,
  OrchestrationV2ThreadRuntimeAttachmentResult,
  ScopedThreadRef,
} from "@t3tools/contracts";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import { useMemo } from "react";

import {
  createCurrentRuntimeStopController,
  currentRuntimeStopMenuTarget,
  type CurrentRuntimeStopOperation,
  type CurrentRuntimeStopOutcome,
} from "../components/threadActionMenu.logic";
import {
  clearCurrentRuntimeStopPointer,
  reserveCurrentRuntimeStopPointer,
  type CurrentRuntimeStopPointer,
  useComposerDraftStore,
} from "../composerDraftStore";
import { threadContinuation, threadEnvironment } from "../state/threads";
import { useAtomCommand } from "../state/use-atom-command";
import { useAtomQueryRunner } from "../state/use-atom-query-runner";

export type { CurrentThreadRuntimeStopState } from "@t3tools/client-runtime/state/thread-continuation";
export type { CurrentRuntimeStopOperation, CurrentRuntimeStopOutcome } from "../components/threadActionMenu.logic";

export type CurrentRuntimeStopCapture =
  | { readonly status: "current"; readonly target: OrchestrationV2CurrentThreadRuntimeTarget }
  | { readonly status: "known-stopped"; readonly observedAt: string }
  | { readonly status: "unavailable"; readonly reason: string };

export interface CurrentRuntimeStopPort {
  readonly capture: (ref: ScopedThreadRef) => Promise<CurrentRuntimeStopCapture>;
  readonly request: (
    ref: ScopedThreadRef,
    target: OrchestrationV2CurrentThreadRuntimeTarget,
  ) => Promise<CurrentRuntimeStopOutcome>;
  readonly observe: (
    ref: ScopedThreadRef,
    expectedOperation?: CurrentRuntimeStopOperation,
  ) => Promise<CurrentRuntimeStopOutcome | null>;
}

function snapshotTarget(target: OrchestrationV2CurrentThreadRuntimeTarget): OrchestrationV2CurrentThreadRuntimeTarget {
  return Object.freeze({ ...target, binding: Object.freeze({ ...target.binding }) });
}

function snapshotOutcome(outcome: CurrentRuntimeStopOutcome): CurrentRuntimeStopOutcome {
  return Object.freeze({ ...outcome, target: outcome.target === null ? null : snapshotTarget(outcome.target) });
}

function sameTarget(
  a: OrchestrationV2CurrentThreadRuntimeTarget,
  b: OrchestrationV2CurrentThreadRuntimeTarget,
): boolean {
  return a.driver === b.driver && a.evidenceRevision === b.evidenceRevision &&
    a.binding.threadId === b.binding.threadId &&
    a.binding.providerThreadId === b.binding.providerThreadId &&
    a.binding.providerSessionId === b.binding.providerSessionId &&
    a.binding.instanceId === b.binding.instanceId &&
    a.binding.runtimeGeneration === b.binding.runtimeGeneration &&
    a.binding.nativeThreadId === b.binding.nativeThreadId;
}

function unknown(reason: string): CurrentRuntimeStopOutcome {
  return {
    status: "unknown", commandAccepted: false, queueFenceInstalled: false, reason,
    commandId: null, target: null,
  };
}

function errorReason(error: unknown): string {
  return error instanceof Error ? error.message : "Runtime stop information is unavailable.";
}

export function createCurrentRuntimeStopPort(options: {
  readonly readPointer: (ref: ScopedThreadRef) => CurrentRuntimeStopPointer | null;
  readonly readAttachment: (ref: ScopedThreadRef) => Promise<OrchestrationV2ThreadRuntimeAttachmentResult>;
  readonly reserve: Parameters<typeof createCurrentRuntimeStopController>[0]["reserve"];
  readonly clear: Parameters<typeof createCurrentRuntimeStopController>[0]["clear"];
  readonly stop: Parameters<typeof createCurrentRuntimeStopController>[0]["stop"];
  readonly observe: Parameters<typeof createCurrentRuntimeStopController>[0]["observe"];
}): CurrentRuntimeStopPort {
  const read = (ref: ScopedThreadRef): OrchestrationV2StopCurrentThreadRuntimeInput | null => {
    const pointer = options.readPointer(ref);
    if (pointer === null) return null;
    if (pointer.environmentId !== ref.environmentId || pointer.threadId !== ref.threadId ||
      pointer.target.binding.threadId !== ref.threadId) {
      throw new Error("The saved runtime stop belongs to another environment or thread.");
    }
    return Object.freeze({ commandId: pointer.commandId, threadId: pointer.threadId, target: snapshotTarget(pointer.target) });
  };
  const controller = createCurrentRuntimeStopController({ ...options, read });

  return {
    capture: async (ref) => {
      try {
        const saved = read(ref);
        const result = await options.readAttachment(ref);
        if (result.threadId !== ref.threadId) {
          return { status: "unavailable", reason: "The attachment read belongs to another thread." };
        }
        const latest = read(ref);
        if (saved?.commandId !== latest?.commandId ||
          (saved !== null && latest !== null && !sameTarget(saved.target, latest.target))) {
          return { status: "unavailable", reason: "The saved stop changed while reading the current runtime." };
        }
        if (result.attachment.status !== "attached") {
          // Nonresidency does not attest a completed native stop.
          return { status: "unavailable", reason: result.attachment.reason };
        }
        const target = currentRuntimeStopMenuTarget(result, ref.threadId);
        if (target === null) {
          return { status: "unavailable", reason: "Current runtime stop is unavailable in this environment." };
        }
        if (saved !== null && !sameTarget(saved.target, target)) {
          return { status: "unavailable", reason: "Observe the saved stop before acting on a different runtime." };
        }
        return { status: "current", target };
      } catch (error) {
        return { status: "unavailable", reason: errorReason(error) };
      }
    },
    request: async (ref, target) => {
      try {
        const saved = read(ref);
        if (target.binding.threadId !== ref.threadId ||
          (saved !== null && !sameTarget(saved.target, target))) {
          return unknown("The requested target does not match the saved current-runtime stop.");
        }
        return snapshotOutcome(await controller(ref, snapshotTarget(target), saved));
      } catch (error) {
        return unknown(errorReason(error));
      }
    },
    observe: async (ref, expectedOperation) => {
      try {
        const saved = read(ref);
        if (saved === null) {
          return expectedOperation === undefined
            ? null
            : unknown("The original saved runtime stop is unavailable. No other operation was observed.");
        }
        if (expectedOperation !== undefined &&
          (saved.commandId !== expectedOperation.commandId || !sameTarget(saved.target, expectedOperation.target))) {
          return unknown("The saved runtime stop no longer matches the original operation. No other operation was observed.");
        }
        const outcome = await controller.observe(ref, saved);
        return outcome === null ? null : snapshotOutcome(outcome);
      } catch (error) {
        return unknown(errorReason(error));
      }
    },
  };
}

export function useCurrentRuntimeStop(): CurrentRuntimeStopPort {
  const readAttachment = useAtomQueryRunner(threadContinuation.runtimeAttachment, {
    refresh: true, reportFailure: false,
  });
  const stop = useAtomCommand(threadEnvironment.stopCurrentThreadRuntime, { reportFailure: false });
  const observe = useAtomCommand(threadEnvironment.observeCurrentThreadRuntimeStop, { reportFailure: false });

  return useMemo(() => createCurrentRuntimeStopPort({
    readPointer: (ref) => useComposerDraftStore.getState().getComposerDraft(ref)?.currentRuntimeStop ?? null,
    readAttachment: async (ref) => {
      const result = await readAttachment({ environmentId: ref.environmentId, input: { threadId: ref.threadId } });
      if (result._tag !== "Success") throw squashAtomCommandFailure(result);
      return result.value;
    },
    reserve: (ref, input) => reserveCurrentRuntimeStopPointer({ environmentId: ref.environmentId, ...input }),
    clear: (ref, input) => clearCurrentRuntimeStopPointer({ environmentId: ref.environmentId, ...input }),
    stop: async (ref, input) => {
      const result = await stop({ environmentId: ref.environmentId, input });
      if (result._tag !== "Success") throw squashAtomCommandFailure(result);
      return result.value;
    },
    observe: async (ref, input) => {
      const result = await observe({ environmentId: ref.environmentId, input });
      if (result._tag !== "Success") throw squashAtomCommandFailure(result);
      return result.value;
    },
  }), [readAttachment, stop, observe]);
}
