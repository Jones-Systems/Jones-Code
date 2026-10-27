import type {
  ModelSelection,
  OrchestrationV2ProviderThread,
  OrchestrationV2RunAttempt,
  ProviderSessionId,
  ThreadId,
} from "@t3tools/contracts";
import { modelSelectionsEqual } from "@t3tools/shared/model";
import type * as DateTime from "effect/DateTime";
import type { RecordedRun, RecordedLifecycleEvent } from "../../orchestration-v2/RecordedTypes.ts";

interface SelectionCommand {
  readonly type: string;
  readonly threadId?: ThreadId | undefined;
  readonly modelSelection?: ModelSelection | undefined;
  readonly modelSelectionExplicit?: boolean | undefined;
  readonly createdBy?: string | undefined;
  readonly restartContinuationOfRunId?: unknown;
  readonly manualContinuationOfRunId?: unknown;
  readonly usageLimitContinuationOfRunId?: unknown;
  readonly notification?: unknown;
  readonly delegatedCompletion?: unknown;
}

type SelectionEvent = {
  readonly type: string;
  readonly threadId?: ThreadId | undefined;
  readonly payload: unknown;
};
const selectionEvent = (event: SelectionEvent) =>
  event.type === "thread.model-selection-updated" || event.type === "thread.provider-switched";

export function isOwnerSelectionMessage(command: SelectionCommand, ingress: boolean): boolean {
  return (
    ingress &&
    command.type === "message.dispatch" &&
    command.createdBy === "user" &&
    command.restartContinuationOfRunId === undefined &&
    command.manualContinuationOfRunId === undefined &&
    command.usageLimitContinuationOfRunId === undefined &&
    command.notification === undefined &&
    command.delegatedCompletion === undefined
  );
}

export function isOwnerSelectionExplicit(
  command: SelectionCommand,
  ingress: boolean,
  saved: ModelSelection,
): boolean {
  if (!ingress || command.modelSelection === undefined) return false;
  if (command.type === "thread.model-selection.set" || command.type === "provider.switch")
    return true;
  // Legacy clients cannot distinguish an intentional equal pick from an inherited snapshot.
  return (
    isOwnerSelectionMessage(command, ingress) &&
    (command.modelSelectionExplicit === true ||
      (command.modelSelectionExplicit === undefined &&
        !modelSelectionsEqual(saved, command.modelSelection)))
  );
}

/** The decide-time classifier emits a selection event even for an equal explicit pick. */
export function ownerSelectionCommit(
  command: SelectionCommand,
  ingress: boolean,
  events: ReadonlyArray<SelectionEvent>,
): ModelSelection | undefined {
  if (!ingress || command.modelSelection === undefined) return;
  if (command.type === "thread.model-selection.set" || command.type === "provider.switch")
    return command.modelSelection;
  if (
    isOwnerSelectionMessage(command, ingress) &&
    command.modelSelectionExplicit !== false &&
    hasSelectionEvent(events, command.threadId)
  )
    return command.modelSelection;
}

export const hasSelectionEvent = (
  events: ReadonlyArray<SelectionEvent>,
  threadId?: ThreadId,
): boolean =>
  events.some(
    (event) => selectionEvent(event) && (threadId === undefined || event.threadId === threadId),
  );

export function pendingOwnerSelection(
  owner: { readonly modelSelection: ModelSelection } | null,
  saved: ModelSelection,
): ModelSelection | undefined {
  return owner !== null && modelSelectionsEqual(owner.modelSelection, saved) ? saved : undefined;
}

/** Starting is committed admission: its provider effect may already have read the binding. */
export function rebindPreAdmissionRun(
  run: RecordedRun,
  attempt: OrchestrationV2RunAttempt | undefined,
  selection: ModelSelection,
  now: DateTime.Utc,
): ReadonlyArray<Omit<RecordedLifecycleEvent, "id">> {
  if (
    (run.status !== "queued" && run.status !== "preparing") ||
    (modelSelectionsEqual(run.modelSelection, selection) &&
      run.providerInstanceId === selection.instanceId &&
      (attempt === undefined || attempt.providerInstanceId === selection.instanceId))
  )
    return [];
  const common = {
    threadId: run.threadId,
    runId: run.id,
    providerInstanceId: selection.instanceId,
    occurredAt: now,
  };
  return [
    {
      ...common,
      type: "run.updated",
      payload: { ...run, modelSelection: selection, providerInstanceId: selection.instanceId },
    },
    ...(attempt === undefined || attempt.providerInstanceId === selection.instanceId
      ? []
      : [
          {
            ...common,
            type: "run-attempt.updated" as const,
            nodeId: attempt.rootNodeId,
            payload: { ...attempt, providerInstanceId: selection.instanceId },
          },
        ]),
  ];
}

export function admittedSessionIds(
  runs: ReadonlyArray<RecordedRun>,
  providerThreads: ReadonlyArray<Pick<OrchestrationV2ProviderThread, "id" | "providerSessionId">>,
): ReadonlySet<ProviderSessionId> {
  const admitted = new Set(
    runs
      .filter((run) => ["starting", "running", "waiting"].includes(run.status))
      .map((run) => run.providerThreadId),
  );
  return new Set(
    providerThreads.flatMap((thread) =>
      admitted.has(thread.id) && thread.providerSessionId !== null
        ? [thread.providerSessionId]
        : [],
    ),
  );
}

export function ownerSteerSelection(input: {
  readonly ownerMessage: boolean;
  readonly explicit: boolean;
  readonly pending: ModelSelection | undefined;
  readonly requested: ModelSelection;
  readonly active: ModelSelection;
}): { readonly modelSelection: ModelSelection; readonly ownerTransition: boolean } {
  const ownerTransition = input.ownerMessage && (input.explicit || input.pending !== undefined);
  return {
    modelSelection: ownerTransition
      ? input.explicit
        ? input.requested
        : input.pending!
      : input.active,
    ownerTransition,
  };
}

export function ownerPromoteSelection(input: {
  readonly ingress: boolean;
  readonly pending: ModelSelection | undefined;
  readonly queued: ModelSelection;
  readonly active: ModelSelection;
  readonly automaticBoundary?: boolean;
}): { readonly modelSelection: ModelSelection; readonly ownerTransition: boolean } {
  if (input.automaticBoundary) return { modelSelection: input.queued, ownerTransition: false };
  const ownerTransition =
    input.ingress &&
    input.pending !== undefined &&
    modelSelectionsEqual(input.pending, input.queued);
  return { modelSelection: ownerTransition ? input.queued : input.active, ownerTransition };
}
