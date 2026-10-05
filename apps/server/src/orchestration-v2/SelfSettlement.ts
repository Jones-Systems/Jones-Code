import type {
  OrchestrationV2AppThread,
  OrchestrationV2Run,
  OrchestrationV2ServerCommand,
  OrchestrationV2ThreadProjection,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunId,
} from "@t3tools/contracts";

type SettlementProjection = Pick<
  OrchestrationV2ThreadProjection,
  "thread" | "runs" | "messages" | "runtimeRequests" | "attempts"
>;
type Caller = {
  readonly providerSessionId: string;
  readonly providerInstanceId: ProviderInstanceId;
};
type Owner = {
  readonly providerSessionId: ProviderSessionId;
  readonly instanceId: ProviderInstanceId;
  readonly providerThreadId: ProviderThreadId;
};

function blocksSettlement(projection: SettlementProjection, exceptRunId: RunId): boolean {
  const automatic = new Set(
    projection.messages
      .filter(
        (message) =>
          message.notification !== undefined || message.delegatedCompletion !== undefined,
      )
      .map((message) => message.id),
  );
  return (
    projection.runs.some(
      (run) =>
        run.id !== exceptRunId &&
        ["preparing", "starting", "running", "waiting", "queued"].includes(run.status) &&
        !(run.status === "queued" && automatic.has(run.userMessageId)),
    ) ||
    projection.runtimeRequests.some(
      (request) =>
        request.status === "pending" &&
        (request.kind !== "user_input" || request.responseCapability.type !== "message"),
    )
  );
}

export function selfSettlementRun(
  projection: SettlementProjection,
  caller: Caller,
  owner: Owner | null,
): OrchestrationV2Run | undefined {
  const run = projection.runs
    .toSorted((a, b) => b.ordinal - a.ordinal)
    .find((candidate) => ["starting", "running"].includes(candidate.status));
  if (
    projection.thread.deletedAt !== null ||
    projection.thread.archivedAt !== null ||
    owner === null ||
    run === undefined ||
    run.activeAttemptId === null ||
    owner.providerSessionId !== caller.providerSessionId ||
    owner.instanceId !== caller.providerInstanceId ||
    owner.providerThreadId !== projection.thread.activeProviderThreadId ||
    run.providerThreadId !== owner.providerThreadId ||
    run.providerInstanceId !== caller.providerInstanceId ||
    !projection.attempts.some(
      (attempt) =>
        attempt.id === run.activeAttemptId &&
        attempt.runId === run.id &&
        attempt.providerThreadId === owner.providerThreadId &&
        attempt.providerInstanceId === owner.instanceId &&
        (attempt.status === "pending" || attempt.status === "running"),
    ) ||
    blocksSettlement(projection, run.id)
  )
    return undefined;
  return run;
}

export function selfSettlementTerminalDisposition(
  projection: SettlementProjection,
  terminalRunId: RunId,
): "ignore" | "cancel" | "settle" {
  const intent = projection.thread.selfSettlement;
  if (intent == null) return "ignore";
  const run = projection.runs.find((candidate) => candidate.id === intent.runId);
  if (
    run === undefined ||
    projection.runs.some(
      (candidate) =>
        candidate.ordinal > run.ordinal &&
        !(
          candidate.status === "queued" &&
          projection.messages.some(
            (message) =>
              message.id === candidate.userMessageId &&
              (message.notification !== undefined || message.delegatedCompletion !== undefined),
          )
        ),
    )
  )
    return "cancel";
  if (terminalRunId !== intent.runId) return "ignore";
  if (["starting", "running", "waiting"].includes(run.status)) return "ignore";
  if (
    run.status !== "completed" ||
    projection.thread.deletedAt !== null ||
    projection.thread.archivedAt !== null ||
    blocksSettlement(projection, run.id)
  )
    return "cancel";
  return "settle";
}

export function cancelsSelfSettlement(command: OrchestrationV2ServerCommand): boolean {
  if (command.type === "message.dispatch")
    return command.notification === undefined && command.delegatedCompletion === undefined;
  return [
    "thread.settle",
    "thread.unsettle",
    "thread.pin",
    "thread.unpin",
    "thread.snooze",
    "thread.unsnooze",
    "thread.auto-settle.set",
    "thread.archive",
    "thread.unarchive",
    "thread.delete",
    "run.interrupt",
    "checkpoint.rollback",
    "queued-message.promote-to-steer",
    "queued-run.edit",
    "queue.resume",
    "provider.switch",
    "provider-session.detach",
    "thread.model-selection.set",
    "runtime-request.respond",
  ].includes(command.type);
}

export type SelfSettlementIntent = NonNullable<OrchestrationV2AppThread["selfSettlement"]>;
