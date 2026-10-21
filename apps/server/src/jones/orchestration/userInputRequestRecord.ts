import type {
  OrchestrationV2InternalCommand,
  OrchestrationV2ExecutionNode,
  OrchestrationV2RuntimeRequest,
  OrchestrationV2TurnItem,
  ProviderTurnId,
} from "@t3tools/contracts";
import type * as DateTime from "effect/DateTime";
import type { RecordedThreadProjection } from "../../orchestration-v2/RecordedTypes.ts";

export type UserInputRequestRecordCommand = Extract<
  OrchestrationV2InternalCommand,
  { readonly type: "user_input_request.record" }
>;

type Projection = {
  readonly thread: Pick<RecordedThreadProjection["thread"], "id" | "deletedAt" | "archivedAt">;
  readonly runs: ReadonlyArray<
    Pick<
      RecordedThreadProjection["runs"][number],
      "id" | "ordinal" | "status" | "rootNodeId" | "providerInstanceId" | "providerThreadId"
    >
  >;
  readonly nodes: ReadonlyArray<Pick<OrchestrationV2ExecutionNode, "id" | "runId" | "kind">>;
  readonly turnItems: ReadonlyArray<Pick<OrchestrationV2TurnItem, "id">>;
  readonly runtimeRequests: ReadonlyArray<Pick<OrchestrationV2RuntimeRequest, "id">>;
};

export function buildUserInputRequestRecord(input: {
  readonly command: UserInputRequestRecordCommand;
  readonly projection: Projection;
  readonly now: DateTime.Utc;
  readonly ordinal: number;
  readonly providerTurnId: ProviderTurnId | null;
}):
  | { readonly error: string }
  | {
      readonly node: OrchestrationV2ExecutionNode;
      readonly item: OrchestrationV2TurnItem;
      readonly request: OrchestrationV2RuntimeRequest;
    } {
  const { command, projection, now, providerTurnId } = input;
  const run = projection.runs.find((candidate) => candidate.id === command.runId);
  const root = projection.nodes.find((node) => node.id === command.rootNodeId);
  if (
    projection.thread.deletedAt !== null ||
    projection.thread.archivedAt !== null ||
    projection.thread.id !== command.threadId ||
    run === undefined ||
    !["starting", "running", "waiting"].includes(run.status) ||
    projection.runs.some(
      (candidate) =>
        candidate.ordinal > run.ordinal && !["queued", "preparing"].includes(candidate.status),
    ) ||
    run.rootNodeId !== command.rootNodeId ||
    run.providerInstanceId !== command.providerInstanceId ||
    root === undefined ||
    root.runId !== run.id ||
    root.kind !== "root_turn"
  )
    return { error: "The calling provider no longer owns this active run and root." };
  if (
    projection.runtimeRequests.some((request) => request.id === command.requestId) ||
    projection.nodes.some((node) => node.id === command.requestNodeId) ||
    projection.turnItems.some((item) => item.id === command.turnItemId)
  )
    return { error: "The response request identifiers are already in use." };
  return {
    node: {
      id: command.requestNodeId,
      threadId: command.threadId,
      runId: run.id,
      parentNodeId: root.id,
      rootNodeId: root.id,
      kind: "user_input_request",
      status: "waiting",
      countsForRun: false,
      providerThreadId: run.providerThreadId,
      providerTurnId,
      nativeItemRef: null,
      runtimeRequestId: command.requestId,
      checkpointScopeId: null,
      startedAt: now,
      completedAt: null,
    },
    item: {
      id: command.turnItemId,
      threadId: command.threadId,
      runId: run.id,
      nodeId: command.requestNodeId,
      providerThreadId: run.providerThreadId,
      providerTurnId,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: input.ordinal,
      status: "waiting",
      title: command.question.header,
      startedAt: now,
      completedAt: null,
      updatedAt: now,
      type: "user_input_request",
      requestId: command.requestId,
      questions: [command.question],
      responseMode: "message",
    },
    request: {
      id: command.requestId,
      nodeId: command.requestNodeId,
      providerTurnId,
      nativeRequestRef: null,
      kind: "user_input",
      status: "pending",
      responseCapability: { type: "message" },
      createdAt: now,
      resolvedAt: null,
    },
  };
}
