import { describe, expect, it } from "vite-plus/test";
import {
  CommandId,
  ThreadId,
  RunId,
  NodeId,
  ProviderInstanceId,
  RuntimeRequestId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import {
  buildUserInputRequestRecord,
  type UserInputRequestRecordCommand,
} from "./userInputRequestRecord.ts";

const now = DateTime.makeUnsafe("2026-10-20T12:00:00Z");
const command: UserInputRequestRecordCommand = {
  type: "user_input_request.record",
  commandId: CommandId.make("record"),
  threadId: ThreadId.make("thread"),
  runId: RunId.make("run"),
  rootNodeId: NodeId.make("root"),
  providerInstanceId: ProviderInstanceId.make("claude"),
  requestId: RuntimeRequestId.make("request"),
  requestNodeId: NodeId.make("request-node"),
  turnItemId: TurnItemId.make("request-item"),
  question: {
    id: "response",
    header: "Action needed",
    question: "Review the plan and reply with changes.",
    options: [],
    required: true,
    allowCustomAnswer: true,
  },
};
const projection = {
  thread: { id: command.threadId, deletedAt: null, archivedAt: null },
  runs: [
    {
      id: command.runId,
      ordinal: 1,
      status: "running" as const,
      rootNodeId: command.rootNodeId,
      providerInstanceId: command.providerInstanceId,
      providerThreadId: null,
    },
  ],
  nodes: [{ id: command.rootNodeId, runId: command.runId, kind: "root_turn" as const }],
  turnItems: [],
  runtimeRequests: [],
};
const input = { command, projection, now, ordinal: 3, providerTurnId: null };
describe("durable user input record", () => {
  it("builds a nonblocking durable open-response request", () => {
    const result = buildUserInputRequestRecord(input);
    expect(result).toMatchObject({
      node: { status: "waiting", countsForRun: false, parentNodeId: command.rootNodeId },
      item: {
        type: "user_input_request",
        responseMode: "message",
        questions: [command.question],
        ordinal: 3,
        nativeItemRef: null,
      },
      request: {
        status: "pending",
        kind: "user_input",
        responseCapability: { type: "message" },
        nativeRequestRef: null,
      },
    });
  });
  it("refuses terminal, newer-run, root, provider, archive, deletion and identifier collisions", () => {
    const run = projection.runs[0]!;
    const variants: Array<typeof input> = [
      { ...input, command: { ...command, providerInstanceId: ProviderInstanceId.make("foreign") } },
      { ...input, command: { ...command, rootNodeId: NodeId.make("wrong") } },
    ];
    for (const variant of variants)
      expect(buildUserInputRequestRecord(variant)).toHaveProperty("error");
    for (const changed of [
      { ...projection, runs: [{ ...run, status: "completed" as const }] },
      { ...projection, runs: [...projection.runs, { ...run, id: RunId.make("new"), ordinal: 2 }] },
      { ...projection, nodes: [] },
      { ...projection, thread: { ...projection.thread, archivedAt: now } },
      { ...projection, thread: { ...projection.thread, deletedAt: now } },
      {
        ...projection,
        nodes: [
          ...projection.nodes,
          { id: command.requestNodeId, runId: command.runId, kind: "root_turn" as const },
        ],
      },
      { ...projection, runtimeRequests: [{ id: command.requestId }] },
      { ...projection, turnItems: [{ id: command.turnItemId }] },
    ])
      expect(buildUserInputRequestRecord({ ...input, projection: changed })).toHaveProperty(
        "error",
      );
  });
});
