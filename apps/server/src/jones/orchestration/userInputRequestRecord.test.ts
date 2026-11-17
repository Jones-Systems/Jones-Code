import { describe, expect, it } from "vite-plus/test";
import {
  CommandId,
  ThreadId,
  RunId,
  NodeId,
  ProviderInstanceId,
  type OrchestrationV2RunStatus,
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
      startedAt: now as DateTime.Utc | null,
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
  it("records batches and rejects empty or duplicate question identifiers", () => {
    const questions = [command.question, { ...command.question, id: "second" }];
    expect(
      buildUserInputRequestRecord({ ...input, command: { ...command, questions } }),
    ).toMatchObject({ item: { questions } });
    for (const invalid of [
      [],
      [command.question, command.question],
      [{ ...command.question, id: " " }],
    ])
      expect(
        buildUserInputRequestRecord({ ...input, command: { ...command, questions: invalid } }),
      ).toHaveProperty("error");
  });
  it("later queued runs cancelled by steering do not revoke the active run", () => {
    const run = projection.runs[0]!;
    const later = (ordinal: number, status: OrchestrationV2RunStatus) => ({
      ...run,
      id: RunId.make(`later-${ordinal}`),
      ordinal,
      status,
      startedAt: null as DateTime.Utc | null,
    });
    for (const runs of [
      [run, later(2, "cancelled"), later(3, "cancelled")],
      [run, later(2, "queued"), later(3, "preparing"), later(4, "cancelled")],
      [run, later(2, "failed"), later(3, "interrupted")],
    ])
      expect(
        buildUserInputRequestRecord({ ...input, projection: { ...projection, runs } }),
      ).toHaveProperty("node");
  });
  it("a later run that started still revokes", () => {
    const run = projection.runs[0]!;
    const later = { ...run, id: RunId.make("later"), ordinal: 2 };
    for (const next of [
      { ...later, status: "completed" as const, startedAt: now },
      { ...later, status: "interrupted" as const, startedAt: now },
      { ...later, status: "cancelled" as const, startedAt: now },
      { ...later, status: "starting" as const, startedAt: null },
      { ...later, status: "running" as const, startedAt: now },
      { ...later, status: "waiting" as const, startedAt: now },
    ])
      expect(
        buildUserInputRequestRecord({
          ...input,
          projection: { ...projection, runs: [run, next] },
        }),
      ).toHaveProperty("error");
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
