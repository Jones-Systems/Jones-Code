import { expect, it } from "@effect/vitest";
import {
  NodeId,
  type OrchestrationV2Run,
  type OrchestrationV2RuntimeRequest,
  ProviderSessionId,
  RunId,
  RuntimeRequestId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { blockingQuestions, questionsChanged } from "./BlockingQuestions.ts";

const nodeId = NodeId.make("node:child");
const runId = RunId.make("run:child");
const request: OrchestrationV2RuntimeRequest = {
  id: RuntimeRequestId.make("request:child"),
  nodeId,
  providerTurnId: null,
  nativeRequestRef: null,
  kind: "user_input",
  status: "pending",
  responseCapability: { type: "live", providerSessionId: ProviderSessionId.make("session:child") },
  createdAt: DateTime.makeUnsafe(0),
  resolvedAt: null,
};
const records = (requests: ReadonlyArray<OrchestrationV2RuntimeRequest>) => ({
  runtimeRequests: requests,
  nodes: [],
  runs: [{ id: runId, rootNodeId: nodeId } as OrchestrationV2Run],
});

it("observes only pending, answerable user input with exact remote run attribution", () => {
  const result = blockingQuestions(
    records([
      request,
      { ...request, id: RuntimeRequestId.make("resolved"), status: "resolved" },
      {
        ...request,
        id: RuntimeRequestId.make("not-resumable"),
        responseCapability: { type: "not_resumable", reason: "Ended" },
      },
      { ...request, id: RuntimeRequestId.make("message"), responseCapability: { type: "message" } },
      { ...request, id: RuntimeRequestId.make("tool"), kind: "dynamic_tool_call" },
    ]),
  );
  expect(result?.requests).toEqual([{ requestId: request.id, runId }]);
  expect(result && questionsChanged(result, undefined)).toBe(true);
  expect(result && questionsChanged(result, result.resumeToken)).toBe(false);
  const cleared = blockingQuestions(records([]))!;
  expect(questionsChanged(cleared, result?.resumeToken)).toBe(true);
  expect(questionsChanged(cleared, undefined)).toBe(false);
});

it("keeps an unchanged request set stable across projection order and detects a replacement", () => {
  const second = { ...request, id: RuntimeRequestId.make("request:second") };
  const first = blockingQuestions(records([request, second]))!;
  const reordered = blockingQuestions(records([second, request]))!;
  expect(reordered.resumeToken).toBe(first.resumeToken);
  const changed = blockingQuestions(records([second]))!;
  expect(questionsChanged(changed, first.resumeToken)).toBe(true);
});

it("refuses an unattributed or overflowing question set instead of reporting no questions", () => {
  expect(blockingQuestions({ ...records([request]), runs: [] })).toBeUndefined();
  expect(
    blockingQuestions(
      records(
        Array.from({ length: 65 }, (_, index) => ({
          ...request,
          id: RuntimeRequestId.make(`request:${index}`),
        })),
      ),
    ),
  ).toBeUndefined();
});
