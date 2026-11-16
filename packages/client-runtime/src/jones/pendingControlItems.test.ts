import {
  NodeId,
  OrchestrationV2ThreadProjection,
  ProviderSessionId,
  RuntimeRequestId,
  TurnItemId,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { expect, it } from "vite-plus/test";
import { v2Now, v2Projection } from "../state/orchestrationV2TestFixtures.ts";
import { derivePendingThreadRequests } from "../state/threadRequests.ts";

it("derives retained native questions, completed async questions and approvals after wire restore", () => {
  const request = (
    id: string,
    kind: "user_input" | "command",
    message = false,
  ): OrchestrationV2RuntimeRequest => ({
    id: RuntimeRequestId.make(id),
    nodeId: NodeId.make(`node:${id}`),
    providerTurnId: null,
    nativeRequestRef: null,
    kind,
    status: "pending",
    responseCapability: message
      ? { type: "message" }
      : { type: "live", providerSessionId: ProviderSessionId.make("session:control") },
    createdAt: v2Now,
    resolvedAt: null,
  });
  const requests = [
    request("native", "user_input"),
    request("async", "user_input", true),
    request("approval", "command"),
  ];
  const items: OrchestrationV2TurnItem[] = requests.map((row, ordinal) => {
    const message = row.responseCapability.type === "message";
    const base = {
      id: TurnItemId.make(`item:${row.id}`),
      threadId: v2Projection.thread.id,
      runId: null,
      nodeId: row.nodeId,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal,
      status: message ? ("completed" as const) : ("waiting" as const),
      title: null,
      startedAt: v2Now,
      completedAt: message ? v2Now : null,
      updatedAt: v2Now,
      requestId: row.id,
    };
    return row.kind === "command"
      ? {
          ...base,
          type: "approval_request",
          requestKind: "command",
          prompt: "Run the focused check?",
          options: [
            { label: "Run once", decision: "accept" },
            { label: "Decline", decision: "decline" },
          ],
        }
      : {
          ...base,
          type: "user_input_request",
          ...(message ? { responseMode: "message" as const } : {}),
          questions: [
            {
              id: "choice",
              header: "Choice",
              question: `Answer ${row.id}?`,
              required: true,
              options: [{ label: "Continue", value: "continue", description: "Continue the task" }],
            },
          ],
        };
  });
  const codec = Schema.fromJsonString(Schema.toCodecJson(OrchestrationV2ThreadProjection));
  const restored = Schema.decodeSync(codec)(
    Schema.encodeSync(codec)({
      ...v2Projection,
      runtimeRequests: requests,
      turnItems: items,
      visibleTurnItems: [],
    }),
  );
  const pending = derivePendingThreadRequests(restored);
  expect(
    pending.userInputs.map(({ requestId, responseCapability, dismissible, questions }) => [
      requestId,
      responseCapability,
      dismissible,
      questions[0]?.question,
    ]),
  ).toEqual([
    ["native", "live", false, "Answer native?"],
    ["async", "message", true, "Answer async?"],
  ]);
  expect(pending.approvals[0]).toMatchObject({
    requestId: "approval",
    detail: "Run the focused check?",
    options: [
      { label: "Run once", decision: "accept" },
      { label: "Decline", decision: "decline" },
    ],
  });
  const ended = {
    ...restored,
    runtimeRequests: restored.runtimeRequests.map((row) =>
      row.kind === "user_input"
        ? {
            ...row,
            status: row.id === "native" ? ("resolved" as const) : ("cancelled" as const),
            resolvedAt: v2Now,
          }
        : row,
    ),
  };
  expect(derivePendingThreadRequests(ended).userInputs).toEqual([]);
  expect(derivePendingThreadRequests(ended).approvals).toEqual(pending.approvals);
  expect(derivePendingThreadRequests({ ...restored, turnItems: [] }).userInputs).toEqual([]);
});
