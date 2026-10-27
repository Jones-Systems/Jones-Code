import { CommandId, NodeId, RuntimeRequestId, TurnItemId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import { makeThreadProjectionFixture } from "../../test-fixtures";
const v2Projection = makeThreadProjectionFixture();
const v2Now = v2Projection.updatedAt;
import { normalizeMonitorAnswers } from "./answers";
import { refOf } from "./store";
import { testRow } from "./fixtures.test-support";
const requestId = RuntimeRequestId.make("actual-request");
const questions = [
  {
    id: "actual-single",
    header: "Single",
    question: "Choose",
    options: [{ label: "Label", description: "Actual option", value: "  exact-provider-value  " }],
    multiSelect: false,
    allowCustomAnswer: false,
  },
  {
    id: "actual-multi",
    header: "Multiple",
    question: "Choose several",
    options: [
      { label: "A", description: "First", value: "value-a" },
      { label: "B", description: "Second", value: "value-b" },
    ],
    multiSelect: true,
    allowCustomAnswer: false,
  },
  {
    id: "actual-custom",
    header: "Text",
    question: "Explain",
    options: [],
    multiSelect: false,
    allowCustomAnswer: true,
  },
];
const projection = {
  ...v2Projection,
  runtimeRequests: [
    {
      id: requestId,
      nodeId: NodeId.make("node"),
      providerTurnId: null,
      nativeRequestRef: null,
      kind: "user_input" as const,
      status: "pending" as const,
      responseCapability: { type: "message" as const },
      createdAt: v2Now,
      resolvedAt: null,
    },
  ],
  turnItems: [
    {
      id: TurnItemId.make("question-item"),
      threadId: v2Projection.thread.id,
      nodeId: NodeId.make("node"),
      runId: null,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: 1,
      status: "completed" as const,
      title: null,
      startedAt: v2Now,
      completedAt: v2Now,
      updatedAt: v2Now,
      type: "user_input_request" as const,
      requestId,
      questions,
    },
  ],
};
const action = {
  ...refOf(testRow()),
  type: "respondToUserInput" as const,
  requestId,
  actionId: "action",
  commandId: CommandId.make("command"),
  draftRevision: 2,
  answers: {
    "actual-single": "  exact-provider-value  ",
    "actual-multi": ["value-a", "value-b"],
    "actual-custom": "  custom answer  ",
  },
};
describe("monitor uses current input normalization", () => {
  it("preserves provider option values, complete multi-question answers and custom normalization", () => {
    expect(normalizeMonitorAnswers(projection, action)).toEqual({
      "actual-single": "  exact-provider-value  ",
      "actual-multi": ["value-a", "value-b"],
      "actual-custom": "custom answer",
    });
  });
  it("refuses invented options, incomplete answers, obsolete requests and non-resumable capability", () => {
    expect(
      normalizeMonitorAnswers(projection, {
        ...action,
        answers: { ...action.answers, "actual-multi": ["invented"] },
      }),
    ).toBeNull();
    expect(
      normalizeMonitorAnswers(projection, {
        ...action,
        answers: { "actual-single": "  exact-provider-value  " },
      }),
    ).toBeNull();
    expect(
      normalizeMonitorAnswers(projection, {
        ...action,
        requestId: RuntimeRequestId.make("next-request"),
      }),
    ).toBeNull();
    expect(
      normalizeMonitorAnswers(
        {
          ...projection,
          runtimeRequests: projection.runtimeRequests.map((request) => ({
            ...request,
            responseCapability: { type: "not_resumable", reason: "provider gone" },
          })),
        },
        action,
      ),
    ).toBeNull();
  });
});
