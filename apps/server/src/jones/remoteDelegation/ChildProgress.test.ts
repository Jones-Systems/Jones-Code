import { expect, it } from "@effect/vitest";
import {
  MessageId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { hasPendingChildRuns, latestTerminalResultRun } from "./ChildProgress.ts";

const now = DateTime.makeUnsafe("2026-10-22T00:00:00.000Z");
const threadId = ThreadId.make("child");
const instanceId = ProviderInstanceId.make("codex");
const run = (ordinal: number, changes: Partial<OrchestrationV2Run> = {}): OrchestrationV2Run => ({
  id: RunId.make(`run-${ordinal}`),
  threadId,
  ordinal,
  providerInstanceId: instanceId,
  modelSelection: { instanceId, model: "gpt-6" },
  providerThreadId: null,
  userMessageId: MessageId.make(`message-${ordinal}`),
  rootNodeId: null,
  activeAttemptId: null,
  status: "completed",
  requestedAt: now,
  startedAt: now,
  completedAt: now,
  checkpointId: null,
  contextHandoffId: null,
  ...changes,
});
const original = run(1);

it("reports a failed follow-up startup even without a provider start", () => {
  const failed = run(2, { status: "failed", startedAt: null });
  expect(latestTerminalResultRun({ runs: [original, failed], messages: [] }, original)).toBe(
    failed,
  );
});

it.each(["starting", "running", "waiting", "queued"] as const)(
  "keeps a later unheld %s run pending",
  (status) => {
    expect(hasPendingChildRuns({ runs: [original, run(2, { status })] }, original)).toBe(true);
  },
);

it("ignores held queues, terminal runs and the original or earlier run for pending work", () => {
  expect(
    hasPendingChildRuns(
      {
        runs: [
          run(0, { status: "running" }),
          run(1, { status: "running" }),
          run(2, { status: "queued", queueHeld: true }),
          run(3, { status: "failed" }),
        ],
      },
      original,
    ),
  ).toBe(false);
  expect(hasPendingChildRuns({ runs: [run(1, { status: "running" })] }, undefined)).toBe(true);
});

it("excludes monitor, rolled-back and never-started cancelled follow-ups", () => {
  const monitor = run(2);
  const message: OrchestrationV2ConversationMessage = {
    id: MessageId.make("monitor"),
    threadId,
    runId: monitor.id,
    nodeId: null,
    role: "user",
    text: "Monitor update",
    attachments: [],
    streaming: false,
    createdAt: now,
    updatedAt: now,
    createdBy: "system",
    creationSource: "server",
    notification: { source: { kind: "monitor" }, outcome: "completed", summary: "Monitor update" },
  };
  expect(
    latestTerminalResultRun(
      {
        runs: [
          original,
          monitor,
          run(3, { status: "rolled_back" }),
          run(4, { status: "cancelled", startedAt: null }),
          run(5, { status: "running" }),
        ],
        messages: [message],
      },
      original,
    ),
  ).toBe(original);
});

it("includes the original terminal run without a start", () => {
  const cancelled = run(1, { status: "cancelled", startedAt: null });
  expect(latestTerminalResultRun({ runs: [cancelled], messages: [] }, cancelled)).toBe(cancelled);
});

it("orders results by completion time before submission ordinal and breaks ties by ordinal", () => {
  const finishedLater = run(2, {
    completedAt: DateTime.makeUnsafe("2026-10-22T00:01:00.000Z"),
  });
  const submittedLater = run(3);
  expect(
    latestTerminalResultRun(
      { runs: [submittedLater, finishedLater, original], messages: [] },
      original,
    ),
  ).toBe(finishedLater);
  expect(
    latestTerminalResultRun({ runs: [submittedLater, original], messages: [] }, original),
  ).toBe(submittedLater);
  expect(latestTerminalResultRun({ runs: [], messages: [] }, undefined)).toBeUndefined();
});

it("does not report an earlier task's result when observing a later original run", () => {
  const earlier = run(1);
  const current = run(2, { status: "running", completedAt: null });
  expect(
    latestTerminalResultRun({ runs: [earlier, current], messages: [] }, current),
  ).toBeUndefined();
  expect(latestTerminalResultRun({ runs: [earlier], messages: [] }, undefined)).toBe(earlier);
});
