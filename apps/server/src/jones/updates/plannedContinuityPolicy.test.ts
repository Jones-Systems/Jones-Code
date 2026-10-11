import { expect, it } from "@effect/vitest";
import {
  MessageId,
  RunId,
  RuntimeRequestId,
  NodeId,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { workModeFixture } from "../workMode/Fixtures.testkit.ts";
import {
  canReleasePlannedQueue,
  captureThreadContinuity,
  changesPlannedControl,
  continuationRunIds,
} from "./plannedContinuityPolicy.ts";

function queuedFixture(): OrchestrationV2ThreadProjection {
  const projection = workModeFixture();
  const run = {
    ...projection.runs[0]!,
    id: RunId.make("queued-run"),
    ordinal: 2,
    queuePosition: 1,
    userMessageId: MessageId.make("queued-message"),
    status: "queued" as const,
    startedAt: null,
    completedAt: null,
  };
  return {
    ...projection,
    runs: [...projection.runs, run],
    messages: [
      ...projection.messages,
      {
        ...projection.messages[0]!,
        id: run.userMessageId,
        runId: run.id,
        text: "Captured queued work",
      },
    ],
  };
}
const capture = (projection: OrchestrationV2ThreadProjection) =>
  captureThreadContinuity({
    projection,
    explicitContinuation: true,
    workModeEnabled: false,
    liveWorkOwner: false,
    nowMs: 0,
    receiptRowId: 1,
    receiptCommandId: "capture-anchor",
  });

it("only releases the unchanged queue after its root or exact continuation succeeds", () => {
  const projection = queuedFixture();
  const snapshot = capture(projection)!;
  expect(snapshot.queue).not.toBeNull();
  const held = {
    ...projection,
    runs: projection.runs.map((run) =>
      run.status === "queued" ? { ...run, queueHeld: true } : run,
    ),
  };
  expect(canReleasePlannedQueue(snapshot, held)).toBe(true);
  const root = projection.runs[0]!;
  for (const status of ["running", "waiting", "failed", "cancelled", "interrupted"] as const)
    expect(
      canReleasePlannedQueue(snapshot, { ...held, runs: [{ ...root, status }, held.runs[1]!] }),
    ).toBe(false);
  const continued = {
    ...root,
    id: RunId.make("continued-root"),
    ordinal: 3,
    restartContinuationOfRunId: root.id,
    completedAt: DateTime.makeUnsafe(1),
  };
  const recovered = {
    ...held,
    runs: [{ ...root, status: "cancelled" as const }, held.runs[1]!, continued],
  };
  expect(canReleasePlannedQueue(snapshot, recovered)).toBe(true);
  expect(
    canReleasePlannedQueue(snapshot, {
      ...recovered,
      runs: [
        ...recovered.runs,
        {
          ...continued,
          id: RunId.make("unrelated"),
          ordinal: 4,
          restartContinuationOfRunId: undefined,
        },
      ],
    }),
  ).toBe(false);
  expect(
    canReleasePlannedQueue(snapshot, {
      ...held,
      messages: held.messages.map((message) =>
        message.id === held.runs[1]!.userMessageId
          ? { ...message, text: "Edited after capture" }
          : message,
      ),
    }),
  ).toBe(false);
  expect(
    canReleasePlannedQueue(snapshot, {
      ...held,
      thread: { ...held.thread, branch: "different-branch" },
    }),
  ).toBe(false);
});

it("preserves preexisting holds and recognizes later Stop or hold even without new run events", () => {
  const projection = queuedFixture();
  expect(
    capture({ ...projection, runs: projection.runs.map((run) => ({ ...run, queueHeld: true })) }),
  ).toBeUndefined();
  const snapshot = capture(projection)!;
  const ids = continuationRunIds(snapshot, projection);
  for (const command_type of [
    "queue.hold",
    "turn.interrupt",
    "thread.runtime.stop",
    "thread.metadata.update",
    "message.dispatch",
  ])
    expect(
      changesPlannedControl({ command_id: "later-user-command", command_type }, snapshot, ids),
    ).toBe(true);
  expect(
    changesPlannedControl({ command_id: "visit", command_type: "thread.visit" }, snapshot, ids),
  ).toBe(false);
  expect(
    changesPlannedControl(
      {
        command_id: `command:restart-continuation:${snapshot.sourceRunId}`,
        command_type: "message.dispatch",
      },
      snapshot,
      ids,
    ),
  ).toBe(false);
});

it("does not capture roots paused for approval or user input", () => {
  const projection = queuedFixture();
  for (const kind of ["permission", "user_input"] as const) {
    const request = {
      id: RuntimeRequestId.make("pending-request"),
      nodeId: NodeId.make("pending-node"),
      providerTurnId: null,
      nativeRequestRef: null,
      kind,
      status: "pending" as const,
      responseCapability: { type: "message" as const },
      createdAt: DateTime.makeUnsafe(0),
      resolvedAt: null,
    };
    expect(capture({ ...projection, runtimeRequests: [request] })).toBeUndefined();
  }
});
