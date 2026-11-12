import { assert, it } from "@effect/vitest";
import { CheckpointScopeId, RunAttemptId, RunId } from "@t3tools/contracts";
import { canSettleWaitingRun, type WaitingRunStopEvidence } from "./WaitingRunSettlement.ts";

const runId = RunId.make("waiting-run");
const attemptId = RunAttemptId.make("ended-attempt");
const fixture = {
  run: { id: runId, activeAttemptId: attemptId, status: "waiting" as const },
  attempt: { id: attemptId, runId, status: "completed" as const },
  providerTurn: { runAttemptId: attemptId, status: "completed" as const },
  checkpointEffects: [],
  evidence: "persisted_terminal" as WaitingRunStopEvidence | undefined,
};

it.each(["persisted_terminal", "absent_session", "adapter_confirmed"] as const)(
  "waiting finalization accepts positive %s evidence after the attempt ends",
  (evidence) =>
    assert.isTrue(
      canSettleWaitingRun({
        ...fixture,
        evidence,
        providerTurn: {
          ...fixture.providerTurn,
          status: evidence === "persisted_terminal" ? "completed" : "running",
        },
      }),
    ),
);

it("waiting finalization rejects unknown evidence, live attempts, and stale ownership", () => {
  assert.isFalse(canSettleWaitingRun({ ...fixture, evidence: undefined }));
  for (const status of ["pending", "running"] as const) {
    assert.isFalse(canSettleWaitingRun({ ...fixture, attempt: { ...fixture.attempt, status } }));
    assert.isFalse(
      canSettleWaitingRun({ ...fixture, providerTurn: { ...fixture.providerTurn, status } }),
    );
  }
  assert.isFalse(
    canSettleWaitingRun({
      ...fixture,
      run: { ...fixture.run, activeAttemptId: RunAttemptId.make("new-attempt") },
    }),
  );
  assert.isFalse(
    canSettleWaitingRun({
      ...fixture,
      providerTurn: { ...fixture.providerTurn, runAttemptId: RunAttemptId.make("old-attempt") },
    }),
  );
  assert.isFalse(
    canSettleWaitingRun({
      ...fixture,
      attempt: { ...fixture.attempt, runId: RunId.make("other-run") },
    }),
  );
});

it("waiting finalization protects live capture and ignores settled or other-run capture", () => {
  const request = {
    type: "checkpoint.capture" as const,
    runId,
    scopeId: CheckpointScopeId.make("scope"),
  };
  for (const status of ["pending", "running"] as const) {
    assert.isFalse(canSettleWaitingRun({ ...fixture, checkpointEffects: [{ request, status }] }));
    assert.isTrue(
      canSettleWaitingRun({
        ...fixture,
        checkpointEffects: [{ request: { ...request, runId: RunId.make("other-run") }, status }],
      }),
    );
  }
  for (const status of ["succeeded", "failed", "cancelled"] as const) {
    assert.isTrue(canSettleWaitingRun({ ...fixture, checkpointEffects: [{ request, status }] }));
  }
});
