import { assert, it } from "@effect/vitest";
import { CommandId, EventId, MessageId, ProjectId, RunId, ThreadId } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import {
  LegacyPreparationFailureDecision,
  RecordedLifecycleEvent,
  RecordedRunJson,
  type RecordedRun,
} from "../../orchestration-v2/RecordedTypes.ts";
import { legacyBootstrapCreateCommandId } from "../../orchestration-v2/LegacyBootstrap.ts";
import * as Integrity from "./Integrity.ts";

const threadId = ThreadId.make("integrity:T");
const runId = RunId.make("integrity:R");
const releaseCommandId = CommandId.make("integrity:C");
const createCommandId = legacyBootstrapCreateCommandId(threadId, releaseCommandId);
const failureCommandId = `${createCommandId}:fail`;
const deleteCommandId = `${createCommandId}:failure-delete`;
const policy = {
  version: 1,
  createCommandId,
  birthCommandId: `${createCommandId}:initial-message`,
  releaseCommandId,
  projectId: ProjectId.make("integrity:P"),
  threadId,
  messageId: MessageId.make("integrity:M"),
  payloadHash: "integrity-fixture",
  ownsNewThread: true,
  runId,
};
const anchors = {
  claimEventId: "integrity:claim",
  claimSequence: 1,
  claimReceiptSequence: 1,
  birthEventId: "integrity:birth",
  birthSequence: 2,
  birthReceiptSequence: 2,
};
const workspace = {
  preparationGeneration: "integrity-generation",
  workspacePath: "/integrity",
  projectWorkspaceRoot: "/integrity",
};
// Decoded with the exact persisted codecs, so a malformed fixture cannot pass vacuously.
const decision = Schema.decodeUnknownSync(LegacyPreparationFailureDecision)({
  version: 1,
  status: "known_workspace_failure",
  policy,
  ...anchors,
  ...workspace,
  failedEffectId: "integrity-effect",
  failedInputHash: "integrity-hash",
  outcomeCommandId: `${createCommandId}:outcome`,
  outcomeEventId: `${createCommandId}:outcome:event`,
  outcomeEventSequence: 3,
  outcomeReceiptSequence: 3,
  failureCommandId,
  evidenceEventId: `${failureCommandId}:event`,
  deletion: {
    basis: "workspace_failure",
    provenance: {
      version: 1,
      type: "no_control",
      commandId: deleteCommandId,
      threadId,
      runId,
      policy,
      ...anchors,
      ...workspace,
      evidenceEventId: `${deleteCommandId}:event`,
      control: {
        version: 1,
        type: "no_control",
        policy,
        runId,
        threadId,
        ...anchors,
        ...workspace,
      },
    },
    failureCommandId,
    failureEvidenceEventId: `${failureCommandId}:event`,
    failureEventSequence: 4,
    failureReceiptSequence: 4,
  },
});
const run: RecordedRun = Schema.decodeSync(RecordedRunJson)({
  id: runId,
  threadId,
  ordinal: 1,
  providerInstanceId: "codex",
  modelSelection: { instanceId: "codex", model: "test" },
  providerThreadId: null,
  userMessageId: policy.messageId,
  rootNodeId: null,
  activeAttemptId: null,
  status: "failed",
  requestedAt: "2026-11-17T10:00:00.000Z",
  startedAt: null,
  completedAt: "2026-11-17T10:01:00.000Z",
  checkpointId: null,
  contextHandoffId: null,
});
const accepted: RecordedRun = {
  ...run,
  legacyBootstrap: decision.policy,
  legacyPreparationFailureDecision: decision,
  legacyPreparationFailureKnown: true,
};
const decodeLifecycleEventFixture = Schema.decodeSync(RecordedLifecycleEvent);
const runUpdated = (payload: RecordedRun): Integrity.RunEvent => {
  const event = decodeLifecycleEventFixture({
    id: EventId.make("integrity:ordinary"),
    type: "run.updated",
    threadId,
    runId,
    occurredAt: run.requestedAt,
    payload,
  });
  if (event.type !== "run.updated") throw new Error("Expected run.updated");
  return event;
};
const replaced = /Malformed or replaced legacy workspace-failure provenance/;

it("keeps recorded private values when a stale snapshot omits them", () => {
  const {
    legacyPreparationFailureDecision: _decision,
    legacyPreparationFailureKnown: _known,
    ...stale
  } = accepted;
  const retained = Integrity.retainRunMetadata(accepted, stale, runUpdated(stale));
  assert.deepStrictEqual(retained.legacyPreparationFailureDecision, decision);
  assert.strictEqual(retained.legacyPreparationFailureKnown, true);
  assert.deepStrictEqual(retained.legacyBootstrap, accepted.legacyBootstrap);
});

it("accepts the exact accepted deletion and rejects replaced workspace-failure provenance", () => {
  assert.isTrue(Integrity.validateFailureDecision(accepted, accepted, runUpdated(accepted)));
  const deletion = decision.deletion!;
  const provenance = deletion.provenance;
  if (provenance.type !== "no_control") throw new Error("Expected no-control proof");
  const tampered: RecordedRun = {
    ...accepted,
    legacyPreparationFailureDecision: {
      ...decision,
      deletion: {
        ...deletion,
        provenance: {
          ...provenance,
          control: { ...provenance.control, workspacePath: "/integrity/replaced" },
        },
      },
    },
  };
  assert.throws(
    () => Integrity.validateFailureDecision(accepted, tampered, runUpdated(tampered)),
    replaced,
  );
  assert.throws(
    () => Integrity.retainRunMetadata(accepted, tampered, runUpdated(tampered)),
    replaced,
  );
  const rewritten: RecordedRun = {
    ...accepted,
    legacyPreparationFailureDecision: { ...decision, failedInputHash: "integrity-other-hash" },
  };
  assert.throws(
    () => Integrity.validateFailureDecision(accepted, rewritten, runUpdated(rewritten)),
    replaced,
  );
});

it("rejects a decision that no recorded preparation authenticates", () => {
  const { deletion: _deletion, ...original } = decision;
  const fabricated: RecordedRun = { ...run, legacyPreparationFailureDecision: original };
  for (const current of [undefined, { ...run, status: "preparing" as const }])
    assert.throws(
      () => Integrity.validateFailureDecision(current, fabricated, runUpdated(fabricated)),
      replaced,
    );
});
