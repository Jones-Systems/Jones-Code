// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";

import { assert, describe, it } from "@effect/vitest";
import { CommandId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { checkpointRefForScopeOrdinal } from "../src/orchestration-v2/CheckpointService.ts";

import {
  gitRefExists,
  gitShowFileAtRef,
  makeOrchestrationIntegrationHarness,
} from "./OrchestrationEngineHarness.integration.ts";

// Retired V1 duplicates are owned by these real V2 suites. This fixture keeps
// the Jones linked-worktree/tool-edit/finalization/Claude-rollback combination.
// Optional live-Codex runtime-mode switching has no claimed synthetic equivalent.
const retiredV1Coverage = {
  turnLifecycleAndProviderSelection:
    "src/orchestration-v2/testkit/OrchestratorReplayFixtures.integration.test.ts",
  claudeLifecycleAndInterrupt:
    "src/orchestration-v2/testkit/ClaudeReplayFixtures.integration.test.ts",
  persistedSessionRecovery:
    "src/orchestration-v2/testkit/OrchestratorReplayRecovery.integration.test.ts",
  approvalResponse: "src/orchestration-v2/RuntimeRequestService.test.ts",
  missingRollbackBinding: "src/orchestration-v2/CheckpointRollbackService.test.ts",
  checkpointScopeOwnership: "src/orchestration-v2/CheckpointScopeOwnership.test.ts",
} as const;

// Kept as structured maintenance evidence, without claiming these suites ran here.
describe(`V2 engine integration (${Object.keys(retiredV1Coverage).length} canonical coverage owners)`, () => {
  for (const driver of ["codex", "claudeAgent"] as const) {
    it.live(
      `${driver}: isolates linked-worktree edits and settles checkpoints before filesystem/conversation rollback`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const harness = yield* makeOrchestrationIntegrationHarness(driver);
            const { fs, workspaceDir, repositoryDir, threadId, orchestrator } = harness;
            const readme = NodePath.join(workspaceDir, "README.md");
            assert.equal((yield* fs.stat(NodePath.join(workspaceDir, ".git"))).type, "File");

            const firstOrdinal = yield* harness.dispatchTurn({
              text: "First edit",
              contents: "v2\n",
            });
            const first = yield* harness.settle(firstOrdinal);
            const firstRun = first.runs.find((run) => run.ordinal === firstOrdinal)!;
            assert.equal(firstRun.status, "completed");
            assert.notEqual(firstRun.checkpointId, null);
            const firstCheckpoint = first.checkpoints.find(
              (checkpoint) => checkpoint.id === firstRun.checkpointId,
            )!;
            const baseline = first.checkpoints.find(
              (checkpoint) => checkpoint.ordinalWithinScope === 0,
            )!;
            assert.equal(firstCheckpoint.status, "ready");
            assert.equal(baseline.status, "ready");
            assert.equal(gitShowFileAtRef(workspaceDir, baseline.ref, "README.md"), "v1\n");
            assert.equal(gitShowFileAtRef(workspaceDir, firstCheckpoint.ref, "README.md"), "v2\n");
            assert.equal(
              firstCheckpoint.files.some((file) => file.path === "README.md"),
              true,
            );
            assert.deepEqual(
              first.messages.map((message) => [message.role, message.text]),
              [
                ["user", "Request 1"],
                ["assistant", "First edit"],
              ],
            );
            assert.equal(
              first.turnItems.some((item) => item.type === "file_change" && item.newStr === "v2\n"),
              true,
            );
            const firstCaptureEffects = yield* harness.outbox.listByCommandId(
              CommandId.make(`command:effect:checkpoint.capture:${firstRun.id}`),
            );
            assert.equal(firstCaptureEffects.length, 1);
            assert.equal(firstCaptureEffects[0]?.status, "succeeded");

            const secondOrdinal = yield* harness.dispatchTurn({
              text: "Second edit",
              contents: "v3\n",
            });
            const second = yield* harness.settle(secondOrdinal);
            const secondRun = second.runs.find((run) => run.ordinal === secondOrdinal)!;
            const secondCheckpoint = second.checkpoints.find(
              (checkpoint) => checkpoint.id === secondRun.checkpointId,
            )!;
            assert.equal(secondRun.status, "completed");
            assert.equal(secondCheckpoint.status, "ready");
            assert.equal(secondCheckpoint.scopeId, firstCheckpoint.scopeId);
            assert.equal(gitShowFileAtRef(workspaceDir, secondCheckpoint.ref, "README.md"), "v3\n");
            assert.equal(yield* fs.readFileString(readme), "v3\n");
            assert.equal(
              yield* fs.readFileString(NodePath.join(repositoryDir, "README.md")),
              "v1\n",
            );
            assert.deepEqual(harness.conversation(), [
              ["user", "Request 1"],
              ["assistant", "First edit"],
              ["user", "Request 2"],
              ["assistant", "Second edit"],
            ]);
            const secondCaptureEffects = yield* harness.outbox.listByCommandId(
              CommandId.make(`command:effect:checkpoint.capture:${secondRun.id}`),
            );
            assert.equal(secondCaptureEffects.length, 1);
            assert.equal(secondCaptureEffects[0]?.status, "succeeded");
            assert.deepEqual(
              second.messages.map((message) => [message.role, message.text]),
              [
                ["user", "Request 1"],
                ["assistant", "First edit"],
                ["user", "Request 2"],
                ["assistant", "Second edit"],
              ],
            );

            // Both checkpoint capture transactions and their actual outbox executions
            // have completed before Claude receives the rollback request.
            yield* orchestrator.dispatch({
              type: "checkpoint.rollback",
              commandId: CommandId.make("rollback:first"),
              threadId,
              checkpointId: firstCheckpoint.id,
              scopeId: firstCheckpoint.scopeId,
            });
            yield* harness.worker.drain(32);
            const reverted = yield* orchestrator.getThreadProjection(threadId);
            assert.equal(reverted.runs.find((run) => run.id === firstRun.id)?.status, "completed");
            assert.equal(
              reverted.runs.find((run) => run.id === secondRun.id)?.status,
              "rolled_back",
            );
            assert.equal(
              reverted.checkpoints.find((checkpoint) => checkpoint.id === firstCheckpoint.id)
                ?.status,
              "ready",
            );
            assert.equal(
              reverted.checkpoints.find((checkpoint) => checkpoint.id === secondCheckpoint.id)
                ?.status,
              "stale",
            );
            assert.equal(gitRefExists(workspaceDir, firstCheckpoint.ref), true);
            assert.equal(gitRefExists(workspaceDir, secondCheckpoint.ref), false);
            assert.equal(yield* fs.readFileString(readme), "v2\n");
            assert.equal(
              yield* fs.readFileString(NodePath.join(repositoryDir, "README.md")),
              "v1\n",
            );
            assert.deepEqual(harness.conversation(), [
              ["user", "Request 1"],
              ["assistant", "First edit"],
            ]);
            assert.equal(harness.rollbackCalls.length, 1);
            assert.equal(harness.rollbackCalls[0]?.target.type, "provider_turn");
            assert.equal(harness.rollbackCalls[0]?.target.appRunOrdinal, firstOrdinal);
            assert.equal(harness.rollbackCalls[0]?.target.checkpointId, firstCheckpoint.id);
            assert.equal(harness.rollbackCalls[0]?.providerThreadTurns.length, 2);
            assert.equal(
              (yield* orchestrator.getThreadProjection(harness.primaryThreadId)).thread.title,
              "Unrelated primary checkout",
            );
            const checkpointRows = yield* harness.sql<{
              readonly checkpoint_id: string;
              readonly status: string;
            }>`
          SELECT checkpoint_id, status FROM orchestration_v2_projection_checkpoints
          WHERE thread_id = ${threadId} ORDER BY ordinal_within_scope
        `;
            assert.equal(
              checkpointRows.find((row) => row.checkpoint_id === secondCheckpoint.id)?.status,
              "stale",
            );
            const rollbackEffects = yield* harness.outbox.listByCommandId(
              CommandId.make("rollback:first"),
            );
            assert.equal(rollbackEffects.length, 1);
            assert.equal(rollbackEffects[0]?.status, "succeeded");
          }),
        ),
    );

    it.live(
      `${driver}: persists approval resolution and delivers it before the bounded file edit`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const harness = yield* makeOrchestrationIntegrationHarness(driver);
            const ordinal = yield* harness.dispatchTurn({
              text: "Approved edit",
              contents: "approved\n",
              approval: true,
            });
            const pending = yield* harness.waitForProjection((projection) =>
              projection.runtimeRequests.some((request) => request.status === "pending"),
            );
            const request = pending.runtimeRequests.find((entry) => entry.status === "pending")!;
            assert.equal(
              yield* harness.fs.readFileString(NodePath.join(harness.workspaceDir, "README.md")),
              "v1\n",
            );
            assert.equal(harness.approvalResponses.length, 0);
            yield* harness.orchestrator.dispatch({
              type: "runtime-request.respond",
              commandId: CommandId.make("approval:respond"),
              threadId: harness.threadId,
              requestId: request.id,
              decision: "accept",
            });
            yield* harness.worker.drain(32);
            const completed = yield* harness.settle(ordinal);
            assert.equal(
              completed.runtimeRequests.find((entry) => entry.id === request.id)?.status,
              "resolved",
            );
            assert.equal(
              completed.runtimeRequests.find((entry) => entry.id === request.id)?.decision,
              "accept",
            );
            assert.equal(harness.approvalResponses.length, 1);
            assert.equal(harness.approvalResponses[0]?.requestId, request.id);
            assert.equal(harness.approvalResponses[0]?.decision, "accept");
            assert.equal(
              yield* harness.fs.readFileString(NodePath.join(harness.workspaceDir, "README.md")),
              "approved\n",
            );
            assert.equal(
              completed.runs.find((run) => run.ordinal === ordinal)?.status,
              "completed",
            );
            const delivery = yield* harness.outbox.listByCommandId(
              CommandId.make("approval:respond"),
            );
            assert.equal(delivery.length, 1);
            assert.equal(delivery[0]?.status, "succeeded");
          }),
        ),
    );

    it.live(`${driver}: persists failed runs without claiming a finalized checkpoint`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const harness = yield* makeOrchestrationIntegrationHarness(driver);
          const ordinal = yield* harness.dispatchTurn({ text: "Failure reported", fail: true });
          const failed = yield* harness.settle(ordinal);
          const run = failed.runs.find((entry) => entry.ordinal === ordinal)!;
          assert.equal(run.status, "failed");
          assert.equal(run.checkpointId, null);
          assert.equal(
            failed.attempts.find((attempt) => attempt.runId === run.id)?.status,
            "failed",
          );
          assert.equal(
            failed.providerTurns.find((turn) => turn.runAttemptId === run.activeAttemptId)?.status,
            "failed",
          );
          assert.equal(
            failed.turnItems.some(
              (item) => item.type === "error" && item.failure.message === "Local fixture failure",
            ),
            true,
          );
          assert.equal(
            failed.checkpoints.some((checkpoint) => checkpoint.runId === run.id),
            false,
          );
          const failedScope = failed.checkpointScopes.find((scope) => scope.runId === run.id)!;
          assert.equal(
            gitRefExists(
              harness.workspaceDir,
              checkpointRefForScopeOrdinal({
                scopeId: failedScope.id,
                ordinalWithinScope: ordinal,
              }),
            ),
            false,
          );
          assert.deepEqual(
            yield* harness.outbox.listByCommandId(
              CommandId.make(`command:effect:checkpoint.capture:${run.id}`),
            ),
            [],
          );
          assert.equal(
            yield* harness.fs.readFileString(NodePath.join(harness.workspaceDir, "README.md")),
            "v1\n",
          );
          assert.equal(
            yield* harness.fs.readFileString(NodePath.join(harness.repositoryDir, "README.md")),
            "v1\n",
          );
        }),
      ),
    );
  }
});
