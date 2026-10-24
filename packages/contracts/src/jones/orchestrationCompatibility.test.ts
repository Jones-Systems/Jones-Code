import { describe, expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import { ProviderInstanceId } from "../index.ts";
import {
  OrchestrationV2AppThread,
  OrchestrationV2Run,
  OrchestrationV2RunAttemptJson,
  OrchestrationV2ThreadLaunchWorkspaceStrategy,
  OrchestrationV2Command,
  OrchestrationV2ThreadShell,
} from "../orchestrationV2.ts";

const now = DateTime.makeUnsafe("2026-04-20T00:00:00.000Z");
const decodeOrchestrationV2Command = Schema.decodeUnknownSync(OrchestrationV2Command);
const decodeOrchestrationV2ThreadShell = Schema.decodeUnknownSync(OrchestrationV2ThreadShell);

describe("orchestration V2 contracts", () => {
  it("keeps legacy receiving correlation out of public thread and run schemas", () => {
    expect(Object.keys(OrchestrationV2AppThread.fields)).not.toContain("legacyBootstrapClaim");
    for (const privateField of [
      "legacyBootstrap",
      "legacyPreparationFailureKnown",
      "legacyPreparation",
      "legacyReleaseDecision",
      "workspaceRunSetupScript",
    ]) {
      expect(Object.keys(OrchestrationV2Run.fields)).not.toContain(privateField);
    }
    expect(OrchestrationV2Run.fields.workspacePreparation).toBeDefined();
  });

  it("decodes historical thread shell JSON without pendingBackgroundTasks as empty roster", () => {
    const shell = decodeOrchestrationV2ThreadShell({
      createdBy: "user",
      creationSource: "web",
      id: "thread-1",
      projectId: "project-1",
      title: "Thread",
      providerInstanceId: "claudeAgent",
      modelSelection: {
        instanceId: ProviderInstanceId.make("claudeAgent"),
        model: "claude-sonnet",
      },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      lineage: {
        parentThreadId: null,
        relationshipToParent: null,
        rootThreadId: "thread-1",
      },
      forkedFrom: null,
      activeProviderThreadId: "provider-thread-1",
      latestRunId: "run-1",
      activeRunId: null,
      status: "completed",
      pendingRuntimeRequest: null,
      latestVisibleMessage: null,
      latestUserMessageAt: null,
      hasActionableProposedPlan: false,
      itemCount: 0,
      visibleItemCount: 0,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      deletedAt: null,
    });

    expect(shell.pendingBackgroundTasks).toEqual([]);
    expect(shell.threadMessagesBlocked ?? false).toBe(false);
    expect(
      decodeOrchestrationV2ThreadShell({ ...shell, threadMessagesBlocked: true })
        .threadMessagesBlocked,
    ).toBe(true);
  });
});

describe("provider settlement compatibility", () => {
  const settlement = {
    runAttemptId: "attempt-1",
    providerTurnId: "provider-turn-1",
    status: "completed",
    completedAt: "2026-09-01T12:00:05.000Z",
  };
  const attempt = {
    id: "attempt-1",
    runId: "run-1",
    attemptOrdinal: 1,
    rootNodeId: "node-1",
    providerInstanceId: "codex",
    providerThreadId: "provider-thread-1",
    providerTurnId: null,
    reason: "initial",
    status: "running",
    startedAt: null,
    completedAt: null,
  };
  it.each([undefined, null, settlement])(
    "retains absent, explicit null and attributed settlement in persisted attempts: %s",
    (value) => {
      const input = { ...attempt, ...(value === undefined ? {} : { providerSettlement: value }) };
      const decoded = Schema.decodeUnknownSync(OrchestrationV2RunAttemptJson)(input);
      const encoded = Schema.encodeSync(OrchestrationV2RunAttemptJson)(decoded);
      expect(encoded).toEqual(input);
      expect(Object.hasOwn(encoded, "providerSettlement")).toBe(value !== undefined);
    },
  );
  it.each(["running", "waiting", "superseded"])(
    "rejects nonterminal provider settlement %s",
    (status) => {
      expect(() =>
        Schema.decodeUnknownSync(OrchestrationV2RunAttemptJson)({
          ...attempt,
          providerSettlement: { ...settlement, status },
        }),
      ).toThrow();
    },
  );
  it("requires attributed identities and a fixed completion time", () => {
    for (const field of ["runAttemptId", "providerTurnId", "completedAt"]) {
      const incomplete = { ...settlement, [field]: null };
      expect(() =>
        Schema.decodeUnknownSync(OrchestrationV2RunAttemptJson)({
          ...attempt,
          providerSettlement: incomplete,
        }),
      ).toThrow();
    }
  });
});

describe("queued tool delivery command compatibility", () => {
  it("preserves absent, false and true eligibility without a decode default", () => {
    const command = {
      type: "message.dispatch",
      commandId: "queue-compat",
      threadId: "thread",
      messageId: "message",
      createdBy: "user",
      creationSource: "web",
      text: "Queue",
      attachments: [],
      dispatchMode: { type: "queue_after_active" },
    };
    expect(decodeOrchestrationV2Command(command)).not.toHaveProperty("queuedToolBoundaryEligible");
    for (const value of [false, true])
      expect(
        decodeOrchestrationV2Command({ ...command, queuedToolBoundaryEligible: value }),
      ).toHaveProperty("queuedToolBoundaryEligible", value);
    expect(() =>
      decodeOrchestrationV2Command({ ...command, queuedToolBoundaryEligible: 1 }),
    ).toThrow();
  });
});

describe("worktree launch base", () => {
  const decode = Schema.decodeUnknownSync(OrchestrationV2ThreadLaunchWorkspaceStrategy);

  it("round-trips an omitted base for automatic server selection", () => {
    const input = { type: "worktree", branch: "feature", startFromOrigin: true };
    expect(Schema.encodeSync(OrchestrationV2ThreadLaunchWorkspaceStrategy)(decode(input))).toEqual(
      input,
    );
  });

  it("preserves explicit bases and rejects blank bases", () => {
    expect(decode({ type: "worktree", baseRef: "release/stable" })).toEqual({
      type: "worktree",
      baseRef: "release/stable",
    });
    expect(() => decode({ type: "worktree", baseRef: " " })).toThrow();
  });
});
