import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  OrchestrationV2ImportedHistoryReviewBasis,
  type OrchestrationV2ServerCommand,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ThreadProjection,
  ProviderInstanceId,
  ProjectId,
  ProviderSessionId,
  ProviderThreadId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import { CursorProviderCapabilitiesV2 } from "./Adapters/CursorAdapterV2.ts";
import { GrokProviderCapabilitiesV2 } from "./Adapters/GrokAdapterV2.ts";
import * as CommandPolicy from "./CommandPolicy.ts";

const commandId = CommandId.make("command-policy-test");
const threadId = ThreadId.make("command-policy-thread");
const activeRunId = RunId.make("command-policy-active-run");

it.effect(
  "rejects pull request discovery at public ingress even with a server-looking command id",
  () =>
    Effect.gen(function* () {
      const command: OrchestrationV2ServerCommand = {
        type: "thread.pull-request.sync",
        commandId: CommandId.make("server:thread-pull-request:forged"),
        threadId,
        projectId: ProjectId.make("command-policy-project"),
        snapshotSequence: 0,
        expected: {
          workspaceRoot: "/workspace/project",
          branch: "feature",
          worktreePath: null,
          linkedPullRequest: null,
          branchPullRequest: null,
        },
        branchPullRequest: null,
      };
      const error = yield* CommandPolicy.validatePublicCommand(command).pipe(Effect.flip);
      assert.equal(error._tag, "CommandPolicyPublicIngressError");
      assert.equal(error.commandId, command.commandId);
      assert.equal(error.commandType, command.type);
    }),
);

it.effect("rejects imported-history consent at generic ingress before hydration and effects", () =>
  Effect.gen(function* () {
    let hydrationCalls = 0;
    let effectCalls = 0;
    const command: OrchestrationV2ServerCommand = {
      type: "thread.imported-history.start",
      commandId: CommandId.make("server:imported-history:forged"),
      threadId,
      reviewedBasis: yield* Schema.decodeUnknownEffect(OrchestrationV2ImportedHistoryReviewBasis)(
        "reviewed-basis",
      ).pipe(Effect.orDie),
      delivery: {
        type: "queued_run",
        runId: activeRunId,
        messageId: MessageId.make("command-policy-imported-message"),
      },
    };
    const error = yield* CommandPolicy.validatePublicCommand(command).pipe(
      Effect.andThen(
        Effect.sync(() => {
          hydrationCalls += 1;
        }),
      ),
      Effect.andThen(
        Effect.sync(() => {
          effectCalls += 1;
        }),
      ),
      Effect.flip,
    );
    assert.equal(error._tag, "CommandPolicyPublicIngressError");
    assert.equal(error.commandType, "thread.imported-history.start");
    assert.equal(hydrationCalls, 0);
    assert.equal(effectCalls, 0);
  }),
);

it.effect("keeps ordinary creation and message dispatch available at public ingress", () =>
  Effect.gen(function* () {
    yield* CommandPolicy.validatePublicCommand({
      type: "thread.create",
      commandId,
      threadId,
      projectId: ProjectId.make("command-policy-project"),
      title: "Ordinary thread",
      modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.1-codex" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "agent",
      creationSource: "mcp",
    });
    yield* CommandPolicy.validatePublicCommand({
      type: "message.dispatch",
      commandId,
      threadId,
      messageId: MessageId.make("command-policy-message"),
      text: "Continue",
      attachments: [],
      dispatchMode: { type: "start_immediately" },
      createdBy: "user",
      creationSource: "web",
    });
  }),
);

const baseCapabilities: OrchestrationV2ProviderCapabilities = CodexProviderCapabilitiesV2;

function capabilities(
  override: (current: OrchestrationV2ProviderCapabilities) => OrchestrationV2ProviderCapabilities,
): OrchestrationV2ProviderCapabilities {
  return override(baseCapabilities);
}

function dispatchProjection(
  sessionCapabilities?: OrchestrationV2ProviderCapabilities,
): OrchestrationV2ThreadProjection {
  const providerThreadId = ProviderThreadId.make("command-policy-provider-thread");
  const providerSessionId = ProviderSessionId.make("command-policy-provider-session");
  return {
    runs:
      sessionCapabilities === undefined
        ? []
        : [{ id: activeRunId, status: "running", providerThreadId }],
    providerThreads:
      sessionCapabilities === undefined ? [] : [{ id: providerThreadId, providerSessionId }],
    providerSessions:
      sessionCapabilities === undefined
        ? []
        : [{ id: providerSessionId, capabilities: sessionCapabilities }],
  } as unknown as OrchestrationV2ThreadProjection;
}

it("resolves automatic message delivery from authoritative provider capabilities", () => {
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      dispatchProjection(),
      { type: "start_immediately" },
      "auto",
    ),
    { type: "start_immediately" },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      dispatchProjection(baseCapabilities),
      { type: "start_immediately" },
      "auto",
    ),
    { type: "steer_active", targetRunId: activeRunId },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      dispatchProjection(
        capabilities((current) => ({
          ...current,
          turns: {
            ...current.turns,
            supportsActiveSteering: false,
            supportsQueuedMessages: true,
            supportsSteeringByInterruptRestart: true,
          },
        })),
      ),
      { type: "start_immediately" },
      "auto",
    ),
    { type: "queue_after_active" },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      dispatchProjection(
        capabilities((current) => ({
          ...current,
          turns: {
            ...current.turns,
            supportsActiveSteering: false,
            supportsQueuedMessages: false,
            supportsSteeringByInterruptRestart: true,
          },
        })),
      ),
      { type: "start_immediately" },
      "auto",
    ),
    { type: "restart_active", targetRunId: activeRunId },
  );
});

it.each(["preparing", "starting"] as const)(
  "queues an automatic message while the handoff run is %s",
  (status) => {
    const projection = dispatchProjection(baseCapabilities);
    assert.deepEqual(
      CommandPolicy.resolveMessageDispatchIntent(
        { ...projection, runs: projection.runs.map((run) => ({ ...run, status })) },
        { type: "start_immediately" },
        "auto",
      ),
      { type: "queue_after_active" },
    );
  },
);

it("targets the latest active run for explicit steer and restart intent", () => {
  const projection = dispatchProjection(baseCapabilities);
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(projection, { type: "start_immediately" }, "steer"),
    { type: "steer_active", targetRunId: activeRunId },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      projection,
      { type: "start_immediately" },
      "restart",
    ),
    { type: "restart_active", targetRunId: activeRunId },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      dispatchProjection(),
      { type: "start_immediately" },
      "steer",
    ),
    { type: "start_immediately" },
  );
});

const layer = it.layer(CommandPolicy.layer);

layer("CommandPolicyV2", (it) => {
  it.effect("prefers direct active steering when the provider supports it", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const result = yield* policy.decideSteeringExecution({
        commandId,
        threadId,
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: baseCapabilities,
      });

      assert.equal(result, "active_steering");
    }),
  );

  it.effect("uses interrupt-and-restart steering when direct steering is unavailable", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const result = yield* policy.decideSteeringExecution({
        commandId,
        threadId,
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: capabilities((current) => ({
          ...current,
          turns: {
            ...current.turns,
            supportsActiveSteering: false,
            supportsInterrupt: true,
            supportsSteeringByInterruptRestart: true,
          },
        })),
      });

      assert.equal(result, "interrupt_restart");
    }),
  );

  it.effect("uses interrupt-and-restart steering for Grok ACP", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const result = yield* policy.decideSteeringExecution({
        commandId,
        threadId,
        providerInstanceId: ProviderInstanceId.make("grok"),
        capabilities: GrokProviderCapabilitiesV2,
      });

      assert.equal(result, "interrupt_restart");
    }),
  );

  it.effect("honors an explicit interrupt-and-restart request", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const result = yield* policy.decideSteeringExecution({
        commandId,
        threadId,
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: CodexProviderCapabilitiesV2,
        forceRestart: true,
      });

      assert.equal(result, "interrupt_restart");
    }),
  );

  it.effect("reports the actually missing capability across the steering matrix", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;
      const providerInstanceId = ProviderInstanceId.make("codex");
      const fallbackDetail =
        "providerInstanceId cannot steer active turns directly or by interrupt-and-restart";
      const forcedRestartDetail =
        "providerInstanceId cannot satisfy a required interrupt-and-restart";

      const cases: ReadonlyArray<{
        readonly forceRestart: boolean;
        readonly supportsActiveSteering: boolean;
        readonly supportsInterrupt: boolean;
        readonly supportsSteeringByInterruptRestart: boolean;
        readonly expected:
          | { readonly type: "policy"; readonly value: CommandPolicy.SteeringExecutionPolicyV2 }
          | {
              readonly type: "error";
              readonly capability: CommandPolicy.CommandPolicyCapability;
              readonly detail: string;
            };
      }> = [
        // Ordinary steering prefers direct active steering whenever available.
        {
          forceRestart: false,
          supportsActiveSteering: true,
          supportsInterrupt: false,
          supportsSteeringByInterruptRestart: false,
          expected: { type: "policy", value: "active_steering" },
        },
        {
          forceRestart: false,
          supportsActiveSteering: true,
          supportsInterrupt: false,
          supportsSteeringByInterruptRestart: true,
          expected: { type: "policy", value: "active_steering" },
        },
        {
          forceRestart: false,
          supportsActiveSteering: true,
          supportsInterrupt: true,
          supportsSteeringByInterruptRestart: false,
          expected: { type: "policy", value: "active_steering" },
        },
        {
          forceRestart: false,
          supportsActiveSteering: true,
          supportsInterrupt: true,
          supportsSteeringByInterruptRestart: true,
          expected: { type: "policy", value: "active_steering" },
        },
        // Ordinary steering falls back to interrupt-and-restart, then to a
        // typed error naming the capability that is actually missing.
        {
          forceRestart: false,
          supportsActiveSteering: false,
          supportsInterrupt: true,
          supportsSteeringByInterruptRestart: true,
          expected: { type: "policy", value: "interrupt_restart" },
        },
        {
          forceRestart: false,
          supportsActiveSteering: false,
          supportsInterrupt: true,
          supportsSteeringByInterruptRestart: false,
          expected: {
            type: "error",
            capability: "interrupt_restart_steering",
            detail: fallbackDetail,
          },
        },
        {
          forceRestart: false,
          supportsActiveSteering: false,
          supportsInterrupt: false,
          supportsSteeringByInterruptRestart: true,
          expected: { type: "error", capability: "active_steering", detail: fallbackDetail },
        },
        {
          forceRestart: false,
          supportsActiveSteering: false,
          supportsInterrupt: false,
          supportsSteeringByInterruptRestart: false,
          expected: { type: "error", capability: "active_steering", detail: fallbackDetail },
        },
        // A forced restart skips direct steering and only succeeds via
        // interrupt-and-restart; every other combination must report that
        // capability instead of claiming live steering is unsupported.
        {
          forceRestart: true,
          supportsActiveSteering: true,
          supportsInterrupt: true,
          supportsSteeringByInterruptRestart: true,
          expected: { type: "policy", value: "interrupt_restart" },
        },
        {
          forceRestart: true,
          supportsActiveSteering: false,
          supportsInterrupt: true,
          supportsSteeringByInterruptRestart: true,
          expected: { type: "policy", value: "interrupt_restart" },
        },
        {
          forceRestart: true,
          supportsActiveSteering: true,
          supportsInterrupt: true,
          supportsSteeringByInterruptRestart: false,
          expected: {
            type: "error",
            capability: "interrupt_restart_steering",
            detail: forcedRestartDetail,
          },
        },
        {
          forceRestart: true,
          supportsActiveSteering: false,
          supportsInterrupt: true,
          supportsSteeringByInterruptRestart: false,
          expected: {
            type: "error",
            capability: "interrupt_restart_steering",
            detail: forcedRestartDetail,
          },
        },
        {
          forceRestart: true,
          supportsActiveSteering: true,
          supportsInterrupt: false,
          supportsSteeringByInterruptRestart: true,
          expected: {
            type: "error",
            capability: "interrupt_restart_steering",
            detail: forcedRestartDetail,
          },
        },
        {
          forceRestart: true,
          supportsActiveSteering: false,
          supportsInterrupt: false,
          supportsSteeringByInterruptRestart: true,
          expected: {
            type: "error",
            capability: "interrupt_restart_steering",
            detail: forcedRestartDetail,
          },
        },
        {
          forceRestart: true,
          supportsActiveSteering: true,
          supportsInterrupt: false,
          supportsSteeringByInterruptRestart: false,
          expected: {
            type: "error",
            capability: "interrupt_restart_steering",
            detail: forcedRestartDetail,
          },
        },
        {
          forceRestart: true,
          supportsActiveSteering: false,
          supportsInterrupt: false,
          supportsSteeringByInterruptRestart: false,
          expected: {
            type: "error",
            capability: "interrupt_restart_steering",
            detail: forcedRestartDetail,
          },
        },
      ];

      for (const entry of cases) {
        const decision = policy.decideSteeringExecution({
          commandId,
          threadId,
          providerInstanceId,
          forceRestart: entry.forceRestart,
          capabilities: capabilities((current) => ({
            ...current,
            turns: {
              ...current.turns,
              supportsActiveSteering: entry.supportsActiveSteering,
              supportsInterrupt: entry.supportsInterrupt,
              supportsSteeringByInterruptRestart: entry.supportsSteeringByInterruptRestart,
            },
          })),
        });

        if (entry.expected.type === "policy") {
          assert.equal(yield* decision, entry.expected.value);
          continue;
        }

        const error = yield* decision.pipe(Effect.flip);
        assert.instanceOf(error, CommandPolicy.CommandPolicyCapabilityUnsupportedError);
        assert.equal(error.commandId, commandId);
        assert.equal(error.threadId, threadId);
        assert.equal(error.providerInstanceId, providerInstanceId);
        assert.equal(error.capability, entry.expected.capability);
        assert.equal(error.detail, entry.expected.detail);
      }
    }),
  );

  it.effect("returns typed capability errors for unsupported active steering", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const error = yield* policy
        .decideSteeringExecution({
          commandId,
          threadId,
          providerInstanceId: ProviderInstanceId.make("codex"),
          capabilities: capabilities((current) => ({
            ...current,
            turns: {
              ...current.turns,
              supportsActiveSteering: false,
              supportsInterrupt: false,
              supportsSteeringByInterruptRestart: false,
            },
          })),
        })
        .pipe(Effect.flip);

      assert.instanceOf(error, CommandPolicy.CommandPolicyCapabilityUnsupportedError);
      assert.equal(error.capability, "active_steering");
    }),
  );

  it.effect("guards native fork behind fork and identity capabilities", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const error = yield* policy
        .ensureNativeFork({
          commandId,
          threadId,
          providerInstanceId: ProviderInstanceId.make("codex"),
          fromSpecificTurn: true,
          capabilities: capabilities((current) => ({
            ...current,
            identity: {
              ...current.identity,
              nativeThreadIds: "weak",
            },
          })),
        })
        .pipe(Effect.flip);

      assert.instanceOf(error, CommandPolicy.CommandPolicyCapabilityUnsupportedError);
      assert.equal(error.capability, "native_fork");
    }),
  );

  it.effect("uses a native fork when the provider supports the requested source point", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const result = yield* policy.decideForkExecution({
        commandId,
        threadId,
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: CodexProviderCapabilitiesV2,
        sameProvider: true,
        hasStrongNativeSource: true,
        sourceRunStatus: "completed",
        fromSpecificTurn: true,
      });

      assert.equal(result, "native_fork");
    }),
  );

  it.effect("falls back to portable context when Cursor cannot fork natively", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const result = yield* policy.decideForkExecution({
        commandId,
        threadId,
        providerInstanceId: ProviderInstanceId.make("cursor"),
        capabilities: CursorProviderCapabilitiesV2,
        sameProvider: true,
        hasStrongNativeSource: true,
        sourceRunStatus: "completed",
        fromSpecificTurn: true,
      });

      assert.equal(result, "portable_context");
    }),
  );

  it.effect("falls back to portable context when Grok ACP cannot fork natively", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const result = yield* policy.decideForkExecution({
        commandId,
        threadId,
        providerInstanceId: ProviderInstanceId.make("grok"),
        capabilities: GrokProviderCapabilitiesV2,
        sameProvider: true,
        hasStrongNativeSource: true,
        sourceRunStatus: "completed",
        fromSpecificTurn: true,
      });

      assert.equal(result, "portable_context");
    }),
  );

  it.effect("returns a typed error when neither native nor portable fork is available", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const error = yield* policy
        .decideForkExecution({
          commandId,
          threadId,
          providerInstanceId: ProviderInstanceId.make("cursor"),
          capabilities: capabilities((current) => ({
            ...current,
            threads: {
              ...current.threads,
              canForkThread: false,
            },
            context: {
              ...current.context,
              canConsumeHandoffSummaries: false,
            },
          })),
          sameProvider: true,
          hasStrongNativeSource: true,
          sourceRunStatus: "completed",
          fromSpecificTurn: true,
        })
        .pipe(Effect.flip);

      assert.instanceOf(error, CommandPolicy.CommandPolicyCapabilityUnsupportedError);
      assert.equal(error.capability, "context_handoff");
    }),
  );

  it.effect("guards rollback behind provider rollback snapshot support", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const error = yield* policy
        .ensureRollback({
          commandId,
          threadId,
          providerInstanceId: ProviderInstanceId.make("codex"),
          capabilities: capabilities((current) => ({
            ...current,
            checkpointing: {
              ...current.checkpointing,
              providerRollbackReturnsSnapshot: false,
            },
          })),
        })
        .pipe(Effect.flip);

      assert.instanceOf(error, CommandPolicy.CommandPolicyCapabilityUnsupportedError);
      assert.equal(error.capability, "rollback_snapshot");
    }),
  );

  it.effect("guards fork-delta handoff behind context handoff capabilities", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const error = yield* policy
        .ensureContextHandoff({
          commandId,
          threadId,
          providerInstanceId: ProviderInstanceId.make("codex"),
          strategy: "fork_delta_context",
          capabilities: capabilities((current) => ({
            ...current,
            context: {
              ...current.context,
              supportsDeltaHandoff: false,
            },
          })),
        })
        .pipe(Effect.flip);

      assert.instanceOf(error, CommandPolicy.CommandPolicyCapabilityUnsupportedError);
      assert.equal(error.capability, "context_handoff");
    }),
  );

  it.effect("guards queued turns behind queued-message support", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const error = yield* policy
        .ensureQueuedMessages({
          commandId,
          threadId,
          providerInstanceId: ProviderInstanceId.make("codex"),
          capabilities: capabilities((current) => ({
            ...current,
            turns: {
              ...current.turns,
              supportsQueuedMessages: false,
            },
          })),
        })
        .pipe(Effect.flip);

      assert.instanceOf(error, CommandPolicy.CommandPolicyCapabilityUnsupportedError);
      assert.equal(error.capability, "queued_messages");
    }),
  );
});
