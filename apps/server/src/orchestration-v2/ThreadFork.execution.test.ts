import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  ContextTransferId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";

import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import { ClaudeProviderCapabilitiesV2 } from "./Adapters/ClaudeAdapterV2.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import type { ProviderAdapterV2Shape } from "./ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./testkit/ProviderReplayHarness.ts";

const forkCases = (["codex", "claudeAgent"] as const).flatMap((driverName) => {
  const driver = ProviderDriverKind.make(driverName);
  const instanceId = ProviderInstanceId.make(driver);
  const modelSelection = { instanceId, model: "test-model" };
  const adapter: ProviderAdapterV2Shape = {
    instanceId,
    driver,
    getCapabilities: () =>
      Effect.succeed(
        driver === "codex" ? CodexProviderCapabilitiesV2 : ClaudeProviderCapabilitiesV2,
      ),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: () => Effect.die("Execution is paused after dispatch for handoff inspection"),
  };
  const layer = Layer.unwrap(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const workspaceRoot = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-fork-boundary-workspace-",
      });
      return makeOrchestratorV2ReplayLayerWithRegistry(
        { name: `fork-boundary-${driver}` },
        ProviderAdapterRegistry.makeLayer([adapter]),
        {
          runEffectWorker: false,
          checkoutFixture: {
            projects: [
              {
                projectId: ProjectId.make("fork-boundary-project"),
                title: "Fork boundary",
                workspaceRoot,
              },
            ],
            resolvePath: () => undefined,
          },
        },
      );
    }),
  ).pipe(Layer.provide(NodeServices.layer));

  return (["failed", "interrupted", "cancelled"] as const).map((status) => ({
    driver,
    status,
    instanceId,
    modelSelection,
    layer,
  }));
});

it.effect.each(forkCases)(
  "bounds $driver context when continuing a fork of a $status run",
  ({ driver, status, instanceId, modelSelection, layer }) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const now = yield* DateTime.now;
      const sourceThreadId = ThreadId.make("fork-boundary-source");
      const targetThreadId = ThreadId.make("fork-boundary-target");
      const providerThreadId = ProviderThreadId.make("fork-boundary-native-thread");
      const sourceRunId = RunId.make("fork-boundary-source-run");
      const attemptId = RunAttemptId.make("interrupted-source-attempt");
      const providerTurnId = ProviderTurnId.make("interrupted-source-turn");
      const rootNodeId = NodeId.make("interrupted-source-root");

      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create-source"),
        threadId: sourceThreadId,
        projectId: ProjectId.make("fork-boundary-project"),
        title: "Fork boundary source",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
      yield* eventSink.write({
        events: [
          {
            id: EventId.make("source-provider-thread"),
            type: "provider-thread.updated",
            threadId: sourceThreadId,
            occurredAt: now,
            payload: {
              id: providerThreadId,
              driver,
              providerInstanceId: instanceId,
              providerSessionId: null,
              appThreadId: sourceThreadId,
              ownerNodeId: null,
              nativeThreadRef: { driver, nativeId: "native-source", strength: "strong" },
              nativeConversationHeadRef: null,
              status: "idle",
              firstRunOrdinal: 1,
              lastRunOrdinal: 2,
              handoffIds: [],
              forkedFrom: null,
              createdAt: now,
              updatedAt: now,
            },
          },
        ],
      });
      // A cancelled queue entry has no provider turn; an early interruption
      // can have a turn but no native assistant cursor.
      if (status === "interrupted") {
        yield* eventSink.write({
          events: [
            {
              id: EventId.make("source-attempt"),
              type: "run-attempt.created",
              threadId: sourceThreadId,
              runId: sourceRunId,
              occurredAt: now,
              payload: {
                id: attemptId,
                runId: sourceRunId,
                attemptOrdinal: 1,
                rootNodeId,
                providerInstanceId: instanceId,
                providerThreadId,
                providerTurnId,
                reason: "initial",
                status,
                startedAt: now,
                completedAt: now,
              },
            },
            {
              id: EventId.make("source-provider-turn"),
              type: "provider-turn.updated",
              threadId: sourceThreadId,
              occurredAt: now,
              payload: {
                id: providerTurnId,
                providerThreadId,
                nodeId: rootNodeId,
                runAttemptId: attemptId,
                nativeTurnRef: { driver, nativeId: "turn:synthetic", strength: "weak" },
                ordinal: 1,
                status,
                startedAt: now,
                completedAt: now,
              },
            },
          ],
        });
      }
      for (const ordinal of [1, 2]) {
        const runId = ordinal === 1 ? sourceRunId : RunId.make("later-run");
        const messageId = MessageId.make(`source-message-${ordinal}`);
        yield* eventSink.write({
          events: [
            {
              id: EventId.make(`run-${ordinal}`),
              type: "run.created",
              threadId: sourceThreadId,
              runId,
              occurredAt: now,
              payload: {
                id: runId,
                threadId: sourceThreadId,
                ordinal,
                providerInstanceId: instanceId,
                modelSelection,
                providerThreadId,
                userMessageId: messageId,
                rootNodeId: null,
                activeAttemptId: ordinal === 1 && status === "interrupted" ? attemptId : null,
                status: ordinal === 1 ? status : "completed",
                queuePosition: null,
                requestedAt: now,
                startedAt: now,
                completedAt: now,
                checkpointId: null,
                contextHandoffId: null,
              },
            },
            {
              id: EventId.make(`item-${ordinal}`),
              type: "turn-item.updated",
              threadId: sourceThreadId,
              runId,
              occurredAt: now,
              payload: {
                id: TurnItemId.make(`item-${ordinal}`),
                threadId: sourceThreadId,
                runId,
                nodeId: null,
                providerThreadId,
                providerTurnId: null,
                nativeItemRef: null,
                parentItemId: null,
                ordinal,
                status: "completed",
                title: null,
                startedAt: now,
                completedAt: now,
                updatedAt: now,
                type: "user_message",
                createdBy: "user",
                creationSource: "web",
                inputIntent: "turn_start",
                messageId,
                text: ordinal === 1 ? "INCLUDED_SOURCE_MARKER" : "EXCLUDED_LATER_MARKER",
                attachments: [],
              },
            },
          ],
        });
      }
      yield* orchestrator.dispatch({
        type: "thread.fork",
        commandId: CommandId.make("fork-source"),
        sourceThreadId,
        targetThreadId,
        sourcePoint: { type: "run", runId: sourceRunId },
        createdBy: "user",
        creationSource: "web",
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("continue-fork"),
        threadId: targetThreadId,
        messageId: MessageId.make("continue-fork"),
        text: "Continue from the selected source run",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      const target = yield* orchestrator.getThreadProjection(targetThreadId);
      assert.equal(target.contextTransfers[0]?.resolution?.strategy, "portable_context");
      assert.lengthOf(target.contextHandoffs, 1);
      const handoff = target.contextHandoffs[0]!;
      const history = handoff.history?.messages.map((message) => message.text).join("\n") ?? "";
      assert.include(`${handoff.summaryText}\n${history}`, "INCLUDED_SOURCE_MARKER");
      assert.notInclude(`${handoff.summaryText}\n${history}`, "EXCLUDED_LATER_MARKER");
      assert.isNull(target.providerThreads[0]?.forkedFrom);
    }).pipe(Effect.provide(layer)),
);

// The provider-switch row registers its target so planning succeeds and the defer guard decides.
const switchTargetInstanceId = ProviderInstanceId.make("different-provider");
const providerSwitchGuardLayer = Layer.unwrap(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const workspaceRoot = yield* fs.makeTempDirectoryScoped({
      prefix: "t3-fork-defer-guard-workspace-",
    });
    const adapter = (
      instanceId: ProviderInstanceId,
      driver: ProviderDriverKind,
    ): ProviderAdapterV2Shape => ({
      instanceId,
      driver,
      getCapabilities: () =>
        Effect.succeed(
          driver === "codex" ? CodexProviderCapabilitiesV2 : ClaudeProviderCapabilitiesV2,
        ),
      planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
      openSession: () => Effect.die("Execution is paused after dispatch for guard inspection"),
    });
    return makeOrchestratorV2ReplayLayerWithRegistry(
      { name: "fork-defer-guard-provider-switch" },
      ProviderAdapterRegistry.makeLayer([
        adapter(ProviderInstanceId.make("codex"), ProviderDriverKind.make("codex")),
        adapter(switchTargetInstanceId, ProviderDriverKind.make("claudeAgent")),
      ]),
      {
        runEffectWorker: false,
        checkoutFixture: {
          projects: [
            {
              projectId: ProjectId.make("fork-boundary-project"),
              title: "Fork boundary",
              workspaceRoot,
            },
          ],
          resolvePath: () => undefined,
        },
      },
    );
  }),
).pipe(Layer.provide(NodeServices.layer));

it.effect.each(["mergeback", "provider switch"] as const)(
  "refuses forced complex defer for a $0 before checkpoint/provider effects",
  (reason) =>
    Effect.gen(function* () {
      const { instanceId, modelSelection, driver } = forkCases[0]!;
      const layer = reason === "provider switch" ? providerSwitchGuardLayer : forkCases[0]!.layer;
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const sink = yield* EventSink.EventSinkV2;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("defer-guard-root");
        const providerThreadId = ProviderThreadId.make("defer-guard-native");
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make("defer-guard-create"),
          threadId,
          projectId: ProjectId.make("fork-boundary-project"),
          title: "Defer guard",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdBy: "user",
          creationSource: "web",
        });
        yield* sink.write({
          events: [
            {
              id: EventId.make("defer-guard-native-event"),
              type: "provider-thread.updated",
              threadId,
              occurredAt: now,
              payload: {
                id: providerThreadId,
                driver,
                providerInstanceId: instanceId,
                providerSessionId: null,
                appThreadId: threadId,
                ownerNodeId: null,
                nativeThreadRef: { driver, nativeId: "defer-guard-source", strength: "strong" },
                nativeConversationHeadRef: null,
                status: "idle",
                firstRunOrdinal: 1,
                lastRunOrdinal: 1,
                handoffIds: [],
                forkedFrom: null,
                createdAt: now,
                updatedAt: now,
              },
            },
          ],
        });
        if (reason === "mergeback")
          yield* sink.write({
            events: [
              {
                id: EventId.make("defer-guard-transfer-event"),
                type: "context-transfer.created",
                threadId,
                occurredAt: now,
                payload: {
                  id: ContextTransferId.make("defer-guard-transfer"),
                  type: "merge_back",
                  sourceThreadId: ThreadId.make("defer-guard-fork"),
                  targetThreadId: threadId,
                  sourcePoint: {
                    threadId: ThreadId.make("defer-guard-fork"),
                    runId: RunId.make("defer-guard-source-run"),
                  },
                  basePoint: null,
                  sourceProviderInstanceId: instanceId,
                  targetProviderInstanceId: null,
                  targetRunId: null,
                  status: "pending",
                  resolution: null,
                  createdBy: "user",
                  error: null,
                  createdAt: now,
                  updatedAt: now,
                  consumedAt: null,
                },
              },
            ],
          });
        const error = yield* orchestrator
          .dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("defer-guard-send"),
            threadId,
            messageId: MessageId.make("defer-guard-message"),
            text: "Forced deferred command",
            attachments: [],
            modelSelection:
              reason === "provider switch"
                ? { ...modelSelection, instanceId: switchTargetInstanceId }
                : modelSelection,
            dispatchMode: { type: "defer_start" },
            createdBy: "user",
            creationSource: "web",
          })
          .pipe(Effect.flip);
        assert.equal(error._tag, "OrchestratorDispatchError");
        if (error._tag === "OrchestratorDispatchError")
          assert.equal(
            error.cause,
            "Deferred standalone preparation supports only a pending fork.",
          );
        const projection = yield* orchestrator.getThreadProjection(threadId);
        assert.lengthOf(projection.runs, 0);
        assert.lengthOf(projection.checkpointScopes, 0);
      }).pipe(Effect.provide(layer));
    }),
);
