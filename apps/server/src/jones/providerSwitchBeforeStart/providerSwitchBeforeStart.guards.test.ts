import * as NodeServices from "@effect/platform-node/NodeServices";
import * as FileSystem from "effect/FileSystem";
import * as Exit from "effect/Exit";
import * as ServerConfig from "../../config.ts";
import * as TestFixture from "./testFixture.ts";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderTurnId,
  ThreadId,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderSwitch from "../../orchestration-v2/ProviderSwitchService.ts";
import * as ProviderReplayHarness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";

const current = ProviderInstanceId.make("alias");
const target = ProviderInstanceId.make("original");
const driver = ProviderDriverKind.make("codex");
const threadId = ThreadId.make("thread:guards");
const modelSelection = { instanceId: current, model: "gpt-5.4" };
const now = DateTime.makeUnsafe("2026-10-23T00:00:00Z");

const variants: ReadonlyArray<{
  name: string;
  change: (projection: OrchestrationV2ThreadProjection) => OrchestrationV2ThreadProjection;
}> = [
  {
    name: "wrong pending driver",
    change: (p) => ({
      ...p,
      providerThreads: p.providerThreads.map((thread) => ({
        ...thread,
        driver: ProviderDriverKind.make("claudeAgent"),
      })),
    }),
  },
  {
    name: "missing interrupt proof",
    change: (p) => ({
      ...p,
      turnItems: p.turnItems.filter((item) => item.type !== "run_interrupt_result"),
    }),
  },
  {
    name: "unknown interrupt text",
    change: (p) => ({
      ...p,
      turnItems: p.turnItems.map((item) =>
        item.type === "run_interrupt_result" ? { ...item, message: "Interrupted" } : item,
      ),
    }),
  },
  {
    name: "started run",
    change: (p) => ({ ...p, runs: p.runs.map((run) => ({ ...run, startedAt: now })) }),
  },
  {
    name: "active run",
    change: (p) => ({ ...p, runs: p.runs.map((run) => ({ ...run, status: "starting" })) }),
  },
  {
    name: "started attempt",
    change: (p) => ({
      ...p,
      attempts: p.attempts.map((attempt) => ({ ...attempt, startedAt: now })),
    }),
  },
  {
    name: "saved attempt native identity",
    change: (p) => ({
      ...p,
      attempts: p.attempts.map((attempt) => ({ ...attempt, nativeThreadId: "native:started" })),
    }),
  },
  {
    name: "started node",
    change: (p) => ({ ...p, nodes: p.nodes.map((node) => ({ ...node, startedAt: now })) }),
  },
  {
    name: "native node item",
    change: (p) => ({
      ...p,
      nodes: p.nodes.map((node) => ({
        ...node,
        nativeItemRef: { driver, nativeId: "item:started", strength: "strong" },
      })),
    }),
  },
  {
    name: "native conversation head",
    change: (p) => ({
      ...p,
      providerThreads: p.providerThreads.map((thread) => ({
        ...thread,
        nativeConversationHeadRef: { driver, nativeId: "head:started", strength: "strong" },
      })),
    }),
  },
  {
    name: "provider turn without native cursor",
    change: (p) => ({
      ...p,
      providerTurns: [
        {
          id: ProviderTurnId.make("turn:started"),
          providerThreadId: p.providerThreads[0]!.id,
          nodeId: p.nodes[0]!.id,
          runAttemptId: p.attempts[0]!.id,
          nativeTurnRef: null,
          ordinal: 1,
          status: "interrupted",
          startedAt: null,
          completedAt: now,
        },
      ],
    }),
  },
  {
    name: "live session",
    change: (p) => ({
      ...p,
      providerSessions: [
        {
          id: ProviderSessionId.make("session:started"),
          driver,
          providerInstanceId: current,
          status: "ready",
          cwd: "/synthetic",
          model: modelSelection.model,
          capabilities: CodexProviderCapabilitiesV2,
          createdAt: now,
          updatedAt: now,
          lastError: null,
        },
      ],
    }),
  },
  {
    name: "stopped session",
    change: (p) => ({
      ...p,
      providerSessions: [
        {
          id: ProviderSessionId.make("session:stopped"),
          driver,
          providerInstanceId: current,
          status: "stopped",
          cwd: "/synthetic",
          model: modelSelection.model,
          capabilities: CodexProviderCapabilitiesV2,
          createdAt: now,
          updatedAt: now,
          lastError: null,
        },
      ],
    }),
  },
  { name: "missing attempt", change: (p) => ({ ...p, attempts: [] }) },
  { name: "missing run", change: (p) => ({ ...p, runs: [] }) },
  {
    name: "foreign run",
    change: (p) => ({
      ...p,
      runs: p.runs.map((run) => ({ ...run, threadId: ThreadId.make("foreign") })),
    }),
  },
  { name: "missing current metadata", change: (p) => p },
  { name: "unloaded evidence", change: (p) => p },
];

it.effect.each(variants)("keeps the saved-conversation guard for $name", ({ name, change }) => {
  const missingMetadata = Ref.makeUnsafe(false);
  const adapter = (instanceId: ProviderInstanceId) =>
    ({
      instanceId,
      driver,
      getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
      planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
      openSession: () => Effect.die("Guard checks must not open sessions"),
    }) as ProviderAdapter.ProviderAdapterV2["Service"];
  const registry = Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistryV2, {
    get: (id) => Effect.succeed(adapter(id)),
    list: () => Effect.succeed([current, target]),
    getMetadata: (instanceId) =>
      Effect.gen(function* () {
        if (instanceId === current && (yield* Ref.get(missingMetadata))) {
          return yield* new ProviderAdapterRegistry.ProviderAdapterRegistryLookupError({
            instanceId,
          });
        }
        return {
          driver,
          continuationKey: "codex:shared",
          enabled: true,
          capabilities: CodexProviderCapabilitiesV2,
        };
      }),
  });
  const layer = Layer.merge(
    ProviderSwitch.layer.pipe(Layer.provide(registry)),
    ProviderReplayHarness.layerWithRegistry(
      { name: "provider-switch-before-start-guards" },
      registry,
      { runEffectWorker: false, serverConfigLayer: TestFixture.layer },
    ),
  );
  return Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const service = yield* ProviderSwitch.ProviderSwitchServiceV2;
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("create"),
      threadId,
      projectId: ProjectId.make("project:guards"),
      title: "Guard fixture",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("message"),
      threadId,
      messageId: MessageId.make("message:guards"),
      text: "Guard fixture",
      attachments: [],
      createdBy: "user",
      creationSource: "web",
      dispatchMode: { type: "defer_start" },
    });
    const pending = yield* orchestrator.getThreadProjection(threadId);
    yield* orchestrator.dispatch({
      type: "run.interrupt",
      commandId: CommandId.make("interrupt"),
      threadId,
      runId: pending.runs[0]!.id,
    });
    const before = yield* orchestrator.getThreadProjection(threadId);
    const selection = { instanceId: target, model: modelSelection.model };
    const valid = yield* service.plan({
      projection: before,
      beforeStartEvidence: before,
      targetModelSelection: selection,
    });
    assert.equal(valid.rebindPendingProviderThread?.id, before.thread.activeProviderThreadId);
    if (name === "missing current metadata") yield* Ref.set(missingMetadata, true);
    const input = change(before);
    const error = yield* service
      .plan({
        projection: input,
        targetModelSelection: selection,
        ...(name === "unloaded evidence" ? {} : { beforeStartEvidence: input }),
      })
      .pipe(Effect.flip);
    assert.instanceOf(error, ProviderSwitch.ProviderSwitchPlanError);
    assert.nestedPropertyVal(
      error,
      "cause.cause",
      name === "missing current metadata"
        ? "Cannot switch Codex accounts because the saved conversation is not compatible with the target account. Check that both accounts share the Codex sessions directory."
        : "Cannot switch Codex accounts without a valid saved conversation. Check that both accounts share the Codex sessions directory.",
    );
  }).pipe(Effect.provide(layer));
});

it.effect.each(["success", "failure"] as const)(
  "cleans the captured fixture directory on %s",
  (outcome) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const captured = yield* Ref.make<string | undefined>(undefined);
      const result = yield* Effect.gen(function* () {
        const config = yield* ServerConfig.ServerConfig;
        yield* Ref.set(captured, config.baseDir);
        assert.equal(yield* fs.exists(config.baseDir), true);
        if (outcome === "failure") return yield* Effect.fail("deliberate fixture failure");
      }).pipe(Effect.provide(TestFixture.layer), Effect.exit);
      assert.equal(Exit.isFailure(result), outcome === "failure");
      const directory = yield* Ref.get(captured);
      assert.isDefined(directory);
      assert.equal(yield* fs.exists(directory!), false);
    }).pipe(Effect.provide(NodeServices.layer)),
);
