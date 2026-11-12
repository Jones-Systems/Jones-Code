import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";

import * as TestFixture from "./testFixture.ts";

const original = ProviderInstanceId.make("codex-original");
const alias = ProviderInstanceId.make("codex-alias");
const modelSelection = { instanceId: alias, model: "gpt-5.4", options: [] };
const driver = ProviderDriverKind.make("codex");

const makeLayer = () => {
  const aliasRemoved = Ref.makeUnsafe(false);
  const lookups = Ref.makeUnsafe<ReadonlyArray<ProviderInstanceId>>([]);
  const registry = Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistryV2, {
    get: (instanceId) =>
      Effect.gen(function* () {
        yield* Ref.update(lookups, (ids) => [...ids, instanceId]);
        if (instanceId === alias && (yield* Ref.get(aliasRemoved))) {
          return yield* new ProviderAdapterRegistry.ProviderAdapterRegistryLookupError({
            instanceId,
          });
        }
        return {
          instanceId,
          driver,
          getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
          planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
          openSession: () => Effect.die("Idle provider selection must not open a session"),
        } as ProviderAdapter.ProviderAdapterV2["Service"];
      }),
    list: () => Effect.succeed([original, alias]),
    getMetadata: (instanceId) =>
      Effect.gen(function* () {
        if (instanceId === alias && (yield* Ref.get(aliasRemoved))) {
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
  return {
    aliasRemoved,
    lookups,
    layer: ProviderReplayHarness.layerWithRegistry(
      { name: "provider-switch-before-start" },
      registry,
      { runEffectWorker: false, serverConfigLayer: TestFixture.layer },
    ),
  };
};

it.effect(
  "rebinds an interrupted pending owner without starting work, then dispatches after alias removal",
  () => {
    const test = makeLayer();
    return Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make("thread:before-start");
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create"),
        threadId,
        projectId: ProjectId.make("project:before-start"),
        title: "Retain original input",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "plan",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("message"),
        threadId,
        messageId: MessageId.make("message:original"),
        createdBy: "user",
        creationSource: "web",
        text: "Keep this request",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "defer_start" },
      });
      const pending = yield* orchestrator.getThreadProjection(threadId);
      yield* orchestrator.dispatch({
        type: "run.interrupt",
        commandId: CommandId.make("interrupt"),
        threadId,
        runId: pending.runs[0]!.id,
        holdQueue: true,
      });
      const before = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(before.runs[0]!.startedAt, null);
      assert.equal(before.providerTurns.length, 0);
      assert.ok(
        before.turnItems.some(
          (item) =>
            item.type === "run_interrupt_result" &&
            item.message === "Run interrupted before provider start",
        ),
      );
      const switched = yield* orchestrator.dispatch({
        type: "provider.switch",
        commandId: CommandId.make("switch"),
        threadId,
        modelSelection: { ...modelSelection, instanceId: original },
      });
      assert.deepEqual(
        switched.storedEvents.map(({ event }) => event.type),
        ["provider-thread.updated", "thread.provider-switched"],
      );
      const after = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(after.thread.providerInstanceId, original);
      assert.equal(after.thread.activeProviderThreadId, before.thread.activeProviderThreadId);
      assert.equal(after.providerThreads[0]!.providerInstanceId, original);
      assert.equal(after.providerThreads[0]!.providerSessionId, null);
      assert.deepEqual(after.runs, before.runs);
      assert.deepEqual(after.attempts, before.attempts);
      assert.deepEqual(after.messages, before.messages);
      assert.deepEqual(after.turnItems, before.turnItems);
      assert.deepEqual(after.providerTurns, []);
      assert.deepEqual(after.providerSessions, []);
      assert.equal(after.thread.runtimeMode, before.thread.runtimeMode);
      assert.equal(after.thread.interactionMode, before.thread.interactionMode);
      assert.deepEqual(after.thread.modelSelection.options, before.thread.modelSelection.options);
      yield* Ref.set(test.aliasRemoved, true);
      yield* Ref.set(test.lookups, []);
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("continue"),
        threadId,
        messageId: MessageId.make("message:continue"),
        createdBy: "user",
        creationSource: "web",
        text: "Continue when requested",
        attachments: [],
        modelSelection: { ...modelSelection, instanceId: original },
        dispatchMode: { type: "defer_start" },
      });
      const continued = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(continued.runs.at(-1)!.providerInstanceId, original);
      assert.notInclude(yield* Ref.get(test.lookups), alias);
    }).pipe(Effect.provide(test.layer));
  },
);
