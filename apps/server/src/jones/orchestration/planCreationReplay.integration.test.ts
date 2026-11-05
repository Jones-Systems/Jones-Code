import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  PlanId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as ServerConfig from "../../config.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as Registry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Harness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Database from "../../persistence/Sqlite.ts";
import { dispatchDeliveryEvidence } from "./deliveryEvidence.ts";

it.effect.each([false, true])(
  "replays committed plan create/message receipts before changed defaults and catalog; explicit %s",
  (explicit) => Effect.scoped(Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "jones-plan-receipt-" });
    const configContext = yield* Layer.build(ServerConfig.layerTest(cwd, { prefix: "jones-plan-receipt-state-" }));
    const config = yield* ServerConfig.ServerConfig.pipe(Effect.provide(configContext));
    const instanceId = ProviderInstanceId.make("codex-plan-receipt");
    const selection = { instanceId, model: "gpt-6.1-sol" };
    let catalogAvailable = true;
    let catalogReads = 0;
    const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
      instanceId,
      driver: ProviderDriverKind.make("codex"),
      getCapabilities: () => Effect.sync(() => {
        catalogReads++;
        if (!catalogAvailable) throw new Error("Receipt replay must precede catalog validation");
        return CodexProviderCapabilitiesV2;
      }),
      planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
      openSession: () => Effect.die("This receipt fixture never starts a native provider"),
    };
    const database = Database.layerMemory;
    const runtime = Harness.layerWithRegistryAndServices(
      { name: "plan-create-receipts", runtimePolicyOverride: { cwd } },
      Registry.layerFromAdapters([adapter]),
      { databaseLayer: database, runEffectWorker: false, serverConfigLayer: Layer.succeed(ServerConfig.ServerConfig, config) },
    );
    const layer = Layer.merge(runtime, Receipts.layer.pipe(Layer.provide(database)));
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const receipts = yield* Receipts.CommandReceiptStoreV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const sink = yield* EventSink.EventSinkV2;
      const outbox = yield* Outbox.EffectOutboxV2;
      const projectId = ProjectId.make("project:plan-receipt");
      const sourceId = ThreadId.make("thread:source-plan");
      const planId = PlanId.make("plan:receipt");
      const targetId = ThreadId.make("thread:implementation");
      const create = {
        type: "thread.create", commandId: CommandId.make("plan:create"), threadId: targetId,
        projectId, title: "Implement plan", modelSelection: selection,
        runtimeMode: "full-access", interactionMode: "default", branch: null, worktreePath: cwd,
        createdBy: "user", creationSource: "web",
      } as const;
      yield* orchestrator.dispatch({ ...create, threadId: sourceId, commandId: CommandId.make("plan:source") });
      yield* sink.write({ events: [{
        id: EventId.make("fixture:active-source-plan"), type: "plan.updated", threadId: sourceId,
        nodeId: NodeId.make("fixture:plan-node"), occurredAt: yield* DateTime.now,
        payload: {
          id: planId, threadId: sourceId, runId: null, nodeId: NodeId.make("fixture:plan-node"),
          kind: "proposed_plan", markdown: "# Captured plan", status: "active",
        },
      }] });
      // Discard the transport reply after the real event/receipt transaction committed.
      const committedCreate = yield* orchestrator.dispatch(create);
      assert.equal((yield* dispatchDeliveryEvidence(receipts, create)).status, "accepted");
      const createReads = catalogReads;
      catalogAvailable = false;
      const replayedCreate = yield* orchestrator.dispatch(create);
      assert.isTrue(replayedCreate.replayed);
      assert.equal(replayedCreate.sequence, committedCreate.sequence);
      assert.deepEqual(replayedCreate.storedEvents, committedCreate.storedEvents);
      assert.equal(catalogReads, createReads);
      assert.lengthOf(replayedCreate.storedEvents.filter(({ event }) => event.type === "thread.created" && event.threadId === targetId), 1);
      catalogAvailable = true;
      const message = {
        type: "message.dispatch", commandId: CommandId.make("plan:first-message"),
        threadId: targetId, messageId: MessageId.make("plan:message"),
        text: "Implement the captured plan", attachments: [],
        modelSelection: selection, modelSelectionExplicit: explicit,
        sourcePlanRef: { threadId: sourceId, planId }, dispatchMode: { type: "start_immediately" },
        createdBy: "user", creationSource: "web",
      } as const;
      const committedMessage = yield* orchestrator.dispatch(message);
      const before = yield* projections.getThreadProjection(targetId);
      assert.lengthOf(before.messages.filter((row) => row.id === message.messageId), 1);
      assert.lengthOf(before.runs, 1);
      assert.deepEqual(before.runs[0]!.sourcePlanRef, message.sourcePlanRef);
      assert.deepEqual(before.runs[0]!.modelSelection, selection);
      assert.equal((yield* projections.getPlan(sourceId, planId))?.status, "completed");
      yield* orchestrator.dispatch({
        type: "thread.model-selection.set", commandId: CommandId.make("plan:new-default"), threadId: targetId,
        modelSelection: { ...selection, model: "newer-default-model" },
      });
      const effects = yield* outbox.listByThreadId(targetId);
      const messageReads = catalogReads;
      catalogAvailable = false;
      const replayedMessage = yield* orchestrator.dispatch(message);
      assert.isTrue(replayedMessage.replayed);
      assert.equal(replayedMessage.sequence, committedMessage.sequence);
      assert.deepEqual(replayedMessage.storedEvents, committedMessage.storedEvents);
      assert.equal(catalogReads, messageReads);
      const after = yield* projections.getThreadProjection(targetId);
      assert.lengthOf(after.runs, 1);
      assert.equal(after.runs[0]!.id, before.runs[0]!.id);
      assert.lengthOf(after.messages.filter((row) => row.id === message.messageId), 1);
      assert.deepEqual(yield* outbox.listByThreadId(targetId), effects);
      const receipt = yield* receipts.getByCommandId(message.commandId);
      assert.isTrue(Option.isSome(receipt));
      assert.deepEqual(yield* dispatchDeliveryEvidence(receipts, message), {
        kind: "dispatch", commandId: message.commandId, commandType: "message.dispatch",
        threadId: targetId, status: "accepted",
      });
      catalogAvailable = true;
      const rejectedCreate = { ...create, commandId: CommandId.make("plan:rejected-create") };
      yield* orchestrator.dispatch(rejectedCreate).pipe(Effect.flip);
      assert.equal((yield* dispatchDeliveryEvidence(receipts, rejectedCreate)).status, "rejected");
      const afterRejection = yield* outbox.listByThreadId(targetId);
      catalogAvailable = false;
      const repeated = yield* orchestrator.dispatch(rejectedCreate).pipe(Effect.flip);
      assert.equal(repeated._tag, "OrchestratorCommandPreviouslyRejectedError");
      assert.deepEqual(yield* outbox.listByThreadId(targetId), afterRejection);
      catalogAvailable = true;
      const rejectedMessageTarget = ThreadId.make("thread:rejected-plan-message");
      const createdForRejection = { ...create, threadId: rejectedMessageTarget, commandId: CommandId.make("plan:create-before-message-rejection") };
      yield* orchestrator.dispatch(createdForRejection);
      const rejectedMessage = {
        ...message, threadId: rejectedMessageTarget, commandId: CommandId.make("plan:rejected-message"),
        messageId: MessageId.make("plan:rejected-message"),
        sourcePlanRef: { threadId: sourceId, planId: PlanId.make("missing-plan") },
      };
      yield* orchestrator.dispatch(rejectedMessage).pipe(Effect.flip);
      assert.equal((yield* dispatchDeliveryEvidence(receipts, createdForRejection)).status, "accepted");
      assert.equal((yield* dispatchDeliveryEvidence(receipts, rejectedMessage)).status, "rejected");
      catalogAvailable = false;
      assert.equal((yield* orchestrator.dispatch(rejectedMessage).pipe(Effect.flip))._tag, "OrchestratorCommandPreviouslyRejectedError");
      const retainedTarget = yield* projections.getThreadProjection(rejectedMessageTarget);
      assert.equal(retainedTarget.thread.id, rejectedMessageTarget);
      assert.lengthOf(retainedTarget.runs, 0);
    }).pipe(Effect.provide(layer));
  })).pipe(Effect.provide(NodeServices.layer)),
);
