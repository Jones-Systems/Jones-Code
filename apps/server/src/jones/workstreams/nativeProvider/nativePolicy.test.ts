import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
  WorkstreamsNativeSettlementRequest,
  WorkstreamsNativeSettlementResponse,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";
import * as NativeEvidence from "./evidence.ts";
import * as CheckpointService from "../../../orchestration-v2/CheckpointService.ts";
import * as CommandPolicy from "../../../orchestration-v2/CommandPolicy.ts";
import * as CommandReceiptStore from "../../../orchestration-v2/CommandReceiptStore.ts";
import * as ContextHandoffService from "../../../orchestration-v2/ContextHandoffService.ts";
import * as EffectOutbox from "../../../orchestration-v2/EffectOutbox.ts";
import * as EventSink from "../../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../../orchestration-v2/EventStore.ts";
import * as IdAllocator from "../../../orchestration-v2/IdAllocator.ts";
import * as Orchestrator from "../../../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../../../orchestration-v2/ProjectionStore.ts";
import * as ProjectStore from "../../../orchestration-v2/ProjectStore.ts";
import * as ProviderAdapterRegistry from "../../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderContinuationRequests from "../../../orchestration-v2/ProviderContinuationRequests.ts";
import * as ProviderSessionManager from "../../../orchestration-v2/ProviderSessionManager.ts";
import * as ProviderSwitchService from "../../../orchestration-v2/ProviderSwitchService.ts";
import * as RuntimePolicy from "../../../orchestration-v2/RuntimePolicy.ts";
import * as ThreadForkService from "../../../orchestration-v2/ThreadForkService.ts";
import * as TurnItemPositionStore from "../../../orchestration-v2/TurnItemPositionStore.ts";
import { NativeProviderAttempts, NativeProviderAttemptsLive } from "./attemptRepository.ts";
import {
  makeWorkstreamsNativeProvider,
  sha256Bytes,
  type NativeProviderPorts,
} from "./service.ts";
import { makeProviderFixture, binding, request, requestBytesSha256, now } from "./testFixtures.ts";

const database = SqlitePersistenceMemory;
const stores = Layer.mergeAll(
  EventStore.layer,
  ProjectionStore.layer,
  ProjectStore.layer,
  CommandReceiptStore.layer,
  EffectOutbox.layer,
  TurnItemPositionStore.layer,
).pipe(Layer.provide(database));
// Settlement, its policy, projections, event sink and receipts are real. These
// unrelated capabilities fail if a native settlement tries to invoke them.
const runtime = Orchestrator.layer.pipe(
  Layer.provide(
    Layer.mergeAll(
      stores,
      EventSink.layerFromStores.pipe(Layer.provide(Layer.mergeAll(stores, database))),
      CommandPolicy.layer,
      IdAllocator.layer,
      RuntimePolicy.layer,
      ProviderAdapterRegistry.makeLayer([]),
      ProviderContinuationRequests.layer,
      Layer.mock(CheckpointService.CheckpointServiceV2)({}),
      Layer.mock(ContextHandoffService.ContextHandoffServiceV2)({}),
      Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({}),
      Layer.mock(ProviderSwitchService.ProviderSwitchServiceV2)({}),
      Layer.mock(ThreadForkService.ThreadForkServiceV2)({}),
      NodeServices.layer,
    ),
  ),
);
const testLayer = Layer.mergeAll(
  runtime,
  stores,
  database,
  NativeProviderAttemptsLive.pipe(Layer.provide(database)),
  NativeEvidence.makeNativeProviderEvidenceLayer().pipe(Layer.provide(database)),
);
const fixture = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const attempts = yield* NativeProviderAttempts;
  const sql = yield* SqlClient.SqlClient;
  const evidence = yield* NativeEvidence.NativeProviderEvidence;
  const threadId = ThreadId.make(request.identity.native_id);
  const at = DateTime.makeUnsafe(now);
  const instanceId = ProviderInstanceId.make("codex");
  const modelSelection = { instanceId, model: "synthetic-model" };
  const thread: OrchestrationV2AppThread = {
    createdBy: "user",
    creationSource: "web",
    id: threadId,
    projectId: ProjectId.make("project-synthetic"),
    title: "Synthetic thread",
    providerInstanceId: instanceId,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: at,
    updatedAt: at,
    archivedAt: null,
    deletedAt: null,
    settledAt: null,
    settledOverride: null,
    lastVisitedAt: null,
  };
  yield* projections.apply({
    id: EventId.make("thread-create-synthetic"),
    type: "thread.created",
    threadId,
    occurredAt: at,
    payload: thread,
  });
  const ports: NativeProviderPorts = {
    authority: makeProviderFixture().ports.authority,
    build: Effect.succeed(Option.some(binding.build)),
    threadExists: (id) =>
      orchestrator.getThreadShell(id).pipe(Effect.map((shell) => shell !== null)),
    engine: orchestrator,
    evidence,
    attempts,
  };
  return {
    orchestrator,
    projections,
    evidence,
    attempts,
    sql,
    threadId,
    at,
    modelSelection,
    ports,
  };
});

it.effect(
  "native policy denial retains pins, snooze and history and produces only a rejected receipt",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const thread = (yield* f.projections.getThreadProjection(f.threadId)).thread;
      yield* f.projections.apply({
        id: EventId.make("control-state-synthetic"),
        type: "thread.metadata-updated",
        threadId: f.threadId,
        occurredAt: f.at,
        payload: {
          ...thread,
          pinnedAt: f.at,
          pinOrderKey: "synthetic-order",
          snoozedAt: f.at,
          snoozedUntil: DateTime.makeUnsafe("2030-01-01T00:00:00.000Z"),
        },
      });
      yield* f.projections.apply({
        id: EventId.make("run-synthetic"),
        type: "run.created",
        threadId: f.threadId,
        occurredAt: f.at,
        payload: {
          id: RunId.make("run-synthetic"),
          threadId: f.threadId,
          ordinal: 1,
          providerInstanceId: f.modelSelection.instanceId,
          modelSelection: f.modelSelection,
          providerThreadId: null,
          userMessageId: MessageId.make("history-synthetic"),
          rootNodeId: null,
          activeAttemptId: null,
          status: "running",
          requestedAt: f.at,
          startedAt: f.at,
          completedAt: null,
          checkpointId: null,
          contextHandoffId: null,
        },
      });
      const before = yield* f.projections.getThreadProjection(f.threadId);
      const provider = makeWorkstreamsNativeProvider(f.ports);
      const result = yield* provider.settle(binding, request, requestBytesSha256);
      yield* Schema.decodeUnknownEffect(WorkstreamsNativeSettlementResponse)(result);
      assert.strictEqual(result.state, "terminal");
      if (result.state === "terminal") assert.strictEqual(result.result.native_outcome, "denied");
      assert.deepEqual(yield* f.projections.getThreadProjection(f.threadId), before);
      assert.strictEqual((yield* f.sql`SELECT sequence FROM orchestration_events`).length, 0);
      assert.strictEqual(
        (yield* f.sql`SELECT command_id FROM orchestration_command_receipts`).length,
        1,
      );
      const attempt = Option.getOrThrow(yield* f.attempts.get(request));
      const snapshot = yield* f.evidence.readSnapshotByCommandId(
        attempt.nativeCommandId,
        ThreadId.make(attempt.request.identity.native_id),
      );
      const receipt = Option.getOrThrow(snapshot.receipt);
      assert.strictEqual(receipt.status, "rejected");
      assert.strictEqual(receipt.commandType, "thread.settle");
      assert.deepEqual(snapshot.events, []);
      assert.strictEqual(Object.hasOwn(receipt, "error"), false);
      assert.deepEqual(yield* provider.lookup(binding, request, requestBytesSha256), result);
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "real Orchestrator settles and unsettles with exact V2 receipt and event associations",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const provider = makeWorkstreamsNativeProvider(f.ports);
      for (const action of ["settle", "unsettle"] as const) {
        const input = {
          ...request,
          command_id: `registry-synthetic-${action}`,
          native_action: action,
        };
        const digest = sha256Bytes(
          yield* Schema.encodeEffect(Schema.fromJsonString(WorkstreamsNativeSettlementRequest))(
            input,
          ),
        );
        const result = yield* provider.settle(binding, input, digest);
        yield* Schema.decodeUnknownEffect(WorkstreamsNativeSettlementResponse)(result);
        assert.strictEqual(result.state, "terminal");
        if (result.state !== "terminal") throw new Error("Expected durable terminal evidence.");
        assert.strictEqual(result.result.native_outcome, "committed");
        const attempt = Option.getOrThrow(yield* f.attempts.get(input));
        assert.notStrictEqual(attempt.dispatchStartedAt, null);
        assert.notStrictEqual(attempt.nativeCommandId, input.command_id);
        const snapshot = yield* f.evidence.readSnapshotByCommandId(
        attempt.nativeCommandId,
        ThreadId.make(attempt.request.identity.native_id),
      );
        const receipt = Option.getOrThrow(snapshot.receipt);
        assert.strictEqual(receipt.commandId, attempt.nativeCommandId);
        assert.strictEqual(receipt.commandType, `thread.${action}`);
        assert.strictEqual(receipt.aggregateKind, "thread");
        assert.strictEqual(receipt.aggregateId, input.identity.native_id);
        assert.strictEqual(receipt.status, "accepted");
        assert.ok(snapshot.events.length > 0 && snapshot.events.length <= 256);
        for (const [index, event] of snapshot.events.entries()) {
          assert.strictEqual(event.applicationEventVersion, 2);
          assert.strictEqual(event.commandId, attempt.nativeCommandId);
          assert.strictEqual(event.aggregateKind, "thread");
          assert.strictEqual(event.aggregateId, input.identity.native_id);
          assert.ok(event.sequence > 0 && event.sequence <= receipt.resultSequence);
          if (index > 0)
            assert.strictEqual(event.sequence, snapshot.events[index - 1]!.sequence + 1);
        }
        const last = snapshot.events.at(-1)!;
        assert.strictEqual(last.sequence, receipt.resultSequence);
        assert.strictEqual(last.occurredAt, receipt.acceptedAt);
        const settlement = snapshot.events.filter(
          (event) => event.type === "thread.settled" || event.type === "thread.unsettled",
        );
        assert.strictEqual(settlement.length, 1);
        assert.strictEqual(
          settlement[0]!.type,
          action === "settle" ? "thread.settled" : "thread.unsettled",
        );
        assert.strictEqual(result.settlement_event!.eventId, settlement[0]!.eventId);
        assert.strictEqual(settlement[0]!.eventId.length, 42);
        assert.match(
          settlement[0]!.eventId,
          /^event:[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
        );
        const stored = yield* f.sql<{ event_id: string }>`
        SELECT event_id FROM orchestration_events WHERE sequence = ${receipt.resultSequence}
      `;
        assert.strictEqual(stored.length, 1);
        assert.strictEqual(stored[0]!.event_id, result.settlement_event!.eventId);
        assert.strictEqual(result.native_receipt!.resultSequence, receipt.resultSequence);
        const before = yield* f.sql`SELECT sequence FROM orchestration_events ORDER BY sequence`;
        const reopened = makeWorkstreamsNativeProvider(f.ports);
        assert.deepEqual(yield* reopened.settle(binding, input, digest), result);
        assert.deepEqual(yield* reopened.lookup(binding, input, digest), result);
        assert.deepEqual(
          yield* f.sql`SELECT sequence FROM orchestration_events ORDER BY sequence`,
          before,
        );
        assert.strictEqual(
          (yield* f.orchestrator.getThreadShell(f.threadId))!.settledOverride,
          action === "settle" ? "settled" : "active",
        );
      }
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "lost reply after real dispatch remains observation-only across provider reconstruction",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const committed = yield* Deferred.make<void>();
      let dispatches = 0;
      const provider = makeWorkstreamsNativeProvider({
        ...f.ports,
        engine: {
          dispatch: (command) =>
            Effect.gen(function* () {
              dispatches += 1;
              yield* f.orchestrator.dispatch(command);
              yield* Deferred.succeed(committed, undefined);
              return yield* Effect.never;
            }),
        },
      });
      const pending = yield* Effect.forkChild(
        provider.settle(binding, request, requestBytesSha256),
      );
      yield* Deferred.await(committed);
      yield* Fiber.interrupt(pending);
      const before = yield* f.sql`SELECT sequence FROM orchestration_events ORDER BY sequence`;
      const recovered = yield* provider.lookup(binding, request, requestBytesSha256);
      yield* Schema.decodeUnknownEffect(WorkstreamsNativeSettlementResponse)(recovered);
      assert.strictEqual(recovered.state, "terminal");
      if (recovered.state === "terminal")
        assert.strictEqual(recovered.result.native_outcome, "committed");
      assert.deepEqual(yield* provider.settle(binding, request, requestBytesSha256), recovered);
      assert.deepEqual(
        yield* makeWorkstreamsNativeProvider(f.ports).settle(binding, request, requestBytesSha256),
        recovered,
      );
      assert.strictEqual(dispatches, 1);
      assert.deepEqual(
        yield* f.sql`SELECT sequence FROM orchestration_events ORDER BY sequence`,
        before,
      );
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "colon-bearing thread settlement preserves the exact compact durable ID on the closed wire",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const threadId = ThreadId.make("thread:delegated:synthetic");
      const thread = (yield* f.projections.getThreadProjection(f.threadId)).thread;
      yield* f.projections.apply({
        id: EventId.make("colon-thread-create-synthetic"),
        type: "thread.created",
        threadId,
        occurredAt: f.at,
        payload: {
          ...thread,
          id: threadId,
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
        },
      });
      const input = {
        ...request,
        command_id: "registry-colon-thread-settle",
        identity: { ...request.identity, native_id: threadId },
      };
      const digest = sha256Bytes(
        yield* Schema.encodeEffect(Schema.fromJsonString(WorkstreamsNativeSettlementRequest))(
          input,
        ),
      );
      const provider = makeWorkstreamsNativeProvider(f.ports);
      const result = yield* provider.settle(binding, input, digest);
      yield* Schema.decodeUnknownEffect(WorkstreamsNativeSettlementResponse)(result);
      assert.strictEqual(result.state, "terminal");
      if (result.state !== "terminal") throw new Error("Expected durable terminal evidence.");
      assert.strictEqual(result.result.native_outcome, "committed");
      const attempt = Option.getOrThrow(yield* f.attempts.get(input));
      const snapshot = yield* f.evidence.readSnapshotByCommandId(
        attempt.nativeCommandId,
        ThreadId.make(attempt.request.identity.native_id),
      );
      const receipt = Option.getOrThrow(snapshot.receipt);
      const event = snapshot.events.filter((event) => event.type === "thread.settled");
      assert.strictEqual(event.length, 1);
      assert.strictEqual(event[0]!.eventId, result.settlement_event!.eventId);
      assert.strictEqual(event[0]!.eventId.length, 42);
      assert.match(
        event[0]!.eventId,
        /^event:[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
      );
      const stored = yield* f.sql<{ event_id: string }>`
      SELECT event_id FROM orchestration_events WHERE sequence = ${receipt.resultSequence}
    `;
      assert.strictEqual(stored.length, 1);
      assert.strictEqual(stored[0]!.event_id, result.settlement_event!.eventId);
      const before = yield* f.sql`SELECT sequence FROM orchestration_events ORDER BY sequence`;
      assert.deepEqual(yield* provider.settle(binding, input, digest), result);
      assert.deepEqual(yield* provider.lookup(binding, input, digest), result);
      assert.deepEqual(
        yield* f.sql`SELECT sequence FROM orchestration_events ORDER BY sequence`,
        before,
      );
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "manual settlement uses compact IDs while ordinary command events retain their full context",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const pinCommandId = CommandId.make("manual:pin:synthetic");
      yield* f.orchestrator.dispatch({
        type: "thread.pin",
        commandId: pinCommandId,
        threadId: f.threadId,
      });
      const pin = yield* f.evidence.readSnapshotByCommandId(pinCommandId, f.threadId);
      const pinned = pin.events.filter((event) => event.type === "thread.pinned");
      assert.strictEqual(pinned.length, 1);
      const prefix = `event:thread:${encodeURIComponent(f.threadId)}:command:${encodeURIComponent(pinCommandId)}:`;
      assert.strictEqual(pinned[0]!.eventId.slice(0, prefix.length), prefix);
      assert.match(
        pinned[0]!.eventId.slice(prefix.length),
        /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
      );
      const commandId = CommandId.make("manual:settle:synthetic");
      yield* f.orchestrator.dispatch({ type: "thread.settle", commandId, threadId: f.threadId });
      const snapshot = yield* f.evidence.readSnapshotByCommandId(commandId, f.threadId);
      const receipt = Option.getOrThrow(snapshot.receipt);
      const settled = snapshot.events.filter((event) => event.type === "thread.settled");
      assert.strictEqual(settled.length, 1);
      assert.strictEqual(settled[0]!.eventId.length, 42);
      assert.match(
        settled[0]!.eventId,
        /^event:[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
      );
      const stored = yield* f.sql<{ event_id: string }>`
      SELECT event_id FROM orchestration_events WHERE sequence = ${settled[0]!.sequence}
    `;
      assert.strictEqual(stored[0]!.event_id, settled[0]!.eventId);
      const before = yield* f.sql`SELECT sequence FROM orchestration_events ORDER BY sequence`;
      const replay = yield* f.orchestrator.dispatch({
        type: "thread.settle",
        commandId,
        threadId: f.threadId,
      });
      assert.strictEqual(replay.sequence, receipt.resultSequence);
      assert.deepEqual(yield* f.evidence.readSnapshotByCommandId(commandId, f.threadId), snapshot);
      assert.deepEqual(
        yield* f.sql`SELECT sequence FROM orchestration_events ORDER BY sequence`,
        before,
      );
    }).pipe(Effect.provide(testLayer)),
);
