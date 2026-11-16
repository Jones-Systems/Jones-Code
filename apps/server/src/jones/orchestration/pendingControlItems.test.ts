import { assert, it } from "@effect/vitest";
import {
  EventId,
  NodeId,
  MessageId,
  RunId,
  ProviderSessionId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
  OrchestrationV2ThreadBoundedSnapshot,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2TurnItem,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import {
  boundedSnapshotProjection,
  omitLocalVisibleTurnItems,
} from "@t3tools/shared/orchestrationV2BoundedSnapshot";
import { pendingControlTurnItems } from "./pendingControlItems.ts";
import { dedupeSnapshotBodies } from "@t3tools/shared/orchestrationV2DedupedBodies";
import {
  v2Now,
  v2Projection,
} from "../../../../../packages/client-runtime/src/state/orchestrationV2TestFixtures.ts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Schema from "effect/Schema";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import { buildBoundedThreadProjection } from "../../orchestration-v2/threadHistoryPaging.ts";
import {
  shouldPublishShellApplicationEvent,
  toShellApplicationEvent,
  shellStreamItemFromThreadShell,
} from "../../orchestration-v2/ShellStream.ts";

const snapshotCodec = Schema.fromJsonString(
  Schema.toCodecJson(OrchestrationV2ThreadBoundedSnapshot),
);

const layer = Layer.mergeAll(
  ProjectionStore.layer.pipe(Layer.provideMerge(SqlitePersistence.layerMemory)),
  SqlitePersistence.layerMemory,
);

function control(key: string, ordinal: number, mode: "live" | "message" | "approval") {
  const request: OrchestrationV2RuntimeRequest = {
    id: RuntimeRequestId.make(key),
    nodeId: NodeId.make(`node:${key}`),
    providerTurnId: null,
    nativeRequestRef: null,
    kind: mode === "approval" ? "command" : "user_input",
    status: "pending",
    responseCapability:
      mode === "message"
        ? { type: "message" }
        : { type: "live", providerSessionId: ProviderSessionId.make("session:control") },
    createdAt: v2Now,
    resolvedAt: null,
  };
  const base = {
    id: TurnItemId.make(`item:${key}`),
    threadId: v2Projection.thread.id,
    runId: null,
    nodeId: request.nodeId,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal,
    status: mode === "message" ? ("completed" as const) : ("waiting" as const),
    title: null,
    startedAt: v2Now,
    completedAt: mode === "message" ? v2Now : null,
    updatedAt: v2Now,
    requestId: request.id,
  };
  const item: OrchestrationV2TurnItem =
    mode === "approval"
      ? {
          ...base,
          type: "approval_request",
          requestKind: "command",
          prompt: "Run the focused check?",
          options: [
            { label: "Run once", decision: "accept" },
            { label: "Decline", decision: "decline" },
          ],
        }
      : {
          ...base,
          type: "user_input_request",
          ...(mode === "message" ? { responseMode: "message" as const } : {}),
          questions: [
            {
              id: "choice",
              header: "Choice",
              question: `Answer ${key}?`,
              required: true,
              options: [{ label: "Continue", value: "continue", description: "Continue the task" }],
            },
          ],
        };
  return { request, item };
}

function controlBodies(projection: Pick<OrchestrationV2ThreadProjection, "turnItems">) {
  return projection.turnItems.filter(
    (item) => item.type === "user_input_request" || item.type === "approval_request",
  );
}

const controls = [
  control("native", 1, "live"),
  control("async", 2, "message"),
  control("approval", 3, "approval"),
];
const tools: OrchestrationV2TurnItem[] = Array.from({ length: 6 }, (_, index) => ({
  ...controls[0]!.item,
  id: TurnItemId.make(`tool:${index}`),
  type: "command_execution",
  nodeId: null,
  ordinal: index + 4,
  status: "completed",
  input: "synthetic check",
  output: "x".repeat(4_096),
  completedAt: v2Now,
}));

it.layer(layer)("pending control bodies", (it) => {
  it.effect(
    "keeps answerable controls through real SQL windows and wire trimming without changing history",
    () =>
      Effect.gen(function* () {
        const store = yield* ProjectionStore.ProjectionStoreV2;
        const sql = yield* SqlClient.SqlClient;
        const threadId = v2Projection.thread.id;
        const runId = RunId.make("controls:run");
        yield* store.apply({
          id: EventId.make("controls:thread"),
          type: "thread.created",
          threadId,
          occurredAt: v2Now,
          payload: v2Projection.thread,
        });
        yield* store.apply({
          id: EventId.make("controls:run"),
          type: "run.updated",
          threadId,
          occurredAt: v2Now,
          payload: {
            id: runId,
            threadId,
            ordinal: 1,
            providerInstanceId: v2Projection.thread.providerInstanceId,
            modelSelection: v2Projection.thread.modelSelection,
            providerThreadId: null,
            userMessageId: MessageId.make("controls:message"),
            rootNodeId: null,
            activeAttemptId: null,
            status: "completed",
            requestedAt: v2Now,
            startedAt: v2Now,
            completedAt: v2Now,
            checkpointId: null,
            contextHandoffId: null,
          },
        });
        for (const { request, item } of controls) {
          yield* store.apply({
            id: EventId.make(`controls:${request.id}`),
            type: "runtime-request.updated",
            threadId,
            occurredAt: v2Now,
            payload: request,
          });
          yield* store.apply({
            id: EventId.make(`controls:${item.id}`),
            type: "turn-item.updated",
            threadId,
            occurredAt: v2Now,
            payload: { ...item, runId },
          });
        }
        for (const item of tools)
          yield* store.apply({
            id: EventId.make(`controls:${item.id}`),
            type: "turn-item.updated",
            threadId,
            occurredAt: v2Now,
            payload: { ...item, runId },
          });

        // The same request/node in another owner must never supply this thread's control body.
        const foreignId = ThreadId.make("foreign-owner");
        yield* store.apply({
          id: EventId.make("foreign:thread"),
          type: "thread.created",
          threadId: foreignId,
          occurredAt: v2Now,
          payload: {
            ...v2Projection.thread,
            id: foreignId,
            lineage: { rootThreadId: foreignId, parentThreadId: null, relationshipToParent: null },
          },
        });
        const foreignItem = {
          ...controls[0]!.item,
          id: TurnItemId.make("foreign:control"),
          threadId: foreignId,
        };
        yield* store.apply({
          id: EventId.make("foreign:item"),
          type: "turn-item.updated",
          threadId: foreignId,
          occurredAt: v2Now,
          payload: foreignItem,
        });
        const mismatched = {
          ...controls[0]!.item,
          id: TurnItemId.make("wrong:node"),
          nodeId: NodeId.make("wrong:node"),
          ordinal: 0,
        };
        yield* store.apply({
          id: EventId.make("wrong:item"),
          type: "turn-item.updated",
          threadId,
          occurredAt: v2Now,
          payload: mismatched,
        });

        // SQL decodes command bodies; the synthetic input's question-only extra
        // fields are not command_execution fields. Specify the stored bodies.
        const expectedRecentTools = [4, 5].map((index) => ({
          id: TurnItemId.make(`tool:${index}`),
          threadId,
          runId,
          nodeId: null,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: null,
          parentItemId: null,
          ordinal: index + 4,
          type: "command_execution" as const,
          status: "completed" as const,
          title: null,
          startedAt: v2Now,
          completedAt: v2Now,
          updatedAt: v2Now,
          input: "synthetic check",
          output: "x".repeat(4_096),
        }));
        const initial = yield* store.getThreadSnapshotWindow(threadId, { rowLimit: 2 });
        assert.deepEqual(
          initial.projection.visibleTurnItems,
          // Upstream keeps pending controls out of the row window and appends
          // them to turnItems after windowing; clients read them from turnItems.
          expectedRecentTools.map((item, position) => ({
            item,
            position,
            visibility: "local",
            sourceThreadId: threadId,
            sourceItemId: item.id,
          })),
        );
        assert.lengthOf(initial.projection.turnItems, 5);
        const policy = { maxItems: 1, maxEncodedBytes: 2_048 };
        const bounded = buildBoundedThreadProjection({
          projection: initial.projection,
          snapshotSequence: initial.snapshotSequence,
          policy,
        });
        const baseline = buildBoundedThreadProjection({
          projection: {
            ...initial.projection,
            turnItems: initial.projection.turnItems.filter(
              (item) => !controls.some((control) => control.item.id === item.id),
            ),
          },
          snapshotSequence: initial.snapshotSequence,
          policy,
        });
        assert.deepEqual(bounded.projection.visibleTurnItems, baseline.projection.visibleTurnItems);
        assert.strictEqual(bounded.historyCursor, baseline.historyCursor);
        assert.strictEqual(bounded.hasMoreHistory, baseline.hasMoreHistory);
        assert.isTrue(bounded.hasMoreHistory);
        assert.strictEqual(bounded.latestLocalTurnOrdinal, 9);
        assert.strictEqual(
          new Set(bounded.projection.turnItems.map((item) => item.id)).size,
          bounded.projection.turnItems.length,
        );
        const retained = controlBodies(bounded.projection);
        assert.deepEqual(
          retained,
          controls.map(({ item }) => ({ ...item, runId })),
        );
        const byteBounded = buildBoundedThreadProjection({
          projection: initial.projection,
          snapshotSequence: initial.snapshotSequence,
          policy: { maxItems: 100, maxEncodedBytes: 2_048 },
        });
        assert.lengthOf(byteBounded.projection.visibleTurnItems, 1);
        assert.deepEqual(controlBodies(byteBounded.projection), retained);
        // Both HTTP and WS use this bounded codec and restore path after opt-in compaction.
        for (const compact of [false, true]) {
          const projection = compact
            ? omitLocalVisibleTurnItems(dedupeSnapshotBodies(bounded.projection))
            : bounded.projection;
          assert.isNotNull(projection);
          const encoded = yield* Schema.encodeEffect(snapshotCodec)({
            ...bounded,
            projection: projection!,
            snapshotSequence: initial.snapshotSequence,
            ...(compact
              ? { turnItemsOmitLocalVisible: true as const, idTable: true as const }
              : {}),
          });
          const restored = boundedSnapshotProjection(
            yield* Schema.decodeEffect(snapshotCodec)(encoded),
          );
          assert.deepEqual(controlBodies(restored), retained);
          assert.deepEqual(
            restored.turnItems.map(({ id }) => id),
            bounded.projection.turnItems.map(({ id }) => id),
          );
        }

        const forkId = ThreadId.make("controls:fork");
        yield* store.apply({
          id: EventId.make("controls:fork"),
          type: "thread.created",
          threadId: forkId,
          occurredAt: v2Now,
          payload: {
            ...v2Projection.thread,
            id: forkId,
            lineage: {
              rootThreadId: threadId,
              parentThreadId: threadId,
              relationshipToParent: "fork",
            },
            forkedFrom: { type: "run", threadId, runId },
          },
        });
        const fork = yield* store.getThreadSnapshotWindow(forkId, { rowLimit: 2 });
        assert.deepEqual(
          fork.projection.visibleTurnItems.map(({ item }) => item.id),
          [...tools.slice(-2).map(({ id }) => id), TurnItemId.make(`turn-item:fork:${forkId}`)],
        );
        assert.deepEqual(controlBodies(fork.projection), []);
        assert.isFalse(
          fork.projection.turnItems.some((item) =>
            controls.some(({ item: controlItem }) => controlItem.id === item.id),
          ),
        );

        // Reconnect replaces the snapshot, rather than depending on a previously cached body.
        assert.deepEqual(
          (yield* store.getThreadSnapshotWindow(threadId, { rowLimit: 2 })).projection,
          initial.projection,
        );
        for (const [index, status] of (["resolved", "cancelled"] as const).entries()) {
          const request = controls[index]!.request;
          yield* store.apply({
            id: EventId.make(`controls:end:${request.id}`),
            type: "runtime-request.updated",
            threadId,
            occurredAt: v2Now,
            payload: { ...request, status, resolvedAt: v2Now },
          });
          if (index === 0) {
            const partlyResolved = yield* store.getThreadSnapshotWindow(threadId, { rowLimit: 2 });
            assert.deepEqual(
              controlBodies(partlyResolved.projection).map(({ requestId }) => requestId),
              ["async", "approval"],
            );
          }
        }
        const ended = yield* store.getThreadSnapshotWindow(threadId, { rowLimit: 2 });
        const reloaded = buildBoundedThreadProjection({
          projection: ended.projection,
          snapshotSequence: ended.snapshotSequence,
          policy,
        });
        assert.deepEqual(controlBodies(reloaded.projection), [{ ...controls[2]!.item, runId }]);
        assert.isFalse(
          reloaded.projection.turnItems.some(
            (item) => item.id === controls[0]!.item.id || item.id === controls[1]!.item.id,
          ),
        );
        assert.isTrue(ended.projection.turnItems.some((item) => item.id === controls[2]!.item.id));
        assert.deepEqual(
          ended.projection.visibleTurnItems,
          expectedRecentTools.map((item, position) => ({
            item,
            position,
            visibility: "local",
            sourceThreadId: threadId,
            sourceItemId: item.id,
          })),
        );

        const plan = yield* sql.unsafe<{ detail: string }>(
          `EXPLAIN QUERY PLAN SELECT item.payload_json FROM json_each(?) AS request CROSS JOIN orchestration_v2_projection_turn_items AS item ON item.node_id = json_extract(request.value, '$.nodeId') WHERE item.thread_id = ? AND item.type = json_extract(request.value, '$.type') AND json_extract(item.payload_json, '$.requestId') = json_extract(request.value, '$.id')`,
          [JSON.stringify([{ ...controls[0]!.request, type: "user_input_request" }]), threadId],
        );
        assert.isTrue(plan.some((row) => row.detail.includes("turn_items_node_ordinal_idx")));
      }),
  );

  it.effect(
    "publishes child secret creation and every terminal state as authoritative shell updates",
    () =>
      Effect.gen(function* () {
        const store = yield* ProjectionStore.ProjectionStoreV2;
        const threadId = ThreadId.make("secret-child");
        const runId = RunId.make("secret-child:run");
        const thread = {
          ...v2Projection.thread,
          id: threadId,
          lineage: {
            rootThreadId: v2Projection.thread.id,
            parentThreadId: v2Projection.thread.id,
            relationshipToParent: "subagent" as const,
          },
        };
        yield* store.apply({
          id: EventId.make("secret-child:created"),
          type: "thread.created",
          threadId,
          occurredAt: v2Now,
          payload: thread,
        });
        yield* store.apply({
          id: EventId.make("secret-child:running"),
          type: "run.updated",
          threadId,
          occurredAt: v2Now,
          payload: {
            id: runId,
            threadId,
            ordinal: 1,
            providerInstanceId: thread.providerInstanceId,
            modelSelection: thread.modelSelection,
            providerThreadId: null,
            userMessageId: MessageId.make("secret-child:message"),
            rootNodeId: null,
            activeAttemptId: null,
            status: "running",
            requestedAt: v2Now,
            startedAt: v2Now,
            completedAt: null,
            checkpointId: null,
            contextHandoffId: null,
          },
        });
        const subagents = new Set([threadId]);
        let sequence = 0;
        for (const terminal of ["saved", "declined", "cancelled"] as const) {
          for (const secretStatus of ["pending", terminal] as const) {
            sequence += 1;
            const event: OrchestrationV2DomainEvent = {
              id: EventId.make(`secret-child:${sequence}`),
              type: "turn-item.updated",
              threadId,
              occurredAt: v2Now,
              payload: {
                ...controls[0]!.item,
                id: TurnItemId.make("secret-child:secret"),
                threadId,
                runId,
                type: "secret_request",
                label: "Synthetic credential",
                reason: "Fixture only",
                secretStatus,
                status: secretStatus === "pending" ? "waiting" : "completed",
              },
            };
            yield* store.apply(event);
            const stored = { sequence, commandId: null, event };
            assert.isTrue(shouldPublishShellApplicationEvent(stored, subagents));
            const invalidation = toShellApplicationEvent(stored);
            assert.isTrue("event" in invalidation);
            if (!("event" in invalidation)) throw new Error("Expected thread invalidation");
            const shell = (yield* store.getShellSnapshot()).threads.find(
              ({ id }) => id === threadId,
            )!;
            const update = shellStreamItemFromThreadShell({ stored: invalidation, shell });
            assert.strictEqual(update.kind, "thread.updated");
            if (update.kind !== "thread.updated") throw new Error("Expected shell update");
            assert.strictEqual(
              update.thread.pendingRuntimeRequest?.id ?? null,
              secretStatus === "pending" ? "secret-child:secret" : null,
            );
            const ordinary = {
              ...stored,
              event: { ...event, payload: { ...tools[0]!, threadId, runId } },
            };
            assert.isFalse(shouldPublishShellApplicationEvent(ordinary, subagents));
          }
        }
      }),
  );
});

it("wire trimming adds pending local controls once and preserves supplied dependencies", () => {
  const foreign = {
    ...controls[0]!.item,
    id: TurnItemId.make("foreign"),
    threadId: ThreadId.make("source-owner"),
  };
  const internal = ["auth_refresh", "dynamic_tool_call"].map((kind, index) => ({
    request: {
      ...controls[0]!.request,
      id: RuntimeRequestId.make(kind),
      kind,
    } as OrchestrationV2RuntimeRequest,
    item: {
      ...controls[0]!.item,
      id: TurnItemId.make(kind),
      requestId: RuntimeRequestId.make(kind),
      ordinal: index + 10,
    } as OrchestrationV2TurnItem,
  }));
  const items = [
    ...controls.map(({ item }) => item),
    ...tools,
    foreign,
    ...internal.map(({ item }) => item),
  ];
  const projection = {
    ...v2Projection,
    runtimeRequests: [
      ...controls.map(({ request }) => request),
      ...internal.map(({ request }) => request),
    ],
    turnItems: items,
    visibleTurnItems: tools.map((item, position) => ({
      item,
      position,
      visibility: "local" as const,
      sourceThreadId: item.threadId,
      sourceItemId: item.id,
    })),
  };
  assert.deepEqual(
    pendingControlTurnItems(projection),
    controls.map(({ item }) => item),
  );
  assert.deepEqual(pendingControlTurnItems({ ...projection, runtimeRequests: [] }), []);
  assert.deepEqual(
    pendingControlTurnItems({
      ...projection,
      runtimeRequests: projection.runtimeRequests.map((request) => ({
        ...request,
        nodeId: NodeId.make("wrong-node"),
      })),
    }),
    [],
  );
  assert.deepEqual(
    pendingControlTurnItems({
      ...projection,
      runtimeRequests: projection.runtimeRequests.map((request) => ({
        ...request,
        status: "resolved" as const,
      })),
    }),
    [],
  );
  const bounded = buildBoundedThreadProjection({
    projection,
    snapshotSequence: 1,
    policy: { maxItems: 1, maxEncodedBytes: 2_048 },
  });
  assert.deepEqual(bounded.projection.turnItems, [
    tools[5]!,
    ...controls.map(({ item }) => item),
    foreign,
    ...internal.map(({ item }) => item),
  ]);
  const visible = {
    ...projection,
    visibleTurnItems: controls.map(({ item }, position) => ({
      item,
      position,
      visibility: "local" as const,
      sourceThreadId: item.threadId,
      sourceItemId: item.id,
    })),
  };
  const all = buildBoundedThreadProjection({ projection: visible, snapshotSequence: 1 });
  assert.deepEqual(all.projection.turnItems, [
    ...controls.map(({ item }) => item),
    foreign,
    ...internal.map(({ item }) => item),
  ]);
  assert.equal(new Set(all.projection.turnItems.map((item) => item.id)).size, 6);
  assert.deepEqual(all.projection.visibleTurnItems, visible.visibleTurnItems);
});
