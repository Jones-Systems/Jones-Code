import { assert, it } from "@effect/vitest";
import { CommandId, EventId, OrchestrationV2AppThread, ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import type { Intent } from "./IdleWorktreeAdmission.ts";

const database = SqlitePersistence.layerMemory;
const stores = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(Layer.provide(database));
const layer = Layer.mergeAll(
  database,
  stores,
  Receipts.layer.pipe(Layer.provide(database)),
  EventSink.layer.pipe(Layer.provide(Layer.merge(stores, database))),
);
const now = DateTime.makeUnsafe("2026-10-29T00:00:00.000Z");
const threadId = ThreadId.make("idle-atomic-thread");
const commandId = CommandId.make("idle-atomic-create");
const intent: Intent = {
  threadId,
  commandId,
  claimType: "thread.create",
  requestKey: "new-worktree-without-message",
  generation: "preparation-1",
};
const thread = Schema.decodeUnknownSync(OrchestrationV2AppThread)({
  id: threadId,
  projectId: "project",
  title: "Idle worktree",
  createdBy: "user",
  creationSource: "web",
  providerInstanceId: "codex",
  modelSelection: { instanceId: "codex", model: "requested" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  activeProviderThreadId: null,
  lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
  forkedFrom: null,
  createdAt: now,
  updatedAt: now,
  archivedAt: null,
  settledOverride: null,
  settledAt: null,
  lastVisitedAt: null,
  deletedAt: null,
});
const claim = {
  commandId,
  threadId,
  commandType: "thread.create",
  acceptedAt: now,
  idleWorktreeAdmission: intent,
  events: [
    {
      id: EventId.make("idle-atomic-event"),
      type: "thread.created" as const,
      threadId,
      occurredAt: now,
      payload: thread,
    },
  ],
  effects: [],
};

it.effect("publishes thread creation only after its hold and accepted receipt commit together", () =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const receipts = yield* Receipts.CommandReceiptStoreV2;
    const observer = yield* sink.stream({ threadId }).pipe(
      Stream.filter(({ event }) => event.type === "thread.created"),
      Stream.mapEffect(() => sink.idleWorktreeAdmission!.read(threadId)),
      Stream.runHead,
      Effect.forkChild,
    );
    const accepted = yield* sink.commitCommand(claim);
    const observed = yield* Fiber.join(observer);
    assert.isTrue(accepted.committed);
    assert.isTrue(Option.isSome(observed));
    if (Option.isSome(observed)) assert.equal(observed.value?.phase, "pending");
    assert.equal(
      (yield* receipts.getByCommandId(commandId)).pipe(Option.getOrThrow).status,
      "accepted",
    );
    const replay = yield* sink.commitCommand({
      ...claim,
      idleWorktreeAdmission: { ...intent, generation: "retry-generation" },
    });
    assert.isFalse(replay.committed);
    assert.equal((yield* sink.idleWorktreeAdmission!.read(threadId))?.generation, intent.generation);
  }).pipe(Effect.provide(layer)),
);

it.effect("rolls back the thread, event and receipt when idle intent cannot commit", () =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const receipts = yield* Receipts.CommandReceiptStoreV2;
    const sql = yield* SqlClient.SqlClient;
    yield* sink
      .commitCommand({
        ...claim,
        idleWorktreeAdmission: { ...intent, commandId: CommandId.make("wrong-claim") },
      })
      .pipe(Effect.flip);
    assert.isTrue(Option.isNone(yield* receipts.getByCommandId(commandId)));
    assert.isEmpty(
      yield* sql`SELECT * FROM orchestration_v2_projection_threads WHERE thread_id = ${threadId}`,
    );
    assert.isEmpty(
      yield* sink.readByCommandId({ commandId }).pipe(
        Stream.runCollect,
        Effect.map((events) => Array.from(events)),
      ),
    );
    assert.isNull(yield* sink.idleWorktreeAdmission!.read(threadId));
  }).pipe(Effect.provide(layer)),
);

it.effect("records an expected-empty claim hold in the same metadata transaction", () =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    yield* sink.commitCommand({ ...claim, idleWorktreeAdmission: undefined });
    const updateId = CommandId.make("idle-claim-existing-draft");
    yield* sink.commitCommand({
      ...claim,
      commandId: updateId,
      commandType: "thread.metadata.update",
      idleWorktreeAdmission: { ...intent, commandId: updateId, claimType: "thread.metadata.update" },
      events: [
        {
          ...claim.events[0]!,
          id: EventId.make("idle-claim-existing-event"),
          type: "thread.metadata-updated",
        },
      ],
    });
    assert.equal((yield* sink.idleWorktreeAdmission!.read(threadId))?.commandId, updateId);
  }).pipe(Effect.provide(layer)),
);

it.effect("an idle hold does not block an upstream thread delete", () =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const sql = yield* SqlClient.SqlClient;
    yield* sink.commitCommand(claim);
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* sql`DELETE FROM orchestration_v2_projection_threads WHERE thread_id = ${threadId}`;
    assert.isNull(yield* sink.idleWorktreeAdmission!.read(threadId));
    assert.lengthOf(
      yield* sql`SELECT * FROM jones_idle_worktree_admission WHERE thread_id = ${threadId}`,
      1,
    );
  }).pipe(Effect.provide(layer)),
);
