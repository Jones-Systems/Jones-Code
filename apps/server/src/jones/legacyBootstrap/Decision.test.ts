import { assert, it } from "@effect/vitest";
import { CommandId, EventId, ProjectId, ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import { RecordedAppThreadJson } from "../../orchestration-v2/RecordedTypes.ts";
import { legacyBootstrapCreateCommandId } from "../../orchestration-v2/LegacyBootstrap.ts";
import * as Sqlite from "../../persistence/Sqlite.ts";
import * as Storage from "./Storage.ts";
import { withRetainedRecords } from "./Decision.ts";

const decodeThread = Schema.decodeEffect(RecordedAppThreadJson);

it.effect(
  "memory projections keep private metadata in their explicit adapter through ordinary updates and public snapshots",
  () =>
    Effect.gen(function* () {
      const projections = yield* Projections.ProjectionStoreV2;
      const privateRecords = withRetainedRecords(
        projections,
        Storage.make(yield* SqlClient.SqlClient),
      );
      const threadId = ThreadId.make("memory-legacy");
      const projectId = ProjectId.make("memory-project");
      const releaseCommandId = CommandId.make("memory-C");
      const createCommandId = legacyBootstrapCreateCommandId(threadId, releaseCommandId);
      const policy = {
        version: 1 as const,
        projectId,
        threadId,
        messageId: "memory-message",
        createCommandId,
        releaseCommandId,
        birthCommandId: `${createCommandId}:initial-message`,
        ownsNewThread: true,
        payloadHash: "synthetic",
      };
      const thread = yield* decodeThread({
        id: threadId,
        projectId,
        title: "Before",
        createdBy: "user",
        creationSource: "web",
        providerInstanceId: "codex",
        modelSelection: { instanceId: "codex", model: "test" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        activeProviderThreadId: null,
        lineage: { rootThreadId: threadId, parentThreadId: null, relationshipToParent: null },
        forkedFrom: null,
        createdAt: "2026-11-17T10:00:00.000Z",
        updatedAt: "2026-11-17T10:00:00.000Z",
        archivedAt: null,
        deletedAt: null,
        settledAt: null,
        settledOverride: null,
        lastVisitedAt: null,
        snoozedAt: null,
        snoozedUntil: null,
        legacyBootstrapClaim: policy,
      });
      yield* projections.apply({
        id: EventId.make("memory-claim"),
        type: "thread.created",
        threadId,
        occurredAt: thread.createdAt,
        payload: thread,
      });
      const publicThread = yield* projections.getThread(threadId);
      assert.isFalse("legacyBootstrapClaim" in publicThread);
      const at = DateTime.makeUnsafe("2026-11-17T10:01:00.000Z");
      yield* projections.apply({
        id: EventId.make("memory-update"),
        type: "thread.metadata-updated",
        threadId,
        occurredAt: at,
        payload: { ...publicThread, title: "After", updatedAt: at },
      });
      assert.equal(
        (yield* privateRecords.getThread(threadId)).legacyBootstrapClaim?.createCommandId,
        createCommandId,
      );
      const selected = yield* privateRecords.getThreadRecords(threadId, ["runs"], { runIds: [] });
      assert.deepStrictEqual(selected.runs, []);
      assert.equal(selected.thread.title, "After");
      assert.isFalse(
        "legacyBootstrapClaim" in (yield* projections.getThreadProjection(threadId)).thread,
      );
    }).pipe(Effect.provide(Layer.mergeAll(Projections.layerMemory, Sqlite.layerMemory))),
);
