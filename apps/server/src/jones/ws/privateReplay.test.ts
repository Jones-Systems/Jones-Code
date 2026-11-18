import { assert, it } from "@effect/vitest";
import {
  EventId,
  type OrchestrationV2AppThread,
  type OrchestrationV2StoredEvent,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import { OrchestratorProjectionError } from "../../orchestration-v2/Orchestrator.ts";
import type { RecordedLifecycleEvent } from "../../orchestration-v2/RecordedTypes.ts";
import { THREAD_RESUME_MAX_REPLAY_EVENTS } from "../../orchestration-v2/ThreadStream.ts";
import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";
import * as OrchestrationEventStore from "../../persistence/OrchestrationEventStore.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import { subscribeOrchestrationV2Thread } from "../../ws.ts";

const now = DateTime.makeUnsafe("2026-11-17T00:00:00.000Z");
const threadId = ThreadId.make("thread:private-replay");
const projectId = ProjectId.make("project:private-replay");
const instanceId = ProviderInstanceId.make("codex");

const thread = (title: string): OrchestrationV2AppThread => ({
  id: threadId,
  projectId,
  title,
  providerInstanceId: instanceId,
  modelSelection: { instanceId, model: "synthetic" },
  createdBy: "user",
  creationSource: "web",
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
  deletedAt: null,
  settledAt: null,
  settledOverride: null,
  lastVisitedAt: null,
});

const metadataUpdate = (title: string): RecordedLifecycleEvent => ({
  id: EventId.make(`private-replay:${title}`),
  type: "thread.metadata-updated",
  threadId,
  occurredAt: now,
  payload: thread(title),
});

// Receiving-era shared-ledger preflight rows, at the positions receiving committed them.
// Their payloads are deliberately undecodable: replay must exclude these rows in SQL, not
// decode them and drop them afterwards.
const insertHistoricalPreflight = Effect.fn("privateReplay.test.preflight")(function* (
  type: "legacy-bootstrap.preflight-intent" | "legacy-bootstrap.preflight-outcome",
) {
  const sql = yield* SqlClient.SqlClient;
  const [row] = yield* sql<{ readonly sequence: number }>`
    INSERT INTO orchestration_events (
      event_id, aggregate_kind, stream_id, stream_version, event_type,
      occurred_at, actor_kind, payload_json, metadata_json, application_event_version
    ) VALUES (
      ${`private-replay:${type}`}, 'thread', ${threadId},
      COALESCE((SELECT MAX(stream_version) + 1 FROM orchestration_events
        WHERE aggregate_kind = 'thread' AND stream_id = ${threadId}), 0),
      ${type}, ${DateTime.formatIso(now)}, 'server', '{}', '{}', 2
    ) RETURNING sequence
  `;
  if (row === undefined) return yield* Effect.die("Historical preflight position required");
  return row.sequence;
});

const layerStore = OrchestrationEventStore.layer.pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
);

it.effect("private preflight rows neither consume the resume replay limit nor reach the wire", () =>
  Effect.gen(function* () {
    const store = yield* OrchestrationEventStore.OrchestrationEventStore;
    const [base] = yield* store.appendAgentEvents({ events: [metadataUpdate("base")] });
    const cursor = base!.sequence;
    yield* insertHistoricalPreflight("legacy-bootstrap.preflight-intent");
    // Exactly the replay limit of public rows; counting either private row would exceed it.
    const publics = yield* store.appendAgentEvents({
      events: Array.from({ length: THREAD_RESUME_MAX_REPLAY_EVENTS }, (_, index) =>
        metadataUpdate(`public-${index}`),
      ),
    });
    const outcome = yield* insertHistoricalPreflight("legacy-bootstrap.preflight-outcome");
    // The high-water includes the private outcome, so live resumes after it.
    const highWater = yield* store.latestAgentSequence(threadId);
    assert.equal(highWater, outcome);

    const liveFrom: Array<number | undefined> = [];
    let liveSequence = 0;
    const threads = Layer.mock(ThreadManagement.ThreadManagementService)({
      ensureLegacyTranscript: () => Effect.void,
      getThreadSnapshot: () => Effect.die("Private preflight must not force a snapshot"),
      getThreadSnapshotWindow: () => Effect.die("Private preflight must not force a snapshot"),
      getThreadShell: () => Effect.die("Replay holds no creation event"),
      streamStoredEventsFrom: (input) => {
        liveFrom.push(input?.afterSequence);
        return store.streamApplicationEvents({ afterSequence: input?.afterSequence ?? 0 }).pipe(
          Stream.filter((event): event is OrchestrationV2StoredEvent => "event" in event),
          Stream.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })),
        );
      },
    });
    const items = yield* subscribeOrchestrationV2Thread({
      threadId,
      afterSequence: cursor,
      requestCompletionMarker: true,
      acceptCompactThreadVisits: true,
    }).pipe(
      Effect.flatMap((stream) =>
        stream.pipe(
          Stream.tap((item) =>
            item.kind === "synchronized"
              ? store.appendAgentEvents({ events: [metadataUpdate("live")] }).pipe(
                  Effect.tap((stored) =>
                    Effect.sync(() => {
                      liveSequence = stored[0]!.sequence;
                    }),
                  ),
                  Effect.flatMap(store.publishCommitted),
                )
              : Effect.void,
          ),
          Stream.takeUntil((item) => item.kind === "event" && item.sequence === liveSequence),
          Stream.runCollect,
        ),
      ),
      Effect.provide(threads),
    );

    const frames = Array.from(items);
    assert.deepEqual(
      frames.map((item) => item.kind),
      [
        ...Array.from({ length: THREAD_RESUME_MAX_REPLAY_EVENTS }, () => "event"),
        "synchronized",
        "event",
      ],
    );
    assert.deepEqual(
      frames.flatMap((item) => (item.kind === "event" ? [item.sequence] : [])),
      [...publics.map((stored) => stored.sequence), liveSequence],
    );
    assert.isFalse(
      frames.some(
        (item) => item.kind === "event" && item.event.type.startsWith("legacy-bootstrap."),
      ),
    );
    assert.deepEqual(liveFrom, [highWater]);
  }).pipe(Effect.scoped, Effect.provide(layerStore)),
);
