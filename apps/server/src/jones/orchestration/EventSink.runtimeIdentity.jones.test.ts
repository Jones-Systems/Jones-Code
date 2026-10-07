import { assert, it } from "@effect/vitest";
import {
  EventId,
  ThreadId,
  ProviderThreadId,
  OrchestrationV2AppThread,
  OrchestrationV2ProviderThread,
  type ProviderRuntimeEvidenceCapture,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import { unobservedRuntimeIdentity } from "../../orchestration-v2/ProviderAdapter.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";

const stores = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provide(SqlitePersistenceMemory),
);
const testLayer = Layer.mergeAll(
  stores,
  EventSink.layer.pipe(Layer.provide(Layer.merge(stores, SqlitePersistenceMemory))),
);
const decodeThread = Schema.decodeUnknownSync(OrchestrationV2AppThread);
const decodeProviderThread = Schema.decodeUnknownSync(OrchestrationV2ProviderThread);
const now = DateTime.makeUnsafe("2026-09-01T00:00:00.000Z");

const seedOwner = (revision: number, generation = "producer-1") =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const projection = yield* ProjectionStore.ProjectionStoreV2;
    const app = decodeThread({
      createdBy: "user",
      creationSource: "web",
      id: "app-thread",
      projectId: "project",
      title: "Runtime identity",
      providerInstanceId: "codex",
      modelSelection: { instanceId: "codex", model: "requested" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: "provider-thread",
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: "app-thread" },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    });
    const owner = decodeProviderThread({
      id: "provider-thread",
      driver: "codex",
      providerInstanceId: "codex",
      providerSessionId: "session",
      appThreadId: app.id,
      ownerNodeId: null,
      nativeThreadRef: { driver: "codex", nativeId: "native-thread", strength: "strong" },
      nativeConversationHeadRef: null,
      status: "active",
      firstRunOrdinal: 1,
      lastRunOrdinal: 1,
      handoffIds: [],
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      runtimeIdentity: {
        runtimeGeneration: generation,
        evidenceRevision: revision,
        requested: {
          providerInstanceId: "codex",
          providerDriver: "codex",
          model: "current-request",
          serviceTier: null,
        },
        observed: {
          backend: { status: "unknown" },
          model: { status: "observed", value: "native-current", sourceEvent: "native" },
          account: { status: "unavailable", reason: "Not attested." },
          serviceTier: { status: "unavailable", reason: "Not reported." },
        },
      },
    });
    yield* sink.write({
      events: [
        {
          id: EventId.make("thread-created"),
          type: "thread.created",
          threadId: app.id,
          occurredAt: now,
          payload: app,
        },
        {
          id: EventId.make("owner-bound"),
          type: "provider-thread.updated",
          threadId: app.id,
          occurredAt: now,
          payload: owner,
        },
      ],
    });
    if (
      owner.providerSessionId === null ||
      owner.nativeThreadRef === null ||
      owner.nativeThreadRef.nativeId === null
    ) {
      return yield* Effect.die(new Error("Runtime evidence fixture requires a native binding"));
    }
    const capture: ProviderRuntimeEvidenceCapture = {
      threadId: app.id,
      providerThreadId: owner.id,
      providerSessionId: owner.providerSessionId,
      providerInstanceId: owner.providerInstanceId,
      driver: owner.driver,
      nativeThreadId: owner.nativeThreadRef.nativeId,
      runtimeGeneration: "producer-1",
      evidenceRevision: 1,
    };
    return { sink, projection, app, owner, capture };
  });

it.effect.each([
  {
    title: "allows pinned generation evidence revision advance without blocking terminal state",
    revision: 2,
    pinned: true,
    committed: true,
  },
  {
    title: "allows pinned generation evidence revision equality",
    revision: 1,
    pinned: true,
    committed: true,
  },
  {
    title: "rejects pinned generation evidence revision regression",
    revision: 0,
    pinned: true,
    committed: false,
  },
  {
    title: "rejects unpinned evidence revision advance",
    revision: 2,
    pinned: false,
    committed: false,
  },
  {
    title: "allows unpinned evidence revision equality",
    revision: 1,
    pinned: false,
    committed: true,
  },
])("$title", ({ revision, pinned, committed }) =>
  Effect.gen(function* () {
    const { sink, projection, app, owner, capture } = yield* seedOwner(revision);
    const { runtimeGeneration: _generation, ...unpinned } = capture;
    const staleSnapshot = {
      ...owner,
      status: "idle" as const,
      runtimeIdentity: {
        ...owner.runtimeIdentity!,
        requested: { ...owner.runtimeIdentity!.requested, model: "stale-request" },
        observed: { ...owner.runtimeIdentity!.observed, model: { status: "unknown" as const } },
      },
    };
    const stored = yield* sink.write({
      runtimeEvidence: pinned ? capture : unpinned,
      events: [
        {
          id: EventId.make("terminal-state"),
          type: "provider-thread.updated",
          threadId: app.id,
          occurredAt: now,
          payload: staleSnapshot,
        },
      ],
    });
    assert.equal(stored.length, committed ? 1 : 0);
    const row = (yield* projection.getThreadRecords(app.id, ["providerThreads"]))
      .providerThreads[0]!;
    assert.equal(row.status, committed ? "idle" : "active");
    assert.deepEqual(
      row.runtimeIdentity,
      owner.runtimeIdentity,
      "late snapshots must preserve newer requested and observed evidence",
    );
  }).pipe(Effect.provide(testLayer)),
);

it.effect("rejects a producer replaced after normalization and before commit", () =>
  Effect.gen(function* () {
    const { sink, projection, app, owner, capture } = yield* seedOwner(1);
    const normalized = {
      id: EventId.make("late-provider-state"),
      type: "provider-thread.updated" as const,
      threadId: app.id,
      occurredAt: now,
      payload: { ...owner, status: "idle" as const },
    };
    const normalizedReady = yield* Deferred.make<void>();
    const commit = yield* Deferred.make<void>();
    const writer = yield* Effect.gen(function* () {
      yield* Deferred.succeed(normalizedReady, undefined);
      yield* Deferred.await(commit);
      return yield* sink.write({ runtimeEvidence: capture, events: [normalized] });
    }).pipe(Effect.forkChild);
    yield* Deferred.await(normalizedReady);
    yield* sink.write({
      runtimeIdentityBoundary: { expectedGeneration: "producer-1" },
      events: [
        {
          id: EventId.make("replacement-boundary"),
          type: "provider-thread.updated",
          threadId: app.id,
          occurredAt: now,
          payload: {
            ...owner,
            runtimeIdentity: { ...owner.runtimeIdentity!, runtimeGeneration: "producer-2" },
          },
        },
      ],
    });
    yield* Deferred.succeed(commit, undefined);
    assert.deepEqual(yield* Fiber.join(writer), []);
    const current = (yield* projection.getThreadRecords(app.id, ["providerThreads"]))
      .providerThreads[0]!;
    assert.equal(current.status, "active");
    assert.equal(current.runtimeIdentity?.runtimeGeneration, "producer-2");
  }).pipe(Effect.provide(testLayer)),
);

it.effect.each([
  { title: "app thread", change: { threadId: "another-app" } },
  { title: "provider thread", change: { providerThreadId: "another-owner" } },
  { title: "generation", change: { runtimeGeneration: "other-producer" } },
  { title: "native thread", change: { nativeThreadId: "other-native" } },
  { title: "provider session", change: { providerSessionId: "other-session" } },
  { title: "provider instance", change: { providerInstanceId: "other-instance" } },
  { title: "driver", change: { driver: "claudeAgent" } },
])("rejects captured runtime $title drift", ({ change }) =>
  Effect.gen(function* () {
    const { sink, app, owner, capture } = yield* seedOwner(1);
    if ("threadId" in change) {
      const otherThreadId = ThreadId.make(change.threadId);
      yield* sink.write({
        events: [
          {
            id: EventId.make("other-thread-created"),
            type: "thread.created",
            threadId: otherThreadId,
            occurredAt: now,
            payload: {
              ...app,
              id: otherThreadId,
              activeProviderThreadId: null,
              lineage: {
                parentThreadId: null,
                relationshipToParent: null,
                rootThreadId: otherThreadId,
              },
            },
          },
        ],
      });
    }
    const altered = { ...capture, ...change } as ProviderRuntimeEvidenceCapture;
    assert.deepEqual(
      yield* sink.write({
        runtimeEvidence: altered,
        events: [
          {
            id: EventId.make("stale-state"),
            type: "provider-thread.updated",
            threadId: app.id,
            occurredAt: now,
            payload: { ...owner, status: "idle" },
          },
        ],
      }),
      [],
    );
  }).pipe(Effect.provide(testLayer)),
);

it.effect("rejects pinned runtime evidence whose owner row is missing", () =>
  Effect.gen(function* () {
    const { sink, app, owner, capture } = yield* seedOwner(1);
    assert.deepEqual(
      yield* sink.write({
        runtimeEvidence: {
          ...capture,
          providerThreadId: ProviderThreadId.make("absent-owner"),
        } as ProviderRuntimeEvidenceCapture,
        events: [
          {
            id: EventId.make("owner-missing"),
            type: "provider-thread.updated",
            threadId: app.id,
            occurredAt: now,
            payload: owner,
          },
        ],
      }),
      [],
    );
  }).pipe(Effect.provide(testLayer)),
);

it.effect.each([
  {
    title: "keeps a same-request start writable after observation-only revision progress",
    changed: false,
    stale: false,
  },
  {
    title: "clears observations for a new request after observation-only revision progress",
    changed: true,
    stale: false,
  },
  {
    title: "rejects a request writer after an intervening requested-configuration change",
    changed: true,
    stale: true,
  },
])("$title", ({ changed, stale }) =>
  Effect.gen(function* () {
    const { sink, projection, app, owner, capture } = yield* seedOwner(2);
    const requested = {
      ...owner.runtimeIdentity!.requested,
      model: changed ? "next-request" : "current-request",
    };
    const stored = yield* sink.write({
      runtimeEvidence: capture,
      runtimeIdentityRequest: requested,
      runtimeIdentityPreviousRequest: {
        ...owner.runtimeIdentity!.requested,
        model: stale ? "older-request" : "current-request",
      },
      events: [
        {
          id: EventId.make("running-request"),
          type: "provider-thread.updated",
          threadId: app.id,
          occurredAt: now,
          payload: {
            ...owner,
            status: "active",
            runtimeIdentity: { ...owner.runtimeIdentity!, requested },
          },
        },
      ],
    });
    assert.equal(stored.length, stale ? 0 : 1);
    const current = (yield* projection.getThreadRecords(app.id, ["providerThreads"]))
      .providerThreads[0]!;
    assert.equal(
      current.runtimeIdentity?.requested.model,
      stale ? "current-request" : requested.model,
    );
    assert.equal(current.runtimeIdentity?.runtimeGeneration, "producer-1");
    assert.deepEqual(
      current.runtimeIdentity?.observed,
      !stale && changed ? unobservedRuntimeIdentity() : owner.runtimeIdentity!.observed,
    );
  }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "accepts pinned revision progress while rejecting the same unpinned producer capture",
  () =>
    Effect.gen(function* () {
      const { sink, app, owner, capture } = yield* seedOwner(2);
      const event = {
        type: "provider-thread.updated" as const,
        threadId: app.id,
        occurredAt: now,
        payload: { ...owner, status: "idle" as const },
      };
      assert.lengthOf(
        yield* sink.write({
          runtimeEvidence: capture,
          events: [{ ...event, id: EventId.make("pinned-progress") }],
        }),
        1,
      );
      const { runtimeGeneration: _generation, ...unpinned } = capture;
      assert.deepEqual(
        yield* sink.write({
          runtimeEvidence: unpinned,
          events: [{ ...event, id: EventId.make("unpinned-progress") }],
        }),
        [],
      );
    }).pipe(Effect.provide(testLayer)),
);
