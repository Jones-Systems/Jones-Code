import { assert, it } from "@effect/vitest";
import {
  EventId,
  ProjectId,
  RuntimeRequestId,
  ThreadId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProviderSessionManager from "../../orchestration-v2/ProviderSessionManager.ts";
import {
  runtimeBinding,
  requestedRuntimeIdentity,
  unobservedRuntimeIdentity,
} from "../../orchestration-v2/ProviderAdapter.ts";
import type * as RuntimeObservation from "../provider/observations/ProviderThreadRuntimeObservation.ts";
import { workModeFixture } from "../workMode/Fixtures.testkit.ts";
import * as RuntimeCensus from "./RuntimeCensus.ts";
import type { CurrentThreadRuntimeAttachment } from "./CurrentThreadRuntimeAttachment.ts";

const fixture = workModeFixture();
const provider = {
  ...fixture.providerThreads[0]!,
  runtimeIdentity: {
    runtimeGeneration: "actual-incarnation",
    evidenceRevision: 7,
    requested: requestedRuntimeIdentity(
      fixture.thread.modelSelection,
      fixture.providerThreads[0]!.driver,
    ),
    observed: unobservedRuntimeIdentity(),
  },
};
const binding = runtimeBinding(provider, "actual-incarnation")!;
const commit = (events: ReadonlyArray<OrchestrationV2DomainEvent>) =>
  Effect.gen(function* () {
    const sink = yield* Effect.serviceOption(EventSink.EventSinkV2);
    if (Option.isSome(sink)) yield* sink.value.write({ events });
    else {
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      yield* Effect.forEach(events, (event) => projections.apply(event), { discard: true });
    }
  });
const seed = (running = false) =>
  commit([
    {
      id: EventId.make("census:thread"),
      type: "thread.created",
      threadId: fixture.thread.id,
      occurredAt: fixture.thread.createdAt,
      payload: fixture.thread,
    },
    {
      id: EventId.make("census:provider"),
      type: "provider-thread.updated",
      threadId: fixture.thread.id,
      occurredAt: provider.updatedAt,
      payload: provider,
    },
    {
      id: EventId.make("census:run"),
      type: "run.created",
      threadId: fixture.thread.id,
      occurredAt: fixture.thread.createdAt,
      payload: {
        ...fixture.runs[0]!,
        ...(running ? { status: "running" as const, completedAt: null } : {}),
      },
    },
  ]);
const stores = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provide(SqlitePersistenceMemory),
);
const sqlite = Layer.mergeAll(
  stores,
  SqlitePersistenceMemory,
  EventSink.layer.pipe(Layer.provide(Layer.merge(stores, SqlitePersistenceMemory))),
);
const cases = [
  { storage: "sqlite", layer: sqlite },
  { storage: "memory", layer: ProjectionStore.layerMemory },
];
const ports = (input: {
  readonly attachment?: Effect.Effect<CurrentThreadRuntimeAttachment>;
  readonly observation?: Effect.Effect<RuntimeObservation.ProviderRuntimeObservation>;
}) =>
  Layer.succeed(ProviderSessionManager.ProviderSessionManagerV2, {
    isMcpCallerAttached: () => Effect.succeed(false),
    shutdown: Effect.void,
    open: () => Effect.die("A census must never open a provider"),
    get: () => Effect.succeed(Option.none()),
    close: () => Effect.die("A census must never close a provider"),
    closeInstance: () => Effect.die("A census must never close a provider instance"),
    release: () => Effect.die("A census must never release a provider"),
    detach: () => Effect.die("A census must never detach a provider"),
    readCurrentThreadRuntimeAttachment: () =>
      input.attachment ??
      Effect.succeed({
        status: "attached",
        binding,
        evidenceRevision: 7,
        observedAt: "1970-01-01T00:00:00.000Z",
        isCurrent: Effect.succeed(true),
        physicalIncarnation: {
          status: "unknown",
          reason: "physical_incarnation_capture_unavailable",
        },
      }),
    observeThreadActivity: () =>
      input.observation ??
      DateTime.now.pipe(
        Effect.map((now) => ({
          status: "monitoring",
          binding,
          observedAt: DateTime.formatIso(now),
          backgroundCoverage: "complete",
        })),
      ),
  });

it.effect.each(cases)(
  "$storage: application census excludes archived/deleted threads and scopes projects",
  ({ layer }) =>
    Effect.gen(function* () {
      yield* seed(true);
      const extra = ["archive", "delete", "other"] as const;
      yield* commit(
        extra.map((kind) => {
          const id = ThreadId.make(`census-${kind}`);
          return {
            id: EventId.make(`census:${kind}`),
            type: "thread.created" as const,
            threadId: id,
            occurredAt: fixture.thread.createdAt,
            payload: {
              ...fixture.thread,
              id,
              activeProviderThreadId: null,
              lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
              projectId:
                kind === "other" ? ProjectId.make("other-project") : fixture.thread.projectId,
              archivedAt: kind === "archive" ? fixture.thread.createdAt : null,
              deletedAt: kind === "delete" ? fixture.thread.createdAt : null,
            },
          };
        }),
      );
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const getCandidates = projections.getOperatingCountsCandidates;
      if (getCandidates === undefined)
        return yield* Effect.die("The candidate fixture requires its real projection census");
      assert.equal((yield* getCandidates()).threads.length, 2);
      const scoped = yield* getCandidates({
        projectId: fixture.thread.projectId,
      });
      assert.deepEqual(
        scoped.threads.map((thread) => thread.id),
        [fixture.thread.id],
      );
      assert.equal(scoped.threads[0]!.activityRunStatus, "running");
      assert.equal(scoped.snapshotSequence, 6);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(cases)(
  "$storage: native application observations retain unknown external background coverage",
  ({ layer }) =>
    Effect.gen(function* () {
      yield* seed();
      const census = yield* RuntimeCensus.RuntimeCensus;
      const counts = yield* census.readOperatingCounts(fixture.thread.projectId);
      assert.equal(counts.total, 1);
      assert.equal(counts.operating, 1);
      assert.equal(counts.backgroundOperating, 1);
      assert.equal(counts.backgroundUnknown, 0);
      assert.equal(counts.applicationCensusCoverage, "complete");
      assert.equal(counts.countScope, "application");
      assert.equal(counts.nativeBackgroundCoverage, "unknown");
    }).pipe(
      Effect.provide(RuntimeCensus.layer.pipe(Layer.provideMerge(Layer.merge(layer, ports({}))))),
    ),
);

it.effect.each(cases)(
  "$storage: an empty application census never attests external provider coverage",
  ({ layer }) =>
    Effect.gen(function* () {
      const census = yield* RuntimeCensus.RuntimeCensus;
      const counts = yield* census.readOperatingCounts();
      assert.equal(counts.total, 0);
      assert.equal(counts.applicationCensusCoverage, "complete");
      assert.equal(counts.nativeBackgroundCoverage, "unknown");
    }).pipe(
      Effect.provide(RuntimeCensus.layer.pipe(Layer.provideMerge(Layer.merge(layer, ports({}))))),
    ),
);

it.effect.each(cases)(
  "$storage: unavailable native activity does not promote projected running work",
  ({ layer }) =>
    Effect.gen(function* () {
      yield* seed(true);
      const census = yield* RuntimeCensus.RuntimeCensus;
      const counts = yield* census.readOperatingCounts();
      assert.equal(counts.operating, 0);
      assert.equal(counts.foregroundUnknown, 1);
      assert.equal(counts.backgroundUnknown, 1);
    }).pipe(
      Effect.provide(
        RuntimeCensus.layer.pipe(
          Layer.provideMerge(
            Layer.merge(
              layer,
              ports({
                observation: Effect.succeed({
                  status: "unknown",
                  reason: "native_activity_unsupported",
                }),
              }),
            ),
          ),
        ),
      ),
    ),
);

it.effect.each(cases)(
  "$storage: replacement after native sampling invalidates the whole count read",
  ({ layer }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const current = yield* Ref.make(true);
        const started = yield* Deferred.make<void>();
        const resume = yield* Deferred.make<void>();
        const dependencies = ports({
          attachment: Effect.succeed({
            status: "attached",
            binding,
            evidenceRevision: 7,
            observedAt: "1970-01-01T00:00:00.000Z",
            isCurrent: Ref.get(current),
            physicalIncarnation: {
              status: "unknown",
              reason: "physical_incarnation_capture_unavailable",
            },
          }),
          observation: Effect.gen(function* () {
            yield* Deferred.succeed(started, undefined);
            yield* Deferred.await(resume);
            return {
              status: "monitoring",
              binding,
              observedAt: DateTime.formatIso(yield* DateTime.now),
              backgroundCoverage: "complete",
            } as const;
          }),
        });
        yield* Effect.gen(function* () {
          yield* seed();
          const census = yield* RuntimeCensus.RuntimeCensus;
          const sampling = yield* census.readOperatingCounts().pipe(Effect.forkScoped);
          yield* Deferred.await(started);
          yield* Ref.set(current, false);
          yield* Deferred.succeed(resume, undefined);
          const failure = yield* Fiber.join(sampling).pipe(Effect.flip);
          assert.equal(failure.reason, "census_changed");
        }).pipe(
          Effect.provide(
            RuntimeCensus.layer.pipe(Layer.provideMerge(Layer.merge(layer, dependencies))),
          ),
        );
      }),
    ),
);

it.effect("sqlite: a projection read failure remains unavailable instead of returning zero", () =>
  Effect.gen(function* () {
    yield* seed();
    const sql = yield* SqlClient.SqlClient;
    yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = '{"invalid":true}'`;
    const census = yield* RuntimeCensus.RuntimeCensus;
    const failure = yield* census.readOperatingCounts().pipe(Effect.flip);
    assert.equal(failure.reason, "census_unavailable");
  }).pipe(
    Effect.provide(RuntimeCensus.layer.pipe(Layer.provideMerge(Layer.merge(sqlite, ports({}))))),
  ),
);

it.effect.each(cases)(
  "$storage: application changes during native sampling fail rather than mix snapshots",
  ({ layer }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const resume = yield* Deferred.make<void>();
        const dependencies = ports({
          observation: Effect.gen(function* () {
            yield* Deferred.succeed(started, undefined);
            yield* Deferred.await(resume);
            return {
              status: "monitoring",
              binding,
              observedAt: DateTime.formatIso(yield* DateTime.now),
              backgroundCoverage: "complete",
            } as const;
          }),
        });
        yield* Effect.gen(function* () {
          yield* seed();
          const census = yield* RuntimeCensus.RuntimeCensus;
          const sampling = yield* census.readOperatingCounts().pipe(Effect.forkScoped);
          yield* Deferred.await(started);
          const id = ThreadId.make("census-new-thread");
          yield* commit([
            {
              id: EventId.make("census:new-thread"),
              type: "thread.created",
              threadId: id,
              occurredAt: fixture.thread.createdAt,
              payload: {
                ...fixture.thread,
                id,
                activeProviderThreadId: null,
                lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
              },
            },
          ]);
          yield* Deferred.succeed(resume, undefined);
          const failure = yield* Fiber.join(sampling).pipe(Effect.flip);
          assert.equal(failure.reason, "census_changed");
        }).pipe(
          Effect.provide(
            RuntimeCensus.layer.pipe(Layer.provideMerge(Layer.merge(layer, dependencies))),
          ),
        );
      }),
    ),
);

it.effect.each(cases)(
  "$storage: projection waiting facts survive unsupported native activity without promoting it",
  ({ layer }) =>
    Effect.gen(function* () {
      yield* seed(true);
      const census = yield* RuntimeCensus.RuntimeCensus;
      const rootNodeId = fixture.runs[0]!.rootNodeId;
      if (rootNodeId === null)
        return yield* Effect.die("The waiting fixture requires its real root node");
      for (const kind of ["command", "user_input"] as const) {
        yield* commit([
          {
            id: EventId.make(`census:request-${kind}`),
            type: "runtime-request.updated",
            threadId: fixture.thread.id,
            occurredAt: fixture.thread.createdAt,
            payload: {
              id: RuntimeRequestId.make("census-request"),
              nodeId: rootNodeId,
              providerTurnId: null,
              nativeRequestRef: null,
              kind,
              status: "pending",
              responseCapability: { type: "not_resumable", reason: "synthetic read fixture" },
              createdAt: fixture.thread.createdAt,
              resolvedAt: null,
            },
          },
        ]);
        const counts = yield* census.readOperatingCounts();
        assert.equal(counts.foregroundWaitingApproval, kind === "command" ? 1 : 0);
        assert.equal(counts.foregroundWaitingInput, kind === "user_input" ? 1 : 0);
        assert.equal(counts.operating, 0);
        assert.equal(counts.backgroundUnknown, 1);
      }
    }).pipe(
      Effect.provide(
        RuntimeCensus.layer.pipe(
          Layer.provideMerge(
            Layer.merge(
              layer,
              ports({
                observation: Effect.succeed({
                  status: "unknown",
                  reason: "native_activity_unsupported",
                }),
              }),
            ),
          ),
        ),
      ),
    ),
);

it.effect("a standalone count binding without a session owner reports typed unavailability", () =>
  Effect.gen(function* () {
    const census = yield* RuntimeCensus.RuntimeCensus;
    const failure = yield* census.readOperatingCounts().pipe(Effect.flip);
    assert.equal(failure.reason, "census_unavailable");
  }).pipe(Effect.provide(RuntimeCensus.layer.pipe(Layer.provide(ProjectionStore.layerMemory)))),
);

it.effect("an unadopted projection census capability reports typed unavailability", () =>
  Effect.gen(function* () {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const legacyProjection = { ...projections };
    delete legacyProjection.getOperatingCountsCandidates;
    yield* Effect.gen(function* () {
      const census = yield* RuntimeCensus.RuntimeCensus;
      const failure = yield* census.readOperatingCounts().pipe(Effect.flip);
      assert.equal(failure.reason, "census_unavailable");
    }).pipe(
      Effect.provide(
        RuntimeCensus.layer.pipe(
          Layer.provide(
            Layer.merge(
              Layer.succeed(ProjectionStore.ProjectionStoreV2, legacyProjection),
              ports({}),
            ),
          ),
        ),
      ),
    );
  }).pipe(Effect.provide(ProjectionStore.layerMemory)),
);
