import { assert, it } from "@effect/vitest";
import { CommandId, EventId, ProjectId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import type { OrchestrationV2ShellStreamItem } from "@t3tools/contracts";
import * as Projection from "../../orchestration-v2/ProjectionStore.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Events from "../../persistence/OrchestrationEventStore.ts";
import * as Projects from "../../orchestration-v2/ProjectStore.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import * as Enrichment from "../../project/ProjectEnrichmentService.ts";
import { subscribeOrchestrationV2Shell } from "../../ws.ts";
import * as Materialization from "../orchestration/shellMaterialization.ts";
import { refreshItems } from "../orchestration/shellThreadRefresh.ts";
import { makeCommit } from "./Transactions.ts";
import * as Changes from "./DependencyChanges.ts";
import * as Fixture from "./testkit.ts";
import { runtime } from "./runtimeTestkit.ts";

const threadReads = Layer.unwrap(Effect.gen(function* () {
  const projections = yield* Projection.ProjectionStoreV2;
  return Layer.mock(Threads.ThreadManagementService)({ getThreadShell: (threadId) => projections.getThreadShell(threadId).pipe(Effect.mapError((cause) => new Orchestrator.OrchestratorProjectionError({ threadId, cause }))) });
}));
const layer = Materialization.layer.pipe(Layer.provide(threadReads), Layer.provide(Events.layer), Layer.provideMerge(Fixture.layer));

it.effect("a Jones-only release invalidates both shared cache subscriptions at equal application sequence", () => Effect.gen(function* () {
  const f = yield* Fixture.seed;
  const changes = yield* Changes.DependencyChanges;
  const cache = yield* Materialization.ShellMaterialization;
  yield* f.send("cached");
  const sequence = yield* f.store.sequence;
  const first = yield* cache.forSubscription;
  const second = yield* cache.forSubscription;
  assert.equal((yield* first.get(f.waiter.id, sequence))?.pendingBackgroundTasks.length, 1);
  assert.equal((yield* second.get(f.waiter.id, sequence))?.pendingBackgroundTasks.length, 1);
  yield* makeCommit(f.sql, f.store, changes)(f.store.release(f.waiter.id, "dependency:cached"));
  assert.equal(yield* f.store.sequence, sequence);
  assert.deepEqual((yield* first.get(f.waiter.id, sequence))?.pendingBackgroundTasks, []);
  assert.deepEqual((yield* second.get(f.waiter.id, sequence))?.pendingBackgroundTasks, []);
}).pipe(Effect.provide(layer)));

it.effect("quiet reconnect selects equal-sequence Jones changes and emits only subscriber-visible active rows", () => Effect.gen(function* () {
  const f = yield* Fixture.seed;
  yield* f.send("resume");
  const threadSequence = yield* f.store.sequence;
  const projectId = ProjectId.make("later-project");
  const commandId = CommandId.make("later-project-command");
  const stamp = DateTime.formatIso(Fixture.time);
  yield* f.sink.commitProjectCommand({ commandId, projectId, commandType: "project.create", acceptedAt: Fixture.time, event: {
    eventId: EventId.make("later-project-event"), aggregateKind: "project", aggregateId: projectId, occurredAt: stamp,
    commandId, causationEventId: null, correlationId: commandId, metadata: {}, type: "project.created",
    payload: { projectId, title: "Later project event", workspaceRoot: "/synthetic/project", defaultModelSelection: null, scripts: [], createdAt: stamp, updatedAt: stamp },
  } });
  const sequence = yield* f.store.sequence;
  assert.isAbove(sequence, threadSequence);
  const dependenciesBefore = (yield* f.projection.getShellSnapshot()).threads;
  yield* f.sql.withTransaction(f.store.release(f.waiter.id, "dependency:resume"));
  const ids = yield* f.store.changedThreads(sequence);
  assert.include(ids, f.waiter.id);
  const threads = (yield* f.projection.getShellSnapshot()).threads;
  const items = refreshItems({ schemaVersion: 1, snapshotSequence: sequence, projects: [], threads, archivedThreads: [] }, ids);
  const waiter = items.find((item) => item.kind === "thread.refreshed" && item.thread.id === f.waiter.id)!;
  assert(waiter.kind === "thread.refreshed");
  assert.equal(waiter.sequence, sequence);
  assert.deepEqual(waiter.thread.pendingBackgroundTasks, []);
  assert.equal(dependenciesBefore.find((thread) => thread.id === f.waiter.id)?.pendingBackgroundTasks.length, 1);
  assert.deepEqual(refreshItems({ schemaVersion: 1, snapshotSequence: sequence, projects: [], threads: [], archivedThreads: threads }, ids), ids.map((threadId) => ({ kind: "thread.omitted", sequence, location: "active", threadId })));
}).pipe(Effect.provide(layer)));

it.effect("CR-R3: the actual filtered shell subscription removes an eventlessly stopped child live and on quiet reconnect", () => Effect.gen(function* () {
  const f = yield* Fixture.seed;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const child = { ...f.waiter, lineage: { rootThreadId: f.receiver.id, parentThreadId: f.receiver.id, relationshipToParent: "subagent" as const } };
  yield* f.sink.write({ events: [f.event({ type: "thread.metadata-updated", threadId: child.id, payload: child })] });
  yield* f.send("filtered-child");
  const enrichment = yield* PubSub.unbounded<Enrichment.ProjectEnrichmentChange>();
  const threadReads = Layer.mock(Threads.ThreadManagementService)({
    readShellSnapshot: orchestrator.readShellSnapshot,
    getThreadShell: orchestrator.getThreadShell,
    listActiveSubagentThreadIds: () => f.projection.listActiveSubagentThreadIds().pipe(Effect.orDie),
  });
  const shellServices = Layer.mergeAll(
    Materialization.layer.pipe(Layer.provide(threadReads), Layer.provide(Events.layer)),
    threadReads, Events.layer,
    Layer.mock(Projects.ProjectStoreV2)({ listShells: () => Effect.succeed([]) }),
    Layer.mock(ProjectService.ProjectService)({ getShell: () => Effect.succeedNone }),
    Layer.mock(Enrichment.ProjectEnrichmentService)({ subscribeChanges: PubSub.subscribe(enrichment) }),
  );
  yield* Effect.gen(function* () {
    const output = yield* Queue.unbounded<OrchestrationV2ShellStreamItem>();
    const legacyOutput = yield* Queue.unbounded<OrchestrationV2ShellStreamItem>();
    const open = (acceptThreadRefresh: boolean, destination: typeof output) => Effect.scoped(Effect.gen(function* () {
      const stream = yield* subscribeOrchestrationV2Shell({ omitIdleSubagents: true, acceptThreadRefresh, requestCompletionMarker: true });
      yield* stream.pipe(Stream.runForEach((item) => Queue.offer(destination, item)));
    })).pipe(Effect.forkScoped);
    const live = yield* open(true, output);
    const legacy = yield* open(false, legacyOutput);
    const initial = yield* Queue.take(output);
    assert(initial.kind === "snapshot");
    assert.equal(initial.snapshot.threads.find((thread) => thread.id === child.id)?.pendingBackgroundTasks.length, 1);
    const sequence = initial.snapshot.snapshotSequence;
    assert.equal((yield* Queue.take(output)).kind, "synchronized");
    assert.equal((yield* Queue.take(legacyOutput)).kind, "snapshot");
    assert.equal((yield* Queue.take(legacyOutput)).kind, "synchronized");
    const stop = yield* orchestrator.dispatch({ type: "thread.stop", commandId: CommandId.make("stop-filtered-child"), threadId: child.id });
    assert.deepEqual(stop.storedEvents, []);
    assert.equal(yield* f.store.sequence, sequence);
    assert.deepEqual((yield* f.projection.getThreadShell(child.id))?.pendingBackgroundTasks, []);
    yield* TestClock.adjust("25 millis");
    const omitted = yield* Stream.fromQueue(output).pipe(Stream.filter((item) => item.kind === "thread.omitted" && item.threadId === child.id), Stream.runHead);
    assert.deepEqual(Option.getOrThrow(omitted), { kind: "thread.omitted", sequence, location: "active", threadId: child.id });
    assert.isTrue(Option.isNone(yield* Queue.poll(legacyOutput)));
    yield* Fiber.interrupt(live);
    yield* Fiber.interrupt(legacy);
    // No application event occurred after the client's cursor. Reconnect still repairs its stale child row.
    const resume = (acceptThreadRefresh: boolean) => Effect.scoped(Effect.gen(function* () {
      const stream = yield* subscribeOrchestrationV2Shell({ afterSequence: sequence, omitIdleSubagents: true, acceptThreadRefresh, requestCompletionMarker: true });
      return yield* stream.pipe(Stream.takeUntil((item) => item.kind === "synchronized"), Stream.runCollect);
    }));
    const resumed = yield* resume(true);
    assert.isFalse(resumed.some((item) => item.kind === "snapshot"));
    assert.isTrue(resumed.some((item) => item.kind === "thread.omitted" && item.threadId === child.id && item.sequence === sequence));
    assert.deepEqual(yield* resume(false), [{ kind: "synchronized" }]);
  }).pipe(Effect.provide(shellServices));
}).pipe(Effect.provide(runtime)));
