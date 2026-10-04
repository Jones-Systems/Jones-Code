import * as NetAddress from "effect/unstable/net/NetAddress";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CheckpointScopeId,
  CommandId,
  EnvironmentId,
  EventId,
  type NodeId,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type Project,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderSessionId,
  type ProviderTurnId,
  type RunAttemptId,
  type RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";
import { HttpServer } from "effect/unstable/http";
import { SqlClient } from "effect/unstable/sql";

import { ProviderWorkspaceMissingError } from "../provider/Errors.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as McpProviderSession from "../mcp/McpProviderSession.ts";
import * as McpSessionRegistry from "../mcp/McpSessionRegistry.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as ServerSettings from "../serverSettings.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import {
  ProviderAdapterEventStreamError,
  ProviderAdapterOpenSessionError,
  type ProviderAdapterV2Event,
  ProviderAdapterProtocolError,
  type ProviderAdapterV2RuntimePolicy,
  type ProviderAdapterV2OpenSessionInput,
  type ProviderAdapterV2TurnInput,
  type ProviderAdapterV2Error,
  type ProviderAdapterV2SessionRuntime,
  type ProviderAdapterV2Shape,
  type ProviderRuntimeBinding,
  type ProviderRuntimeObservation,
  type ProviderContinuationSourceIdentity,
  type ProviderPendingStartStopInput,
} from "./ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderEventIngestor from "./ProviderEventIngestor.ts";
import {
  readProviderEventOrigin,
  stampProviderEvent,
  type ProviderEventOrigin,
} from "./ProviderEventOrigin.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import type { LegacyLeaseInventoryError, LegacyLeaseOwnerV1 } from "./LegacyLeaseCleanup.ts";
import {
  registerProviderManagedActorProducer,
  type ProviderManagedActorAdmissionV1,
  type ProviderManagedActorIssuerV1,
} from "./ProviderManagedActorCompletion.ts";
import {
  OrdinaryCheckoutAdmissionV1,
  OrdinaryCheckoutCaptureV1,
  OrdinaryCheckoutExecutionExecutorV1,
  OrdinaryCheckoutUseV1,
  makeOrdinaryCheckoutExecutionRefV1,
  ordinaryApplicationIncarnationV1,
  ordinaryCheckoutAdmissionIdV1,
  ordinaryCheckoutAdmissionRefV1,
  ordinaryCheckoutCommandDigestV1,
} from "./OrdinaryCheckoutOwnership.ts";
import { nativeEffectEvidenceFromCause } from "./ProviderFailure.ts";

const TestDatabaseLayer = SqlitePersistenceMemory;
const TestStoresLayer = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provide(TestDatabaseLayer),
);
const TestEventSinkLayer = EventSink.layer.pipe(
  Layer.provide(Layer.mergeAll(TestStoresLayer, TestDatabaseLayer)),
);
const FailingReleaseEventSinkLayer = Layer.effect(
  EventSink.EventSinkV2,
  Effect.gen(function* () {
    const delegate = yield* EventSink.EventSinkV2;
    return EventSink.EventSinkV2.of({
      ...delegate,
      write: (input) =>
        input.events.some(
          (event) =>
            event.type === "provider-session.updated" &&
            (event.payload.status === "stopped" || event.payload.status === "error"),
        )
          ? Effect.fail(new EventSink.EventSinkWriteError({ eventCount: input.events.length }))
          : delegate.write(input),
    });
  }),
).pipe(Layer.provide(TestEventSinkLayer));

const CodexCapabilities: OrchestrationV2ProviderCapabilities = CodexProviderCapabilitiesV2;
const ExclusiveCapabilities: OrchestrationV2ProviderCapabilities = {
  ...CodexCapabilities,
  sessions: {
    ...CodexCapabilities.sessions,
    supportsMultipleProviderThreadsPerSession: false,
  },
};

interface TestProviderRuntimeState {
  readonly openCount: number;
  readonly closeCount: number;
  readonly interruptCount: number;
  readonly resumeCount: number;
  readonly unloadedNativeThreadIds: ReadonlyArray<string>;
  readonly eventQueues: ReadonlyMap<string, Queue.Queue<ProviderAdapterV2Event, Cause.Done>>;
}

const emptyState: TestProviderRuntimeState = {
  openCount: 0,
  closeCount: 0,
  interruptCount: 0,
  resumeCount: 0,
  unloadedNativeThreadIds: [],
  eventQueues: new Map(),
};

function capturedTestOrigin(
  runtime: ProviderAdapterV2SessionRuntime,
  token: object,
  revalidateCurrent: ProviderEventOrigin["producer"]["revalidateCurrent"],
): ProviderEventOrigin {
  return {
    producer: {
      token,
      driver: runtime.driver,
      instanceId: runtime.instanceId,
      providerSessionId: runtime.providerSessionId,
      ...(runtime.runtimeGeneration === undefined
        ? {}
        : { runtimeGeneration: runtime.runtimeGeneration }),
      revalidateCurrent,
    },
  };
}

function requireNativeThreadId(providerThread: OrchestrationV2ProviderThread): string {
  const nativeThreadId = providerThread.nativeThreadRef?.nativeId;
  if (nativeThreadId == null)
    throw new Error(`Provider thread ${providerThread.id} has no native thread id.`);
  return nativeThreadId;
}

function capturedTestTurnOrigin(
  runtime: ProviderAdapterV2SessionRuntime,
  producer: ProviderEventOrigin["producer"],
  turn: ProviderAdapterV2TurnInput,
  providerTurnId: ProviderTurnId,
): ProviderEventOrigin {
  return {
    producer,
    turn: {
      binding: {
        threadId: turn.threadId,
        providerThreadId: turn.providerThread.id,
        providerSessionId: runtime.providerSessionId,
        instanceId: runtime.instanceId,
        nativeThreadId: requireNativeThreadId(turn.providerThread),
        runtimeGeneration: runtime.runtimeGeneration!,
      },
      runId: turn.runId,
      attemptId: turn.attemptId,
      providerTurnId,
    },
  };
}

function runtimeIdentityEvent(input: {
  readonly runtime: ProviderAdapterV2SessionRuntime;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly generation: string;
  readonly observedModel: string;
}): Extract<ProviderAdapterV2Event, { readonly type: "runtime_identity.observed" }> {
  return {
    type: "runtime_identity.observed",
    driver: input.runtime.driver,
    binding: {
      threadId: input.providerThread.appThreadId!,
      providerThreadId: input.providerThread.id,
      providerSessionId: input.runtime.providerSessionId,
      instanceId: input.runtime.instanceId,
      nativeThreadId: requireNativeThreadId(input.providerThread),
      runtimeGeneration: input.generation,
    },
    attestation: {
      runtimeGeneration: input.generation,
      requested: {
        providerInstanceId: input.runtime.instanceId,
        providerDriver: input.runtime.driver,
        model: modelSelection.model,
        serviceTier: null,
      },
      observed: {
        backend: { status: "unknown" },
        account: { status: "unknown" },
        serviceTier: { status: "unknown" },
        model: {
          status: "observed",
          value: input.observedModel,
          sourceEvent: "test-runtime.initialized",
        },
      },
    },
  };
}

for (const outcome of ["success", "failure", "interruption"] as const) {
  it.effect(
    `publishes typed identity only after managed binding commits and discards unsuccessful setup (${outcome})`,
    () =>
      Effect.gen(function* () {
        const state = yield* Ref.make(emptyState);
        const entered = yield* Deferred.make<void>();
        const gate = yield* Deferred.make<void>();
        let generation = "";
        let source: ProviderContinuationSourceIdentity | undefined;
        let opened: ProviderAdapterV2OpenSessionInput | undefined;
        let candidateCurrent = true;
        let nativeCompleted = false;
        yield* Effect.gen(function* () {
          const fixture = yield* preparePendingStartStopFixture(`identity-binding-${outcome}`);
          const priorGeneration = generation;
          const nextGeneration = `identity-binding-${outcome}:replacement`;
          const queue = (yield* Ref.get(state)).eventQueues.get(String(fixture.sessionId))!;
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          const store = yield* EventStore.EventStoreV2;
          const observed = () =>
            projections
              .getThreadProjection(fixture.threadId)
              .pipe(
                Effect.map(
                  (snapshot) =>
                    snapshot.providerSessions.find((session) => session.id === fixture.sessionId)
                      ?.runtimeIdentity,
                ),
              );
          const drainThroughMarker = (event: ProviderAdapterV2Event) =>
            Effect.gen(function* () {
              const subscription = yield* fixture.runtime.subscribeEvents!;
              const marker: ProviderAdapterV2Event = {
                type: "provider_thread.updated",
                driver: CODEX_DRIVER,
                providerThread: fixture.providerThread,
              };
              yield* Queue.offerAll(queue, [event, marker]);
              const seen = yield* subscription.events.pipe(Stream.runHead);
              assert.isTrue(Option.isSome(seen));
              if (Option.isSome(seen)) assert.strictEqual(seen.value, marker);
            });
          const replacement = yield* opened!.withRuntimeReplacement!(
            nextGeneration,
            Effect.gen(function* () {
              generation = nextGeneration;
              source = undefined;
              yield* opened!.beforeRuntimeReplacement!(nextGeneration);
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(gate);
              if (outcome === "failure")
                return yield* new ProviderAdapterProtocolError({
                  driver: CODEX_DRIVER,
                  detail: "Synthetic managed initialization failed.",
                });
              source = {
                driverKind: CODEX_DRIVER,
                continuationKey: "test-runtime:identity-store",
                runtimeGeneration: generation,
              };
              nativeCompleted = true;
            }).pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  if (!nativeCompleted) candidateCurrent = false;
                }),
              ),
            ),
          ).pipe(Effect.exit, Effect.forkChild);
          yield* Deferred.await(entered);
          const candidate = runtimeIdentityEvent({
            runtime: fixture.runtime,
            providerThread: fixture.providerThread,
            generation: nextGeneration,
            observedModel: "native-candidate-model",
          });
          const origin = capturedTestOrigin(
            fixture.runtime,
            {},
            Effect.suspend(() =>
              candidateCurrent
                ? Effect.void
                : Effect.fail("The failed initialization source is no longer current."),
            ),
          );
          yield* drainThroughMarker(stampProviderEvent(candidate, origin));
          assert.isUndefined(yield* observed());
          if (outcome === "interruption") {
            yield* Fiber.interrupt(replacement);
          } else {
            yield* Deferred.succeed(gate, undefined);
            assert.equal(Exit.isSuccess(yield* Fiber.join(replacement)), outcome === "success");
          }
          if (outcome !== "success")
            yield* fixture.manager.registerRuntimeBinding({
              threadId: fixture.threadId,
              providerSessionId: fixture.sessionId,
              providerThreadId: fixture.providerThread.id,
            });
          const after = yield* observed();
          if (outcome === "success") {
            assert.deepEqual(after, candidate.attestation);
            const owner = (yield* fixture.sink.readProviderRuntimeEvidence(fixture.threadId))!;
            assert.deepEqual(
              yield* fixture.sink.readProviderContinuationSourceIdentity(owner.binding),
              source,
            );
            yield* drainThroughMarker(
              stampProviderEvent(
                runtimeIdentityEvent({
                  runtime: fixture.runtime,
                  providerThread: fixture.providerThread,
                  generation: priorGeneration,
                  observedModel: "stale-prior-model",
                }),
                origin,
              ),
            );
            assert.deepEqual(yield* observed(), candidate.attestation);
          } else {
            assert.isUndefined(after);
            yield* drainThroughMarker(
              stampProviderEvent(
                runtimeIdentityEvent({
                  runtime: fixture.runtime,
                  providerThread: fixture.providerThread,
                  generation: nextGeneration,
                  observedModel: "late-failed-model",
                }),
                origin,
              ),
            );
            assert.isUndefined(yield* observed());
          }
          const persisted = yield* store
            .read({ threadId: fixture.threadId, eventType: "provider-session.updated" })
            .pipe(Stream.runCollect);
          assert.lengthOf(
            persisted.filter(
              (stored) =>
                stored.event.type === "provider-session.updated" &&
                stored.event.payload.runtimeIdentity !== undefined,
            ),
            outcome === "success" ? 1 : 0,
          );
        }).pipe(
          Effect.provide(
            makeTestLayer({
              state,
              idleTimeoutMs: 60_000,
              runtimeGeneration: () => generation,
              continuationSourceIdentity: () => source,
              beforeOpen: (input) =>
                Effect.sync(() => {
                  opened = input;
                  generation = input.nativeOperation!.runtimeGeneration!;
                }),
            }),
          ),
        );
      }),
  );
}

function testAcknowledgedTurn(
  turn: ProviderAdapterV2TurnInput,
  providerTurnId: ProviderTurnId,
  now: DateTime.Utc,
): Extract<ProviderAdapterV2Event, { readonly type: "provider_turn.updated" }> {
  return {
    type: "provider_turn.updated",
    driver: CODEX_DRIVER,
    threadId: turn.threadId,
    providerTurn: {
      id: providerTurnId,
      providerThreadId: turn.providerThread.id,
      nodeId: turn.rootNodeId,
      runAttemptId: turn.attemptId,
      nativeTurnRef: null,
      ordinal: turn.providerTurnOrdinal,
      status: "running",
      startedAt: now,
      completedAt: null,
    },
  };
}

function supersedeResidentStartAttempt(input: {
  readonly sink: EventSink.EventSinkV2["Service"];
  readonly ids: IdAllocator.IdAllocatorV2Shape;
  readonly runtime: ProviderAdapterV2SessionRuntime;
  readonly turn: ProviderAdapterV2TurnInput;
  readonly now: DateTime.Utc;
}) {
  return Effect.gen(function* () {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const snapshot = yield* projections.getThreadProjection(input.turn.threadId);
    const run = snapshot.runs.find((candidate) => candidate.id === input.turn.runId)!;
    const attempt = snapshot.attempts.find((candidate) => candidate.id === input.turn.attemptId)!;
    const attemptId = input.ids.derive.runAttempt({ runId: run.id, attemptOrdinal: 2 });
    yield* input.sink.write({
      events: [
        {
          id: yield* input.ids.allocate.event({ threadId: input.turn.threadId }),
          type: "run.updated",
          threadId: input.turn.threadId,
          runId: run.id,
          occurredAt: input.now,
          payload: { ...run, activeAttemptId: attemptId },
        },
        {
          id: yield* input.ids.allocate.event({ threadId: input.turn.threadId }),
          type: "run-attempt.updated",
          threadId: input.turn.threadId,
          runId: run.id,
          occurredAt: input.now,
          payload: {
            ...attempt,
            id: attemptId,
            attemptOrdinal: 2,
            providerTurnId: null,
            status: "running",
          },
        },
      ],
    });
    const next: ProviderAdapterV2TurnInput = {
      ...input.turn,
      appThread: yield* projections.getThread(input.turn.threadId),
      attemptId,
      nativeOperation: {
        operationId: `${input.turn.nativeOperation!.operationId}:attempt:2`,
        operation: "start_turn",
      },
    };
    yield* input.runtime.startTurn(next);
    return next;
  });
}

it.effect("rejects a same-source superseded attempt before activity ACK and fanout", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    yield* Effect.gen(function* () {
      const fixture = yield* preparePendingStartStopFixture("origin-same-source-attempt");
      yield* fixture.runtime.startTurn(fixture.turn);
      const token = {};
      const producer = capturedTestOrigin(fixture.runtime, token, Effect.void).producer;
      const originalTurnId = fixture.ids.derive.providerTurn({
        driver: CODEX_DRIVER,
        nativeTurnId: "same-source-original-turn",
      });
      const originalOrigin = capturedTestTurnOrigin(
        fixture.runtime,
        producer,
        fixture.turn,
        originalTurnId,
      );
      const queue = (yield* Ref.get(state)).eventQueues.get(String(fixture.sessionId))!;
      const first = yield* fixture.runtime.subscribeEvents!;
      const acknowledged = testAcknowledgedTurn(fixture.turn, originalTurnId, fixture.now);
      yield* Queue.offer(queue, stampProviderEvent(acknowledged, originalOrigin));
      const firstSeen = yield* first.events.pipe(Stream.runHead);
      assert.isTrue(Option.isSome(firstSeen));
      if (Option.isSome(firstSeen)) assert.strictEqual(firstSeen.value, acknowledged);
      const originalTerminal: ProviderAdapterV2Event = {
        type: "turn.terminal",
        driver: CODEX_DRIVER,
        providerThreadId: fixture.providerThread.id,
        providerTurnId: originalTurnId,
        runOrdinal: 1,
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      };
      const finished = yield* fixture.runtime.subscribeEvents!;
      yield* Queue.offer(queue, stampProviderEvent(originalTerminal, originalOrigin));
      assert.isTrue(Option.isSome(yield* finished.events.pipe(Stream.runHead)));
      const next = yield* supersedeResidentStartAttempt(fixture);
      const nextTurnId = fixture.ids.derive.providerTurn({
        driver: CODEX_DRIVER,
        nativeTurnId: "same-source-current-turn",
      });
      const fresh = testAcknowledgedTurn(next, nextTurnId, fixture.now);
      const subscriber = yield* fixture.runtime.subscribeEvents!;
      yield* Queue.offerAll(queue, [
        stampProviderEvent({ ...originalTerminal }, originalOrigin),
        stampProviderEvent({ ...acknowledged }, originalOrigin),
        stampProviderEvent(
          fresh,
          capturedTestTurnOrigin(fixture.runtime, producer, next, nextTurnId),
        ),
      ]);
      const delivered = yield* subscriber.events.pipe(Stream.runHead);
      assert.isTrue(Option.isSome(delivered));
      if (Option.isSome(delivered)) {
        assert.strictEqual(delivered.value, fresh);
        assert.strictEqual(readProviderEventOrigin(delivered.value)!.producer.token, token);
        assert.equal(readProviderEventOrigin(delivered.value)!.turn!.attemptId, next.attemptId);
      }
      yield* TestClock.adjust(1_001);
      assert.equal((yield* Ref.get(state)).closeCount, 0);
      assert.equal((yield* Ref.get(state)).openCount, 1);
      assert.isTrue(Option.isSome(yield* fixture.manager.get(fixture.sessionId)));
    }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1_000 })));
  }),
);

it.effect("rejects subscriber-queued turn ownership superseded on the same source", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    yield* Effect.gen(function* () {
      const fixture = yield* preparePendingStartStopFixture("origin-same-source-subscriber");
      yield* fixture.runtime.startTurn(fixture.turn);
      const producer = capturedTestOrigin(fixture.runtime, {}, Effect.void).producer;
      const oldTurnId = fixture.ids.derive.providerTurn({
        driver: CODEX_DRIVER,
        nativeTurnId: "same-source-buffered-old-turn",
      });
      const oldOrigin = capturedTestTurnOrigin(fixture.runtime, producer, fixture.turn, oldTurnId);
      const buffered = yield* fixture.runtime.subscribeEvents!;
      const witness = yield* fixture.runtime.subscribeEvents!;
      const old = testAcknowledgedTurn(fixture.turn, oldTurnId, fixture.now);
      const queue = (yield* Ref.get(state)).eventQueues.get(String(fixture.sessionId))!;
      yield* Queue.offer(queue, stampProviderEvent(old, oldOrigin));
      const seen = yield* witness.events.pipe(Stream.runHead);
      assert.isTrue(Option.isSome(seen));
      if (Option.isSome(seen)) assert.strictEqual(seen.value, old);
      const next = yield* supersedeResidentStartAttempt(fixture);
      const freshTurnId = fixture.ids.derive.providerTurn({
        driver: CODEX_DRIVER,
        nativeTurnId: "same-source-buffered-new-turn",
      });
      const fresh = testAcknowledgedTurn(next, freshTurnId, fixture.now);
      yield* Queue.offer(
        queue,
        stampProviderEvent(
          fresh,
          capturedTestTurnOrigin(fixture.runtime, producer, next, freshTurnId),
        ),
      );
      const delivered = yield* buffered.events.pipe(Stream.runHead);
      assert.isTrue(Option.isSome(delivered));
      if (Option.isSome(delivered)) assert.strictEqual(delivered.value, fresh);
      assert.equal((yield* Ref.get(state)).openCount, 1);
    }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000 })));
  }),
);

it.effect(
  "rejects old adapter-queued events after their captured callback source is replaced",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const oldToken = {};
      const freshToken = {};
      const currentSource = yield* Ref.make(oldToken);
      yield* Effect.gen(function* () {
        const fixture = yield* preparePendingStartStopFixture("origin-old-adapter-queue");
        const subscription = yield* fixture.runtime.subscribeEvents!;
        const received = yield* subscription.events.pipe(Stream.runHead, Effect.forkChild);
        const oldOrigin = capturedTestOrigin(
          fixture.runtime,
          oldToken,
          Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.andThen(Ref.get(currentSource)),
            Effect.flatMap((token) =>
              token === oldToken ? Effect.void : Effect.fail("original_callback_source_replaced"),
            ),
          ),
        );
        const staleSession: ProviderAdapterV2Event = {
          type: "provider_session.updated",
          driver: CODEX_DRIVER,
          providerSession: {
            ...fixture.runtime.providerSession,
            status: "error",
            lastError: "stale source",
            updatedAt: fixture.now,
          },
        };
        const pending = yield* makePendingRuntimeRequestEvents({
          idAllocator: fixture.ids,
          threadId: fixture.threadId,
          providerSessionId: fixture.sessionId,
          providerThread: fixture.providerThread,
          now: fixture.now,
        });
        const queue = (yield* Ref.get(state)).eventQueues.get(String(fixture.sessionId))!;
        yield* Queue.offer(queue, stampProviderEvent(staleSession, oldOrigin));
        yield* Deferred.await(entered);
        yield* Queue.offerAll(
          queue,
          pending.providerEvents.map((event) => stampProviderEvent(event, oldOrigin)),
        );
        yield* Ref.set(currentSource, freshToken);
        yield* Deferred.succeed(release, undefined);
        const fresh: ProviderAdapterV2Event = {
          type: "provider_session.updated",
          driver: CODEX_DRIVER,
          providerSession: {
            ...fixture.runtime.providerSession,
            status: "ready",
            updatedAt: fixture.now,
          },
        };
        const freshOrigin = capturedTestOrigin(
          fixture.runtime,
          freshToken,
          Ref.get(currentSource).pipe(
            Effect.flatMap((token) =>
              token === freshToken ? Effect.void : Effect.fail("fresh_callback_source_replaced"),
            ),
          ),
        );
        yield* Queue.offer(queue, stampProviderEvent(fresh, freshOrigin));
        const delivered = yield* Fiber.join(received);
        assert.isTrue(Option.isSome(delivered));
        if (Option.isSome(delivered)) {
          assert.strictEqual(delivered.value, fresh);
          assert.strictEqual(readProviderEventOrigin(delivered.value)!.producer.token, freshToken);
        }
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        assert.isEmpty((yield* projections.getThreadProjection(fixture.threadId)).runtimeRequests);
        const store = yield* EventStore.EventStoreV2;
        const persisted = yield* store
          .read({ threadId: fixture.threadId, eventType: "provider-session.updated" })
          .pipe(Stream.runCollect);
        assert.isFalse(
          persisted.some(
            (event) =>
              event.event.type === "provider-session.updated" &&
              event.event.payload.status === "error",
          ),
        );
        assert.isTrue(Option.isSome(yield* fixture.manager.get(fixture.sessionId)));
        assert.equal((yield* Ref.get(state)).closeCount, 0);
      }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000 })));
    }),
);

it.effect(
  "drops old subscriber-queued origins and preserves fresh event identity through fanout",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      let generation = "origin-buffered-old-generation";
      const oldToken = {};
      const freshToken = {};
      const source = yield* Ref.make(oldToken);
      yield* Effect.gen(function* () {
        const fixture = yield* preparePendingStartStopFixture("origin-old-subscriber-queue");
        const buffered = yield* fixture.runtime.subscribeEvents!;
        const witness = yield* fixture.runtime.subscribeEvents!;
        const queue = (yield* Ref.get(state)).eventQueues.get(String(fixture.sessionId))!;
        const old: ProviderAdapterV2Event = {
          type: "provider_thread.updated",
          driver: CODEX_DRIVER,
          providerThread: fixture.providerThread,
        };
        const oldOrigin = capturedTestOrigin(
          fixture.runtime,
          oldToken,
          Ref.get(source).pipe(
            Effect.flatMap((token) =>
              token === oldToken ? Effect.void : Effect.fail("old_source_replaced"),
            ),
          ),
        );
        yield* Queue.offer(queue, stampProviderEvent(old, oldOrigin));
        const seen = yield* witness.events.pipe(Stream.runHead);
        assert.isTrue(Option.isSome(seen));
        if (Option.isSome(seen)) assert.strictEqual(seen.value, old);
        generation = "origin-buffered-fresh-generation";
        yield* Ref.set(source, freshToken);
        const fresh: ProviderAdapterV2Event = {
          type: "provider_thread.updated",
          driver: CODEX_DRIVER,
          providerThread: { ...fixture.providerThread, status: "active" },
        };
        yield* Queue.offer(
          queue,
          stampProviderEvent(
            fresh,
            capturedTestOrigin(
              fixture.runtime,
              freshToken,
              Ref.get(source).pipe(
                Effect.flatMap((token) =>
                  token === freshToken ? Effect.void : Effect.fail("fresh_source_replaced"),
                ),
              ),
            ),
          ),
        );
        const delivered = yield* buffered.events.pipe(Stream.runHead);
        assert.isTrue(Option.isSome(delivered));
        if (Option.isSome(delivered)) {
          assert.strictEqual(delivered.value, fresh);
          assert.equal(
            readProviderEventOrigin(delivered.value)!.producer.runtimeGeneration,
            generation,
          );
        }
      }).pipe(
        Effect.provide(
          makeTestLayer({ state, idleTimeoutMs: 60_000, runtimeGeneration: () => generation }),
        ),
      );
    }),
);

it.effect(
  "rejects a stale terminal before releasing the pinned stop barrier and drains its fresh original turn",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const interruptEntered = yield* Deferred.make<void>();
      const completed = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const fixture = yield* preparePendingStartStopFixture("origin-stop-terminal");
        const turnId = fixture.ids.derive.providerTurn({
          driver: CODEX_DRIVER,
          nativeTurnId: "origin-stop-actual-turn",
        });
        yield* fixture.sink.write({
          events: [
            {
              id: yield* fixture.ids.allocate.event({ threadId: fixture.threadId }),
              type: "provider-turn.updated",
              threadId: fixture.threadId,
              runId: fixture.turn.runId,
              driver: CODEX_DRIVER,
              occurredAt: fixture.now,
              payload: {
                id: turnId,
                providerThreadId: fixture.providerThread.id,
                nodeId: fixture.turn.rootNodeId,
                runAttemptId: fixture.turn.attemptId,
                nativeTurnRef: null,
                ordinal: 1,
                status: "running",
                startedAt: fixture.now,
                completedAt: null,
              },
            },
          ],
        });
        const binding = (yield* fixture.sink.readCurrentProviderRuntimeOwner(fixture.threadId))!;
        const subscription = yield* fixture.runtime.subscribeEvents!;
        const received = yield* subscription.events.pipe(Stream.runCollect, Effect.forkChild);
        const stopping = yield* fixture.manager
          .stopPinnedRuntime({
            operationId: "effect:origin-stop-terminal",
            binding: { ...binding.binding, runtimeGeneration: binding.binding.runtimeGeneration! },
            expectedEvidenceRevision: binding.evidenceRevision,
          })
          .pipe(
            Effect.tap(() => Deferred.succeed(completed, undefined)),
            Effect.forkChild,
          );
        yield* Deferred.await(interruptEntered);
        const terminal: ProviderAdapterV2Event = {
          type: "turn.terminal",
          driver: CODEX_DRIVER,
          providerThreadId: fixture.providerThread.id,
          providerTurnId: turnId,
          runOrdinal: 1,
          status: "interrupted",
          failure: null,
          threadDisposition: "reusable",
        };
        const queue = (yield* Ref.get(state)).eventQueues.get(String(fixture.sessionId))!;
        yield* Queue.offer(
          queue,
          stampProviderEvent(
            { ...terminal },
            capturedTestOrigin(fixture.runtime, {}, Effect.fail("old_native_callback_replaced")),
          ),
        );
        for (let i = 0; i < 20; i++) yield* Effect.yieldNow;
        assert.isFalse(yield* Deferred.isDone(completed));
        assert.equal((yield* Ref.get(state)).closeCount, 0);
        const origin = {
          ...capturedTestOrigin(fixture.runtime, {}, Effect.void),
          turn: {
            binding: {
              threadId: fixture.threadId,
              providerThreadId: fixture.providerThread.id,
              providerSessionId: fixture.sessionId,
              instanceId: fixture.runtime.instanceId,
              nativeThreadId: fixture.providerThread.nativeThreadRef!.nativeId,
              runtimeGeneration: fixture.runtime.runtimeGeneration!,
            },
            runId: fixture.turn.runId,
            attemptId: fixture.turn.attemptId,
            providerTurnId: turnId,
          },
        };
        yield* Queue.offer(queue, stampProviderEvent(terminal, origin));
        assert.equal((yield* Fiber.join(stopping)).status, "stopped");
        assert.deepEqual(yield* Fiber.join(received), [terminal]);
        assert.equal((yield* Ref.get(state)).closeCount, 1);
      }).pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 60_000,
            beforeInterruptTurn: () => Deferred.succeed(interruptEntered, undefined),
          }),
        ),
      );
    }),
);

it.effect("drains current stamped terminal objects after the provider scope exits", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const closed = yield* Deferred.make<void>();
    yield* Effect.gen(function* () {
      const fixture = yield* preparePendingStartStopFixture("origin-exit-terminal-drain");
      yield* fixture.runtime.startTurn(fixture.turn);
      const queue = (yield* Ref.get(state)).eventQueues.get(String(fixture.sessionId))!;
      const token = {};
      const turnId = fixture.ids.derive.providerTurn({
        driver: CODEX_DRIVER,
        nativeTurnId: "origin-exit-turn",
      });
      const origin = capturedTestTurnOrigin(
        fixture.runtime,
        capturedTestOrigin(fixture.runtime, token, Effect.void).producer,
        fixture.turn,
        turnId,
      );
      const acknowledgement = yield* fixture.runtime.subscribeEvents!;
      const acknowledged = testAcknowledgedTurn(fixture.turn, turnId, fixture.now);
      yield* Queue.offer(queue, stampProviderEvent(acknowledged, origin));
      const seen = yield* acknowledgement.events.pipe(Stream.runHead);
      assert.isTrue(Option.isSome(seen));
      if (Option.isSome(seen)) assert.strictEqual(seen.value, acknowledged);
      const subscription = yield* fixture.runtime.subscribeEvents!;
      const terminal: ProviderAdapterV2Event = {
        type: "turn.terminal",
        driver: CODEX_DRIVER,
        providerThreadId: fixture.providerThread.id,
        providerTurnId: turnId,
        runOrdinal: 1,
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      };
      const stopped: ProviderAdapterV2Event = {
        type: "provider_session.updated",
        driver: CODEX_DRIVER,
        providerSession: {
          ...fixture.runtime.providerSession,
          status: "stopped",
          updatedAt: fixture.now,
        },
      };
      yield* Queue.offerAll(queue, [
        stampProviderEvent(terminal, origin),
        stampProviderEvent(stopped, origin),
      ]);
      yield* Queue.end(queue);
      yield* Deferred.await(closed);
      assert.equal((yield* Ref.get(state)).closeCount, 1);
      const received = yield* subscription.events.pipe(Stream.runCollect);
      assert.deepEqual(received, [terminal, stopped]);
      assert.strictEqual(received[0], terminal);
      assert.strictEqual(received[1], stopped);
      assert.strictEqual(readProviderEventOrigin(received[0]!)!.producer.token, token);
    }).pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 60_000,
          afterScopeClose: Deferred.succeed(closed, undefined).pipe(Effect.asVoid),
        }),
      ),
    );
  }),
);

it.effect(
  "rejects retained subscriber events when another resident replaces their original runtime object",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const closed = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const fixture = yield* preparePendingStartStopFixture("origin-resident-object-replacement");
        const buffered = yield* fixture.runtime.subscribeEvents!;
        const witness = yield* fixture.runtime.subscribeEvents!;
        const origin = capturedTestOrigin(fixture.runtime, {}, Effect.void);
        const original: ProviderAdapterV2Event = {
          type: "provider_thread.updated",
          driver: CODEX_DRIVER,
          providerThread: fixture.providerThread,
        };
        const stopped: ProviderAdapterV2Event = {
          type: "provider_session.updated",
          driver: CODEX_DRIVER,
          providerSession: {
            ...fixture.runtime.providerSession,
            status: "stopped",
            updatedAt: fixture.now,
          },
        };
        const queue = (yield* Ref.get(state)).eventQueues.get(String(fixture.sessionId))!;
        yield* Queue.offer(queue, stampProviderEvent(original, origin));
        const seen = yield* witness.events.pipe(Stream.runHead);
        assert.isTrue(Option.isSome(seen));
        if (Option.isSome(seen)) assert.strictEqual(seen.value, original);
        yield* Queue.offer(queue, stampProviderEvent(stopped, origin));
        yield* Queue.end(queue);
        yield* Deferred.await(closed);
        const replacement = yield* fixture.manager.open({
          threadId: fixture.threadId,
          providerSessionId: fixture.sessionId,
          modelSelection,
          runtimePolicy,
        });
        assert.notStrictEqual(replacement, fixture.runtime);
        assert.isEmpty(yield* buffered.events.pipe(Stream.runCollect));
        assert.equal((yield* Ref.get(state)).openCount, 2);
      }).pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 60_000,
            afterScopeClose: Deferred.succeed(closed, undefined).pipe(Effect.asVoid),
          }),
        ),
      );
    }),
);

function seedManagedReplacementOwner(input: {
  readonly sink: EventSink.EventSinkV2["Service"];
  readonly ids: IdAllocator.IdAllocatorV2Shape;
  readonly threadId: ThreadId;
  readonly sessionId: ProviderSessionId;
  readonly now: DateTime.Utc;
  readonly nativeId: string;
}) {
  return Effect.gen(function* () {
    const providerThread = {
      ...makeProviderThread({
        idAllocator: input.ids,
        threadId: input.threadId,
        providerSessionId: input.sessionId,
        now: input.now,
      }),
      id: input.ids.derive.providerThread({ driver: CODEX_DRIVER, nativeThreadId: input.nativeId }),
      nativeThreadRef: {
        driver: CODEX_DRIVER,
        nativeId: input.nativeId,
        strength: "strong" as const,
      },
    };
    const created = yield* makeThreadCreatedEvent({
      idAllocator: input.ids,
      threadId: input.threadId,
      now: input.now,
    });
    yield* input.sink.write({
      events: [
        { ...created, payload: { ...created.payload, activeProviderThreadId: providerThread.id } },
        {
          id: yield* input.ids.allocate.event({ threadId: input.threadId }),
          type: "provider-thread.updated",
          threadId: input.threadId,
          occurredAt: input.now,
          payload: providerThread,
        },
      ],
    });
    return providerThread;
  });
}

function preparePendingStartStopFixture(name: string, registerBinding = true) {
  return Effect.gen(function* () {
    const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
    const sink = yield* EventSink.EventSinkV2;
    const ids = yield* IdAllocator.IdAllocatorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const now = yield* DateTime.now;
    const threadId = ThreadId.make(name);
    const sessionId = yield* ids.allocate.providerSession({
      providerInstanceId: modelSelection.instanceId,
      threadId,
    });
    const providerThread = yield* seedManagedReplacementOwner({
      sink,
      ids,
      threadId,
      sessionId,
      now,
      nativeId: `${name}-native`,
    });
    const runtime = yield* manager.open({
      threadId,
      providerSessionId: sessionId,
      modelSelection,
      runtimePolicy,
    });
    const runId = ids.derive.run({ threadId, ordinal: 1 });
    const attemptId = ids.derive.runAttempt({ runId, attemptOrdinal: 1 });
    const rootNodeId = ids.derive.rootNode({ runId });
    const messageId = yield* persistStartedRuntimeFixture({
      eventSink: sink,
      idAllocator: ids,
      threadId,
      providerThread,
      runId,
      attemptId,
      rootNodeId,
      now,
    });
    if (registerBinding)
      yield* manager.registerRuntimeBinding({
        threadId,
        providerSessionId: sessionId,
        providerThreadId: providerThread.id,
        runId,
        attemptId,
      });
    const turn: ProviderAdapterV2TurnInput = {
      appThread: yield* projections.getThread(threadId),
      threadId,
      runId,
      attemptId,
      rootNodeId,
      runOrdinal: 1,
      providerTurnOrdinal: 1,
      providerThread,
      modelSelection,
      runtimePolicy,
      nativeOperation: { operationId: `${name}-actual-start`, operation: "start_turn" },
      message: {
        messageId,
        text: "pending start",
        attachments: [],
        createdBy: "user",
        creationSource: "web",
      },
    };
    const stopInput = (recorded: ProviderAdapterV2TurnInput): ProviderPendingStartStopInput => ({
      binding: {
        threadId,
        providerThreadId: providerThread.id,
        providerSessionId: sessionId,
        instanceId: runtime.instanceId,
        nativeThreadId: providerThread.nativeThreadRef!.nativeId,
        runtimeGeneration: runtime.runtimeGeneration!,
      },
      runId,
      attemptId,
      startOperation: recorded.nativeOperation!,
    });
    return {
      manager,
      sink,
      ids,
      now,
      threadId,
      sessionId,
      providerThread,
      runtime,
      turn,
      stopInput,
    };
  });
}

function managedActorAdmission(
  fixture: Effect.Success<ReturnType<typeof preparePendingStartStopFixture>>,
) {
  return Effect.gen(function* () {
    const birth = (yield* fixture.sink.readApplicationBirthRecord(fixture.threadId))!;
    const commandId = CommandId.make(`command:managed-actor:${fixture.threadId}`);
    const command = { type: "message.dispatch", commandId, threadId: fixture.threadId };
    const nowMs = DateTime.toEpochMillis(fixture.now);
    const capture = yield* Schema.decodeUnknownEffect(OrdinaryCheckoutCaptureV1)({
      version: 1,
      commandId,
      commandType: command.type,
      canonicalCommand: command,
      commandDigest: ordinaryCheckoutCommandDigestV1(command),
      origin: { kind: "command" },
      threadId: fixture.threadId,
      applicationBirth: birth,
      projectId: "synthetic-actor-project",
      canonicalProjectRoot: "/synthetic-actor-repo",
      canonicalCheckoutPath: "/synthetic-actor-checkout",
      branch: "actor-fixture",
      lease: {
        resourcePath: "/synthetic-actor-checkout",
        leaseId: `synthetic-lease:${fixture.threadId}`,
        ownerThreadId: fixture.threadId,
        ownerIncarnation: ordinaryApplicationIncarnationV1(birth),
        branch: "actor-fixture",
        acquiredAtMs: nowMs,
        renewedAtMs: nowMs,
        expiresAtMs: nowMs + 300000,
      },
    }).pipe(Effect.orDie);
    const at = DateTime.formatIso(fixture.now);
    const admission = yield* Schema.decodeUnknownEffect(OrdinaryCheckoutAdmissionV1)({
      version: 1,
      admissionId: ordinaryCheckoutAdmissionIdV1(capture),
      capture: yield* Schema.encodeEffect(OrdinaryCheckoutCaptureV1)(capture).pipe(Effect.orDie),
      receipt: {
        commandId,
        threadId: fixture.threadId,
        commandType: command.type,
        acceptedAt: at,
        resultSequence: birth.sequence,
        status: "accepted",
        error: null,
      },
      eventBasis: [
        {
          eventId: birth.eventId,
          sequence: birth.sequence,
          threadId: fixture.threadId,
          commandId,
          eventType: "message.accepted",
        },
      ],
      run: {
        runId: fixture.turn.runId,
        runAttemptId: fixture.turn.attemptId,
        nodeId: fixture.turn.rootNodeId,
        messageId: fixture.turn.message.messageId,
      },
      recordedAt: at,
    }).pipe(Effect.orDie);
    const admissionRef = ordinaryCheckoutAdmissionRefV1(admission);
    const source = {
      kind: "outbox",
      link: {
        version: 1,
        effectId: `effect:${commandId}:provider-turn.start`,
        commandId,
        threadId: fixture.threadId,
        requestSha256: "c".repeat(64),
        admission: admissionRef,
        recordedAt: at,
      },
      workerId: "synthetic-actor-worker",
      expectedAttempt: 1,
      leaseExpiresAt: DateTime.formatIso(DateTime.add(fixture.now, { minutes: 5 })),
    };
    const originalUse = yield* Schema.decodeUnknownEffect(OrdinaryCheckoutUseV1)({
      version: 1,
      kind: "ordinary_checkout_use",
      operationId: source.link.effectId,
      admission: admissionRef,
      source,
      lease: capture.lease,
    }).pipe(Effect.orDie);
    return {
      startExecution: makeOrdinaryCheckoutExecutionRefV1({
        originalUse,
        executor: yield* Schema.decodeUnknownEffect(OrdinaryCheckoutExecutionExecutorV1)({
          kind: "actual_outbox_claim",
          source,
        }).pipe(Effect.orDie),
      }),
      admission,
      checkpointScopeId: CheckpointScopeId.make(`scope:${fixture.threadId}`),
      providerThreadId: fixture.providerThread.id,
    } satisfies ProviderManagedActorAdmissionV1;
  });
}

it.effect("prepares the actual raw managed actor before entry without inventing a generation", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    let raw: ProviderAdapterV2SessionRuntime | undefined;
    let offered: ProviderManagedActorAdmissionV1 | undefined;
    const order: Array<string> = [];
    yield* Effect.gen(function* () {
      const fixture = yield* preparePendingStartStopFixture("managed-actor-raw-preparation", false);
      const admission = yield* managedActorAdmission(fixture);
      assert.notStrictEqual(raw, fixture.runtime);
      const copied = yield* fixture.manager
        .prepareOrdinaryManagedActorRun({ runtime: { ...fixture.runtime }, admission })
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(copied));
      assert.isEmpty(order);
      const reader = yield* fixture.manager.prepareOrdinaryManagedActorRun({
        runtime: fixture.runtime,
        admission,
      });
      yield* Effect.addFinalizer(() => reader.release);
      assert.equal(offered!.providerThreadId, fixture.providerThread.id);
      assert.deepEqual(offered!.admission.run, admission.admission.run);
      assert.isUndefined(raw!.runtimeGeneration);
      assert.deepEqual(order, ["prepare"]);
      yield* raw!.startTurn(fixture.turn);
      assert.deepEqual(order, ["prepare", "native-entry"]);
      assert.equal((yield* reader.readClosure).status, "pending");
      assert.isTrue(Exit.isFailure(yield* reader.revalidateCompletionBinding.pipe(Effect.exit)));
    }).pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 60_000,
          runtimeGeneration: () => undefined,
          onRuntimeCreated: (runtime) => {
            raw = runtime;
            registerProviderManagedActorProducer(runtime, ({ admission }) =>
              Effect.sync(() => {
                offered = admission;
                order.push("prepare");
                return { revalidateMutation: Effect.void, revalidateCompletion: Effect.void };
              }),
            );
          },
          beforeStartTurn: () =>
            Effect.sync(() => {
              order.push("native-entry");
            }),
        }),
      ),
    );
  }),
);

it.effect(
  "qualifies a completed original actor binding after lifecycle stop while rejecting new mutation",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      let raw: ProviderAdapterV2SessionRuntime | undefined;
      let issuer: ProviderManagedActorIssuerV1 | undefined;
      yield* Effect.gen(function* () {
        const fixture = yield* preparePendingStartStopFixture("managed-actor-early-completion");
        const admission = yield* managedActorAdmission(fixture);
        const capture = yield* fixture.manager.captureOrdinaryExecutionAttachment({
          runtime: fixture.runtime,
          threadId: fixture.threadId,
          providerThread: fixture.providerThread,
          runId: fixture.turn.runId,
          attemptId: fixture.turn.attemptId,
        });
        const reader = yield* fixture.manager.prepareOrdinaryManagedActorRun({
          runtime: fixture.runtime,
          admission,
        });
        yield* Effect.addFinalizer(() => reader.release);
        const actor = yield* issuer!.admitActor({
          kind: "native_request",
          actualSource: {
            sourceId: "synthetic-original-local-task",
            driver: raw!.driver,
            instanceId: raw!.instanceId,
            providerSessionId: raw!.providerSessionId,
            threadId: fixture.threadId,
            providerThreadId: fixture.providerThread.id,
          },
          completionMode: "task_join",
        });
        yield* issuer!.markActorEntered(actor);
        const task = yield* Effect.void.pipe(Effect.forkChild);
        yield* issuer!.requireTaskJoin(actor, { taskId: "synthetic-owned-task", fiber: task });
        yield* issuer!.joinTask(actor, "synthetic-owned-task");
        yield* issuer!.seal;
        Object.assign(raw!.providerSession, { status: "stopped" });
        assert.isTrue(Exit.isFailure(yield* capture.revalidateCaptured.pipe(Effect.exit)));
        yield* capture.revalidateCompletionBinding;
        yield* reader.revalidateCompletionBinding;
        assert.equal((yield* reader.readClosure).status, "pending");
        assert.equal((yield* Ref.get(state)).closeCount, 0);
        Object.assign(raw!.providerSession, { status: "ready" });
        yield* supersedeResidentStartAttempt(fixture);
        assert.isTrue(Exit.isFailure(yield* capture.revalidateCompletionBinding.pipe(Effect.exit)));
      }).pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 60_000,
            onRuntimeCreated: (runtime) => {
              raw = runtime;
              registerProviderManagedActorProducer(runtime, (input) =>
                Effect.sync(() => {
                  issuer = input.issuer;
                  return { revalidateMutation: Effect.void, revalidateCompletion: Effect.void };
                }),
              );
            },
          }),
        ),
      );
    }),
);

it.effect("captures an actual ordinary execution result and rejects a copied runtime", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    yield* Effect.gen(function* () {
      const fixture = yield* preparePendingStartStopFixture("ordinary-capture-result");
      yield* fixture.runtime.startTurn(fixture.turn);
      const capture = yield* fixture.manager.captureOrdinaryExecutionAttachment({
        runtime: fixture.runtime,
        threadId: fixture.threadId,
        providerThread: fixture.providerThread,
        runId: fixture.turn.runId,
        attemptId: fixture.turn.attemptId,
      });
      yield* capture.revalidateCaptured;
      const owner = (yield* fixture.sink.readCurrentProviderRuntimeOwner(fixture.threadId))!;
      const runtimeGeneration = fixture.runtime.runtimeGeneration;
      if (runtimeGeneration === undefined)
        return yield* Effect.die("The captured fixture runtime must have a generation.");
      assert.deepEqual(capture.binding, {
        threadId: fixture.threadId,
        providerThreadId: fixture.providerThread.id,
        providerSessionId: fixture.sessionId,
        instanceId: fixture.runtime.instanceId,
        runtimeGeneration,
        nativeThreadId: requireNativeThreadId(fixture.providerThread),
        evidenceRevision: owner.evidenceRevision,
      });
      assert.equal(capture.runId, fixture.turn.runId);
      assert.equal(capture.attemptId, fixture.turn.attemptId);
      assert.isUndefined(capture.providerTurnId);
      assert.isTrue(
        Exit.isFailure(
          yield* fixture.manager
            .captureOrdinaryExecutionAttachment({
              runtime: { ...fixture.runtime },
              threadId: fixture.threadId,
              providerThread: fixture.providerThread,
            })
            .pipe(Effect.exit),
        ),
      );
      assert.strictEqual(
        Option.getOrUndefined(yield* fixture.manager.get(fixture.sessionId)),
        fixture.runtime,
      );
      yield* capture.revalidateCaptured;
      assert.equal((yield* Ref.get(state)).closeCount, 0);
    }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000 })));
  }),
);

it.effect(
  "retains a late produced ordinary capture after its original attempt is superseded without stopping the replacement",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      yield* Effect.gen(function* () {
        const fixture = yield* preparePendingStartStopFixture("ordinary-capture-late-result");
        yield* fixture.runtime.startTurn(fixture.turn);
        const next = yield* supersedeResidentStartAttempt(fixture);
        const before = yield* Ref.get(state);
        const captured = yield* fixture.manager
          .captureOrdinaryExecutionAttachment({
            runtime: fixture.runtime,
            threadId: fixture.threadId,
            providerThread: fixture.providerThread,
            runId: fixture.turn.runId,
            attemptId: fixture.turn.attemptId,
          })
          .pipe(Effect.exit);
        assert.isTrue(Exit.isSuccess(captured));
        if (Exit.isFailure(captured)) return;
        assert.equal(captured.value.runId, fixture.turn.runId);
        assert.equal(captured.value.attemptId, fixture.turn.attemptId);
        assert.notEqual(captured.value.attemptId, next.attemptId);
        assert.isTrue(Exit.isFailure(yield* captured.value.revalidateCaptured.pipe(Effect.exit)));
        assert.equal(
          (yield* captured.value.stopCaptured({ operationId: "ordinary-capture-late-stop" }))
            .status,
          "unknown",
        );
        assert.deepEqual(yield* Ref.get(state), before);
        assert.strictEqual(
          Option.getOrThrow(yield* fixture.manager.get(fixture.sessionId)),
          fixture.runtime,
        );
      }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000 })));
    }),
);

it.effect(
  "stops the captured ordinary target inside its reservation while preserving a later pooled sibling",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      yield* Effect.gen(function* () {
        const fixture = yield* preparePendingStartStopFixture("ordinary-capture-pooled");
        const capture = yield* fixture.manager.captureOrdinaryExecutionAttachment({
          runtime: fixture.runtime,
          threadId: fixture.threadId,
          providerThread: fixture.providerThread,
        });
        const sibling = ThreadId.make("ordinary-capture-pooled-sibling");
        const siblingProvider = yield* seedManagedReplacementOwner({
          ...fixture,
          threadId: sibling,
          nativeId: "ordinary-capture-sibling-native",
        });
        yield* fixture.manager.open({
          threadId: sibling,
          providerSessionId: fixture.sessionId,
          modelSelection,
          runtimePolicy,
        });
        yield* fixture.manager.registerRuntimeBinding({
          threadId: sibling,
          providerSessionId: fixture.sessionId,
          providerThreadId: siblingProvider.id,
        });
        yield* capture.revalidateCaptured;
        const siblingBefore = yield* fixture.manager.readCurrentThreadRuntimeAttachment(sibling);
        const result = yield* capture.stopCaptured({ operationId: "ordinary-capture-pooled-stop" });
        assert.equal(result.status, "stopped");
        assert.equal((yield* Ref.get(state)).closeCount, 0);
        assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, [
          fixture.providerThread.nativeThreadRef!.nativeId,
        ]);
        const siblingAfter = yield* fixture.manager.readCurrentThreadRuntimeAttachment(sibling);
        assert.equal(siblingAfter.status, "attached");
        if (siblingBefore.status === "attached" && siblingAfter.status === "attached")
          assert.deepEqual(siblingAfter.binding, siblingBefore.binding);
        assert.isTrue(Exit.isFailure(yield* capture.revalidateCaptured.pipe(Effect.exit)));
      }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000 })));
    }),
);

it.effect(
  "rejects an ordinary capture after detach and reattach without stopping the replacement attachment",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      yield* Effect.gen(function* () {
        const fixture = yield* preparePendingStartStopFixture("ordinary-capture-reattach");
        const capture = yield* fixture.manager.captureOrdinaryExecutionAttachment({
          runtime: fixture.runtime,
          threadId: fixture.threadId,
          providerThread: fixture.providerThread,
        });
        yield* fixture.manager.detach({
          providerSessionId: fixture.sessionId,
          threadId: fixture.threadId,
        });
        const replacement = yield* fixture.manager.open({
          threadId: fixture.threadId,
          providerSessionId: fixture.sessionId,
          modelSelection,
          runtimePolicy,
        });
        assert.strictEqual(replacement, fixture.runtime);
        yield* fixture.manager.registerRuntimeBinding({
          threadId: fixture.threadId,
          providerSessionId: fixture.sessionId,
          providerThreadId: fixture.providerThread.id,
        });
        const before = yield* Ref.get(state);
        assert.isTrue(Exit.isFailure(yield* capture.revalidateCaptured.pipe(Effect.exit)));
        assert.equal(
          (yield* capture.stopCaptured({ operationId: "ordinary-capture-stale-stop" })).status,
          "unknown",
        );
        assert.deepEqual(yield* Ref.get(state), before);
        const fresh = yield* fixture.manager.captureOrdinaryExecutionAttachment({
          runtime: replacement,
          threadId: fixture.threadId,
          providerThread: fixture.providerThread,
        });
        assert.notEqual(fresh.captureId, capture.captureId);
        yield* fresh.revalidateCaptured;
      }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000 })));
    }),
);

it.effect(
  "retains absent ordinary capture evidence without fabricating a registered stop binding",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      yield* Effect.gen(function* () {
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const sink = yield* EventSink.EventSinkV2;
        const ids = yield* IdAllocator.IdAllocatorV2;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("ordinary-capture-unregistered");
        const sessionId = yield* ids.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const existing = yield* seedManagedReplacementOwner({
          sink,
          ids,
          now,
          threadId,
          sessionId,
          nativeId: "unused-native",
        });
        const providerThread = {
          ...existing,
          nativeThreadRef: null,
          nativeConversationHeadRef: null,
        };
        yield* sink.write({
          events: [
            {
              id: yield* ids.allocate.event({ threadId }),
              type: "provider-thread.updated",
              threadId,
              occurredAt: now,
              payload: providerThread,
            },
          ],
        });
        const runtime = yield* manager.open({
          threadId,
          providerSessionId: sessionId,
          modelSelection,
          runtimePolicy,
        });
        const capture = yield* manager.captureOrdinaryExecutionAttachment({
          runtime,
          threadId,
          providerThread,
        });
        yield* capture.revalidateCaptured;
        assert.isFalse(Object.hasOwn(capture.binding, "runtimeGeneration"));
        assert.isFalse(Object.hasOwn(capture.binding, "nativeThreadId"));
        assert.isFalse(Object.hasOwn(capture.binding, "evidenceRevision"));
        assert.equal(
          (yield* capture.stopCaptured({ operationId: "ordinary-capture-unregistered-stop" }))
            .status,
          "unknown",
        );
        assert.equal((yield* Ref.get(state)).closeCount, 0);
        assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, []);
      }).pipe(
        Effect.provide(
          makeTestLayer({ state, idleTimeoutMs: 60_000, runtimeGeneration: () => undefined }),
        ),
      );
    }),
);

it.effect("rejects the old ordinary capture after an actual same-object managed replacement", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    let generation = "";
    let source: ProviderContinuationSourceIdentity | undefined;
    let opened: ProviderAdapterV2OpenSessionInput | undefined;
    yield* Effect.gen(function* () {
      const fixture = yield* preparePendingStartStopFixture("ordinary-capture-generation");
      const capture = yield* fixture.manager.captureOrdinaryExecutionAttachment({
        runtime: fixture.runtime,
        threadId: fixture.threadId,
        providerThread: fixture.providerThread,
      });
      yield* opened!.withRuntimeReplacement!(
        "ordinary-capture-new-generation",
        Effect.gen(function* () {
          generation = "ordinary-capture-new-generation";
          source = undefined;
          yield* opened!.beforeRuntimeReplacement!(generation);
          source = {
            driverKind: CODEX_DRIVER,
            continuationKey: "ordinary-capture-native-home",
            runtimeGeneration: generation,
          };
        }),
      );
      const before = yield* Ref.get(state);
      assert.isTrue(Exit.isFailure(yield* capture.revalidateCaptured.pipe(Effect.exit)));
      assert.equal(
        (yield* capture.stopCaptured({ operationId: "ordinary-capture-old-generation-stop" }))
          .status,
        "unknown",
      );
      assert.deepEqual(yield* Ref.get(state), before);
      const current = Option.getOrThrow(yield* fixture.manager.get(fixture.sessionId));
      assert.strictEqual(current, fixture.runtime);
      const fresh = yield* fixture.manager.captureOrdinaryExecutionAttachment({
        runtime: current,
        threadId: fixture.threadId,
        providerThread: fixture.providerThread,
      });
      assert.equal(fresh.binding.runtimeGeneration, generation);
      assert.notEqual(fresh.captureId, capture.captureId);
      yield* fresh.revalidateCaptured;
    }).pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 60_000,
          runtimeGeneration: () => generation,
          continuationSourceIdentity: () => source,
          beforeOpen: (input) =>
            Effect.sync(() => {
              opened = input;
              generation = input.nativeOperation!.runtimeGeneration!;
            }),
        }),
      ),
    );
  }),
);

it.effect(
  "stops an exact resident session with managed readback and preserves the registered native tuple",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      yield* Effect.gen(function* () {
        const fixture = yield* preparePendingStartStopFixture("pinned-stop-resident");
        const owner = (yield* fixture.sink.readCurrentProviderRuntimeOwner(fixture.threadId))!;
        const runtimeGeneration = owner.binding.runtimeGeneration;
        if (runtimeGeneration === null)
          return yield* Effect.die("The pinned fixture owner must have a generation.");
        const binding = { ...owner.binding, runtimeGeneration };
        const result = yield* fixture.manager.stopPinnedRuntime({
          operationId: "effect:pinned-stop-resident",
          binding,
          expectedEvidenceRevision: owner.evidenceRevision,
        });
        assert.deepEqual(result, {
          status: "stopped",
          operationId: "effect:pinned-stop-resident",
          binding,
          cancelledPendingStart: false,
          interruptedProviderTurnIds: [],
          readback: { threadAttached: false },
        });
        assert.equal((yield* Ref.get(state)).closeCount, 1);
        assert.isTrue(Option.isNone(yield* fixture.manager.get(fixture.sessionId)));
        assert.equal(
          (yield* fixture.manager.readCurrentThreadRuntimeAttachment(fixture.threadId)).status,
          "stopped",
        );
        assert.deepEqual(yield* fixture.sink.readProviderRuntimeEvidence(fixture.threadId), owner);
      }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000 })));
    }),
);

it.effect("stops one pinned pooled target by unloading it and preserves the sibling resident", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    yield* Effect.gen(function* () {
      const fixture = yield* preparePendingStartStopFixture("pinned-stop-pooled");
      const sibling = ThreadId.make("pinned-stop-pooled-sibling");
      const siblingProvider = yield* seedManagedReplacementOwner({
        ...fixture,
        threadId: sibling,
        nativeId: "pinned-stop-pooled-sibling-native",
      });
      yield* fixture.manager.open({
        threadId: sibling,
        providerSessionId: fixture.sessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* fixture.manager.registerRuntimeBinding({
        threadId: sibling,
        providerSessionId: fixture.sessionId,
        providerThreadId: siblingProvider.id,
      });
      const siblingBefore = yield* fixture.manager.readCurrentThreadRuntimeAttachment(sibling);
      const owner = (yield* fixture.sink.readCurrentProviderRuntimeOwner(fixture.threadId))!;
      const result = yield* fixture.manager.stopPinnedRuntime({
        operationId: "effect:pinned-stop-pooled",
        binding: {
          ...owner.binding,
          runtimeGeneration: owner.binding.runtimeGeneration!,
        },
        expectedEvidenceRevision: owner.evidenceRevision,
      });
      assert.equal(result.status, "stopped");
      assert.equal((yield* Ref.get(state)).closeCount, 0);
      assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, [
        fixture.providerThread.nativeThreadRef!.nativeId,
      ]);
      const siblingAfter = yield* fixture.manager.readCurrentThreadRuntimeAttachment(sibling);
      assert.equal(siblingAfter.status, "attached");
      if (siblingBefore.status === "attached" && siblingAfter.status === "attached")
        assert.deepEqual(siblingAfter.binding, siblingBefore.binding);
      assert.equal(
        (yield* fixture.manager.readCurrentThreadRuntimeAttachment(fixture.threadId)).status,
        "stopped",
      );
    }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000 })));
  }),
);

it.effect("waits for the pinned active turn terminal before completing managed stop", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const interruptEntered =
      yield* Deferred.make<Parameters<ProviderAdapterV2SessionRuntime["interruptTurn"]>[0]>();
    const interruptAck = yield* Deferred.make<void>();
    const completed = yield* Deferred.make<void>();
    yield* Effect.gen(function* () {
      const fixture = yield* preparePendingStartStopFixture("pinned-stop-active");
      const turnId = fixture.ids.derive.providerTurn({
        driver: CODEX_DRIVER,
        nativeTurnId: "pinned-stop-active-native-turn",
      });
      yield* fixture.sink.write({
        events: [
          {
            id: yield* fixture.ids.allocate.event({ threadId: fixture.threadId }),
            type: "provider-turn.updated",
            threadId: fixture.threadId,
            runId: fixture.turn.runId,
            driver: CODEX_DRIVER,
            occurredAt: fixture.now,
            payload: {
              id: turnId,
              providerThreadId: fixture.providerThread.id,
              nodeId: fixture.turn.rootNodeId,
              runAttemptId: fixture.turn.attemptId,
              nativeTurnRef: null,
              ordinal: 1,
              status: "running",
              startedAt: fixture.now,
              completedAt: null,
            },
          },
        ],
      });
      const owner = (yield* fixture.sink.readCurrentProviderRuntimeOwner(fixture.threadId))!;
      const subscription = yield* fixture.runtime.subscribeEvents!;
      const received = yield* Stream.runCollect(subscription.events).pipe(Effect.forkChild);
      const stopping = yield* fixture.manager
        .stopPinnedRuntime({
          operationId: "effect:pinned-stop-active",
          binding: { ...owner.binding, runtimeGeneration: owner.binding.runtimeGeneration! },
          expectedEvidenceRevision: owner.evidenceRevision,
        })
        .pipe(
          Effect.tap(() => Deferred.succeed(completed, undefined)),
          Effect.forkChild,
        );
      const dispatched = yield* Deferred.await(interruptEntered);
      assert.equal(dispatched.providerTurnId, turnId);
      assert.equal(dispatched.providerThread.id, fixture.providerThread.id);
      assert.equal(dispatched.requestRuntimeRestart, true);
      assert.equal((yield* Ref.get(state)).closeCount, 0);
      yield* Deferred.succeed(interruptAck, undefined);
      for (let i = 0; i < 20; i++) yield* Effect.yieldNow;
      assert.isFalse(yield* Deferred.isDone(completed));
      const terminal: ProviderAdapterV2Event = {
        type: "turn.terminal",
        driver: CODEX_DRIVER,
        providerThreadId: fixture.providerThread.id,
        providerTurnId: turnId,
        runOrdinal: 1,
        status: "interrupted",
        failure: null,
        threadDisposition: "reusable",
      };
      yield* Queue.offer(
        (yield* Ref.get(state)).eventQueues.get(String(fixture.sessionId))!,
        terminal,
      );
      const result = yield* Fiber.join(stopping);
      assert.equal(result.status, "stopped");
      if (result.status === "stopped")
        assert.deepEqual(result.interruptedProviderTurnIds, [turnId]);
      assert.deepEqual(
        (yield* Fiber.join(received)).filter((event) => event.type === "turn.terminal"),
        [terminal],
      );
      assert.equal((yield* Ref.get(state)).closeCount, 1);
    }).pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 60_000,
          beforeInterruptTurn: (input) =>
            Deferred.succeed(interruptEntered, input).pipe(
              Effect.andThen(Deferred.await(interruptAck)),
            ),
        }),
      ),
    );
  }),
);

it.effect(
  "stops a captured unbound single-owner session without inventing a native thread ID",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      yield* Effect.gen(function* () {
        const fixture = yield* preparePendingStartStopFixture("pinned-stop-unbound");
        const unbound = {
          ...fixture.providerThread,
          nativeThreadRef: null,
          nativeConversationHeadRef: null,
        };
        yield* fixture.sink.write({
          events: [
            {
              id: yield* fixture.ids.allocate.event({ threadId: fixture.threadId }),
              type: "provider-thread.updated",
              threadId: fixture.threadId,
              occurredAt: fixture.now,
              payload: unbound,
            },
          ],
        });
        yield* fixture.manager.registerRuntimeBinding({
          threadId: fixture.threadId,
          providerSessionId: fixture.sessionId,
          providerThreadId: unbound.id,
          runId: fixture.turn.runId,
          attemptId: fixture.turn.attemptId,
        });
        const owner = (yield* fixture.sink.readCurrentProviderRuntimeOwner(fixture.threadId))!;
        assert.isNull(owner.binding.nativeThreadId);
        const result = yield* fixture.manager.stopPinnedRuntime({
          operationId: "effect:pinned-stop-unbound",
          binding: {
            ...owner.binding,
            runtimeGeneration: owner.binding.runtimeGeneration!,
          },
          expectedEvidenceRevision: owner.evidenceRevision,
        });
        assert.equal(result.status, "stopped");
        if (result.status === "stopped") assert.isNull(result.binding.nativeThreadId);
        assert.equal((yield* Ref.get(state)).closeCount, 1);
        assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, []);
      }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000 })));
    }),
);

it.effect(
  "reserves pending-start stop through cleanup and excludes concurrent attachment and reuse",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const started = yield* Deferred.make<ProviderAdapterV2TurnInput>();
      const cleanupEntered = yield* Deferred.make<void>();
      const cleanupGate = yield* Deferred.make<void>();
      const reopened = yield* Deferred.make<void>();
      let opened: ProviderAdapterV2OpenSessionInput | undefined;
      yield* Effect.gen(function* () {
        const fixture = yield* preparePendingStartStopFixture("pending-stop-exclusive");
        const start = yield* fixture.runtime
          .startTurn(fixture.turn)
          .pipe(Effect.exit, Effect.forkChild);
        const recorded = yield* Deferred.await(started);
        const sibling = ThreadId.make("pending-stop-exclusive-sibling");
        const siblingProvider = yield* seedManagedReplacementOwner({
          ...fixture,
          threadId: sibling,
          nativeId: "pending-stop-exclusive-sibling-native",
        });
        const stopping = yield* (
          opened?.withPendingStartStop?.(
            fixture.stopInput(recorded),
            Effect.gen(function* () {
              yield* Deferred.succeed(cleanupEntered, undefined);
              yield* Deferred.await(cleanupGate);
              yield* Fiber.interrupt(start);
            }),
          ) ?? Effect.succeed({ status: "unknown" as const, reason: "missing_reservation" })
        ).pipe(Effect.forkChild);
        // A missing hook fails the observable result without waiting for a gate it never reaches.
        if (opened?.withPendingStartStop === undefined) {
          assert.equal((yield* Fiber.join(stopping)).status, "cancelled");
          return;
        }
        yield* Deferred.await(cleanupEntered);
        assert.isTrue(
          Exit.isFailure(
            yield* fixture.manager
              .detach({
                providerSessionId: fixture.sessionId,
                threadId: fixture.threadId,
                revokeMcpCredential: true,
              })
              .pipe(Effect.exit),
          ),
        );
        assert.equal(
          (yield* fixture.manager.readCurrentThreadRuntimeAttachment(fixture.threadId)).status,
          "attached",
        );
        assert.isTrue(
          Exit.isFailure(
            yield* fixture.runtime
              .resumeThread({ threadId: sibling, providerThread: siblingProvider })
              .pipe(Effect.exit),
          ),
        );
        assert.isTrue(
          Exit.isFailure(
            yield* fixture.runtime
              .resumeThread({ threadId: fixture.threadId, providerThread: fixture.providerThread })
              .pipe(Effect.exit),
          ),
        );
        assert.equal((yield* Ref.get(state)).resumeCount, 0);
        const open = yield* fixture.manager
          .open({
            threadId: sibling,
            providerSessionId: fixture.sessionId,
            modelSelection,
            runtimePolicy,
          })
          .pipe(
            Effect.tap(() => Deferred.succeed(reopened, undefined)),
            Effect.forkChild,
          );
        for (let i = 0; i < 20; i++) yield* Effect.yieldNow;
        assert.isFalse(yield* Deferred.isDone(reopened));
        yield* Deferred.succeed(cleanupGate, undefined);
        const result = yield* Fiber.join(stopping);
        assert.equal(result.status, "cancelled");
        if (result.status === "cancelled")
          assert.equal(result.startOperationId, recorded.nativeOperation!.operationId);
        assert.equal((yield* Ref.get(state)).closeCount, 1);
        yield* Fiber.join(open);
        assert.equal((yield* Ref.get(state)).openCount, 2);
        assert.equal(
          (yield* fixture.manager.readCurrentThreadRuntimeAttachment(fixture.threadId)).status,
          "stopped",
        );
      }).pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 60_000,
            beforeOpen: (input) =>
              Effect.sync(() => {
                opened = input;
              }),
            beforeStartTurn: (turn) =>
              Deferred.succeed(started, turn).pipe(Effect.andThen(Effect.never)),
          }),
        ),
      );
    }),
);

it.effect(
  "rejects pending-start incarnation stop for a pooled sibling or changed original operation",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const started = yield* Deferred.make<ProviderAdapterV2TurnInput>();
      let opened: ProviderAdapterV2OpenSessionInput | undefined;
      let effects = 0;
      yield* Effect.gen(function* () {
        const fixture = yield* preparePendingStartStopFixture("pending-stop-pooled");
        yield* fixture.runtime.startTurn(fixture.turn).pipe(Effect.exit, Effect.forkChild);
        const recorded = yield* Deferred.await(started);
        const input = fixture.stopInput(recorded);
        const changed = {
          ...input,
          startOperation: { ...input.startOperation, operationId: "unrelated-start" },
        };
        const stop = Effect.sync(() => {
          effects++;
        });
        assert.equal(
          (yield* (
            opened?.withPendingStartStop?.(changed, stop) ??
              Effect.succeed({ status: "unknown" as const })
          )).status,
          "unknown",
        );
        const sibling = ThreadId.make("pending-stop-pooled-sibling");
        yield* seedManagedReplacementOwner({
          ...fixture,
          threadId: sibling,
          nativeId: "pending-stop-pooled-sibling-native",
        });
        yield* fixture.manager.open({
          threadId: sibling,
          providerSessionId: fixture.sessionId,
          modelSelection,
          runtimePolicy,
        });
        assert.equal(
          (yield* (
            opened?.withPendingStartStop?.(input, stop) ??
              Effect.succeed({ status: "unknown" as const })
          )).status,
          "unknown",
        );
        assert.equal(effects, 0);
        assert.equal((yield* Ref.get(state)).closeCount, 0);
        assert.equal((yield* Ref.get(state)).openCount, 1);
        assert.equal(
          (yield* fixture.manager.readCurrentThreadRuntimeAttachment(fixture.threadId)).status,
          "attached",
        );
        assert.isTrue(Option.isSome(yield* fixture.manager.get(fixture.sessionId)));
      }).pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 60_000,
            beforeOpen: (input) =>
              Effect.sync(() => {
                opened = input;
              }),
            beforeStartTurn: (turn) =>
              Deferred.succeed(started, turn).pipe(Effect.andThen(Effect.never)),
          }),
        ),
      );
    }),
);

it.effect(
  "matches pending stop to the refreshed dispatch generation while retaining the original start operation",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const started = yield* Deferred.make<ProviderAdapterV2TurnInput>();
      let opened: ProviderAdapterV2OpenSessionInput | undefined;
      let generation = "";
      let registerDispatch: Effect.Effect<void, ProviderAdapterV2Error> = Effect.void;
      yield* Effect.gen(function* () {
        const fixture = yield* preparePendingStartStopFixture("pending-stop-refreshed");
        const originalGeneration = generation;
        registerDispatch = fixture.manager
          .registerRuntimeBinding({
            threadId: fixture.threadId,
            providerSessionId: fixture.sessionId,
            providerThreadId: fixture.providerThread.id,
            runId: fixture.turn.runId,
            attemptId: fixture.turn.attemptId,
          })
          .pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterProtocolError({
                  driver: CODEX_DRIVER,
                  detail: "Synthetic dispatch registration failed",
                  cause,
                }),
            ),
          );
        const start = yield* fixture.runtime
          .startTurn(fixture.turn)
          .pipe(Effect.exit, Effect.forkChild);
        const recorded = yield* Deferred.await(started);
        assert.equal(recorded.nativeOperation?.runtimeGeneration, originalGeneration);
        assert.notEqual(generation, originalGeneration);
        const input = fixture.stopInput(recorded);
        assert.equal(input.binding.runtimeGeneration, generation);
        const result = yield* (
          opened?.withPendingStartStop?.(input, Fiber.interrupt(start).pipe(Effect.asVoid)) ??
            Effect.succeed({ status: "unknown" as const, reason: "missing_reservation" })
        );
        assert.equal(result.status, "cancelled");
        if (result.status === "cancelled")
          assert.equal(result.startOperationId, recorded.nativeOperation!.operationId);
        assert.equal((yield* Ref.get(state)).closeCount, 1);
        assert.equal((yield* Ref.get(state)).openCount, 1);
      }).pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 60_000,
            runtimeGeneration: () => generation,
            beforeOpen: (input) =>
              Effect.sync(() => {
                opened = input;
                generation = input.nativeOperation!.runtimeGeneration!;
              }),
            beforeStartTurn: (turn) =>
              Effect.sync(() => {
                generation = "pending-stop-actual-dispatch";
              }).pipe(
                Effect.andThen(Effect.suspend(() => registerDispatch)),
                Effect.andThen(Deferred.succeed(started, turn)),
                Effect.andThen(Effect.never),
              ),
          }),
        ),
      );
    }),
);

it.effect("does not repeat failed pending-start cleanup after its reservation releases", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const started = yield* Deferred.make<ProviderAdapterV2TurnInput>();
    let opened: ProviderAdapterV2OpenSessionInput | undefined;
    let effects = 0;
    yield* Effect.gen(function* () {
      const fixture = yield* preparePendingStartStopFixture("pending-stop-failed");
      yield* fixture.runtime.startTurn(fixture.turn).pipe(Effect.exit, Effect.forkChild);
      const input = fixture.stopInput(yield* Deferred.await(started));
      const stop = Effect.sync(() => {
        effects++;
      }).pipe(Effect.andThen(unimplemented("Synthetic captured cleanup failure")));
      const first = yield* (
        opened?.withPendingStartStop?.(input, stop) ??
          Effect.succeed({ status: "unknown" as const })
      );
      assert.equal(first.status, "unknown");
      const repeated = yield* (
        opened?.withPendingStartStop?.(input, stop) ??
          Effect.succeed({ status: "unknown" as const })
      );
      assert.equal(repeated.status, "unknown");
      assert.equal(effects, 1);
      assert.equal((yield* Ref.get(state)).closeCount, 0);
      assert.equal(fixture.runtime.runtimeGeneration, input.binding.runtimeGeneration);
      assert.equal(
        (yield* fixture.manager.readCurrentThreadRuntimeAttachment(fixture.threadId)).status,
        "attached",
      );
    }).pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 60_000,
          beforeOpen: (input) =>
            Effect.sync(() => {
              opened = input;
            }),
          beforeStartTurn: (turn) =>
            Deferred.succeed(started, turn).pipe(Effect.andThen(Effect.never)),
        }),
      ),
    );
  }),
);

it.effect(
  "publishes the original logical terminal before closing pending continuation subscriptions",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const recorded = yield* Ref.make<ProviderAdapterV2TurnInput | undefined>(undefined);
      let opened: ProviderAdapterV2OpenSessionInput | undefined;
      yield* Effect.gen(function* () {
        const fixture = yield* preparePendingStartStopFixture("pending-stop-continuation");
        const subscription = yield* fixture.runtime.subscribeEvents!;
        const received = yield* Stream.runCollect(subscription.events).pipe(Effect.forkChild);
        yield* fixture.runtime.startTurn(fixture.turn);
        const input = fixture.stopInput((yield* Ref.get(recorded))!);
        const terminal: ProviderAdapterV2Event = {
          type: "turn.terminal",
          driver: CODEX_DRIVER,
          providerThreadId: fixture.providerThread.id,
          providerTurnId: fixture.ids.derive.providerTurn({
            driver: CODEX_DRIVER,
            nativeTurnId: "actual-local-continuation",
          }),
          runOrdinal: 1,
          status: "interrupted",
          failure: null,
          threadDisposition: "reusable",
        };
        const events = (yield* Ref.get(state)).eventQueues.get(String(fixture.sessionId))!;
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver: CODEX_DRIVER,
          threadId: fixture.threadId,
          providerTurn: {
            id: terminal.providerTurnId,
            providerThreadId: fixture.providerThread.id,
            nodeId: fixture.turn.rootNodeId,
            runAttemptId: fixture.turn.attemptId,
            nativeTurnRef: null,
            ordinal: 1,
            status: "running",
            startedAt: fixture.now,
            completedAt: null,
          },
        });
        const result = yield* (
          opened?.withPendingStartStop?.(
            input,
            Queue.offer(events, terminal).pipe(Effect.asVoid),
          ) ?? Effect.succeed({ status: "unknown" as const, reason: "missing_reservation" })
        );
        assert.equal(result.status, "cancelled");
        assert.deepEqual(
          (yield* Fiber.join(received)).filter((event) => event.type === "turn.terminal"),
          [terminal],
        );
        assert.equal((yield* Ref.get(state)).closeCount, 1);
      }).pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 60_000,
            beforeOpen: (input) =>
              Effect.sync(() => {
                opened = input;
              }),
            beforeStartTurn: (turn) => Ref.set(recorded, turn),
          }),
        ),
      );
    }),
);

it.effect(
  "keeps the pending-start reservation after caller timeout until actual cleanup finishes",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const started = yield* Deferred.make<ProviderAdapterV2TurnInput>();
      const cleanupEntered = yield* Deferred.make<void>();
      const cleanupGate = yield* Deferred.make<void>();
      const reopened = yield* Deferred.make<void>();
      let opened: ProviderAdapterV2OpenSessionInput | undefined;
      yield* Effect.gen(function* () {
        const fixture = yield* preparePendingStartStopFixture("pending-stop-timeout");
        const start = yield* fixture.runtime
          .startTurn(fixture.turn)
          .pipe(Effect.exit, Effect.forkChild);
        const input = fixture.stopInput(yield* Deferred.await(started));
        const hook = opened!.withPendingStartStop!;
        const stopping = yield* hook(
          input,
          Deferred.succeed(cleanupEntered, undefined).pipe(
            Effect.andThen(Deferred.await(cleanupGate)),
            Effect.andThen(Fiber.interrupt(start)),
            Effect.asVoid,
          ),
        ).pipe(Effect.forkChild);
        yield* Deferred.await(cleanupEntered);
        yield* TestClock.adjust("30 seconds");
        const result = yield* Fiber.join(stopping);
        assert.equal(result.status, "unknown");
        if (result.status === "unknown")
          assert.equal(result.reason, "pending_start_cleanup_timeout");
        assert.isTrue(
          Exit.isFailure(
            yield* fixture.runtime
              .resumeThread({ threadId: fixture.threadId, providerThread: fixture.providerThread })
              .pipe(Effect.exit),
          ),
        );
        const open = yield* fixture.manager
          .open({
            threadId: fixture.threadId,
            providerSessionId: fixture.sessionId,
            modelSelection,
            runtimePolicy,
          })
          .pipe(
            Effect.tap(() => Deferred.succeed(reopened, undefined)),
            Effect.forkChild,
          );
        for (let i = 0; i < 20; i++) yield* Effect.yieldNow;
        assert.isFalse(yield* Deferred.isDone(reopened));
        assert.equal((yield* Ref.get(state)).closeCount, 0);
        yield* Deferred.succeed(cleanupGate, undefined);
        yield* Fiber.join(open);
        assert.equal((yield* Ref.get(state)).closeCount, 1);
        assert.equal((yield* Ref.get(state)).openCount, 2);
      }).pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 60_000,
            beforeOpen: (input) =>
              Effect.sync(() => {
                opened = input;
              }),
            beforeStartTurn: (turn) =>
              Deferred.succeed(started, turn).pipe(Effect.andThen(Effect.never)),
          }),
        ),
      );
    }),
);

it.effect(
  "rejects managed replacement of a pooled runtime before any factory or generation change",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      let generation = "";
      let opened: ProviderAdapterV2OpenSessionInput | undefined;
      let factories = 0;
      yield* Effect.gen(function* () {
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const sink = yield* EventSink.EventSinkV2;
        const ids = yield* IdAllocator.IdAllocatorV2;
        const now = yield* DateTime.now;
        const first = ThreadId.make("managed-pooled-first");
        const second = ThreadId.make("managed-pooled-second");
        const sessionId = yield* ids.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId: first,
        });
        const firstProvider = yield* seedManagedReplacementOwner({
          sink,
          ids,
          threadId: first,
          sessionId,
          now,
          nativeId: "managed-pooled-first-native",
        });
        const secondProvider = yield* seedManagedReplacementOwner({
          sink,
          ids,
          threadId: second,
          sessionId,
          now,
          nativeId: "managed-pooled-second-native",
        });
        yield* manager.open({
          threadId: first,
          providerSessionId: sessionId,
          modelSelection,
          runtimePolicy,
        });
        yield* manager.registerRuntimeBinding({
          threadId: first,
          providerSessionId: sessionId,
          providerThreadId: firstProvider.id,
        });
        yield* manager.open({
          threadId: second,
          providerSessionId: sessionId,
          modelSelection,
          runtimePolicy,
        });
        yield* manager.registerRuntimeBinding({
          threadId: second,
          providerSessionId: sessionId,
          providerThreadId: secondProvider.id,
        });
        const before = yield* sink.readProviderRuntimeEvidence(first);
        const failed = yield* opened!.withRuntimeReplacement!(
          "managed-pooled-next",
          Effect.sync(() => {
            generation = "managed-pooled-next";
            factories++;
          }),
        ).pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(failed));
        assert.equal(factories, 0);
        assert.equal(generation, before!.binding.runtimeGeneration);
        assert.deepEqual(yield* sink.readProviderRuntimeEvidence(first), before);
        assert.equal(
          (yield* sink.readProviderRuntimeEvidence(second))?.binding.runtimeGeneration,
          generation,
        );
        assert.equal((yield* Ref.get(state)).openCount, 1);
      }).pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 60_000,
            runtimeGeneration: () => generation,
            beforeOpen: (input) =>
              Effect.sync(() => {
                opened = input;
                generation = input.nativeOperation!.runtimeGeneration!;
              }),
          }),
        ),
      );
    }),
);

it.effect(
  "reserves single-owner managed replacement through registration and blocks concurrent attachments",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const entered = yield* Deferred.make<void>();
      const gate = yield* Deferred.make<void>();
      const attached = yield* Deferred.make<void>();
      let generation = "";
      let source: ProviderContinuationSourceIdentity | undefined;
      let opened: ProviderAdapterV2OpenSessionInput | undefined;
      yield* Effect.gen(function* () {
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const sink = yield* EventSink.EventSinkV2;
        const ids = yield* IdAllocator.IdAllocatorV2;
        const now = yield* DateTime.now;
        const owner = ThreadId.make("managed-single-owner");
        const sibling = ThreadId.make("managed-single-sibling");
        const sessionId = yield* ids.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId: owner,
        });
        const provider = yield* seedManagedReplacementOwner({
          sink,
          ids,
          threadId: owner,
          sessionId,
          now,
          nativeId: "managed-single-native",
        });
        const siblingProvider = yield* seedManagedReplacementOwner({
          sink,
          ids,
          threadId: sibling,
          sessionId,
          now,
          nativeId: "managed-single-sibling-native",
        });
        const runtime = yield* manager.open({
          threadId: owner,
          providerSessionId: sessionId,
          modelSelection,
          runtimePolicy,
        });
        source = {
          driverKind: CODEX_DRIVER,
          continuationKey: "codex:managed-single-store",
          runtimeGeneration: generation,
        };
        yield* manager.registerRuntimeBinding({
          threadId: owner,
          providerSessionId: sessionId,
          providerThreadId: provider.id,
        });
        const previous = yield* sink.readProviderRuntimeEvidence(owner);
        const replacement = yield* opened!.withRuntimeReplacement!(
          "managed-single-next",
          Effect.gen(function* () {
            generation = "managed-single-next";
            source = undefined;
            yield* opened!.beforeRuntimeReplacement!(generation);
            const reserved = yield* sink.readProviderRuntimeEvidence(owner).pipe(Effect.orDie);
            assert.equal(reserved?.binding.runtimeGeneration, generation);
            assert.isNull(
              yield* sink
                .readProviderContinuationSourceIdentity(reserved!.binding)
                .pipe(Effect.orDie),
            );
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(gate);
            source = {
              driverKind: CODEX_DRIVER,
              continuationKey: "codex:managed-single-store",
              runtimeGeneration: generation,
            };
          }),
        ).pipe(Effect.forkChild);
        yield* Deferred.await(entered);
        const premature = yield* runtime
          .resumeThread({ threadId: sibling, providerThread: siblingProvider })
          .pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(premature));
        assert.equal((yield* Ref.get(state)).resumeCount, 0);
        const attach = yield* manager
          .open({ threadId: sibling, providerSessionId: sessionId, modelSelection, runtimePolicy })
          .pipe(
            Effect.tap(() => Deferred.succeed(attached, undefined)),
            Effect.forkChild,
          );
        for (let i = 0; i < 20; i++) yield* Effect.yieldNow;
        assert.isFalse(yield* Deferred.isDone(attached));
        yield* Deferred.succeed(gate, undefined);
        yield* Fiber.join(replacement);
        const committed = yield* sink.readProviderRuntimeEvidence(owner);
        assert.equal(committed?.binding.runtimeGeneration, generation);
        assert.isAbove(committed!.evidenceRevision, previous!.evidenceRevision);
        assert.deepEqual(
          yield* sink.readProviderContinuationSourceIdentity(committed!.binding),
          source,
        );
        yield* Fiber.join(attach);
        assert.isTrue(yield* Deferred.isDone(attached));
        assert.equal((yield* Ref.get(state)).openCount, 1);
      }).pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 60_000,
            runtimeGeneration: () => generation,
            continuationSourceIdentity: () => source,
            beforeOpen: (input) =>
              Effect.sync(() => {
                opened = input;
                generation = input.nativeOperation!.runtimeGeneration!;
              }),
          }),
        ),
      );
    }),
);

it.effect(
  "releases the managed attachment reservation after failed creation while the new generation stays unknown",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      let generation = "";
      let opened: ProviderAdapterV2OpenSessionInput | undefined;
      yield* Effect.gen(function* () {
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const sink = yield* EventSink.EventSinkV2;
        const ids = yield* IdAllocator.IdAllocatorV2;
        const now = yield* DateTime.now;
        const owner = ThreadId.make("managed-failed-owner");
        const sibling = ThreadId.make("managed-failed-sibling");
        const sessionId = yield* ids.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId: owner,
        });
        const provider = yield* seedManagedReplacementOwner({
          sink,
          ids,
          threadId: owner,
          sessionId,
          now,
          nativeId: "managed-failed-native",
        });
        yield* seedManagedReplacementOwner({
          sink,
          ids,
          threadId: sibling,
          sessionId,
          now,
          nativeId: "managed-failed-sibling-native",
        });
        yield* manager.open({
          threadId: owner,
          providerSessionId: sessionId,
          modelSelection,
          runtimePolicy,
        });
        yield* manager.registerRuntimeBinding({
          threadId: owner,
          providerSessionId: sessionId,
          providerThreadId: provider.id,
        });
        const failed = yield* opened!.withRuntimeReplacement!(
          "managed-failed-next",
          Effect.gen(function* () {
            generation = "managed-failed-next";
            yield* opened!.beforeRuntimeReplacement!(generation);
            return yield* unimplemented("Synthetic managed factory failure after reservation.");
          }),
        ).pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(failed));
        if (Exit.isFailure(failed))
          assert.equal(nativeEffectEvidenceFromCause(failed.cause)?.outcome, "unknown");
        const evidence = yield* sink.readProviderRuntimeEvidence(owner);
        assert.equal(evidence?.binding.runtimeGeneration, "managed-failed-next");
        assert.isNull(evidence?.observation);
        assert.equal((yield* manager.observeCurrentThreadRuntime(owner)).status, "unknown");
        assert.isNull(yield* sink.readProviderContinuationSourceIdentity(evidence!.binding));
        yield* manager.open({
          threadId: sibling,
          providerSessionId: sessionId,
          modelSelection,
          runtimePolicy,
        });
        assert.equal(generation, "managed-failed-next");
        assert.equal((yield* Ref.get(state)).openCount, 1);
      }).pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 60_000,
            runtimeGeneration: () => generation,
            beforeOpen: (input) =>
              Effect.sync(() => {
                opened = input;
                generation = input.nativeOperation!.runtimeGeneration!;
              }),
          }),
        ),
      );
    }),
);

it.effect("rejects native idle evidence when the actual process changes during observation", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    let generation = "resident-generation";
    const effect = Effect.gen(function* () {
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const ids = yield* IdAllocator.IdAllocatorV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("native-observation-replacement");
      const sessionId = yield* ids.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const providerThread = makeProviderThread({
        idAllocator: ids,
        threadId,
        providerSessionId: sessionId,
        now,
      });
      const created = yield* makeThreadCreatedEvent({ idAllocator: ids, threadId, now });
      yield* eventSink.write({
        events: [
          {
            ...created,
            payload: { ...created.payload, activeProviderThreadId: providerThread.id },
          },
          {
            id: yield* ids.allocate.event({ threadId }),
            type: "provider-thread.updated",
            threadId,
            occurredAt: now,
            payload: providerThread,
          },
        ],
      });
      const runtime = yield* manager.open({
        threadId,
        providerSessionId: sessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* manager.registerRuntimeBinding({
        threadId,
        providerSessionId: sessionId,
        providerThreadId: providerThread.id,
      });
      const observation = yield* manager.observeThreadRuntime({
        threadId,
        providerThreadId: providerThread.id,
        providerSessionId: sessionId,
        instanceId: modelSelection.instanceId,
        runtimeGeneration: generation,
        nativeThreadId: "native-thread",
      });
      assert.equal(observation.status, "unknown");
      if (observation.status === "unknown")
        assert.equal(observation.reason, "runtime_binding_changed");
      assert.equal(runtime.runtimeGeneration, "replacement-generation");
      assert.equal((yield* Ref.get(state)).openCount, 1);
    });
    yield* effect.pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 60_000,
          runtimeGeneration: () => generation,
          observeThreadRuntime: (binding) =>
            Effect.gen(function* () {
              const observedAt = DateTime.formatIso(yield* DateTime.now);
              generation = "replacement-generation";
              return { status: "idle", binding, observedAt };
            }),
        }),
      ),
    );
  }),
);

it.effect("fences retained old evidence before a nonresident replacement factory opens", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    let generation = "";
    let replacement = false;
    let factoryFenceChecked = false;
    let previous: EventSink.ProviderRuntimeEvidenceV2 | null = null;
    let capturedSource: ProviderContinuationSourceIdentity | undefined;
    let beforeOpen: (
      input: ProviderAdapterV2OpenSessionInput,
    ) => Effect.Effect<void, ProviderAdapterV2Error> = () => Effect.void;
    yield* Effect.gen(function* () {
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const ids = yield* IdAllocator.IdAllocatorV2;
      const threadId = ThreadId.make("retained-old-runtime-fence");
      const now = yield* DateTime.now;
      const oldSessionId = yield* ids.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const providerThread = makeProviderThread({
        idAllocator: ids,
        threadId,
        providerSessionId: oldSessionId,
        now,
      });
      const created = yield* makeThreadCreatedEvent({ idAllocator: ids, threadId, now });
      yield* eventSink.write({
        events: [
          {
            ...created,
            payload: { ...created.payload, activeProviderThreadId: providerThread.id },
          },
          {
            id: yield* ids.allocate.event({ threadId }),
            type: "provider-thread.updated",
            threadId,
            occurredAt: now,
            payload: providerThread,
          },
        ],
      });
      beforeOpen = (input) =>
        Effect.gen(function* () {
          generation = input.nativeOperation!.runtimeGeneration!;
          if (!replacement) {
            capturedSource = {
              driverKind: CODEX_DRIVER,
              continuationKey: "codex:retained-actual-home",
              runtimeGeneration: generation,
            };
            return;
          }
          assert.isTrue(
            Option.isNone(yield* manager.get(input.providerSessionId).pipe(Effect.orDie)),
          );
          yield* input.beforeRuntimeReplacement!(generation);
          const reserved = yield* eventSink
            .readProviderRuntimeEvidence(threadId)
            .pipe(Effect.orDie);
          assert.equal(reserved?.binding.runtimeGeneration, generation);
          assert.equal(reserved?.binding.providerSessionId, input.providerSessionId);
          assert.isAbove(reserved!.evidenceRevision, previous!.evidenceRevision);
          assert.isNull(
            yield* eventSink
              .readProviderContinuationSourceIdentity(reserved!.binding)
              .pipe(Effect.orDie),
          );
          assert.deepEqual(
            yield* eventSink
              .readProviderContinuationSourceIdentity(previous!.binding)
              .pipe(Effect.orDie),
            capturedSource,
          );
          assert.equal(
            (yield* manager.observeCurrentThreadRuntime(threadId).pipe(Effect.orDie)).status,
            "unknown",
          );
          factoryFenceChecked = true;
        });
      yield* manager.open({
        threadId,
        providerSessionId: oldSessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* manager.registerRuntimeBinding({
        threadId,
        providerSessionId: oldSessionId,
        providerThreadId: providerThread.id,
      });
      previous = yield* eventSink.readProviderRuntimeEvidence(threadId);
      assert.isNotNull(previous);
      yield* manager.close(oldSessionId);
      const nextSessionId = yield* ids.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      yield* eventSink.write({
        events: [
          {
            id: yield* ids.allocate.event({ threadId }),
            type: "provider-session.attached",
            threadId,
            occurredAt: now,
            payload: makeProviderSession({ providerSessionId: nextSessionId, now }),
          },
          {
            id: yield* ids.allocate.event({ threadId }),
            type: "provider-thread.updated",
            threadId,
            occurredAt: now,
            payload: { ...providerThread, providerSessionId: nextSessionId },
          },
        ],
      });
      replacement = true;
      yield* manager.open({
        threadId,
        providerSessionId: nextSessionId,
        modelSelection,
        runtimePolicy,
      });
      assert.isTrue(factoryFenceChecked);
      assert.equal((yield* Ref.get(state)).openCount, 2);
    }).pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 60_000,
          beforeOpen: (input) => beforeOpen(input),
          runtimeGeneration: () => generation,
          continuationSourceIdentity: () => capturedSource,
        }),
      ),
    );
  }),
);

it.effect("revalidates native resume proof before reusing an already loaded thread", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    let gateCalls = 0;
    yield* Effect.gen(function* () {
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const ids = yield* IdAllocator.IdAllocatorV2;
      const threadId = ThreadId.make("cached-native-resume-proof");
      const now = yield* DateTime.now;
      const sessionId = yield* ids.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const providerThread = makeProviderThread({
        idAllocator: ids,
        threadId,
        providerSessionId: sessionId,
        now,
      });
      const created = yield* makeThreadCreatedEvent({ idAllocator: ids, threadId, now });
      yield* eventSink.write({
        events: [
          {
            ...created,
            payload: { ...created.payload, activeProviderThreadId: providerThread.id },
          },
          {
            id: yield* ids.allocate.event({ threadId }),
            type: "provider-thread.updated",
            threadId,
            occurredAt: now,
            payload: providerThread,
          },
        ],
      });
      const runtime = yield* manager.open({
        threadId,
        providerSessionId: sessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* runtime.resumeThread({ threadId, providerThread });
      const failed = yield* runtime
        .resumeThread({
          threadId,
          providerThread,
          beforeNativeResume: (actual) => {
            gateCalls++;
            assert.isUndefined(actual);
            return unimplemented("The current native source identity is unproved.");
          },
        })
        .pipe(Effect.flip);
      assert.equal(gateCalls, 1);
      assert.equal(nativeEffectEvidenceFromCause(failed)?.outcome, "unknown");
      assert.equal((yield* Ref.get(state)).resumeCount, 1);
    }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000 })));
  }),
);

it.effect(
  "keeps generation and clears stale observations for a resident model and tier change",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const generation = "live-model-and-tier-generation";
      const firstSelection: ModelSelection = {
        ...modelSelection,
        model: "gpt-5-codex",
        options: [{ id: "serviceTier", value: "priority" }],
      };
      const secondSelection: ModelSelection = {
        ...modelSelection,
        model: "gpt-5.6-sol",
        options: [{ id: "serviceTier", value: "flex" }],
      };
      yield* Effect.gen(function* () {
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const eventSink = yield* EventSink.EventSinkV2;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const ids = yield* IdAllocator.IdAllocatorV2;
        const threadId = ThreadId.make("live-model-and-tier-change");
        const now = yield* DateTime.now;
        const sessionId = yield* ids.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const providerThread = makeProviderThread({
          idAllocator: ids,
          threadId,
          providerSessionId: sessionId,
          now,
        });
        const created = yield* makeThreadCreatedEvent({ idAllocator: ids, threadId, now });
        yield* eventSink.write({
          events: [
            {
              ...created,
              payload: {
                ...created.payload,
                modelSelection: firstSelection,
                activeProviderThreadId: providerThread.id,
              },
            },
            {
              id: yield* ids.allocate.event({ threadId }),
              type: "provider-thread.updated",
              threadId,
              occurredAt: now,
              payload: providerThread,
            },
          ],
        });
        const runtime = yield* manager.open({
          threadId,
          providerSessionId: sessionId,
          modelSelection: firstSelection,
          runtimePolicy,
        });
        yield* runtime.resumeThread({
          threadId,
          providerThread,
          modelSelection: firstSelection,
          runtimePolicy,
        });
        yield* manager.registerRuntimeBinding({
          threadId,
          providerSessionId: sessionId,
          providerThreadId: providerThread.id,
        });
        const before = yield* projections.getThreadProviderContext(
          threadId,
          modelSelection.instanceId,
        );
        const session = before.providerSessions.find((row) => row.id === sessionId)!;
        yield* eventSink.write({
          events: [
            {
              id: yield* ids.allocate.event({ threadId }),
              type: "provider-session.updated",
              threadId,
              occurredAt: now,
              payload: {
                ...session,
                runtimeIdentity: {
                  runtimeGeneration: generation,
                  requested: {
                    providerInstanceId: modelSelection.instanceId,
                    providerDriver: CODEX_DRIVER,
                    model: firstSelection.model,
                    serviceTier: "priority",
                  },
                  observed: {
                    backend: { status: "unknown" },
                    account: { status: "unknown" },
                    model: {
                      status: "observed",
                      value: "old-native-model",
                      sourceEvent: "codex.thread/open",
                    },
                    serviceTier: {
                      status: "observed",
                      value: "priority",
                      sourceEvent: "codex.thread/open",
                    },
                  },
                },
              },
            },
          ],
        });
        yield* runtime.resumeThread({
          threadId,
          providerThread,
          modelSelection: secondSelection,
          runtimePolicy,
        });
        const after = yield* projections.getThreadProviderContext(
          threadId,
          modelSelection.instanceId,
        );
        const identity = after.providerSessions.find(
          (row) => row.id === sessionId,
        )?.runtimeIdentity;
        assert.equal((yield* Ref.get(state)).openCount, 1);
        assert.equal((yield* Ref.get(state)).resumeCount, 2);
        assert.equal(runtime.runtimeGeneration, generation);
        assert.equal(identity?.runtimeGeneration, generation);
        assert.equal(identity?.requested.model, secondSelection.model);
        assert.equal(identity?.requested.serviceTier, "flex");
        assert.deepEqual(identity?.observed.model, { status: "unknown" });
        assert.deepEqual(identity?.observed.serviceTier, { status: "unknown" });
      }).pipe(
        Effect.provide(
          makeTestLayer({ state, idleTimeoutMs: 60_000, runtimeGeneration: () => generation }),
        ),
      );
    }),
);

it.effect("persists native-bound source identity only for its actual matching generation", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    let generation = "captured-native-generation";
    const sourceIdentity = {
      driverKind: CODEX_DRIVER,
      continuationKey: "codex:actual-native-home",
      runtimeGeneration: generation,
    };
    yield* Effect.gen(function* () {
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const ids = yield* IdAllocator.IdAllocatorV2;
      const threadId = ThreadId.make("captured-continuation-source");
      const now = yield* DateTime.now;
      const sessionId = yield* ids.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const providerThread = makeProviderThread({
        idAllocator: ids,
        threadId,
        providerSessionId: sessionId,
        now,
      });
      const created = yield* makeThreadCreatedEvent({ idAllocator: ids, threadId, now });
      yield* eventSink.write({
        events: [
          {
            ...created,
            payload: { ...created.payload, activeProviderThreadId: providerThread.id },
          },
          {
            id: yield* ids.allocate.event({ threadId }),
            type: "provider-thread.updated",
            threadId,
            occurredAt: now,
            payload: providerThread,
          },
        ],
      });
      yield* manager.open({
        threadId,
        providerSessionId: sessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* manager.registerRuntimeBinding({
        threadId,
        providerSessionId: sessionId,
        providerThreadId: providerThread.id,
      });
      const historical = yield* eventSink.readProviderRuntimeEvidence(threadId);
      assert.isNotNull(historical);
      assert.deepEqual(
        yield* eventSink.readProviderContinuationSourceIdentity(historical!.binding),
        sourceIdentity,
      );
      generation = "reserved-next-generation";
      yield* manager.registerRuntimeBinding({
        threadId,
        providerSessionId: sessionId,
        providerThreadId: providerThread.id,
      });
      const next = yield* eventSink.readProviderRuntimeEvidence(threadId);
      assert.isNotNull(next);
      assert.isNull(yield* eventSink.readProviderContinuationSourceIdentity(next!.binding));
      assert.deepEqual(
        yield* eventSink.readProviderContinuationSourceIdentity(historical!.binding),
        sourceIdentity,
      );
    }).pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 60_000,
          runtimeGeneration: () => generation,
          continuationSourceIdentity: () => sourceIdentity,
        }),
      ),
    );
  }),
);

it.effect("retains the current resident attachment after the selected target account changes", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    yield* Effect.gen(function* () {
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const ids = yield* IdAllocator.IdAllocatorV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("resident-owner-selected-next-account");
      const sessionId = yield* ids.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const providerThread = makeProviderThread({
        idAllocator: ids,
        threadId,
        providerSessionId: sessionId,
        now,
      });
      const created = yield* makeThreadCreatedEvent({ idAllocator: ids, threadId, now });
      const currentThread = { ...created.payload, activeProviderThreadId: providerThread.id };
      yield* eventSink.write({
        events: [
          { ...created, payload: currentThread },
          {
            id: yield* ids.allocate.event({ threadId }),
            type: "provider-thread.updated",
            threadId,
            occurredAt: now,
            payload: providerThread,
          },
        ],
      });
      yield* manager.open({
        threadId,
        providerSessionId: sessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* manager.registerRuntimeBinding({
        threadId,
        providerSessionId: sessionId,
        providerThreadId: providerThread.id,
      });
      const before = yield* manager.readCurrentThreadRuntimeAttachment(threadId);
      assert.equal(before.status, "attached");
      const nextInstanceId = ProviderInstanceId.make("selected-next-account");
      yield* eventSink.write({
        events: [
          {
            id: yield* ids.allocate.event({ threadId }),
            type: "thread.model-selection-updated",
            threadId,
            occurredAt: now,
            payload: {
              ...currentThread,
              providerInstanceId: nextInstanceId,
              modelSelection: { ...modelSelection, instanceId: nextInstanceId },
            },
          },
        ],
      });
      const after = yield* manager.readCurrentThreadRuntimeAttachment(threadId);
      assert.equal(after.status, "attached");
      if (before.status === "attached" && after.status === "attached")
        assert.deepEqual(after.binding, before.binding);
      assert.equal((yield* Ref.get(state)).openCount, 1);
    }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000 })));
  }),
);

it.effect("counts current monitoring without hydrating unrelated model metadata", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const projectId = ProjectId.make("counts-narrow-project");
    yield* Effect.gen(function* () {
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const sink = yield* EventSink.EventSinkV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const sql = yield* SqlClient.SqlClient;
      const ids = yield* IdAllocator.IdAllocatorV2;
      const now = yield* DateTime.now;
      const currentThreadId = ThreadId.make("counts-current-monitor");
      const unrelatedThreadId = ThreadId.make("counts-unrelated-model");
      const sessionId = yield* ids.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: currentThreadId,
      });
      const providerThread = makeProviderThread({
        idAllocator: ids,
        threadId: currentThreadId,
        providerSessionId: sessionId,
        now,
      });
      const current = yield* makeThreadCreatedEvent({
        idAllocator: ids,
        threadId: currentThreadId,
        projectId,
        now,
      });
      const unrelated = yield* makeThreadCreatedEvent({
        idAllocator: ids,
        threadId: unrelatedThreadId,
        projectId,
        now,
      });
      yield* sink.write({
        events: [
          current,
          unrelated,
          {
            id: yield* ids.allocate.event({ threadId: currentThreadId }),
            type: "provider-thread.updated",
            threadId: currentThreadId,
            occurredAt: now,
            payload: providerThread,
          },
        ],
      });
      yield* manager.open({
        threadId: currentThreadId,
        providerSessionId: sessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* manager.registerRuntimeBinding({
        threadId: currentThreadId,
        providerSessionId: sessionId,
        providerThreadId: providerThread.id,
      });
      yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json =
      json_set(payload_json, '$.modelSelection', json('{"instanceId":7,"model":{}}')) WHERE thread_id = ${unrelatedThreadId}`;
      const shell = yield* projections.getShellSnapshot().pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(shell));
      const counts = yield* manager.getOperatingCounts({ projectId });
      assert.equal(counts.total, 2);
      assert.equal(counts.operating, 1);
      assert.equal(counts.backgroundOperating, 1);
      assert.equal(counts.backgroundUnknown, 0);
      assert.equal((yield* Ref.get(state)).openCount, 1);
      assert.equal(
        (yield* manager.getOperatingCounts({
          projectId: ProjectId.make("counts-unrelated-project"),
        })).total,
        0,
      );
    }).pipe(
      Effect.provide(
        Layer.merge(
          makeTestLayer({
            state,
            idleTimeoutMs: 60_000,
            runtimeGeneration: () => "counts-actual-generation",
            observeThreadRuntime: (binding) =>
              Effect.map(DateTime.now, (now) => ({
                status: "monitoring" as const,
                binding,
                observedAt: DateTime.formatIso(now),
              })),
          }),
          TestDatabaseLayer,
        ),
      ),
    );
  }),
);

it.effect("holds a guarded pooled detach when current native target records cannot be read", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    let failTargetRead = false;
    yield* Effect.gen(function* () {
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const ids = yield* IdAllocator.IdAllocatorV2;
      const threadId = ThreadId.make("guarded-stop-unreadable-target");
      const sibling = ThreadId.make("guarded-stop-sibling");
      const now = yield* DateTime.now;
      const sessionId = yield* ids.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const providerThread = makeProviderThread({
        idAllocator: ids,
        threadId,
        providerSessionId: sessionId,
        now,
      });
      const created = yield* makeThreadCreatedEvent({ idAllocator: ids, threadId, now });
      yield* eventSink.write({
        events: [
          {
            ...created,
            payload: { ...created.payload, activeProviderThreadId: providerThread.id },
          },
          yield* makeThreadCreatedEvent({ idAllocator: ids, threadId: sibling, now }),
          {
            id: yield* ids.allocate.event({ threadId }),
            type: "provider-thread.updated",
            threadId,
            occurredAt: now,
            payload: providerThread,
          },
        ],
      });
      yield* manager.open({
        threadId,
        providerSessionId: sessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* manager.open({
        threadId: sibling,
        providerSessionId: sessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* manager.registerRuntimeBinding({
        threadId,
        providerSessionId: sessionId,
        providerThreadId: providerThread.id,
      });
      const attachment = yield* manager.readCurrentThreadRuntimeAttachment(threadId);
      assert.equal(attachment.status, "attached");
      if (attachment.status !== "attached") return;
      failTargetRead = true;
      const result = yield* manager
        .detach({ threadId, providerSessionId: sessionId, expectedBinding: attachment.binding })
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(result));
      assert.equal(
        (yield* manager.readCurrentThreadRuntimeAttachment(threadId)).status,
        "attached",
      );
      assert.equal((yield* Ref.get(state)).closeCount, 0);
      assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, []);
    }).pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 60_000,
          failProviderThreadRecordsRead: () => failTargetRead,
        }),
      ),
    );
  }),
);

const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.4",
} satisfies ModelSelection;
const CODEX_DRIVER = ProviderDriverKind.make("codex");

const runtimePolicy = {
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: process.cwd(),
} satisfies ProviderAdapterV2RuntimePolicy;

function makeProviderSession(input: {
  readonly providerSessionId: ProviderSessionId;
  readonly now: DateTime.Utc;
  readonly capabilities?: OrchestrationV2ProviderCapabilities;
}): OrchestrationV2ProviderSession {
  return {
    id: input.providerSessionId,
    driver: CODEX_DRIVER,
    providerInstanceId: modelSelection.instanceId,
    status: "ready",
    cwd: process.cwd(),
    model: "gpt-5.4",
    capabilities: input.capabilities ?? CodexCapabilities,
    createdAt: input.now,
    updatedAt: input.now,
    lastError: null,
  };
}

function makeThreadCreatedEvent(input: {
  readonly idAllocator: IdAllocator.IdAllocatorV2Shape;
  readonly threadId: ThreadId;
  readonly now: DateTime.Utc;
  readonly projectId?: ProjectId;
}) {
  return Effect.gen(function* () {
    const projectId =
      input.projectId ??
      (yield* input.idAllocator.allocate.project({
        fixtureName: "provider-session-manager",
      }));
    const providerThreadId = input.idAllocator.derive.providerThread({
      driver: CODEX_DRIVER,
      nativeThreadId: "native-thread",
    });
    const thread: OrchestrationV2AppThread = {
      createdBy: "user",
      creationSource: "web",
      id: input.threadId,
      projectId,
      title: "Provider session manager",
      providerInstanceId: modelSelection.instanceId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: providerThreadId,
      lineage: {
        parentThreadId: null,
        relationshipToParent: null,
        rootThreadId: input.threadId,
      },
      forkedFrom: null,
      createdAt: input.now,
      updatedAt: input.now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    };
    return {
      id: yield* input.idAllocator.allocate.event({ threadId: input.threadId }),
      type: "thread.created" as const,
      threadId: input.threadId,
      occurredAt: input.now,
      payload: thread,
    };
  });
}

function makeProviderThread(input: {
  readonly idAllocator: IdAllocator.IdAllocatorV2Shape;
  readonly threadId: ThreadId;
  readonly providerSessionId: ProviderSessionId;
  readonly now: DateTime.Utc;
}): OrchestrationV2ProviderThread {
  return {
    id: input.idAllocator.derive.providerThread({
      driver: CODEX_DRIVER,
      nativeThreadId: "native-thread",
    }),
    driver: CODEX_DRIVER,
    providerInstanceId: modelSelection.instanceId,
    providerSessionId: input.providerSessionId,
    appThreadId: input.threadId,
    ownerNodeId: null,
    nativeThreadRef: {
      driver: CODEX_DRIVER,
      nativeId: "native-thread",
      strength: "strong",
    },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: null,
    lastRunOrdinal: null,
    handoffIds: [],
    forkedFrom: null,
    createdAt: input.now,
    updatedAt: input.now,
  };
}

function persistStartedRuntimeFixture(input: {
  readonly eventSink: EventSink.EventSinkV2["Service"];
  readonly idAllocator: IdAllocator.IdAllocatorV2Shape;
  readonly threadId: ThreadId;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly runId: RunId;
  readonly attemptId: RunAttemptId;
  readonly rootNodeId: NodeId;
  readonly now: DateTime.Utc;
}) {
  return Effect.gen(function* () {
    const messageId = yield* input.idAllocator.allocate.message({
      threadId: input.threadId,
      ordinal: 1,
    });
    yield* input.eventSink.write({
      events: [
        {
          id: yield* input.idAllocator.allocate.event({ threadId: input.threadId }),
          type: "provider-thread.updated",
          threadId: input.threadId,
          occurredAt: input.now,
          payload: input.providerThread,
        },
        {
          id: yield* input.idAllocator.allocate.event({ threadId: input.threadId }),
          type: "run.updated",
          threadId: input.threadId,
          runId: input.runId,
          occurredAt: input.now,
          payload: {
            id: input.runId,
            threadId: input.threadId,
            ordinal: 1,
            providerInstanceId: modelSelection.instanceId,
            modelSelection,
            providerThreadId: input.providerThread.id,
            userMessageId: messageId,
            rootNodeId: input.rootNodeId,
            activeAttemptId: input.attemptId,
            status: "running",
            requestedAt: input.now,
            startedAt: input.now,
            completedAt: null,
            checkpointId: null,
            contextHandoffId: null,
          },
        },
        {
          id: yield* input.idAllocator.allocate.event({ threadId: input.threadId }),
          type: "run-attempt.updated",
          threadId: input.threadId,
          runId: input.runId,
          occurredAt: input.now,
          payload: {
            id: input.attemptId,
            runId: input.runId,
            attemptOrdinal: 1,
            rootNodeId: input.rootNodeId,
            providerInstanceId: modelSelection.instanceId,
            providerThreadId: input.providerThread.id,
            providerTurnId: null,
            reason: "initial",
            status: "running",
            startedAt: input.now,
            completedAt: null,
          },
        },
      ],
    });
    return messageId;
  });
}

function unimplemented(detail: string) {
  return Effect.fail(
    new ProviderAdapterProtocolError({
      driver: CODEX_DRIVER,
      detail,
    }),
  );
}

function makeProviderAdapter(
  state: Ref.Ref<TestProviderRuntimeState>,
  options: {
    readonly failEventStream?: boolean;
    readonly capabilities?: OrchestrationV2ProviderCapabilities;
    readonly mcpConfigs?: Ref.Ref<
      ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
    >;
    readonly onRuntimeCreated?: (runtime: ProviderAdapterV2SessionRuntime) => void;
    readonly beforeResume?: Effect.Effect<void>;
    readonly beforeOpen?: (
      input: ProviderAdapterV2OpenSessionInput,
    ) => Effect.Effect<void, ProviderAdapterV2Error>;
    readonly beforeStartTurn?: (
      input: ProviderAdapterV2TurnInput,
    ) => Effect.Effect<void, ProviderAdapterV2Error>;
    readonly beforeInterruptTurn?: (
      input: Parameters<ProviderAdapterV2SessionRuntime["interruptTurn"]>[0],
    ) => Effect.Effect<void, ProviderAdapterV2Error>;
    readonly afterScopeClose?: Effect.Effect<void>;
    readonly hasPendingBackgroundWork?: Effect.Effect<boolean>;
    readonly hangSessionScopeClose?: boolean;
    readonly beforeUnload?: Effect.Effect<void>;
    readonly runtimeGeneration?: () => string | undefined;
    readonly continuationSourceIdentity?: () => ProviderContinuationSourceIdentity | undefined;
    readonly observeThreadRuntime?: (
      binding: ProviderRuntimeBinding,
    ) => Effect.Effect<ProviderRuntimeObservation>;
  } = {},
): ProviderAdapterV2Shape {
  return {
    instanceId: ProviderInstanceId.make("codex"),
    driver: CODEX_DRIVER,
    getCapabilities: () => Effect.succeed(options.capabilities ?? CodexCapabilities),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: (input) =>
      Effect.gen(function* () {
        if (options.beforeOpen !== undefined) {
          yield* options.beforeOpen(input);
        }
        if (options.mcpConfigs !== undefined) {
          yield* Ref.update(options.mcpConfigs, (configs) => [
            ...configs,
            McpProviderSession.readMcpProviderSession(input.threadId),
          ]);
        }
        const now = yield* DateTime.now;
        const events = yield* Queue.unbounded<ProviderAdapterV2Event, Cause.Done>();
        const session = makeProviderSession({
          providerSessionId: input.providerSessionId,
          now,
          ...(options.capabilities === undefined ? {} : { capabilities: options.capabilities }),
        });
        yield* Ref.update(state, (current) => {
          const eventQueues = new Map(current.eventQueues);
          eventQueues.set(String(input.providerSessionId), events);
          return {
            ...current,
            openCount: current.openCount + 1,
            eventQueues,
          };
        });
        yield* Effect.addFinalizer(() =>
          Ref.update(state, (current) => ({
            ...current,
            closeCount: current.closeCount + 1,
          })).pipe(Effect.andThen(options.afterScopeClose ?? Effect.void)),
        );
        if (options.hangSessionScopeClose === true) {
          // Registered last so it runs first on scope close, wedging the
          // close before the closeCount finalizer, like a provider process
          // that never yields its message stream.
          yield* Effect.addFinalizer(() => Effect.never);
        }

        const runtime = {
          get continuationSourceIdentity() {
            return options.continuationSourceIdentity?.();
          },
          ...(options.observeThreadRuntime === undefined
            ? {}
            : { observeThreadRuntime: options.observeThreadRuntime }),
          instanceId: ProviderInstanceId.make("codex"),
          driver: CODEX_DRIVER,
          providerSessionId: input.providerSessionId,
          providerSession: session,
          events: options.failEventStream
            ? Stream.fail(
                new ProviderAdapterEventStreamError({
                  driver: CODEX_DRIVER,
                  providerSessionId: input.providerSessionId,
                  cause: "process exited",
                }),
              )
            : Stream.fromQueue(events),
          ...(options.hasPendingBackgroundWork === undefined
            ? {}
            : { hasPendingBackgroundWork: options.hasPendingBackgroundWork }),
          ensureThread: () => unimplemented("ensureThread unused in test"),
          resumeThread: (threadInput) =>
            (options.beforeResume ?? Effect.void).pipe(
              Effect.andThen(
                Ref.update(state, (current) => ({
                  ...current,
                  resumeCount: current.resumeCount + 1,
                })),
              ),
              Effect.as(threadInput.providerThread),
            ),
          startTurn: (turnInput) => options.beforeStartTurn?.(turnInput) ?? Effect.void,
          steerTurn: () => Effect.void,
          interruptTurn: (input) =>
            Ref.update(state, (current) => ({
              ...current,
              interruptCount: current.interruptCount + 1,
            })).pipe(Effect.andThen(options.beforeInterruptTurn?.(input) ?? Effect.void)),
          unloadThread: ({ providerThread }) =>
            (options.beforeUnload ?? Effect.void).pipe(
              Effect.andThen(
                Ref.update(state, (current) => ({
                  ...current,
                  unloadedNativeThreadIds: [
                    ...current.unloadedNativeThreadIds,
                    providerThread.nativeThreadRef?.nativeId ?? "",
                  ],
                })),
              ),
            ),
          respondToRuntimeRequest: () => Effect.void,
          readThreadSnapshot: () => unimplemented("readThreadSnapshot unused in test"),
          rollbackThread: () => unimplemented("rollbackThread unused in test"),
          forkThread: () => unimplemented("forkThread unused in test"),
        } satisfies ProviderAdapterV2SessionRuntime;
        // A fixture generation may be absent at read time, so expose it the way the
        // manager exposes a dynamic runtime generation.
        Object.defineProperty(runtime, "runtimeGeneration", {
          configurable: true,
          enumerable: true,
          get: () =>
            options.runtimeGeneration === undefined
              ? input.nativeOperation?.runtimeGeneration
              : options.runtimeGeneration(),
        });
        options.onRuntimeCreated?.(runtime);
        return runtime;
      }),
  };
}

function makeTestLayer(input: {
  readonly state: Ref.Ref<TestProviderRuntimeState>;
  readonly idleTimeoutMs: number;
  readonly maxIdlePinMs?: number;
  readonly configureMcp?: boolean;
  readonly failEventStream?: boolean;
  readonly capabilities?: OrchestrationV2ProviderCapabilities;
  readonly mcpConfigs?: Ref.Ref<
    ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
  >;
  readonly onRuntimeCreated?: (runtime: ProviderAdapterV2SessionRuntime) => void;
  readonly beforeResume?: Effect.Effect<void>;
  readonly beforeOpen?: (
    input: ProviderAdapterV2OpenSessionInput,
  ) => Effect.Effect<void, ProviderAdapterV2Error>;
  readonly beforeStartTurn?: (
    input: ProviderAdapterV2TurnInput,
  ) => Effect.Effect<void, ProviderAdapterV2Error>;
  readonly beforeInterruptTurn?: (
    input: Parameters<ProviderAdapterV2SessionRuntime["interruptTurn"]>[0],
  ) => Effect.Effect<void, ProviderAdapterV2Error>;
  readonly afterScopeClose?: Effect.Effect<void>;
  readonly failReleaseEventWrites?: boolean;
  readonly hasPendingBackgroundWork?: Effect.Effect<boolean>;
  readonly hangSessionScopeClose?: boolean;
  readonly beforeUnload?: Effect.Effect<void>;
  readonly serverSettingsLayer?: ReturnType<typeof ServerSettings.layerTest>;
  readonly projectServiceLayer?: Layer.Layer<ProjectService.ProjectService>;
  readonly runtimeGeneration?: () => string | undefined;
  readonly continuationSourceIdentity?: () => ProviderContinuationSourceIdentity | undefined;
  readonly failProviderThreadRecordsRead?: () => boolean;
  readonly observeThreadRuntime?: (
    binding: ProviderRuntimeBinding,
  ) => Effect.Effect<ProviderRuntimeObservation>;
}) {
  const configuredEventSinkLayer = input.failReleaseEventWrites
    ? FailingReleaseEventSinkLayer
    : TestEventSinkLayer;
  const guardedProjectionLayer =
    input.failProviderThreadRecordsRead === undefined
      ? TestStoresLayer
      : Layer.effect(
          ProjectionStore.ProjectionStoreV2,
          Effect.gen(function* () {
            const delegate = yield* ProjectionStore.ProjectionStoreV2;
            return ProjectionStore.ProjectionStoreV2.of({
              ...delegate,
              getThreadRecords: (...args) =>
                input.failProviderThreadRecordsRead?.() === true &&
                args[1].some((field) => field === "providerThreads") &&
                args[1].some((field) => field === "providerTurns")
                  ? Effect.fail(
                      new ProjectionStore.ProjectionStoreReadError({
                        threadId: args[0],
                        cause: "Synthetic current target read failure",
                      }),
                    )
                  : delegate.getThreadRecords(...args),
            });
          }),
        ).pipe(Layer.provide(TestStoresLayer));
  const registryLayer = ProviderAdapterRegistry.makeSingleLayer(
    makeProviderAdapter(input.state, {
      failEventStream: input.failEventStream ?? false,
      ...(input.capabilities === undefined ? {} : { capabilities: input.capabilities }),
      ...(input.mcpConfigs === undefined ? {} : { mcpConfigs: input.mcpConfigs }),
      ...(input.onRuntimeCreated === undefined ? {} : { onRuntimeCreated: input.onRuntimeCreated }),
      ...(input.beforeOpen === undefined ? {} : { beforeOpen: input.beforeOpen }),
      ...(input.beforeResume === undefined ? {} : { beforeResume: input.beforeResume }),
      ...(input.beforeStartTurn === undefined ? {} : { beforeStartTurn: input.beforeStartTurn }),
      ...(input.beforeInterruptTurn === undefined
        ? {}
        : { beforeInterruptTurn: input.beforeInterruptTurn }),
      ...(input.afterScopeClose === undefined ? {} : { afterScopeClose: input.afterScopeClose }),
      ...(input.hasPendingBackgroundWork === undefined
        ? {}
        : { hasPendingBackgroundWork: input.hasPendingBackgroundWork }),
      ...(input.hangSessionScopeClose === undefined
        ? {}
        : { hangSessionScopeClose: input.hangSessionScopeClose }),
      ...(input.beforeUnload === undefined ? {} : { beforeUnload: input.beforeUnload }),
      ...(input.runtimeGeneration === undefined
        ? {}
        : { runtimeGeneration: input.runtimeGeneration }),
      ...(input.continuationSourceIdentity === undefined
        ? {}
        : { continuationSourceIdentity: input.continuationSourceIdentity }),
      ...(input.observeThreadRuntime === undefined
        ? {}
        : { observeThreadRuntime: input.observeThreadRuntime }),
    }),
  );
  const providerEventIngestorTestLayer = ProviderEventIngestor.layer.pipe(
    Layer.provide(Layer.mergeAll(configuredEventSinkLayer, IdAllocator.layer, TestStoresLayer)),
  );
  return Layer.mergeAll(
    TestStoresLayer,
    configuredEventSinkLayer,
    IdAllocator.layer,
    TestMcpRegistryLayer,
    ProviderSessionManager.layerWithOptions({
      idleTimeoutMs: input.idleTimeoutMs,
      ...(input.configureMcp === undefined ? {} : { configureMcp: input.configureMcp }),
      ...(input.maxIdlePinMs === undefined ? {} : { maxIdlePinMs: input.maxIdlePinMs }),
    }).pipe(
      Layer.provide(
        Layer.mergeAll(
          registryLayer,
          configuredEventSinkLayer,
          IdAllocator.layer,
          providerEventIngestorTestLayer,
          TestMcpRegistryLayer,
          TestStoresLayer,
          guardedProjectionLayer,
          ...(input.serverSettingsLayer === undefined ? [] : [input.serverSettingsLayer]),
          ...(input.projectServiceLayer === undefined ? [] : [input.projectServiceLayer]),
        ),
      ),
    ),
  ).pipe(Layer.provide(NodeServices.layer));
}

const fakeHttpServer = HttpServer.HttpServer.of({
  address: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 43123),
  serve: (() => Effect.void) as HttpServer.HttpServer["Service"]["serve"],
});

const fakeEnvironment = ServerEnvironment.ServerEnvironment.of({
  getEnvironmentId: Effect.succeed(EnvironmentId.make("environment-provider-session-manager")),
  getDescriptor: Effect.die("unused"),
});

const TestMcpRegistryLayer = Layer.effect(
  McpSessionRegistry.McpSessionRegistry,
  McpSessionRegistry.__testing.make(),
).pipe(
  Layer.provide(Layer.succeed(HttpServer.HttpServer, fakeHttpServer)),
  Layer.provide(Layer.succeed(ServerEnvironment.ServerEnvironment, fakeEnvironment)),
  Layer.provide(NodeServices.layer),
);

function makeBrowserAccessProject(projectId: ProjectId): Project {
  return {
    id: projectId,
    title: "Browser access project",
    workspaceRoot: process.cwd(),
    repositoryIdentity: null,
    faviconPath: null,
    projectIcon: null,
    defaultModelSelection: null,
    defaultThreadEnvMode: null,
    autoPull: false,
    scripts: [],
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    deletedAt: null,
  };
}

const configuredEffortDefaultAccount = ProviderInstanceId.make("codex-configured-default");
function configuredEffortSelection(effort: string): ModelSelection {
  return {
    instanceId: configuredEffortDefaultAccount,
    model: "openai.gpt-5.4",
    options: [{ id: "reasoningEffort", value: effort }],
  };
}

function runConfiguredEffortDispatch(input: {
  readonly setupMode: "existing" | "managed";
  readonly projectSource?: "override" | "project-shell";
}) {
  return Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const captured = yield* Ref.make<ReadonlyArray<ProviderAdapterV2TurnInput>>([]);
    let openedSelection: ModelSelection | undefined;
    const projectId = ProjectId.make(
      `configured-effort-${input.setupMode}-${input.projectSource ?? "environment"}`,
    );
    const settingsLayer = ServerSettings.layerTest({
      defaultModelSelection: configuredEffortSelection("xhigh"),
      projectSettingsFolded: false,
      providerInstances: {
        [modelSelection.instanceId]: {
          driver: CODEX_DRIVER,
          config: { setupMode: input.setupMode },
        },
        [configuredEffortDefaultAccount]: {
          driver: CODEX_DRIVER,
          config: { setupMode: input.setupMode },
        },
      },
      ...(input.projectSource === "override"
        ? {
            projectSettingsOverrides: {
              [projectId]: { defaultModelSelection: configuredEffortSelection("low") },
            },
          }
        : {}),
    });
    const projectServiceLayer = Layer.mock(ProjectService.ProjectService)({
      getById: (id) =>
        Effect.succeed(
          Option.some({
            ...makeBrowserAccessProject(id),
            defaultModelSelection:
              input.projectSource === "project-shell" ? configuredEffortSelection("low") : null,
          }),
        ),
    });
    yield* Effect.gen(function* () {
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const sink = yield* EventSink.EventSinkV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const ids = yield* IdAllocator.IdAllocatorV2;
      const settings = yield* ServerSettings.ServerSettingsService;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make(
        `configured-effort-${input.setupMode}-${input.projectSource ?? "environment"}`,
      );
      const sessionId = yield* ids.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const providerThread = makeProviderThread({
        idAllocator: ids,
        threadId,
        providerSessionId: sessionId,
        now,
      });
      const runId = ids.derive.run({ threadId, ordinal: 1 });
      const attemptId = ids.derive.runAttempt({ runId, attemptOrdinal: 1 });
      const rootNodeId = ids.derive.rootNode({ runId });
      yield* sink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator: ids, threadId, projectId, now })],
      });
      const runtime = yield* manager.open({
        threadId,
        providerSessionId: sessionId,
        modelSelection,
        runtimePolicy,
      });
      const messageId = yield* persistStartedRuntimeFixture({
        eventSink: sink,
        idAllocator: ids,
        threadId,
        providerThread,
        runId,
        attemptId,
        rootNodeId,
        now,
      });
      const appThread = yield* projections.getThread(threadId);
      const turn: ProviderAdapterV2TurnInput = {
        appThread,
        threadId,
        runId,
        runOrdinal: 1,
        providerTurnOrdinal: 1,
        attemptId,
        rootNodeId,
        providerThread,
        modelSelection,
        runtimePolicy,
        message: {
          messageId,
          text: "configured effort",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
        },
      };
      yield* runtime.startTurn(turn);
      let sent = yield* Ref.get(captured);
      assert.deepEqual(sent[0]?.configuredDefaultModelSelection, {
        driver: CODEX_DRIVER,
        modelSelection: configuredEffortSelection(
          input.projectSource === undefined ? "xhigh" : "low",
        ),
      });
      assert.deepEqual(sent[0]?.modelSelection, modelSelection);
      yield* settings.updateSettings({
        defaultModelSelection: configuredEffortSelection("medium"),
      });
      yield* runtime.startTurn(turn);
      sent = yield* Ref.get(captured);
      assert.deepEqual(sent[1]?.configuredDefaultModelSelection, {
        driver: CODEX_DRIVER,
        modelSelection: configuredEffortSelection(
          input.projectSource === undefined ? "medium" : "low",
        ),
      });
      const explicit: ModelSelection = {
        ...modelSelection,
        options: [{ id: "reasoningEffort", value: "high" }],
      };
      yield* runtime.startTurn({ ...turn, modelSelection: explicit });
      sent = yield* Ref.get(captured);
      assert.deepEqual(sent[2]?.modelSelection, explicit);
      assert.isUndefined(sent[2]?.configuredDefaultModelSelection);
      assert.deepEqual(openedSelection, modelSelection);
      assert.deepEqual((yield* projections.getThread(threadId)).modelSelection, modelSelection);
      assert.deepEqual(
        (yield* projections.getThreadProjection(threadId)).runs.find((run) => run.id === runId)
          ?.modelSelection,
        modelSelection,
      );
      assert.equal((yield* Ref.get(state)).openCount, 1);
    }).pipe(
      Effect.provide(
        Layer.merge(
          makeTestLayer({
            state,
            idleTimeoutMs: 60_000,
            serverSettingsLayer: settingsLayer,
            projectServiceLayer,
            beforeOpen: (open) =>
              Effect.sync(() => {
                openedSelection = open.modelSelection;
              }),
            beforeStartTurn: (turn) => Ref.update(captured, (values) => [...values, turn]),
          }),
          settingsLayer,
        ),
      ),
    );
  });
}

for (const setupMode of ["existing", "managed"] as const) {
  it.effect(
    `dispatches current cross-account configured effort without persisting it (${setupMode})`,
    () => runConfiguredEffortDispatch({ setupMode }),
  );
}
for (const projectSource of ["override", "project-shell"] as const) {
  it.effect(
    `dispatches project configured effort before the environment default (${projectSource})`,
    () => runConfiguredEffortDispatch({ setupMode: "existing", projectSource }),
  );
}

function runBrowserAccessScenario(input: {
  readonly enableAgentBrowserAccess: boolean;
  readonly projectOverride: boolean;
  readonly deviceOverride?: boolean;
  readonly createThread?: boolean;
  readonly projectExists?: boolean;
}) {
  return Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const mcpConfigs = yield* Ref.make<
      ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
    >([]);
    const projectId = ProjectId.make("project-provider-session-manager-browser-access");
    const threadId = ThreadId.make("thread-provider-session-manager-browser-access");
    const projectServiceLayer = Layer.mock(ProjectService.ProjectService)({
      getById: (requestedProjectId) =>
        Effect.succeed(
          input.projectExists === false
            ? Option.none()
            : Option.some(makeBrowserAccessProject(requestedProjectId)),
        ),
    });

    yield* Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      if (input.createThread !== false) {
        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now, projectId })],
        });
      }
      yield* manager
        .open({ threadId, providerSessionId, modelSelection, runtimePolicy })
        .pipe(Effect.ignore);
    }).pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 1_000,
          mcpConfigs,
          projectServiceLayer,
          serverSettingsLayer: ServerSettings.layerTest({
            enableAgentBrowserAccess: input.enableAgentBrowserAccess,
            projectSettingsOverrides: {
              [projectId]: {
                enableAgentBrowserAccess: input.projectOverride,
                ...(input.deviceOverride === undefined
                  ? {}
                  : { enableAgentDeviceAccess: input.deviceOverride }),
              },
            },
          }),
        }),
      ),
    );

    return (yield* Ref.get(mcpConfigs))[0];
  });
}

function makePendingRuntimeRequestEvents(input: {
  readonly idAllocator: IdAllocator.IdAllocatorV2Shape;
  readonly threadId: ThreadId;
  readonly providerSessionId: ProviderSessionId;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly now: DateTime.Utc;
}) {
  return Effect.gen(function* () {
    const requestId = yield* input.idAllocator.allocate.runtimeRequest({
      driver: CODEX_DRIVER,
      nativeRequestId: "pending-approval",
    });
    const nodeId = input.idAllocator.derive.approvalNode({ requestId });
    const node = {
      id: nodeId,
      threadId: input.threadId,
      runId: null,
      parentNodeId: null,
      rootNodeId: nodeId,
      kind: "approval_request" as const,
      status: "waiting" as const,
      countsForRun: false,
      providerThreadId: input.providerThread.id,
      providerTurnId: null,
      nativeItemRef: null,
      runtimeRequestId: requestId,
      checkpointScopeId: null,
      startedAt: input.now,
      completedAt: null,
    };
    const request = {
      id: requestId,
      nodeId,
      providerTurnId: null,
      nativeRequestRef: {
        driver: CODEX_DRIVER,
        nativeId: "pending-approval",
        strength: "strong" as const,
      },
      kind: "command" as const,
      status: "pending" as const,
      responseCapability: {
        type: "live" as const,
        providerSessionId: input.providerSessionId,
      },
      createdAt: input.now,
      resolvedAt: null,
    };
    const turnItem = {
      id: input.idAllocator.derive.approvalTurnItem({ requestId }),
      threadId: input.threadId,
      runId: null,
      nodeId,
      providerThreadId: input.providerThread.id,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: 1,
      status: "waiting" as const,
      title: null,
      startedAt: input.now,
      completedAt: null,
      updatedAt: input.now,
      type: "approval_request" as const,
      requestId,
      requestKind: "command" as const,
    };
    const events = [
      {
        id: yield* input.idAllocator.allocate.event({
          threadId: input.threadId,
          providerSessionId: input.providerSessionId,
        }),
        type: "node.updated" as const,
        threadId: input.threadId,
        nodeId,
        driver: CODEX_DRIVER,
        occurredAt: input.now,
        payload: node,
      },
      {
        id: yield* input.idAllocator.allocate.event({
          threadId: input.threadId,
          providerSessionId: input.providerSessionId,
        }),
        type: "runtime-request.updated" as const,
        threadId: input.threadId,
        nodeId,
        driver: CODEX_DRIVER,
        occurredAt: input.now,
        payload: request,
      },
      {
        id: yield* input.idAllocator.allocate.event({
          threadId: input.threadId,
          providerSessionId: input.providerSessionId,
        }),
        type: "turn-item.updated" as const,
        threadId: input.threadId,
        nodeId,
        driver: CODEX_DRIVER,
        occurredAt: input.now,
        payload: turnItem,
      },
    ] satisfies ReadonlyArray<OrchestrationV2DomainEvent>;
    const providerEvents = [
      {
        type: "runtime_request.updated" as const,
        driver: CODEX_DRIVER,
        threadId: input.threadId,
        runtimeRequest: request,
      },
      {
        type: "node.updated" as const,
        driver: CODEX_DRIVER,
        node,
      },
      {
        type: "turn_item.updated" as const,
        driver: CODEX_DRIVER,
        turnItem,
      },
    ] satisfies ReadonlyArray<ProviderAdapterV2Event>;
    return { events, providerEvents, requestId, nodeId };
  });
}

it.effect("ProviderSessionManagerV2 opens independent sessions concurrently", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const openStartedCount = yield* Ref.make(0);
    const firstOpenStarted = yield* Deferred.make<void>();
    const secondOpenStarted = yield* Deferred.make<void>();
    const releaseOpens = yield* Deferred.make<void>();
    const beforeOpen = () =>
      Effect.gen(function* () {
        const openNumber = yield* Ref.modify(openStartedCount, (count) => [count + 1, count + 1]);
        yield* Deferred.succeed(openNumber === 1 ? firstOpenStarted : secondOpenStarted, undefined);
        yield* Deferred.await(releaseOpens);
      });

    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const firstThreadId = ThreadId.make("thread-provider-session-manager-concurrent-a");
      const secondThreadId = ThreadId.make("thread-provider-session-manager-concurrent-b");
      const firstProviderSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: firstThreadId,
      });
      const secondProviderSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: secondThreadId,
      });

      yield* eventSink.write({
        events: [
          yield* makeThreadCreatedEvent({ idAllocator, threadId: firstThreadId, now }),
          yield* makeThreadCreatedEvent({ idAllocator, threadId: secondThreadId, now }),
        ],
      });
      const firstFiber = yield* manager
        .open({
          threadId: firstThreadId,
          providerSessionId: firstProviderSessionId,
          modelSelection,
          runtimePolicy,
        })
        .pipe(Effect.forkScoped);
      yield* Deferred.await(firstOpenStarted);
      const secondFiber = yield* manager
        .open({
          threadId: secondThreadId,
          providerSessionId: secondProviderSessionId,
          modelSelection,
          runtimePolicy,
        })
        .pipe(Effect.forkScoped);

      yield* Deferred.await(secondOpenStarted);
      assert.equal(yield* Ref.get(openStartedCount), 2);
      yield* Deferred.succeed(releaseOpens, undefined);
      const [firstRuntime, secondRuntime] = yield* Effect.all([
        Fiber.join(firstFiber),
        Fiber.join(secondFiber),
      ]);
      assert.notStrictEqual(firstRuntime, secondRuntime);
      assert.equal((yield* Ref.get(state)).openCount, 2);
    });

    yield* effect.pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 60_000,
          beforeOpen,
        }),
      ),
    );
  }),
);

it.effect("ProviderSessionManagerV2 closes every live session for a provider instance", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const firstThreadId = ThreadId.make("thread-provider-session-manager-logout-a");
      const secondThreadId = ThreadId.make("thread-provider-session-manager-logout-b");
      const firstProviderSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: firstThreadId,
      });
      const secondProviderSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: secondThreadId,
      });

      yield* eventSink.write({
        events: [
          yield* makeThreadCreatedEvent({ idAllocator, threadId: firstThreadId, now }),
          yield* makeThreadCreatedEvent({ idAllocator, threadId: secondThreadId, now }),
        ],
      });
      yield* manager.open({
        threadId: firstThreadId,
        providerSessionId: firstProviderSessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* manager.open({
        threadId: secondThreadId,
        providerSessionId: secondProviderSessionId,
        modelSelection,
        runtimePolicy,
      });

      yield* manager.closeInstance(modelSelection.instanceId);

      assert.isTrue(Option.isNone(yield* manager.get(firstProviderSessionId)));
      assert.isTrue(Option.isNone(yield* manager.get(secondProviderSessionId)));
      assert.equal((yield* Ref.get(state)).closeCount, 2);
    });

    yield* effect.pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 60_000,
        }),
      ),
    );
  }),
);

it.effect("ProviderSessionManagerV2 opens a duplicate session only once", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const openStartedCount = yield* Ref.make(0);
    const firstOpenStarted = yield* Deferred.make<void>();
    const releaseOpen = yield* Deferred.make<void>();
    const beforeOpen = () =>
      Ref.updateAndGet(openStartedCount, (count) => count + 1).pipe(
        Effect.tap(() => Deferred.succeed(firstOpenStarted, undefined)),
        Effect.andThen(Deferred.await(releaseOpen)),
      );

    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-single-flight");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const open = manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      const firstFiber = yield* open.pipe(Effect.forkScoped);
      yield* Deferred.await(firstOpenStarted);
      const secondFiber = yield* open.pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;
      assert.equal(yield* Ref.get(openStartedCount), 1);

      yield* Deferred.succeed(releaseOpen, undefined);
      const [firstRuntime, secondRuntime] = yield* Effect.all([
        Fiber.join(firstFiber),
        Fiber.join(secondFiber),
      ]);
      assert.strictEqual(firstRuntime, secondRuntime);
      assert.equal((yield* Ref.get(state)).openCount, 1);
    });

    yield* effect.pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 60_000,
          beforeOpen,
        }),
      ),
    );
  }),
);

it.effect("ProviderSessionManagerV2 releases live sessions when its layer shuts down", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-shutdown");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });

      const liveState = yield* Ref.get(state);
      assert.equal(liveState.openCount, 1);
      assert.equal(liveState.closeCount, 0);
    });

    yield* effect.pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 60_000,
        }),
      ),
    );

    assert.equal((yield* Ref.get(state)).closeCount, 1);
  }),
);

it.effect("ProviderSessionManagerV2 closes event subscriptions normally on server shutdown", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-shutdown-subscription");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      const runtime = yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      const bufferedSubscription = yield* runtime.subscribeEvents!;
      const activeSubscription = yield* runtime.subscribeEvents!;
      const adapterQueue = (yield* Ref.get(state)).eventQueues.get(String(providerSessionId));
      assert.isDefined(adapterQueue);
      yield* Queue.offer(adapterQueue!, {
        type: "provider_session.updated",
        driver: CODEX_DRIVER,
        providerSession: runtime.providerSession,
      });
      assert.isTrue(Option.isSome(yield* activeSubscription.events.pipe(Stream.runHead)));

      yield* manager.shutdown;

      assert.isEmpty(yield* bufferedSubscription.events.pipe(Stream.runCollect));
    });

    yield* effect.pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000 })));
  }),
);

it.effect("ProviderSessionManagerV2 drains subscribers when the provider stops", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-provider-stop");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      const runtime = yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      const subscription = yield* runtime.subscribeEvents!;
      const collected = yield* subscription.events.pipe(Stream.runCollect, Effect.forkScoped);
      const adapterQueue = (yield* Ref.get(state)).eventQueues.get(String(providerSessionId));
      assert.isDefined(adapterQueue);
      const providerThreadId = idAllocator.derive.providerThread({
        driver: CODEX_DRIVER,
        nativeThreadId: "provider-stop-thread",
      });
      const providerTurnId = idAllocator.derive.providerTurn({
        driver: CODEX_DRIVER,
        nativeTurnId: "provider-stop-turn",
      });
      yield* Queue.offer(adapterQueue!, {
        type: "turn.terminal",
        driver: CODEX_DRIVER,
        providerThreadId,
        providerTurnId,
        runOrdinal: 1,
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      });
      yield* Queue.offer(adapterQueue!, {
        type: "provider_session.updated",
        driver: CODEX_DRIVER,
        providerSession: {
          ...runtime.providerSession,
          status: "stopped",
          updatedAt: now,
        },
      });
      yield* Queue.end(adapterQueue!);

      const events = Array.from(yield* Fiber.join(collected));
      assert.deepEqual(
        events.map((event) => event.type),
        ["turn.terminal", "provider_session.updated"],
      );
      assert.isTrue(Option.isNone(yield* manager.get(providerSessionId)));
      assert.equal((yield* Ref.get(state)).closeCount, 1);
    });

    yield* effect.pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000 })));
  }),
);

it.effect(
  "ProviderSessionManagerV2 issues MCP credentials before opening and revokes them on close",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const mcpConfigs = yield* Ref.make<
        ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
      >([]);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const registry = yield* McpSessionRegistry.McpSessionRegistry;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("thread-provider-session-manager-mcp");
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });

        const captured = (yield* Ref.get(mcpConfigs))[0];
        assert.isDefined(captured);
        assert.equal(captured?.threadId, threadId);
        assert.equal(captured?.providerInstanceId, modelSelection.instanceId);
        assert.equal(captured?.endpoint, "http://127.0.0.1:43123/mcp");
        const token = captured?.authorizationHeader.replace(/^Bearer\s+/, "");
        assert.isDefined(token);
        const resolved = yield* registry.resolve(token!);
        assert.equal(resolved?.threadId, threadId);
        assert.deepEqual(
          resolved?.capabilities,
          new Set([
            "preview",
            "orchestration",
            "worktree",
            "pull-requests",
            "organization",
            "decision-snapshot",
          ]),
        );

        yield* manager.close(providerSessionId);
        assert.isUndefined(McpProviderSession.readMcpProviderSession(threadId));
        assert.isUndefined(yield* registry.resolve(token!));
      });

      yield* effect.pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 1_000,
            mcpConfigs,
          }),
        ),
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 withholds the preview capability when agent browser access is off",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const mcpConfigs = yield* Ref.make<
        ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
      >([]);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const registry = yield* McpSessionRegistry.McpSessionRegistry;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("thread-provider-session-manager-no-browser");
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });

        const captured = (yield* Ref.get(mcpConfigs))[0];
        assert.isDefined(captured);
        assert.equal(captured?.browserToolsAvailable, false);
        const token = captured?.authorizationHeader.replace(/^Bearer\s+/, "");
        const resolved = yield* registry.resolve(token!);
        assert.deepEqual(
          resolved?.capabilities,
          new Set([
            "orchestration",
            "worktree",
            "pull-requests",
            "organization",
            "decision-snapshot",
          ]),
        );

        yield* manager.close(providerSessionId);
      });

      yield* effect.pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 1_000,
            mcpConfigs,
            // orDie: the test layer's settings-normalization error cannot
            // occur for a literal override and the slot requires error never.
            serverSettingsLayer: ServerSettings.layerTest({
              enableAgentBrowserAccess: false,
            }).pipe(Layer.orDie),
          }),
        ),
      );
    }),
);

it.effect("ProviderSessionManagerV2 honors a project browser-access opt-out", () =>
  Effect.gen(function* () {
    const captured = yield* runBrowserAccessScenario({
      enableAgentBrowserAccess: true,
      projectOverride: false,
    });
    assert.isDefined(captured);
    assert.equal(captured?.browserToolsAvailable, false);
  }),
);

it.effect("ProviderSessionManagerV2 honors a project browser-access opt-in", () =>
  Effect.gen(function* () {
    const captured = yield* runBrowserAccessScenario({
      enableAgentBrowserAccess: false,
      projectOverride: true,
    });
    assert.isDefined(captured);
    assert.equal(captured?.browserToolsAvailable, true);
  }),
);

it.effect("ProviderSessionManagerV2 fails browser access closed for a missing project", () =>
  Effect.gen(function* () {
    const captured = yield* runBrowserAccessScenario({
      enableAgentBrowserAccess: true,
      projectOverride: true,
      projectExists: false,
    });
    assert.isDefined(captured);
    assert.equal(captured?.browserToolsAvailable, false);
  }),
);

it.effect("ProviderSessionManagerV2 fails browser access closed for a missing thread", () =>
  Effect.gen(function* () {
    const captured = yield* runBrowserAccessScenario({
      enableAgentBrowserAccess: true,
      projectOverride: true,
      createThread: false,
    });
    assert.isDefined(captured);
    assert.equal(captured?.browserToolsAvailable, false);
  }),
);

it.effect("ProviderSessionManagerV2 revokes MCP credentials when release persistence fails", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const mcpConfigs = yield* Ref.make<
      ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
    >([]);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const registry = yield* McpSessionRegistry.McpSessionRegistry;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-mcp-release-failure");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });

      const captured = (yield* Ref.get(mcpConfigs))[0];
      const token = captured?.authorizationHeader.replace(/^Bearer\s+/, "");
      assert.isDefined(token);
      assert.isDefined(yield* registry.resolve(token!));

      const closeError = yield* manager.close(providerSessionId).pipe(Effect.flip);
      assert.equal(closeError._tag, "ProviderSessionCloseError");
      assert.isUndefined(McpProviderSession.readMcpProviderSession(threadId));
      assert.isUndefined(yield* registry.resolve(token!));
    });

    yield* effect.pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 1_000,
          mcpConfigs,
          failReleaseEventWrites: true,
        }),
      ),
    );
  }),
);

it.effect("ProviderSessionManagerV2 duplicate detach preserves replacement MCP credentials", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const mcpConfigs = yield* Ref.make<
      ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
    >([]);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const registry = yield* McpSessionRegistry.McpSessionRegistry;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-replacement-mcp");
      const oldSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const replacementSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId: oldSessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* manager.detach({ providerSessionId: oldSessionId, threadId });
      yield* manager.open({
        threadId,
        providerSessionId: replacementSessionId,
        modelSelection,
        runtimePolicy,
      });

      const replacement = (yield* Ref.get(mcpConfigs)).at(-1);
      assert.isDefined(replacement);
      const replacementToken = replacement?.authorizationHeader.replace(/^Bearer\s+/, "");
      assert.isDefined(replacementToken);
      assert.equal(
        McpProviderSession.readMcpProviderSession(threadId)?.providerSessionId,
        replacement?.providerSessionId,
      );

      yield* manager.detach({ providerSessionId: oldSessionId, threadId });

      assert.equal(
        McpProviderSession.readMcpProviderSession(threadId)?.providerSessionId,
        replacement?.providerSessionId,
      );
      assert.equal((yield* registry.resolve(replacementToken!))?.threadId, threadId);
    });

    yield* effect.pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 1_000,
          capabilities: ExclusiveCapabilities,
          mcpConfigs,
        }),
      ),
    );
  }),
);

it.effect(
  "ProviderSessionManagerV2 detach of a superseded live session preserves replacement MCP credentials",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const mcpConfigs = yield* Ref.make<
        ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
      >([]);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const registry = yield* McpSessionRegistry.McpSessionRegistry;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("thread-provider-session-manager-superseded-mcp");
        const oldSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const replacementSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        yield* manager.open({
          threadId,
          providerSessionId: oldSessionId,
          modelSelection,
          runtimePolicy,
        });
        // The replacement opens while the old session is still attached: this is
        // the workspace-handoff sequence, where the queued continuation run can
        // start its session before the outbox executes the old session's detach.
        yield* manager.open({
          threadId,
          providerSessionId: replacementSessionId,
          modelSelection,
          runtimePolicy,
        });

        const replacement = (yield* Ref.get(mcpConfigs)).at(-1);
        assert.isDefined(replacement);
        const replacementToken = replacement?.authorizationHeader.replace(/^Bearer\s+/, "");
        assert.isDefined(replacementToken);
        assert.equal(
          McpProviderSession.readMcpProviderSession(threadId)?.providerSessionId,
          replacement?.providerSessionId,
        );

        // First (non-duplicate) detach of the superseded session must not revoke
        // the replacement's credential or clear its config slot.
        yield* manager.detach({ providerSessionId: oldSessionId, threadId });

        assert.equal(
          McpProviderSession.readMcpProviderSession(threadId)?.providerSessionId,
          replacement?.providerSessionId,
        );
        assert.equal((yield* registry.resolve(replacementToken!))?.threadId, threadId);
      });

      yield* effect.pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 1_000,
            capabilities: ExclusiveCapabilities,
            mcpConfigs,
          }),
        ),
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 keeps a thread's MCP credential stable across detach and re-attach on a shared session",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const mcpConfigs = yield* Ref.make<
        ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
      >([]);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const registry = yield* McpSessionRegistry.McpSessionRegistry;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("thread-provider-session-manager-stable-mcp");
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });

        const original = (yield* Ref.get(mcpConfigs)).at(-1);
        assert.isDefined(original);
        const originalToken = original?.authorizationHeader.replace(/^Bearer\s+/, "");
        assert.isDefined(originalToken);

        // Workspace-change handoff on a shared multi-thread session (codex):
        // the thread detaches while the provider process keeps running, and the
        // process's MCP client keeps using the credential it was started with.
        yield* manager.detach({ providerSessionId, threadId, detail: "Workspace changed." });
        assert.equal(
          (yield* registry.resolve(originalToken!))?.threadId,
          threadId,
          "detach must not revoke the credential the live provider process still holds",
        );

        // The continuation run re-attaches the same thread to the same session;
        // the credential must be reused, not rotated, so the provider process's
        // long-lived MCP client stays authorized.
        yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        assert.equal(
          McpProviderSession.readMcpProviderSession(threadId)?.providerSessionId,
          original?.providerSessionId,
          "re-attach must reuse the existing credential, not rotate it",
        );
        assert.equal((yield* registry.resolve(originalToken!))?.threadId, threadId);

        // Releasing the session (provider process gone) still revokes.
        yield* manager.close(providerSessionId);
        assert.isUndefined(yield* registry.resolve(originalToken!));
      });

      yield* effect.pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 1_000,
            mcpConfigs,
          }),
        ),
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 revokes a rotated credential despite a stale record on another live session",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const mcpConfigs = yield* Ref.make<
        ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
      >([]);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const registry = yield* McpSessionRegistry.McpSessionRegistry;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("thread-provider-session-manager-stale-record");
        const s1 = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const s2 = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        // S1 (shared session) records credential C1 for the thread, then the
        // thread detaches; S1 stays alive with the stale record.
        yield* manager.open({ threadId, providerSessionId: s1, modelSelection, runtimePolicy });
        yield* manager.detach({ providerSessionId: s1, threadId });

        // The credential dies externally, so S2's attach must rotate to C2.
        yield* registry.revokeThread(threadId);
        yield* manager.open({ threadId, providerSessionId: s2, modelSelection, runtimePolicy });
        const rotated = McpProviderSession.readMcpProviderSession(threadId);
        assert.isDefined(rotated);
        const rotatedToken = rotated?.authorizationHeader.replace(/^Bearer\s+/, "");
        assert.isDefined(yield* registry.resolve(rotatedToken!));

        // Releasing S2 must revoke C2 even though S1 still carries a stale
        // record (of dead C1) for the same thread.
        yield* manager.close(s2);
        assert.isUndefined(
          yield* registry.resolve(rotatedToken!),
          "stale record on S1 must not veto revoking S2's rotated credential",
        );
        yield* manager.close(s1);
      });

      yield* effect.pipe(
        Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1_000, mcpConfigs })),
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 protects a reused credential from a predecessor release during open",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const mcpConfigs = yield* Ref.make<
        ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
      >([]);
      const duringOpen = yield* Ref.make<Effect.Effect<void>>(Effect.void);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const registry = yield* McpSessionRegistry.McpSessionRegistry;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("thread-provider-session-manager-open-race");
        const s1 = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const s2 = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        yield* manager.open({ threadId, providerSessionId: s1, modelSelection, runtimePolicy });
        const original = (yield* Ref.get(mcpConfigs)).at(-1);
        const originalToken = original?.authorizationHeader.replace(/^Bearer\s+/, "");
        assert.isDefined(originalToken);
        yield* manager.detach({ providerSessionId: s1, threadId });

        // While S2's provider process is spawning (after prepare reused the
        // credential, before the entry is visible), the predecessor session
        // releases. Eager adapters (ACP, OpenCode) bake the credential into
        // the process during openSession, so the release must not revoke it;
        // rotating afterwards cannot repair those adapters.
        yield* Ref.set(duringOpen, manager.close(s1).pipe(Effect.orDie));
        yield* manager.open({ threadId, providerSessionId: s2, modelSelection, runtimePolicy });

        const slot = McpProviderSession.readMcpProviderSession(threadId);
        assert.equal(
          slot?.providerSessionId,
          original?.providerSessionId,
          "the credential the adapter was configured with must remain current",
        );
        assert.equal(
          (yield* registry.resolve(originalToken!))?.threadId,
          threadId,
          "the predecessor release must not revoke a credential reserved by an in-flight open",
        );
        yield* manager.close(s2);
      });

      yield* effect.pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 1_000,
            mcpConfigs,
            beforeOpen: (input) =>
              input.providerSessionId === undefined
                ? Effect.void
                : Ref.get(duringOpen).pipe(
                    Effect.flatten,
                    Effect.tap(() => Ref.set(duringOpen, Effect.void)),
                  ),
          }),
        ),
      );
    }),
);

it.effect("ProviderSessionManagerV2 terminal detach revokes the thread's MCP credential", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const mcpConfigs = yield* Ref.make<
      ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
    >([]);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const registry = yield* McpSessionRegistry.McpSessionRegistry;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-terminal-detach");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({ threadId, providerSessionId, modelSelection, runtimePolicy });
      const issued = (yield* Ref.get(mcpConfigs)).at(-1);
      const token = issued?.authorizationHeader.replace(/^Bearer\s+/, "");
      assert.isDefined(yield* registry.resolve(token!));

      // Archive/delete detaches carry revokeMcpCredential: the token must die
      // with the thread even though the shared provider process lives on.
      yield* manager.detach({
        providerSessionId,
        threadId,
        detail: "Thread deleted.",
        revokeMcpCredential: true,
      });
      assert.isUndefined(yield* registry.resolve(token!));
      assert.isUndefined(McpProviderSession.readMcpProviderSession(threadId));
    });

    yield* effect.pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1_000, mcpConfigs })));
  }),
);

it.effect("ProviderSessionManagerV2 releases idle sessions without sweeping all sessions", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const projectId = yield* idAllocator.allocate.project({
        fixtureName: "provider-session-manager-idle",
      });
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-idle",
        projectId,
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });

      yield* TestClock.adjust("1 second");
      yield* Effect.yieldNow;

      const liveSession = yield* manager.get(providerSessionId);
      const runtimeState = yield* Ref.get(state);
      const projection = yield* projectionStore.getThreadProjection(threadId);

      assert.isTrue(Option.isNone(liveSession));
      assert.equal(runtimeState.openCount, 1);
      assert.equal(runtimeState.closeCount, 1);
      assert.equal(projection.providerSessions.at(-1)?.status, "stopped");
    });

    yield* effect.pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1000 })));
  }),
);

it.effect("ProviderSessionManagerV2 persists release when session scope close hangs", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-hung-close",
        projectId: yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-hung-close",
        }),
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });

      yield* TestClock.adjust("1 second");
      yield* Effect.yieldNow;
      assert.isTrue(Option.isNone(yield* manager.get(providerSessionId)));

      yield* TestClock.adjust("30 seconds");
      yield* Effect.yieldNow;
      const projection = yield* projectionStore.getThreadProjection(threadId);
      assert.equal(projection.providerSessions.at(-1)?.status, "stopped");
      assert.equal((yield* Ref.get(state)).closeCount, 0);
    });

    yield* effect.pipe(
      Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1000, hangSessionScopeClose: true })),
    );
  }),
);

it.effect("ProviderSessionManagerV2 defers idle release while background work is pending", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const pendingWork = yield* Ref.make(true);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-idle-pin",
        projectId: yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-idle-pin",
        }),
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });

      yield* TestClock.adjust("3 seconds");
      yield* Effect.yieldNow;
      assert.isTrue(Option.isSome(yield* manager.get(providerSessionId)));
      assert.equal((yield* Ref.get(state)).closeCount, 0);

      yield* Ref.set(pendingWork, false);
      yield* TestClock.adjust("1 second");
      yield* Effect.yieldNow;
      assert.isTrue(Option.isNone(yield* manager.get(providerSessionId)));
      assert.equal((yield* Ref.get(state)).closeCount, 1);
    });

    yield* effect.pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 1000,
          hasPendingBackgroundWork: Ref.get(pendingWork),
        }),
      ),
    );
  }),
);

it.effect("ProviderSessionManagerV2 releases pinned idle sessions once the pin cap expires", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-pin-cap",
        projectId: yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-pin-cap",
        }),
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });

      yield* TestClock.adjust("3 seconds");
      yield* Effect.yieldNow;
      assert.isTrue(Option.isSome(yield* manager.get(providerSessionId)));

      yield* TestClock.adjust("1 second");
      yield* Effect.yieldNow;
      assert.isTrue(Option.isNone(yield* manager.get(providerSessionId)));
      assert.equal((yield* Ref.get(state)).closeCount, 1);
    });

    yield* effect.pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 1000,
          maxIdlePinMs: 3000,
          hasPendingBackgroundWork: Effect.succeed(true),
        }),
      ),
    );
  }),
);

it.effect(
  "ProviderSessionManagerV2 does not idle-release a session that turns busy during the pending-work check",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const firstCheck = yield* Ref.make(true);
      const checkEntered = yield* Deferred.make<void>();
      const checkGate = yield* Deferred.make<void>();
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        const now = yield* DateTime.now;
        const projectId = yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-busy-during-check",
        });
        const threadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-busy-during-check",
          projectId,
        });
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const providerThread = makeProviderThread({
          idAllocator,
          threadId,
          providerSessionId,
          now,
        });
        const runId = idAllocator.derive.run({ threadId, ordinal: 1 });
        const attemptId = idAllocator.derive.runAttempt({ runId, attemptOrdinal: 1 });
        const rootNodeId = idAllocator.derive.rootNode({ runId });
        const providerTurnId = idAllocator.derive.providerTurn({
          driver: CODEX_DRIVER,
          nativeTurnId: "native-turn-busy-during-check",
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        const runtime = yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        yield* runtime.events.pipe(Stream.runDrain, Effect.forkScoped);
        const messageId = yield* persistStartedRuntimeFixture({
          eventSink,
          idAllocator,
          threadId,
          providerThread,
          runId,
          attemptId,
          rootNodeId,
          now,
        });
        const appThread = (yield* projectionStore.getThreadProjection(threadId)).thread;

        yield* TestClock.adjust("1 second");
        yield* Deferred.await(checkEntered);

        // The release fiber is parked inside the pending-work check, so the
        // idle decision it already made is stale once this turn marks the
        // session busy.
        const turnFiber = yield* runtime
          .startTurn({
            appThread,
            threadId,
            runId,
            runOrdinal: 1,
            providerTurnOrdinal: 1,
            attemptId,
            rootNodeId,
            providerThread,
            message: {
              createdBy: "user",
              creationSource: "web",
              messageId,
              text: "hello",
              attachments: [],
            },
            modelSelection,
            runtimePolicy,
          })
          .pipe(Effect.forkDetach);
        for (let i = 0; i < 10; i += 1) {
          yield* Effect.yieldNow;
        }
        yield* Deferred.succeed(checkGate, undefined);
        yield* Fiber.join(turnFiber);
        yield* Effect.yieldNow;

        assert.isTrue(Option.isSome(yield* manager.get(providerSessionId)));
        assert.equal((yield* Ref.get(state)).closeCount, 0);

        const queue = (yield* Ref.get(state)).eventQueues.get(String(providerSessionId));
        assert.isDefined(queue);
        yield* Queue.offer(queue!, {
          type: "turn.terminal",
          driver: CODEX_DRIVER,
          providerThreadId: providerThread.id,
          providerTurnId,
          runOrdinal: 1,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* TestClock.adjust("1 second");
        yield* Effect.yieldNow;
        assert.isTrue(Option.isNone(yield* manager.get(providerSessionId)));
        assert.equal((yield* Ref.get(state)).closeCount, 1);
      });

      yield* effect.pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 1000,
            // Uninterruptible so the markBusy-triggered interrupt cannot land
            // inside the check, mirroring an adapter that masks interruption
            // while inspecting its own state.
            hasPendingBackgroundWork: Effect.uninterruptible(
              Effect.gen(function* () {
                if (yield* Ref.getAndSet(firstCheck, false)) {
                  yield* Deferred.succeed(checkEntered, undefined);
                  yield* Deferred.await(checkGate);
                }
                return false;
              }),
            ),
          }),
        ),
      );
    }),
);

it.effect("ProviderSessionManagerV2 does not apply a stale idle pin to a replacement session", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const firstCheck = yield* Ref.make(true);
    const checkEntered = yield* Deferred.make<void>();
    const checkGate = yield* Deferred.make<void>();
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-stale-pin",
        projectId: yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-stale-pin",
        }),
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });

      // Park the first idle fiber inside an uninterruptible pending-work probe.
      yield* TestClock.adjust("1 second");
      yield* Deferred.await(checkEntered);

      // close removes the map entry first, then waits to interrupt the idle
      // fiber (still uninterruptible). That window lets a replacement open
      // under the same providerSessionId before the stale probe finishes.
      const closeFiber = yield* manager.close(providerSessionId).pipe(Effect.forkDetach);
      for (let i = 0; i < 20; i += 1) {
        yield* Effect.yieldNow;
      }
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      assert.equal((yield* Ref.get(state)).openCount, 2);

      // Stale probe reports pending work against the old runtime; the pin
      // stamp must no-op on the replacement (runtime / generation mismatch).
      yield* Deferred.succeed(checkGate, undefined);
      yield* Fiber.join(closeFiber);
      for (let i = 0; i < 10; i += 1) {
        yield* Effect.yieldNow;
      }

      assert.isTrue(Option.isSome(yield* manager.get(providerSessionId)));
      assert.equal((yield* Ref.get(state)).closeCount, 1);

      // Replacement has no pending background work. After one idle window it
      // must release. A stale pin stamp would have deferred release until
      // maxIdlePinMs.
      yield* TestClock.adjust("1 second");
      yield* Effect.yieldNow;
      assert.isTrue(Option.isNone(yield* manager.get(providerSessionId)));
      assert.equal((yield* Ref.get(state)).closeCount, 2);
    });

    yield* effect.pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 1000,
          maxIdlePinMs: 60_000,
          hasPendingBackgroundWork: Effect.uninterruptible(
            Effect.gen(function* () {
              if (yield* Ref.getAndSet(firstCheck, false)) {
                yield* Deferred.succeed(checkEntered, undefined);
                yield* Deferred.await(checkGate);
                return true;
              }
              return false;
            }),
          ),
        }),
      ),
    );
  }),
);

it.effect(
  "ProviderSessionManagerV2 keeps active sessions alive until the provider turn terminates",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        const now = yield* DateTime.now;
        const projectId = yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-active",
        });
        const threadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-active",
          projectId,
        });
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const providerThread = makeProviderThread({
          idAllocator,
          threadId,
          providerSessionId,
          now,
        });
        const runId = idAllocator.derive.run({ threadId, ordinal: 1 });
        const attemptId = idAllocator.derive.runAttempt({ runId, attemptOrdinal: 1 });
        const rootNodeId = idAllocator.derive.rootNode({ runId });
        const providerTurnId = idAllocator.derive.providerTurn({
          driver: CODEX_DRIVER,
          nativeTurnId: "native-turn",
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        const runtime = yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        yield* runtime.events.pipe(Stream.runDrain, Effect.forkScoped);
        const messageId = yield* persistStartedRuntimeFixture({
          eventSink,
          idAllocator,
          threadId,
          providerThread,
          runId,
          attemptId,
          rootNodeId,
          now,
        });
        const appThread = (yield* projectionStore.getThreadProjection(threadId)).thread;
        yield* runtime.startTurn({
          appThread,
          threadId,
          runId,
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          attemptId,
          rootNodeId,
          providerThread,
          message: {
            createdBy: "user",
            creationSource: "web",
            messageId,
            text: "hello",
            attachments: [],
          },
          modelSelection,
          runtimePolicy,
        });

        yield* TestClock.adjust("2 seconds");
        yield* Effect.yieldNow;
        assert.equal((yield* Ref.get(state)).closeCount, 0);

        const queue = (yield* Ref.get(state)).eventQueues.get(String(providerSessionId));
        assert.isDefined(queue);
        yield* Queue.offer(queue!, {
          type: "turn.terminal",
          driver: CODEX_DRIVER,
          providerThreadId: providerThread.id,
          providerTurnId,
          runOrdinal: 1,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* TestClock.adjust("1 second");
        yield* Effect.yieldNow;

        const liveSession = yield* manager.get(providerSessionId);
        const projection = yield* projectionStore.getThreadProjection(threadId);
        assert.isTrue(Option.isNone(liveSession));
        assert.equal((yield* Ref.get(state)).closeCount, 1);
        assert.equal(projection.providerSessions.at(-1)?.status, "stopped");
      });

      yield* effect.pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1000 })));
    }),
);

it.effect("ProviderSessionManagerV2 uses the same release path for runtime failures", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const projectId = yield* idAllocator.allocate.project({
        fixtureName: "provider-session-manager-runtime-error",
      });
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-runtime-error",
        projectId,
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* manager.release({
        providerSessionId,
        reason: "runtime_error",
        detail: "process exited",
      });

      const liveSession = yield* manager.get(providerSessionId);
      const runtimeState = yield* Ref.get(state);
      const projection = yield* projectionStore.getThreadProjection(threadId);

      assert.isTrue(Option.isNone(liveSession));
      assert.equal(runtimeState.closeCount, 1);
      assert.equal(projection.providerSessions.at(-1)?.status, "error");
      assert.equal(projection.providerSessions.at(-1)?.lastError, "process exited");
    });

    yield* effect.pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1000 })));
  }),
);

it.effect("ProviderSessionManagerV2 releases sessions when provider event streams fail", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const projectId = yield* idAllocator.allocate.project({
        fixtureName: "provider-session-manager-stream-error",
      });
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-stream-error",
        projectId,
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      const runtime = yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* runtime.events.pipe(Stream.runDrain, Effect.ignore, Effect.forkScoped);
      yield* Effect.yieldNow;

      const liveSession = yield* manager.get(providerSessionId);
      const runtimeState = yield* Ref.get(state);
      const projection = yield* projectionStore.getThreadProjection(threadId);

      assert.isTrue(Option.isNone(liveSession));
      assert.equal(runtimeState.closeCount, 1);
      assert.equal(projection.providerSessions.at(-1)?.status, "error");
    });

    yield* effect.pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 1000,
          failEventStream: true,
        }),
      ),
    );
  }),
);

it.effect("ProviderSessionManagerV2 marks pending runtime requests non-live on release", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const projectId = yield* idAllocator.allocate.project({
        fixtureName: "provider-session-manager-request-expire",
      });
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-request-expire",
        projectId,
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const providerThread = makeProviderThread({
        idAllocator,
        threadId,
        providerSessionId,
        now,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      const pendingRequest = yield* makePendingRuntimeRequestEvents({
        idAllocator,
        threadId,
        providerSessionId,
        providerThread,
        now,
      });
      yield* eventSink.write({ events: pendingRequest.events });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* manager.release({
        providerSessionId,
        reason: "runtime_error",
        detail: "process exited",
      });

      const projection = yield* projectionStore.getThreadProjection(threadId);
      const request = projection.runtimeRequests.at(-1);
      const requestNode = projection.nodes.find((node) => node.id === request?.nodeId);
      const requestTurnItem = projection.turnItems.find(
        (item) => item.type === "approval_request" && item.requestId === request?.id,
      );

      assert.equal(request?.status, "expired");
      assert.equal(request?.responseCapability.type, "not_resumable");
      assert.equal(requestNode?.status, "failed");
      assert.equal(requestTurnItem?.status, "failed");
    });

    yield* effect.pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1000 })));
  }),
);
it.effect("ProviderSessionManagerV2 terminalizes a pending input transcript item on release", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const projectId = yield* idAllocator.allocate.project({
        fixtureName: "provider-session-manager-request-expire",
      });
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-request-expire",
        projectId,
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const providerThread = makeProviderThread({
        idAllocator,
        threadId,
        providerSessionId,
        now,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      const pendingRequest = yield* makePendingRuntimeRequestEvents({
        idAllocator,
        threadId,
        providerSessionId,
        providerThread,
        now,
      });
      yield* eventSink.write({
        events: pendingRequest.events.map((event) =>
          event.type === "turn-item.updated"
            ? { ...event, payload: { ...event.payload, type: "user_input_request", questions: [] } }
            : event,
        ),
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* manager.release({
        providerSessionId,
        reason: "runtime_error",
        detail: "process exited",
      });

      const projection = yield* projectionStore.getThreadProjection(threadId);
      const request = projection.runtimeRequests.at(-1);
      const requestNode = projection.nodes.find((node) => node.id === request?.nodeId);
      const requestTurnItem = projection.turnItems.find(
        (item) => item.type === "user_input_request" && item.requestId === request?.id,
      );

      assert.equal(request?.status, "expired");
      assert.equal(request?.responseCapability.type, "not_resumable");
      assert.equal(requestNode?.status, "failed");
      assert.equal(requestTurnItem?.status, "failed");
    });

    yield* effect.pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1000 })));
  }),
);

it.effect("ProviderSessionManagerV2 persists session-scoped runtime requests without a run", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const projectId = yield* idAllocator.allocate.project({
        fixtureName: "provider-session-manager-session-request",
      });
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-session-request",
        projectId,
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const providerThread = makeProviderThread({
        idAllocator,
        threadId,
        providerSessionId,
        now,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      const pendingRequest = yield* makePendingRuntimeRequestEvents({
        idAllocator,
        threadId,
        providerSessionId,
        providerThread,
        now,
      });
      const afterSequence = yield* eventSink.latestSequence({ threadId });
      const persistedFiber = yield* eventSink.stream({ threadId, afterSequence }).pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "runtime-request.updated" ||
            stored.event.type === "node.updated" ||
            stored.event.type === "turn-item.updated",
        ),
        Stream.take(3),
        Stream.runCollect,
        Effect.forkScoped,
      );
      const adapterEvents = (yield* Ref.get(state)).eventQueues.get(String(providerSessionId));
      assert.isDefined(adapterEvents);
      yield* Queue.offerAll(adapterEvents!, pendingRequest.providerEvents);
      const persisted = Array.from(yield* Fiber.join(persistedFiber));

      assert.sameMembers(
        persisted.map((stored) => stored.event.type),
        ["runtime-request.updated", "node.updated", "turn-item.updated"],
      );
      const projection = yield* projectionStore.getThreadProjection(threadId);
      const request = projection.runtimeRequests.find(
        (candidate) => candidate.id === pendingRequest.requestId,
      );
      const node = projection.nodes.find((candidate) => candidate.id === pendingRequest.nodeId);
      const turnItem = projection.turnItems.find(
        (candidate) =>
          candidate.type === "approval_request" && candidate.requestId === pendingRequest.requestId,
      );
      assert.equal(request?.status, "pending");
      assert.equal(request?.providerTurnId, null);
      assert.equal(node?.runId, null);
      assert.equal(node?.status, "waiting");
      assert.equal(turnItem?.runId, null);
      assert.equal(turnItem?.status, "waiting");
    });

    yield* effect.pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1000 })));
  }),
);

it.effect(
  "ProviderSessionManagerV2 preserves item identity during eager native session activation",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        const now = yield* DateTime.now;
        const projectId = yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-request-expire",
        });
        const threadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-request-expire",
          projectId,
        });
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const providerThread = makeProviderThread({
          idAllocator,
          threadId,
          providerSessionId,
          now,
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        yield* eventSink.write({
          events: (yield* makePendingRuntimeRequestEvents({
            idAllocator,
            threadId,
            providerSessionId,
            providerThread,
            now,
          })).events,
        });
        yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
          initialNativeThreadId: "native-import",
          initialProviderItemIdentityVersion: 2,
        });
        yield* manager.release({
          providerSessionId,
          reason: "runtime_error",
          detail: "process exited",
        });

        const projection = yield* projectionStore.getThreadProjection(threadId);
        const request = projection.runtimeRequests.at(-1);
        const requestNode = projection.nodes.find((node) => node.id === request?.nodeId);
        const requestTurnItem = projection.turnItems.find(
          (item) => item.type === "approval_request" && item.requestId === request?.id,
        );

        assert.equal(request?.status, "expired");
        assert.equal(request?.responseCapability.type, "not_resumable");
        assert.equal(requestNode?.status, "failed");
        assert.equal(requestTurnItem?.status, "failed");
      });

      yield* effect.pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 1000,
            beforeOpen: (input) =>
              Effect.sync(() => assert.equal(input.initialProviderItemIdentityVersion, 2)),
          }),
        ),
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 keeps a multi-thread session alive until all turns finish",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        const now = yield* DateTime.now;
        const projectId = yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-multi-thread-active",
        });
        const firstThreadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-multi-thread-active-a",
          projectId,
        });
        const secondThreadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-multi-thread-active-b",
          projectId,
        });
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId: firstThreadId,
        });
        const firstProviderThread = makeProviderThread({
          idAllocator,
          threadId: firstThreadId,
          providerSessionId,
          now,
        });
        const secondProviderThread: OrchestrationV2ProviderThread = {
          ...makeProviderThread({ idAllocator, threadId: secondThreadId, providerSessionId, now }),
          id: idAllocator.derive.providerThread({
            driver: CODEX_DRIVER,
            nativeThreadId: "native-thread-b",
          }),
          nativeThreadRef: {
            driver: CODEX_DRIVER,
            nativeId: "native-thread-b",
            strength: "strong",
          },
        };
        const firstRunId = idAllocator.derive.run({ threadId: firstThreadId, ordinal: 1 });
        const secondRunId = idAllocator.derive.run({ threadId: secondThreadId, ordinal: 1 });
        const firstProviderTurnId = idAllocator.derive.providerTurn({
          driver: CODEX_DRIVER,
          nativeTurnId: "native-turn-a",
        });
        const secondProviderTurnId = idAllocator.derive.providerTurn({
          driver: CODEX_DRIVER,
          nativeTurnId: "native-turn-b",
        });

        yield* eventSink.write({
          events: [
            yield* makeThreadCreatedEvent({ idAllocator, threadId: firstThreadId, now }),
            yield* makeThreadCreatedEvent({ idAllocator, threadId: secondThreadId, now }).pipe(
              Effect.map((event) => ({
                ...event,
                payload: { ...event.payload, activeProviderThreadId: secondProviderThread.id },
              })),
            ),
          ],
        });
        const runtime = yield* manager.open({
          threadId: firstThreadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        yield* manager.open({
          threadId: secondThreadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        yield* runtime.events.pipe(Stream.runDrain, Effect.forkScoped);
        const firstMessageId = yield* persistStartedRuntimeFixture({
          eventSink,
          idAllocator,
          threadId: firstThreadId,
          providerThread: firstProviderThread,
          runId: firstRunId,
          attemptId: idAllocator.derive.runAttempt({ runId: firstRunId, attemptOrdinal: 1 }),
          rootNodeId: idAllocator.derive.rootNode({ runId: firstRunId }),
          now,
        });
        const secondMessageId = yield* persistStartedRuntimeFixture({
          eventSink,
          idAllocator,
          threadId: secondThreadId,
          providerThread: secondProviderThread,
          runId: secondRunId,
          attemptId: idAllocator.derive.runAttempt({ runId: secondRunId, attemptOrdinal: 1 }),
          rootNodeId: idAllocator.derive.rootNode({ runId: secondRunId }),
          now,
        });
        const firstAppThread = (yield* projectionStore.getThreadProjection(firstThreadId)).thread;
        const secondAppThread = (yield* projectionStore.getThreadProjection(secondThreadId)).thread;
        yield* runtime.startTurn({
          appThread: firstAppThread,
          threadId: firstThreadId,
          runId: firstRunId,
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          attemptId: idAllocator.derive.runAttempt({ runId: firstRunId, attemptOrdinal: 1 }),
          rootNodeId: idAllocator.derive.rootNode({ runId: firstRunId }),
          providerThread: firstProviderThread,
          message: {
            createdBy: "user",
            creationSource: "web",
            messageId: firstMessageId,
            text: "first",
            attachments: [],
          },
          modelSelection,
          runtimePolicy,
        });
        yield* runtime.startTurn({
          appThread: secondAppThread,
          threadId: secondThreadId,
          runId: secondRunId,
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          attemptId: idAllocator.derive.runAttempt({ runId: secondRunId, attemptOrdinal: 1 }),
          rootNodeId: idAllocator.derive.rootNode({ runId: secondRunId }),
          providerThread: secondProviderThread,
          message: {
            createdBy: "user",
            creationSource: "web",
            messageId: secondMessageId,
            text: "second",
            attachments: [],
          },
          modelSelection,
          runtimePolicy,
        });

        const queue = (yield* Ref.get(state)).eventQueues.get(String(providerSessionId));
        assert.isDefined(queue);
        yield* Queue.offer(queue!, {
          type: "turn.terminal",
          driver: CODEX_DRIVER,
          providerThreadId: firstProviderThread.id,
          providerTurnId: firstProviderTurnId,
          runOrdinal: 1,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* TestClock.adjust("2 seconds");
        yield* Effect.yieldNow;
        assert.equal((yield* Ref.get(state)).closeCount, 0);

        yield* Queue.offer(queue!, {
          type: "turn.terminal",
          driver: CODEX_DRIVER,
          providerThreadId: secondProviderThread.id,
          providerTurnId: secondProviderTurnId,
          runOrdinal: 1,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* TestClock.adjust("1 second");
        yield* Effect.yieldNow;
        assert.equal((yield* Ref.get(state)).closeCount, 1);
      });

      yield* effect.pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1000 })));
    }),
);

it.effect(
  "ProviderSessionManagerV2 opens one shared runtime, broadcasts events, and detaches threads independently",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const now = yield* DateTime.now;
        const projectId = yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-shared-runtime",
        });
        const firstThreadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-shared-runtime-a",
          projectId,
        });
        const secondThreadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-shared-runtime-b",
          projectId,
        });
        const providerSessionId = idAllocator.derive.providerSession({
          providerInstanceId: modelSelection.instanceId,
        });

        yield* eventSink.write({
          events: [
            yield* makeThreadCreatedEvent({ idAllocator, threadId: firstThreadId, now }),
            yield* makeThreadCreatedEvent({ idAllocator, threadId: secondThreadId, now }),
          ],
        });
        const firstProviderThread = makeProviderThread({
          idAllocator,
          threadId: firstThreadId,
          providerSessionId,
          now,
        });
        const secondProviderThread = makeProviderThread({
          idAllocator,
          threadId: secondThreadId,
          providerSessionId,
          now,
        });
        const firstRunId = idAllocator.derive.run({ threadId: firstThreadId, ordinal: 1 });
        yield* eventSink.write({
          events: [
            {
              id: yield* idAllocator.allocate.event({ threadId: firstThreadId }),
              type: "provider-thread.updated",
              threadId: firstThreadId,
              driver: CODEX_DRIVER,
              occurredAt: now,
              payload: firstProviderThread,
            },
            {
              id: yield* idAllocator.allocate.event({ threadId: firstThreadId }),
              type: "provider-turn.updated",
              threadId: firstThreadId,
              runId: firstRunId,
              driver: CODEX_DRIVER,
              occurredAt: now,
              payload: {
                id: idAllocator.derive.providerTurn({
                  driver: CODEX_DRIVER,
                  nativeTurnId: "native-turn-shared-runtime-a",
                }),
                providerThreadId: firstProviderThread.id,
                nodeId: idAllocator.derive.rootNode({ runId: firstRunId }),
                runAttemptId: null,
                nativeTurnRef: null,
                ordinal: 1,
                status: "running",
                startedAt: now,
                completedAt: null,
              },
            },
          ],
        });
        const firstRuntime = yield* manager.open({
          threadId: firstThreadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        const secondRuntime = yield* manager.open({
          threadId: secondThreadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });

        assert.strictEqual(firstRuntime, secondRuntime);
        assert.equal((yield* Ref.get(state)).openCount, 1);
        const resumeSecondThread = secondRuntime.resumeThread({
          providerThread: secondProviderThread,
          threadId: secondThreadId,
          modelSelection,
          runtimePolicy,
        });
        yield* resumeSecondThread;
        yield* resumeSecondThread;
        assert.equal((yield* Ref.get(state)).resumeCount, 1);
        yield* secondRuntime.resumeThread({
          providerThread: secondProviderThread,
          threadId: secondThreadId,
          modelSelection: { ...modelSelection, model: "gpt-5.4-mini" },
          runtimePolicy,
        });
        assert.equal((yield* Ref.get(state)).resumeCount, 2);
        yield* resumeSecondThread;
        assert.equal((yield* Ref.get(state)).resumeCount, 3);
        const subscribe = firstRuntime.subscribeEvents;
        assert.isDefined(subscribe);
        if (subscribe === undefined) return;
        const firstSubscription = yield* subscribe;
        const secondSubscription = yield* subscribe;
        const queue = (yield* Ref.get(state)).eventQueues.get(String(providerSessionId));
        assert.isDefined(queue);
        yield* Queue.offer(queue!, {
          type: "provider_session.updated",
          driver: CODEX_DRIVER,
          providerSession: firstRuntime.providerSession,
        });
        const received = yield* Effect.all([
          firstSubscription.events.pipe(Stream.runHead),
          secondSubscription.events.pipe(Stream.runHead),
        ]);
        assert.isTrue(received.every(Option.isSome));
        assert.isTrue(
          received.every(
            (event) => Option.isSome(event) && event.value.type === "provider_session.updated",
          ),
        );

        yield* manager.detach({ providerSessionId, threadId: secondThreadId });
        yield* manager.open({
          threadId: secondThreadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        yield* resumeSecondThread;
        assert.equal((yield* Ref.get(state)).resumeCount, 4);

        // The second thread has no persisted provider thread, so nothing is unloaded.
        assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, []);

        yield* manager.detach({ providerSessionId, threadId: firstThreadId });
        assert.isTrue(Option.isSome(yield* manager.get(providerSessionId)));
        assert.equal((yield* Ref.get(state)).closeCount, 0);
        assert.equal((yield* Ref.get(state)).interruptCount, 1);
        // The runtime stays up for the second thread; the first thread's
        // native state is unloaded after its turn is interrupted.
        assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, ["native-thread"]);

        yield* manager.detach({ providerSessionId, threadId: secondThreadId });
        yield* TestClock.adjust("1 second");
        yield* Effect.yieldNow;
        assert.equal((yield* Ref.get(state)).closeCount, 1);
      });

      yield* effect.pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1000 })));
    }),
);

it.effect(
  "ProviderSessionManagerV2 re-attaching a thread waits for its in-flight unload, then reloads it",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const unloadStarted = yield* Deferred.make<void>();
      const releaseUnload = yield* Deferred.make<void>();
      // Resumes the provider had served when the unload actually reached it.
      let resumesBeforeUnload: number | undefined;
      // The unload parks after detach removed the attachment, leaving the
      // window in which the same thread's next turn re-attaches it.
      const beforeUnload = Effect.gen(function* () {
        yield* Deferred.succeed(unloadStarted, undefined);
        yield* Deferred.await(releaseUnload);
        resumesBeforeUnload = (yield* Ref.get(state)).resumeCount;
      });
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const now = yield* DateTime.now;
        const projectId = yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-unload-race",
        });
        const threadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-unload-race-a",
          projectId,
        });
        const otherThreadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-unload-race-b",
          projectId,
        });
        const providerSessionId = idAllocator.derive.providerSession({
          providerInstanceId: modelSelection.instanceId,
        });
        const providerThread = makeProviderThread({
          idAllocator,
          threadId,
          providerSessionId,
          now,
        });
        yield* eventSink.write({
          events: [
            yield* makeThreadCreatedEvent({ idAllocator, threadId, now }),
            yield* makeThreadCreatedEvent({ idAllocator, threadId: otherThreadId, now }),
            {
              id: yield* idAllocator.allocate.event({ threadId }),
              type: "provider-thread.updated",
              threadId,
              driver: CODEX_DRIVER,
              occurredAt: now,
              payload: providerThread,
            },
          ],
        });
        const runtime = yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        // A second thread keeps the shared runtime up after the detach.
        yield* manager.open({
          threadId: otherThreadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        // Resuming re-attaches the thread to the shared runtime.
        const resume = runtime.resumeThread({
          providerThread,
          threadId,
          modelSelection,
          runtimePolicy,
        });
        yield* resume;

        const detach = yield* manager
          .detach({ providerSessionId, threadId })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(unloadStarted);
        // The same thread's next turn re-attaches while the unload is parked.
        // Give it room to run: unfixed, it reaches the provider's resume
        // here; serialized, it waits for the unload.
        const reattach = yield* resume.pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        yield* Effect.yieldNow;
        yield* Deferred.succeed(releaseUnload, undefined);
        yield* Fiber.join(detach);
        yield* Fiber.join(reattach);

        // The unload reached the provider before the re-attached resume, so
        // that resume reloads the thread instead of being torn down after it.
        assert.equal(resumesBeforeUnload, 1);
        assert.equal((yield* Ref.get(state)).resumeCount, 2);
        assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, ["native-thread"]);
        assert.isTrue(Option.isSome(yield* manager.get(providerSessionId)));
      });

      yield* effect.pipe(
        Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1000, beforeUnload })),
        Effect.scoped,
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 rejects a second thread when the provider runtime is exclusive",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const now = yield* DateTime.now;
        const projectId = yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-exclusive-runtime",
        });
        const firstThreadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-exclusive-runtime-a",
          projectId,
        });
        const secondThreadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-exclusive-runtime-b",
          projectId,
        });
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId: firstThreadId,
        });
        yield* eventSink.write({
          events: [
            yield* makeThreadCreatedEvent({ idAllocator, threadId: firstThreadId, now }),
            yield* makeThreadCreatedEvent({ idAllocator, threadId: secondThreadId, now }),
          ],
        });

        yield* manager.open({
          threadId: firstThreadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        const error = yield* manager
          .open({
            threadId: secondThreadId,
            providerSessionId,
            modelSelection,
            runtimePolicy,
          })
          .pipe(Effect.flip);

        assert.equal(error._tag, "ProviderSessionOpenError");
        assert.equal((yield* Ref.get(state)).openCount, 1);
      });

      yield* effect.pipe(
        Effect.provide(
          makeTestLayer({ state, idleTimeoutMs: 1000, capabilities: ExclusiveCapabilities }),
        ),
      );
    }),
);

for (const workspaceState of ["missing", "file"] as const) {
  it.effect(`rejects a ${workspaceState} workspace before opening a provider session`, () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const root = yield* fileSystem.makeTempDirectoryScoped();
      const cwd = `${root}/workspace`;
      if (workspaceState === "file") yield* fileSystem.writeFileString(cwd, "not a directory");
      const state = yield* Ref.make(emptyState);
      yield* Effect.gen(function* () {
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const eventSink = yield* EventSink.EventSinkV2;
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const threadId = ThreadId.make(`thread-${workspaceState}-workspace`);
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        yield* eventSink.write({
          events: [
            yield* makeThreadCreatedEvent({
              idAllocator,
              threadId,
              now: yield* DateTime.now,
            }),
          ],
        });
        const error = yield* manager
          .open({
            threadId,
            providerSessionId,
            modelSelection,
            runtimePolicy: { ...runtimePolicy, cwd },
          })
          .pipe(Effect.flip);
        assert.instanceOf(error, ProviderWorkspaceMissingError);
        assert.include(error.message, cwd);
        assert.include(error.message, "Restore the folder at this path before retrying.");
        assert.equal((yield* Ref.get(state)).openCount, 0);
        assert.isTrue(Option.isNone(yield* manager.get(providerSessionId)));
        assert.deepEqual(
          (yield* projectionStore.getThreadProjection(threadId)).providerSessions,
          [],
        );
      }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000 })));
    }).pipe(Effect.provide(NodeServices.layer)),
  );
}

it.effect(
  "rejects a deleted workspace before reusing a live session without changing its state",
  () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const root = yield* fileSystem.makeTempDirectoryScoped();
      const cwd = `${root}/workspace`;
      yield* fileSystem.makeDirectory(cwd);
      const state = yield* Ref.make(emptyState);
      yield* Effect.gen(function* () {
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const eventSink = yield* EventSink.EventSinkV2;
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const threadId = ThreadId.make("thread-deleted-live-workspace");
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        yield* eventSink.write({
          events: [
            yield* makeThreadCreatedEvent({
              idAllocator,
              threadId,
              now: yield* DateTime.now,
            }),
          ],
        });
        const input = {
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy: { ...runtimePolicy, cwd },
        };
        const runtime = yield* manager.open(input);
        const before = yield* projectionStore.getThreadProjection(threadId);
        yield* fileSystem.remove(cwd, { recursive: true });
        const error = yield* manager.open(input).pipe(Effect.flip);
        assert.instanceOf(error, ProviderWorkspaceMissingError);
        assert.equal((yield* Ref.get(state)).openCount, 1);
        assert.equal((yield* Ref.get(state)).closeCount, 0);
        assert.strictEqual(Option.getOrThrow(yield* manager.get(providerSessionId)), runtime);
        assert.deepEqual(
          (yield* projectionStore.getThreadProjection(threadId)).providerSessions,
          before.providerSessions,
        );
      }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000 })));
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "ProviderSessionManagerV2 applies project device access independently of browser access",
  () =>
    Effect.gen(function* () {
      const enabled = yield* runBrowserAccessScenario({
        enableAgentBrowserAccess: false,
        projectOverride: false,
        deviceOverride: true,
      });
      assert.isTrue(enabled?.capabilities?.has("device"));
      assert.isFalse(enabled?.browserToolsAvailable);
      const denied = yield* runBrowserAccessScenario({
        enableAgentBrowserAccess: false,
        projectOverride: false,
        deviceOverride: true,
        projectExists: false,
      });
      assert.isFalse(denied?.capabilities?.has("device"));
    }),
);

function legacyInventoryOwner(
  threadId: ThreadId,
  replacementBirth: LegacyLeaseOwnerV1["replacementBirth"] = null,
): LegacyLeaseOwnerV1 {
  return {
    originalBirth: {
      kind: "application_v1_thread_birth",
      threadId,
      eventId: EventId.make(`legacy-${threadId}`),
      sequence: 1,
      projectId: ProjectId.make("legacy-project"),
      createdAt: "2026-01-01T00:00:00.000Z",
    },
    importedBirth: null,
    replacementBirth,
  };
}

it.effect("legacy inventory retains pending MCP-disabled opens until resident insertion", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const entered = yield* Deferred.make<void>();
    const finish = yield* Deferred.make<void>();
    yield* Effect.gen(function* () {
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const ids = yield* IdAllocator.IdAllocatorV2;
      const threadId = ThreadId.make("legacy-pending-open");
      const sink = yield* EventSink.EventSinkV2;
      yield* sink.write({
        events: [
          yield* makeThreadCreatedEvent({ idAllocator: ids, threadId, now: yield* DateTime.now }),
        ],
      });
      const providerSessionId = yield* ids.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const fiber = yield* manager
        .open({ threadId, providerSessionId, modelSelection, runtimePolicy })
        .pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      const pending = yield* manager
        .withLegacyOwnerAbsent(legacyInventoryOwner(threadId), () => Effect.succeed("released"))
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(pending));
      yield* Deferred.succeed(finish, undefined);
      yield* Fiber.join(fiber);
      const resident = yield* manager
        .withLegacyOwnerAbsent(legacyInventoryOwner(threadId), () => Effect.succeed("released"))
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(resident));
      yield* manager.close(providerSessionId);
      assert.equal(
        yield* manager.withLegacyOwnerAbsent(legacyInventoryOwner(threadId), (revalidate) =>
          revalidate.pipe(Effect.as("released")),
        ),
        "released",
      );
    }).pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 60_000,
          configureMcp: false,
          beforeOpen: () =>
            Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(finish))),
        }),
      ),
    );
  }),
);

it.effect(
  "legacy inventory excludes only an admission-proved V2 replacement and retains imported ownership",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      yield* Effect.gen(function* () {
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const ids = yield* IdAllocator.IdAllocatorV2;
        const sink = yield* EventSink.EventSinkV2;
        const threadId = ThreadId.make("legacy-replacement");
        const now = yield* DateTime.now;
        yield* sink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator: ids, threadId, now })],
        });
        const birth = yield* sink.readApplicationBirthRecord(threadId);
        assert.isNotNull(birth);
        const providerSessionId = yield* ids.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const runtime = yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        const owner = legacyInventoryOwner(threadId, birth);
        assert.equal(
          yield* manager.withLegacyOwnerAbsent(owner, (revalidate) =>
            revalidate.pipe(Effect.as("released")),
          ),
          "released",
        );
        const imported = { ...owner, importedBirth: birth, replacementBirth: null };
        assert.isTrue(
          Exit.isFailure(
            yield* manager.withLegacyOwnerAbsent(imported, () => Effect.void).pipe(Effect.exit),
          ),
        );
        assert.strictEqual(Option.getOrThrow(yield* manager.get(providerSessionId)), runtime);
        assert.equal((yield* Ref.get(state)).closeCount, 0);
      }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000, configureMcp: false })));
    }),
);

it.effect(
  "legacy inventory never retroactively classifies an existing unclassified admission",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      yield* Effect.gen(function* () {
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const ids = yield* IdAllocator.IdAllocatorV2;
        const sink = yield* EventSink.EventSinkV2;
        const threadId = ThreadId.make("legacy-unclassified");
        const providerSessionId = yield* ids.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        yield* manager.open({ threadId, providerSessionId, modelSelection, runtimePolicy });
        yield* sink.write({
          events: [
            yield* makeThreadCreatedEvent({ idAllocator: ids, threadId, now: yield* DateTime.now }),
          ],
        });
        const owner = legacyInventoryOwner(
          threadId,
          yield* sink.readApplicationBirthRecord(threadId),
        );
        assert.isTrue(
          Exit.isFailure(
            yield* manager.withLegacyOwnerAbsent(owner, () => Effect.void).pipe(Effect.exit),
          ),
        );
      }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000, configureMcp: false })));
    }),
);

it.effect("legacy inventory reservation blocks new admission and expires its revalidation", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const opened = yield* Deferred.make<void>();
    yield* Effect.gen(function* () {
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const ids = yield* IdAllocator.IdAllocatorV2;
      const threadId = ThreadId.make("legacy-reserved");
      const providerSessionId = yield* ids.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      let captured: Effect.Effect<void, LegacyLeaseInventoryError> =
        Effect.die("missing reservation");
      const fiber = yield* manager.withLegacyOwnerAbsent(
        legacyInventoryOwner(threadId),
        (revalidate) =>
          Effect.gen(function* () {
            captured = revalidate;
            const started = yield* Deferred.make<void>();
            const pending = yield* Deferred.succeed(started, undefined).pipe(
              Effect.andThen(
                manager.open({ threadId, providerSessionId, modelSelection, runtimePolicy }),
              ),
              Effect.forkChild,
            );
            yield* Deferred.await(started);
            assert.isTrue(Option.isNone(yield* Deferred.poll(opened)));
            yield* revalidate;
            return pending;
          }),
      );
      yield* Fiber.join(fiber);
      assert.isTrue(Exit.isFailure(yield* captured.pipe(Effect.exit)));
      assert.equal((yield* Ref.get(state)).openCount, 1);
    }).pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 60_000,
          configureMcp: false,
          beforeOpen: () => Deferred.succeed(opened, undefined).pipe(Effect.asVoid),
        }),
      ),
    );
  }),
);

it.effect("legacy inventory retains a pending resume even for a proved replacement", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const entered = yield* Deferred.make<void>();
    const finish = yield* Deferred.make<void>();
    yield* Effect.gen(function* () {
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const ids = yield* IdAllocator.IdAllocatorV2;
      const sink = yield* EventSink.EventSinkV2;
      const threadId = ThreadId.make("legacy-pending-resume");
      const now = yield* DateTime.now;
      yield* sink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator: ids, threadId, now })],
      });
      const owner = legacyInventoryOwner(
        threadId,
        yield* sink.readApplicationBirthRecord(threadId),
      );
      const providerSessionId = yield* ids.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const runtime = yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      const providerThread = makeProviderThread({
        idAllocator: ids,
        threadId,
        providerSessionId,
        now,
      });
      const fiber = yield* runtime
        .resumeThread({ threadId, providerThread })
        .pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      assert.isTrue(
        Exit.isFailure(
          yield* manager.withLegacyOwnerAbsent(owner, () => Effect.void).pipe(Effect.exit),
        ),
      );
      yield* Deferred.succeed(finish, undefined);
      yield* Fiber.join(fiber);
      yield* manager.withLegacyOwnerAbsent(owner, (revalidate) => revalidate);
      assert.equal((yield* Ref.get(state)).resumeCount, 1);
    }).pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 60_000,
          configureMcp: false,
          beforeResume: Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Deferred.await(finish)),
          ),
        }),
      ),
    );
  }),
);

for (const outcome of ["known_no_effect", "unknown"] as const) {
  it.effect(`legacy inventory releases failed-open admission only with ${outcome} evidence`, () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      yield* Effect.gen(function* () {
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const ids = yield* IdAllocator.IdAllocatorV2;
        const threadId = ThreadId.make(`legacy-open-${outcome}`);
        const providerSessionId = yield* ids.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        assert.isTrue(
          Exit.isFailure(
            yield* manager
              .open({ threadId, providerSessionId, modelSelection, runtimePolicy })
              .pipe(Effect.exit),
          ),
        );
        const absence = yield* manager
          .withLegacyOwnerAbsent(legacyInventoryOwner(threadId), (revalidate) => revalidate)
          .pipe(Effect.exit);
        assert.equal(Exit.isSuccess(absence), outcome === "known_no_effect");
        assert.equal((yield* Ref.get(state)).openCount, 0);
      }).pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 60_000,
            configureMcp: false,
            beforeOpen: (input) =>
              Effect.fail(
                new ProviderAdapterOpenSessionError({
                  driver: CODEX_DRIVER,
                  providerSessionId: input.providerSessionId,
                  nativeEffect: { ...input.nativeOperation!, outcome },
                }),
              ),
          }),
        ),
      );
    }),
  );
}

it.effect("legacy inventory retains removed residency until scope close actually finishes", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const entered = yield* Deferred.make<void>();
    const finish = yield* Deferred.make<void>();
    yield* Effect.gen(function* () {
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const ids = yield* IdAllocator.IdAllocatorV2;
      const threadId = ThreadId.make("legacy-delayed-close");
      const sink = yield* EventSink.EventSinkV2;
      yield* sink.write({
        events: [
          yield* makeThreadCreatedEvent({ idAllocator: ids, threadId, now: yield* DateTime.now }),
        ],
      });
      const providerSessionId = yield* ids.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      yield* manager.open({ threadId, providerSessionId, modelSelection, runtimePolicy });
      const closing = yield* manager.close(providerSessionId).pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      assert.isTrue(Option.isNone(yield* manager.get(providerSessionId)));
      assert.isTrue(
        Exit.isFailure(
          yield* manager
            .withLegacyOwnerAbsent(legacyInventoryOwner(threadId), () => Effect.void)
            .pipe(Effect.exit),
        ),
      );
      yield* Deferred.succeed(finish, undefined);
      yield* Fiber.join(closing);
      yield* manager.withLegacyOwnerAbsent(
        legacyInventoryOwner(threadId),
        (revalidate) => revalidate,
      );
    }).pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 60_000,
          configureMcp: false,
          afterScopeClose: Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Deferred.await(finish)),
          ),
        }),
      ),
    );
  }),
);
