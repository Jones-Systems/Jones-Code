import {
  OrchestrationV2ProviderThread,
  RequestedRuntimeIdentity,
  ObservedRuntimeIdentity,
  ProviderRuntimeBinding,
  type EventId,
  type ProviderInstanceId,
  type ProviderSessionId,
  type ThreadId,
} from "@t3tools/contracts";
import {
  ProviderRuntimeBindingError,
  runtimeBinding,
  unobservedRuntimeIdentity,
  type CapturedRuntimeBindingPublication,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2Error,
  type ProviderAdapterV2SessionRuntime,
  type RuntimeBindingPublicationInput,
} from "@t3tools/provider-core/server/ProviderAdapter";
import * as Origin from "@t3tools/provider-core/server/jones/ProviderEventOrigin";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Semaphore from "effect/Semaphore";
import type * as Sink from "../../orchestration-v2/EventSink.ts";
import type * as Store from "../../orchestration-v2/EventStore.ts";
import type * as Ingestor from "../../orchestration-v2/ProviderEventIngestor.ts";
import type * as Physical from "../runtime/PhysicalRuntimeEvidence.ts";

export interface PublicationReservation {
  readonly previous: Map<string, string | null>;
  readonly bindings: Map<string, ThreadId>;
  readonly invalidated: Set<string>;
}

const publicationKey = Schema.encodeSync(Schema.fromJsonString(Schema.Tuple([Schema.String, Schema.String])));
const equivalentBinding = Schema.toEquivalence(ProviderRuntimeBinding);
const equivalentThread = Schema.toEquivalence(OrchestrationV2ProviderThread);
const equivalentRequested = Schema.toEquivalence(RequestedRuntimeIdentity);
const equivalentObserved = Schema.toEquivalence(ObservedRuntimeIdentity);
const sameRequested = (
  left: RequestedRuntimeIdentity | undefined,
  right: RequestedRuntimeIdentity,
) => left !== undefined && equivalentRequested(left, right);
const sameObserved = (left: ObservedRuntimeIdentity | undefined, right: ObservedRuntimeIdentity) =>
  left !== undefined && equivalentObserved(left, right);

export function makeProviderRecoveryPublication(ports: {
  readonly driver: ProviderAdapterV2SessionRuntime["driver"];
  readonly instanceId: ProviderInstanceId;
  readonly providerSessionId: ProviderSessionId;
  readonly reservations: Map<string, PublicationReservation>;
  readonly allocate: (threadId: ThreadId) => Effect.Effect<EventId, ProviderAdapterV2Error>;
  readonly readThread: (threadId: ThreadId, id: string) => Effect.Effect<OrchestrationV2ProviderThread | undefined, ProviderAdapterV2Error>;
  readonly events: Pick<Store.EventStoreV2Shape, "read">;
  readonly sink: Pick<Sink.EventSinkV2Shape, "write">;
  readonly ingestor: Pick<Ingestor.ProviderEventIngestorV2Shape, "ingestNormalized">;
  readonly physical: Pick<Effect.Success<typeof Physical.make>, "bind">;
  readonly forgetTransfers: (threadId: ThreadId) => Effect.Effect<void>;
  readonly revalidateGuard: (threadId: ThreadId) => Effect.Effect<void, ProviderAdapterV2Error>;
  readonly bindGuard: (binding: ProviderRuntimeBinding) => Effect.Effect<void, ProviderAdapterV2Error>;
}) {
  const retained = new Map<
    string,
    Array<{
      readonly input: RuntimeBindingPublicationInput;
      readonly handle: CapturedRuntimeBindingPublication;
      completed: boolean;
    }>
  >();
  const failure = (cause: unknown) =>
    new ProviderRuntimeBindingError({
      driver: ports.driver,
      detail:
        "Failed to establish the actual provider runtime binding; native effects must not be replayed.",
      cause,
    });
  const preparation = Semaphore.makeUnsafe(1);
  const prepare = Effect.fn("providerRecovery.preparePublication")(function* (input: RuntimeBindingPublicationInput) {
    const key = publicationKey([input.runtimeGeneration, input.providerThread.id]);
    const operations = retained.get(key) ?? [];
    const existing = operations.find((operation) =>
      equivalentThread(operation.input.providerThread, input.providerThread) &&
      sameRequested(operation.input.requested, input.requested) &&
      sameObserved(operation.input.observed, input.observed) &&
      operation.input.producerOrigin === input.producerOrigin &&
      operation.input.physicalProcess === input.physicalProcess);
    if (existing !== undefined) return existing.handle;
    if (operations.at(-1)?.completed === false)
      return yield* failure("The unresolved original publication input changed.");
    const reservation = ports.reservations.get(input.runtimeGeneration);
    const thread = input.providerThread;
    const binding = runtimeBinding(thread, input.runtimeGeneration);
    if (
      reservation === undefined || binding === undefined ||
      binding.driver !== ports.driver || thread.nativeThreadRef?.driver !== ports.driver ||
      binding.providerInstanceId !== ports.instanceId ||
      binding.providerSessionId !== ports.providerSessionId ||
      input.requested.providerInstanceId !== ports.instanceId ||
      input.requested.providerDriver !== ports.driver
    ) return yield* failure("The launch reservation or native ownership was lost.");
    const expectedGeneration = reservation.bindings.has(thread.id)
      ? input.runtimeGeneration : (reservation.previous.get(thread.id) ?? null);
    if (reservation.invalidated.has(thread.id))
      return yield* failure("Another producer already owns this native conversation.");
    const predecessor = yield* ports.readThread(binding.threadId, thread.id);
    if ((predecessor?.runtimeIdentity?.runtimeGeneration ?? null) !== expectedGeneration)
      return yield* failure("The original predecessor was not admitted for this binding.");
    const boundaryId = yield* ports.allocate(binding.threadId);
    const observationId = yield* ports.allocate(binding.threadId);
    const boundaryThread: OrchestrationV2ProviderThread = {
      ...thread,
      runtimeIdentity: {
        runtimeGeneration: input.runtimeGeneration,
        requested: input.requested,
        observed: input.observed,
      };
      if (input.producerOrigin !== undefined)
        Origin.stampProviderEvent(observedEvent, { producer: input.producerOrigin });
      const revalidate = Effect.gen(function* () {
        if (
          retained.get(key)?.at(-1)?.handle !== handle ||
          ports.reservations.get(input.runtimeGeneration) !== reservation ||
          reservation.invalidated.has(thread.id)
        )
          return yield* failure(
            "The original launch reservation no longer owns this conversation.",
          );
        yield* ports.revalidateGuard(binding.threadId);
        yield* Origin.revalidateProviderEventOrigin(observedEvent, {
          driver: ports.driver,
          instanceId: ports.instanceId,
          providerSessionId: ports.providerSessionId,
        });
      });
    });
    const readEvent = (eventId: EventId) => ports.events.read({
      eventId, threadId: binding.threadId, eventType: "provider-thread.updated", limit: 1,
    }).pipe(Stream.runCollect, Effect.map((events) => events[0]?.event));
    const assertCurrent = Effect.gen(function* () {
      yield* revalidate;
      const current = yield* ports.readThread(binding.threadId, thread.id);
      const currentBinding = current === undefined ? undefined : runtimeBinding(current, input.runtimeGeneration);
      if (current?.runtimeIdentity?.runtimeGeneration !== input.runtimeGeneration ||
          current.nativeThreadRef?.driver !== ports.driver ||
          currentBinding === undefined || !equivalentBinding(binding, currentBinding) ||
          !sameRequested(current.runtimeIdentity.requested, input.requested))
        return yield* failure("Another producer already owns this native conversation.");
      return current;
    });
    const sameBoundary = (candidate: OrchestrationV2ProviderThread) =>
      candidate.runtimeIdentity?.runtimeGeneration === input.runtimeGeneration &&
      sameRequested(candidate.runtimeIdentity.requested, input.requested) &&
      sameObserved(candidate.runtimeIdentity.observed, boundaryThread.runtimeIdentity!.observed) &&
      equivalentThread({ ...candidate, runtimeIdentity: boundaryThread.runtimeIdentity }, boundaryThread);
    let inFlight: Deferred.Deferred<Exit.Exit<OrchestrationV2ProviderThread, ProviderRuntimeBindingError>> | undefined;
    const attempt: Effect.Effect<OrchestrationV2ProviderThread, ProviderRuntimeBindingError> = Effect.gen(function* () {
      yield* revalidate;
      const committedBoundary = yield* readEvent(boundaryId);
      if (committedBoundary === undefined) {
        const current = yield* ports.readThread(binding.threadId, thread.id);
        if ((current?.runtimeIdentity?.runtimeGeneration ?? null) !== expectedGeneration)
          return yield* failure("The original predecessor changed before binding publication.");
        yield* revalidate;
        const written = yield* ports.sink.write({
          runtimeIdentityBoundary: { expectedGeneration }, events: [boundary],
        });
        if (written.length === 0) return yield* failure("The native binding boundary did not commit.");
      } else if (committedBoundary.type !== "provider-thread.updated" ||
                 !sameBoundary(committedBoundary.payload)) {
        return yield* failure("The retained boundary event does not match its original publication.");
      }
      yield* assertCurrent;
      yield* ports.forgetTransfers(binding.threadId);
      reservation.bindings.set(thread.id, binding.threadId);
      const committedObservation = yield* readEvent(observationId);
      if (committedObservation === undefined) {
        yield* assertCurrent;
        yield* ports.ingestor.ingestNormalized({
          providerSessionId: ports.providerSessionId, providerInstanceId: ports.instanceId,
          threadId: binding.threadId, event: observedEvent, runtimeIdentityEventId: observationId,
        });
      } else {
        const identity = committedObservation.type === "provider-thread.updated"
          ? committedObservation.payload : undefined;
        const observedBinding = identity === undefined ? undefined : runtimeBinding(identity, input.runtimeGeneration);
        if (identity?.runtimeIdentity?.runtimeGeneration !== input.runtimeGeneration ||
            observedBinding === undefined || !equivalentBinding(binding, observedBinding) ||
            !sameRequested(identity?.runtimeIdentity?.requested, input.requested) ||
            !sameObserved(identity?.runtimeIdentity?.observed, input.observed))
          return yield* failure("The retained observation event does not match its original publication.");
      }
      const committed = yield* assertCurrent;
      if (!sameObserved(committed.runtimeIdentity?.observed, input.observed))
        return yield* failure("The runtime identity observation did not commit.");
      yield* ports.bindGuard(binding);
      yield* assertCurrent;
      yield* ports.physical.bind(binding, input.physicalProcess);
      const result = yield* assertCurrent;
      const retainedEntry = retained.get(key)?.at(-1);
      if (retainedEntry?.handle === handle) retainedEntry.completed = true;
      return result;
    }).pipe(Effect.mapError(failure));
    const complete: Effect.Effect<OrchestrationV2ProviderThread, ProviderRuntimeBindingError> = Effect.uninterruptibleMask((restore) => Effect.gen(function* () {
      if (inFlight === undefined) {
        const completion = Deferred.makeUnsafe<Exit.Exit<OrchestrationV2ProviderThread, ProviderRuntimeBindingError>>();
        inFlight = completion;
        yield* attempt.pipe(
          Effect.exit,
          Effect.flatMap((result) => Effect.sync(() => { inFlight = undefined; }).pipe(
            Effect.andThen(Deferred.succeed(completion, result)),
          )),
          Effect.forkDetach({ startImmediately: true }),
        );
      const handle: CapturedRuntimeBindingPublication = { binding, complete };
      retained.set(key, [...operations, { input, handle, completed: false }]);
      return handle;
    },
    Effect.mapError(failure),
    preparation.withPermits(1),
  );
  return {
    prepare,
    bind: (input: RuntimeBindingPublicationInput) =>
      prepare(input).pipe(Effect.flatMap((publication) => publication.complete)),
  };
}
