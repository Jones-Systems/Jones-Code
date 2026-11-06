import { assert, it } from "@effect/vitest";
import {
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2StoredEvent,
} from "@t3tools/contracts";
import {
  ProviderRuntimeBindingError,
  requestedRuntimeIdentity,
  unobservedRuntimeIdentity,
} from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import {
  makeProviderRecoveryPublication,
  type PublicationReservation,
} from "./providerRecoveryPublication.ts";

it.effect(
  "concurrent completion joins one attempt and recovers guard and physical registration from the original events",
  () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const guardEntered = yield* Deferred.make<void>();
      const guardRelease = yield* Deferred.make<void>();
      const threadId = ThreadId.make("publication-thread");
      const instanceId = ProviderInstanceId.make("codex");
      const providerSessionId = ProviderSessionId.make("publication-session");
      const driver = ProviderDriverKind.make("codex");
      const requested = requestedRuntimeIdentity({ instanceId, model: "synthetic" }, driver);
      const providerThread: OrchestrationV2ProviderThread = {
        id: ProviderThreadId.make("publication-provider"),
        driver,
        providerInstanceId: instanceId,
        providerSessionId,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: { driver, nativeId: "native", strength: "strong" },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: null,
        lastRunOrdinal: null,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      };
      let current: OrchestrationV2ProviderThread | undefined;
      const stored: Array<OrchestrationV2StoredEvent> = [];
      let allocated = 0;
      let observationWrites = 0;
      let guardCalls = 0;
      let physicalCalls = 0;
      const reservation: PublicationReservation = {
        previous: new Map(),
        bindings: new Map(),
        invalidated: new Set(),
      };
      const owner = makeProviderRecoveryPublication({
        driver,
        instanceId,
        providerSessionId,
        reservations: new Map([["generation", reservation]]),
        allocate: () => Effect.sync(() => EventId.make(`publication-event:${++allocated}`)),
        readThread: () => Effect.sync(() => current),
        events: {
          read: (input) =>
            Stream.fromIterable(
              stored.filter(
                (row) =>
                  row.event.id === input?.eventId &&
                  row.event.threadId === input?.threadId &&
                  row.event.type === input?.eventType,
              ),
            ),
        },
        sink: {
          write: (input) =>
            Effect.sync(() => {
              const rows = input.events.map((event) => ({
                event,
                sequence: stored.length + 1,
                commandId: null,
              }));
              stored.push(...rows);
              const update = input.events.find((event) => event.type === "provider-thread.updated");
              if (update?.type === "provider-thread.updated") current = update.payload;
              return rows;
            }),
        },
        ingestor: {
          ingestNormalized: (input) =>
            Effect.sync(() => {
              assert.equal(input.event.type, "runtime_identity.observed");
              if (input.event.type !== "runtime_identity.observed" || current === undefined)
                return [];
              observationWrites++;
              current = {
                ...current,
                runtimeIdentity: {
                  runtimeGeneration: "generation",
                  requested: input.event.requested,
                  observed: input.event.observed,
                },
              };
              const row: OrchestrationV2StoredEvent = {
                sequence: stored.length + 1,
                commandId: null,
                event: {
                  id: input.runtimeIdentityEventId!,
                  type: "provider-thread.updated",
                  threadId,
                  occurredAt: now,
                  payload: current,
                },
              };
              stored.push(row);
              return [row];
            }),
        },
        revalidateGuard: () => Effect.void,
        forgetTransfers: () => Effect.void,
        bindGuard: () =>
          Effect.gen(function* () {
            guardCalls++;
            if (guardCalls === 1) {
              yield* Deferred.succeed(guardEntered, undefined);
              yield* Deferred.await(guardRelease);
              return yield* new ProviderRuntimeBindingError({
                driver,
                detail: "guard observation interrupted",
              });
            }
          }),
        physical: {
          bind: () =>
            Effect.sync(() => {
              physicalCalls++;
              if (physicalCalls === 1) throw new Error("physical registration interrupted");
            }),
        },
      });
      const input = {
        providerThread,
        runtimeGeneration: "generation",
        requested,
        observed: unobservedRuntimeIdentity(),
      };
      const publication = yield* owner.prepare(input);
      assert.isNull(yield* owner.readCompleted(publication));
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(owner.readCompleted({ ...publication }))),
      );
      const first = yield* publication.complete.pipe(Effect.exit, Effect.forkChild);
      yield* Deferred.await(guardEntered);
      const joined = yield* publication.complete.pipe(Effect.exit, Effect.forkChild);
      yield* Effect.yieldNow;
      assert.strictEqual(yield* owner.prepare(input), publication);
      yield* Deferred.succeed(guardRelease, undefined);
      assert.isTrue(Exit.isFailure(yield* Fiber.join(first)));
      assert.isTrue(Exit.isFailure(yield* Fiber.join(joined)));
      assert.equal(guardCalls, 1);
      assert.isNull(yield* owner.readCompleted(publication));
      const originalBoundary = stored[0]!;
      if (originalBoundary.event.type !== "provider-thread.updated")
        return yield* Effect.die("The original binding boundary is missing.");
      stored[0] = {
        ...originalBoundary,
        event: {
          ...originalBoundary.event,
          payload: { ...originalBoundary.event.payload, status: "error" },
        },
      };
      assert.isTrue(Exit.isFailure(yield* Effect.exit(publication.complete)));
      assert.equal(guardCalls, 1);
      assert.equal(physicalCalls, 0);
      stored[0] = originalBoundary;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(publication.complete)));
      yield* publication.complete;
      assert.equal(allocated, 2);
      assert.equal(stored.length, 2);
      assert.equal(observationWrites, 1);
      assert.equal(physicalCalls, 2);
      const completed = current!;
      assert.strictEqual(yield* owner.readCompleted(publication), completed);
      assert.strictEqual(yield* owner.readCompleted(publication), completed);
      assert.equal(guardCalls, 3);
      assert.equal(physicalCalls, 2);
      assert.equal(observationWrites, 1);
      assert.equal(stored.length, 2);
      for (const changed of [
        { ...completed, nativeThreadRef: { ...completed.nativeThreadRef!, nativeId: "other" } },
        {
          ...completed,
          runtimeIdentity: { ...completed.runtimeIdentity!, runtimeGeneration: "other" },
        },
        {
          ...completed,
          runtimeIdentity: {
            ...completed.runtimeIdentity!,
            requested: { ...requested, model: "other" },
          },
        },
        {
          ...completed,
          runtimeIdentity: {
            ...completed.runtimeIdentity!,
            observed: {
              ...completed.runtimeIdentity!.observed,
              model: { status: "observed" as const, value: "other", sourceEvent: "other" },
            },
          },
        },
      ]) {
        current = changed;
        assert.isTrue(Exit.isFailure(yield* Effect.exit(owner.readCompleted(publication))));
      }
      current = completed;
      reservation.invalidated.add(providerThread.id);
      assert.isTrue(Exit.isFailure(yield* Effect.exit(owner.readCompleted(publication))));
      reservation.invalidated.delete(providerThread.id);
      const successor = yield* owner.prepare({
        ...input,
        requested: { ...requested, model: "successor" },
      });
      assert.notStrictEqual(successor, publication);
      assert.isNull(yield* owner.readCompleted(successor));
      assert.isTrue(Exit.isFailure(yield* Effect.exit(owner.readCompleted(publication))));
      assert.strictEqual(yield* owner.prepare(input), publication);
      assert.isTrue(Exit.isFailure(yield* Effect.exit(publication.complete)));
      assert.equal(physicalCalls, 2);
    }),
);
