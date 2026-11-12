import { assert, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderTurnId,
  ThreadId,
  type ProviderRuntimeEvidenceCapture,
} from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { makeProviderControlTerminalDrain } from "@t3tools/provider-core/server/jones/providerControlOutcome";
import { retryAdmissionUnavailable } from "./AcceptedTerminalRecovery.ts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as SqlError from "effect/sql/SqlError";
import { SqliteReaderAdmissionError } from "@t3tools/shared/jones/sqliteReaders/admissionFailure";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProviderEventIngestor from "../../orchestration-v2/ProviderEventIngestor.ts";
import type {
  captureRootRunTermination,
  rootTerminalEvent,
} from "../../orchestration-v2/RunExecutionService.test.ts";

const admission = () => new SqliteReaderAdmissionError({ reason: "queue-timeout" });
const busyWrite = () =>
  new EventSink.EventSinkWriteError({
    eventCount: 1,
    cause: new SqlError.SqlError({ reason: new SqlError.LockTimeoutError({ cause: null }) }),
  });

// The upstream harness owns its consumer lifetime; registration avoids copying it.
export function registerAcceptedTerminalRecoveryTests(
  capture: typeof captureRootRunTermination,
  terminalEvent: typeof rootTerminalEvent,
) {
  it.effect.each([
    { driver: "codex", status: "completed", seam: "ownership" },
    { driver: "claudeCode", status: "completed", seam: "pending-input" },
    { driver: "codex", status: "completed", seam: "write" },
    { driver: "claudeCode", status: "failed", seam: "write" },
  ] as const)("accepted $driver $status terminal survives one failed $seam operation", (scenario) =>
    Effect.gen(function* () {
      const driver = ProviderDriverKind.make(scenario.driver);
      const completedAt = DateTime.makeUnsafe("2026-11-11T23:00:00Z");
      const drain = makeProviderControlTerminalDrain();
      let evidence: ProviderRuntimeEvidenceCapture | undefined;
      const reads = yield* Ref.make(0);
      const ingests = yield* Ref.make(0);
      const writes = yield* Ref.make(0);
      const interrupts = yield* Ref.make(0);
      const result = yield* capture({
        key: `accepted:${scenario.driver}:${scenario.status}:${scenario.seam}`,
        driver,
        shouldFinalizeRun: () =>
          Effect.gen(function* () {
            if (
              (yield* Ref.updateAndGet(reads, (n) => n + 1)) === 1 &&
              scenario.seam === "ownership"
            )
              return yield* new ProjectionStore.ProjectionStoreReadError({
                threadId: ThreadId.make("thread:accepted"),
                cause: admission(),
              });
            return true;
          }),
        events: (ids) => {
          evidence = {
            threadId: ids.threadId,
            providerThreadId: ids.providerThreadId,
            providerSessionId: ProviderSessionId.make(
              `session:accepted:${scenario.driver}:${scenario.status}:${scenario.seam}`,
            ),
            providerInstanceId: ProviderInstanceId.make("codex"),
            driver,
            nativeThreadId: "original-native-thread",
            runtimeGeneration: "original-generation",
            evidenceRevision: 1,
          };
          const event = {
            ...terminalEvent(ids, scenario.status),
            runtimeEvidence: evidence,
            driver,
            providerTurn: {
              id: ids.rootProviderTurnId,
              providerThreadId: ids.providerThreadId,
              nodeId: ids.rootNodeId,
              runAttemptId: ids.attemptId,
              nativeTurnRef:
                scenario.driver === "codex"
                  ? { driver, nativeId: "original-native-turn", strength: "strong" }
                  : null,
              ordinal: 1,
              status: scenario.status,
              startedAt: DateTime.subtract(completedAt, { seconds: 10 }),
              completedAt,
            },
          } satisfies ProviderAdapter.ProviderAdapterV2Event;
          drain.capture(event);
          return Stream.make(event);
        },
        ingestNormalized: (input) =>
          Effect.gen(function* () {
            if (
              (yield* Ref.updateAndGet(ingests, (n) => n + 1)) === 1 &&
              scenario.seam === "pending-input"
            )
              return yield* new ProviderEventIngestor.ProviderEventNormalizeError({
                providerSessionId: input.providerSessionId,
                threadId: input.threadId,
                providerEvent: input.event,
                cause: admission(),
              });
            return [];
          }),
        beforeTerminalWrite: (request) =>
          Effect.gen(function* () {
            assert.strictEqual(request.runtimeEvidence, evidence);
            if ((yield* Ref.updateAndGet(writes, (n) => n + 1)) === 1 && scenario.seam === "write")
              return yield* busyWrite();
            return true;
          }),
        interruptTurn: () => Ref.update(interrupts, (n) => n + 1),
      });
      yield* drain.awaitIngested;
      assert.deepEqual(result.observed, [
        `run:${scenario.status === "completed" ? "waiting" : "failed"}`,
        "pull-requests-refreshed",
      ]);
      const attempt = result.events.find((event) => event.type === "run-attempt.updated")?.payload;
      assert.equal(attempt?.providerSettlement?.status, scenario.status);
      assert.deepEqual(attempt?.providerSettlement?.completedAt, completedAt);
      const turn = result.events.find((event) => event.type === "provider-turn.updated")?.payload;
      assert.equal(
        turn?.nativeTurnRef?.nativeId ?? null,
        scenario.driver === "codex" ? "original-native-turn" : null,
      );
      assert.deepEqual(turn?.completedAt, completedAt);
      assert.equal(yield* Ref.get(interrupts), 0);
      assert.equal(result.committedEffects.length, scenario.status === "completed" ? 1 : 0);
      assert.equal(result.events.filter((event) => event.type === "run-attempt.updated").length, 1);
      if (scenario.seam === "write") assert.equal(yield* Ref.get(reads), 1);
    }),
  );

  it.effect.each(["Stop", "successor"] as const)(
    "accepted-terminal recovery cannot overwrite a racing %s",
    (race) =>
      Effect.gen(function* () {
        let current = true;
        let writes = 0;
        const interrupts = yield* Ref.make(0);
        const result = yield* capture({
          key: `accepted-race:${race}`,
          shouldFinalizeRun: () => Effect.succeed(current),
          events: (ids) =>
            Stream.make<ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>>(
              terminalEvent(ids, "completed"),
            ),
          beforeTerminalWrite: () =>
            Effect.suspend(() => {
              if (++writes === 1) {
                current = false;
                return Effect.fail(busyWrite());
              }
              return Effect.succeed(current);
            }),
          interruptTurn: () => Ref.update(interrupts, (n) => n + 1),
        });
        assert.deepEqual(result.events, []);
        assert.deepEqual(result.committedEffects, []);
        assert.equal(yield* Ref.get(interrupts), 0);
      }),
  );

  it.effect(
    "a non-transient finalization error preserves the accepted failed native settlement",
    () =>
      Effect.gen(function* () {
        const nativeAt = DateTime.makeUnsafe("2026-11-11T23:00:00Z");
        let writes = 0;
        const result = yield* capture({
          key: "accepted-nontransient",
          shouldFinalizeRun: () => Effect.succeed(true),
          events: (ids) =>
            Stream.make<ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>>({
              ...terminalEvent(ids, "failed"),
              providerTurn: {
                id: ids.rootProviderTurnId,
                providerThreadId: ids.providerThreadId,
                nodeId: ids.rootNodeId,
                runAttemptId: ids.attemptId,
                nativeTurnRef: null,
                ordinal: 1,
                status: "failed",
                startedAt: nativeAt,
                completedAt: nativeAt,
              },
            }),
          beforeTerminalWrite: () =>
            Effect.suspend(() =>
              ++writes === 1
                ? Effect.fail(new EventSink.EventSinkWriteError({ eventCount: 1 }))
                : Effect.succeed(true),
            ),
          interruptTurn: () => Effect.die("Accepted terminal must not issue control"),
        });
        const attempt = result.events.find(
          (event) => event.type === "run-attempt.updated",
        )?.payload;
        assert.equal(attempt?.providerSettlement?.status, "failed");
        assert.deepEqual(attempt?.providerSettlement?.completedAt, nativeAt);
        const error = result.written.find(
          (item) => item.type === "error" && item.id.startsWith("turn-item:terminal-finalization:"),
        );
        const native = result.written.find(
          (item) => item.type === "error" && item.id !== error?.id,
        );
        assert.equal(native?.type === "error" ? native.failure.message : null, "Provider failed");
        assert.equal(
          error?.type === "error" ? error.failure.code : null,
          "terminal_finalization_failed",
        );
        assert.isTrue(error?.id.startsWith("turn-item:terminal-finalization:") ?? false);
      }),
  );

  it.effect(
    "an earlier completed goal-turn snapshot and a later active turn do not settle the run",
    () =>
      Effect.gen(function* () {
        const active = yield* Deferred.make<void>();
        const close = yield* Deferred.make<void>();
        let reads = 0;
        const result = yield* capture({
          key: "accepted-goal-chain",
          ingestNormalized: (input) =>
            input.event.type === "provider_turn.updated" &&
            input.event.providerTurn.status === "completed"
              ? retryAdmissionUnavailable(
                  Effect.suspend(() =>
                    ++reads === 1
                      ? Effect.fail(
                          new ProviderEventIngestor.ProviderEventNormalizeError({
                            providerSessionId: input.providerSessionId,
                            threadId: input.threadId,
                            providerEvent: input.event,
                            cause: admission(),
                          }),
                        )
                      : Effect.succeed([]),
                  ),
                )
              : Effect.succeed([]),
          shouldFinalizeRun: () => Effect.succeed(true),
          events: (ids) =>
            Stream.make<ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>>(
              {
                type: "provider_turn.updated",
                driver: ProviderDriverKind.make("codex"),
                providerTurn: {
                  id: ids.rootProviderTurnId,
                  providerThreadId: ids.providerThreadId,
                  nodeId: ids.rootNodeId,
                  runAttemptId: ids.attemptId,
                  nativeTurnRef: null,
                  ordinal: 1,
                  status: "completed",
                  startedAt: null,
                  completedAt: DateTime.makeUnsafe("2026-11-11T23:00:00Z"),
                },
              },
              {
                type: "provider_turn.updated",
                driver: ProviderDriverKind.make("codex"),
                providerTurn: {
                  id: ProviderTurnId.make(`${ids.rootProviderTurnId}:next`),
                  providerThreadId: ids.providerThreadId,
                  nodeId: ids.rootNodeId,
                  runAttemptId: ids.attemptId,
                  nativeTurnRef: null,
                  ordinal: 2,
                  status: "running",
                  startedAt: null,
                  completedAt: null,
                },
              },
            ).pipe(
              Stream.concat(
                Stream.fromEffect(
                  Deferred.succeed(active, undefined).pipe(Effect.andThen(Deferred.await(close))),
                ).pipe(Stream.drain),
              ),
              Stream.concat(Stream.failCause(Cause.interrupt())),
            ),
          afterStart: (closed) =>
            Effect.gen(function* () {
              yield* Deferred.await(active);
              assert.isFalse(yield* Deferred.isDone(closed));
              yield* Deferred.succeed(close, undefined);
            }),
          interruptTurn: () => Effect.die("A native snapshot must not issue control"),
        });
        assert.deepEqual(result.observed, []);
        assert.deepEqual(result.events, []);
        assert.equal(reads, 2);
      }),
  );
  it.effect(
    "three exhausted mid-run admission failures keep the existing local failure path and reason code",
    () =>
      Effect.gen(function* () {
        let reads = 0;
        let interrupts = 0;
        const result = yield* capture({
          key: "accepted-midrun-exhausted",
          shouldFinalizeRun: () => Effect.succeed(true),
          events: (ids) =>
            Stream.make<ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>>({
              type: "provider_turn.updated",
              driver: ProviderDriverKind.make("codex"),
              providerTurn: {
                id: ids.rootProviderTurnId,
                providerThreadId: ids.providerThreadId,
                nodeId: ids.rootNodeId,
                runAttemptId: ids.attemptId,
                nativeTurnRef: null,
                ordinal: 1,
                status: "running",
                startedAt: null,
                completedAt: null,
              },
            }),
          ingestNormalized: (input) =>
            retryAdmissionUnavailable(
              Effect.suspend(() => {
                reads++;
                return Effect.fail(
                  new ProviderEventIngestor.ProviderEventNormalizeError({
                    providerSessionId: input.providerSessionId,
                    threadId: input.threadId,
                    providerEvent: input.event,
                    cause: admission(),
                  }),
                );
              }),
            ),
          interruptTurn: () =>
            Effect.sync(() => {
              interrupts++;
            }),
        });
        assert.equal(reads, 3);
        assert.equal(interrupts, 1);
        assert.deepEqual(result.observed, ["run:failed", "pull-requests-refreshed"]);
        const error = result.written.find((item) => item.type === "error");
        assert.equal(
          error?.type === "error" ? error.failure.code : null,
          "reader_admission_queue-timeout",
        );
      }),
  );
}
