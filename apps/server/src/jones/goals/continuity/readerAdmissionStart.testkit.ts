import { expect, it } from "@effect/vitest";
import {
  GoalContinuityError,
  RunAttemptId,
  OrchestrationV2DomainEventJson,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import { SqliteReaderAdmissionError } from "@t3tools/shared/jones/sqliteReaders/admissionFailure";
import type { ContinuityRequirement } from "@t3tools/provider-core/server/jones/GoalContinuityContext";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { SqlError, classifySqliteError } from "effect/sql/SqlError";
import { ProjectionStoreReadError } from "../../../orchestration-v2/ProjectionStore.ts";
import type { ProviderTurnStartError } from "../../../orchestration-v2/ProviderTurnStartService.ts";
import { CurrentEffectClaim, EffectClaimLost } from "../../workspaceLifecycle/ActiveEffectClaim.ts";
import type * as GoalContinuity from "./GoalContinuity.ts";

export interface ReaderAdmissionHarnessInput {
  readonly text: string;
  readonly nativeResume?: "same" | "changed" | "missing";
  readonly continuityRead?: GoalContinuity.GoalContinuity["Service"]["requirementFor"];
  readonly runtimeRecoveryRead?: () => Effect.Effect<void, ProjectionStoreReadError>;
  readonly retireBeforeRevalidation?: boolean;
}

interface Harness {
  readonly start: Effect.Effect<void, ProviderTurnStartError>;
  readonly startWithRetry: Effect.Effect<void, ProviderTurnStartError>;
  readonly open: unknown;
  readonly detach: unknown;
  readonly startRootRun: unknown;
  readonly fallbackEnsure: unknown;
  readonly projection: () => OrchestrationV2ThreadProjection;
  readonly setProjection: (projection: OrchestrationV2ThreadProjection) => void;
  readonly interruptRun: () => void;
  readonly attemptId: string;
  readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
}

const admission = (reason: "queue-timeout" | "checkpoint-held") =>
  new SqlError({ reason: classifySqliteError(new SqliteReaderAdmissionError({ reason })) });
const unavailable = (reason: "queue-timeout" | "checkpoint-held") =>
  Object.defineProperty(new GoalContinuityError({ reason: "unavailable" }), "cause", {
    value: admission(reason),
  });
const absent: ContinuityRequirement = {
  outcome: "absent",
  gate: { outcome: "clear", fenceRevision: "0" },
};

/** Registers Jones regressions in the existing upstream start-service harness without copying it. */
export function registerReaderAdmissionStartTests(
  makeHarness: (input: ReaderAdmissionHarnessInput) => Harness,
) {
  it.effect.each(["absent", "required"] as const)(
    "retries continuity admission before exactly one start (%s)",
    (outcome) =>
      Effect.gen(function* () {
        let reads = 0;
        let requirement: ContinuityRequirement = absent;
        const harness = makeHarness({
          text: "Continue",
          nativeResume: "same",
          continuityRead: () =>
            Effect.suspend(() =>
              ++reads === 1
                ? Effect.fail(unavailable("queue-timeout"))
                : Effect.succeed(requirement),
            ),
        });
        const original = harness.projection();
        const provider = original.providerThreads[0]!;
        if (outcome === "required")
          requirement = {
            outcome,
            owner: {
              environmentId: "environment:mcp-test",
              threadId: original.thread.id,
              providerInstanceId: provider.providerInstanceId,
              providerThreadId: provider.id,
              driver: provider.driver,
              nativeThreadId: provider.nativeThreadRef!.nativeId!,
            } as import("@t3tools/contracts").ContinuityOwner,
            target: {
              goalId: "goal",
              revision: "1",
              generation: "1",
              snapshotDigest: "a".repeat(64),
            },
            gate: { outcome: "clear", fenceRevision: "0" },
          };
        expect((yield* Effect.result(harness.startWithRetry))._tag).toBe("Failure");
        expect(harness.projection().runs.at(-1)?.status).toBe("starting");
        expect(harness.open).not.toHaveBeenCalled();
        yield* harness.start;
        expect(reads).toBe(2);
        expect(harness.open).toHaveBeenCalledOnce();
        expect(harness.startRootRun).toHaveBeenCalledOnce();
        expect(harness.fallbackEnsure).not.toHaveBeenCalled();
        expect(harness.projection().runs.at(-1)?.id).toBe(original.runs.at(-1)?.id);
        expect(harness.projection().runs.at(-1)?.activeAttemptId).toBe(harness.attemptId);
        expect(harness.open).toHaveBeenCalledWith(
          expect.objectContaining({
            providerSessionId: provider.providerSessionId,
            initialNativeThreadId: provider.nativeThreadRef!.nativeId,
          }),
        );
      }),
  );

  it.effect("exhausted continuity admission reports availability and never opens a provider", () =>
    Effect.gen(function* () {
      const harness = makeHarness({
        text: "Continue",
        continuityRead: () => Effect.fail(unavailable("checkpoint-held")),
      });
      for (let retry = 0; retry < 4; retry++) yield* Effect.result(harness.startWithRetry);
      yield* harness.start;
      expect(harness.open).not.toHaveBeenCalled();
      expect(harness.startRootRun).not.toHaveBeenCalled();
      expect(harness.projection().runs.at(-1)?.status).toBe("failed");
      const error = harness.projection().turnItems.find((item) => item.type === "error");
      expect(error).toMatchObject({
        title: "Continuity check unavailable",
        failure: { class: "unknown", code: "reader_admission_checkpoint-held", retryable: true },
      });
      if (error?.type === "error")
        expect(error.failure.message).toContain("No provider work was started");
    }),
  );

  it.effect.each(["delivery_pending", "delivery_unknown", "recovery_required"] as const)(
    "retry preserves a real %s fence",
    (reason) =>
      Effect.gen(function* () {
        let reads = 0;
        const harness = makeHarness({
          text: "Continue",
          continuityRead: () =>
            Effect.suspend(() =>
              ++reads === 1
                ? Effect.fail(unavailable("checkpoint-held"))
                : Effect.succeed({
                    outcome: "hold",
                    reason,
                    current: null,
                    gate: {
                      outcome: "hold",
                      reason,
                      fenceRevision: "9",
                      deliveryIds: ["original-delivery"],
                    },
                  } as const),
            ),
        });
        yield* Effect.result(harness.startWithRetry);
        yield* harness.start;
        expect(harness.open).not.toHaveBeenCalled();
        expect(harness.projection().turnItems).toContainEqual(
          expect.objectContaining({ title: "Goal continuity requires attention" }),
        );
      }),
  );

  it.effect("permanent or unclassified continuity errors keep the existing hold", () =>
    Effect.gen(function* () {
      const harness = makeHarness({
        text: "Continue",
        continuityRead: () => Effect.fail(new GoalContinuityError({ reason: "unavailable" })),
      });
      yield* harness.startWithRetry;
      expect(harness.open).not.toHaveBeenCalled();
      expect(harness.projection().turnItems).toContainEqual(
        expect.objectContaining({ title: "Goal continuity requires attention" }),
      );
    }),
  );

  it.effect.each(["stop", "successor"] as const)("%s fences a delayed admission retry", (mode) =>
    Effect.gen(function* () {
      let reads = 0;
      const harness = makeHarness({
        text: "Continue",
        continuityRead: () =>
          Effect.suspend(() =>
            ++reads === 1 ? Effect.fail(unavailable("queue-timeout")) : Effect.succeed(absent),
          ),
      });
      yield* Effect.result(harness.startWithRetry);
      if (mode === "stop") {
        harness.interruptRun();
        yield* harness.start;
      } else {
        const projection = harness.projection();
        const successor = RunAttemptId.make("newer-attempt");
        harness.setProjection({
          ...projection,
          runs: projection.runs.map((run) => ({ ...run, activeAttemptId: successor })),
          attempts: [...projection.attempts, { ...projection.attempts[0]!, id: successor }],
        });
        yield* harness.startWithRetry.pipe(
          Effect.provide(
            Layer.mock(CurrentEffectClaim)({
              nativeCustody: true,
              check: Effect.fail(new EffectClaimLost({ effectId: "superseded-start-effect" })),
            }),
          ),
          Effect.result,
        );
        expect(harness.projection().runs.at(-1)?.activeAttemptId).toBe(successor);
      }
      expect(reads).toBe(1);
      expect(harness.open).not.toHaveBeenCalled();
      expect(harness.startRootRun).not.toHaveBeenCalled();
    }),
  );

  it.effect.each([2, 3])(
    "run-state read failure at check %s is unavailable, with no provider open",
    (failedRead) =>
      Effect.gen(function* () {
        let reads = 0;
        const harness = makeHarness({
          text: "Continue",
          nativeResume: "same",
          retireBeforeRevalidation: true,
          runtimeRecoveryRead: () =>
            Effect.suspend(() =>
              ++reads === failedRead
                ? Effect.fail(
                    new ProjectionStoreReadError({
                      threadId: harness.projection().thread.id,
                      cause: admission("queue-timeout"),
                    }),
                  )
                : Effect.void,
            ),
        });
        yield* harness.start;
        expect(harness.open).not.toHaveBeenCalled();
        expect(harness.detach).toHaveBeenCalledOnce();
        expect(harness.projection().turnItems).toContainEqual(
          expect.objectContaining({
            title: "Provider start could not verify its run state",
            failure: expect.objectContaining({
              class: "unknown",
              code: "reader_admission_queue-timeout",
              retryable: true,
            }),
          }),
        );
      }),
  );

  it.effect("retry after a failed post-retirement read reuses completed retirement", () =>
    Effect.gen(function* () {
      let reads = 0;
      const harness = makeHarness({
        text: "Continue",
        nativeResume: "same",
        retireBeforeRevalidation: true,
        runtimeRecoveryRead: () =>
          Effect.suspend(() =>
            ++reads === 2
              ? Effect.fail(
                  new ProjectionStoreReadError({
                    threadId: harness.projection().thread.id,
                    cause: admission("checkpoint-held"),
                  }),
                )
              : Effect.void,
          ),
      });
      yield* Effect.result(harness.startWithRetry);
      expect(harness.open).not.toHaveBeenCalled();
      expect(harness.detach).toHaveBeenCalledOnce();
      yield* harness.start;
      expect(harness.detach).toHaveBeenCalledOnce();
      expect(harness.open).toHaveBeenCalledOnce();
      expect(harness.startRootRun).toHaveBeenCalledOnce();
    }),
  );

  it.effect.each(["read", "selection"] as const)(
    "distinguishes an unclassified failed read from a real selection change (%s)",
    (mode) =>
      Effect.gen(function* () {
        let reads = 0;
        const harness = makeHarness({
          text: "Continue",
          nativeResume: "same",
          retireBeforeRevalidation: true,
          runtimeRecoveryRead: () =>
            Effect.suspend(() => {
              if (++reads !== 2) return Effect.void;
              if (mode === "read")
                return Effect.fail(
                  new ProjectionStoreReadError({
                    threadId: harness.projection().thread.id,
                    cause: new Error("private-query"),
                  }),
                );
              const projection = harness.projection();
              harness.setProjection({
                ...projection,
                runs: projection.runs.map((run) => ({
                  ...run,
                  modelSelection: { ...run.modelSelection, model: "changed-selection" },
                })),
              });
              return Effect.void;
            }),
        });
        yield* harness.start;
        expect(harness.open).not.toHaveBeenCalled();
        expect(harness.detach).toHaveBeenCalledOnce();
        expect(harness.projection().turnItems).toContainEqual(
          expect.objectContaining({
            title:
              mode === "read"
                ? "Provider start could not verify its run state"
                : "Provider selection changed before opening",
            failure: expect.objectContaining({
              code: null,
              class: mode === "read" ? "unknown" : "provider_error",
            }),
          }),
        );
        if (mode === "read") {
          const item = harness
            .projection()
            .turnItems.find((candidate) => candidate.type === "error");
          if (item?.type === "error") {
            expect(item.failure.message).not.toContain("private-query");
            expect(item.failure.message).not.toContain("selection changed");
          }
        }
        if (mode === "selection") {
          // These are the existing persisted item identities and wire failure classes.
          const held = makeHarness({
            text: "Continue",
            continuityRead: () => Effect.fail(new GoalContinuityError({ reason: "unavailable" })),
          });
          yield* held.start;
          for (const event of [...harness.events, ...held.events]) {
            const json = yield* Schema.encodeEffect(OrchestrationV2DomainEventJson)(event);
            expect(yield* Schema.decodeEffect(OrchestrationV2DomainEventJson)(json)).toEqual(event);
          }
        }
      }),
  );
}
