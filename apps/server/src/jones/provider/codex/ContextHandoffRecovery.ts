import type { OrchestrationV2ProviderThread } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { ContextHandoffBudgetError } from "@t3tools/provider-core/server/failure";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { deliverContextHandoffs } from "../../../orchestration-v2/ContextHandoffDelivery.ts";

const isContextHandoffBudgetError = Schema.is(ContextHandoffBudgetError);

// Only the local budget failure is recoverable. Persist/inject failures retain
// their original semantics, and the second delivery has no recovery handler.
export const deliverWithHandoffRecovery = <
  InjectError = never,
  PersistError = never,
  BudgetError = never,
>(
  delivery: Parameters<typeof deliverContextHandoffs<InjectError, PersistError, BudgetError>>[0],
  recovery: {
    readonly input: ProviderAdapter.ProviderAdapterV2TurnInput;
    readonly compact?: ProviderAdapter.ProviderAdapterV2SessionRuntime["compactForContextHandoff"];
    readonly revalidate: Effect.Effect<void, ProviderAdapter.ProviderAdapterV2Error>;
    readonly budgetAfter: (
      usage: NonNullable<OrchestrationV2ProviderThread["contextUsage"]>,
    ) => number;
    readonly reclaimableBudget: number;
  },
) => {
  let deliveryEffectsStarted = false;
  const markDelivery = Effect.sync(() => {
    deliveryEffectsStarted = true;
  });
  const deliver = (budget: typeof delivery.budget) =>
    recovery.revalidate.pipe(
      Effect.andThen(
        deliverContextHandoffs({
          ...delivery,
          budget,
          persist: (handoff) =>
            recovery.revalidate.pipe(
              Effect.andThen(markDelivery),
              Effect.andThen(delivery.persist(handoff)),
            ),
          ...(delivery.inject === undefined
            ? {}
            : {
                inject: (history: ProviderAdapter.ProviderAdapterV2HistoricalContext) =>
                  recovery.revalidate.pipe(
                    Effect.andThen(markDelivery),
                    Effect.andThen(delivery.inject!(history)),
                  ),
              }),
        }),
      ),
    );
  return deliver(delivery.budget).pipe(
    Effect.catch((error) => {
      if (deliveryEffectsStarted || !isContextHandoffBudgetError(error)) return Effect.fail(error);
      return Effect.gen(function* () {
        const nativeId = delivery.providerThread.nativeThreadRef?.nativeId;
        if (
          delivery.deferInline ||
          !recovery.compact ||
          !nativeId ||
          delivery.handoffs.some(
            (handoff) =>
              handoff.delivery?.nativeThreadId === nativeId &&
              handoff.delivery.status === "pending",
          )
        )
          return yield* Effect.fail(error);
        // A pure preview proves that the intact request plus reserves can fit
        // after reclaiming history. Zero occupancy is a capacity bound, not telemetry.
        yield* deliverContextHandoffs({
          handoffs: delivery.handoffs,
          providerThread: delivery.providerThread,
          alreadyDeliveredItemIds: delivery.alreadyDeliveredItemIds,
          budget: recovery.reclaimableBudget,
          persist: () => Effect.void,
        });
        yield* recovery.revalidate;
        const usage = yield* recovery.compact({
          ...recovery.input,
          revalidateStartAdmission: recovery.revalidate,
        });
        yield* recovery.revalidate;
        return yield* deliver(recovery.budgetAfter(usage));
      });
    }),
  );
};
