import { ProviderSessionId } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { WriterRetirementError, type Retirement } from "./RetirementLedger.ts";

export class WriterRetirementPendingError extends Schema.TaggedError<WriterRetirementPendingError>()(
  "WriterRetirementPendingError",
  {
    providerSessionId: ProviderSessionId,
    // In-process continuation of the captured record; never another retirement dispatch.
    awaitCompletion: Schema.declare(
      (value): value is Effect.Effect<void, WriterRetirementError> => Effect.isEffect(value),
    ),
  },
) {
  override get message(): string {
    return `Still awaiting native writer retirement proof for session ${this.providerSessionId}.`;
  }
}

export class WriterRetirementWaitExpiredError extends Schema.TaggedError<WriterRetirementWaitExpiredError>()(
  "WriterRetirementWaitExpiredError",
  { providerSessionId: ProviderSessionId },
) {
  override get message(): string {
    return `The previous provider session ${this.providerSessionId} is still releasing this conversation. Retirement proof was not observed within ten minutes. Please retry.`;
  }
}
export const isWriterRetirementWaitExpired = Schema.is(WriterRetirementWaitExpiredError);
const isPending = Schema.is(WriterRetirementPendingError);

// A caller deadline does not fail the retained worker or discard its exact proof observer.
export const awaitWriterRetirement = Effect.fn("jones.awaitWriterRetirement")(function* (
  record: Retirement,
  timeoutMs: number,
) {
  const awaitCompletion = Deferred.await(record.completion);
  yield* awaitCompletion.pipe(
    Effect.timeoutOrElse({
      duration: timeoutMs,
      orElse: () =>
        new WriterRetirementPendingError({
          providerSessionId: record.providerSessionId,
          awaitCompletion,
        }),
    }),
  );
});

// A pending conversation barrier must retain every captured record, including later ones.
export const awaitWriterRetirements = Effect.fn("jones.awaitWriterRetirements")(function* (
  records: ReadonlyArray<Retirement>,
  timeoutMs: number,
) {
  yield* Effect.forEach(records, (record) => awaitWriterRetirement(record, timeoutMs), {
    concurrency: 1,
    discard: true,
  }).pipe(
    Effect.catchTags({
      WriterRetirementPendingError: (pending) =>
        new WriterRetirementPendingError({
          providerSessionId: pending.providerSessionId,
          awaitCompletion: Effect.forEach(records, (record) => Deferred.await(record.completion), {
            concurrency: 1,
            discard: true,
          }),
        }),
    }),
  );
});

// One budget spans all retirement barriers in this admission, including their initial waits.
export const RETIREMENT_ADMISSION_BUDGET_MS = 10 * 60 * 1_000;
export const awaitRetirementForAdmission = Effect.fn("jones.awaitRetirementForAdmission")(
  function* <E, R, E2, R2>(
    operation: Effect.Effect<void, E, R>,
    deadline: number,
    revalidate: Effect.Effect<void, E2, R2>,
  ) {
    const result = yield* Effect.result(operation);
    if (result._tag === "Success") return;
    if (!isPending(result.failure)) return yield* Effect.fail(result.failure);
    const pending = result.failure;
    const remaining = Math.max(0, deadline - (yield* Clock.currentTimeMillis));
    // Run cancellation/currentness must remain responsive even when native closure takes minutes.
    yield* Effect.raceFirst(
      pending.awaitCompletion,
      Effect.forever(Effect.sleep("1 second").pipe(Effect.andThen(revalidate))),
    ).pipe(
      Effect.timeoutOrElse({
        duration: remaining,
        orElse: () =>
          new WriterRetirementWaitExpiredError({ providerSessionId: pending.providerSessionId }),
      }),
    );
  },
);
