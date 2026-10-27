import { assert, describe, it } from "@effect/vitest";
import { ServerSelfUpdateError } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import {
  ServiceLauncherClientError,
  ServiceLauncherRejectedError,
} from "../../cloud/serviceLauncherClient.ts";
import { awaitHandoffOutcome } from "./handoffOutcome.ts";
import type { OperationReconciliation } from "./launcherOperation.ts";

const operationId = "12345678-1234-4234-8234-123456789abc";
const failed = (cause: unknown) =>
  Cause.fail(new ServerSelfUpdateError({ reason: "handoff failed", cause }));
const timedOut = failed(new ServiceLauncherClientError({ operation: "timeout" }));
const refused = failed(
  new ServiceLauncherRejectedError({ targetVersion: "1.1.0", reason: "request-expired: late" }),
);
// A reservation without an outcome: the receipt that once reopened admission (HFR-440-F1).
const reservedOnly: OperationReconciliation = {
  state: "blocked",
  operationId,
  reason: "Reserved update has no proven native outcome; reconcile before retrying.",
};

/** Scripted receipts and answers; the last entry repeats. */
const launcher = (
  receipts: ReadonlyArray<OperationReconciliation>,
  answers?: ReadonlyArray<"accepted" | "rejected" | "unknown" | "timeout">,
) => {
  const calls = { reconcile: 0, query: 0 };
  const at = <A>(items: ReadonlyArray<A>, index: number) =>
    items[Math.min(index, items.length - 1)]!;
  return {
    calls,
    reconcile: () => Effect.sync(() => at(receipts, calls.reconcile++)),
    ...(answers === undefined
      ? {}
      : {
          query: () =>
            Effect.suspend(() => {
              const answer = at(answers, calls.query++);
              return answer === "timeout"
                ? Effect.fail(new ServiceLauncherClientError({ operation: "timeout" }))
                : Effect.succeed({ outcome: answer });
            }),
        }),
  };
};

describe("awaitHandoffOutcome", () => {
  it.effect("a refusal the launcher sent proves rejection unless receipts show acceptance", () =>
    Effect.gen(function* () {
      const plain = launcher([reservedOnly]);
      assert.strictEqual(
        yield* awaitHandoffOutcome({ operationId, cause: refused, reconcile: plain.reconcile }),
        "rejected",
      );
      const contradicted = launcher([{ state: "pending", operationId, updateId: operationId }]);
      assert.strictEqual(
        yield* awaitHandoffOutcome({
          operationId,
          cause: refused,
          reconcile: contradicted.reconcile,
        }),
        "accepted",
      );
    }),
  );

  it.effect("unknown answers and an abandoned-looking receipt hold until a late acceptance", () =>
    Effect.gen(function* () {
      const script = launcher(
        [reservedOnly],
        ["timeout", "unknown", "unknown", "unknown", "unknown", "unknown", "accepted"],
      );
      const outcome = yield* awaitHandoffOutcome({
        operationId,
        cause: timedOut,
        ...script,
      }).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      assert.strictEqual(script.calls.query, 1);
      // Backs off 1, 2, 4, 8, 16 then 30 seconds; time alone never decides.
      for (const [seconds, asked] of [
        [1, 2],
        [2, 3],
        [4, 4],
        [8, 5],
        [16, 6],
        [29, 6],
        [1, 7],
      ] as const) {
        yield* TestClock.adjust(Duration.seconds(seconds));
        assert.strictEqual(script.calls.query, asked);
      }
      assert.strictEqual(yield* Fiber.join(outcome), "accepted");
      assert.strictEqual(script.calls.reconcile, 7);
    }),
  );

  it.effect("a rejection the launcher records or answers later reopens", () =>
    Effect.gen(function* () {
      const recorded = launcher([reservedOnly, { state: "rejected", operationId }], ["unknown"]);
      const fromReceipt = yield* awaitHandoffOutcome({
        operationId,
        cause: timedOut,
        ...recorded,
      }).pipe(Effect.forkChild);
      yield* TestClock.adjust(Duration.seconds(1));
      assert.strictEqual(yield* Fiber.join(fromReceipt), "rejected");
      const answered = launcher([reservedOnly], ["rejected"]);
      assert.strictEqual(
        yield* awaitHandoffOutcome({ operationId, cause: timedOut, ...answered }),
        "rejected",
      );
    }),
  );

  it.effect("evidence for another operation, or a failed acceptance, never reads as rejected", () =>
    Effect.gen(function* () {
      const stale = launcher(
        [
          { state: "rejected", operationId: "22345678-1234-4234-8234-123456789abc" },
          { state: "blocked", operationId, updateId: operationId },
        ],
        ["unknown"],
      );
      const outcome = yield* awaitHandoffOutcome({
        operationId,
        cause: timedOut,
        ...stale,
      }).pipe(Effect.forkChild);
      yield* TestClock.adjust(Duration.seconds(1));
      assert.strictEqual(yield* Fiber.join(outcome), "accepted");
    }),
  );

  it.effect("without the outcome capability or an operation, only exit ends an unknown hold", () =>
    Effect.gen(function* () {
      const old = launcher([reservedOnly]);
      const fromOld = yield* awaitHandoffOutcome({
        operationId,
        cause: timedOut,
        reconcile: old.reconcile,
      }).pipe(Effect.forkChild);
      const unbound = yield* awaitHandoffOutcome({
        operationId: undefined,
        cause: timedOut,
        reconcile: old.reconcile,
      }).pipe(Effect.forkChild);
      yield* TestClock.adjust(Duration.days(1));
      assert.isUndefined(fromOld.pollUnsafe());
      assert.isUndefined(unbound.pollUnsafe());
      assert.strictEqual(old.calls.reconcile, 1);
      // A refusal still proves rejection of a request that carried no operation.
      assert.strictEqual(
        yield* awaitHandoffOutcome({
          operationId: undefined,
          cause: refused,
          reconcile: old.reconcile,
        }),
        "rejected",
      );
    }),
  );
});
