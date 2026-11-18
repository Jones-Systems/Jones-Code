import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import { ReadPriority } from "@t3tools/shared/jones/sqliteReadPriority";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Ref from "effect/Ref";
import { inventoryRead, withInventoryTrigger, type InventoryTrigger } from "./InventoryRead.ts";

describe("cleanup inventory priority", () => {
  it.effect(
    "uses the queued trigger in the worker and its fork, then restores writer priority",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const observed = yield* Ref.make<ReadonlyArray<string>>([]);
          const worker = yield* makeDrainableWorker((trigger: InventoryTrigger) =>
            Effect.gen(function* () {
              const read = yield* ReadPriority.pipe(inventoryRead, Effect.forkScoped);
              const priority = yield* Fiber.join(read);
              // A priority wrapper on one read must not classify subsequent writer work.
              const writer = yield* ReadPriority;
              yield* Ref.update(observed, (rows) => [...rows, `${trigger}:${priority}:${writer}`]);
            }).pipe(withInventoryTrigger(trigger)),
          );
          // Enqueue from the opposite request contexts. Queueing a request must not
          // accidentally copy its priority into a long-lived cleanup worker.
          yield* worker.enqueue("automatic").pipe(withInventoryTrigger("manual"));
          yield* worker.enqueue("manual").pipe(withInventoryTrigger("automatic"));
          yield* worker.drain;
          expect(yield* Ref.get(observed)).toEqual([
            "automatic:background:interactive",
            "manual:interactive:interactive",
          ]);
        }),
      ),
  );

  it.effect("preserves the original admission failure rather than an empty inventory", () =>
    Effect.gen(function* () {
      const failure = {
        _tag: "SqlError",
        cause: { _tag: "SqliteReaderAdmissionError", reason: "generation-retired" },
      } as const;
      const result = yield* Effect.fail(failure).pipe(
        inventoryRead,
        withInventoryTrigger("automatic"),
        Effect.flip,
      );
      expect(result).toBe(failure);
      expect(result.cause.reason).toBe("generation-retired");
    }),
  );
});
