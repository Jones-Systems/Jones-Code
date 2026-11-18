import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import { backgroundRead, interactiveRead } from "../serverParallelism/readClassification.ts";

export type InventoryTrigger = "automatic" | "manual";

const automaticInventory = Context.Reference<boolean>("jones/StorageCleanup/automaticInventory", {
  defaultValue: () => false,
});

// Install the explicit queued trigger when the worker executes the sweep. This
// context selects priority only; it must never route the sweep's writers to a reader.
export const withInventoryTrigger =
  (trigger: InventoryTrigger) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    Effect.provideService(effect, automaticInventory, trigger === "automatic");

// Apply around each executing inventory read, including late revalidation.
// Reader routing belongs to the read service or its narrow readOnly boundary.
export const inventoryRead = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.flatMap(automaticInventory, (automatic) =>
    effect.pipe(automatic ? backgroundRead : interactiveRead),
  );
