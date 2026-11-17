import * as Effect from "effect/Effect";
import { makeRuntimeUseStore } from "./RuntimeUseStore.ts";

/** Read-only interpretation of the complete original journal. It creates no
 * execution or retirement authority and never trusts the current state alone. */
export const makeRuntimeHistory = Effect.gen(function* () {
  const store = yield* makeRuntimeUseStore;
  const read = Effect.fn("RuntimeHistory.read")(function* () {
    const records = yield* store.inventory();
    const blockers = records
      .filter((record) => {
        if (
          record.state !== "retired" ||
          record.retirement === null ||
          record.binding.original.nativeReference !== null
        )
          return true;
        if (record.retirement.outcome === "not_invoked") return false;
        return (
          record.binding.actor.kind === "terminal_spawn" ||
          record.producers.length === 0 ||
          record.producers.some((producer) => producer.coverage !== "complete")
        );
      })
      .map((record) => ({ kind: "runtime", target: record.binding.canonicalPath }));
    return { records, blockers };
  });
  return { read };
});
