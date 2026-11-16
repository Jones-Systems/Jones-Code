import type { ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

/** Fiber-local to the transaction; a rollback discards the entire accumulator. */
export const ChangedThreads = Context.Reference<Set<ThreadId> | undefined>(
  "jones/threadResultDependencies/ChangedThreads",
  { defaultValue: () => undefined },
);
export const changed = (threadId: ThreadId) =>
  ChangedThreads.pipe(
    Effect.flatMap((threads) =>
      Effect.sync(() => {
        threads?.add(threadId);
      }),
    ),
  );
