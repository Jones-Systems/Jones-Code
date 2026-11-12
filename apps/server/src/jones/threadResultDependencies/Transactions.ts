import { ChangedThreads } from "./ChangeTracking.ts";
import type { ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";
import type { makeDependencyStore } from "./DependencyStore.ts";
import type { DependencyChanges } from "./DependencyChanges.ts";

/** Capture the watermark and affected IDs in the write snapshot, then signal after commit. */
export const makeCommit =
  (
    sql: SqlClient.SqlClient,
    store: ReturnType<typeof makeDependencyStore>,
    changes: DependencyChanges["Service"],
  ) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.suspend(() => {
      const changedThreads = new Set<ThreadId>();
      return Effect.uninterruptibleMask((restore) =>
        restore(
          sql.withTransaction(
            Effect.gen(function* () {
              const result = yield* effect;
              const sequence = yield* store.sequence;
              return { result, sequence, threadIds: [...changedThreads] };
            }).pipe(Effect.provideService(ChangedThreads, changedThreads)),
          ),
        ).pipe(
          Effect.tap(({ sequence, threadIds }) => changes.publish({ sequence, threadIds })),
          Effect.map(({ result }) => result),
        ),
      );
    });
