import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export type SqliteReadPriority = "interactive" | "background";

export const ReadPriority = Context.Reference<SqliteReadPriority>(
  "@t3tools/shared/jones/sqliteReadPriority/ReadPriority",
  { defaultValue: () => "interactive" },
);

/** Priority and read-only routing are independent, scoped properties. */
export const withReadPriority =
  (priority: SqliteReadPriority) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    Effect.provideService(effect, ReadPriority, priority);
