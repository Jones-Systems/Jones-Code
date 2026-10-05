import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

class UnownedCommitTransactionError extends Schema.TaggedError<UnownedCommitTransactionError>()(
  "UnownedCommitTransactionError",
  { message: Schema.String },
) {}

const CommitPublications = Context.Reference<Array<Effect.Effect<void>> | undefined>(
  "t3/orchestration-v2/commit-publications",
  { defaultValue: () => undefined },
);
const CommitFinalizers = Context.Reference<Map<object, Effect.Effect<void>> | undefined>(
  "t3/orchestration-v2/commit-finalizers",
  { defaultValue: () => undefined },
);

/** Memory handles and notifications become visible only after the owning SQL commit. */
export const makeCommitTransaction = Effect.fn("makeCommitTransaction")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const assertOwned = Effect.gen(function* () {
    if (
      Option.isSome(yield* Effect.serviceOption(sql.transactionService)) &&
      (yield* CommitPublications) === undefined
    )
      return yield* new UnownedCommitTransactionError({
        message: "Use the owning commit transaction for an enclosing write.",
      });
  });
  const afterCommit = (publication: Effect.Effect<void>) =>
    Effect.gen(function* () {
      yield* assertOwned;
      const publications = yield* CommitPublications;
      if (publications === undefined) yield* publication;
      else publications.push(publication);
    });
  const withTransaction = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        yield* assertOwned;
        const parent = yield* CommitPublications;
        const inheritedFinalizers = yield* CommitFinalizers;
        const publications: Array<Effect.Effect<void>> = [];
        const finalizers = inheritedFinalizers ?? new Map<object, Effect.Effect<void>>();
        const transaction = restore(
          sql.withTransaction(
            effect.pipe(
              Effect.provideService(CommitPublications, publications),
              Effect.provideService(CommitFinalizers, finalizers),
            ),
          ),
        ).pipe(
          Effect.tap(() =>
            Effect.gen(function* () {
              if (parent !== undefined) parent.push(...publications);
              else
                yield* Effect.forEach(publications, (publication) => publication, {
                  concurrency: 1,
                  discard: true,
                });
            }),
          ),
        );
        return yield* inheritedFinalizers === undefined
          ? transaction.pipe(
              Effect.ensuring(
                Effect.suspend(() =>
                  Effect.forEach(finalizers.values(), (finalizer) => finalizer, {
                    concurrency: 1,
                    discard: true,
                  }),
                ),
              ),
            )
          : transaction;
      }),
    );
  const retainUntilSettlement = (
    key: object,
    acquire: Effect.Effect<void>,
    release: Effect.Effect<void>,
  ) =>
    Effect.uninterruptible(
      Effect.gen(function* () {
        const finalizers = yield* CommitFinalizers;
        if (finalizers === undefined)
          return yield* new UnownedCommitTransactionError({
            message: "A publication lane requires an owning commit transaction.",
          });
        if (finalizers.has(key)) return;
        yield* acquire;
        finalizers.set(key, release);
      }),
    );
  const requireOwned = Effect.gen(function* () {
    yield* assertOwned;
    if ((yield* CommitPublications) === undefined)
      return yield* new UnownedCommitTransactionError({
        message: "This write requires the current owning commit transaction.",
      });
  });
  return { withTransaction, afterCommit, retainUntilSettlement, requireOwned };
});
