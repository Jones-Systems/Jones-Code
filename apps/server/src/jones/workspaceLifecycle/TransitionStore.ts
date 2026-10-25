import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { canonical } from "../ordinaryCheckout/CheckoutProtocol.ts";
import { Binding, LifecycleError } from "./Protocol.ts";

const Transition = Schema.Struct({
  id: Schema.NonEmptyString,
  source: Schema.NonEmptyString,
  binding: Binding,
  expectedGeneration: Schema.NullOr(Schema.NonEmptyString),
  generation: Schema.NullOr(Schema.NonEmptyString),
  state: Schema.Literals(["prepared", "invoking", "confirmed"]),
});
export type Transition = typeof Transition.Type;
const isLifecycleError = Schema.is(LifecycleError);
const decodeBinding = Schema.decodeEffect(Schema.fromJsonString(Binding));
const decodeTransition = Schema.decodeUnknownEffect(Transition);
export class TransitionStore extends Context.Service<
  TransitionStore,
  {
    readonly outsideTransaction: Effect.Effect<void, LifecycleError>;
    readonly read: (id: string) => Effect.Effect<Transition | null, LifecycleError>;
    readonly current: (worktree: string) => Effect.Effect<Transition | null, LifecycleError>;
    readonly prior: (worktree: string) => Effect.Effect<Transition | null, LifecycleError>;
    readonly prepare: (transition: Transition) => Effect.Effect<Transition, LifecycleError>;
    readonly begin: (transition: Transition) => Effect.Effect<void, LifecycleError>;
    readonly confirm: (
      transition: Transition,
      generation: string,
    ) => Effect.Effect<Transition, LifecycleError>;
  }
>()("t3/jones/workspaceLifecycle/TransitionStore") {}

// This schema belongs to the Jones migration loader. Merely constructing the store never creates or migrates it.
export const migration = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE jones_workspace_lifecycle_transitions (
    transition_id TEXT PRIMARY KEY NOT NULL,
    worktree TEXT NOT NULL,
    source_json TEXT NOT NULL,
    binding_json TEXT NOT NULL CHECK(json_valid(binding_json)),
    expected_generation TEXT,
    confirmed_generation TEXT,
    state TEXT NOT NULL CHECK(state IN ('prepared','invoking','confirmed')),
    CHECK((state='confirmed')=(confirmed_generation IS NOT NULL))
  )`;
  yield* sql`CREATE UNIQUE INDEX jones_workspace_lifecycle_pending ON jones_workspace_lifecycle_transitions(worktree) WHERE state!='confirmed'`;
  yield* sql`CREATE TABLE jones_workspace_lifecycle_bindings (
    worktree TEXT PRIMARY KEY NOT NULL,
    transition_id TEXT NOT NULL REFERENCES jones_workspace_lifecycle_transitions(transition_id)
  )`;
  yield* sql`CREATE TRIGGER jones_workspace_lifecycle_identity BEFORE UPDATE ON jones_workspace_lifecycle_transitions
    WHEN NEW.transition_id IS NOT OLD.transition_id OR NEW.worktree IS NOT OLD.worktree OR
      NEW.source_json IS NOT OLD.source_json OR NEW.binding_json IS NOT OLD.binding_json OR
      NEW.expected_generation IS NOT OLD.expected_generation OR
      (OLD.state='confirmed' AND (NEW.state IS NOT OLD.state OR NEW.confirmed_generation IS NOT OLD.confirmed_generation)) OR
      (OLD.state='invoking' AND NEW.state='prepared')
    BEGIN SELECT RAISE(ABORT,'Workspace lifecycle transition identity is immutable'); END`;
  yield* sql`CREATE TRIGGER jones_workspace_lifecycle_no_delete BEFORE DELETE ON jones_workspace_lifecycle_transitions
    BEGIN SELECT RAISE(ABORT,'Workspace lifecycle transitions must be retained'); END`;
  yield* sql`CREATE TRIGGER jones_workspace_lifecycle_no_replace BEFORE INSERT ON jones_workspace_lifecycle_transitions
    WHEN EXISTS(SELECT 1 FROM jones_workspace_lifecycle_transitions WHERE transition_id=NEW.transition_id)
    BEGIN SELECT RAISE(IGNORE); END`;
});

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const storageError = (cause: unknown) =>
    isLifecycleError(cause) ? cause : new LifecycleError({ reason: "storage", cause });
  const read = Effect.fn("WorkspaceLifecycleTransitionStore.read")(function* (id: string) {
    const rows = yield* sql<{
      transition_id: string;
      worktree: string;
      source_json: string;
      binding_json: string;
      expected_generation: string | null;
      confirmed_generation: string | null;
      state: "prepared" | "invoking" | "confirmed";
    }>`SELECT * FROM jones_workspace_lifecycle_transitions WHERE transition_id=${id}`;
    const row = rows[0];
    if (!row) return null;
    const binding = yield* decodeBinding(row.binding_json);
    if (binding.worktree !== row.worktree) return yield* new LifecycleError({ reason: "identity" });
    return yield* decodeTransition({
      id: row.transition_id,
      source: row.source_json,
      binding,
      expectedGeneration: row.expected_generation,
      generation: row.confirmed_generation,
      state: row.state,
    });
  }, Effect.mapError(storageError));
  const original = Effect.fn("WorkspaceLifecycleTransitionStore.original")(function* (
    transition: Transition,
  ) {
    const row = yield* read(transition.id);
    if (
      !row ||
      row.source !== transition.source ||
      canonical(row.binding) !== canonical(transition.binding) ||
      row.expectedGeneration !== transition.expectedGeneration
    )
      return yield* new LifecycleError({ reason: "identity" });
    return row;
  });
  const prior = Effect.fn("WorkspaceLifecycleTransitionStore.prior")(function* (worktree: string) {
    const rows = yield* sql<{
      transition_id: string;
    }>`SELECT transition_id FROM jones_workspace_lifecycle_bindings WHERE worktree=${worktree}`;
    return rows[0] ? yield* read(rows[0].transition_id) : null;
  }, Effect.mapError(storageError));
  return TransitionStore.of({
    outsideTransaction: Effect.gen(function* () {
      if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
        return yield* new LifecycleError({ reason: "transaction" });
    }),
    read,
    prior,
    current: Effect.fn("WorkspaceLifecycleTransitionStore.current")(function* (worktree) {
      const pending =
        yield* sql`SELECT transition_id FROM jones_workspace_lifecycle_transitions WHERE worktree=${worktree} AND state!='confirmed' LIMIT 1`;
      if (pending.length > 0) return yield* new LifecycleError({ reason: "unknown" });
      return yield* prior(worktree);
    }, Effect.mapError(storageError)),
    prepare: Effect.fn("WorkspaceLifecycleTransitionStore.prepare")(function* (transition) {
      return yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const prior = yield* read(transition.id);
            if (prior !== null) return yield* original(transition);
            if (transition.state !== "prepared" || transition.generation !== null)
              return yield* new LifecycleError({ reason: "identity" });
            const pending =
              yield* sql`SELECT transition_id FROM jones_workspace_lifecycle_transitions WHERE worktree=${transition.binding.worktree} AND state!='confirmed' LIMIT 1`;
            if (pending.length > 0) return yield* new LifecycleError({ reason: "unknown" });
            yield* sql`INSERT INTO jones_workspace_lifecycle_transitions VALUES (
          ${transition.id},${transition.binding.worktree},${transition.source},${canonical(transition.binding)},
          ${transition.expectedGeneration},NULL,'prepared')`;
            return yield* original(transition);
          }),
        )
        .pipe(Effect.mapError(storageError));
    }),
    begin: Effect.fn("WorkspaceLifecycleTransitionStore.begin")(function* (transition) {
      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            if ((yield* original(transition)).state !== "prepared")
              return yield* new LifecycleError({ reason: "unknown" });
            yield* sql`UPDATE jones_workspace_lifecycle_transitions SET state='invoking' WHERE transition_id=${transition.id} AND state='prepared'`;
          }),
        )
        .pipe(Effect.mapError(storageError));
    }),
    confirm: Effect.fn("WorkspaceLifecycleTransitionStore.confirm")(
      function* (transition, generation) {
        return yield* sql
          .withTransaction(
            Effect.gen(function* () {
              const prior = yield* original(transition);
              if (
                prior.state === "prepared" ||
                (prior.generation !== null && prior.generation !== generation)
              )
                return yield* new LifecycleError({ reason: "conflict" });
              if (prior.state === "confirmed") return prior;
              yield* sql`UPDATE jones_workspace_lifecycle_transitions SET state='confirmed',confirmed_generation=${generation} WHERE transition_id=${transition.id}`;
              yield* sql`INSERT INTO jones_workspace_lifecycle_bindings VALUES (${transition.binding.worktree},${transition.id})
          ON CONFLICT(worktree) DO UPDATE SET transition_id=excluded.transition_id`;
              return { ...prior, state: "confirmed" as const, generation };
            }),
          )
          .pipe(Effect.mapError(storageError));
      },
    ),
  });
});
export const layer = Layer.effect(TransitionStore, make);
