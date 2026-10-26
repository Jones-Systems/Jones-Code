import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import * as NativeSql from "./sql.ts";

export interface Binding extends NativeSql.OwnerScope {
  readonly home_environment_id: string;
  readonly home_generation: number;
  readonly principal_id: string;
  readonly binding_version: number;
  readonly state: "active" | "fenced";
}
type Failure = SqlError | NativeSql.StoreError;
export class BindingStore extends Context.Service<
  BindingStore,
  {
    readonly read: () => Effect.Effect<Binding | null, SqlError>;
    readonly bind: (
      binding: Binding,
      expectedVersion: number | null,
    ) => Effect.Effect<void, Failure>;
    readonly assertDestination: (binding: Omit<Binding, "state">) => Effect.Effect<void, Failure>;
  }
>()("t3/jones/workstreams/native/store/bindingStore") {}
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const db = yield* NativeSql.NativeSql;
  const read = Effect.fn("BindingStore.read")(() =>
    sql<Binding>`SELECT registry_instance_id,owner_id,home_environment_id,home_generation,principal_id,binding_version,state FROM jones_workstreams_binding_state WHERE singleton = 1 AND state != 'unbound'`.pipe(
      Effect.map((rows) => rows[0] ?? null),
    ),
  );
  const bind = Effect.fn("BindingStore.bind")((binding: Binding, expectedVersion: number | null) =>
    db.transaction(
      binding,
      Effect.gen(function* () {
        const rows = yield* sql<{
          binding_version: number;
          registry_instance_id: string | null;
          owner_id: string | null;
          home_environment_id: string | null;
          home_generation: number | null;
        }>`SELECT * FROM jones_workstreams_binding_state WHERE singleton = 1`;
        const old = rows[0];
        if (
          (expectedVersion === null) !== !old ||
          (old && old.binding_version !== expectedVersion) ||
          binding.binding_version !== (expectedVersion ?? 0) + 1
        )
          return yield* new NativeSql.StoreError({ reason: "conflict" });
        if (
          old?.registry_instance_id &&
          (old.registry_instance_id !== binding.registry_instance_id ||
            old.owner_id !== binding.owner_id)
        )
          return yield* new NativeSql.StoreError({ reason: "binding_mismatch" });
        if (
          old &&
          (old.home_environment_id !== binding.home_environment_id ||
            old.home_generation !== binding.home_generation)
        ) {
          const open =
            yield* sql`SELECT operation_id FROM jones_workstreams_operations WHERE state IN ('accepted','dispatching','in_progress','reconciling') LIMIT 1`;
          if (open.length) return yield* new NativeSql.StoreError({ reason: "conflict" });
        }
        if (!old)
          yield* sql`INSERT INTO jones_workstreams_binding_state ${sql.insert({ singleton: 1, ...binding })}`;
        else
          yield* sql`UPDATE jones_workstreams_binding_state SET registry_instance_id = ${binding.registry_instance_id}, owner_id = ${binding.owner_id}, home_environment_id = ${binding.home_environment_id}, home_generation = ${binding.home_generation}, principal_id = ${binding.principal_id}, binding_version = ${binding.binding_version}, state = ${binding.state} WHERE singleton = 1`;
      }),
    ),
  );
  const assertDestination = Effect.fn("BindingStore.assertDestination")(
    (binding: Omit<Binding, "state">) =>
      db.transaction(
        binding,
        Effect.gen(function* () {
          const current = yield* read();
          if (
            !current ||
            current.state !== "active" ||
            current.registry_instance_id !== binding.registry_instance_id ||
            current.owner_id !== binding.owner_id ||
            current.home_environment_id !== binding.home_environment_id ||
            current.home_generation !== binding.home_generation ||
            current.binding_version !== binding.binding_version ||
            current.principal_id !== binding.principal_id
          )
            return yield* new NativeSql.StoreError({ reason: "binding_mismatch" });
        }),
      ),
  );
  return BindingStore.of({ read, bind, assertDestination });
});
/** @public Canonical service layer. */
export const layer = Layer.effect(BindingStore, make);
