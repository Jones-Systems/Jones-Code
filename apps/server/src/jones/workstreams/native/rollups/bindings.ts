import * as SqlClient from "effect/sql/SqlClient";
import { canonicalWorkstreamsSnapshotJson as recordKey } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as RegistryStore from "../store/registryStore.ts";
import * as NativeSql from "../store/sql.ts";
import { NativeHome } from "../mode/authorityMode.ts";
import { RollupError, attempt } from "./evidence.ts";
const Id = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9._:-]{1,512}$/));
const Version = Schema.Int.check(Schema.isGreaterThanOrEqualTo(1));
const Selection = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("thread"),
    conversation_id: Id,
    thread_id: Id,
    selected_branch_id: Id,
  }),
  Schema.Struct({ kind: Schema.Literal("project"), project_id: Id, project_version: Version }),
]);

export const RollupBinding = Schema.Struct({
  owner_id: Id,
  workstream_id: Id,
  native_reference_id: Id,
  provider_id: Id,
  account_id: Id,
  selection: Selection,
  binding_version: Version,
  state: Schema.Literals(["active", "revoked"]),
});
export type RollupBinding = typeof RollupBinding.Type;
const decodeRollupBinding = Schema.decodeUnknownSync(RollupBinding);

export class Bindings extends Context.Service<
  Bindings,
  {
    readonly list: () => Effect.Effect<readonly RollupBinding[], RollupError>;
    readonly put: (
      binding: RollupBinding,
      expectedVersion: number | null,
    ) => Effect.Effect<void, RollupError>;
  }
>()("t3/jones/workstreams/native/rollups/bindings") {}
const make = Effect.gen(function* () {
  const home = yield* NativeHome,
    registry = yield* RegistryStore.RegistryStore,
    db = yield* NativeSql.NativeSql,
    sql = yield* SqlClient.SqlClient;
  const scope = { registry_instance_id: home.registry_instance_id, owner_id: home.owner_id };
  const list = Effect.fn("RollupBindings.list")(() =>
    db
      .transaction(
        scope,
        Effect.gen(function* () {
          const rows = yield* sql<{
            workstream_id: string;
            native_reference_id: string;
            provider_id: string;
            state: string;
            binding_version: number;
            envelope: string;
          }>`
      SELECT * FROM jones_workstreams_rollup_bindings WHERE registry_instance_id=${scope.registry_instance_id} AND owner_id=${scope.owner_id}
      ORDER BY workstream_id COLLATE BINARY,native_reference_id COLLATE BINARY LIMIT 1001`;
          if (rows.length > 1000) return yield* new RollupError({ code: "content_unavailable" });
          const result: RollupBinding[] = [];
          for (const row of rows) {
            const raw = yield* db.openJson(
              {
                ...scope,
                purpose: "rollup_binding",
                record_id: recordKey([row.workstream_id, row.native_reference_id]),
                revision: row.binding_version,
              },
              row.envelope,
            );
            const binding = yield* attempt(() =>
              decodeRollupBinding(raw, { onExcessProperty: "error" }),
            );
            if (
              binding.owner_id !== scope.owner_id ||
              [
                "workstream_id",
                "native_reference_id",
                "provider_id",
                "state",
                "binding_version",
              ].some((k) => row[k as keyof typeof row] !== binding[k as keyof RollupBinding])
            )
              return yield* new RollupError({ code: "content_unavailable" });
            result.push(binding);
          }
          return result;
        }),
      )
      .pipe(Effect.mapError(() => new RollupError({ code: "content_unavailable" }))),
  );
  const put = Effect.fn("RollupBindings.put")(
    (binding: RollupBinding, expectedVersion: number | null) =>
      db
        .transaction(
          scope,
          Effect.gen(function* () {
            yield* attempt(
              () => decodeRollupBinding(binding, { onExcessProperty: "error" }),
              "invalid_request",
            );
            if (
              binding.owner_id !== scope.owner_id ||
              binding.binding_version !== (expectedVersion ?? 0) + 1
            )
              return yield* new RollupError({ code: "invalid_request" });
            const context = yield* registry.context(scope);
            if (
              !context ||
              context.state !== "active" ||
              context.home_environment_id !== home.home_environment_id ||
              home.served_by_environment_id !== home.home_environment_id ||
              context.server_generation !== home.server_generation
            )
              return yield* new RollupError({ code: "content_unavailable" });
            const current = yield* list();
            if (expectedVersion === null && current.length >= 1000)
              return yield* new RollupError({ code: "content_unavailable" });
            if (
              binding.state === "active" &&
              current.some(
                (b) =>
                  b.state === "active" &&
                  b.native_reference_id === binding.native_reference_id &&
                  b.workstream_id !== binding.workstream_id,
              )
            )
              return yield* new RollupError({ code: "invalid_request" });
            yield* registry.put(
              scope,
              "rollup_bindings",
              {
                metadata: {
                  workstream_id: binding.workstream_id,
                  native_reference_id: binding.native_reference_id,
                  provider_id: binding.provider_id,
                  state: binding.state,
                  binding_version: binding.binding_version,
                },
                value: binding,
              },
              expectedVersion,
            );
            // Bindings are native authority: every committed revision fences older manifests.
            yield* registry.advance(scope, context);
          }),
        )
        .pipe(Effect.mapError(() => new RollupError({ code: "content_unavailable" }))),
  );
  return Bindings.of({ list, put });
});
/** @public Home-internal binding administration; caller authorization belongs to parent composition. */
export const layer = Layer.effect(Bindings, make);
