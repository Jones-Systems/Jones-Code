import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { WorkstreamOwnerGrant } from "@t3tools/contracts";
import type { AuthorizedOwner } from "../domain/state.ts";
import { NativeHome, type AdmittedCaller } from "../mode/authorityMode.ts";
import * as RegistryStore from "../store/registryStore.ts";
import * as BindingStore from "../store/bindingStore.ts";
import * as NativeSql from "../store/sql.ts";

export class RegistryError extends Schema.TaggedError<RegistryError>()("NativeRegistryError", {
  code: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {
  override get message() {
    return "Native Workstreams request refused.";
  }
}

export class Session extends Context.Service<
  Session,
  {
    readonly authorize: (
      caller: AdmittedCaller,
      permission: "workstreams:read" | "workstreams:write",
    ) => Effect.Effect<
      { readonly context: RegistryStore.RegistryContext; readonly authorization: AuthorizedOwner },
      RegistryError
    >;
  }
>()("t3/jones/workstreams/native/registry/session") {}

const decodeGrant = Schema.decodeUnknownEffect(WorkstreamOwnerGrant);
const decodePermissionsJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(WorkstreamOwnerGrant.fields.permissions),
);
const isRegistryError = Schema.is(RegistryError);
const make = Effect.gen(function* () {
  const home = yield* NativeHome;
  const scope = { registry_instance_id: home.registry_instance_id, owner_id: home.owner_id };
  const registry = yield* RegistryStore.RegistryStore;
  const bindings = yield* BindingStore.BindingStore;
  const db = yield* NativeSql.NativeSql;
  return Session.of({
    authorize: Effect.fn("NativeSession.authorize")((caller, permission) =>
      db
        .transaction(
          scope,
          Effect.gen(function* () {
            const context = yield* registry.context(scope);
            if (!context) return yield* new RegistryError({ code: "unavailable" });
            const binding = yield* bindings.read();
            if (
              !binding ||
              binding.state !== "active" ||
              binding.principal_id !== caller.principal_id ||
              binding.registry_instance_id !== home.registry_instance_id ||
              binding.owner_id !== home.owner_id ||
              binding.home_environment_id !== home.home_environment_id ||
              binding.home_generation !== home.server_generation ||
              binding.binding_version !== caller.binding_version ||
              caller.source_environment_id !== home.served_by_environment_id
            )
              return yield* new RegistryError({ code: "forbidden" });
            const principals = yield* registry.listPlain(scope, "principals");
            if (
              !principals.some(
                (p) =>
                  p.principal_id === caller.principal_id &&
                  p.environment_id === caller.source_environment_id &&
                  p.kind === "environment",
              )
            )
              return yield* new RegistryError({ code: "forbidden" });
            const grants = (yield* registry.listPlain(scope, "grants"))
              .filter((g) => g.principal_id === caller.principal_id)
              .toSorted((a, b) => Number(b.grant_version) - Number(a.grant_version));
            const row = grants[0];
            if (!row) return yield* new RegistryError({ code: "forbidden" });
            const grant = yield* decodeGrant({
              ...row,
              owner_id: home.owner_id,
              permissions: yield* decodePermissionsJson(String(row.permissions)).pipe(
                Effect.mapError((cause) => new RegistryError({ code: "forbidden", cause })),
              ),
            }).pipe(Effect.mapError((cause) => new RegistryError({ code: "forbidden", cause })));
            if (grant.state !== "active" || !grant.permissions.includes(permission))
              return yield* new RegistryError({ code: "forbidden" });
            if (context.state !== "active")
              return yield* new RegistryError({
                code: context.state === "importing" ? "import_incomplete" : "fenced",
              });
            if (
              context.home_environment_id !== home.home_environment_id ||
              home.served_by_environment_id !== home.home_environment_id
            )
              return yield* new RegistryError({ code: "home_route_required" });
            if (context.server_generation !== home.server_generation)
              return yield* new RegistryError({ code: "generation_conflict" });
            if (context.canonical_origin !== home.canonical_origin)
              return yield* new RegistryError({ code: "registry_mismatch" });
            return {
              context,
              authorization: {
                owner_id: home.owner_id,
                actor: { principal_id: caller.principal_id },
                grant,
                server_generation: context.server_generation,
              },
            };
          }),
        )
        .pipe(
          Effect.mapError((cause) =>
            isRegistryError(cause) ? cause : new RegistryError({ code: "unavailable", cause }),
          ),
        ),
    ),
  });
});
export const layer = Layer.effect(Session, make);
