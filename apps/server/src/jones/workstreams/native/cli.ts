import * as Context from "effect/Context";
import * as Console from "effect/Console";
import { Command, Flag } from "effect/cli";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import {
  WorkstreamsNativeRegistry,
  WorkstreamsNativeId,
  closedWorkstreamsNative,
} from "@t3tools/contracts/jones/workstreamsNative";
import * as Keys from "./crypto/keyProvider.ts";
import * as Store from "./store/registryStore.ts";
import * as Binding from "./store/bindingStore.ts";
import * as NativeSql from "./store/sql.ts";
import { RegistryError } from "./registry/session.ts";

const RegistryInit = closedWorkstreamsNative(
  Schema.Struct({
    registry: WorkstreamsNativeRegistry,
    principal_id: WorkstreamsNativeId,
    content_key_id: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9._:-]{1,128}$/)),
    local_key_id: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9._:-]{1,128}$/)),
  }),
);
export type RegistryInit = typeof RegistryInit.Type;
export class NativeAdmin extends Context.Service<
  NativeAdmin,
  {
    readonly initialize: (
      input: RegistryInit,
    ) => Effect.Effect<Store.RegistryContext, RegistryError>;
  }
>()("t3/jones/workstreams/native/cli/NativeAdmin") {}
/** The CLI boundary supplies the explicitly selected database and key provider. Reads never initialize either. */
const decodeInit = Schema.decodeUnknownEffect(RegistryInit);
const isRegistryError = Schema.is(RegistryError);
const isKeyUnavailable = Schema.is(Keys.KeyUnavailable);
const decodeInitJson = Schema.decodeUnknownEffect(Schema.fromJsonString(RegistryInit));
const encodeRegistryJson = Schema.encodeEffect(Schema.fromJsonString(WorkstreamsNativeRegistry));
export const layer = Layer.effect(
  NativeAdmin,
  Effect.gen(function* () {
    const store = yield* Store.RegistryStore,
      bindings = yield* Binding.BindingStore,
      db = yield* NativeSql.NativeSql,
      keys = yield* Keys.WorkstreamsKeyProvider,
      sql = yield* SqlClient.SqlClient;
    return NativeAdmin.of({
      initialize: Effect.fn("NativeAdmin.initialize")((input) =>
        db
          .transaction(
            input.registry,
            Effect.gen(function* () {
              yield* decodeInit(input).pipe(
                Effect.mapError((cause) => new RegistryError({ code: "invalid_request", cause })),
              );
              if (
                input.registry.server_generation !== 1 ||
                input.content_key_id === input.local_key_id
              )
                return yield* new RegistryError({ code: "invalid_request" });
              if ((yield* store.context(input.registry)) || (yield* bindings.read()))
                return yield* new RegistryError({ code: "already_initialized" });
              // Key provisioning is explicit and external. Init verifies both content keys and the index key.
              yield* keys.get(input.content_key_id);
              yield* keys.get(input.local_key_id);
              yield* keys.indexKey(input.registry.registry_instance_id);
              const scope = {
                registry_instance_id: input.registry.registry_instance_id,
                owner_id: input.registry.owner_id,
              };
              const at = DateTime.formatIso(yield* DateTime.now);
              const context: Store.RegistryContext = {
                ...input.registry,
                registry_version: 0,
                lineage: "native",
                active_key_id: input.content_key_id,
                state: "active",
              };
              yield* store.initialize(context);
              for (const [key_id, scope] of [
                [input.content_key_id, "home"],
                [input.local_key_id, "local"],
              ] as const)
                yield* sql`INSERT INTO jones_workstreams_keys ${sql.insert({ key_id, registry_instance_id: input.registry.registry_instance_id, scope, state: "active", created_at: at })}`;
              yield* store.appendPlain(scope, "principals", {
                principal_id: input.principal_id,
                environment_id: input.registry.home_environment_id,
                kind: "environment",
                provenance: '{"kind":"native-init"}',
              });
              yield* store.appendPlain(scope, "grants", {
                principal_id: input.principal_id,
                grant_version: 1,
                grant_id: "grant:" + input.principal_id,
                permissions: '["workstreams:read","workstreams:write"]',
                state: "active",
              });
              yield* bindings.bind(
                {
                  ...scope,
                  home_environment_id: input.registry.home_environment_id,
                  home_generation: 1,
                  principal_id: input.principal_id,
                  binding_version: 1,
                  state: "active",
                },
                null,
              );
              return (yield* store.context(input.registry))!;
            }),
          )
          .pipe(
            Effect.mapError((cause) =>
              isRegistryError(cause)
                ? cause
                : new RegistryError({
                    code: isKeyUnavailable(cause) ? "key_unavailable" : "unavailable",
                    cause,
                  }),
            ),
          ),
      ),
    });
  }),
);

export interface InitSelection {
  readonly home: string;
  readonly keyProvider: string;
}
/** A CLI boundary factory: the parent binds the selected home and provider before opening SQLite. */
export const registryCommand = <E, R>(
  bootstrap: (selection: InitSelection) => Layer.Layer<NativeAdmin, E, R>,
) =>
  Command.make("registry").pipe(
    Command.withDescription("Manage an explicitly selected native Workstreams home."),
    Command.withSubcommands([
      Command.make("init", {
        home: Flag.String("home"),
        keyProvider: Flag.String("key-provider"),
        request: Flag.String("request"),
      }).pipe(
        Command.withDescription(
          "Initialize a new native registry with explicitly provisioned keys.",
        ),
        Command.withHandler((flags) =>
          Effect.gen(function* () {
            const input = yield* decodeInitJson(flags.request, { onExcessProperty: "error" });
            const context = yield* Effect.gen(function* () {
              return yield* (yield* NativeAdmin).initialize(input);
            }).pipe(
              Effect.provide(bootstrap({ home: flags.home, keyProvider: flags.keyProvider })),
            );
            yield* Console.log(yield* encodeRegistryJson(context));
          }),
        ),
      ),
    ]),
  );
