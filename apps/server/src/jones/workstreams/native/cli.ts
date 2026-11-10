import * as Context from "effect/Context";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Option from "effect/Option";
import * as SecretStore from "../../../auth/ServerSecretStore.ts";
import * as SourceSetup from "./runtime/sourceSetup.ts";
import * as Transition from "./transition/cli.ts";
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

const KeyId = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9._:-]{1,128}$/));
const RegistryInit = closedWorkstreamsNative(
  Schema.Struct({
    registry: WorkstreamsNativeRegistry,
    principal_id: WorkstreamsNativeId,
    content_key_id: KeyId,
    local_key_id: KeyId,
  }),
);
export type RegistryInit = typeof RegistryInit.Type;
const MemberKeyInit = closedWorkstreamsNative(
  Schema.Struct({ registry: WorkstreamsNativeRegistry, local_key_id: KeyId }),
);
export type MemberKeyInit = typeof MemberKeyInit.Type;
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
const decodeMemberKeyInit = Schema.decodeUnknownEffect(MemberKeyInit);
const decodeMemberKeyInitJson = Schema.decodeUnknownEffect(Schema.fromJsonString(MemberKeyInit));
const encodeKeysJson = Schema.encodeEffect(
  Schema.fromJsonString(Schema.Struct({ created: Schema.Array(Schema.String) })),
);
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

export class NativeKeyAdmin extends Context.Service<
  NativeKeyAdmin,
  {
    readonly initialize: (
      input: RegistryInit,
    ) => Effect.Effect<{ readonly created: readonly string[] }, RegistryError>;
  }
>()("t3/jones/workstreams/native/cli/NativeKeyAdmin") {}
/** Names the KeyProvider reads for the content, local and registry index keys of `registry init`. */
export const keyNames = (input: RegistryInit) => [
  `workstreams-content-key-${input.content_key_id}`,
  `workstreams-content-key-${input.local_key_id}`,
  `workstreams-index-key-${input.registry.registry_instance_id}`,
];
/** Creates all three keys from CSPRNG bytes or none: any existing key refuses, and a failed run removes only keys it created. */
export const keyLayer = Layer.effect(
  NativeKeyAdmin,
  Effect.gen(function* () {
    const secrets = yield* SecretStore.ServerSecretStore,
      crypto = yield* SecureRandom.SecureRandom;
    const refuse = (code: string) => (cause: unknown) => new RegistryError({ code, cause });
    return NativeKeyAdmin.of({
      initialize: Effect.fn("NativeKeyAdmin.initialize")(function* (input) {
        yield* decodeInit(input).pipe(Effect.mapError(refuse("invalid_request")));
        if (input.content_key_id === input.local_key_id)
          return yield* new RegistryError({ code: "invalid_request" });
        const names = keyNames(input);
        for (const name of names)
          if (Option.isSome(yield* secrets.get(name).pipe(Effect.mapError(refuse("unavailable")))))
            return yield* new RegistryError({ code: "key_exists" });
        const created: string[] = [];
        yield* Effect.gen(function* () {
          for (const name of names) {
            const bytes = yield* crypto.randomBytes(32);
            yield* secrets
              .create(name, bytes)
              .pipe(
                Effect.mapError((cause) =>
                  refuse(
                    SecretStore.isSecretAlreadyExistsError(cause) ? "key_exists" : "unavailable",
                  )(cause),
                ),
              );
            created.push(name);
          }
        }).pipe(
          Effect.tapError(() =>
            Effect.forEach(created, (name) => secrets.remove(name).pipe(Effect.ignore), {
              discard: true,
            }),
          ),
        );
        return { created: names };
      }),
    });
  }),
);

export class NativeMemberKeyAdmin extends Context.Service<
  NativeMemberKeyAdmin,
  {
    readonly initialize: (
      input: MemberKeyInit,
    ) => Effect.Effect<{ readonly created: readonly string[] }, RegistryError>;
  }
>()("t3/jones/workstreams/native/cli/NativeMemberKeyAdmin") {}
/**
 * A member holds no registry copy, only the active `local` key row (no FK to a registry row) and its
 * key file for the home's registry id. Any registry row or key row for that id, or an existing file,
 * refuses. The row write and the readback share one transaction; a failed run removes only its file.
 */
export const memberKeyLayer = Layer.effect(
  NativeMemberKeyAdmin,
  Effect.gen(function* () {
    const secrets = yield* SecretStore.ServerSecretStore,
      crypto = yield* SecureRandom.SecureRandom,
      sql = yield* SqlClient.SqlClient;
    const refuse = (code: string) => (cause: unknown) => new RegistryError({ code, cause });
    return NativeMemberKeyAdmin.of({
      initialize: Effect.fn("NativeMemberKeyAdmin.initialize")(function* (input) {
        yield* decodeMemberKeyInit(input).pipe(Effect.mapError(refuse("invalid_request")));
        const key_id = input.local_key_id,
          registry_instance_id = input.registry.registry_instance_id,
          name = `workstreams-content-key-${key_id}`;
        let created = false;
        yield* sql
          .withTransaction(
            Effect.gen(function* () {
              const [existing] = yield* sql<{ registries: number; keys: number }>`SELECT
                (SELECT count(*) FROM jones_workstreams_registries WHERE registry_instance_id = ${registry_instance_id}) AS registries,
                (SELECT count(*) FROM jones_workstreams_keys WHERE key_id = ${key_id} OR registry_instance_id = ${registry_instance_id}) AS keys`;
              if (existing!.registries !== 0)
                return yield* new RegistryError({ code: "already_initialized" });
              if (existing!.keys !== 0) return yield* new RegistryError({ code: "key_exists" });
              if (
                Option.isSome(yield* secrets.get(name).pipe(Effect.mapError(refuse("unavailable"))))
              )
                return yield* new RegistryError({ code: "key_exists" });
              const bytes = yield* crypto.randomBytes(32);
              yield* secrets
                .create(name, bytes)
                .pipe(
                  Effect.mapError((cause) =>
                    refuse(
                      SecretStore.isSecretAlreadyExistsError(cause) ? "key_exists" : "unavailable",
                    )(cause),
                  ),
                );
              created = true;
              const at = DateTime.formatIso(yield* DateTime.now);
              yield* sql`INSERT INTO jones_workstreams_keys ${sql.insert({ key_id, registry_instance_id, scope: "local", state: "active", created_at: at })}`;
              // The no-replace trigger ignores a duplicate key id silently; read the row back.
              const rows = yield* sql<{ key_id: string }>`SELECT key_id FROM jones_workstreams_keys
                WHERE registry_instance_id = ${registry_instance_id} AND scope = 'local' AND state = 'active'`;
              if (rows.length !== 1 || rows[0]!.key_id !== key_id)
                return yield* new RegistryError({ code: "unavailable" });
            }),
          )
          .pipe(
            Effect.mapError((cause) =>
              isRegistryError(cause) ? cause : new RegistryError({ code: "unavailable", cause }),
            ),
            Effect.tapError(() =>
              created ? secrets.remove(name).pipe(Effect.ignore) : Effect.void,
            ),
          );
        return { created: [name] };
      }),
    });
  }),
);

export interface InitSelection {
  readonly home: string;
  readonly keyProvider: string;
}
/** A CLI boundary factory: the parent binds the selected home and provider before opening SQLite. */
export const registryCommand = <E, R, SE, SR, TE, TR, KE, KR, ME, MR>(
  bootstrap: (selection: InitSelection) => Layer.Layer<NativeAdmin, E, R>,
  sourceBootstrap: (
    selection: InitSelection,
    action: "plan" | "apply" | "readback",
  ) => Layer.Layer<SourceSetup.NativeSourceSetup, SE, SR>,
  transitionBootstrap: (
    selection: InitSelection,
  ) => Layer.Layer<Transition.WorkstreamsTransition, TE, TR>,
  keyBootstrap: (selection: InitSelection) => Layer.Layer<NativeKeyAdmin, KE, KR>,
  memberKeyBootstrap: (selection: InitSelection) => Layer.Layer<NativeMemberKeyAdmin, ME, MR>,
) =>
  Command.make("registry").pipe(
    Command.withDescription("Manage an explicitly selected native Workstreams home."),
    Command.withSubcommands([
      SourceSetup.command(sourceBootstrap),
      Transition.command(transitionBootstrap),
      Command.make("keys").pipe(
        Command.withDescription("Provision native registry keys in the selected secret store."),
        Command.withSubcommands([
          Command.make("init", {
            home: Flag.String("home"),
            keyProvider: Flag.String("key-provider"),
            request: Flag.String("request").pipe(
              Flag.withDescription("The same registry-init JSON that `registry init` takes."),
            ),
          }).pipe(
            Command.withDescription(
              "Create the content, local and index keys for a new registry; never overwrites or prints keys.",
            ),
            Command.withHandler((flags) =>
              Effect.gen(function* () {
                const input = yield* decodeInitJson(flags.request, { onExcessProperty: "error" });
                const result = yield* Effect.gen(function* () {
                  return yield* (yield* NativeKeyAdmin).initialize(input);
                }).pipe(
                  Effect.provide(
                    keyBootstrap({ home: flags.home, keyProvider: flags.keyProvider }),
                  ),
                );
                yield* Console.log(yield* encodeKeysJson(result));
              }),
            ),
          ),
          Command.make("member-init", {
            home: Flag.String("home"),
            keyProvider: Flag.String("key-provider"),
            request: Flag.String("request").pipe(
              Flag.withDescription(
                "JSON {registry, local_key_id}: the home's registry and this member's local key id.",
              ),
            ),
          }).pipe(
            Command.withDescription(
              "Create a member's local key and its active key row for the home registry; never overwrites or prints keys.",
            ),
            Command.withHandler((flags) =>
              Effect.gen(function* () {
                const input = yield* decodeMemberKeyInitJson(flags.request, {
                  onExcessProperty: "error",
                });
                const result = yield* Effect.gen(function* () {
                  return yield* (yield* NativeMemberKeyAdmin).initialize(input);
                }).pipe(
                  Effect.provide(
                    memberKeyBootstrap({ home: flags.home, keyProvider: flags.keyProvider }),
                  ),
                );
                yield* Console.log(yield* encodeKeysJson(result));
              }),
            ),
          ),
        ]),
      ),
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
