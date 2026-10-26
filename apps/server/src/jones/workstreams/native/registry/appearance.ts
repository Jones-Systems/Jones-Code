// @effect-diagnostics nodeBuiltinImport:off -- Appearance receipts bind canonical request digests.
import * as NodeCrypto from "node:crypto";
import * as Wire from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { NativeHome, type AdmittedCaller } from "../mode/authorityMode.ts";
import * as NativeSql from "../store/sql.ts";
import * as RegistryStore from "../store/registryStore.ts";
import * as Registry from "./service.ts";
import { RegistryError } from "./session.ts";
import { canonicalJson } from "../domain/validation.ts";
export class Appearance extends Context.Service<
  Appearance,
  {
    readonly read: (
      caller: AdmittedCaller,
      ids: readonly string[],
    ) => Effect.Effect<Wire.WorkstreamAppearanceResult, RegistryError>;
    readonly save: (
      caller: AdmittedCaller,
      input: Wire.WorkstreamAppearanceWrite,
    ) => Effect.Effect<Wire.WorkstreamAppearance, RegistryError>;
  }
>()("t3/jones/workstreams/native/registry/appearance") {}
const decodeAppearanceRead = Schema.decodeUnknownEffect(Wire.WorkstreamAppearanceRead);
const decodeAppearance = Schema.decodeUnknownEffect(Wire.WorkstreamAppearance);
const isRegistryError = Schema.is(RegistryError);
const decodeAppearanceWrite = Schema.decodeUnknownEffect(Wire.WorkstreamAppearanceWrite);
export const layer = Layer.effect(
  Appearance,
  Effect.gen(function* () {
    const home = yield* NativeHome,
      db = yield* NativeSql.NativeSql,
      store = yield* RegistryStore.RegistryStore,
      registry = yield* Registry.Registry;
    const scope = { registry_instance_id: home.registry_instance_id, owner_id: home.owner_id };
    const read = Effect.fn("NativeAppearance.read")(
      (caller: AdmittedCaller, ids: readonly string[]) =>
        db
          .transaction(
            scope,
            Effect.gen(function* () {
              yield* decodeAppearanceRead({ workstream_ids: ids }).pipe(
                Effect.mapError((cause) => new RegistryError({ code: "invalid_request", cause })),
              );
              const s = yield* registry.snapshot(caller);
              const rows = yield* store.list(scope, "appearance");
              const items: Wire.WorkstreamAppearance[] = [];
              for (const id of ids) {
                if (!s.state.workstreams.has(id))
                  return yield* new RegistryError({ code: "not_found" });
                const row = rows.find((x) => x.metadata.workstream_id === id);
                items.push(
                  row
                    ? yield* decodeAppearance(row.value).pipe(
                        Effect.mapError(
                          (cause) => new RegistryError({ code: "unavailable", cause }),
                        ),
                      )
                    : { workstream_id: id, border_color: null, version: 0 },
                );
              }
              return {
                supported: true as const,
                page: {
                  owner_id: home.owner_id,
                  server_generation: s.context.server_generation,
                  permissions: s.authorization.grant.permissions,
                  items,
                },
              };
            }),
          )
          .pipe(
            Effect.mapError((cause) =>
              isRegistryError(cause) ? cause : new RegistryError({ code: "unavailable", cause }),
            ),
          ),
    );
    const save = Effect.fn("NativeAppearance.save")(
      (caller: AdmittedCaller, input: Wire.WorkstreamAppearanceWrite) =>
        db
          .transaction(
            scope,
            Effect.gen(function* () {
              const s = yield* registry.snapshot(caller, "workstreams:write");
              yield* decodeAppearanceWrite(input).pipe(
                Effect.mapError((cause) => new RegistryError({ code: "invalid_request", cause })),
              );
              if (input.expected_server_generation !== s.context.server_generation)
                return yield* new RegistryError({ code: "generation_conflict" });
              const digest = NodeCrypto.createHash("sha256")
                .update(canonicalJson(input))
                .digest("hex");
              const saved = (yield* store.list(scope, "appearance_receipts")).find(
                (x) => x.metadata.command_id === input.command_id,
              );
              if (saved) {
                if (
                  saved.metadata.principal_id !== caller.principal_id ||
                  saved.metadata.request_sha256 !== digest
                )
                  return yield* new RegistryError({ code: "idempotency_conflict" });
                return yield* decodeAppearance(saved.value).pipe(
                  Effect.mapError((cause) => new RegistryError({ code: "unavailable", cause })),
                );
              }
              if (!s.state.workstreams.has(input.workstream_id))
                return yield* new RegistryError({ code: "not_found" });
              const old = (yield* store.list(scope, "appearance")).find(
                (x) => x.metadata.workstream_id === input.workstream_id,
              );
              const version = Number(old?.metadata.version ?? 0);
              if (input.expected_version !== version)
                return yield* new RegistryError({ code: "version_conflict" });
              const value = {
                workstream_id: input.workstream_id,
                border_color: input.border_color,
                version: version + 1,
              };
              yield* store.put(
                scope,
                "appearance",
                { metadata: { workstream_id: input.workstream_id, version: value.version }, value },
                old ? version : null,
              );
              yield* store.put(
                scope,
                "appearance_receipts",
                {
                  metadata: {
                    command_id: input.command_id,
                    principal_id: caller.principal_id,
                    request_sha256: digest,
                    origin: "native",
                  },
                  value,
                },
                null,
              );
              return value;
            }),
          )
          .pipe(
            Effect.mapError((cause) =>
              isRegistryError(cause) ? cause : new RegistryError({ code: "unavailable", cause }),
            ),
          ),
    );
    return Appearance.of({ read, save });
  }),
);
