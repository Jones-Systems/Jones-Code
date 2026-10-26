import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as ServerSecretStore from "../../../../auth/ServerSecretStore.ts";

export interface KeyScope {
  readonly registry_instance_id: string;
  readonly owner_id: string;
  readonly key_scope?: "home" | "local";
}
export class KeyUnavailable extends Schema.TaggedError<KeyUnavailable>()("KeyUnavailable", {
  reason: Schema.Literals(["missing", "invalid_key", "store_unavailable", "active_key_missing"]),
}) {
  override get message() {
    return "Workstreams encryption key unavailable.";
  }
}
export class WorkstreamsKeyProvider extends Context.Service<
  WorkstreamsKeyProvider,
  {
    readonly activeKeyId: (scope: KeyScope) => Effect.Effect<string, KeyUnavailable>;
    readonly get: (keyId: string) => Effect.Effect<Uint8Array, KeyUnavailable>;
    readonly indexKey: (registryId: string) => Effect.Effect<Uint8Array, KeyUnavailable>;
  }
>()("t3/jones/workstreams/native/crypto/keyProvider/WorkstreamsKeyProvider") {}

const make = Effect.gen(function* () {
  const secrets = yield* ServerSecretStore.ServerSecretStore;
  const sql = yield* SqlClient.SqlClient;
  const readKey = Effect.fn("WorkstreamsKeyProvider.readKey")(function* (name: string) {
    if (!/^[A-Za-z0-9._:-]{1,200}$/.test(name))
      return yield* new KeyUnavailable({ reason: "invalid_key" });
    const value = yield* secrets
      .get(name)
      .pipe(Effect.mapError(() => new KeyUnavailable({ reason: "store_unavailable" })));
    if (Option.isNone(value)) return yield* new KeyUnavailable({ reason: "missing" });
    if (value.value.byteLength !== 32) return yield* new KeyUnavailable({ reason: "invalid_key" });
    return Uint8Array.from(value.value);
  });
  const activeKeyId = Effect.fn("WorkstreamsKeyProvider.activeKeyId")(function* (scope: KeyScope) {
    const rows = yield* (
      scope.key_scope === "local"
        ? sql<{
            key_id: string;
          }>`SELECT key_id FROM jones_workstreams_keys WHERE registry_instance_id = ${scope.registry_instance_id} AND scope = 'local' AND state = 'active'`
        : sql<{
            key_id: string;
          }>`SELECT k.key_id FROM jones_workstreams_keys k WHERE k.registry_instance_id = ${scope.registry_instance_id} AND k.scope = 'home' AND k.state = 'active' AND (NOT EXISTS(SELECT 1 FROM jones_workstreams_registries r WHERE r.registry_instance_id = ${scope.registry_instance_id} AND r.owner_id = ${scope.owner_id}) OR EXISTS(SELECT 1 FROM jones_workstreams_registries r WHERE r.registry_instance_id = ${scope.registry_instance_id} AND r.owner_id = ${scope.owner_id} AND r.active_key_id = k.key_id))`
    ).pipe(Effect.mapError(() => new KeyUnavailable({ reason: "store_unavailable" })));
    if (rows.length !== 1 || !rows[0]?.key_id)
      return yield* new KeyUnavailable({ reason: "active_key_missing" });
    return rows[0].key_id;
  });
  const get = Effect.fn("WorkstreamsKeyProvider.get")((id: string) =>
    readKey(`workstreams-content-key-${id}`),
  );
  const indexKey = Effect.fn("WorkstreamsKeyProvider.indexKey")((id: string) =>
    readKey(`workstreams-index-key-${id}`),
  );
  return WorkstreamsKeyProvider.of({ activeKeyId, get, indexKey });
});
/** @public Canonical service layer; key creation belongs to explicit admin commands. */
export const layer = Layer.effect(WorkstreamsKeyProvider, make);
