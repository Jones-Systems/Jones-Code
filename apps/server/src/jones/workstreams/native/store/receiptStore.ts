// @effect-diagnostics nodeBuiltinImport:off -- SHA-256 binds the exact retained request bytes for idempotency.
import * as NodeCrypto from "node:crypto";
import { WorkstreamReceipt } from "@t3tools/contracts";
import type { WorkstreamsNativeReceiptLookup } from "@t3tools/contracts/jones/workstreamsNative";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import * as KeyProvider from "../crypto/keyProvider.ts";
import * as ArchiveStore from "./archiveStore.ts";
import * as NativeSql from "./sql.ts";

export interface ReceiptRecord {
  readonly receipt: WorkstreamReceipt;
  readonly request_body: string;
  readonly origin: "native" | "legacy_open";
}
const Payload = Schema.Struct({ receipt: WorkstreamReceipt, request_body: Schema.String });
const decode = Schema.decodeUnknownEffect(Payload);
type Failure = SqlError | NativeSql.StoreError | NativeSql.CryptoError;
export class ReceiptStore extends Context.Service<
  ReceiptStore,
  {
    readonly read: (
      scope: NativeSql.OwnerScope,
      commandId: string,
    ) => Effect.Effect<ReceiptRecord | null, Failure>;
    readonly save: (
      scope: NativeSql.OwnerScope,
      value: ReceiptRecord,
      registryVersion: number | null,
    ) => Effect.Effect<ReceiptRecord, Failure>;
    readonly lookup: (
      scope: NativeSql.OwnerScope,
      commandId: string,
      expectedGeneration: number,
    ) => Effect.Effect<WorkstreamsNativeReceiptLookup, Failure>;
  }
>()("t3/jones/workstreams/native/store/receiptStore") {}
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const db = yield* NativeSql.NativeSql;
  const archive = yield* ArchiveStore.ArchiveStore;
  const keys = yield* KeyProvider.WorkstreamsKeyProvider;
  const read = Effect.fn("ReceiptStore.read")((scope: NativeSql.OwnerScope, commandId: string) =>
    db.transaction(
      scope,
      Effect.gen(function* () {
        const rows = yield* sql<{
          envelope: string;
          registry_version: number | null;
          origin: "native" | "legacy_open";
          request_sha256: string;
          principal_id: string;
          server_generation: number;
          state: string;
        }>`SELECT * FROM jones_workstreams_receipts WHERE registry_instance_id = ${scope.registry_instance_id} AND owner_id = ${scope.owner_id} AND command_id = ${commandId}`;
        const row = rows[0];
        if (!row) return null;
        const value = yield* db.openJson(
          {
            ...scope,
            purpose: "receipt",
            record_id: commandId,
            revision: row.registry_version ?? 0,
          },
          row.envelope,
        );
        const payload = yield* decode(value).pipe(
          Effect.mapError(() => new NativeSql.StoreError({ reason: "invalid_payload" })),
        );
        if (
          payload.receipt.owner_id !== scope.owner_id ||
          payload.receipt.command_id !== commandId ||
          payload.receipt.actor.principal_id !== row.principal_id ||
          payload.receipt.request_sha256 !== row.request_sha256 ||
          payload.receipt.server_generation !== row.server_generation ||
          payload.receipt.state !== row.state
        )
          return yield* new NativeSql.StoreError({ reason: "invalid_payload" });
        return { ...payload, origin: row.origin };
      }),
    ),
  );
  const save = Effect.fn("ReceiptStore.save")(
    (scope: NativeSql.OwnerScope, input: ReceiptRecord, registryVersion: number | null) =>
      db.transaction(
        scope,
        Effect.gen(function* () {
          const payload = yield* decode(input).pipe(
            Effect.mapError(() => new NativeSql.StoreError({ reason: "invalid_payload" })),
          );
          const value = payload.receipt;
          if (
            value.owner_id !== scope.owner_id ||
            NodeCrypto.createHash("sha256").update(payload.request_body).digest("hex") !==
              value.request_sha256 ||
            (value.state === "committed" && value.registry_version !== registryVersion)
          )
            return yield* new NativeSql.StoreError({ reason: "invalid_payload" });
          const old = yield* read(scope, value.command_id);
          const legacy = yield* archive.receipt(scope, value.command_id);
          if (legacy) return yield* new NativeSql.StoreError({ reason: "idempotency_conflict" });
          if (old) {
            if (
              old.request_body !== payload.request_body ||
              old.receipt.actor.principal_id !== value.actor.principal_id ||
              old.receipt.server_generation !== value.server_generation ||
              old.receipt.operation !== value.operation ||
              old.receipt.accepted_at !== value.accepted_at ||
              old.origin !== input.origin
            )
              return yield* new NativeSql.StoreError({ reason: "idempotency_conflict" });
            if (old.receipt.state === "committed" || old.receipt.state === "rejected") return old;
          }
          const envelope = yield* db.sealJson(
            {
              ...scope,
              purpose: "receipt",
              record_id: value.command_id,
              revision: registryVersion ?? 0,
            },
            payload,
          );
          const completed =
            value.state === "committed" || value.state === "rejected" ? value.completed_at : null;
          if (!old)
            yield* sql`INSERT INTO jones_workstreams_receipts ${sql.insert({ ...scope, command_id: value.command_id, principal_id: value.actor.principal_id, operation: value.operation, state: value.state, request_sha256: value.request_sha256, server_generation: value.server_generation, registry_version: registryVersion, accepted_at: value.accepted_at, completed_at: completed, origin: input.origin, envelope })}`;
          else
            yield* sql`UPDATE jones_workstreams_receipts SET state = ${value.state}, registry_version = ${registryVersion}, completed_at = ${completed}, envelope = ${envelope} WHERE registry_instance_id = ${scope.registry_instance_id} AND owner_id = ${scope.owner_id} AND command_id = ${value.command_id}`;
          return { ...payload, origin: input.origin };
        }),
      ),
  );
  const lookup = Effect.fn("ReceiptStore.lookup")(
    (scope: NativeSql.OwnerScope, commandId: string, expectedGeneration: number) =>
      db
        .transaction(
          scope,
          Effect.gen(function* (): Effect.fn.Return<WorkstreamsNativeReceiptLookup, Failure> {
            const contexts = yield* sql<{
              server_generation: number;
              registry_version: number;
              lineage: string;
              state: string;
            }>`SELECT server_generation, registry_version, lineage, state FROM jones_workstreams_registries WHERE registry_instance_id = ${scope.registry_instance_id} AND owner_id = ${scope.owner_id}`;
            const context = contexts[0];
            if (!context) return { kind: "unknown", reason: "import_incomplete" };
            if (context.server_generation !== expectedGeneration)
              return { kind: "unknown", reason: "generation_mismatch" };
            yield* keys.get(yield* keys.activeKeyId(scope));
            const native = yield* read(scope, commandId);
            if (native)
              return {
                kind: "present",
                source: native.origin === "legacy_open" ? "legacy_open" : "native",
                receipt: native.receipt,
              };
            const legacy = yield* archive.receipt(scope, commandId);
            if (legacy) return { kind: "present", source: "legacy_archive", receipt: legacy };
            const reservations =
              yield* sql`SELECT command_id FROM jones_workstreams_external_reservations WHERE registry_instance_id = ${scope.registry_instance_id} AND owner_id = ${scope.owner_id} AND command_id = ${commandId}`;
            if (reservations.length || context.state !== "active")
              return { kind: "unknown", reason: "import_incomplete" };
            const coverage =
              context.lineage === "native" ? "complete" : yield* archive.coverage(scope);
            if (coverage !== "complete") return { kind: "unknown", reason: coverage };
            return {
              kind: "proven_absent",
              witness: {
                server_generation: context.server_generation,
                registry_version: context.registry_version,
                archive: "complete",
              },
            };
          }),
        )
        .pipe(
          Effect.catchTags({
            KeyUnavailable: () =>
              Effect.succeed<WorkstreamsNativeReceiptLookup>({
                kind: "unknown",
                reason: "key_unavailable",
              }),
          }),
        ),
  );
  return ReceiptStore.of({ read, save, lookup });
});
/** @public Canonical service layer. */
export const layer = Layer.effect(ReceiptStore, make);
