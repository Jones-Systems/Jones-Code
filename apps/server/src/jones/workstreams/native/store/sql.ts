import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import * as Envelope from "../crypto/envelope.ts";
import type { KeyScope } from "../crypto/keyProvider.ts";

export const jsonText = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

export type OwnerScope = Pick<KeyScope, "registry_instance_id" | "owner_id">;
export class StoreError extends Schema.TaggedError<StoreError>()("WorkstreamsStoreError", {
  reason: Schema.Literals([
    "owner_mismatch",
    "invalid_payload",
    "conflict",
    "not_found",
    "fenced",
    "idempotency_conflict",
    "binding_mismatch",
  ]),
}) {
  override get message() {
    return "Workstreams store operation refused.";
  }
}
export type CryptoError =
  | Envelope.EnvelopeError
  | import("../crypto/keyProvider.ts").KeyUnavailable;
const ActiveOwner = Context.Reference<OwnerScope | undefined>(
  "jones/workstreams/native/ActiveOwner",
  { defaultValue: () => undefined },
);
export class NativeSql extends Context.Service<
  NativeSql,
  {
    readonly transaction: <A, E, R>(
      scope: OwnerScope,
      effect: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | StoreError | SqlError, R>;
    readonly sealJson: (
      context: Envelope.EnvelopeContext,
      value: unknown,
    ) => Effect.Effect<string, CryptoError | StoreError>;
    readonly openJson: (
      context: Envelope.EnvelopeContext,
      envelope: string,
    ) => Effect.Effect<unknown, CryptoError | StoreError>;
  }
>()("t3/jones/workstreams/native/store/sql/NativeSql") {}
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const decodeJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Envelope.WorkstreamsEnvelope;
  const transaction: NativeSql["Service"]["transaction"] = Effect.fn("NativeSql.transaction")(
    function* <A, E, R>(scope: OwnerScope, effect: Effect.Effect<A, E, R>) {
      const current = yield* ActiveOwner;
      if (
        current &&
        (scope.registry_instance_id !== current.registry_instance_id ||
          scope.owner_id !== current.owner_id)
      ) {
        return yield* new StoreError({ reason: "owner_mismatch" });
      }
      return yield* sql.withTransaction(effect.pipe(Effect.provideService(ActiveOwner, scope)));
    },
  );
  const sealJson = Effect.fn("NativeSql.sealJson")(function* (
    context: Envelope.EnvelopeContext,
    value: unknown,
  ) {
    const bytes = yield* encodeJson(value).pipe(
      Effect.mapError(() => new StoreError({ reason: "invalid_payload" })),
    );
    return yield* crypto.seal(context, bytes);
  });
  const openJson = Effect.fn("NativeSql.openJson")(function* (
    context: Envelope.EnvelopeContext,
    envelope: string,
  ) {
    const bytes = yield* crypto.open(context, envelope);
    return yield* decodeJson(bytes).pipe(
      Effect.mapError(() => new StoreError({ reason: "invalid_payload" })),
    );
  });
  return NativeSql.of({ transaction, sealJson, openJson });
});
/** @public Canonical service layer; NodeSqliteClient supplies BEGIN IMMEDIATE. */
export const layer = Layer.effect(NativeSql, make);
