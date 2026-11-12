// @effect-diagnostics nodeBuiltinImport:off -- Node HMAC supplies the keyed identity index, which Effect Crypto does not expose.
import * as NodeCrypto from "node:crypto";
import { WorkstreamNativeIdentity } from "@t3tools/contracts";
import { canonicalWorkstreamsSnapshotJson } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as KeyProvider from "./keyProvider.ts";

export class IdentityIndexError extends Schema.TaggedError<IdentityIndexError>()(
  "IdentityIndexError",
  {},
) {
  override get message() {
    return "Invalid Workstreams reference identity.";
  }
}
export class WorkstreamsIdentityIndex extends Context.Service<
  WorkstreamsIdentityIndex,
  {
    readonly digest: (
      scope: KeyProvider.KeyScope,
      identity: typeof WorkstreamNativeIdentity.Type,
    ) => Effect.Effect<string, KeyProvider.KeyUnavailable | IdentityIndexError>;
  }
>()("t3/jones/workstreams/native/crypto/identityIndex/WorkstreamsIdentityIndex") {}
const decodeIdentity = Schema.decodeUnknownEffect(WorkstreamNativeIdentity);
const make = Effect.gen(function* () {
  const keys = yield* KeyProvider.WorkstreamsKeyProvider;
  const digest = Effect.fn("WorkstreamsIdentityIndex.digest")(function* (
    scope: KeyProvider.KeyScope,
    identity: typeof WorkstreamNativeIdentity.Type,
  ) {
    const value = yield* decodeIdentity(identity).pipe(
      Effect.mapError(() => new IdentityIndexError({})),
    );
    const key = yield* keys.indexKey(scope.registry_instance_id);
    return NodeCrypto.createHmac("sha256", key)
      .update(
        canonicalWorkstreamsSnapshotJson([
          "jones-workstreams-identity-v1",
          scope.registry_instance_id,
          scope.owner_id,
          value,
        ]),
      )
      .digest("hex");
  });
  return WorkstreamsIdentityIndex.of({ digest });
});
/** @public Canonical service layer. */
export const layer = Layer.effect(WorkstreamsIdentityIndex, make);
