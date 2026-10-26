// @effect-diagnostics nodeBuiltinImport:off -- Node AES-GCM supplies authenticated encryption, which Effect Crypto does not expose.
import { canonicalWorkstreamsSnapshotJson } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as NodeCrypto from "node:crypto";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as KeyProvider from "./keyProvider.ts";

export interface EnvelopeContext extends KeyProvider.KeyScope {
  readonly purpose: string;
  readonly record_id: string;
  readonly revision: number;
}
export class EnvelopeError extends Schema.TaggedError<EnvelopeError>()("EnvelopeError", {
  reason: Schema.Literals([
    "invalid_context",
    "malformed",
    "authentication_failed",
    "encryption_failed",
  ]),
}) {
  override get message() {
    return "Workstreams encrypted envelope failed.";
  }
}
export class WorkstreamsEnvelope extends Context.Service<
  WorkstreamsEnvelope,
  {
    readonly seal: (
      context: EnvelopeContext,
      plaintext: string,
    ) => Effect.Effect<string, KeyProvider.KeyUnavailable | EnvelopeError>;
    readonly open: (
      context: EnvelopeContext,
      envelope: string,
    ) => Effect.Effect<string, KeyProvider.KeyUnavailable | EnvelopeError>;
  }
>()("t3/jones/workstreams/native/crypto/envelope/WorkstreamsEnvelope") {}

const aad = (context: EnvelopeContext, keyId: string): Uint8Array => {
  if (
    !Number.isSafeInteger(context.revision) ||
    context.revision < 0 ||
    !context.registry_instance_id ||
    !context.owner_id ||
    !context.purpose ||
    !context.record_id ||
    !keyId
  ) {
    throw new Error("invalid context");
  }
  return Buffer.from(
    canonicalWorkstreamsSnapshotJson([
      "jones-workstreams-envelope-v1",
      context.registry_instance_id,
      context.owner_id,
      context.purpose,
      context.record_id,
      context.revision,
      keyId,
    ]),
    "utf8",
  );
};
const base64 = (value: unknown, size?: number): Buffer => {
  if (
    typeof value !== "string" ||
    !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value)
  )
    throw new Error("base64");
  const bytes = Buffer.from(value, "base64");
  if (bytes.toString("base64") !== value || (size !== undefined && bytes.length !== size))
    throw new Error("size");
  return bytes;
};
const parse = (text: string) => {
  const value: unknown = JSON.parse(text);
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error("shape");
  const row = value as Record<string, unknown>;
  if (
    Object.keys(row).length !== 6 ||
    row.v !== 1 ||
    row.alg !== "A256GCM" ||
    typeof row.key_id !== "string" ||
    !/^[A-Za-z0-9._:-]{1,128}$/.test(row.key_id)
  )
    throw new Error("shape");
  return {
    keyId: row.key_id,
    nonce: base64(row.nonce, 12),
    ct: base64(row.ct),
    tag: base64(row.tag, 16),
  };
};
const make = Effect.gen(function* () {
  const keys = yield* KeyProvider.WorkstreamsKeyProvider;
  const seal = Effect.fn("WorkstreamsEnvelope.seal")(function* (
    context: EnvelopeContext,
    plaintext: string,
  ) {
    const keyId = yield* keys.activeKeyId(context);
    const associated = yield* Effect.try({
      try: () => aad(context, keyId),
      catch: () => new EnvelopeError({ reason: "invalid_context" }),
    });
    const key = yield* keys.get(keyId);
    return yield* Effect.try({
      try: () => {
        const nonce = NodeCrypto.randomBytes(12);
        const cipher = NodeCrypto.createCipheriv("aes-256-gcm", key, nonce);
        cipher.setAAD(associated);
        const ct = Buffer.concat([cipher.update(plaintext, "utf8"), cipher.final()]);
        return canonicalWorkstreamsSnapshotJson({
          v: 1,
          alg: "A256GCM",
          key_id: keyId,
          nonce: nonce.toString("base64"),
          ct: ct.toString("base64"),
          tag: cipher.getAuthTag().toString("base64"),
        });
      },
      catch: () => new EnvelopeError({ reason: "encryption_failed" }),
    });
  });
  const open = Effect.fn("WorkstreamsEnvelope.open")(function* (
    context: EnvelopeContext,
    text: string,
  ) {
    const envelope = yield* Effect.try({
      try: () => parse(text),
      catch: () => new EnvelopeError({ reason: "malformed" }),
    });
    const associated = yield* Effect.try({
      try: () => aad(context, envelope.keyId),
      catch: () => new EnvelopeError({ reason: "invalid_context" }),
    });
    const key = yield* keys.get(envelope.keyId);
    return yield* Effect.try({
      try: () => {
        const decipher = NodeCrypto.createDecipheriv("aes-256-gcm", key, envelope.nonce);
        decipher.setAAD(associated);
        decipher.setAuthTag(envelope.tag);
        return Buffer.concat([decipher.update(envelope.ct), decipher.final()]).toString("utf8");
      },
      catch: () => new EnvelopeError({ reason: "authentication_failed" }),
    });
  });
  return WorkstreamsEnvelope.of({ seal, open });
});
/** @public Canonical service layer. */
export const layer = Layer.effect(WorkstreamsEnvelope, make);
