import { assert, describe, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as AuthPairingLinks from "./AuthPairingLinks.ts";
import AuthAuthorizationScopes from "./Migrations/031_AuthAuthorizationScopes.ts";
import AuthPairingProofKeyThumbprint from "./Migrations/032_AuthPairingProofKeyThumbprint.ts";

const layer = AuthPairingLinks.layer.pipe(
  Layer.provide(
    Layer.effectDiscard(
      Effect.gen(function* () {
        yield* AuthAuthorizationScopes;
        yield* AuthPairingProofKeyThumbprint;
      }),
    ).pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" }))),
  ),
);

const createdAt = DateTime.makeUnsafe("2026-10-01T00:00:00.000Z");
const now = DateTime.makeUnsafe("2026-10-01T00:01:00.000Z");
const expiresAt = DateTime.makeUnsafe("2026-10-01T00:02:00.000Z");
const pairingLink: AuthPairingLinks.CreateAuthPairingLinkInput = {
  id: "pairing-link",
  credential: "synthetic-pairing-token",
  method: "one-time-token",
  scopes: ["access:read", "access:write"],
  subject: "synthetic-subject",
  label: "Test client",
  proofKeyThumbprint: null,
  createdAt,
  expiresAt,
};
const consumeInput: AuthPairingLinks.ConsumeAuthPairingLinkInput = {
  credential: pairingLink.credential,
  proofKeyThumbprint: null,
  consumedAt: now,
  now,
};

describe("AuthPairingLinkRepository.consumeAvailable", () => {
  it.effect("consumes a link without requested scopes exactly once", () =>
    Effect.gen(function* () {
      const repository = yield* AuthPairingLinks.AuthPairingLinkRepository;
      yield* repository.create(pairingLink);

      const consumed = yield* repository.consumeAvailable(consumeInput);
      assert.deepStrictEqual(Option.getOrThrow(consumed), {
        ...pairingLink,
        consumedAt: now,
        revokedAt: null,
      });
      assert.isTrue(Option.isNone(yield* repository.consumeAvailable(consumeInput)));
      const stored = yield* repository.getByCredential({ credential: pairingLink.credential });
      assert.deepStrictEqual(Option.getOrThrow(stored).consumedAt, now);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("accepts requested scopes with a nonempty intersection", () =>
    Effect.gen(function* () {
      const repository = yield* AuthPairingLinks.AuthPairingLinkRepository;
      yield* repository.create(pairingLink);

      const consumed = yield* repository.consumeAvailable({
        ...consumeInput,
        requestedScopes: ["access:read", "terminal:read"],
      });
      assert.deepStrictEqual(Option.getOrThrow(consumed).scopes, pairingLink.scopes);
      assert.isTrue(Option.isNone(yield* repository.consumeAvailable(consumeInput)));
    }).pipe(Effect.provide(layer)),
  );

  it.effect("leaves the link available after empty or disjoint scope requests", () =>
    Effect.gen(function* () {
      const repository = yield* AuthPairingLinks.AuthPairingLinkRepository;
      yield* repository.create(pairingLink);

      for (const requestedScopes of [[], ["terminal:read"]] as const) {
        assert.isTrue(
          Option.isNone(yield* repository.consumeAvailable({ ...consumeInput, requestedScopes })),
        );
        const stored = yield* repository.getByCredential({ credential: pairingLink.credential });
        assert.isNull(Option.getOrThrow(stored).consumedAt);
      }
      assert.isTrue(
        Option.isSome(
          yield* repository.consumeAvailable({ ...consumeInput, requestedScopes: ["access:read"] }),
        ),
      );
    }).pipe(Effect.provide(layer)),
  );

  it.effect("rejects missing or mismatched proof keys without consuming the link", () =>
    Effect.gen(function* () {
      const repository = yield* AuthPairingLinks.AuthPairingLinkRepository;
      yield* repository.create({ ...pairingLink, proofKeyThumbprint: "expected-proof-key" });

      for (const proofKeyThumbprint of [null, "wrong-proof-key"]) {
        assert.isTrue(
          Option.isNone(
            yield* repository.consumeAvailable({ ...consumeInput, proofKeyThumbprint }),
          ),
        );
        const stored = yield* repository.getByCredential({ credential: pairingLink.credential });
        assert.isNull(Option.getOrThrow(stored).consumedAt);
      }
      assert.isTrue(
        Option.isSome(
          yield* repository.consumeAvailable({
            ...consumeInput,
            proofKeyThumbprint: "expected-proof-key",
          }),
        ),
      );
    }).pipe(Effect.provide(layer)),
  );

  it.effect("rejects expired and revoked links without recording consumption", () =>
    Effect.gen(function* () {
      const repository = yield* AuthPairingLinks.AuthPairingLinkRepository;
      yield* repository.create(pairingLink);
      assert.isTrue(
        Option.isNone(yield* repository.consumeAvailable({ ...consumeInput, now: expiresAt })),
      );
      assert.isTrue(yield* repository.revoke({ id: pairingLink.id, revokedAt: now }));
      assert.isTrue(Option.isNone(yield* repository.consumeAvailable(consumeInput)));
      const stored = yield* repository.getByCredential({ credential: pairingLink.credential });
      assert.isNull(Option.getOrThrow(stored).consumedAt);
      assert.deepStrictEqual(Option.getOrThrow(stored).revokedAt, now);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("allows only one of two concurrent consumption attempts", () =>
    Effect.gen(function* () {
      const repository = yield* AuthPairingLinks.AuthPairingLinkRepository;
      yield* repository.create(pairingLink);
      const results = yield* Effect.all(
        [repository.consumeAvailable(consumeInput), repository.consumeAvailable(consumeInput)],
        { concurrency: "unbounded" },
      );
      assert.strictEqual(results.filter(Option.isSome).length, 1);
      assert.strictEqual(results.filter(Option.isNone).length, 1);
    }).pipe(Effect.provide(layer)),
  );
});
