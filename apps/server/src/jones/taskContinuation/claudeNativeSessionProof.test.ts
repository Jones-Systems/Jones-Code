import { ProviderDriverKind, RunAttemptId, RunId } from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import { ProviderAdapterProtocolError } from "@t3tools/provider-core/server/ProviderAdapter";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";

import { makeClaudeNativeSessionProof } from "./claudeNativeSessionProof.ts";
import {
  NativeSessionIdentityRejected,
  type NativeSessionObservation,
  type NativeSessionRequirement,
} from "./requiredNativeSession.ts";

const requirement = (confirm: NativeSessionRequirement["confirm"]): NativeSessionRequirement => ({
  runId: RunId.make("run:proof"),
  attemptId: RunAttemptId.make("attempt:proof"),
  nativeSessionId: "expected-native-session",
  confirm,
});

describe("Claude native session proof", () => {
  it.effect("serializes concurrent proof consumers and writes one exact attempt witness", () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const observations: NativeSessionObservation[] = [];
      const expected = requirement((observation) =>
        Effect.gen(function* () {
          observations.push(observation);
          yield* Deferred.succeed(entered, undefined);
          yield* Deferred.await(release);
        }),
      );
      const proof = yield* makeClaudeNativeSessionProof(expected);
      const consumers = yield* Effect.all(
        [
          proof.confirm(expected.nativeSessionId, "generation:actual", true),
          proof.confirm(expected.nativeSessionId, "generation:actual"),
        ],
        { concurrency: "unbounded" },
      ).pipe(Effect.forkScoped);
      yield* Deferred.await(entered);
      assert.equal(observations.length, 1);
      assert.isUndefined(consumers.pollUnsafe());
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(consumers);
      yield* proof.confirm(expected.nativeSessionId, "generation:actual", true);
      assert.deepEqual(observations, [
        {
          runId: expected.runId,
          attemptId: expected.attemptId,
          nativeSessionId: expected.nativeSessionId,
          runtimeGeneration: "generation:actual",
        },
      ]);
    }),
  );

  it.effect.each([
    { observed: null, init: false, reason: "output_before_init" },
    { observed: "other-native-session", init: true, reason: "init_mismatch" },
  ] as const)("refuses $reason without recording a witness", ({ observed, init, reason }) =>
    Effect.gen(function* () {
      let writes = 0;
      const expected = requirement(() =>
        Effect.sync(() => {
          writes += 1;
        }),
      );
      const proof = yield* makeClaudeNativeSessionProof(expected);
      const error = yield* proof.confirm(observed, "generation:actual", init).pipe(Effect.flip);
      assert.isTrue(Schema.is(NativeSessionIdentityRejected)(error));
      if (!Schema.is(NativeSessionIdentityRejected)(error)) return;
      assert.equal(error.expected, expected.nativeSessionId);
      assert.equal(error.observed, observed);
      assert.equal(error.reason, reason);
      const later = yield* proof
        .confirm(expected.nativeSessionId, "generation:actual")
        .pipe(Effect.flip);
      assert.strictEqual(later, error);
      assert.equal(writes, 0);
    }),
  );

  it.effect("keeps a failed witness write failed for subsequent content and callbacks", () =>
    Effect.gen(function* () {
      let writes = 0;
      const unavailable = new ProviderAdapterProtocolError({
        driver: ProviderDriverKind.make("claudeAgent"),
        detail: "Continuation verification storage is unavailable",
      });
      const expected = requirement(() =>
        Effect.sync(() => {
          writes += 1;
        }).pipe(Effect.andThen(Effect.fail(unavailable))),
      );
      const proof = yield* makeClaudeNativeSessionProof(expected);
      assert.strictEqual(
        yield* proof.confirm(expected.nativeSessionId, "generation:actual", true).pipe(Effect.flip),
        unavailable,
      );
      assert.strictEqual(
        yield* proof.confirm(expected.nativeSessionId, "generation:actual").pipe(Effect.flip),
        unavailable,
      );
      assert.equal(writes, 1);
    }),
  );
});
