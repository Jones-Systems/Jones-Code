import { assert, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type { IssuedSession } from "../../auth/SessionStore.ts";
import { NativeEnrollmentError } from "./service.ts";
import { makeNativeEnrollmentOperations } from "./operations.ts";
import { enrollmentRequest as request } from "./testFixtures.ts";

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const issued: IssuedSession = {
  sessionId: request.session.session_id,
  token: "synthetic.private.token",
  method: "bearer-access-token",
  client: { deviceType: "bot" },
  scopes: [
    "workstreams:native:context",
    "workstreams:native:settlement",
    "workstreams:native:reconciliation",
  ],
  expiresAt: DateTime.makeUnsafe(request.session.expires_at),
};

it.effect(
  "plan and readback use only observation ports and do not initialize credentials or records",
  () =>
    Effect.gen(function* () {
      let effects = 0;
      const operations = makeNativeEnrollmentOperations({
        enrollments: {
          inspect: () => Effect.succeed({ state: "reserved" }),
          reserve: () =>
            Effect.sync(() => {
              effects++;
              return { state: "reserved" as const };
            }),
        },
        materialize: () =>
          Effect.sync(() => {
            effects++;
            return issued;
          }),
        credential: {
          observe: () => Effect.succeed({ sha256: "a".repeat(64) }),
          publish: () => Effect.die("publish in read-only operation"),
        },
      });
      assert.strictEqual((yield* operations.plan(request)).state, "planned");
      assert.strictEqual((yield* operations.readback(request, "a".repeat(64))).state, "unchanged");
      assert.strictEqual(effects, 0);
    }).pipe(Effect.scoped),
);

it.effect(
  "apply passes bearer only to the private writer and recovers reserved records without reserving again",
  () =>
    Effect.gen(function* () {
      let reserves = 0;
      let privateToken = "";
      const operations = makeNativeEnrollmentOperations({
        enrollments: {
          inspect: () => Effect.succeed({ state: "reserved" }),
          reserve: () =>
            Effect.sync(() => {
              reserves++;
              return { state: "reserved" as const };
            }),
        },
        materialize: () => Effect.succeed(issued),
        credential: {
          observe: () => Effect.succeed({ sha256: "b".repeat(64) }),
          publish: (token) =>
            Effect.sync(() => {
              privateToken = token;
              return {
                state: "unchanged" as const,
                beforeSha256: "b".repeat(64),
                afterSha256: "b".repeat(64),
              };
            }),
        },
      });
      const receipt = yield* operations.apply(request, null);
      assert.strictEqual(receipt.state, "unchanged");
      assert.strictEqual(reserves, 0);
      assert.strictEqual(privateToken, issued.token);
      assert.deepEqual(receipt.completed_phases, [
        "native_records_reserved",
        "native_credential_published",
      ]);
      assert.strictEqual(encodeJson(receipt).includes(issued.token), false);
      assert.strictEqual(encodeJson(receipt).includes("authorization_header"), false);
    }).pipe(Effect.scoped),
);

it.effect(
  "unknown commit and failed publication remain distinct and never trigger an automatic retry",
  () =>
    Effect.gen(function* () {
      let reserves = 0;
      let publishes = 0;
      const operations = makeNativeEnrollmentOperations({
        enrollments: {
          inspect: () => Effect.succeed({ state: "absent" }),
          reserve: () =>
            Effect.sync(() => {
              reserves++;
            }).pipe(
              Effect.flatMap(() =>
                Effect.fail(new NativeEnrollmentError({ code: "unknown_commit" })),
              ),
            ),
        },
        materialize: () => Effect.succeed(issued),
        credential: {
          observe: () => Effect.succeed({ sha256: null }),
          publish: () =>
            Effect.sync(() => {
              publishes++;
              return { state: "unknown" as const, beforeSha256: null, afterSha256: null };
            }),
        },
      });
      assert.strictEqual((yield* operations.apply(request, null)).reason, "commit_unresolved");
      assert.strictEqual(reserves, 1);
      assert.strictEqual(publishes, 0);
    }).pipe(Effect.scoped),
);
