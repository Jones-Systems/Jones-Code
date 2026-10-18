import * as NodeServices from "@effect/platform-node/NodeServices";
import { EnvironmentId } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as TestClock from "effect/testing/TestClock";
import * as ServerSecretStore from "../../../auth/ServerSecretStore.ts";
import * as SessionStore from "../../../auth/SessionStore.ts";
import * as ServerConfig from "../../../config.ts";
import * as ServerEnvironment from "../../../environment/ServerEnvironment.ts";
import * as AuthSessions from "../../../persistence/AuthSessions.ts";
import * as SqlitePersistence from "../../../persistence/Sqlite.ts";
import { deriveNativeEnrollmentSession } from "./request.ts";
import { enrollmentRequest } from "./testFixtures.ts";

// The ordinary server session store owns key creation; the materializer only reads that key.
const layerServerSessions = SessionStore.layer.pipe(
  Layer.provideMerge(ServerSecretStore.layer),
  Layer.provideMerge(SqlitePersistence.layerMemory),
  Layer.provide(
    Layer.succeed(ServerEnvironment.ServerEnvironmentIdentity, {
      getEnvironmentId: Effect.succeed(EnvironmentId.make("synthetic-environment")),
    }),
  ),
  Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-native-bearer-test-" })),
  Layer.provideMerge(NodeServices.layer),
);
const expected = deriveNativeEnrollmentSession(enrollmentRequest);
const atIssuance = TestClock.setTime(expected.issuedAt.epochMilliseconds);

it.effect(
  "reserved bearer materialization repeats the exact verified token after a lost reply",
  () =>
    Effect.gen(function* () {
      yield* atIssuance;
      const store = yield* SessionStore.SessionStore;
      const repository = yield* AuthSessions.AuthSessionRepository;
      yield* repository.createIfAbsent(expected);
      const materialize = yield* SessionStore.makeReservedBearerSessionMaterializer;
      const first = yield* materialize(expected);
      const second = yield* materialize(expected);
      expect(second.token).toBe(first.token);
      expect(first.expiresAt.epochMilliseconds).toBe(expected.expiresAt.epochMilliseconds);
      const verified = yield* store.verify(first.token);
      expect(verified.sessionId).toBe(expected.sessionId);
      expect(verified.subject).toBe(expected.subject);
      expect(verified.method).toBe("bearer-access-token");
      expect([...verified.scopes]).toEqual([...expected.scopes]);
      expect((yield* repository.listActive({ now: expected.issuedAt })).length).toBe(1);
    }).pipe(Effect.provide(layerServerSessions), Effect.scoped),
);

it.effect(
  "reserved bearer materialization never creates, widens, revives or changes a reserved record",
  () =>
    Effect.gen(function* () {
      yield* atIssuance;
      const repository = yield* AuthSessions.AuthSessionRepository;
      const materialize = yield* SessionStore.makeReservedBearerSessionMaterializer;
      expect((yield* Effect.flip(materialize(expected)))._tag).toBe("SessionCredentialIssueError");
      expect(Option.isNone(yield* repository.getById({ sessionId: expected.sessionId }))).toBe(
        true,
      );
      yield* repository.createIfAbsent(expected);
      for (const changed of [
        { ...expected, subject: "other-subject" },
        { ...expected, scopes: ["orchestration:read"] as const },
        { ...expected, method: "browser-session-cookie" as const },
        { ...expected, issuedAt: DateTime.add(expected.issuedAt, { milliseconds: 1 }) },
        { ...expected, expiresAt: DateTime.add(expected.expiresAt, { milliseconds: 1 }) },
        { ...expected, client: { ...expected.client, label: "other-label" } },
        { ...expected, client: { ...expected.client, deviceType: "desktop" as const } },
      ])
        expect((yield* Effect.flip(materialize(changed)))._tag).toBe("SessionCredentialIssueError");
      yield* TestClock.adjust(Duration.days(30));
      expect((yield* Effect.flip(materialize(expected)))._tag).toBe("SessionCredentialIssueError");
      yield* atIssuance;
      yield* repository.revoke({ sessionId: expected.sessionId, revokedAt: expected.issuedAt });
      expect((yield* Effect.flip(materialize(expected)))._tag).toBe("SessionCredentialIssueError");
    }).pipe(Effect.provide(layerServerSessions), Effect.scoped),
);

it.effect(
  "the enrollment materializer requires an existing signing key and never generates one",
  () =>
    Effect.gen(function* () {
      let generated = 0;
      const secrets = ServerSecretStore.ServerSecretStore.of({
        get: () => Effect.succeed(Option.none()),
        set: () => Effect.void,
        create: () => Effect.void,
        getOrCreateRandom: () =>
          Effect.sync(() => {
            generated++;
            return new Uint8Array(32);
          }),
        remove: () => Effect.void,
      });
      const result = yield* SessionStore.makeReservedBearerSessionMaterializer.pipe(
        Effect.provideService(ServerSecretStore.ServerSecretStore, secrets),
        Effect.result,
      );
      expect(result._tag).toBe("Failure");
      expect(generated).toBe(0);
    }).pipe(
      Effect.provide(AuthSessions.layer.pipe(Layer.provideMerge(SqlitePersistence.layerMemory))),
    ),
);
