// @effect-diagnostics nodeBuiltinImport:off -- Synthetic isolated auth fixtures retain all task-created files outside the source tree.
// @effect-diagnostics effectFnOpportunity:off -- Test callbacks exercise actual SQL/session boundaries.
import * as NodeCrypto from "node:crypto";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as EffectNodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqlite from "@t3tools/shared/nodeSqliteClient";
import {
  AuthSessionId,
  AuthOrchestrationReadScope,
  AuthOrchestrationOperateScope,
  EnvironmentId,
} from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import * as Rows from "../../../persistence/AuthSessions.ts";
import authMigration from "../../../persistence/Migrations/031_AuthAuthorizationScopes.ts";
import * as Sessions from "../../../auth/SessionStore.ts";
import * as Secrets from "../../../auth/ServerSecretStore.ts";
import * as Config from "../../../config.ts";
import * as Environment from "../../../environment/ServerEnvironment.ts";
import * as Cli from "./cliSession.ts";

const key = Uint8Array.from({ length: 32 }, (_, i) => i + 1);
const secretStore = (
  get: Secrets.ServerSecretStore["Service"]["get"] = () => Effect.succeedSome(Uint8Array.from(key)),
) =>
  Secrets.ServerSecretStore.of({
    get,
    getOrCreateRandom: () => Effect.succeed(Uint8Array.from(key)),
    create: () => Effect.die("No fixture key creation"),
    set: () => Effect.die("No fixture key replacement"),
    remove: () => Effect.die("No fixture key removal"),
  });
const fixture = Layer.effectDiscard(authMigration).pipe(
  Layer.provideMerge(NodeSqlite.layer({ filename: ":memory:" })),
);
const layer = Rows.layer.pipe(
  Layer.provideMerge(fixture),
  Layer.provideMerge(Layer.mergeAll(NodeServices.layer, EffectNodeCrypto.layer)),
);
const now = Date.parse("2026-08-02T12:00:00Z");
const useSession = (mayWrite = true) => Cli.withWorkstreamCliSession(mayWrite, Effect.succeed);
const verify = (token: string) =>
  Effect.gen(function* () {
    const store = yield* Sessions.make.pipe(
      Effect.provide(
        Config.layerTest(
          process.cwd(),
          NodePath.join(
            process.env.TMPDIR ?? NodeOS.tmpdir(),
            "nw-cli-session-" + NodeCrypto.randomUUID(),
          ),
        ),
      ),
      Effect.provideService(Environment.ServerEnvironmentIdentity, {
        getEnvironmentId: Effect.succeed(EnvironmentId.make("synthetic:cli-recovery")),
      }),
    );
    return yield* store.verify(token);
  });
const test = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.provide(layer),
    Effect.provideService(Secrets.ServerSecretStore, secretStore()),
    Effect.scoped,
  );

it.effect(
  "independent invocations preserve the exact original client, token, expiry and ordinary session verification",
  () =>
    test(
      Effect.gen(function* () {
        yield* TestClock.setTime(now);
        const first = yield* useSession();
        yield* TestClock.setTime(now + 60_000);
        const second = yield* useSession(false);
        expect(second).toEqual(first);
        expect((yield* verify(second.token)).sessionId).toBe(first.sessionId);
        expect(second.scopes).toEqual([AuthOrchestrationReadScope, AuthOrchestrationOperateScope]);
        const rows = yield* Rows.AuthSessionRepository;
        const active = yield* rows.listActive({ now: DateTime.toUtc(yield* DateTime.now) });
        expect(active).toHaveLength(1);
        expect(active[0]?.issuedAt.epochMilliseconds).toBe(now);
        expect(first.expiresAt.epochMilliseconds).toBe(now + 30 * 86400_000);
      }),
    ),
);

it.effect(
  "an unknown request failure retains that same client for later status and exact retry",
  () =>
    test(
      Effect.gen(function* () {
        const failed = yield* Cli.withWorkstreamCliSession(true, () =>
          Effect.fail("unknown_reply"),
        ).pipe(Effect.result);
        expect(failed).toMatchObject({ _tag: "Failure", failure: "unknown_reply" });
        const rows = yield* Rows.AuthSessionRepository;
        const before = yield* rows.listActive({ now: DateTime.toUtc(yield* DateTime.now) });
        expect(before).toHaveLength(1);
        const later = yield* useSession(false);
        expect(later.sessionId).toBe(before[0]?.sessionId);
      }),
    ),
);

it.effect(
  "read-only commands without a recovery client get only five-minute read access and revoke it after use",
  () =>
    test(
      Effect.gen(function* () {
        yield* TestClock.setTime(now);
        const read = yield* Cli.withWorkstreamCliSession(false, (issued) =>
          Effect.gen(function* () {
            const validated = yield* verify(issued.token);
            expect(validated.scopes).toEqual([AuthOrchestrationReadScope]);
            expect(issued.expiresAt.epochMilliseconds).toBe(now + 300_000);
            return issued;
          }),
        );
        expect((yield* verify(read.token).pipe(Effect.flip))._tag).toBe("SessionTokenRevokedError");
        const rows = yield* Rows.AuthSessionRepository;
        expect(yield* rows.listActive({ now: DateTime.toUtc(yield* DateTime.now) })).toEqual([]);
      }),
    ),
);

it.effect("concurrent independent acquisitions reserve one recovery client", () =>
  test(
    Effect.gen(function* () {
      const all = yield* Effect.all(
        Array.from({ length: 6 }, () => useSession()),
        { concurrency: 6 },
      );
      expect(new Set(all.map((v) => v.sessionId)).size).toBe(1);
      expect(new Set(all.map((v) => v.token)).size).toBe(1);
      const sql = yield* SqlClient.SqlClient;
      expect(
        (yield* sql<{ count: number }>`SELECT count(*) AS count FROM auth_sessions`)[0]?.count,
      ).toBe(1);
    }),
  ),
);

it.effect.each(["revoke", "expire"] as const)(
  "%s never revives the original accepted caller or extends its lease",
  (change) =>
    test(
      Effect.gen(function* () {
        yield* TestClock.setTime(now);
        const first = yield* useSession();
        if (change === "revoke") {
          const rows = yield* Rows.AuthSessionRepository;
          yield* rows.revoke({
            sessionId: first.sessionId,
            revokedAt: DateTime.toUtc(yield* DateTime.now),
          });
        } else yield* TestClock.setTime(first.expiresAt.epochMilliseconds + 1);
        expect((yield* verify(first.token).pipe(Effect.result))._tag).toBe("Failure");
        const replacement = yield* useSession();
        expect(replacement.sessionId).not.toBe(first.sessionId);
        expect((yield* verify(replacement.token)).sessionId).toBe(replacement.sessionId);
        expect((yield* verify(first.token).pipe(Effect.result))._tag).toBe("Failure");
      }),
    ),
);

it.effect("narrowing an active recovery client never reacquires its former write ceiling", () =>
  test(
    Effect.gen(function* () {
      const first = yield* useSession();
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE auth_sessions SET scopes = ${JSON.stringify([AuthOrchestrationReadScope])} WHERE session_id = ${first.sessionId}`;
      const narrowed = yield* useSession();
      expect(narrowed.sessionId).toBe(first.sessionId);
      expect((yield* verify(narrowed.token)).scopes).toEqual([AuthOrchestrationReadScope]);
      expect(narrowed.expiresAt).toEqual(first.expiresAt);
      expect(
        (yield* sql<{ count: number }>`SELECT count(*) AS count FROM auth_sessions`)[0]?.count,
      ).toBe(1);
    }),
  ),
);

it.effect.each(["subject", "method", "scope", "lifetime", "metadata", "ambiguous"] as const)(
  "refuses %s corruption instead of choosing or minting a different client",
  (field) =>
    test(
      Effect.gen(function* () {
        const first = yield* useSession();
        const sql = yield* SqlClient.SqlClient;
        if (field === "subject") yield* sql`UPDATE auth_sessions SET subject = 'some-other-client'`;
        if (field === "method")
          yield* sql`UPDATE auth_sessions SET method = 'browser-session-cookie'`;
        if (field === "scope")
          yield* sql`UPDATE auth_sessions SET scopes = '["auth:access-write"]'`;
        if (field === "lifetime")
          yield* sql`UPDATE auth_sessions SET expires_at = '2099-01-01T00:00:00.000Z'`;
        if (field === "metadata")
          yield* sql`UPDATE auth_sessions SET client_device_type = 'browser'`;
        if (field === "ambiguous") {
          const rows = yield* Rows.AuthSessionRepository;
          const original = yield* rows.getById({ sessionId: first.sessionId });
          if (Option.isNone(original)) return yield* Effect.die("Original reservation missing");
          yield* rows.create({
            ...original.value,
            sessionId: AuthSessionId.make(NodeCrypto.randomUUID()),
          });
        }
        expect((yield* useSession().pipe(Effect.result))._tag).toBe("Failure");
        expect(
          (yield* sql<{ count: number }>`SELECT count(*) AS count FROM auth_sessions`)[0]?.count,
        ).toBe(field === "ambiguous" ? 2 : 1);
      }),
    ),
);

it.effect(
  "prepares signing material before the writer so unrelated same-client writes can commit while it waits",
  () =>
    test(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`CREATE TABLE jones_cli_session_probe (value INTEGER)`;
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        let keyCalls = 0;
        const secret = secretStore(() =>
          Effect.gen(function* () {
            keyCalls++;
            expect(Option.isNone(yield* Effect.serviceOption(sql.transactionService))).toBe(true);
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(release);
            return Option.some(Uint8Array.from(key));
          }),
        );
        const pending = yield* useSession().pipe(
          Effect.provideService(Secrets.ServerSecretStore, secret),
          Effect.forkScoped({ startImmediately: true }),
        );
        yield* Deferred.await(entered);
        yield* sql.withTransaction(sql`INSERT INTO jones_cli_session_probe VALUES (1)`);
        expect(yield* sql`SELECT * FROM jones_cli_session_probe`).toHaveLength(1);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(pending);
        expect(keyCalls).toBe(1);
      }),
    ),
);

it.effect("ambient writers refuse before any signing-key lookup or grant creation", () =>
  test(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      let calls = 0;
      const secret = secretStore(() =>
        Effect.sync(() => {
          calls++;
          return Option.some(Uint8Array.from(key));
        }),
      );
      const failed = yield* sql.withTransaction(
        useSession().pipe(Effect.provideService(Secrets.ServerSecretStore, secret), Effect.flip),
      );
      expect(failed).toMatchObject({
        _tag: "SessionCredentialIssueError",
        cause: "preparation_in_transaction",
      });
      expect(calls).toBe(0);
      expect(yield* sql`SELECT * FROM auth_sessions`).toEqual([]);
    }),
  ),
);

it.effect("a missing existing signing key refuses before reserving any client", () =>
  test(
    Effect.gen(function* () {
      const missing = secretStore(() => Effect.succeedNone);
      expect(
        (yield* useSession().pipe(
          Effect.provideService(Secrets.ServerSecretStore, missing),
          Effect.result,
        ))._tag,
      ).toBe("Failure");
      const sql = yield* SqlClient.SqlClient;
      expect(yield* sql`SELECT * FROM auth_sessions`).toEqual([]);
    }),
  ),
);
