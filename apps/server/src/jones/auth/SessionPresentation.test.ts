import { assert, it } from "@effect/vitest";
import { AuthSessionId, type AuthClientSession } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as Sqlite from "../../persistence/Sqlite.ts";
import * as Sessions from "../../persistence/AuthSessions.ts";
import { makeSessionPresentation } from "./SessionPresentation.ts";

const layer = Sessions.layer.pipe(Layer.provideMerge(Sqlite.layerMemory));
const now = "2026-11-03T10:00:00.000Z";
const decodeAuthSessionRecord = Schema.decodeEffect(Sessions.AuthSessionRecord);

const fixture = Effect.gen(function* () {
  yield* TestClock.setTime(DateTime.toEpochMillis(DateTime.makeUnsafe(now)));
  const sessions = yield* Sessions.AuthSessionRepository;
  const sql = yield* SqlClient.SqlClient;
  const record = yield* decodeAuthSessionRecord({
    sessionId: "presentation-session",
    subject: "Synthetic presentation client",
    scopes: ["orchestration:read"],
    method: "bearer-access-token",
    issuedAt: "2026-11-03T09:00:00.000Z",
    expiresAt: "2026-11-03T11:00:00.000Z",
    revokedAt: null,
    lastConnectedAt: null,
    client: {
      label: "Synthetic",
      ipAddress: null,
      userAgent: "Synthetic test client",
      deviceType: "desktop",
      os: null,
      browser: null,
    },
  });
  yield* sessions.create(record);
  const presentation = yield* makeSessionPresentation;
  const authorized: AuthClientSession = {
    ...record,
    current: false,
    connected: true,
    client: { label: "Synthetic", userAgent: "Synthetic test client", deviceType: "desktop" },
  };
  return { sessions, sql, record, presentation, authorized };
});

it.effect(
  "sparse hints merge omitted fields, overlay only the existing client, and preserve its upstream row",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const before = yield* f.sql`SELECT * FROM auth_sessions`;
      yield* f.presentation.write(f.record, { surface: "mobile", appVersion: "1.2.0" });
      yield* f.presentation.write(f.record, { appVersion: "1.3.0" });
      yield* f.presentation.write(f.record, {});
      const active = yield* f.presentation.overlay(f.authorized);
      assert.deepEqual(active.client, {
        ...f.authorized.client,
        surface: "mobile",
        appVersion: "1.3.0",
      });
      assert.deepEqual(active.scopes, f.authorized.scopes);
      assert.deepEqual(yield* f.sql`SELECT * FROM auth_sessions`, before);
      assert.strictEqual(
        (yield* f.sessions.listActive({ now: DateTime.makeUnsafe(now) })).length,
        1,
      );
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["phantom", "subject", "scope", "issued-at", "expires-at", "client"] as const)(
  "presentation write rejects a changed expected %s without creating metadata or sessions",
  (field) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const expected = {
        ...f.record,
        ...(field === "phantom" ? { sessionId: AuthSessionId.make("phantom-session") } : {}),
        ...(field === "subject" ? { subject: "Foreign client" } : {}),
        ...(field === "scope" ? { scopes: ["orchestration:operate" as const] } : {}),
        ...(field === "issued-at"
          ? { issuedAt: DateTime.makeUnsafe("2026-11-03T09:00:01.000Z") }
          : {}),
        ...(field === "expires-at"
          ? { expiresAt: DateTime.makeUnsafe("2026-11-03T11:00:01.000Z") }
          : {}),
        ...(field === "client"
          ? { client: { ...f.record.client, userAgent: "Different client" } }
          : {}),
      };
      const before = yield* f.sql`SELECT * FROM auth_sessions`;
      assert.strictEqual(
        (yield* f.presentation.write(expected, { surface: "mobile" }).pipe(Effect.flip))._tag,
        "SessionPresentationError",
      );
      assert.deepEqual(yield* f.sql`SELECT * FROM jones_auth_session_client_metadata`, []);
      assert.deepEqual(yield* f.sql`SELECT * FROM auth_sessions`, before);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["revoked", "expired"] as const)(
  "%s sessions cannot write or receive presentation hints",
  (kind) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.presentation.write(f.record, { surface: "mobile" });
      if (kind === "revoked")
        yield* f.sessions.revoke({
          sessionId: f.record.sessionId,
          revokedAt: DateTime.makeUnsafe(now),
        });
      else
        yield* TestClock.setTime(
          DateTime.toEpochMillis(DateTime.makeUnsafe("2026-11-03T11:00:00.000Z")),
        );
      yield* f.presentation.write(f.record, { appVersion: "2.0" }).pipe(Effect.flip);
      assert.deepEqual(yield* f.presentation.overlay(f.authorized), f.authorized);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "stale hints do not follow a changed client association or manufacture phantom active sessions",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.presentation.write(f.record, { surface: "mobile", appVersion: "1.0" });
      yield* f.sql`INSERT INTO jones_auth_session_client_metadata(session_id,identity_digest,client_surface) VALUES('phantom',${"a".repeat(64)},'mobile')`;
      yield* f.sql`UPDATE auth_sessions SET client_user_agent='Changed synthetic client' WHERE session_id=${f.record.sessionId}`;
      const current = yield* f.sessions.getById({ sessionId: f.record.sessionId });
      assert.isTrue(Option.isSome(current));
      const changed = {
        ...f.authorized,
        client: { ...f.authorized.client, userAgent: "Changed synthetic client" },
      };
      assert.deepEqual(yield* f.presentation.overlay(changed), changed);
      assert.deepEqual(yield* f.presentation.overlay(f.authorized), f.authorized);
      assert.deepEqual(
        (yield* f.sessions.listActive({ now: DateTime.makeUnsafe(now) })).map(
          (row) => row.sessionId,
        ),
        [f.record.sessionId],
      );
    }).pipe(Effect.provide(layer)),
);
