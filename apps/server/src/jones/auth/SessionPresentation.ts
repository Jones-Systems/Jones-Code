// @effect-diagnostics nodeBuiltinImport:off -- Synchronous SHA-256 labels untrusted presentation metadata; no credential material is hashed or persisted.
import * as NodeCrypto from "node:crypto";
import { importedHistoryCanonicalJson } from "@t3tools/shared/jones/importedHistoryCanonical";
import type { AuthClientSession } from "@t3tools/contracts";
import { JonesAuthClientPresentation } from "@t3tools/contracts/jones/authClientPresentation";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as AuthSessions from "../../persistence/AuthSessions.ts";

export class SessionPresentationError extends Schema.TaggedError<SessionPresentationError>()(
  "SessionPresentationError",
  { reason: Schema.Literal("inactive_or_changed_session") },
) {}

const sameSession = (a: AuthSessions.AuthSessionRecord, b: AuthSessions.AuthSessionRecord) =>
  a.sessionId === b.sessionId &&
  a.subject === b.subject &&
  a.method === b.method &&
  importedHistoryCanonicalJson(a.scopes) === importedHistoryCanonicalJson(b.scopes) &&
  importedHistoryCanonicalJson(a.client) === importedHistoryCanonicalJson(b.client) &&
  DateTime.toEpochMillis(a.issuedAt) === DateTime.toEpochMillis(b.issuedAt) &&
  DateTime.toEpochMillis(a.expiresAt) === DateTime.toEpochMillis(b.expiresAt);
const identityDigest = (record: AuthSessions.AuthSessionRecord) =>
  NodeCrypto.createHash("sha256")
    .update(
      importedHistoryCanonicalJson({
        sessionId: record.sessionId,
        subject: record.subject,
        method: record.method,
        scopes: record.scopes,
        client: record.client,
        issuedAt: DateTime.formatIso(record.issuedAt),
        expiresAt: DateTime.formatIso(record.expiresAt),
      }),
    )
    .digest("hex");
const matchesClient = (
  actual: AuthSessions.AuthSessionRecord["client"],
  authorized: AuthClientSession["client"],
) =>
  actual.deviceType === authorized.deviceType &&
  (actual.label ?? undefined) === authorized.label &&
  (actual.ipAddress ?? undefined) === authorized.ipAddress &&
  (actual.userAgent ?? undefined) === authorized.userAgent &&
  (actual.os ?? undefined) === authorized.os &&
  (actual.browser ?? undefined) === authorized.browser;
const refused = () => new SessionPresentationError({ reason: "inactive_or_changed_session" });
const decodeJonesAuthClientPresentation = Schema.decodeEffect(JonesAuthClientPresentation, {
  onExcessProperty: "error",
});
const decodeJonesAuthClientPresentation2 = Schema.decodeUnknownEffect(JonesAuthClientPresentation);

export const makeSessionPresentation = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const sessions = yield* AuthSessions.AuthSessionRepository;
  const write = Effect.fn("SessionPresentation.write")(function* (
    expected: AuthSessions.AuthSessionRecord,
    input: JonesAuthClientPresentation,
  ) {
    const hints = yield* decodeJonesAuthClientPresentation(input);
    yield* sql.withTransaction(
      Effect.gen(function* () {
        const now = DateTime.toEpochMillis(yield* DateTime.now);
        const actual = yield* sessions.getById({ sessionId: expected.sessionId });
        if (
          Option.isNone(actual) ||
          actual.value.revokedAt !== null ||
          DateTime.toEpochMillis(actual.value.issuedAt) > now ||
          DateTime.toEpochMillis(actual.value.expiresAt) <= now ||
          !sameSession(actual.value, expected)
        )
          return yield* refused();
        if (hints.surface === undefined && hints.appVersion === undefined) return;
        yield* sql`INSERT INTO jones_auth_session_client_metadata(session_id,identity_digest,client_surface,client_app_version)
        VALUES(${expected.sessionId},${identityDigest(actual.value)},${hints.surface ?? null},${hints.appVersion ?? null})
        ON CONFLICT(session_id) DO UPDATE SET
          identity_digest=excluded.identity_digest,
          client_surface=CASE WHEN jones_auth_session_client_metadata.identity_digest=excluded.identity_digest
            THEN COALESCE(excluded.client_surface,jones_auth_session_client_metadata.client_surface) ELSE excluded.client_surface END,
          client_app_version=CASE WHEN jones_auth_session_client_metadata.identity_digest=excluded.identity_digest
            THEN COALESCE(excluded.client_app_version,jones_auth_session_client_metadata.client_app_version) ELSE excluded.client_app_version END`;
      }),
    );
  });
  const overlay = Effect.fn("SessionPresentation.overlay")(function* (
    authorized: AuthClientSession,
  ) {
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const now = DateTime.toEpochMillis(yield* DateTime.now);
        const actual = yield* sessions.getById({ sessionId: authorized.sessionId });
        if (
          Option.isNone(actual) ||
          actual.value.revokedAt !== null ||
          DateTime.toEpochMillis(actual.value.issuedAt) > now ||
          DateTime.toEpochMillis(actual.value.expiresAt) <= now ||
          !matchesClient(actual.value.client, authorized.client) ||
          actual.value.subject !== authorized.subject ||
          actual.value.method !== authorized.method ||
          importedHistoryCanonicalJson(actual.value.scopes) !==
            importedHistoryCanonicalJson(authorized.scopes) ||
          DateTime.toEpochMillis(actual.value.issuedAt) !==
            DateTime.toEpochMillis(authorized.issuedAt) ||
          DateTime.toEpochMillis(actual.value.expiresAt) !==
            DateTime.toEpochMillis(authorized.expiresAt)
        )
          return authorized;
        const [row] = yield* sql<{
          readonly surface: string | null;
          readonly appVersion: string | null;
        }>`
        SELECT client_surface AS surface,client_app_version AS appVersion FROM jones_auth_session_client_metadata WHERE session_id=${authorized.sessionId} AND identity_digest=${identityDigest(actual.value)}`;
        if (row === undefined) return authorized;
        const hints = yield* decodeJonesAuthClientPresentation2({
          ...(row.surface === null ? {} : { surface: row.surface }),
          ...(row.appVersion === null ? {} : { appVersion: row.appVersion }),
        });
        return {
          ...authorized,
          client: { ...authorized.client, ...hints },
        } satisfies AuthClientSession;
      }),
    );
  });
  return { write, overlay };
});
