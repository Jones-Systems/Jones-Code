// @effect-diagnostics nodeBuiltinImport:off -- The local CLI reserves only an existing selected server's standard auth session.
// @effect-diagnostics effectFnOpportunity:off -- The scoped callback is the CLI invocation boundary.
import * as NodeCrypto from "node:crypto";
import {
  AuthOrchestrationReadScope,
  AuthOrchestrationOperateScope,
  AuthSessionId,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as AuthRows from "../../../persistence/AuthSessions.ts";
import * as Sessions from "../../../auth/SessionStore.ts";

const WORKSTREAM_CLI_RECOVERY_LABEL = "Jones Workstreams CLI recovery v1";
const scopes: ReadonlyArray<AuthEnvironmentScope> = [
  AuthOrchestrationReadScope,
  AuthOrchestrationOperateScope,
];
const maximumLifetime = 30 * 24 * 60 * 60 * 1000;
const refuse = (cause: string) => new Sessions.SessionCredentialIssueError({ cause });

/** Rematerialize the same current local client, never a replacement for a saved operation's caller.
 * No token is persisted. Expiry/revocation still fences that client's accepted operations.
 * Signing material is prepared before the writer; concurrent CLI invocations serialize reservation. */
export const withWorkstreamCliSession = Effect.fn("WorkstreamCli.withSession")(function* <A, E, R>(
  mayWrite: boolean,
  use: (issued: Sessions.IssuedSession) => Effect.Effect<A, E, R>,
) {
  const sql = yield* SqlClient.SqlClient;
  if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
    return yield* refuse("preparation_in_transaction");
  const rows = yield* AuthRows.AuthSessionRepository;
  const materialize = yield* Sessions.makeReservedBearerSessionMaterializer;
  return yield* Effect.acquireUseRelease(
    sql.withTransaction(
      Effect.gen(function* () {
        const now = DateTime.toUtc(yield* DateTime.now);
        const candidates = (yield* rows.listActive({ now })).filter(
          (row) => row.client.label === WORKSTREAM_CLI_RECOVERY_LABEL,
        );
        if (candidates.length > 1) return yield* refuse("ambiguous_cli_session");
        let session: AuthRows.CreateAuthSessionInput;
        const retained = candidates.length === 1 || mayWrite;
        if (candidates.length === 1) {
          const row = candidates[0]!;
          if (
            row.subject !== "cli-issued-session" ||
            row.method !== "bearer-access-token" ||
            row.client.deviceType !== "bot" ||
            row.client.ipAddress !== null ||
            row.client.userAgent !== null ||
            row.client.os !== null ||
            row.client.browser !== null ||
            row.scopes.some((scope) => !scopes.includes(scope)) ||
            row.expiresAt.epochMilliseconds - row.issuedAt.epochMilliseconds > maximumLifetime
          )
            return yield* refuse("invalid_cli_session");
          // Preserve narrowed scopes and the original issue/expiry, without refreshing the lease.
          session = row;
        } else {
          session = {
            sessionId: AuthSessionId.make(NodeCrypto.randomUUID()),
            subject: "cli-issued-session",
            method: "bearer-access-token",
            scopes: mayWrite ? scopes : [AuthOrchestrationReadScope],
            client: {
              label: mayWrite ? WORKSTREAM_CLI_RECOVERY_LABEL : "Jones Workstreams CLI read",
              deviceType: "bot",
              ipAddress: null,
              userAgent: null,
              os: null,
              browser: null,
            },
            issuedAt: now,
            expiresAt: DateTime.toUtc(DateTime.add(now, mayWrite ? { days: 30 } : { minutes: 5 })),
          };
          yield* rows.createIfAbsent(session);
        }
        return { issued: yield* materialize(session), retained };
      }),
    ),
    ({ issued }) => use(issued),
    ({ issued, retained }) =>
      retained
        ? Effect.void
        : Effect.flatMap(DateTime.now, (now) =>
            rows.revoke({ sessionId: issued.sessionId, revokedAt: DateTime.toUtc(now) }),
          ).pipe(Effect.ignore({ log: true })),
  );
});
