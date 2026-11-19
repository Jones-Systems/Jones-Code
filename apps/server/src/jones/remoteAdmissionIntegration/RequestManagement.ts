import {
  AuthOrchestrationReadScope,
  AuthRequestsManageScope,
  EnvironmentId,
  type EnvironmentSessionPrincipalShape,
  type RemoteAdmissionStatus,
} from "@t3tools/contracts";
import * as DTO from "@t3tools/contracts/jones/requestManagement";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import { ownerRequestPredicate } from "../remoteAdmission/peerBookkeeping.ts";
import * as RemoteAdmission from "../remoteAdmission/RemoteAdmission.ts";
import { evidenceDigest } from "./LaunchRecovery.ts";
import { ReceiverReviewReaders } from "./ReviewService.ts";

const forbidden: DTO.RequestManagementError = {
  code: "requests_owner_required",
  message: "This Requests operation requires its authorized local owner or linked session.",
};
const missing: DTO.RequestManagementError = {
  code: "request_unavailable",
  message: "Request not found.",
};
const stale: DTO.RequestManagementError = {
  code: "revision_changed",
  message: "The request changed. Inspect it again before editing.",
};

interface Row {
  readonly id: string;
  readonly binding_digest: string;
  readonly operation: string;
  readonly status: RemoteAdmissionStatus;
  readonly created_at: string;
  readonly updated_at: string;
  readonly command_id: string | null;
  readonly dispatch_outcome: string | null;
  readonly recovery_revision: number | null;
  readonly recovered_outcome: string | null;
  readonly recorded_outcome: string | null;
  readonly has_live_grant: number;
  readonly label: string | null;
  readonly dismissed_at: string | null;
  readonly revision: number | null;
}

const recoveryOutcome = (row: Row): DTO.RequestSummary["recoveryOutcome"] => {
  if (row.operation !== "t3_thread_launch") return null;
  const outcome = row.recovered_outcome ?? row.recorded_outcome;
  if (outcome === "created" || outcome === "refused" || outcome === "not_created") return outcome;
  if (row.status === "denied" || row.status === "cancelled") return "refused";
  return outcome === "unknown" || row.command_id !== null ? "unknown" : null;
};

const summary = (row: Row): DTO.RequestSummary => ({
  id: row.id,
  operation: row.operation,
  status: row.status,
  createdAt: row.created_at,
  updatedAt: row.updated_at,
  recoveryOutcome: recoveryOutcome(row),
});

const canDismiss = (row: Row) => {
  if (row.has_live_grant !== 0) return false;
  if (row.status === "denied" || row.status === "cancelled") return true;
  if (row.status !== "dispatched") return false;
  const outcome = recoveryOutcome(row);
  return row.operation === "t3_thread_launch"
    ? outcome === "created" || outcome === "refused" || outcome === "not_created"
    : row.dispatch_outcome === "success" || row.dispatch_outcome === "failure";
};

const managed = (environmentId: EnvironmentId, row: Row): DTO.ManagedRequest => ({
  ...summary(row),
  label: row.label,
  dismissed: row.dismissed_at !== null,
  canDismiss: canDismiss(row),
  revision: evidenceDigest([
    environmentId,
    row.id,
    row.binding_digest,
    row.updated_at,
    row.status,
    row.command_id,
    row.dispatch_outcome,
    row.recovery_revision,
    recoveryOutcome(row),
    row.has_live_grant,
    row.revision ?? 0,
  ]),
});

// A null session is selected only for a trusted local thread or the owner CLI.
type RequestReader = { readonly sessionId: string } | null;

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const admissions = yield* RemoteAdmission.RemoteAdmission;
  const readers = yield* ReceiverReviewReaders;
  const receiverEnvironmentId = EnvironmentId.make(readers.receiverEnvironmentId);
  const nowIso = DateTime.now.pipe(Effect.map(DateTime.formatIso));

  const rows = sql`SELECT a.id,a.binding_digest,json_extract(a.envelope_json,'$.operation') operation,
    a.status,a.created_at,a.updated_at,a.command_id,
    e.outcome dispatch_outcome,r.revision recovery_revision,
    json_extract(r.result_json,'$.outcome') recovered_outcome,
    json_extract(original.result_json,'$.launchResult.outcome') recorded_outcome,
    EXISTS(SELECT 1 FROM jones_remote_admission_grants g WHERE g.admission_id=a.id
      AND (g.state='pending' OR (g.state='active' AND g.expires_at>strftime('%Y-%m-%dT%H:%M:%fZ','now')))) has_live_grant,
    m.label,m.dismissed_at,m.revision
    FROM jones_remote_admissions a
    LEFT JOIN jones_remote_admission_extensions e ON e.admission_id=a.id
    LEFT JOIN jones_remote_admission_results original ON original.admission_id=a.id
    LEFT JOIN jones_remote_admission_recovered_results r ON r.admission_id=a.id AND r.revision=(
      SELECT max(revision) FROM jones_remote_admission_recovered_results WHERE admission_id=a.id)
    LEFT JOIN jones_request_management_edits m ON m.admission_id=a.id AND m.revision=(
      SELECT max(revision) FROM jones_request_management_edits WHERE admission_id=a.id)`;
  const visibility = (reader: RequestReader) =>
    reader === null
      ? ownerRequestPredicate(sql)
      : sql`a.client_id=${reader.sessionId} AND ${ownerRequestPredicate(sql)}`;

  const activeSession = Effect.fn("RequestManagement.activeSession")(function* (
    id: string,
    subject: string,
    permission: string,
  ) {
    const found = yield* sql`SELECT 1 FROM auth_sessions
      WHERE session_id=${id} AND subject=${subject} AND method='bearer-access-token'
      AND revoked_at IS NULL AND expires_at>${yield* nowIso}
      AND EXISTS(SELECT 1 FROM json_each(scopes) WHERE value=${permission})`;
    if (!found.length) return yield* Effect.fail(forbidden);
  });

  const authorizeRead = (reader: RequestReader) =>
    reader === null
      ? Effect.void
      : activeSession(reader.sessionId, "mcp-client", AuthOrchestrationReadScope);

  const authorizeOwner = Effect.fn("RequestManagement.authorizeOwner")(function* (
    principal: EnvironmentSessionPrincipalShape,
    target: EnvironmentId,
  ) {
    if (
      principal.method !== "bearer-access-token" ||
      principal.subject !== DTO.REQUESTS_CLI_SUBJECT ||
      !principal.scopes.has(AuthRequestsManageScope)
    )
      return yield* Effect.fail(forbidden);
    yield* activeSession(principal.sessionId, principal.subject, AuthRequestsManageScope);
    if (target !== receiverEnvironmentId)
      return yield* Effect.fail({
        code: "receiver_changed",
        message: "The selected server has a different receiving environment ID.",
      } satisfies DTO.RequestManagementError);
  });

  const find = Effect.fn("RequestManagement.find")(function* (id: string, reader: RequestReader) {
    const result = yield* sql<Row>`${rows} WHERE a.id=${id} AND ${visibility(reader)}`;
    return result[0] ?? null;
  });

  const page = Effect.fn("RequestManagement.page")(function* (
    input: DTO.RequestListInput,
    reader: RequestReader,
    allowDismissedView: boolean,
  ) {
    const { view = "pending", after } = input;
    if (!allowDismissedView && view === "dismissed") return yield* Effect.fail(forbidden);
    const limit = input.limit ?? 50;
    const anchor =
      after === undefined
        ? undefined
        : (yield* sql<{ created_at: string }>`
      SELECT a.created_at FROM jones_remote_admissions a WHERE a.id=${after} AND ${visibility(reader)}`)[0];
    if (after !== undefined && anchor === undefined) return yield* Effect.fail(missing);
    const viewFilter =
      view === "dismissed"
        ? sql`m.dismissed_at IS NOT NULL`
        : view === "pending"
          ? sql`m.dismissed_at IS NULL AND a.status IN ('evaluating','passed','held','approved','dispatching')`
          : sql`m.dismissed_at IS NULL`;
    const afterFilter =
      anchor === undefined
        ? sql`1`
        : sql`(
      a.created_at < ${anchor.created_at} OR (a.created_at=${anchor.created_at} AND a.id<${after}))`;
    const found = yield* sql<Row>`${rows}
      WHERE ${visibility(reader)} AND ${viewFilter} AND ${afterFilter}
      ORDER BY a.created_at DESC,a.id DESC LIMIT ${limit + 1}`;
    const result = found.slice(0, limit);
    return { rows: result, next: found.length > limit ? (result.at(-1)?.id ?? null) : null };
  });

  const list = Effect.fn("RequestManagement.list")(
    (input: DTO.RequestListInput, reader: RequestReader) =>
      sql.withTransaction(
        Effect.gen(function* () {
          yield* authorizeRead(reader);
          const found = yield* page(input, reader, false);
          return { receiverEnvironmentId, requests: found.rows.map(summary), next: found.next };
        }),
      ),
  );
  const inspect = Effect.fn("RequestManagement.inspect")((id: string, reader: RequestReader) =>
    sql.withTransaction(
      Effect.gen(function* () {
        yield* authorizeRead(reader);
        const row = yield* find(id, reader);
        return { receiverEnvironmentId, request: row === null ? null : summary(row) };
      }),
    ),
  );

  const ownerQuery = Effect.fn("RequestManagement.ownerQuery")(
    (input: DTO.OwnerRequestsQuery, principal: EnvironmentSessionPrincipalShape) =>
      sql.withTransaction(
        Effect.gen(function* (): Effect.fn.Return<
          DTO.OwnerRequestsResult,
          DTO.RequestManagementError | SqlError
        > {
          yield* authorizeOwner(principal, input.receiverEnvironmentId);
          if (input.action === "inspect") {
            const row = yield* find(input.id, null);
            if (row === null) return yield* Effect.fail(missing);
            return {
              receiverEnvironmentId,
              action: "inspect",
              request: managed(receiverEnvironmentId, row),
            };
          }
          const found = yield* page(input, null, true);
          return {
            receiverEnvironmentId,
            action: "list",
            requests: found.rows.map((row) => managed(receiverEnvironmentId, row)),
            next: found.next,
          };
        }),
      ),
  );

  const edit = Effect.fn("RequestManagement.edit")(
    (input: DTO.OwnerRequestEdit, principal: EnvironmentSessionPrincipalShape) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            yield* authorizeOwner(principal, input.receiverEnvironmentId);
            const row = yield* find(input.id, null);
            if (row === null) return yield* Effect.fail(missing);
            const current = managed(receiverEnvironmentId, row);
            if (current.revision !== input.expectedRevision) return yield* Effect.fail(stale);
            if (input.action === "dismiss" && !current.canDismiss)
              return yield* Effect.fail({
                code: "request_not_dismissible",
                message:
                  "Dismissal is available only for completed requests with known outcomes and no live grant.",
              } satisfies DTO.RequestManagementError);
            const now = yield* nowIso;
            const label = input.action === "label" ? input.label : row.label;
            const dismissedAt =
              input.action === "dismiss"
                ? (row.dismissed_at ?? now)
                : input.action === "restore"
                  ? null
                  : row.dismissed_at;
            const inboxChanged = dismissedAt !== row.dismissed_at;
            if (label === row.label && !inboxChanged) return { inboxChanged, request: current };
            const revision = (row.revision ?? 0) + 1;
            yield* sql`INSERT INTO jones_request_management_edits
        (admission_id,revision,previous_version,action,label,dismissed_at,changed_at,actor_session_id)
        VALUES (${row.id},${revision},${current.revision},${input.action},${label},${dismissedAt},${now},${principal.sessionId})`;
            if (inboxChanged) {
              yield* sql`UPDATE jones_remote_admission_review_state SET revision=revision+1 WHERE singleton=1`;
            }
            return {
              inboxChanged,
              request: managed(receiverEnvironmentId, {
                ...row,
                label,
                dismissed_at: dismissedAt,
                revision,
              }),
            };
          }),
        )
        .pipe(
          Effect.tap(({ inboxChanged }) =>
            inboxChanged ? admissions.access.invalidate : Effect.void,
          ),
          Effect.map(({ request }) => ({ receiverEnvironmentId, request })),
        ),
  );

  return { list, inspect, ownerQuery, edit };
});

export class RequestManagement extends Context.Service<
  RequestManagement,
  Effect.Success<typeof make>
>()("t3/jones/remoteAdmissionIntegration/RequestManagement") {}

export const layer = Layer.effect(RequestManagement, make);
