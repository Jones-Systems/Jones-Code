import {
  ProjectId,
  ThreadId,
  RemoteAdmissionReceiverPolicy,
  type RemoteAdmissionRecord,
} from "@t3tools/contracts";
import * as Review from "@t3tools/contracts/jones/remoteAdmissionReview";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as DateTime from "effect/DateTime";
import * as SqlClient from "effect/sql/SqlClient";
import { SqlError, UnknownError } from "effect/sql/SqlError";
import * as Managed from "../../policy/ManagedPolicy.ts";
import * as Limits from "../../policy/managedLimits.ts";
import * as Projects from "../../orchestration-v2/ProjectStore.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Environment from "../../environment/ServerEnvironment.ts";
import { clientModeCeiling } from "../../mcp/McpInvocationContext.ts";
import { withinAccessCeiling, type Grant } from "../remoteAdmission/ReceiverAccess.ts";
import * as Admission from "../remoteAdmission/RemoteAdmission.ts";
import { ReceiverClaimTracker } from "./ClaimTracker.ts";
import { ReceiverRecoveryEvidence, RecoveryUnavailable } from "./RecoveryStore.ts";
import { ReceiverReviewReaders } from "./ReviewService.ts";
import { makeLaunchEvidenceReader } from "./EvidenceReader.ts";
import { findGrantSourceAdmission } from "./grantSource.ts";
import { makeReturnBindingReader } from "./returnBinding.ts";

const sqlFailure = (cause: unknown) => new SqlError({ reason: new UnknownError({ cause }) });
const StoredInvocation = Schema.Struct({
  environmentId: Schema.String,
  requestNamespace: Schema.String,
  client: Schema.Struct({
    sessionId: Schema.String,
    label: Schema.String,
    access: Schema.Literals([
      "read-only",
      "approval-required",
      "auto-accept-edits",
      "auto",
      "full-access",
    ]),
    linked: Schema.optionalKey(Schema.Boolean),
    narrowedTo: Schema.optionalKey(
      Schema.Struct({
        runtimeMode: Schema.Literals([
          "approval-required",
          "auto-accept-edits",
          "auto",
          "full-access",
        ]),
        interactionMode: Schema.Literals(["default", "plan"]),
      }),
    ),
  }),
});
/** Immutable verified invocation ceilings plus CURRENT receiver session/project/target policy. */
export function makeCurrentAuthority(
  sql: SqlClient.SqlClient,
  projections: Pick<Projections.ProjectionStoreV2["Service"], "getThreadShell">,
  projects: Pick<Projects.ProjectStoreV2["Service"], "get">,
  managed: Pick<Managed.ManagedPolicy["Service"], "document">,
) {
  return Effect.fn("AdmissionRuntime.currentAuthority")(
    function* (record: RemoteAdmissionRecord) {
      const payload = record.envelope.payload as {
        invocation: unknown;
        params: Record<string, unknown>;
      };
      const scope = yield* Schema.decodeUnknownEffect(StoredInvocation)(payload.invocation);
      const client = scope.client;
      if (
        client.sessionId !== record.envelope.identity.clientId ||
        !Limits.allowsRemoteClients(managed)
      )
        return false;
      const now = DateTime.formatIso(yield* DateTime.now);
      const session = (yield* sql<{
        scopes: string;
      }>`SELECT scopes FROM auth_sessions WHERE session_id=${client.sessionId}
      AND subject='mcp-client' AND method='bearer-access-token' AND revoked_at IS NULL AND expires_at>${now}`)[0];
      if (!session) return false;
      const scopes = yield* Schema.decodeEffect(
        Schema.fromJsonString(Schema.Array(Schema.String)),
      )(session.scopes);
      if (!scopes.includes("orchestration:operate")) return false;
      const policy = (yield* sql<{
        policy_json: string;
      }>`SELECT policy_json FROM jones_remote_admission_policy WHERE singleton=1`)[0];
      if (policy) {
        const current = yield* Schema.decodeEffect(
          Schema.fromJsonString(RemoteAdmissionReceiverPolicy),
        )(policy.policy_json);
        if (current.enabled && current.modelSelection === null) return false;
      }
      const ceiling = clientModeCeiling(client);
      const requested = {
        receiverEnvironmentId: scope.environmentId,
        sessionId: client.sessionId,
        originEnvironmentId: "unused",
        originThreadId: "unused",
        destinationThreadId: record.envelope.target.threadId,
        operation: "t3_thread_send" as const,
        runtimeMode: record.envelope.target.runtimeMode,
        interactionMode: record.envelope.target.interactionMode,
      };
      const base = { ...requested, ...ceiling };
      if (!withinAccessCeiling(base, requested)) return false;
      const policyCeiling = managed.document?.limits?.maxRuntimeMode;
      if (policyCeiling && !withinAccessCeiling({ ...base, runtimeMode: policyCeiling }, requested))
        return false;
      const projectId = record.envelope.target.projectId;
      if (projectId !== null) {
        const project = yield* projects.get(ProjectId.make(projectId));
        if (Option.isNone(project) || project.value.deletedAt !== null) return false;
      }
      const params = payload.params;
      const ids = [
        record.envelope.target.threadId,
        params?.sourceThreadId,
        params?.targetThreadId,
      ].filter((id): id is string => typeof id === "string");
      for (const id of new Set(ids)) {
        const target = yield* projections.getThreadShell(ThreadId.make(id));
        if (
          !target ||
          target.deletedAt !== null ||
          (projectId !== null &&
            id === record.envelope.target.threadId &&
            target.projectId !== projectId)
        )
          return false;
        if (
          client.linked &&
          client.access !== "full-access" &&
          target.linkOrigin?.sessionId !== client.sessionId
        )
          return false;
        if (
          !withinAccessCeiling(base, {
            ...requested,
            runtimeMode: target.runtimeMode,
            interactionMode: target.interactionMode,
          }) ||
          (policyCeiling &&
            !withinAccessCeiling(
              { ...base, runtimeMode: policyCeiling },
              {
                ...requested,
                runtimeMode: target.runtimeMode,
                interactionMode: target.interactionMode,
              },
            ))
        )
          return false;
      }
      return true;
    },
    Effect.mapError(() => new RecoveryUnavailable({})),
  );
}

/** Constructed after admission/tracker, before intake; no intake dependency or duplicate SQL layer. */
export const runtimeAdaptersLayer = Layer.unwrap(
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient,
      admissions = yield* Admission.RemoteAdmission;
    const projections = yield* Projections.ProjectionStoreV2,
      projects = yield* Projects.ProjectStoreV2;
    const managed = yield* Managed.ManagedPolicy,
      tracker = yield* ReceiverClaimTracker;
    const receiverEnvironmentId = yield* (yield* Environment.ServerEnvironment).getEnvironmentId;
    const receipts = yield* Receipts.CommandReceiptStoreV2;
    const authorize = makeCurrentAuthority(sql, projections, projects, managed);
    const liveDestinations = (ids: ReadonlyArray<string>) =>
      Effect.forEach([...new Set(ids)], (id) =>
        projections
          .getThreadShell(ThreadId.make(id))
          .pipe(Effect.map((shell) => (shell && shell.deletedAt === null ? id : null))),
      ).pipe(
        Effect.map((ids) => ids.filter((id): id is string => id !== null)),
        Effect.mapError(sqlFailure),
      );
    const validLeaseIds = Effect.fn("AdmissionRuntime.validLeaseIds")(
      function* (grants: ReadonlyArray<Grant>) {
        const now = DateTime.formatIso(yield* DateTime.now),
          valid: string[] = [];
        for (const grant of grants) {
          if (grant.state !== "active" || grant.expires_at <= now || !grant.destination_thread_id)
            continue;
          const binding = yield* Schema.decodeEffect(
            Schema.fromJsonString(Review.RemoteAdmissionAccessBinding),
          )(grant.binding_json);
          if (
            binding.receiverEnvironmentId !== receiverEnvironmentId ||
            binding.sessionId !== grant.session_id ||
            binding.destinationThreadId !== grant.destination_thread_id
          )
            continue;
          // The original immutable admission stores the verified token ceiling; never invent one from a grant.
          const record = yield* findGrantSourceAdmission(sql, admissions, grant);
          if (!record) continue;
          const current = {
            ...record,
            envelope: {
              ...record.envelope,
              target: {
                ...record.envelope.target,
                threadId: binding.destinationThreadId,
                runtimeMode: binding.runtimeMode,
                interactionMode: binding.interactionMode,
              },
            },
          };
          if (yield* authorize(current)) valid.push(grant.grant_id);
        }
        return valid;
      },
      Effect.mapError(sqlFailure),
    );
    const root = makeReturnBindingReader(sql, admissions);
    return Layer.merge(
      Layer.succeed(ReceiverRecoveryEvidence, {
        read: makeLaunchEvidenceReader(sql, receipts, projections, tracker.has),
        root: Effect.fn("AdmissionRuntime.returnBinding")((record: RemoteAdmissionRecord) =>
          root(record).pipe(Effect.mapError(() => new RecoveryUnavailable({}))),
        ),
        authorize,
      }),
      Layer.succeed(ReceiverReviewReaders, {
        receiverEnvironmentId,
        liveDestinations,
        validLeaseIds,
      }),
    );
  }),
);
