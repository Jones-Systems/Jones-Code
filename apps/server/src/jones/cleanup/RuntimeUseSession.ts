import {
  readRuntimeUseSessionTarget,
  revalidateRuntimeUseConversation,
} from "./RuntimeUseConversation.ts";
import { OrchestrationV2ProviderSessionJson } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { overlaps } from "../ordinaryCheckout/CheckoutTarget.ts";
import {
  assertCheckedDatabaseOwner,
  type CheckedDatabaseOwnerToken,
} from "./CheckedRuntimeOwner.ts";
import { currentRuntimeUseClaim, validateRuntimeUseClaim } from "./RuntimeUseClaim.ts";
import { canonical, RuntimeUseError, runtimeUseError } from "./RuntimeUseProtocol.ts";
import { captureRuntimeUseWorkspace } from "./RuntimeUseWorkspace.ts";

const scopeSchema = Schema.Struct({
  id: Schema.NonEmptyString,
  projectId: Schema.NonEmptyString,
  createdAt: Schema.NonEmptyString,
  worktreePath: Schema.NullOr(Schema.NonEmptyString),
  deletedAt: Schema.NullOr(Schema.String),
});
const decodeScope = Schema.decodeUnknownEffect(Schema.fromJsonString(scopeSchema));
const decodeSession = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2ProviderSessionJson),
);
const fail = (reason: string) => Effect.fail(new RuntimeUseError({ reason }));

/** Captures the actual current caller, target and session. Filesystem identity
 * is observed outside SQL; the returned validation runs in the owning writer
 * transaction. No latest worker, thread or session substitutes for this caller. */
export const captureRuntimeUseSession = Effect.fn("RuntimeUseSession.capture")(function* (input: {
  readonly owner: CheckedDatabaseOwnerToken;
  readonly threadId: string;
  readonly providerSessionId: string;
  readonly instanceId: string;
  readonly driver: string;
}) {
  const sql = yield* SqlClient.SqlClient;
  const claim = Option.getOrNull(yield* currentRuntimeUseClaim);
  const checkOwner = Effect.try({
    try: () => assertCheckedDatabaseOwner(input.owner),
    catch: runtimeUseError,
  });
  const read = Effect.fn("RuntimeUseSession.read")(function* () {
    const threads = yield* sql<{
      readonly thread_id: string;
      readonly project_id: string;
      readonly payload_json: string;
      readonly workspace_root: string;
      readonly project_deleted_at: string | null;
    }>`
      SELECT t.thread_id,t.project_id,t.payload_json,p.workspace_root,p.deleted_at AS project_deleted_at
      FROM orchestration_v2_projection_threads t JOIN projection_projects p ON p.project_id=t.project_id
      WHERE t.thread_id=${input.threadId}`;
    const row = threads[0];
    if (row === undefined || row.project_deleted_at !== null)
      return yield* fail("runtime_use_scope_missing");
    const thread = yield* decodeScope(row.payload_json);
    if (
      thread.id !== input.threadId ||
      thread.id !== row.thread_id ||
      thread.projectId !== row.project_id ||
      thread.deletedAt !== null
    )
      return yield* fail("runtime_use_scope_changed");
    const sessions = yield* sql<{
      readonly payload_json: string;
      readonly provider_instance_id: string;
      readonly driver: string;
      readonly provider: string;
      readonly status: string;
      readonly updated_at: string;
    }>`
      SELECT payload_json,provider_instance_id,driver,provider,status,updated_at FROM orchestration_v2_projection_provider_sessions
      WHERE provider_session_id=${input.providerSessionId}`;
    const sessionRow = sessions[0];
    if (sessionRow === undefined) return yield* fail("runtime_use_session_missing");
    const session = yield* decodeSession(sessionRow.payload_json);
    if (
      session.id !== input.providerSessionId ||
      session.providerInstanceId !== input.instanceId ||
      session.driver !== input.driver ||
      sessionRow.provider_instance_id !== input.instanceId ||
      sessionRow.driver !== input.driver ||
      sessionRow.provider !== input.instanceId ||
      sessionRow.status !== session.status ||
      sessionRow.updated_at !== DateTime.formatIso(session.updatedAt) ||
      session.status === "stopped" ||
      session.status === "error"
    )
      return yield* fail("runtime_use_session_changed");
    // Activity/status may change during an admitted operation; original identity
    // and physical scope may not. Required session fields are decoded in full.
    const target = yield* readRuntimeUseSessionTarget({
      threadId: input.threadId,
      projectId: thread.projectId,
      worktreePath: thread.worktreePath,
      projectRoot: row.workspace_root,
    }).pipe(Effect.provideService(SqlClient.SqlClient, sql));
    return {
      thread,
      projectRoot: row.workspace_root,
      target,
      session: {
        id: session.id,
        createdAt: DateTime.formatIso(session.createdAt),
        cwd: session.cwd,
        providerInstanceId: session.providerInstanceId,
        driver: session.driver,
      },
    };
  });
  yield* checkOwner;
  const snapshot = yield* read();
  const physical = yield* captureRuntimeUseWorkspace(snapshot.target.path);
  const workspace = {
    ...physical,
    revalidate: physical.revalidate.pipe(
      Effect.andThen(revalidateRuntimeUseConversation(snapshot.target.conversation)),
    ),
  };
  yield* workspace.revalidate;
  const validate = Effect.gen(function* () {
    if (canonical(yield* read()) !== canonical(snapshot))
      return yield* fail("runtime_use_scope_or_session_replaced");
    if (claim !== null) {
      if (claim.threadId !== input.threadId)
        return yield* fail("runtime_use_caller_thread_mismatch");
      yield* validateRuntimeUseClaim(claim).pipe(Effect.provideService(SqlClient.SqlClient, sql));
    }
    const deletions = yield* sql<{ readonly worktree_path: string }>`SELECT worktree_path
      FROM jones_deletion_worktree_admissions WHERE state!='completed' ORDER BY worktree_path`;
    if (deletions.some((row) => overlaps(workspace.canonicalPath, row.worktree_path, "/")))
      return yield* fail("runtime_use_conflicting_deletion");
  }).pipe(Effect.mapError(runtimeUseError));
  const validateAttachment = Effect.gen(function* () {
    yield* validate;
    const bindings =
      yield* sql`SELECT thread_id FROM orchestration_v2_projection_provider_session_bindings
      WHERE provider_session_id=${input.providerSessionId} AND thread_id=${input.threadId}`;
    if (bindings.length !== 1) return yield* fail("runtime_use_session_attachment_missing");
  }).pipe(Effect.mapError(runtimeUseError));
  return { snapshot, workspace, claim, validate, validateAttachment, checkOwner };
}, Effect.mapError(runtimeUseError));
