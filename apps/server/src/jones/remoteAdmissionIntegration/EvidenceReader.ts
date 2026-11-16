import { CommandId, ThreadId, type RemoteAdmissionRecord } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import type * as SqlClient from "effect/sql/SqlClient";
import type * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import type * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import {
  classifyLaunchEvidence,
  recoveryIdentities,
  recoveryNamespaceIdentities,
  type LaunchEvidenceInput,
  type RecoveryPath,
} from "./LaunchRecovery.ts";
import { RecoveryUnavailable } from "./RecoveryStore.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";

/** All three stores must be bound to the same receiver SQL client. A failed read defers recovery. */
export function makeLaunchEvidenceReader(
  sql: SqlClient.SqlClient,
  receipts: Receipts.CommandReceiptStoreV2["Service"],
  projections: Projections.ProjectionStoreV2["Service"],
  liveClaim: (admissionId: string) => boolean,
) {
  const presence = makeJonesTablePresence(sql);
  return Effect.fn("AdmissionRecovery.readEvidence")(
    function* (record: RemoteAdmissionRecord) {
      const payload = record.envelope.payload as {
        params: Record<string, unknown>;
        invocation: {
          environmentId: string;
          requestNamespace: string;
          client?: { remoteOrigin?: { environmentId: string; thread?: { threadId: string } } };
        };
      };
      const key =
        typeof payload.params.clientRequestId === "string" ? payload.params.clientRequestId : "";
      const ids = recoveryIdentities(record.envelope.identity.clientId, key);
      // Retry children are receiver-created, with an exact persisted attempt namespace.
      const attempts = (yield* presence("jones_remote_admission_attempts"))
        ? yield* sql<{
            predecessor_admission_id: string;
            attempt_id: string;
          }>`SELECT predecessor_admission_id,attempt_id FROM jones_remote_admission_attempts WHERE child_admission_id=${record.id}`
        : [];
      if (attempts[0]) {
        Object.assign(
          ids,
          recoveryNamespaceIdentities(
            `admission-retry:${attempts[0].predecessor_admission_id}`,
            attempts[0].attempt_id,
          ),
        );
      }
      const origin = payload.invocation.client?.remoteOrigin;
      const readPath = Effect.fn("AdmissionRecovery.readPath")(function* (
        commandId: string,
      ): Effect.fn.Return<RecoveryPath, unknown> {
        const creation = Option.getOrNull(
          yield* receipts.getByCommandId(CommandId.make(commandId)),
        );
        const message = Option.getOrNull(
          yield* receipts.getByCommandId(CommandId.make(`${commandId}:initial-message`)),
        );
        const shell = yield* projections.getThreadShell(ThreadId.make(commandId));
        const claims =
          origin?.thread && (yield* presence("jones_thread_links"))
            ? yield* sql`SELECT 1 FROM jones_thread_links
        WHERE launch_command_id=${commandId} AND kind='launch' AND from_environment_id=${origin.environmentId}
        AND from_thread_id=${origin.thread.threadId} AND to_environment_id=${payload.invocation.environmentId}
        AND to_thread_id=${commandId} AND asserted_by=${`link:${record.envelope.identity.clientId}`}`
            : [];
        return {
          commandId,
          creation,
          message,
          shell,
          claimMatches:
            claims.length > 0 &&
            (shell === null || shell.linkOrigin?.sessionId === record.envelope.identity.clientId),
        };
      });
      // Sequential reads retain the caller's SQL transaction; do not parallelize its connection.
      const hashed = yield* readPath(ids.hashed).pipe(
        Effect.mapError(() => new RecoveryUnavailable({})),
      );
      const legacy = yield* readPath(ids.legacy).pipe(
        Effect.mapError(() => new RecoveryUnavailable({})),
      );
      const live = liveClaim(record.id);
      return {
        keyed: key.length > 0,
        abandonedClaim:
          !live &&
          (record.status === "dispatching" || record.status === "dispatched") &&
          record.commandId !== null,
        readsComplete: true,
        liveClaim: live,
        expectedProjectId: record.envelope.target.projectId,
        initialMessageRequested:
          typeof payload.params.message === "string" ||
          (Array.isArray(payload.params.attachments) && payload.params.attachments.length > 0),
        observedAt: DateTime.formatIso(yield* DateTime.now),
        hashed,
        legacy,
      } satisfies LaunchEvidenceInput;
    },
    Effect.mapError(() => new RecoveryUnavailable({})),
  );
}

/** Policy-off launches have no admission. Positive exact receipts may be observed; absence stays unknown. */
export function makeKeyedLaunchResultReader(
  sql: SqlClient.SqlClient,
  receipts: Receipts.CommandReceiptStoreV2["Service"],
  projections: Projections.ProjectionStoreV2["Service"],
) {
  const presence = makeJonesTablePresence(sql);
  return Effect.fn("AdmissionRecovery.readKeyedResult")(
    function* (sessionId: string, clientRequestId: string) {
      const ids = recoveryIdentities(sessionId, clientRequestId);
      const read = Effect.fn("AdmissionRecovery.keyedPath")(function* (commandId: string) {
        const creation = Option.getOrNull(
          yield* receipts.getByCommandId(CommandId.make(commandId)),
        );
        const message = Option.getOrNull(
          yield* receipts.getByCommandId(CommandId.make(`${commandId}:initial-message`)),
        );
        const shell = yield* projections.getThreadShell(ThreadId.make(commandId));
        const claims = (yield* presence("jones_thread_links"))
          ? yield* sql`SELECT 1 FROM jones_thread_links
        WHERE launch_command_id=${commandId} AND kind='launch' AND to_thread_id=${commandId} AND asserted_by=${`link:${sessionId}`}`
          : [];
        return {
          commandId,
          creation,
          message,
          shell,
          claimMatches:
            claims.length > 0 && (shell === null || shell.linkOrigin?.sessionId === sessionId),
        };
      });
      const hashed = yield* read(ids.hashed),
        legacy = yield* read(ids.legacy);
      return classifyLaunchEvidence({
        keyed: true,
        abandonedClaim: false,
        readsComplete: true,
        liveClaim: false,
        expectedProjectId: hashed.shell?.projectId ?? legacy.shell?.projectId ?? null,
        initialMessageRequested: hashed.message !== null || legacy.message !== null,
        observedAt: DateTime.formatIso(yield* DateTime.now),
        hashed,
        legacy,
      }).result;
    },
    Effect.mapError(() => new RecoveryUnavailable({})),
  );
}
