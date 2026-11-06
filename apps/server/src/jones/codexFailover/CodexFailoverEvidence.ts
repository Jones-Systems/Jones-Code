import type {
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import type {
  ServerNotification__CodexErrorInfo,
  ServerNotification__TurnStatus,
} from "effect-codex-app-server/schema";
import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import { classifyCodexFailoverFailure } from "../provider/codex/CodexFailoverFailure.ts";
import { codexFailoverContraryInfo } from "./CodexFailoverNativeEvidence.ts";

export interface CodexFailoverEvidenceInput {
  readonly providerTurnId: ProviderTurnId;
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly attemptId: RunAttemptId;
  readonly providerThreadId: ProviderThreadId;
  readonly providerInstanceId: ProviderInstanceId;
  /** Terminal occurrence time in ISO 8601 format, supplied by the caller. */
  readonly occurredAt: string;
  readonly status: ServerNotification__TurnStatus | "cancelled";
  readonly evidenceKind?: "provider_result" | "attributed_abort" | "local_failure" | undefined;
  readonly rawErrorInfo?: ServerNotification__CodexErrorInfo | null | undefined;
  readonly capacityExhausted: boolean;
}

export class CodexFailoverEvidence extends Context.Service<
  CodexFailoverEvidence,
  { readonly record: (input: CodexFailoverEvidenceInput) => Effect.Effect<void> }
>()("t3/jones/codexFailover/CodexFailoverEvidence") {}

/** Record native evidence before terminal normalization discards HTTP status. */
export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const tablePresent = makeJonesTablePresence(sql);

  const record = Effect.fn("CodexFailoverEvidence.record")(
    function* (input: CodexFailoverEvidenceInput) {
      if (input.status !== "failed" || input.evidenceKind === "local_failure") return;
      const disposition =
        classifyCodexFailoverFailure(input.rawErrorInfo) ??
        (input.capacityExhausted && !codexFailoverContraryInfo(input.rawErrorInfo)
          ? "unavailable"
          : null);
      if (disposition === null) return;

      yield* sql.withTransaction(
        Effect.gen(function* () {
          if (!(yield* tablePresent("jones_codex_failover_evidence"))) return;
          // Both lookups use upstream primary keys. The provider turn projection
          // may not exist yet when a native terminal beats event ingestion.
          const runs = yield* sql`SELECT 1 FROM orchestration_v2_projection_runs
          WHERE run_id = ${input.runId} AND thread_id = ${input.threadId}
            AND provider_thread_id = ${input.providerThreadId}
            AND provider_instance_id = ${input.providerInstanceId}`;
          if (runs.length === 0) return;
          const attempts = yield* sql`SELECT 1 FROM orchestration_v2_projection_run_attempts
          WHERE attempt_id = ${input.attemptId} AND run_id = ${input.runId}
            AND thread_id = ${input.threadId}
            AND provider_thread_id = ${input.providerThreadId}
            AND provider_instance_id = ${input.providerInstanceId}`;
          if (attempts.length === 0) return;

          yield* sql`INSERT INTO jones_codex_failover_evidence
          (provider_turn_id, thread_id, run_id, attempt_id, provider_thread_id,
            provider_instance_id, disposition, occurred_at)
          VALUES (${input.providerTurnId}, ${input.threadId}, ${input.runId}, ${input.attemptId},
            ${input.providerThreadId}, ${input.providerInstanceId}, ${disposition}, ${input.occurredAt})
          ON CONFLICT(provider_turn_id) DO NOTHING`;
        }),
      );
    },
    (effect, input) =>
      effect.pipe(
        // Catch outside the transaction so failed writes roll back before terminal
        // processing resumes. Do not log native payloads or SQL error text.
        Effect.catchCause(() =>
          Effect.logWarning("Codex failover evidence was not recorded").pipe(
            Effect.annotateLogs({
              providerTurnId: input.providerTurnId.slice(0, 128),
              runId: input.runId.slice(0, 128),
              attemptId: input.attemptId.slice(0, 128),
            }),
          ),
        ),
      ),
  );

  return { record };
});

export const layer = Layer.effect(CodexFailoverEvidence, make);
