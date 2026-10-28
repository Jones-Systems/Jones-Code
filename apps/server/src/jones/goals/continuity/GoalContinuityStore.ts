// @effect-diagnostics-next-line nodeBuiltinImport:off -- Mint a sparse record identity synchronously inside the SQL transaction.
import * as NodeCrypto from "node:crypto";
import {
  ContinuitySnapshot,
  GoalContinuityError,
  OrchestrationV2ProviderThreadJson,
  OrchestrationV2RunAttemptJson,
  type ContinuityOwner,
  type ContinuityWitness,
  type EnvironmentId,
  type ThreadId,
  type RuntimeMode,
  type ProviderInteractionMode,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { RecordedAppThreadJson, RecordedRunJson } from "../../../orchestration-v2/RecordedTypes.ts";
import { makeJonesTablePresence } from "../../persistence/JonesTablePresence.ts";
import { canonicalJson, continuityDigest, sameContinuityOwner, validateSnapshot } from "./model.ts";

export interface ContinuityRecord {
  readonly recordId: string;
  readonly snapshot: ContinuitySnapshot;
  readonly digest: string;
}
export interface ContinuityRowKey {
  readonly kind:
    | "mutation"
    | "proposal"
    | "confirmation"
    | "delivery"
    | "phase"
    | "disposition"
    | "fence";
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly namespace: string;
  readonly key: string;
}
export interface ContinuityStoredRow {
  readonly bodyDigest: string;
  readonly payload: string;
  readonly open: boolean;
}
export interface ContinuityCommitLimits {
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: ProviderInteractionMode;
  readonly linkSessionId?: string;
}

export class GoalContinuityStore extends Context.Service<
  GoalContinuityStore,
  {
    readonly transaction: <A, E, R>(
      effect: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | GoalContinuityError, R>;
    readonly readOnly: <A, E, R>(
      effect: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | GoalContinuityError, R>;
    readonly current: (
      environmentId: EnvironmentId,
      threadId: ThreadId,
    ) => Effect.Effect<ContinuityRecord | null, GoalContinuityError>;
    readonly save: (
      previous: ContinuityRecord | null,
      next: ContinuitySnapshot,
    ) => Effect.Effect<ContinuityRecord, GoalContinuityError>;
    readonly validateBinding: (
      owner: ContinuityOwner,
      witness?: ContinuityWitness | null,
      limits?: ContinuityCommitLimits,
      requireCurrent?: boolean,
    ) => Effect.Effect<void, GoalContinuityError>;
    readonly row: (
      key: ContinuityRowKey,
    ) => Effect.Effect<ContinuityStoredRow | null, GoalContinuityError>;
    readonly writeRow: (
      key: ContinuityRowKey,
      previous: ContinuityStoredRow | null,
      value: unknown,
      open?: boolean,
    ) => Effect.Effect<void, GoalContinuityError>;
    readonly deliveryPage: (
      environmentId: EnvironmentId,
      threadId: ThreadId,
      after: string,
    ) => Effect.Effect<
      readonly (ContinuityStoredRow & { readonly key: string })[],
      GoalContinuityError
    >;
    readonly openDeliveries: (
      environmentId: EnvironmentId,
      threadId: ThreadId,
    ) => Effect.Effect<readonly ContinuityStoredRow[], GoalContinuityError>;
  }
>()("t3/jones/goals/continuity/GoalContinuityStore") {}

const storageError = (cause: unknown) =>
  Schema.is(GoalContinuityError)(cause)
    ? cause
    : new GoalContinuityError({ reason: "unavailable" });
const corrupt = () => new GoalContinuityError({ reason: "corrupt_snapshot" });
const stale = () => new GoalContinuityError({ reason: "stale" });
const decodeSnapshot = Schema.decodeUnknownEffect(Schema.fromJsonString(ContinuitySnapshot));
const decodeThread = Schema.decodeUnknownEffect(Schema.fromJsonString(RecordedAppThreadJson));
const decodeRun = Schema.decodeUnknownEffect(Schema.fromJsonString(RecordedRunJson));
const decodeAttempt = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2RunAttemptJson),
);
const decodeProvider = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2ProviderThreadJson),
);
const ranks: Record<RuntimeMode, number> = {
  "approval-required": 0,
  "auto-accept-edits": 1,
  auto: 2,
  "full-access": 3,
};

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const present = makeJonesTablePresence(sql);
  const row: GoalContinuityStore["Service"]["row"] = Effect.fn("GoalContinuityStore.row")(
    function* (key) {
      if (!(yield* present("jones_goal_continuity_receipts"))) return null;
      const rows = yield* sql<{
        body_digest: string;
        payload_json: string;
        open_gate: number;
      }>`SELECT body_digest, payload_json, open_gate
      FROM jones_goal_continuity_receipts WHERE kind=${key.kind} AND environment_id=${key.environmentId}
      AND thread_id=${key.threadId} AND namespace=${key.namespace} AND receipt_key=${key.key}`;
      const found = rows[0];
      return found
        ? {
            bodyDigest: found.body_digest,
            payload: found.payload_json,
            open: found.open_gate === 1,
          }
        : null;
    },
    Effect.mapError(storageError),
  );
  const writeRow: GoalContinuityStore["Service"]["writeRow"] = Effect.fn(
    "GoalContinuityStore.writeRow",
  )(function* (key, previous, value, open = false) {
    const payload = canonicalJson(value);
    const digest = continuityDigest(value);
    if (previous === null) {
      const inserted = yield* sql`INSERT INTO jones_goal_continuity_receipts
        (kind,environment_id,thread_id,namespace,receipt_key,body_digest,payload_json,open_gate)
        VALUES (${key.kind},${key.environmentId},${key.threadId},${key.namespace},${key.key},${digest},${payload},${open ? 1 : 0})
        ON CONFLICT DO NOTHING RETURNING receipt_key`;
      if (inserted.length !== 1) return yield* stale();
    } else {
      // Mutation/phase/disposition receipts are immutable, including refused closes.
      if (!["delivery", "fence"].includes(key.kind)) return yield* stale();
      const updated =
        yield* sql`UPDATE jones_goal_continuity_receipts SET body_digest=${digest},payload_json=${payload},open_gate=${open ? 1 : 0}
        WHERE kind=${key.kind} AND environment_id=${key.environmentId} AND thread_id=${key.threadId}
        AND namespace=${key.namespace} AND receipt_key=${key.key} AND body_digest=${previous.bodyDigest}
        RETURNING receipt_key`;
      if (updated.length !== 1) return yield* stale();
    }
  }, Effect.mapError(storageError));
  const current: GoalContinuityStore["Service"]["current"] = Effect.fn(
    "GoalContinuityStore.current",
  )(function* (environmentId, threadId) {
    if (!(yield* present("jones_goal_continuity"))) return null;
    const rows = yield* sql<{
      record_id: string;
      snapshot_digest: string;
      snapshot_json: string;
    }>`SELECT record_id,snapshot_digest,snapshot_json
      FROM jones_goal_continuity WHERE environment_id=${environmentId} AND thread_id=${threadId} AND current_record=1`;
    const found = rows[0];
    if (!found) return null;
    const snapshot = yield* decodeSnapshot(found.snapshot_json).pipe(Effect.mapError(corrupt));
    if (
      validateSnapshot(snapshot) ||
      continuityDigest(snapshot) !== found.snapshot_digest ||
      snapshot.owner.environmentId !== environmentId ||
      snapshot.owner.threadId !== threadId
    )
      return yield* corrupt();
    return { recordId: found.record_id, digest: found.snapshot_digest, snapshot };
  }, Effect.mapError(storageError));
  const save: GoalContinuityStore["Service"]["save"] = Effect.fn("GoalContinuityStore.save")(
    function* (previous, next) {
      const issue = validateSnapshot(next);
      if (issue) return yield* new GoalContinuityError({ reason: issue.reason });
      const digest = continuityDigest(next);
      const sameRecord =
        previous !== null &&
        sameContinuityOwner(previous.snapshot.owner, next.owner) &&
        previous.snapshot.intent.goalId === next.intent.goalId &&
        previous.snapshot.intent.revision === next.intent.revision;
      // An explicit handoff back to an earlier conversation creates a new record;
      // it must neither overwrite nor collide with that conversation's history.
      const recordId = sameRecord ? previous!.recordId : NodeCrypto.randomUUID();
      if (previous !== null) {
        const changed = yield* sql`UPDATE jones_goal_continuity SET current_record=0
        WHERE record_id=${previous.recordId} AND current_record=1 AND snapshot_digest=${previous.digest} RETURNING record_id`;
        if (changed.length !== 1) return yield* stale();
      }
      const existing = yield* sql<{
        snapshot_digest: string;
      }>`SELECT snapshot_digest FROM jones_goal_continuity WHERE record_id=${recordId}`;
      if (existing[0]) {
        if (previous?.recordId !== recordId || existing[0].snapshot_digest !== previous.digest)
          return yield* stale();
        yield* sql`UPDATE jones_goal_continuity SET environment_id=${next.owner.environmentId},thread_id=${next.owner.threadId},
        snapshot_digest=${digest},snapshot_json=${canonicalJson(next)},current_record=1 WHERE record_id=${recordId}`;
      } else {
        const inserted =
          yield* sql`INSERT INTO jones_goal_continuity(record_id,environment_id,thread_id,current_record,snapshot_digest,snapshot_json)
        VALUES (${recordId},${next.owner.environmentId},${next.owner.threadId},1,${digest},${canonicalJson(next)}) ON CONFLICT DO NOTHING RETURNING record_id`;
        if (inserted.length !== 1) return yield* stale();
      }
      return { recordId, snapshot: next, digest };
    },
    Effect.mapError(storageError),
  );
  const validateBinding: GoalContinuityStore["Service"]["validateBinding"] = Effect.fn(
    "GoalContinuityStore.validateBinding",
  )(function* (owner, witness, limits, requireCurrent = false) {
    const threads = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id=${owner.threadId}`;
    const providers = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_projection_provider_threads
      WHERE provider_thread_id=${owner.providerThreadId} AND thread_id=${owner.threadId}`;
    if (!threads[0] || !providers[0])
      return yield* new GoalContinuityError({ reason: "unavailable" });
    const thread = yield* decodeThread(threads[0].payload_json).pipe(Effect.mapError(corrupt));
    const provider = yield* decodeProvider(providers[0].payload_json).pipe(
      Effect.mapError(corrupt),
    );
    if (
      thread.deletedAt !== null ||
      ((witness || requireCurrent) &&
        (thread.activeProviderThreadId !== owner.providerThreadId ||
          thread.providerInstanceId !== owner.providerInstanceId)) ||
      provider.appThreadId !== owner.threadId ||
      provider.providerInstanceId !== owner.providerInstanceId ||
      provider.driver !== owner.driver ||
      provider.nativeThreadRef?.strength !== "strong" ||
      provider.nativeThreadRef.driver !== owner.driver ||
      provider.nativeThreadRef.nativeId !== owner.nativeThreadId
    )
      return yield* new GoalContinuityError({ reason: "binding_mismatch" });
    if (
      limits &&
      (ranks[thread.runtimeMode] > ranks[limits.runtimeMode] ||
        (limits.interactionMode === "plan" && thread.interactionMode === "default") ||
        (limits.linkSessionId !== undefined &&
          thread.linkOrigin?.sessionId !== limits.linkSessionId))
    ) {
      return yield* new GoalContinuityError({ reason: "forbidden" });
    }
    if (!witness) return;
    const runs = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_projection_runs WHERE run_id=${witness.runId} AND thread_id=${owner.threadId}`;
    const attempts = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_projection_run_attempts WHERE attempt_id=${witness.attemptId} AND thread_id=${owner.threadId}`;
    const sessions =
      yield* sql`SELECT provider_session_id FROM orchestration_v2_projection_provider_sessions
      WHERE provider_session_id=${witness.providerSessionId} AND provider_instance_id=${owner.providerInstanceId}
      AND driver=${owner.driver} AND status IN ('starting','ready','running','waiting')`;
    const active = yield* sql<{
      run_id: string;
    }>`SELECT run_id FROM orchestration_v2_projection_runs
      WHERE thread_id=${owner.threadId} AND status IN ('preparing','starting','running','waiting')
      ORDER BY ordinal DESC,run_id DESC LIMIT 1`;
    if (!runs[0] || !attempts[0] || !sessions[0])
      return yield* new GoalContinuityError({ reason: "unavailable" });
    const run = yield* decodeRun(runs[0].payload_json).pipe(Effect.mapError(corrupt));
    const attempt = yield* decodeAttempt(attempts[0].payload_json).pipe(Effect.mapError(corrupt));
    if (
      thread.archivedAt !== null ||
      active[0]?.run_id !== witness.runId ||
      run.activeAttemptId !== witness.attemptId ||
      !["starting", "running", "waiting"].includes(run.status) ||
      !["starting", "running", "waiting"].includes(attempt.status) ||
      run.providerInstanceId !== owner.providerInstanceId ||
      run.providerThreadId !== owner.providerThreadId ||
      attempt.runId !== witness.runId ||
      attempt.providerThreadId !== owner.providerThreadId ||
      attempt.providerInstanceId !== owner.providerInstanceId ||
      attempt.nativeThreadId !== owner.nativeThreadId ||
      provider.providerSessionId !== witness.providerSessionId ||
      provider.runtimeIdentity?.runtimeGeneration !== witness.runtimeGeneration
    ) {
      return yield* new GoalContinuityError({ reason: "binding_mismatch" });
    }
  }, Effect.mapError(storageError));
  const deliveryPage: GoalContinuityStore["Service"]["deliveryPage"] = Effect.fn(
    "GoalContinuityStore.deliveryPage",
  )(function* (environmentId, threadId, after) {
    if (!(yield* present("jones_goal_continuity_receipts"))) return [];
    // The primary key supplies the thread-local keyset range. Filtering JSON
    // in SQL could scan an unbounded number of other goals' receipts.
    const rows = yield* sql<{
      receipt_key: string;
      body_digest: string;
      payload_json: string | null;
      open_gate: number;
    }>`SELECT receipt_key,body_digest,open_gate,
      CASE WHEN length(CAST(payload_json AS BLOB)) <= 1048576 THEN payload_json ELSE NULL END AS payload_json
      FROM jones_goal_continuity_receipts
      WHERE kind='delivery' AND environment_id=${environmentId} AND thread_id=${threadId}
      AND namespace='server' AND receipt_key > ${after}
      ORDER BY receipt_key LIMIT 5`;
    return rows.map((entry) => ({
      key: entry.receipt_key,
      bodyDigest: entry.body_digest,
      payload: entry.payload_json ?? "",
      open: entry.open_gate === 1,
    }));
  }, Effect.mapError(storageError));
  const openDeliveries: GoalContinuityStore["Service"]["openDeliveries"] = Effect.fn(
    "GoalContinuityStore.openDeliveries",
  )(function* (environmentId, threadId) {
    if (!(yield* present("jones_goal_continuity_receipts"))) return [];
    const rows = yield* sql<{
      body_digest: string;
      payload_json: string;
      open_gate: number;
    }>`SELECT body_digest,payload_json,open_gate
      FROM jones_goal_continuity_receipts WHERE environment_id=${environmentId} AND thread_id=${threadId} AND kind='delivery' AND open_gate=1`;
    return rows.map((entry) => ({
      bodyDigest: entry.body_digest,
      payload: entry.payload_json,
      open: entry.open_gate === 1,
    }));
  }, Effect.mapError(storageError));
  return GoalContinuityStore.of({
    current,
    save,
    validateBinding,
    row,
    writeRow,
    openDeliveries,
    deliveryPage,
    transaction: (effect) => sql.withTransaction(effect).pipe(Effect.mapError(storageError)),
    readOnly: (effect) =>
      sql.withTransaction(effect).pipe(NodeSqliteClient.readOnly, Effect.mapError(storageError)),
  });
});
export const layer = Layer.effect(GoalContinuityStore, make);
