import { CommandId, ThreadId, type OrchestrationV2ThreadProjection } from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProviderSessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as ThreadCommands from "../../orchestration-v2/ThreadCommandExecutor.ts";
import * as ServerSettings from "../../serverSettings.ts";
import {
  workModeCandidate,
  workModeCommand,
  workModeContext,
  type WorkModeCandidate,
} from "../workMode/Policy.ts";
import { sameOperationBinding, type NativeOperationBinding } from "./launcherOperation.ts";
import type { QualifiedRuntimeBinding } from "../cloud/qualifiedRuntime.ts";
import {
  PlannedThreadSnapshot,
  captureThreadContinuity,
  canReleasePlannedQueue,
  changesPlannedControl,
  continuationRunIds,
  samePlannedThread,
  workOwnerIdentity,
} from "./plannedContinuityPolicy.ts";

export class PlannedContinuityError extends Schema.TaggedError<PlannedContinuityError>()(
  "PlannedContinuityError",
  { cause: Schema.Defect() },
) {}

export interface PlannedUpdateProof {
  readonly operationId: string;
  readonly outcome: "committed" | "rolled-back";
  readonly binding: NativeOperationBinding;
  readonly current: QualifiedRuntimeBinding;
}

export class PlannedUpdateContinuity extends Context.Service<
  PlannedUpdateContinuity,
  {
    readonly capture: (input: {
      readonly operationId: string;
      readonly binding: NativeOperationBinding;
      readonly continueRunningThreads: boolean;
    }) => Effect.Effect<void, PlannedContinuityError>;
    readonly activate: (proof: PlannedUpdateProof) => Effect.Effect<void, PlannedContinuityError>;
    readonly queueThreadIds: Effect.Effect<ReadonlyArray<ThreadId>>;
    readonly queueCommand: (
      projection: OrchestrationV2ThreadProjection,
    ) => Effect.Effect<CommandId | undefined, PlannedContinuityError>;
    readonly finishQueue: (threadId: ThreadId) => Effect.Effect<void, PlannedContinuityError>;
    readonly hasWork: (threadId: ThreadId) => Effect.Effect<boolean, PlannedContinuityError>;
    readonly admitWork: (
      projection: OrchestrationV2ThreadProjection,
      candidate: WorkModeCandidate,
      nowMs: number,
    ) => Effect.Effect<boolean, PlannedContinuityError>;
    readonly finishWork: (threadId: ThreadId) => Effect.Effect<void, PlannedContinuityError>;
    readonly allowWorkStart: (
      threadId: ThreadId,
      messageId: string,
    ) => Effect.Effect<boolean, PlannedContinuityError>;
  }
>()("t3/jones/updates/PlannedUpdateContinuity") {}

const decodeBinding = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      environmentId: Schema.String,
      currentVersion: Schema.String,
      expectedInstalledSource: Schema.String,
      targetSource: Schema.String,
      stagedHandle: Schema.String,
      baseDir: Schema.String,
      dbPath: Schema.String,
      targetVersion: Schema.String,
    }),
  ),
);
const decodeSnapshot = Schema.decodeUnknownSync(Schema.fromJsonString(PlannedThreadSnapshot));

export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const sessions = yield* ProviderSessions.ProviderSessionManagerV2;
  const threadCommands = yield* ThreadCommands.ThreadCommandExecutor;
  const settings = yield* ServerSettings.ServerSettingsService;
  const active = yield* Ref.make<
    | {
        operationId: string;
        threadIds: ReadonlyArray<ThreadId>;
        queueThreadIds: ReadonlyArray<ThreadId>;
      }
    | undefined
  >(undefined);
  const safe = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(Effect.mapError((cause) => new PlannedContinuityError({ cause })));
  const liveOwner = (owner: NonNullable<ReturnType<typeof workModeContext>>) =>
    owner.providerSessionId === null
      ? Effect.succeed(false)
      : sessions
          .get(owner.providerSessionId)
          .pipe(
            Effect.map(
              (session) =>
                Option.isSome(session) &&
                session.value.instanceId === owner.providerInstanceId &&
                session.value.providerSession.status === "ready",
            ),
          );

  const read = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const current = yield* Ref.get(active);
      if (current === undefined || !current.threadIds.includes(threadId)) return undefined;
      const rows = yield* sql<{ snapshot_json: string; queue_done: number; work_done: number }>`
      SELECT snapshot_json,queue_done,work_done FROM jones_planned_update_threads
      WHERE operation_id=${current.operationId} AND thread_id=${threadId}`;
      const row = rows[0];
      if (row === undefined) return undefined;
      const snapshot = yield* Effect.try(() => decodeSnapshot(row.snapshot_json));
      return { ...row, snapshot, operationId: current.operationId };
    });

  const controlsUnchanged = (
    snapshot: PlannedThreadSnapshot,
    projection: OrchestrationV2ThreadProjection,
  ) =>
    Effect.gen(function* () {
      if (snapshot.receiptRowId !== 0) {
        const anchor = yield* sql<{
          command_id: string;
        }>`SELECT command_id FROM orchestration_command_receipts WHERE rowid=${snapshot.receiptRowId}`;
        // A missing anchor cannot distinguish pruning/rowid reuse from a quiet thread.
        if (anchor[0]?.command_id !== snapshot.receiptCommandId) return false;
      }
      const receipts = yield* sql<{ command_id: string; command_type: string }>`
      SELECT command_id,command_type FROM orchestration_command_receipts
      WHERE aggregate_kind='thread' AND aggregate_id=${snapshot.threadId}
        AND rowid>${snapshot.receiptRowId} AND status='accepted'`;
      const ids = continuationRunIds(snapshot, projection);
      if (receipts.some((receipt) => changesPlannedControl(receipt, snapshot, ids))) return false;
      const preferences = yield* settings.getSettings;
      return (
        snapshot.explicitContinuation ||
        resolveProjectSettings(preferences, projection.thread.projectId).settings
          .continueThreadsAfterServerUpdate
      );
    });

  const finish = (threadId: ThreadId, column: "queue_done" | "work_done") =>
    safe(
      Effect.gen(function* () {
        const current = yield* Ref.get(active);
        if (current === undefined || !current.threadIds.includes(threadId)) return;
        yield* sql`UPDATE jones_planned_update_threads SET ${sql(column)}=1
      WHERE operation_id=${current.operationId} AND thread_id=${threadId}`;
        if (column === "queue_done")
          yield* Ref.update(active, (value) =>
            value === undefined
              ? value
              : {
                  ...value,
                  queueThreadIds: value.queueThreadIds.filter((id) => id !== threadId),
                },
          );
      }),
    );

  return PlannedUpdateContinuity.of({
    capture: (input) =>
      safe(
        Effect.gen(function* () {
          const preferences = yield* settings.getSettings;
          const nowMs = DateTime.toEpochMillis(yield* DateTime.now);
          const queued = new Set(yield* projections.getRecoveryThreadIds("queued-runs"));
          const shells = yield* projections.getShellSnapshot({
            location: "active",
            unsettledOnly: true,
          });
          const snapshots: PlannedThreadSnapshot[] = [];
          for (const shell of shells.threads) {
            if (
              !input.continueRunningThreads &&
              !resolveProjectSettings(preferences, shell.projectId).settings
                .continueThreadsAfterServerUpdate
            )
              continue;
            const workCandidate = preferences.workModeEnabled
              ? workModeCandidate(shell, nowMs, { ignoreInterval: true })
              : null;
            if (!queued.has(shell.id) && workCandidate === null) continue;
            yield* threadCommands.withLock(
              shell.id,
              Effect.gen(function* () {
                // Most completed shells belong to evicted sessions; avoid loading those histories.
                let hasLiveWork = false;
                if (workCandidate !== null) {
                  const context = yield* projections.getThreadProviderContext(
                    shell.id,
                    shell.modelSelection.instanceId,
                  );
                  const owner = context.providerThreads.find(
                    (row) => row.id === context.thread.activeProviderThreadId,
                  );
                  if (owner !== undefined) hasLiveWork = yield* liveOwner(owner);
                }
                if (!queued.has(shell.id) && !hasLiveWork) return;
                const projection = yield* projections.getThreadProjection(shell.id);
                const latestControl = (yield* sql<{ command_type: string }>`
            SELECT command_type FROM orchestration_command_receipts
            WHERE aggregate_kind='thread' AND aggregate_id=${shell.id} AND status='accepted'
              AND command_type IN ('message.dispatch','queue.resume','turn.interrupt','thread.stop','provider-session.detach')
            ORDER BY rowid DESC LIMIT 1`)[0]?.command_type;
                if (
                  latestControl !== undefined &&
                  !["message.dispatch", "queue.resume"].includes(latestControl)
                )
                  return;
                const anchor = (yield* sql<{ row_id: number; command_id: string }>`
            SELECT rowid AS row_id,command_id FROM orchestration_command_receipts ORDER BY rowid DESC LIMIT 1`)[0];
                const snapshot = captureThreadContinuity({
                  projection,
                  explicitContinuation: input.continueRunningThreads,
                  workModeEnabled: preferences.workModeEnabled,
                  liveWorkOwner: hasLiveWork,
                  nowMs,
                  receiptRowId: anchor?.row_id ?? 0,
                  receiptCommandId: anchor?.command_id ?? "",
                });
                if (snapshot !== undefined) snapshots.push(snapshot);
              }),
            );
          }
          yield* sql.withTransaction(
            Effect.gen(function* () {
              const prior = yield* sql<{
                binding_json: string;
              }>`SELECT binding_json FROM jones_planned_update_continuity WHERE operation_id=${input.operationId}`;
              if (prior[0] !== undefined) {
                if (prior[0].binding_json !== JSON.stringify(input.binding))
                  return yield* Effect.fail(new Error("Planned update operation binding changed."));
                return;
              }
              yield* sql`INSERT INTO jones_planned_update_continuity(operation_id,binding_json)
          VALUES(${input.operationId},${JSON.stringify(input.binding)})`;
              for (const snapshot of snapshots)
                yield* sql`INSERT INTO jones_planned_update_threads(operation_id,thread_id,snapshot_json)
          VALUES(${input.operationId},${snapshot.threadId},${JSON.stringify(snapshot)})`;
            }),
          );
        }),
      ),
    activate: (proof) =>
      safe(
        Effect.gen(function* () {
          yield* Ref.set(active, undefined);
          const rows = yield* sql<{
            binding_json: string;
            activated: number;
          }>`SELECT binding_json,activated FROM jones_planned_update_continuity WHERE operation_id=${proof.operationId}`;
          if (rows[0] === undefined || rows[0].activated !== 0) return;
          const binding = yield* Effect.try(() => decodeBinding(rows[0]!.binding_json));
          const expectedVersion =
            proof.outcome === "committed" ? binding.targetVersion : binding.currentVersion;
          const expectedSource =
            proof.outcome === "committed" ? binding.targetSource : binding.expectedInstalledSource;
          if (
            !sameOperationBinding(binding, proof.binding) ||
            proof.current.baseDir !== binding.baseDir ||
            proof.current.dbPath !== binding.dbPath ||
            proof.current.environmentId !== binding.environmentId ||
            proof.current.activeVersion !== expectedVersion ||
            proof.current.activeSourceSha !== expectedSource
          )
            return yield* Effect.fail(
              new Error("Planned update outcome does not match the restored native generation."),
            );
          const claimed = yield* sql`UPDATE jones_planned_update_continuity SET activated=1
        WHERE operation_id=${proof.operationId} AND activated=0 RETURNING operation_id`;
          if (claimed.length === 0) return;
          const threads = yield* sql<{
            thread_id: string;
            snapshot_json: string;
            queue_done: number;
          }>`SELECT thread_id,snapshot_json,queue_done FROM jones_planned_update_threads WHERE operation_id=${proof.operationId}`;
          const queued = yield* Effect.try(() =>
            threads.filter(
              (row) => row.queue_done === 0 && decodeSnapshot(row.snapshot_json).queue !== null,
            ),
          );
          yield* Ref.set(active, {
            operationId: proof.operationId,
            threadIds: threads.map((row) => ThreadId.make(row.thread_id)),
            queueThreadIds: queued.map((row) => ThreadId.make(row.thread_id)),
          });
        }),
      ),
    queueThreadIds: Ref.get(active).pipe(Effect.map((value) => value?.queueThreadIds ?? [])),
    queueCommand: (projection) =>
      safe(
        Effect.gen(function* () {
          const row = yield* read(projection.thread.id);
          if (
            row === undefined ||
            row.queue_done !== 0 ||
            !canReleasePlannedQueue(row.snapshot, projection) ||
            !(yield* controlsUnchanged(row.snapshot, projection))
          )
            return undefined;
          return CommandId.make(
            `command:planned-update-queue:${row.operationId}:${projection.thread.id}`,
          );
        }),
      ),
    finishQueue: (threadId) => finish(threadId, "queue_done"),
    hasWork: (threadId) =>
      safe(
        read(threadId).pipe(
          Effect.map(
            (row) =>
              row !== undefined && row.work_done === 0 && row.snapshot.workGeneration !== null,
          ),
        ),
      ),
    admitWork: (projection, candidate, nowMs) =>
      safe(
        Effect.gen(function* () {
          const row = yield* read(projection.thread.id);
          if (
            row === undefined ||
            row.work_done !== 0 ||
            row.snapshot.workGeneration !== candidate.generation ||
            !samePlannedThread(row.snapshot, projection) ||
            !(yield* controlsUnchanged(row.snapshot, projection))
          )
            return false;
          const preferences = yield* settings.getSettings;
          if (!preferences.workModeEnabled) return false;
          const owner = workModeContext(projection, nowMs, { allowStoppedSession: true });
          return owner !== null && workOwnerIdentity(owner) === row.snapshot.workOwner;
        }),
      ),
    finishWork: (threadId) => finish(threadId, "work_done"),
    allowWorkStart: (threadId, messageId) =>
      safe(
        Effect.gen(function* () {
          const row = yield* read(threadId);
          if (row?.snapshot.workGeneration == null) return false;
          const command = workModeCommand({ threadId, generation: row.snapshot.workGeneration });
          if (command.messageId !== messageId) return false;
          const receipt = yield* sql<{
            status: string;
          }>`SELECT status FROM orchestration_command_receipts WHERE command_id=${command.commandId}`;
          if (receipt[0]?.status !== "accepted") return false;
          const projection = yield* projections.getThreadProjection(threadId);
          const owner = projection.providerThreads.find(
            (thread) => thread.id === projection.thread.activeProviderThreadId,
          );
          const preferences = yield* settings.getSettings;
          return (
            preferences.workModeEnabled &&
            owner !== undefined &&
            samePlannedThread(row.snapshot, projection) &&
            workOwnerIdentity(owner) === row.snapshot.workOwner &&
            (yield* controlsUnchanged(row.snapshot, projection))
          );
        }),
      ),
  });
});

export const layer = Layer.effect(PlannedUpdateContinuity, make);
