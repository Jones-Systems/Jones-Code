#!/usr/bin/env node

// @effect-diagnostics nodeBuiltinImport:off -- This native boundary supervises a captured child because synchronous SQLite cannot be interrupted by an Effect timeout.
// @effect-diagnostics globalTimers:off -- Finite arrival timers are drained and cleared; the canonical E supervisor owns termination and reap deadlines.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodePerfHooks from "node:perf_hooks";
import * as NodeURL from "node:url";
import * as Cause from "effect/Cause";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";

import {
  captureSyntheticFixture,
  withOpenSyntheticFixture,
  type SyntheticFixtureCapture,
  type SyntheticFixtureContext,
  type SyntheticDatabaseSource,
} from "../../../scripts/performance-staging/fixtures.mjs";
import {
  createOwnedRoot,
  disposeOwnedRoot,
  type OwnedChildReceipt,
  type OwnedCleanupReceipt,
  type StagingPolicy,
} from "../../../scripts/performance-staging/guard.mjs";
import { runOwnedChild } from "../../../scripts/performance-staging/lifecycle.mjs";
import {
  sourceParentEnvironment,
  syntheticDatabaseSource,
  syntheticSourceParent,
} from "../../../scripts/performance-staging/sources.mjs";

const scriptPath = NodeURL.fileURLToPath(import.meta.url);
const worktreePath = NodePath.resolve(NodePath.dirname(scriptPath), "../../..");
const sourceRevisions = [
  "e5a31aceec91484b64315c63dcce80f6e7581604",
  "414bb8da204c3275cd0b76b2ec4d74dfb09a97e4",
] as const;
const taskRef = "spec.jones-performance-portfolio#task.d-bench.001";
const requestBytes = 24 * 1024;
const stdoutBytes = 64 * 1024;
const stderrBytes = 16 * 1024;

const boundedInt = (minimum: number, maximum: number) =>
  Schema.Int.pipe(Schema.check(Schema.isBetween({ minimum, maximum })));
const text = Schema.String.pipe(Schema.check(Schema.isMinLength(1), Schema.isMaxLength(4096)));
const Binding = Schema.Struct({
  repository: Schema.Literal("Jones-Systems/Jones-Code"),
  sourceRevision: text,
  taskRef: Schema.Literal(taskRef),
  runId: text,
});
const Policy = Schema.Struct({
  homePath: text,
  worktreePaths: Schema.Array(text).pipe(Schema.check(Schema.isMaxLength(16))),
  protectedPaths: Schema.Array(text).pipe(Schema.check(Schema.isMaxLength(32))),
});
const Source = Schema.Struct({
  repository: Schema.Literal("Jones-Systems/Jones-Code"),
  sourceRevision: Schema.Literals(sourceRevisions),
  worktreePath: text,
});
export const BenchmarkRequest = Schema.Struct({
  parentPath: text,
  binding: Binding,
  databaseSource: Source,
  trials: boundedInt(1, 5),
  turns: boundedInt(1, 64),
  historyTurns: boundedInt(3, 256),
  payloadBytes: boundedInt(1, 65536),
  arrival: Schema.Literals(["steady", "burst"]),
  intervalMs: boundedInt(0, 1000),
  burstSize: boundedInt(1, 64),
  timeoutMs: boundedInt(100, 120000),
}).pipe(
  Schema.check(
    Schema.makeFilter(
      (request) => (request.historyTurns + request.turns) * request.payloadBytes <= 4 * 1024 * 1024,
    ),
  ),
);
export type BenchmarkRequest = typeof BenchmarkRequest.Type;
const WorkerRequest = Schema.Struct({
  request: BenchmarkRequest,
  policy: Policy,
});
type WorkerRequest = typeof WorkerRequest.Type;

function refuse(code: string): never {
  throw new Error(code);
}

function decodeJson<Value>(schema: Schema.Codec<Value>, encoded: string): Value {
  if (Buffer.byteLength(encoded) > requestBytes) refuse("request_limit");
  return Schema.decodeUnknownSync(Schema.fromJsonString(schema), {
    onExcessProperty: "error",
  })(encoded);
}

export function benchmarkMetadata() {
  return {
    schema: "jones-sqlite-benchmark-metadata/v1",
    execution: "explicit --run-json only",
    profile: "benchmark-wal",
    taskRef,
    caps: {
      trials: 5,
      turns: 64,
      historyTurns: 256,
      payloadBytes: 65536,
      combinedTurnPayloadBytes: 4 * 1024 * 1024,
      timeoutMs: 120000,
    },
    databaseSources: sourceRevisions.map((sourceRevision) =>
      syntheticDatabaseSource(sourceRevision),
    ),
    boundaries: {
      sqlExecution: "unavailable",
      transaction: "unavailable",
      pureLockWait: "unavailable",
    },
  };
}

export function benchmarkPolicy(homePath = NodeOS.homedir()): StagingPolicy {
  return {
    homePath,
    worktreePaths: [worktreePath],
    protectedPaths: [".t3", ".codex", ".config", ".ssh"].map((name) =>
      NodePath.join(homePath, name),
    ),
  };
}

function summary(values: readonly number[]) {
  const ordered = [...values].sort((left, right) => left - right);
  return {
    samples: ordered.length,
    minimumMs: ordered[0] ?? null,
    medianMs: ordered[Math.floor(ordered.length / 2)] ?? null,
    p95Ms: ordered[Math.max(0, Math.ceil(ordered.length * 0.95) - 1)] ?? null,
    maximumMs: ordered.at(-1) ?? null,
  };
}

function count(capture: SyntheticFixtureCapture, name: string) {
  const table = capture.tables[name];
  if (!table || table.status !== "present") refuse("required_table_absent");
  return table.count;
}

function sameState(left: SyntheticFixtureCapture, right: SyntheticFixtureCapture) {
  return (
    JSON.stringify(left.tables) === JSON.stringify(right.tables) &&
    JSON.stringify(left.coupling) === JSON.stringify(right.coupling) &&
    left.readModel.snapshotSha256 === right.readModel.snapshotSha256
  );
}

async function sourceModules(source: SyntheticDatabaseSource) {
  const require = NodeModule.createRequire(
    NodePath.join(source.worktreePath, "apps/server/package.json"),
  );
  const [Effect, SourceSchema, SqlClient, Contracts] = await Promise.all([
    import(NodeURL.pathToFileURL(require.resolve("effect/Effect")).href) as Promise<
      typeof import("effect/Effect")
    >,
    import(NodeURL.pathToFileURL(require.resolve("effect/Schema")).href) as Promise<
      typeof import("effect/Schema")
    >,
    import(NodeURL.pathToFileURL(require.resolve("effect/unstable/sql/SqlClient")).href) as Promise<
      typeof import("effect/unstable/sql/SqlClient")
    >,
    import(
      NodeURL.pathToFileURL(NodePath.join(source.worktreePath, "packages/contracts/src/index.ts"))
        .href
    ) as Promise<typeof import("@t3tools/contracts")>,
  ]);
  return { Effect, SourceSchema, SqlClient, Contracts };
}

async function runTrial(
  context: SyntheticFixtureContext,
  request: BenchmarkRequest,
  setFailurePhase: (phase: FailurePhase) => void,
) {
  const { Effect, SourceSchema, SqlClient, Contracts } = await sourceModules(
    context.databaseSource,
  );
  setFailurePhase("command-decode");
  const decodeSourceCommand = SourceSchema.decodeUnknownSync(Contracts.OrchestrationCommand);
  setFailurePhase("trial-callback");
  const decodeCommand = (input: unknown) => {
    setFailurePhase("command-decode");
    const command = decodeSourceCommand(input);
    setFailurePhase("trial-callback");
    return command;
  };
  const sql = await context.run(Effect.service(SqlClient.SqlClient));
  const captureCosts: number[] = [];
  const capture = async () => {
    const started = NodePerfHooks.performance.now();
    try {
      return await captureSyntheticFixture(context);
    } finally {
      captureCosts.push(NodePerfHooks.performance.now() - started);
    }
  };
  const before = await capture();
  const storage = () => {
    const bytes = (path: string) => {
      try {
        const info = NodeFS.lstatSync(path, { throwIfNoEntry: false });
        return !info
          ? 0
          : info.isFile() && !info.isSymbolicLink() && info.nlink === 1
            ? info.size
            : null;
      } catch {
        return null;
      }
    };
    return {
      databaseBytes: bytes(context.paths.dbPath),
      walBytes: bytes(`${context.paths.dbPath}-wal`),
    };
  };
  const initialStorage = storage();
  const pragmas = before.runtime.pragmas;
  if (
    before.runtime.profile !== "benchmark-wal" ||
    pragmas.journal_mode !== "wal" ||
    pragmas.synchronous !== 2 ||
    pragmas.foreign_keys !== 1 ||
    pragmas.busy_timeout !== 5000 ||
    pragmas.journal_size_limit !== 32 * 1024 * 1024
  )
    refuse("durability_profile_mismatch");
  const createdAt = "2026-10-02T18:00:00.000Z";
  const threadId = "fixture-empty";
  const payload = "b".repeat(request.payloadBytes);
  const commands = Array.from({ length: request.turns }, (_, index) => {
    const commandBase = { threadId, createdAt };
    return [
      decodeCommand({
        ...commandBase,
        type: "thread.turn.start",
        commandId: `bench-user-${index}`,
        message: { messageId: `bench-user-${index}`, role: "user", text: payload, attachments: [] },
        runtimeMode: "full-access",
        interactionMode: "default",
      }),
      decodeCommand({
        ...commandBase,
        type: "thread.message.assistant.delta",
        commandId: `bench-delta-${index}`,
        messageId: `bench-assistant-${index}`,
        delta: payload,
      }),
      decodeCommand({
        ...commandBase,
        type: "thread.message.assistant.complete",
        commandId: `bench-complete-${index}`,
        messageId: `bench-assistant-${index}`,
      }),
      decodeCommand({
        ...commandBase,
        type: "thread.activity.append",
        commandId: `bench-activity-${index}`,
        activity: {
          id: `bench-activity-${index}`,
          kind: "benchmark.completed",
          summary: "Synthetic work",
          tone: "info",
          turnId: null,
          payload: { synthetic: true },
          createdAt,
        },
      }),
    ];
  });
  const schedulingLag: number[] = [];
  const harnessQueue: number[] = [];
  const trafficEngineCompletion: number[] = [];
  const arrivalCompletion: number[] = [];
  const protocolEngineCompletion: number[] = [];
  let backlog = 0;
  let maximumBacklog = 0;
  let attempts = 0;
  let fulfilledTerminals = 0;
  let failures = 0;
  const eventLoop = NodePerfHooks.monitorEventLoopDelay({ resolution: 10 });
  const utilization = NodePerfHooks.performance.eventLoopUtilization();
  eventLoop.enable();
  await new Promise<void>((resolve) => setImmediate(resolve));
  const started = NodePerfHooks.performance.now();
  let drain = Promise.resolve();
  const timers = new Set<ReturnType<typeof setTimeout>>();
  const arrivals: Promise<void>[] = [];
  const dispatch = async (
    command: typeof Contracts.OrchestrationCommand.Type,
    arrivalAt?: number,
  ) => {
    const dispatchedAt = NodePerfHooks.performance.now();
    if (arrivalAt !== undefined) harnessQueue.push(dispatchedAt - arrivalAt);
    attempts += 1;
    try {
      const result = await context.run(context.engine.dispatch(command));
      fulfilledTerminals += 1;
      return result;
    } catch (error) {
      failures += 1;
      throw error;
    } finally {
      const terminalAt = NodePerfHooks.performance.now();
      if (arrivalAt === undefined) protocolEngineCompletion.push(terminalAt - dispatchedAt);
      else {
        trafficEngineCompletion.push(terminalAt - dispatchedAt);
        arrivalCompletion.push(terminalAt - arrivalAt);
      }
    }
  };
  let elapsedMs;
  let loop;
  try {
    commands.forEach((batch, index) => {
      const offset =
        request.arrival === "steady"
          ? index * request.intervalMs
          : Math.floor(index / request.burstSize) * request.intervalMs;
      arrivals.push(
        new Promise<void>((resolve) => {
          const timer = setTimeout(
            () => {
              timers.delete(timer);
              const offeredAt = NodePerfHooks.performance.now();
              schedulingLag.push(Math.max(0, offeredAt - started - offset));
              backlog += batch.length;
              maximumBacklog = Math.max(maximumBacklog, backlog);
              drain = drain.then(async () => {
                for (const command of batch) {
                  try {
                    await dispatch(command, offeredAt);
                  } finally {
                    backlog -= 1;
                  }
                }
              });
              void drain.catch(() => {});
              resolve();
            },
            Math.max(0, started + offset - NodePerfHooks.performance.now()),
          );
          timers.add(timer);
        }),
      );
    });
    await Promise.all(arrivals);
    await drain;
    elapsedMs = NodePerfHooks.performance.now() - started;
    await new Promise<void>((resolve) => setImmediate(resolve));
    loop = {
      utilization: NodePerfHooks.performance.eventLoopUtilization(utilization).utilization,
      samples: eventLoop.count,
      meanDelayMs: eventLoop.count ? eventLoop.mean / 1e6 : null,
      maximumDelayMs: eventLoop.count ? eventLoop.max / 1e6 : null,
      p95DelayMs: eventLoop.count ? eventLoop.percentile(95) / 1e6 : null,
    };
  } finally {
    for (const timer of timers) clearTimeout(timer);
    eventLoop.disable();
  }
  const afterTraffic = await capture();
  const afterTrafficStorage = storage();
  const replayCommand = commands[0]?.[0];
  if (!replayCommand) refuse("missing_command");
  await dispatch(replayCommand);
  const afterReplay = await capture();
  const acceptedReplayUnchanged = sameState(afterTraffic, afterReplay);
  const rejected = decodeCommand({
    type: "thread.activity.append",
    commandId: "bench-rejected",
    threadId: "bench-absent",
    createdAt,
    activity: {
      id: "bench-rejected",
      kind: "benchmark.completed",
      summary: "Synthetic rejection",
      tone: "info",
      turnId: null,
      payload: { synthetic: true },
      createdAt,
    },
  });
  const expectFailure = async (command: typeof Contracts.OrchestrationCommand.Type) => {
    let failed = false;
    try {
      await dispatch(command);
    } catch {
      failed = true;
    }
    if (!failed) refuse("expected_failure_was_accepted");
  };
  await expectFailure(rejected);
  const afterRejection = await capture();
  await expectFailure(rejected);
  const afterRejectedReplay = await capture();
  const rejectedReplayUnchanged = sameState(afterRejection, afterRejectedReplay);
  const retryCommand = decodeCommand({
    type: "thread.activity.append",
    commandId: "bench-retry",
    threadId,
    createdAt,
    activity: {
      id: "bench-rollback",
      kind: "benchmark.completed",
      summary: "Synthetic retry",
      tone: "info",
      turnId: null,
      payload: { synthetic: true },
      createdAt,
    },
  });
  // Keep this fixed fault local: fail after event append at the actual activity projection, then remove it before retry.
  await context.run(
    sql.unsafe(
      "CREATE TEMP TRIGGER bench_projection_failure BEFORE INSERT ON projection_thread_activities WHEN NEW.activity_id='bench-rollback' BEGIN SELECT RAISE(ABORT,'synthetic benchmark failure'); END",
    ),
  );
  try {
    await expectFailure(retryCommand);
  } finally {
    await context.run(sql.unsafe("DROP TRIGGER bench_projection_failure"));
  }
  const afterRollback = await capture();
  const rollbackUnchanged = sameState(afterRejectedReplay, afterRollback);
  await dispatch(retryCommand);
  const final = await capture();
  const receipts = await context.run(
    sql<{
      command_id: string;
      status: string;
    }>`SELECT command_id,status FROM orchestration_command_receipts WHERE command_id IN ('bench-rejected','bench-retry') ORDER BY command_id`,
  );
  const eventDelta = count(final, "orchestration_events") - count(before, "orchestration_events");
  const receiptDelta =
    count(final, "orchestration_command_receipts") -
    count(before, "orchestration_command_receipts");
  const consistency = {
    acceptedReplayUnchanged,
    rejectedReplayUnchanged,
    rollbackUnchanged,
    rejectionPersisted:
      receipts.find((row) => row.command_id === "bench-rejected")?.status === "rejected",
    retryAccepted: receipts.find((row) => row.command_id === "bench-retry")?.status === "accepted",
    eventDelta,
    expectedEventDelta: request.turns * 5 + 1,
    receiptDelta,
    expectedReceiptDelta: request.turns * 4 + 2,
    replayEquivalent: final.readModel.equivalent,
    snapshotSequence: final.coupling.snapshotSequence,
    journalSequence: final.coupling.maxSequence,
    integrity: final.integrity.ok,
    foreignKeyViolations: final.foreignKeys.violations,
  };
  if (
    !acceptedReplayUnchanged ||
    !rejectedReplayUnchanged ||
    !rollbackUnchanged ||
    !consistency.rejectionPersisted ||
    !consistency.retryAccepted ||
    eventDelta !== consistency.expectedEventDelta ||
    receiptDelta !== consistency.expectedReceiptDelta ||
    !consistency.replayEquivalent ||
    !consistency.integrity ||
    consistency.foreignKeyViolations !== 0 ||
    consistency.snapshotSequence !== consistency.journalSequence ||
    final.integrity.results.length !== 1 ||
    final.integrity.results[0] !== "ok" ||
    final.coupling.missing_receipt_events !== 0 ||
    final.coupling.missing_thread_projects !== 0 ||
    final.coupling.missing_message_threads !== 0 ||
    final.coupling.noncontiguous_streams !== 0 ||
    final.coupling.projectionCursors.some(
      (cursor) =>
        cursor.projector !== "projection.attachment-cleanup" &&
        cursor.sequence !== consistency.journalSequence,
    )
  )
    refuse("workload_consistency_failed");
  return {
    runtime: before.runtime,
    workload: {
      turns: request.turns,
      historyTurns: request.historyTurns,
      payloadBytes: request.payloadBytes,
      arrival: request.arrival,
      intervalMs: request.intervalMs,
      burstSize: request.burstSize,
    },
    traffic: {
      offered: request.turns * 4,
      attempts: request.turns * 4,
      accepted: request.turns * 4,
      finalBacklog: backlog,
      maximumBacklog,
      elapsedMs,
    },
    outcomes: {
      offered: request.turns * 4 + 4,
      attempts,
      accepted: request.turns * 4 + 1,
      fulfilledTerminals,
      failures,
      replays: 2,
      rejections: 2,
      retries: 1,
      retryKind: "synthetic projection abort removed before retry; no production retry policy",
    },
    timing: {
      schedulingLag: summary(schedulingLag),
      traffic: {
        harnessQueue: summary(harnessQueue),
        engineCompletion: summary(trafficEngineCompletion),
        arrivalCompletion: summary(arrivalCompletion),
      },
      protocol: { engineCompletion: summary(protocolEngineCompletion) },
      sqlExecution: {
        status: "unavailable",
        reason: "fixture exposes no statement timing observer",
      },
      transaction: { status: "unavailable", reason: "engine worker owns transaction scope" },
      pureWriterLockWait: {
        status: "unavailable",
        reason: "dispatch includes queue, SQL, projection and publication",
      },
    },
    eventLoop: loop,
    maintenance: {
      canonicalCapture: summary(captureCosts),
      storage: { before: initialStorage, afterTraffic: afterTrafficStorage },
    },
    consistency,
  };
}

type Trial = Awaited<ReturnType<typeof runTrial>>;
const natural = Schema.Int.pipe(Schema.check(Schema.isGreaterThanOrEqualTo(0)));
const milliseconds = Schema.Number.pipe(
  Schema.check(Schema.isFinite(), Schema.isGreaterThanOrEqualTo(0)),
);
const TimingSummary = Schema.Struct({
  samples: natural,
  minimumMs: Schema.NullOr(milliseconds),
  medianMs: Schema.NullOr(milliseconds),
  p95Ms: Schema.NullOr(milliseconds),
  maximumMs: Schema.NullOr(milliseconds),
});
const Unavailable = Schema.Struct({ status: Schema.Literal("unavailable"), reason: text });
const Storage = Schema.Struct({
  databaseBytes: Schema.NullOr(natural),
  walBytes: Schema.NullOr(natural),
});
const TrialFrame = Schema.Struct({
  runtime: Schema.Struct({
    nodeVersion: text,
    sqliteVersion: text,
    profile: Schema.Literal("benchmark-wal"),
    pragmas: Schema.Record(Schema.String, Schema.Union([Schema.String, Schema.Number])),
  }),
  workload: Schema.Struct({
    turns: boundedInt(1, 64),
    historyTurns: boundedInt(3, 256),
    payloadBytes: boundedInt(1, 65536),
    arrival: Schema.Literals(["steady", "burst"]),
    intervalMs: boundedInt(0, 1000),
    burstSize: boundedInt(1, 64),
  }),
  traffic: Schema.Struct({
    offered: natural,
    attempts: natural,
    accepted: natural,
    finalBacklog: natural,
    maximumBacklog: natural,
    elapsedMs: milliseconds,
  }),
  outcomes: Schema.Struct({
    offered: natural,
    attempts: natural,
    accepted: natural,
    fulfilledTerminals: natural,
    failures: natural,
    replays: natural,
    rejections: natural,
    retries: natural,
    retryKind: text,
  }),
  timing: Schema.Struct({
    schedulingLag: TimingSummary,
    traffic: Schema.Struct({
      harnessQueue: TimingSummary,
      engineCompletion: TimingSummary,
      arrivalCompletion: TimingSummary,
    }),
    protocol: Schema.Struct({ engineCompletion: TimingSummary }),
    sqlExecution: Unavailable,
    transaction: Unavailable,
    pureWriterLockWait: Unavailable,
  }),
  eventLoop: Schema.Struct({
    utilization: milliseconds,
    samples: natural,
    meanDelayMs: Schema.NullOr(milliseconds),
    maximumDelayMs: Schema.NullOr(milliseconds),
    p95DelayMs: Schema.NullOr(milliseconds),
  }),
  maintenance: Schema.Struct({
    canonicalCapture: TimingSummary,
    storage: Schema.Struct({ before: Storage, afterTraffic: Storage }),
  }),
  consistency: Schema.Struct({
    acceptedReplayUnchanged: Schema.Literal(true),
    rejectedReplayUnchanged: Schema.Literal(true),
    rollbackUnchanged: Schema.Literal(true),
    rejectionPersisted: Schema.Literal(true),
    retryAccepted: Schema.Literal(true),
    eventDelta: natural,
    expectedEventDelta: natural,
    receiptDelta: natural,
    expectedReceiptDelta: natural,
    replayEquivalent: Schema.Literal(true),
    snapshotSequence: natural,
    journalSequence: natural,
    integrity: Schema.Literal(true),
    foreignKeyViolations: Schema.Literal(0),
  }),
}).pipe(
  Schema.check(
    Schema.makeFilter(
      (trial) =>
        trial.consistency.eventDelta === trial.consistency.expectedEventDelta &&
        trial.consistency.receiptDelta === trial.consistency.expectedReceiptDelta &&
        trial.consistency.snapshotSequence === trial.consistency.journalSequence,
    ),
  ),
);
const failureClasses = [
  "Error",
  "TypeError",
  "RangeError",
  "SyntaxError",
  "ReferenceError",
  "PerformanceStagingError",
  "SqlError",
  "PersistenceSqlError",
  "PersistenceDecodeError",
  "OrchestrationCommandInvariantError",
  "OrchestrationCommandPreviouslyRejectedError",
  "OrchestrationCommandIdConflictError",
  "OrchestrationProjectorDecodeError",
  "SchemaError",
  "unknown",
] as const;
const failureCodes = [
  "invalid_source",
  "invalid_options",
  "unsupported_profile",
  "invalid_recipe",
  "invalid_context",
  "incoherent_fixture",
  "invalid_capture",
  "profile_mismatch",
  "profile_failed",
  "profile_cancelled",
  "profile_integrity_failed",
  "profile_content_changed",
  "profile_header_failed",
  "profile_identity_changed",
  "profile_sidecars_present",
  "profile_checkpoint_failed",
  "closed_context",
  "cleanup_retained",
  "protected_path",
  "outside_boundary",
  "manifest_limit",
  "invalid_owner",
  "unknown_close",
  "unknown_child",
  "cleanup_unknown",
  "ERR_SQLITE_ERROR",
  "ENOENT",
  "EACCES",
  "ERR_MODULE_NOT_FOUND",
  "durability_profile_mismatch",
  "required_table_absent",
  "missing_command",
  "expected_failure_was_accepted",
  "workload_consistency_failed",
] as const;
const safeFailureMessages = [
  "durability_profile_mismatch",
  "required_table_absent",
  "missing_command",
  "expected_failure_was_accepted",
  "workload_consistency_failed",
  "production state failed replay, cursor, coupling or integrity checks",
  "database source must match an exact root-bound baseline",
  "fixture options required",
  "profile must be health-offline-delete or benchmark-wal",
  "signal must be an AbortSignal",
  "coherent-v1 requires 3–256 turns and 1–65536 payload bytes",
  "production lease generation fencing failed",
  "synthetic file identity changed",
  "history recipe did not create an older-page cursor",
  "synthetic reference escapes the owned root",
  "checkpoint metadata names a missing synthetic file",
  "attachment metadata differs from owned file",
  "open fixture cleanup retained its root",
  "Sync adapter can only throw schema errors",
] as const;
const FailureDescriptor = Schema.Struct({
  classification: Schema.Literals(failureClasses),
  code: Schema.NullOr(Schema.Literals(failureCodes)),
  sqliteCode: Schema.NullOr(
    Schema.Int.pipe(Schema.check(Schema.isBetween({ minimum: 0, maximum: 65535 }))),
  ),
  message: Schema.NullOr(Schema.Literals(safeFailureMessages)),
  messageOmitted: Schema.Boolean,
});
const FailurePhase = Schema.Literals(["fixture-production", "trial-callback", "command-decode"]);
type FailurePhase = typeof FailurePhase.Type;
const PrimaryFailure = Schema.Struct({
  phase: FailurePhase,
  causes: Schema.Array(FailureDescriptor).pipe(
    Schema.check(Schema.isMinLength(1), Schema.isMaxLength(4)),
  ),
  truncated: Schema.Boolean,
});

function primaryFailure(error: unknown, phase: FailurePhase): typeof PrimaryFailure.Type {
  const causes: Array<typeof FailureDescriptor.Type> = [];
  const seen = new Set<unknown>();
  const pending = [{ value: error, depth: 0 }];
  let truncated = false;
  let inspected = 0;
  const enqueue = (value: unknown, depth: number) => {
    if (depth > 4 || pending.length >= 4) truncated = true;
    else pending.push({ value, depth });
  };
  while (causes.length < 4 && pending.length && inspected < 16) {
    const node = pending.shift();
    if (!node) break;
    const current = node.value;
    inspected += 1;
    if (seen.has(current)) {
      truncated = true;
      continue;
    }
    seen.add(current);
    if (Cause.isCause(current)) {
      if (current.reasons.length > 4) truncated = true;
      for (const reason of current.reasons.slice(0, 4)) {
        if (Cause.isFailReason(reason)) enqueue(reason.error, node.depth + 1);
        else if (Cause.isDieReason(reason)) enqueue(reason.defect, node.depth + 1);
        else truncated = true;
      }
      continue;
    }
    const object = Predicate.isObject(current) ? current : undefined;
    const tag = object && "_tag" in object ? object._tag : undefined;
    const name = object && "name" in object ? object.name : undefined;
    const code = object && "code" in object ? object.code : undefined;
    const sqliteCode = object && "errcode" in object ? object.errcode : undefined;
    const message = object && "message" in object ? object.message : undefined;
    const firstLine =
      typeof message === "string" ? message.slice(0, 256).split("\n", 1)[0] : undefined;
    const safeMessage = Schema.is(Schema.Literals(safeFailureMessages))(firstLine)
      ? firstLine
      : null;
    causes.push({
      classification: Schema.is(Schema.Literals(failureClasses))(tag)
        ? tag
        : Schema.is(Schema.Literals(failureClasses))(name)
          ? name
          : "unknown",
      code: Schema.is(Schema.Literals(failureCodes))(code) ? code : null,
      sqliteCode: Schema.is(FailureDescriptor.fields.sqliteCode)(sqliteCode) ? sqliteCode : null,
      message: safeMessage,
      messageOmitted: safeMessage === null,
    });
    if (object && "cause" in object && object.cause !== undefined)
      enqueue(object.cause, node.depth + 1);
  }
  if (!causes.length)
    causes.push({
      classification: "unknown",
      code: null,
      sqliteCode: null,
      message: null,
      messageOmitted: true,
    });
  return { phase, causes, truncated: truncated || pending.length > 0 };
}

const WorkerFrame = Schema.Struct({
  schema: Schema.Literal("jones-sqlite-benchmark-worker/v1"),
  status: Schema.Literals(["completed", "failed"]),
  fixtureCleanupKnown: Schema.Boolean,
  primaryFailure: Schema.NullOr(PrimaryFailure),
  trials: Schema.Array(TrialFrame).pipe(Schema.check(Schema.isMaxLength(5))),
});

async function worker(input: WorkerRequest, signal: AbortSignal) {
  const trials: Trial[] = [];
  let fixtureCleanupKnown = true;
  let failurePhase: FailurePhase = "fixture-production";
  try {
    for (let index = 0; index < input.request.trials; index += 1) {
      const result = await withOpenSyntheticFixture(
        {
          parentPath: input.request.parentPath,
          childName: `trial-${index}`,
          binding: input.request.binding,
          databaseSource: input.request.databaseSource,
          policy: input.policy,
          profile: "benchmark-wal",
          recipe: {
            historyTurns: input.request.historyTurns,
            payloadBytes: input.request.payloadBytes,
          },
          signal,
        },
        async (context) => {
          failurePhase = "trial-callback";
          const trial = await runTrial(context, input.request, (phase) => {
            failurePhase = phase;
          });
          failurePhase = "fixture-production";
          return trial;
        },
      );
      fixtureCleanupKnown = result.cleanup.outcome === "complete";
      trials.push(result.value);
    }
    return {
      schema: "jones-sqlite-benchmark-worker/v1",
      status: "completed",
      fixtureCleanupKnown,
      primaryFailure: null,
      trials,
    };
  } catch (error) {
    fixtureCleanupKnown =
      error instanceof Error &&
      "evidence" in error &&
      Schema.is(Schema.Struct({ cleanup: Schema.Struct({ outcome: Schema.Literal("complete") }) }))(
        error.evidence,
      );
    return {
      schema: "jones-sqlite-benchmark-worker/v1",
      status: "failed",
      fixtureCleanupKnown,
      primaryFailure: primaryFailure(error, failurePhase),
      trials,
    };
  }
}

function childSummary(child: OwnedChildReceipt | undefined) {
  return child
    ? {
        outcome: child.outcome,
        pid: child.pid,
        closed: child.closed,
        reaped: child.reaped,
        exitCode: child.exitCode,
        signal: child.signal,
        stopReason: child.stopReason,
        observedBytes: child.observedBytes,
        capturedBytes: child.capturedBytes,
        truncated: child.truncated,
      }
    : { outcome: "not_started", pid: null, closed: true, reaped: true };
}

export async function runSqliteBenchmark(
  input: BenchmarkRequest,
  options: { policy?: StagingPolicy; signal?: AbortSignal; runChild?: typeof runOwnedChild } = {},
) {
  const request = Schema.decodeUnknownSync(BenchmarkRequest, { onExcessProperty: "error" })(input);
  const policy = Schema.decodeUnknownSync(Policy)(options.policy ?? benchmarkPolicy());
  const owner = createOwnedRoot({
    parentPath: request.parentPath,
    // The guard receives one fresh exclusive supervisor name; this is not an Effect program.
    childName: `benchmark-${NodeCrypto.randomUUID()}`,
    binding: request.binding,
    policy,
  });
  let child: OwnedChildReceipt | undefined;
  let frame: typeof WorkerFrame.Type | undefined;
  let reason: string | null = null;
  let cleanup: OwnedCleanupReceipt;
  let childAttempted = false;
  try {
    const encoded = JSON.stringify({
      request: { ...request, parentPath: owner.creationReceipt.canonicalRootPath },
      policy,
    });
    if (Buffer.byteLength(encoded) > requestBytes) refuse("request_limit");
    const args = [scriptPath, "--worker-json", encoded];
    if (args.reduce((bytes, arg) => bytes + Buffer.byteLength(arg) + 1, 0) > 64 * 1024)
      refuse("argv_limit");
    childAttempted = true;
    child = await (options.runChild ?? runOwnedChild)({
      owner,
      executable: process.execPath,
      args,
      env: {
        HOME: policy.homePath,
        LANG: "C.UTF-8",
        TZ: "UTC",
        NODE_NO_WARNINGS: "1",
        [sourceParentEnvironment]: syntheticSourceParent(),
      },
      timeoutMs: request.timeoutMs,
      terminateGraceMs: 1000,
      reapTimeoutMs: 3000,
      maxOutputBytes: stdoutBytes + stderrBytes,
      ...(options.signal ? { signal: options.signal } : {}),
    });
    if (!child.closed || !child.reaped || child.outcome === "unknown")
      refuse("unknown_child_close");
    if (
      child.truncated ||
      Buffer.byteLength(child.stdout) > stdoutBytes ||
      Buffer.byteLength(child.stderr) > stderrBytes
    )
      refuse("output_limit");
    if (child.stdout) {
      frame = Schema.decodeUnknownSync(Schema.fromJsonString(WorkerFrame), {
        onExcessProperty: "error",
      })(child.stdout);
      if (`${JSON.stringify(frame)}\n` !== child.stdout) refuse("invalid_worker_framing");
    }
    if (child.outcome !== "success" || frame?.status !== "completed") refuse(child.outcome);
    if (frame.trials.length !== request.trials || !frame.fixtureCleanupKnown)
      refuse("incomplete_trials");
  } catch (error) {
    reason =
      error instanceof Error &&
      [
        "request_limit",
        "argv_limit",
        "unknown_child_close",
        "output_limit",
        "invalid_worker_framing",
        "incomplete_trials",
        "timed_out",
        "cancelled",
        "failed",
        "spawn_refused",
        "output_limited",
      ].includes(error.message)
        ? error.message
        : "invalid_worker_output";
  } finally {
    const noFixtureStarted =
      (!childAttempted && !child) ||
      (child?.pid === null && child.closed && child.reaped && child.outcome !== "unknown");
    const closed = child?.closed && child.reaped && child.outcome !== "unknown";
    cleanup =
      noFixtureStarted || (closed && frame?.fixtureCleanupKnown)
        ? disposeOwnedRoot(owner, { childReceipts: child ? [child] : [] })
        : {
            schema: "jones-performance-cleanup/v1",
            creationReceipt: owner.creationReceipt,
            outcome: "retained",
            absent: false,
            childReceipts: child ? [child] : [],
            reason: reason ?? "fixture_cleanup_unproved",
          };
  }
  return {
    schema: "jones-sqlite-benchmark/v1",
    status: reason ? "failed" : cleanup.outcome === "complete" ? "completed" : "failed",
    reason: reason ?? (cleanup.outcome === "complete" ? null : "cleanup_retained"),
    primaryFailure: frame?.primaryFailure ?? null,
    binding: request.binding,
    databaseSource: request.databaseSource,
    child: childSummary(child),
    cleanup: {
      outcome: cleanup.outcome,
      absent: cleanup.absent,
      retainedRoot: cleanup.absent ? null : owner.creationReceipt.canonicalRootPath,
    },
    trials: frame?.trials ?? [],
  };
}

if (
  process.argv[1] &&
  NodeURL.pathToFileURL(NodePath.resolve(process.argv[1])).href === import.meta.url
) {
  const cancellation = new AbortController();
  const cancel = () => cancellation.abort();
  process.once("SIGINT", cancel);
  process.once("SIGTERM", cancel);
  try {
    const args = process.argv.slice(2);
    if (args.length === 0 || (args.length === 1 && args[0] === "--help")) {
      process.stdout.write(`${JSON.stringify(benchmarkMetadata())}\n`);
    } else if (args.length === 2 && args[0] === "--worker-json" && args[1]) {
      const result = await worker(decodeJson(WorkerRequest, args[1]), cancellation.signal);
      const frame = Schema.decodeUnknownSync(WorkerFrame, { onExcessProperty: "error" })(result);
      const encoded = `${JSON.stringify(frame)}\n`;
      if (Buffer.byteLength(encoded) > stdoutBytes) refuse("output_limit");
      process.stdout.write(encoded);
      process.exitCode = result.status === "completed" ? 0 : 1;
    } else if (args.length === 2 && args[0] === "--run-json" && args[1]) {
      const result = await runSqliteBenchmark(decodeJson(BenchmarkRequest, args[1]), {
        signal: cancellation.signal,
      });
      process.stdout.write(`${JSON.stringify(result)}\n`);
      process.exitCode = result.status === "completed" ? 0 : 1;
    } else refuse("invalid_arguments");
  } catch {
    process.stdout.write(
      `${JSON.stringify({ schema: "jones-sqlite-benchmark/v1", status: "refused", reason: "invalid_request" })}\n`,
    );
    process.exitCode = 2;
  } finally {
    process.removeListener("SIGINT", cancel);
    process.removeListener("SIGTERM", cancel);
  }
}
