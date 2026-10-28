import * as NodeURL from "node:url";
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import { migrationManifest, runMigrations } from "../../persistence/Migrations.ts";
import migration from "../persistence/Migrations/129_JonesCodexFailover.ts";
import * as Evidence from "./CodexFailoverEvidence.ts";
import * as Store from "./CodexFailoverStore.ts";

const now = "2026-10-26T20:00:00.000Z";
const later = "2026-10-27T20:00:00.000Z";
const threadId = ThreadId.make("thread:store");
const runId = RunId.make("run:store");
const turnId = ProviderTurnId.make("turn:store");
const source = ProviderInstanceId.make("codex-source");
const target = ProviderInstanceId.make("codex-target");
const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const upstream = runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] });
const step: Store.CodexFailoverStep = {
  switchCommandId: CommandId.make("switch:store"),
  threadId,
  chainRootRunId: runId,
  originRunId: runId,
  originProviderTurnId: turnId,
  sourceInstanceId: source,
  sourceFailure: "usage_limit",
  sourceResetAt: later,
  targetInstanceId: target,
  savedSelection: { instanceId: target, model: "saved-model" },
  continuationSelection: {
    instanceId: target,
    model: "origin-model",
    options: [{ id: "effort", value: "high" }],
  },
  continueCommandId: CommandId.make("continue:store"),
  continueMessageId: MessageId.make("message:store"),
  status: "switched",
  createdAt: now,
  updatedAt: now,
};
const seedRun = Effect.fn("CodexFailoverStore.test.seedRun")(function* (
  id: RunId = runId,
  thread: ThreadId = threadId,
  ordinal = 1,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO orchestration_v2_projection_runs
    (run_id, thread_id, ordinal, provider, provider_instance_id, provider_thread_id, status, requested_at, payload_json)
    VALUES (${id}, ${thread}, ${ordinal}, 'codex', ${source}, 'provider-thread:store', 'queued', ${now}, '{}')`;
});
const setup = Effect.gen(function* () {
  yield* upstream;
  yield* migration;
  yield* seedRun();
  const sql = yield* SqlClient.SqlClient;
  const attemptId = RunAttemptId.make("attempt:store");
  const providerThreadId = ProviderThreadId.make("provider-thread:store");
  yield* sql`INSERT INTO orchestration_v2_projection_run_attempts
    (attempt_id, thread_id, run_id, attempt_ordinal, root_node_id, provider, provider_instance_id, provider_thread_id, status, payload_json)
    VALUES (${attemptId}, ${threadId}, ${runId}, 1, 'node:store', 'codex', ${source}, ${providerThreadId}, 'running', '{}')`;
  const recorder = yield* Evidence.make;
  yield* recorder.record({
    providerTurnId: turnId,
    threadId,
    runId,
    attemptId,
    providerThreadId,
    providerInstanceId: source,
    occurredAt: now,
    status: "failed",
    rawErrorInfo: "usageLimitExceeded",
    capacityExhausted: false,
  });
  return yield* Store.make;
});

it.effect(
  "persists separate decisions while preserving immutable evidence and pruning only decided rows",
  () =>
    Effect.gen(function* () {
      const store = yield* setup;
      const sql = yield* SqlClient.SqlClient;
      const immutable = yield* sql`SELECT * FROM jones_codex_failover_evidence`;
      assert.equal((yield* store.getEvidence(turnId))?.state, "recorded");
      assert.lengthOf(yield* store.listOpenEvidence(), 1);
      assert.lengthOf(yield* store.listOpenEvidenceForThread(ThreadId.make("thread:other")), 0);
      assert.isTrue(yield* store.markEvidence(turnId, { state: "deferred", updatedAt: now }));
      assert.equal((yield* store.getEvidence(turnId))?.state, "deferred");
      yield* store.pruneDecidedEvidence(later);
      assert.lengthOf(yield* store.listOpenEvidence(), 1);
      assert.isTrue(
        yield* store.markEvidence(turnId, {
          state: "decided",
          decision: "exhausted",
          updatedAt: later,
        }),
      );
      const reopened = yield* Store.make;
      assert.equal((yield* reopened.getEvidence(turnId))?.decision, "exhausted");
      assert.deepEqual(yield* sql`SELECT * FROM jones_codex_failover_evidence`, immutable);
      assert.lengthOf(yield* store.listOpenEvidence(), 0);
      assert.isFalse(
        yield* store.markEvidence(ProviderTurnId.make("missing"), {
          state: "deferred",
          updatedAt: now,
        }),
      );
      yield* store.pruneDecidedEvidence(now);
      assert.isNotNull(yield* store.getEvidence(turnId));
      yield* store.pruneDecidedEvidence(later);
      assert.isNull(yield* store.getEvidence(turnId));
      assert.deepEqual(yield* sql`SELECT * FROM jones_codex_failover_decisions`, []);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "step insert is idempotent and all recovery/chain/reset lookups preserve typed selections",
  () =>
    Effect.gen(function* () {
      const store = yield* setup;
      assert.isTrue(yield* store.insertStep(step));
      assert.isFalse(yield* store.insertStep({ ...step, targetInstanceId: source }));
      assert.deepEqual(yield* store.getStepByOrigin(runId), step);
      assert.deepEqual(yield* store.getStepByContinueCommand(step.continueCommandId), step);
      assert.deepEqual(yield* store.getStepByContinueMessage(step.continueMessageId), step);
      assert.deepEqual(yield* store.listChainSteps(runId), [step]);
      assert.deepEqual(yield* store.listSwitchedSteps(), [step]);
      assert.deepEqual(yield* store.listUnexpiredUsageResets(threadId, now), [step]);
      assert.deepEqual(yield* store.listUnexpiredUsageResets(threadId, later), []);
      assert.deepEqual(yield* store.listUnexpiredUsageResets(ThreadId.make("other"), now), []);
      assert.isFalse(yield* store.insertStep({ ...step, threadId: ThreadId.make("other") }));
      assert.isTrue(yield* store.setStepStatus(step.switchCommandId, "continued", later));
      assert.lengthOf(yield* store.listSwitchedSteps(), 0);
      assert.equal((yield* store.getStepByOrigin(runId))?.status, "continued");
      const sql = yield* SqlClient.SqlClient;
      yield* sql`DELETE FROM orchestration_v2_projection_runs WHERE run_id = ${runId}`;
      assert.isFalse(yield* store.setStepStatus(step.switchCommandId, "abandoned", later));
      assert.isFalse(
        yield* store.markEvidence(turnId, { state: "decided", decision: "stale", updatedAt: now }),
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "inheritance and overrides survive reconstruction, reject cross-thread writes, and compose atomically",
  () =>
    Effect.gen(function* () {
      const store = yield* setup;
      const sql = yield* SqlClient.SqlClient;
      const queued = RunId.make("run:queued");
      yield* seedRun(queued, threadId, 2);
      yield* store.insertStep(step);
      assert.isFalse(
        yield* store.insertInheritedQueuedRun({
          runId: queued,
          threadId: ThreadId.make("other"),
          inheritedAt: now,
        }),
      );
      assert.isTrue(
        yield* store.insertInheritedQueuedRun({ runId: queued, threadId, inheritedAt: now }),
      );
      assert.isFalse(
        yield* store.insertInheritedQueuedRun({ runId: queued, threadId, inheritedAt: later }),
      );
      const override = {
        runId: queued,
        threadId,
        selection: step.continuationSelection,
        switchCommandId: step.switchCommandId,
        updatedAt: later,
      };
      assert.isFalse(
        yield* store.setQueuedOverride({ ...override, threadId: ThreadId.make("other") }),
      );
      assert.isFalse(
        yield* store.setQueuedOverride({ ...override, switchCommandId: CommandId.make("missing") }),
      );
      const exit = yield* Effect.exit(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* store.setQueuedOverride(override);
            yield* store.setStepStatus(step.switchCommandId, "continued", later);
            return yield* Effect.fail("synthetic refusal");
          }),
        ),
      );
      assert.isTrue(Exit.isFailure(exit));
      assert.isNull(yield* store.getQueuedOverride(queued));
      assert.equal((yield* store.getStepByOrigin(runId))?.status, "switched");
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* store.setQueuedOverride(override);
          yield* store.setStepStatus(step.switchCommandId, "continued", later);
        }),
      );
      const reopened = yield* Store.make;
      assert.deepEqual(yield* reopened.getQueuedOverride(queued), step.continuationSelection);
      assert.lengthOf(yield* reopened.listInheritedQueuedRuns(threadId), 1);
      assert.lengthOf(yield* reopened.listInheritedQueuedRuns(ThreadId.make("other")), 0);
      assert.lengthOf(yield* reopened.listInheritedQueuedRuns(), 1);
      yield* reopened.clearQueuedOverrides(ThreadId.make("other"), later);
      assert.deepEqual(yield* reopened.getQueuedOverride(queued), step.continuationSelection);
      const refusedOwner = yield* Effect.exit(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* reopened.clearQueuedOverrides(threadId, later);
            return yield* Effect.fail("synthetic owner refusal");
          }),
        ),
      );
      assert.isTrue(Exit.isFailure(refusedOwner));
      assert.deepEqual(yield* reopened.getQueuedOverride(queued), step.continuationSelection);
      yield* reopened.clearQueuedOverrides(threadId, later);
      const afterOwner = yield* Store.make;
      assert.isNull(yield* afterOwner.getQueuedOverride(queued));
      const inherited = yield* afterOwner.listInheritedQueuedRuns(threadId);
      assert.lengthOf(inherited, 1);
      assert.equal(inherited[0]?.inheritedAt, now);
      assert.isNull(inherited[0]?.overrideSwitchCommandId);
      yield* reopened.deleteQueuedRuns([queued]);
      assert.isNull(yield* reopened.getQueuedOverride(queued));
    }).pipe(Effect.provide(memory)),
);

it.effect("missing optional tables fail closed and a later migration is observed", () =>
  Effect.gen(function* () {
    yield* upstream;
    const store = yield* Store.make;
    assert.isNull(yield* store.getEvidence(turnId));
    assert.deepEqual(yield* store.listOpenEvidence(), []);
    assert.isFalse(yield* store.markEvidence(turnId, { state: "deferred", updatedAt: now }));
    assert.isFalse(yield* store.insertStep(step));
    assert.isNull(yield* store.getStepByOrigin(runId));
    assert.isNull(yield* store.getStepByContinueCommand(step.continueCommandId));
    assert.isNull(yield* store.getStepByContinueMessage(step.continueMessageId));
    assert.deepEqual(yield* store.listChainSteps(runId), []);
    assert.deepEqual(yield* store.listSwitchedSteps(), []);
    assert.deepEqual(yield* store.listUnexpiredUsageResets(threadId, now), []);
    assert.isFalse(yield* store.setStepStatus(step.switchCommandId, "continued", now));
    assert.isFalse(yield* store.insertInheritedQueuedRun({ runId, threadId, inheritedAt: now }));
    assert.isNull(yield* store.getQueuedOverride(runId));
    assert.isFalse(
      yield* store.setQueuedOverride({
        runId,
        threadId,
        selection: step.savedSelection,
        switchCommandId: step.switchCommandId,
        updatedAt: now,
      }),
    );
    yield* store.deleteQueuedRuns([runId]);
    yield* store.clearQueuedOverrides(threadId, later);
    yield* store.pruneDecidedEvidence(later);
    yield* migration;
    yield* seedRun();
    assert.isTrue(yield* store.insertInheritedQueuedRun({ runId, threadId, inheritedAt: now }));
  }).pipe(Effect.provide(memory)),
);

it.effect(
  "selection revision matches the actual ThreadSelfConfiguration SQL for every event filter",
  () =>
    Effect.gen(function* () {
      yield* upstream;
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.make;
      // Execute the real sibling query rather than a second hand-maintained SQL copy.
      const fs = yield* FileSystem.FileSystem;
      const sourceText = yield* fs.readFileString(
        NodeURL.fileURLToPath(
          new URL("../threadConfiguration/ThreadSelfConfiguration.ts", import.meta.url),
        ),
      );
      const match = sourceText.match(/const revision = [\s\S]*?`([\s\S]*?)`/);
      assert.isNotNull(match);
      const revisionSql = match![1]!.replace("${threadId}", "?");
      assert.equal(yield* store.selectionRevision(threadId), 0);
      const cases = [
        ["thread.created", threadId, "thread", 2],
        ["thread.model-selection-updated", threadId, "thread", 2],
        ["thread.provider-switched", threadId, "thread", 2],
        ["thread.updated", threadId, "thread", 2],
        ["thread.provider-switched", "other-thread", "thread", 2],
        ["thread.provider-switched", threadId, "project", 2],
        ["thread.provider-switched", threadId, "thread", 1],
      ] as const;
      for (const [index, [eventType, streamId, aggregateKind, version]] of cases.entries()) {
        yield* sql`INSERT INTO orchestration_events
        (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, command_id, payload_json, application_event_version, actor_kind, metadata_json)
        VALUES (${`event:${index}`}, ${aggregateKind}, ${streamId}, ${index + 1}, ${eventType}, ${now}, ${`command:${index}`}, '{}', ${version}, 'system', '{}')`;
        const expected = yield* sql.unsafe<{ revision: number }>(revisionSql, [threadId]);
        assert.equal(yield* store.selectionRevision(threadId), expected[0]!.revision);
        assert.equal(yield* store.selectionRevision(threadId), Math.min(index + 1, 3));
      }
    }).pipe(Effect.provide(Layer.merge(memory, NodeFileSystem.layer))),
);
