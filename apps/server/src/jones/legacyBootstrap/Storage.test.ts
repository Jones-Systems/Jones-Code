import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  OrchestrationV2AppThreadJson,
  OrchestrationV2DomainEventJson,
  OrchestrationV2RunJson,
  ProjectId,
  RunId,
  ThreadId,
  ProviderInstanceId,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import {
  RecordedAppThreadJson,
  RecordedRunJson,
  RecordedStoredLifecycleEvent,
  type RecordedAppThread,
  type RecordedRun,
} from "../../orchestration-v2/RecordedTypes.ts";
import {
  legacyBootstrapCreateCommandId,
  legacyPayloadHash,
} from "../../orchestration-v2/LegacyBootstrap.ts";
import { type CommandReceiptV2 } from "../../orchestration-v2/CommandReceiptStore.ts";
import migration from "../persistence/Migrations/164_JonesLegacyBootstrap.ts";
import * as Storage from "./Storage.ts";
import * as S from "./StorageSchema.ts";
import * as M from "./RecordedMetadata.ts";

const decodeStoredLifecycleEvent = Schema.decodeUnknownSync(RecordedStoredLifecycleEvent);
const encodePublicEvent = Schema.encodeEffect(OrchestrationV2DomainEventJson);
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const encodePublicThreadJson = Schema.encodeEffect(
  Schema.fromJsonString(OrchestrationV2AppThreadJson),
);
const encodePublicRunJson = Schema.encodeEffect(Schema.fromJsonString(OrchestrationV2RunJson));
const decodeMetadataJson = Schema.decodeEffect(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);
const encodeRecordedThreadJson = Schema.encodeEffect(Schema.fromJsonString(RecordedAppThreadJson));
const encodeRecordedRunJson = Schema.encodeEffect(Schema.fromJsonString(RecordedRunJson));

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const time = DateTime.makeUnsafe("2026-11-17T10:00:00.000Z");
const threadId = ThreadId.make("legacy:T");
const runId = RunId.make("legacy:R");
const projectId = ProjectId.make("legacy:P");
const messageId = MessageId.make("legacy:M");
const releaseId = CommandId.make("legacy:C");
const createId = legacyBootstrapCreateCommandId(threadId, releaseId);
const birthId = CommandId.make(`${createId}:initial-message`);
const policy = {
  version: 1 as const,
  threadId,
  projectId,
  messageId,
  createCommandId: createId,
  birthCommandId: birthId,
  releaseCommandId: releaseId,
  ownsNewThread: true,
  payloadHash: "historical-fixture",
};
const thread = Schema.decodeSync(RecordedAppThreadJson)({
  id: threadId,
  projectId,
  createdBy: "user",
  creationSource: "web",
  title: "Legacy",
  providerInstanceId: "codex",
  modelSelection: { instanceId: "codex", model: "test" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  activeProviderThreadId: null,
  lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
  forkedFrom: null,
  createdAt: DateTime.formatIso(time),
  updatedAt: DateTime.formatIso(time),
  archivedAt: null,
  settledOverride: null,
  settledAt: null,
  lastVisitedAt: null,
  deletedAt: null,
});
const run = Schema.decodeSync(RecordedRunJson)({
  id: runId,
  threadId,
  ordinal: 1,
  providerInstanceId: "codex",
  modelSelection: { instanceId: "codex", model: "test" },
  providerThreadId: null,
  userMessageId: messageId,
  rootNodeId: null,
  activeAttemptId: null,
  status: "preparing",
  requestedAt: DateTime.formatIso(time),
  startedAt: null,
  completedAt: null,
  checkpointId: null,
  contextHandoffId: null,
});
const threadIdentity: S.ThreadIdentity = {
  threadId,
  projectId,
  createdAt: DateTime.formatIso(time),
  claimEventId: EventId.make("legacy:claim"),
  claimSequence: 1,
  claimCommandId: createId,
};
const runIdentity: S.RunIdentity = {
  threadId,
  runId,
  userMessageId: messageId,
  requestedAt: DateTime.formatIso(time),
  birthEventId: EventId.make("legacy:birth"),
  birthSequence: 2,
  birthCommandId: birthId,
};
const lifecycle = (sequence: number, commandId: CommandId, event: unknown) =>
  decodeStoredLifecycleEvent({ sequence, commandId, event });
const claim = lifecycle(1, createId, {
  id: threadIdentity.claimEventId,
  threadId,
  type: "thread.created",
  occurredAt: time,
  payload: { ...thread, legacyBootstrapClaim: policy },
});
const birth = lifecycle(2, birthId, {
  id: runIdentity.birthEventId,
  threadId,
  runId,
  type: "run.created",
  occurredAt: time,
  payload: { ...run, legacyBootstrap: { ...policy, runId }, workspaceRunSetupScript: true },
});
const receipt = (
  commandId: CommandId,
  commandType: string,
  sequence: number,
): CommandReceiptV2 => ({
  commandId,
  commandType,
  threadId,
  resultSequence: sequence,
  acceptedAt: time,
  status: "accepted",
  error: null,
});
const setup = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`PRAGMA foreign_keys = ON`;
  // Minimal upstream-owned fixture, matching the production row layout used by this adapter.
  yield* sql`CREATE TABLE orchestration_v2_projection_threads(thread_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL)`;
  yield* sql`CREATE TABLE orchestration_v2_projection_runs(run_id TEXT PRIMARY KEY,thread_id TEXT NOT NULL,ordinal INTEGER,payload_json TEXT NOT NULL)`;
  yield* sql`CREATE TABLE orchestration_events(sequence INTEGER PRIMARY KEY AUTOINCREMENT,event_id TEXT UNIQUE,command_id TEXT,stream_id TEXT,aggregate_kind TEXT,event_type TEXT,occurred_at TEXT,payload_json TEXT,metadata_json TEXT,application_event_version INTEGER)`;
  yield* sql`CREATE INDEX idx_orch_events_command_id ON orchestration_events(command_id)`;
  yield* sql`CREATE TABLE orchestration_command_receipts(command_id TEXT PRIMARY KEY,aggregate_kind TEXT,aggregate_id TEXT,command_type TEXT,accepted_at TEXT,result_sequence INTEGER,status TEXT,error TEXT)`;
  yield* sql`CREATE TABLE projection_projects(project_id TEXT PRIMARY KEY,workspace_root TEXT,deleted_at TEXT)`;
  yield* sql`CREATE TABLE orchestration_v2_effect_outbox(effect_id TEXT PRIMARY KEY,payload_json TEXT)`;
  yield* sql`INSERT INTO projection_projects VALUES (${projectId},'/repo',NULL)`;
  yield* migration;
  const store = Storage.make(sql);
  const append = Effect.fn("LegacyStorageFixture.append")(function* (
    stored: RecordedStoredLifecycleEvent,
  ) {
    const value = yield* encodePublicEvent(stored.event);
    const occurredAt = value.occurredAt;
    yield* sql`INSERT INTO orchestration_events(sequence,event_id,command_id,stream_id,aggregate_kind,event_type,occurred_at,payload_json,metadata_json,application_event_version)
      VALUES (${stored.sequence},${stored.event.id},${stored.commandId},${stored.event.threadId},'thread',${stored.event.type},${occurredAt},${yield* encodeJson(value.payload)},${yield* encodeJson({ runId: stored.event.runId })},2)`;
  });
  const ordinary = Effect.fn("LegacyStorageFixture.receipt")(function* (r: CommandReceiptV2) {
    yield* sql`INSERT INTO orchestration_command_receipts VALUES (${r.commandId},'thread',${r.threadId},${r.commandType},${DateTime.formatIso(r.acceptedAt)},${r.resultSequence},${r.status},${r.error})`;
  });
  const publicThreadJson = yield* encodePublicThreadJson(thread);
  const publicRunJson = yield* encodePublicRunJson(run);
  yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES (${threadId},${publicThreadJson})`;
  yield* sql`INSERT INTO orchestration_v2_projection_runs VALUES (${runId},${threadId},1,${publicRunJson})`;
  yield* append(claim);
  yield* append(birth);
  yield* ordinary(receipt(createId, "thread.create", 1));
  yield* ordinary(receipt(birthId, "message.dispatch", 2));
  const record = Effect.gen(function* () {
    const writer = yield* store.forTransaction;
    yield* writer.recordThread({
      identity: threadIdentity,
      metadata: { legacyBootstrapClaim: policy },
      expectedRevision: null,
    });
    yield* writer.recordEvent(claim);
    yield* writer.recordRun({
      identity: runIdentity,
      metadata: M.runMetadata(birth.event.type === "run.created" ? birth.event.payload : run),
      metadataEvent: birth,
      expectedRevision: null,
    });
    return writer;
  });
  return { sql, store, append, ordinary, record, publicThreadJson, publicRunJson };
});
const binding = (
  commandId: CommandId,
  kind: S.CommandBinding["kind"] = "lifecycle",
  body: unknown = { policy },
): S.CommandBinding => ({
  commandId,
  threadId,
  projectId,
  threadCreatedAt: DateTime.formatIso(time),
  kind,
  bindingJson: M.canonical(body),
});

it.effect(
  "writes only sparse Jones metadata and reopens it without copying upstream payloads",
  () =>
    Effect.gen(function* () {
      const f = yield* setup;
      const upstream =
        yield* f.sql`SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE 'jones_%' ORDER BY name`;
      yield* f.sql.withTransaction(f.record);
      assert.equal(
        (yield* f.store.readThread(threadId))?.thread.legacyBootstrapClaim?.createCommandId,
        createId,
      );
      assert.equal((yield* Storage.make(f.sql).readRun(runId))?.run.workspaceRunSetupScript, true);
      assert.deepStrictEqual(
        yield* f.sql`SELECT payload_json FROM orchestration_v2_projection_threads`,
        [{ payload_json: f.publicThreadJson }],
      );
      assert.deepStrictEqual(
        yield* f.sql`SELECT payload_json FROM orchestration_v2_projection_runs`,
        [{ payload_json: f.publicRunJson }],
      );
      assert.deepStrictEqual(
        yield* f.sql`SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE 'jones_%' ORDER BY name`,
        upstream,
      );
      const rows = yield* f.sql<{
        metadata_json: string;
      }>`SELECT metadata_json FROM jones_legacy_bootstrap_runs`;
      assert.deepStrictEqual(
        Object.keys(yield* decodeMetadataJson(rows[0]!.metadata_json)).sort(),
        ["legacyBootstrap", "workspaceRunSetupScript"],
      );
      for (const table of [
        "orchestration_v2_projection_threads",
        "orchestration_v2_projection_runs",
        "orchestration_events",
        "orchestration_v2_effect_outbox",
      ]) {
        const payloads = yield* f.sql.unsafe<{ payload_json: string }>(
          `SELECT payload_json FROM ${table}`,
        );
        for (const row of payloads)
          assert.notMatch(
            row.payload_json,
            /legacyBootstrap|legacyPreparation|workspaceRunSetupScript/,
          );
      }
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "shares the real caller transaction, rolls back every partial private/public write, and rejects escaped/forked writers",
  () =>
    Effect.gen(function* () {
      const f = yield* setup;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(f.store.forTransaction)));
      const escaped = yield* f.sql.withTransaction(f.store.forTransaction);
      assert.isTrue(Exit.isFailure(yield* Effect.exit(escaped.recordEvent(claim))));
      yield* f.sql.withTransaction(
        Effect.gen(function* () {
          const writer = yield* f.store.forTransaction;
          const exit = yield* writer
            .recordEvent(claim)
            .pipe(Effect.forkChild, Effect.flatMap(Fiber.await));
          assert.isTrue(Exit.isFailure(exit));
        }),
      );
      for (const cut of ["claim", "event", "run", "receipt", "outbox"]) {
        const exit = yield* f.sql
          .withTransaction(
            Effect.gen(function* () {
              const w = yield* f.store.forTransaction;
              yield* w.recordThread({
                identity: threadIdentity,
                metadata: { legacyBootstrapClaim: policy },
                expectedRevision: null,
              });
              if (cut === "claim") return yield* Effect.fail("injected");
              yield* w.recordEvent(claim);
              if (cut === "event") return yield* Effect.fail("injected");
              yield* w.recordRun({
                identity: runIdentity,
                metadata: { legacyBootstrap: { ...policy, runId }, workspaceRunSetupScript: true },
                metadataEvent: birth,
                expectedRevision: null,
              });
              if (cut === "run") return yield* Effect.fail("injected");
              yield* w.recordLifecycle(binding(birthId), receipt(birthId, "message.dispatch", 2), [
                birth,
              ]);
              if (cut === "receipt") return yield* Effect.fail("injected");
              yield* f.sql`INSERT INTO orchestration_v2_effect_outbox VALUES ('ordinary','{}')`;
              return yield* Effect.fail("injected");
            }),
          )
          .pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(exit));
        for (const table of ["threads", "runs", "event_metadata", "commands", "terminal_cleanup"])
          assert.deepStrictEqual(
            yield* f.sql.unsafe(`SELECT * FROM jones_legacy_bootstrap_${table}`),
            [],
          );
        assert.deepStrictEqual(yield* f.sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
      }
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "orphan metadata never resurrects rows and never blocks FK-enabled upstream drop/rebuild",
  () =>
    Effect.gen(function* () {
      const f = yield* setup;
      yield* f.sql.withTransaction(f.record);
      const before = yield* f.sql`SELECT * FROM jones_legacy_bootstrap_runs`;
      yield* f.sql`DELETE FROM orchestration_v2_projection_threads`;
      assert.isUndefined(yield* f.store.readThread(threadId));
      assert.isUndefined(yield* f.store.readRun(runId));
      assert.deepStrictEqual(yield* f.store.readLifecycleByCommandId(birthId), []);
      yield* f.sql`DROP TABLE orchestration_v2_projection_threads`;
      yield* f.sql`CREATE TABLE orchestration_v2_projection_threads(thread_id TEXT PRIMARY KEY,payload_json TEXT)`;
      yield* f.sql`INSERT INTO orchestration_v2_projection_threads VALUES (${threadId},${f.publicThreadJson})`;
      assert.equal((yield* f.store.readRun(runId))?.run.workspaceRunSetupScript, true);
      assert.deepStrictEqual(yield* f.sql`SELECT * FROM jones_legacy_bootstrap_runs`, before);
      yield* f.sql`DELETE FROM orchestration_v2_projection_runs`;
      assert.isUndefined(yield* f.store.readRun(runId));
      assert.deepStrictEqual(yield* f.store.readLifecycleByCommandId(birthId), []);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "refuses altered incarnation, stale revisions, immutable policy and actual row identities",
  () =>
    Effect.gen(function* () {
      const f = yield* setup;
      yield* f.sql.withTransaction(f.record);
      for (const input of [
        {
          identity: { ...threadIdentity, claimSequence: 2 },
          metadata: { legacyBootstrapClaim: policy },
          expectedRevision: 0,
        },
        {
          identity: threadIdentity,
          metadata: { legacyBootstrapClaim: { ...policy, ownsNewThread: false } },
          expectedRevision: 0,
        },
        {
          identity: threadIdentity,
          metadata: { legacyBootstrapClaim: policy },
          expectedRevision: null,
        },
      ])
        assert.isTrue(
          Exit.isFailure(
            yield* f.sql
              .withTransaction(
                Effect.gen(function* () {
                  yield* (yield* f.store.forTransaction).recordThread(input);
                }),
              )
              .pipe(Effect.exit),
          ),
        );
      const changed = yield* encodePublicThreadJson({
        ...thread,
        createdAt: DateTime.makeUnsafe("2026-11-18T10:00:00.000Z"),
      });
      yield* f.sql`UPDATE orchestration_v2_projection_threads SET payload_json = ${changed}`;
      assert.isTrue(Exit.isFailure(yield* f.store.readThread(threadId).pipe(Effect.exit)));
      yield* f.sql`UPDATE orchestration_v2_projection_threads SET payload_json = ${f.publicThreadJson}`;
      yield* f.sql`UPDATE orchestration_v2_projection_runs SET thread_id = 'another'`;
      assert.isTrue(Exit.isFailure(yield* f.store.readRun(runId).pipe(Effect.exit)));
    }).pipe(Effect.provide(memory)),
);

it.effect("historical event snapshots stay exact after mutable current metadata advances", () =>
  Effect.gen(function* () {
    const f = yield* setup;
    yield* f.sql.withTransaction(f.record);
    const progressId = CommandId.make("legacy:progress");
    const progress = lifecycle(3, progressId, {
      ...birth.event,
      id: EventId.make("legacy:progress:event"),
      type: "run.updated",
      payload: {
        ...run,
        legacyBootstrap: { ...policy, runId },
        workspaceRunSetupScript: true,
        legacyPreparationFailureKnown: true,
      },
    });
    yield* f.sql.withTransaction(
      Effect.gen(function* () {
        yield* f.append(progress);
        yield* f.ordinary(receipt(progressId, "prepared-run.progress", 3));
        yield* (yield* f.store.forTransaction).recordRun({
          identity: runIdentity,
          metadata: {
            legacyBootstrap: { ...policy, runId },
            workspaceRunSetupScript: true,
            legacyPreparationFailureKnown: true,
          },
          metadataEvent: progress,
          expectedRevision: 0,
        });
      }),
    );
    assert.equal((yield* f.store.readRun(runId))?.run.legacyPreparationFailureKnown, true);
    const old = yield* f.store.readLifecycleByCommandId(birthId);
    assert.equal(old.length, 1);
    assert.equal(
      old[0]?.event.type === "run.created"
        ? old[0].event.payload.legacyPreparationFailureKnown
        : "wrong",
      undefined,
    );
    const later = yield* f.store.readLifecycleByCommandId(progressId);
    assert.equal(
      later[0]?.event.type === "run.updated"
        ? later[0].event.payload.legacyPreparationFailureKnown
        : false,
      true,
    );
    yield* f.sql`UPDATE jones_legacy_bootstrap_event_metadata SET sequence = 999 WHERE event_id = ${birth.event.id}`;
    assert.isTrue(
      Exit.isFailure(yield* f.store.readLifecycleByCommandId(birthId).pipe(Effect.exit)),
    );
  }).pipe(Effect.provide(memory)),
);

it.effect("lifecycle reservation replay compares canonical binding and actual receipt row", () =>
  Effect.gen(function* () {
    const f = yield* setup;
    yield* f.sql.withTransaction(
      Effect.gen(function* () {
        const w = yield* f.store.forTransaction;
        assert.isTrue(
          yield* w.recordLifecycle(binding(birthId), receipt(birthId, "message.dispatch", 2), [
            birth,
          ]),
        );
        assert.isFalse(
          yield* w.recordLifecycle(binding(birthId), receipt(birthId, "message.dispatch", 2), [
            birth,
          ]),
        );
      }),
    );
    assert.isTrue(Exit.isFailure(yield* f.store.assertUnreserved(birthId).pipe(Effect.exit)));
    assert.isTrue(
      Exit.isFailure(
        yield* f.store
          .assertBinding({
            ...binding(birthId),
            bindingJson: M.canonical({ policy: { ...policy, payloadHash: "changed" } }),
          })
          .pipe(Effect.exit),
      ),
    );
    yield* f.sql`UPDATE orchestration_command_receipts SET result_sequence = 400 WHERE command_id = ${birthId}`;
    assert.isTrue(Exit.isFailure(yield* f.store.readLegacyReceipt(birthId).pipe(Effect.exit)));
    yield* f.sql`INSERT INTO orchestration_command_receipts VALUES ('foreign','project',${projectId},'project.create',${DateTime.formatIso(time)},0,'accepted',NULL)`;
    assert.isTrue(
      Exit.isFailure(yield* f.store.readLegacyReceipt(CommandId.make("foreign")).pipe(Effect.exit)),
    );
  }).pipe(Effect.provide(memory)),
);

const preflight = (releaseCommandId = releaseId) => {
  const createCommandId = legacyBootstrapCreateCommandId(threadId, releaseCommandId);
  const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "test" };
  const payload = {
    type: "thread.turn.start",
    commandId: releaseCommandId,
    threadId,
    message: { messageId, role: "user", text: "Prompt", attachments: [] },
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    createdAt: DateTime.formatIso(time),
    bootstrap: {
      createThread: {
        projectId,
        title: "Preflight",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdAt: DateTime.formatIso(time),
      },
      prepareWorktree: {
        projectCwd: "/repo",
        baseBranch: "main",
        startFromOrigin: true,
        requireWorktree: true,
      },
      runSetupScript: false,
    },
  };
  const canonicalPayload = M.canonical(payload);
  const binding: S.PreflightBinding = {
    policy: {
      ...policy,
      createCommandId,
      birthCommandId: CommandId.make(`${createCommandId}:initial-message`),
      releaseCommandId,
      payloadHash: legacyPayloadHash(canonicalPayload),
    },
    canonicalPayload,
    fetch: {
      cwd: "/repo",
      baseRef: "main",
      startFromOrigin: true,
      requireWorktree: true,
      remote: "origin",
    },
  };
  const commandId = CommandId.make(`${createCommandId}:preflight-intent`);
  return {
    commandId,
    event: {
      id: EventId.make(`${commandId}:retained-arbitrary-id`),
      threadId,
      occurredAt: time,
      type: "legacy-bootstrap.preflight-intent" as const,
      payload: binding,
    },
  };
};

it.effect(
  "private preflight ordinals never advance public sequence; exact replay has one winner and unresolved/unknown intents fence replacement",
  () =>
    Effect.gen(function* () {
      const f = yield* setup;
      yield* f.sql`DELETE FROM orchestration_events`;
      yield* f.sql`DELETE FROM orchestration_command_receipts`;
      yield* f.sql`DELETE FROM orchestration_v2_projection_runs`;
      yield* f.sql`DELETE FROM orchestration_v2_projection_threads`;
      const input = preflight();
      const commit = (entry: typeof input) =>
        f.sql.withTransaction(
          Effect.gen(function* () {
            return yield* (yield* f.store.forTransaction).commitPreflight(entry);
          }),
        );
      const results = yield* Effect.all([commit(input), commit(input)], { concurrency: 2 });
      assert.equal(results.filter((result) => result.committed).length, 1);
      assert.equal(results[0]?.receipt.resultSequence, results[1]?.receipt.resultSequence);
      assert.equal(results[0]?.receipt.sequenceDomain, "jones-legacy-ordinal");
      assert.equal(results[0]?.receipt.resultSequence, 1);
      assert.deepStrictEqual(yield* f.sql`SELECT * FROM orchestration_events`, []);
      assert.deepStrictEqual(yield* f.sql`SELECT * FROM orchestration_command_receipts`, []);
      assert.deepStrictEqual(yield* f.sql`SELECT * FROM orchestration_v2_projection_threads`, []);
      const changed = {
        ...input,
        event: {
          ...input.event,
          payload: {
            ...input.event.payload,
            fetch: { ...input.event.payload.fetch, baseRef: "other" },
          },
        },
      };
      assert.isTrue(Exit.isFailure(yield* commit(changed).pipe(Effect.exit)));
      assert.isTrue(
        Exit.isFailure(yield* commit(preflight(CommandId.make("replacement:C"))).pipe(Effect.exit)),
      );
      const outcome = {
        commandId: CommandId.make(
          `${input.event.payload.policy.createCommandId}:preflight-outcome`,
        ),
        event: {
          id: EventId.make("outcome:arbitrary-historical-compatible-id"),
          threadId,
          occurredAt: time,
          type: "legacy-bootstrap.preflight-outcome" as const,
          payload: {
            binding: input.event.payload,
            intentCommandId: input.commandId,
            intentSequence: 1,
            status: "unknown" as const,
          },
        },
      };
      const commitOutcome = (entry = outcome) =>
        f.sql.withTransaction(
          Effect.gen(function* () {
            return yield* (yield* f.store.forTransaction).commitPreflight(entry);
          }),
        );
      assert.isTrue(
        Exit.isFailure(
          yield* commitOutcome({
            ...outcome,
            event: { ...outcome.event, payload: { ...outcome.event.payload, intentSequence: 99 } },
          }).pipe(Effect.exit),
        ),
      );
      const accepted = yield* commitOutcome();
      assert.equal(accepted.receipt.resultSequence, 2);
      assert.equal(
        (yield* Storage.make(f.sql).readLegacyPreflight(outcome.commandId))?.receipt.sequenceDomain,
        "jones-legacy-ordinal",
      );
      assert.isTrue(
        Exit.isFailure(yield* commit(preflight(CommandId.make("replacement:C"))).pipe(Effect.exit)),
      );
      yield* f.sql`INSERT INTO orchestration_command_receipts VALUES ('project-collision','project',${projectId},'project.create',${DateTime.formatIso(time)},0,'accepted',NULL)`;
      assert.isTrue(
        Exit.isFailure(
          yield* f.store.readLegacyPreflight(CommandId.make("project-collision")).pipe(Effect.exit),
        ),
      );
    }).pipe(Effect.provide(memory)),
);

it.effect.each(["interrupted", "closed"] as const)(
  "private D terminal CAS retains one obligation and %s outcome with rollback",
  (terminalOutcome) =>
    Effect.gen(function* () {
      const f = yield* setup;
      yield* f.sql.withTransaction(f.record);
      const commandId = CommandId.make("legacy:D");
      const control: S.TerminalCleanup["control"] = {
        version: 1,
        policy: { ...policy, runId },
        runId,
        threadId,
        claimEventId: claim.event.id,
        claimSequence: 1,
        claimReceiptSequence: 1,
        birthEventId: birth.event.id,
        birthSequence: 2,
        birthReceiptSequence: 2,
        preparationGeneration: "prepared-generation",
        terminalId: "terminal-original",
        generation: "terminal-generation",
      };
      const command = {
        type: "legacy-bootstrap.guard-rejection-delete",
        commandId,
        threadId,
        runId,
        legacyBootstrap: { ...policy, runId },
        legacyOwnedControl: control,
      };
      const descriptor = binding(commandId, "delete", command);
      const tombstone = lifecycle(3, commandId, {
        id: EventId.make("legacy:deleted"),
        threadId,
        type: "thread.deleted",
        occurredAt: time,
        payload: { ...thread, deletedAt: time },
      });
      const privateReceipt = receipt(commandId, command.type, 3);
      const commit = Effect.gen(function* () {
        const w = yield* f.store.forTransaction;
        yield* f.append(tombstone);
        yield* w.recordDelete(descriptor, privateReceipt, [tombstone]);
        yield* w.createTerminalCleanup({
          effectId: "legacy:terminal-cleanup",
          commandId,
          threadId,
          control,
        });
      });
      assert.isTrue(
        Exit.isFailure(
          yield* f.sql
            .withTransaction(commit.pipe(Effect.andThen(Effect.fail("after cleanup insert"))))
            .pipe(Effect.exit),
        ),
      );
      assert.deepStrictEqual(
        yield* f.sql`SELECT * FROM jones_legacy_bootstrap_terminal_cleanup`,
        [],
      );
      assert.deepStrictEqual(yield* f.sql`SELECT * FROM jones_legacy_bootstrap_commands`, []);
      assert.deepStrictEqual(
        yield* f.sql`SELECT * FROM orchestration_events WHERE command_id = ${commandId}`,
        [],
      );
      yield* f.sql.withTransaction(commit);
      assert.equal((yield* f.store.readLegacyReceipt(commandId))?.domain, "legacy-delete");
      assert.deepStrictEqual(
        yield* f.sql`SELECT * FROM orchestration_command_receipts WHERE command_id = ${commandId}`,
        [],
      );
      yield* f.sql.withTransaction(
        Effect.gen(function* () {
          const w = yield* f.store.forTransaction;
          assert.isFalse(yield* w.recordDelete(descriptor, privateReceipt, [tombstone]));
          const same = yield* w.createTerminalCleanup({
            effectId: "legacy:terminal-cleanup",
            commandId,
            threadId,
            control,
          });
          assert.equal(same.revision, 0);
        }),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* f.sql
            .withTransaction(
              Effect.gen(function* () {
                yield* (yield* f.store.forTransaction).createTerminalCleanup({
                  effectId: "legacy:terminal-cleanup",
                  commandId,
                  threadId,
                  control: { ...control, generation: "replacement" },
                });
              }),
            )
            .pipe(Effect.exit),
        ),
      );
      const started = yield* f.sql.withTransaction(
        Effect.gen(function* () {
          return yield* (yield* f.store.forTransaction).claimTerminalCleanup({
            effectId: "legacy:terminal-cleanup",
            expectedRevision: 0,
            attemptId: "owner-attempt",
          });
        }),
      );
      assert.equal(started?.state, "started");
      yield* f.sql.withTransaction(
        Effect.gen(function* () {
          const w = yield* f.store.forTransaction;
          assert.isUndefined(
            yield* w.claimTerminalCleanup({
              effectId: "legacy:terminal-cleanup",
              expectedRevision: 1,
              attemptId: "replacement-owner",
            }),
          );
          assert.isUndefined(
            yield* w.finishTerminalCleanup({
              effectId: "legacy:terminal-cleanup",
              expectedRevision: 1,
              attemptId: "wrong-attempt",
              outcome: "closed",
            }),
          );
          assert.equal(
            (yield* w.finishTerminalCleanup({
              effectId: "legacy:terminal-cleanup",
              expectedRevision: 1,
              attemptId: "owner-attempt",
              outcome: terminalOutcome,
            }))?.state,
            terminalOutcome === "closed" ? "succeeded" : "unknown",
          );
          assert.isUndefined(
            yield* w.claimTerminalCleanup({
              effectId: "legacy:terminal-cleanup",
              expectedRevision: 2,
              attemptId: "replacement-owner",
            }),
          );
        }),
      );
      assert.equal(
        (yield* Storage.make(f.sql).listPendingTerminalCleanup(10))[0]?.state,
        terminalOutcome === "closed" ? undefined : "unknown",
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "preserves exact inline history before public replacement and fails only affected writes when schema is missing",
  () =>
    Effect.gen(function* () {
      const f = yield* setup;
      const historicalThread: RecordedAppThread = { ...thread, legacyBootstrapClaim: policy };
      const historicalRun: RecordedRun = {
        ...run,
        legacyBootstrap: { ...policy, runId },
        workspaceRunSetupScript: true,
      };
      yield* f.sql`UPDATE orchestration_v2_projection_threads SET payload_json = ${yield* encodeRecordedThreadJson(historicalThread)}`;
      yield* f.sql`UPDATE orchestration_v2_projection_runs SET payload_json = ${yield* encodeRecordedRunJson(historicalRun)}`;
      // An explicit historical fixture supplies inline private payloads; fresh append never does this.
      yield* f.sql`UPDATE orchestration_events SET payload_json = ${yield* encodeRecordedRunJson(historicalRun)} WHERE event_id = ${birth.event.id}`;
      yield* f.sql.withTransaction(
        Effect.gen(function* () {
          yield* f.store.preserveInlineThread(threadId);
          yield* f.store.preserveInlineRun(runId);
          yield* f.sql`UPDATE orchestration_v2_projection_threads SET payload_json = ${f.publicThreadJson}`;
          yield* f.sql`UPDATE orchestration_v2_projection_runs SET payload_json = ${f.publicRunJson}`;
        }),
      );
      assert.deepStrictEqual(
        (yield* f.store.readRun(runId))?.run.legacyBootstrap,
        historicalRun.legacyBootstrap,
      );
      yield* f.sql`DROP TABLE jones_legacy_bootstrap_terminal_cleanup`;
      const fresh = Storage.make(f.sql);
      yield* fresh.preserveInlineThread(threadId);
      assert.isTrue(
        Exit.isFailure(yield* f.sql.withTransaction(fresh.forTransaction).pipe(Effect.exit)),
      );
      yield* f.sql`UPDATE orchestration_v2_projection_threads SET payload_json = ${yield* encodeRecordedThreadJson(historicalThread)}`;
      assert.isTrue(
        Exit.isFailure(
          yield* f.sql.withTransaction(fresh.preserveInlineThread(threadId)).pipe(Effect.exit),
        ),
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "historical preflight keeps its upstream position/domain and arbitrary accepted event ID",
  () =>
    Effect.gen(function* () {
      const f = yield* setup;
      yield* f.sql`DELETE FROM orchestration_events`;
      yield* f.sql`DELETE FROM orchestration_command_receipts`;
      yield* f.sql`DELETE FROM orchestration_v2_projection_runs`;
      yield* f.sql`DELETE FROM orchestration_v2_projection_threads`;
      const input = preflight();
      yield* f.sql`INSERT INTO orchestration_events VALUES (42,${input.event.id},${input.commandId},${threadId},'thread',${input.event.type},${DateTime.formatIso(time)},${M.canonical(input.event.payload)},'{}',2)`;
      yield* f.ordinary(receipt(input.commandId, input.event.type, 42));
      const read = yield* f.store.readLegacyPreflight(input.commandId);
      assert.equal(read?.event.id, input.event.id);
      assert.equal(read?.receipt.resultSequence, 42);
      assert.equal(read?.receipt.sequenceDomain, "historical-upstream");
      assert.equal((yield* f.store.readLegacyReceipt(input.commandId))?.domain, "legacy-preflight");
      const replay = yield* f.sql.withTransaction(
        Effect.gen(function* () {
          return yield* (yield* f.store.forTransaction).commitPreflight(input);
        }),
      );
      assert.isFalse(replay.committed);
      assert.equal(replay.receipt.sequenceDomain, "historical-upstream");
      assert.deepStrictEqual(yield* f.sql`SELECT * FROM jones_legacy_bootstrap_commands`, []);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "no-control D creates no terminal obligation and rejects attaching an owned control later",
  () =>
    Effect.gen(function* () {
      const f = yield* setup;
      yield* f.sql.withTransaction(f.record);
      const commandId = CommandId.make("legacy:no-control:D");
      const fullPolicy = { ...policy, runId };
      const shared = {
        version: 1 as const,
        policy: fullPolicy,
        threadId,
        runId,
        claimEventId: claim.event.id,
        claimSequence: 1,
        claimReceiptSequence: 1,
        birthEventId: birth.event.id,
        birthSequence: 2,
        birthReceiptSequence: 2,
        preparationGeneration: "generation",
      };
      const command = {
        type: "legacy-bootstrap.failure-delete",
        commandId,
        threadId,
        runId,
        legacyBootstrap: fullPolicy,
        legacyNoControl: {
          ...shared,
          type: "no_control",
          workspacePath: "/repo/worktree",
          projectWorkspaceRoot: "/repo",
        },
      };
      const tombstone = lifecycle(3, commandId, {
        id: EventId.make("no-control:tombstone"),
        threadId,
        type: "thread.deleted",
        occurredAt: time,
        payload: { ...thread, deletedAt: time },
      });
      yield* f.sql.withTransaction(
        Effect.gen(function* () {
          yield* f.append(tombstone);
          yield* (yield* f.store.forTransaction).recordDelete(
            binding(commandId, "delete", command),
            receipt(commandId, command.type, 3),
            [tombstone],
          );
        }),
      );
      assert.deepStrictEqual(yield* f.store.listPendingTerminalCleanup(10), []);
      assert.isTrue(
        Exit.isFailure(
          yield* f.sql
            .withTransaction(
              Effect.gen(function* () {
                yield* (yield* f.store.forTransaction).createTerminalCleanup({
                  effectId: "forged-obligation",
                  commandId,
                  threadId,
                  control: { ...shared, terminalId: "T", generation: "replaced" },
                });
              }),
            )
            .pipe(Effect.exit),
        ),
      );
      assert.deepStrictEqual(
        yield* f.sql`SELECT * FROM jones_legacy_bootstrap_terminal_cleanup`,
        [],
      );
    }).pipe(Effect.provide(memory)),
);
