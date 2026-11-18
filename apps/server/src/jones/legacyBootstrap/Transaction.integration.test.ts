import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "../../persistence/Sqlite.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Projects from "../../orchestration-v2/ProjectStore.ts";
import {
  RecordedAppThreadJson,
  RecordedRunJson,
  type RecordedLifecycleEvent,
  type RecordedServerCommand,
} from "../../orchestration-v2/RecordedTypes.ts";
import {
  canonicalLegacyPayload,
  legacyPayloadHash,
  legacyBootstrapCreateCommandId,
} from "../../orchestration-v2/LegacyBootstrap.ts";
import migration from "../persistence/Migrations/163_JonesLegacyBootstrap.ts";
import * as Storage from "./Storage.ts";
import * as Decision from "./Decision.ts";

const decodeRun = Schema.decodeEffect(RecordedRunJson);

const layer = EventSink.layer.pipe(
  Layer.provideMerge(Layer.mergeAll(EventStore.layer, Projections.layer, Receipts.layer)),
  Layer.provideMerge(Sqlite.layerMemory),
);
const now = DateTime.makeUnsafe("2026-11-17T10:00:00.000Z");
const threadId = ThreadId.make("transaction-thread");
const projectId = ProjectId.make("transaction-project");
const runId = RunId.make("transaction-run");
const messageId = MessageId.make("transaction-message");
const releaseId = CommandId.make("transaction-C");
const createId = legacyBootstrapCreateCommandId(threadId, releaseId);
const birthId = CommandId.make(`${createId}:initial-message`);
const providerInstanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId: providerInstanceId, model: "synthetic" };
const policy = {
  version: 1 as const,
  threadId,
  projectId,
  messageId,
  createCommandId: createId,
  birthCommandId: birthId,
  releaseCommandId: releaseId,
  ownsNewThread: true,
  payloadHash: "synthetic-input",
};
const publicThread = Schema.decodeSync(RecordedAppThreadJson)({
  id: threadId,
  projectId,
  title: "Synthetic",
  createdBy: "user",
  creationSource: "web",
  providerInstanceId,
  modelSelection,
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  activeProviderThreadId: null,
  lineage: { rootThreadId: threadId, parentThreadId: null, relationshipToParent: null },
  forkedFrom: null,
  createdAt: DateTime.formatIso(now),
  updatedAt: DateTime.formatIso(now),
  archivedAt: null,
  deletedAt: null,
  settledAt: null,
  settledOverride: null,
  lastVisitedAt: null,
  snoozedAt: null,
  snoozedUntil: null,
});
const claimEvent: RecordedLifecycleEvent = {
  id: EventId.make("claim-event"),
  type: "thread.created",
  threadId,
  occurredAt: now,
  payload: { ...publicThread, legacyBootstrapClaim: policy },
};
const claimCommand: RecordedServerCommand = {
  type: "thread.create",
  commandId: createId,
  threadId,
  projectId,
  title: "Synthetic",
  createdBy: "user",
  creationSource: "web",
  modelSelection,
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  legacyBootstrap: policy,
};
const fixture = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const storage = Storage.make(sql);
  if (!(yield* storage.available)) yield* migration;
  const projects = yield* Projects.make;
  const at = DateTime.formatIso(now);
  yield* sql.withTransaction(
    projects.apply({
      sequence: 0,
      eventId: EventId.make("fixture-project"),
      aggregateKind: "project",
      aggregateId: projectId,
      occurredAt: at,
      commandId: null,
      causationEventId: null,
      correlationId: null,
      metadata: {},
      type: "project.created",
      payload: {
        projectId,
        title: "Synthetic",
        workspaceRoot: "/synthetic",
        defaultModelSelection: modelSelection,
        scripts: [],
        createdAt: at,
        updatedAt: at,
      },
    }),
  );
  const sink = yield* EventSink.EventSinkV2;
  const context = yield* Decision.capture(storage, claimCommand, [claimEvent]);
  assert.isDefined(context);
  const publicInput = {
    commandId: createId,
    commandType: "thread.create",
    threadId,
    acceptedAt: now,
    events: [claimEvent],
    effects: [],
  };
  const input = { ...publicInput, legacyBootstrap: context! };
  return { sql, storage, sink, input, publicInput };
});

it.effect(
  "actual EventSink claim and birth store sparse metadata, retain exact event anchors, and replay once",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const first = yield* f.sink.commitCommand(f.input);
      const replay = yield* f.sink.commitCommand(f.input);
      assert.isTrue(first.committed);
      assert.isFalse(replay.committed);
      assert.equal(replay.receipt.resultSequence, first.receipt.resultSequence);
      assert.equal(
        (yield* f.storage.readThread(threadId))?.thread.legacyBootstrapClaim?.createCommandId,
        createId,
      );
      const run = yield* decodeRun({
        id: runId,
        threadId,
        ordinal: 1,
        providerInstanceId,
        modelSelection,
        providerThreadId: null,
        userMessageId: messageId,
        rootNodeId: null,
        activeAttemptId: null,
        status: "preparing",
        requestedAt: DateTime.formatIso(now),
        startedAt: null,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
        legacyBootstrap: { ...policy, runId },
        workspaceRunSetupScript: false,
      });
      const event: RecordedLifecycleEvent = {
        id: EventId.make("birth-event"),
        type: "run.created",
        threadId,
        runId,
        occurredAt: now,
        payload: run,
      };
      const command: Extract<RecordedServerCommand, { readonly type: "message.dispatch" }> = {
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: birthId,
        threadId,
        messageId,
        text: "Synthetic",
        attachments: [],
        dispatchMode: { type: "defer_start" },
        legacyBootstrap: policy,
        workspaceRunSetupScript: false,
      };
      const context = yield* Decision.capture(f.storage, command, [event]);
      assert.isDefined(context);
      const born = yield* f.sink.commitCommand({
        commandId: birthId,
        commandType: command.type,
        threadId,
        acceptedAt: now,
        events: [event],
        effects: [],
        legacyBootstrap: context!,
      });
      assert.isTrue(born.committed);
      assert.equal((yield* f.storage.readRun(runId))?.run.workspaceRunSetupScript, false);
      const exact = yield* f.storage.readLifecycleByCommandId(birthId);
      assert.equal(exact[0]?.event.type, "run.created");
      assert.equal(exact[0]?.sequence, born.receipt.resultSequence);
      for (const table of [
        "orchestration_events",
        "orchestration_v2_projection_threads",
        "orchestration_v2_projection_runs",
      ]) {
        const rows = yield* f.sql.unsafe<{ payload_json: string }>(
          `SELECT payload_json FROM ${table}`,
        );
        for (const row of rows)
          assert.notMatch(
            row.payload_json,
            /legacyBootstrap|legacyPreparation|workspaceRunSetupScript/,
          );
      }
      const generic = Array.from(
        yield* (yield* EventStore.EventStoreV2)
          .readByCommandId({ commandId: birthId })
          .pipe(Stream.runCollect),
      );
      const publicBirth = generic[0];
      assert.isTrue(publicBirth?.event.type === "run.created");
      if (publicBirth?.event.type === "run.created")
        assert.isUndefined(publicBirth.event.payload.legacyBootstrap);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  "jones_legacy_bootstrap_event_metadata",
  "jones_legacy_bootstrap_threads",
  "orchestration_v2_effect_outbox",
] as const)(
  "actual SQL failure at %s rolls back public events, projections, receipt and Jones rows",
  (failurePoint) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.sql.unsafe(
        `CREATE TEMP TRIGGER fail_legacy BEFORE INSERT ON ${failurePoint} BEGIN SELECT RAISE(ABORT, 'synthetic rollback'); END`,
      );
      const result = yield* f.sink
        .commitCommand({
          ...f.input,
          effects: [
            {
              id: "probe-cleanup",
              commandId: createId,
              threadId,
              request: { type: "preview.cleanup" },
            },
          ],
        })
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(result));
      for (const table of [
        "orchestration_events",
        "orchestration_v2_projection_threads",
        "orchestration_command_receipts",
        "orchestration_v2_effect_outbox",
        "jones_legacy_bootstrap_threads",
        "jones_legacy_bootstrap_event_metadata",
        "jones_legacy_bootstrap_commands",
      ]) {
        const rows = yield* f.sql.unsafe(`SELECT * FROM ${table}`);
        assert.equal(rows.length, 0, table);
      }
      yield* f.sql`DROP TRIGGER fail_legacy`;
      assert.isTrue((yield* f.sink.commitCommand(f.input)).committed);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "a changed immutable binding and ordinary accepted/rejected thread/project collisions cannot steal a legacy command ID",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.sink.commitCommand(f.input);
      const changed = yield* Decision.capture(
        f.storage,
        { ...claimCommand, legacyBootstrap: { ...policy, payloadHash: "changed" } },
        [claimEvent],
      );
      assert.isDefined(changed);
      assert.isTrue(
        Exit.isFailure(
          yield* f.sink.commitCommand({ ...f.input, legacyBootstrap: changed! }).pipe(Effect.exit),
        ),
      );
      assert.isTrue(Exit.isFailure(yield* f.sink.commitCommand(f.publicInput).pipe(Effect.exit)));
      assert.isTrue(
        Exit.isFailure(
          yield* f.sink
            .commitRejectedCommand({
              commandId: createId,
              threadId,
              commandType: "thread.delete",
              rejectedAt: now,
              error: "collision",
            })
            .pipe(Effect.exit),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* f.sink
            .commitRejectedProjectCommand({
              commandId: createId,
              projectId,
              commandType: "project.delete",
              rejectedAt: now,
              error: "collision",
            })
            .pipe(Effect.exit),
        ),
      );
      const receipt = yield* (yield* Receipts.CommandReceiptStoreV2).getByCommandId(createId);
      assert.isTrue(Option.isSome(receipt));
      if (Option.isSome(receipt)) assert.equal(receipt.value.commandType, "thread.create");
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "ordinary updates preserve sparse claim metadata without attaching current metadata to an old event",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.sink.commitCommand(f.input);
      yield* f.sink.write({
        commandId: CommandId.make("ordinary-update"),
        events: [
          {
            id: EventId.make("ordinary-update-event"),
            type: "thread.metadata-updated",
            threadId,
            occurredAt: now,
            payload: { ...publicThread, title: "Updated" },
          },
        ],
      });
      assert.equal(
        (yield* f.storage.readThread(threadId))?.thread.legacyBootstrapClaim?.createCommandId,
        createId,
      );
      const old = yield* f.storage.readLifecycleByCommandId(createId);
      const ordinary = yield* f.storage.readLifecycleByCommandId(CommandId.make("ordinary-update"));
      assert.equal(old[0]?.event.type, "thread.created");
      if (ordinary[0]?.event.type === "thread.metadata-updated")
        assert.isUndefined(ordinary[0].event.payload.legacyBootstrapClaim);
      const row = yield* f.sql<{
        payload_json: string;
      }>`SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id = ${threadId}`;
      assert.notMatch(row[0]!.payload_json, /legacyBootstrap/);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "missing private storage refuses the affected claim while ordinary public writes remain available",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.sql`DROP TABLE jones_legacy_bootstrap_threads`;
      assert.isTrue(Exit.isFailure(yield* f.sink.commitCommand(f.input).pipe(Effect.exit)));
      assert.equal((yield* f.sql`SELECT * FROM orchestration_command_receipts`).length, 0);
      assert.equal((yield* f.sql`SELECT * FROM orchestration_events`).length, 0);
      yield* f.sink.write({ events: [{ ...claimEvent, payload: publicThread }] });
      assert.equal((yield* f.sql`SELECT * FROM orchestration_v2_projection_threads`).length, 1);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "private D cannot enter through the ordinary acceptance port without its real reservation and ownership context",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.sink.commitCommand(f.input);
      const deleteId = CommandId.make(`${createId}:failure-delete`);
      const result = yield* f.sink
        .commitCommand({
          commandId: deleteId,
          commandType: "legacy-bootstrap.failure-delete",
          threadId,
          acceptedAt: now,
          events: [
            {
              id: EventId.make("forged-tombstone"),
              type: "thread.deleted",
              threadId,
              occurredAt: now,
              payload: { ...publicThread, deletedAt: now },
            },
          ],
          effects: [],
        })
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(result));
      assert.isUndefined(yield* f.storage.readLegacyReceipt(deleteId));
      assert.equal((yield* f.storage.readThread(threadId))?.thread.deletedAt, null);
      assert.equal((yield* f.sql`SELECT * FROM jones_legacy_bootstrap_terminal_cleanup`).length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "EventSink preflight commits privately, replays its exact ordinal, and fences public thread/project receipts",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const at = DateTime.formatIso(now);
      yield* f.sink.commitProjectCommand({
        commandId: CommandId.make("project-create"),
        projectId,
        commandType: "project.create",
        acceptedAt: now,
        event: {
          eventId: EventId.make("project-created"),
          aggregateKind: "project",
          aggregateId: projectId,
          occurredAt: at,
          commandId: CommandId.make("project-create"),
          causationEventId: null,
          correlationId: null,
          metadata: {},
          type: "project.created",
          payload: {
            projectId,
            title: "Synthetic",
            workspaceRoot: "/synthetic",
            defaultModelSelection: modelSelection,
            scripts: [],
            createdAt: at,
            updatedAt: at,
          },
        },
      });
      const payload = {
        type: "thread.turn.start",
        commandId: releaseId,
        threadId,
        message: { messageId, role: "user", text: "Synthetic", attachments: [] },
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        createdAt: at,
        bootstrap: {
          createThread: {
            projectId,
            title: "Synthetic",
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            createdAt: at,
          },
          prepareWorktree: {
            projectCwd: "/synthetic",
            baseBranch: "main",
            startFromOrigin: true,
            requireWorktree: true,
          },
          runSetupScript: false,
        },
      };
      const canonicalPayload = canonicalLegacyPayload(payload);
      const binding = {
        policy: { ...policy, payloadHash: legacyPayloadHash(canonicalPayload) },
        canonicalPayload,
        fetch: {
          cwd: "/synthetic",
          baseRef: "main",
          startFromOrigin: true,
          requireWorktree: true,
          remote: "origin" as const,
        },
      };
      const commandId = CommandId.make(`${createId}:preflight-intent`);
      const input = {
        commandId,
        event: {
          id: EventId.make("preflight-custom-event-id"),
          type: "legacy-bootstrap.preflight-intent" as const,
          threadId,
          occurredAt: now,
          payload: binding,
        },
      };
      const publicSequence = yield* (yield* EventStore.EventStoreV2).latestApplicationSequence;
      const first = yield* f.sink.commitLegacyPreflight(input);
      const replay = yield* f.sink.commitLegacyPreflight(input);
      assert.isTrue(first.committed);
      assert.isFalse(replay.committed);
      assert.equal(first.receipt.sequenceDomain, "jones-legacy-ordinal");
      assert.equal(replay.receipt.resultSequence, first.receipt.resultSequence);
      assert.equal(
        yield* (yield* EventStore.EventStoreV2).latestApplicationSequence,
        publicSequence,
      );
      const privateRead = yield* f.sink.readLegacyPreflight(commandId);
      assert.isTrue(Option.isSome(privateRead));
      if (Option.isSome(privateRead)) assert.equal(privateRead.value.event.id, input.event.id);
      assert.deepStrictEqual(
        Array.from(
          yield* (yield* EventStore.EventStoreV2)
            .readByCommandId({ commandId })
            .pipe(Stream.runCollect),
        ),
        [],
      );
      assert.isTrue(
        Exit.isFailure(
          yield* f.sink.commitCommand({ ...f.publicInput, commandId }).pipe(Effect.exit),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* f.sink
            .commitRejectedCommand({
              commandId,
              threadId,
              commandType: "thread.create",
              rejectedAt: now,
              error: "collision",
            })
            .pipe(Effect.exit),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* f.sink
            .commitRejectedProjectCommand({
              commandId,
              projectId,
              commandType: "project.create",
              rejectedAt: now,
              error: "collision",
            })
            .pipe(Effect.exit),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* (yield* EventStore.EventStoreV2)
            .append({ commandId, events: [input.event] })
            .pipe(Effect.exit),
        ),
      );
      assert.isTrue(
        Option.isNone(yield* (yield* Receipts.CommandReceiptStoreV2).getByCommandId(commandId)),
      );
    }).pipe(Effect.provide(layer)),
);
