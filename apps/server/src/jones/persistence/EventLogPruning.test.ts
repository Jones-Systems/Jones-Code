import * as NodeURL from "node:url";
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem";
import { assert, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as CodexFailoverStore from "../codexFailover/CodexFailoverStore.ts";
import { readApplicationBirthRecord } from "../importedHistory/ApplicationBirth.ts";
import * as ThreadMetadataReads from "../orchestration/threadMetadataReads.ts";
import { readSelectionEventRevision } from "../threadConfiguration/selectionRevision.ts";
import { makeDependencyStore } from "../threadResultDependencies/DependencyStore.ts";
import * as Evidence from "./historicalSettlementEvidence.testkit.ts";
import { captureNativeProviderSettlementProof } from "../workstreams/nativeProvider/settlementProof.ts";
import { readEventLogPrunedThrough } from "./EventLogPruneBoundary.ts";
import { makeJonesTablePresence } from "./JonesTablePresence.ts";
import { insertTestEvent, pruneEventLogLikeUpstream } from "./prunedEventLog.testkit.ts";

/**
 * Every Jones reader of the event log, classified against upstream's pruner. capture: copied into
 * a Jones row at write time; tolerate: refuses or reports unknown on a missing event; unaffected:
 * reads only rows the pruner keeps. Treating a missing event as proof of absence is never allowed.
 */
type Classification = "capture" | "tolerate" | "unaffected";
interface Reader {
  readonly file: string;
  readonly classification: Classification;
  readonly after: string;
  readonly proof: string;
}
const readers: ReadonlyArray<Reader> = [
  {
    file: "startRefusal/Store.ts",
    classification: "tolerate",
    after:
      "A persistent refusal certificate with a missing settlement event is unknown: recovery retains the unread claim and current delivery without reserving or offering a successor. Missing attempt-birth evidence denies arming an existing hold.",
    proof:
      "startRefusal/Orchestrator.integration.test.ts (upstream-like pruning after crash before reservation); startRefusal/Store.test.ts (missing settlement proof)",
  },
  {
    file: "goals/GoalControl.ts",
    classification: "tolerate",
    after:
      "Incomplete event history discards session-local resume intent before processing replay or advancing its cursor; an unresolved dispatch receipt and binding uncertainty stay unknown and never authorize resend.",
    proof:
      "goals/GoalControl.integration.test.ts (pruned Stop, registration and finite-replay races, boundary failure, unknown start)",
  },
  {
    file: "cleanup/DeletionAdmission.ts",
    classification: "tolerate",
    after:
      "A pruned `thread.deleted` event fails closed (original_birth_or_deletion_command_unavailable at start, original_deletion_event_changed at revalidation); a deletion that is still its stream's head is kept.",
    proof: "cleanup/DeletionAdmission.test.ts",
  },
  {
    file: "cleanup/DeletionGroupWitness.ts",
    classification: "tolerate",
    after:
      "Capture and revalidation read the deletion event, the group's births and its newest edge, run, attempt and session rows; a pruned one fails closed (original_group_deletion_boundary_unavailable, a native_edge/parent_execution/parent_session _unavailable reason, or original_group_generation_changed), never as a smaller group.",
    proof: "cleanup/DeletionGroupWitness.test.ts (pruned group rows)",
  },
  {
    file: "orchestration/QueueHoldProvenance.ts",
    classification: "tolerate",
    after:
      "The result names only holds the history proves automatic; a pruned event can only shrink that set, so an unproven hold stays held.",
    proof:
      "orchestration/QueueHoldProvenance.test.ts (history pruned the way the upstream pruner does)",
  },
  {
    file: "ordinaryCheckout/OrdinaryCheckoutSqlite.ts",
    classification: "tolerate",
    after:
      "A pruned basis event makes the admission unavailable (readAdmission null, renew/start unavailable); Jones's own ownership rows keep the exclusion history.",
    proof: "ordinaryCheckout/OrdinaryCheckout.test.ts",
  },
  {
    file: "settlementOrigins/SettlementOrigins.ts",
    classification: "tolerate",
    after:
      "A pruned `thread.settled` event reports unknown; a missing client marker reports other only inside the marker retention window, unknown beyond it.",
    proof: "settlementOrigins/SettlementOrigins.test.ts",
  },
  {
    file: "threadConfiguration/selectionRevision.ts",
    classification: "tolerate",
    after:
      "The selection revision is raised to the pruned-through sequence, so pruning a newer selection event can never make an older revision current again.",
    proof: "persistence/EventLogPruning.test.ts",
  },
  {
    file: "threadConfiguration/ThreadSelfConfiguration.ts",
    classification: "tolerate",
    after:
      "Only reads the idempotent replay through readByCommandId; a pruned replay finds no event, then fails the clamped revision check instead of applying the change twice.",
    proof: "persistence/EventLogPruning.test.ts",
  },
  {
    file: "codexFailover/CodexFailoverStore.ts",
    classification: "tolerate",
    after:
      "Revision stays the raw newest selection sequence; a pruned switch event leaves no sequence to compare, which keeps the step ineligible.",
    proof: "codexFailover/CodexFailoverEligibility.test.ts (missing switch event)",
  },
  {
    file: "codexFailover/CodexFailover.ts",
    classification: "tolerate",
    after:
      "readByCommandId finds no switch event, so switchSequence is null and the step is ineligible.",
    proof: "codexFailover/CodexFailoverEligibility.test.ts (missing switch event)",
  },
  {
    file: "workstreams/nativeProvider/settlementProof.ts",
    classification: "capture",
    after:
      "The settlement proof row copies the receipt and events at commit; without a proof the reader returns the receipt with no events and the consumer reports unknown (evidence_conflict).",
    proof: "persistence/EventLogPruning.test.ts (captured proof and missing pruned events)",
  },
  {
    file: "nativeCreation/NativeCreationExecutionSqlite.ts",
    classification: "tolerate",
    after:
      "A pruned acceptance event is unresolved ('no exact committed event and receipt'); the claim stays unresolved and nothing executes.",
    proof: "none: needs the full native-creation fixture, reasoned from code",
  },
  {
    file: "orchestration/AcceptedTerminalRecovery.ts",
    classification: "tolerate",
    after:
      "Readback belongs to a live consumer and retains the original command, events and effects in memory. Partial history fails exact-batch verification. Empty history retries only the original guarded publication; an already settled or superseded run refuses finalization. The unconditional interrupt-result batch carries no effects. No recovery state survives a server restart.",
    proof:
      "orchestration/AcceptedTerminalRecovery.test.ts (uncertain commit, exact readback, Stop/successor guards); concurrent pruning during recovery is reasoned from the retained batch and writer guards, not execution-proved",
  },
  {
    file: "orchestration/HistoricalTerminalHistory.ts",
    classification: "tolerate",
    after:
      "Missing restart command events refuse historical admission. Exact event-ID readback compares the retained selected event with Schema-encoded stored facts; missing, pruned or unavailable readback preserves the original failure, never proves absence or permits a new historical commit.",
    proof:
      "orchestration/HistoricalTerminalHistory.test.ts; orchestration/SupersededTerminal.integration.test.ts (authored, CI qualification required)",
  },
  {
    file: "orchestration/RestartPreparation.ts",
    classification: "tolerate",
    after:
      "Missing restart-command events fail run admission with RestartPreparationError; latestSequence reads the retained stream head.",
    proof: "none: needs the full provider fixture, reasoned from code",
  },
  {
    file: "orchestration/deliveryEvidence.ts",
    classification: "tolerate",
    after:
      "Receipt without recorded events reports creation and first-message delivery unknown, never accepted.",
    proof: "orchestration/deliveryEvidence.test.ts (launch-unproven-claim)",
  },
  {
    file: "providerRetirement/providerRecoveryPublication.ts",
    classification: "tolerate",
    after:
      "A pruned boundary event reads as uncommitted, then the generation guard fails the retry; the retry state lives in process memory only.",
    proof: "none: needs the full provider fixture, reasoned from code",
  },
  {
    file: "cleanup/RuntimeUseInstallation.ts",
    classification: "unaffected",
    after:
      "Records the global maximum sequence once, inside the installing migration; the newest event is its stream's head, which the pruner always keeps.",
    proof: "persistence/EventLogPruning.test.ts (high-water readers)",
  },
  {
    file: "testing/SyntheticPresentation.ts",
    classification: "unaffected",
    after:
      "Private presentation fixture installer; its collision check also reads the projection tables, and a pruned event only frees an id it would have refused. Not a production reader.",
    proof: "n/a",
  },
  {
    file: "importedHistory/ApplicationBirth.ts",
    classification: "unaffected",
    after: "Reads `thread.created`, which the pruner always keeps.",
    proof: "persistence/EventLogPruning.test.ts",
  },
  {
    file: "workspaceLifecycle/NativeSource.ts",
    classification: "unaffected",
    after: "Reads `thread.created` (and ApplicationBirth), which the pruner always keeps.",
    proof: "persistence/EventLogPruning.test.ts (ApplicationBirth)",
  },
  {
    file: "threadResultDependencies/WakeObligations.ts",
    classification: "tolerate",
    after:
      "A missing run-attempt.created event denies accepted-start release even when its matching attempt projection survives. The persistent hold retains the answer and outgoing duty across restart; it remains ineligible for wake admission.",
    proof:
      "threadResultDependencies/WakeObligations.test.ts (pruned attempt birth before accepted start)",
  },
  {
    file: "threadResultDependencies/DependencyStore.ts",
    classification: "unaffected",
    after: "The high-water sequence is a stream head, which the pruner always keeps.",
    proof: "persistence/EventLogPruning.test.ts",
  },
  {
    file: "orchestration/threadMetadataReads.ts",
    classification: "unaffected",
    after: "The snapshot watermark is a thread stream head, which the pruner always keeps.",
    proof: "persistence/EventLogPruning.test.ts",
  },
  {
    file: "orchestration/shellMaterialization.ts",
    classification: "unaffected",
    after: "latestApplicationSequence is the same retained stream-head maximum.",
    proof: "persistence/EventLogPruning.test.ts (high-water readers)",
  },
  {
    file: "threadAttention.ts",
    classification: "unaffected",
    after: "latestApplicationSequence is the same retained stream-head maximum.",
    proof: "persistence/EventLogPruning.test.ts (high-water readers)",
  },
  {
    file: "workMode/WorkMode.ts",
    classification: "unaffected",
    after: "latestSequence is the same retained stream-head maximum.",
    proof: "persistence/EventLogPruning.test.ts (high-water readers)",
  },
  {
    file: "performance/v2Workload.ts",
    classification: "unaffected",
    after: "Benchmark harness that reads events it wrote seconds earlier; not a production reader.",
    proof: "n/a",
  },
  {
    file: "persistence/Migrations/100_JonesNativeCreationExecution.ts",
    classification: "unaffected",
    after:
      "Names the event table only in a foreign-key clause that migration 105 removes; it reads nothing.",
    proof: "persistence/UpstreamIsolation.test.ts",
  },
];

const eventLogReader =
  /orchestration_events|EventStoreV2|OrchestrationEventStore|latestSequence|latestApplicationSequence|readByCommandId/;
// The boundary helper names the table to read the pruner's own metadata, not event history.
const exempt = new Set(["persistence/EventLogPruneBoundary.ts"]);
const jonesRoot = NodeURL.fileURLToPath(new URL("..", import.meta.url));

it.effect(
  "every Jones source that reads the event log is classified, and nothing stale remains",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const found: Array<string> = [];
      for (const relative of yield* fs.readDirectory(jonesRoot, { recursive: true })) {
        const file = relative.split("\\").join("/");
        if (!file.endsWith(".ts") || /\.(test|testkit)\.ts$/.test(file) || exempt.has(file))
          continue;
        if (eventLogReader.test(yield* fs.readFileString(`${jonesRoot}${file}`))) found.push(file);
      }
      const classified = readers.map((reader) => reader.file);
      assert.deepEqual(
        found.toSorted(),
        classified.toSorted(),
        "classify new event-log readers in EventLogPruning.test.ts, or remove stale entries",
      );
      assert.strictEqual(new Set(classified).size, classified.length);
    }).pipe(Effect.provide(NodeFileSystem.layer)),
);

const database = SqlitePersistence.layerMemory;
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const thread = ThreadId.make("thread:pruned-reader");
const otherThread = ThreadId.make("thread:pruned-reader-other");
const day = "2026-11-01T00:00:00.000Z";

const prunedThrough = Effect.gen(function* () {
  return yield* readEventLogPrunedThrough(yield* SqlClient.SqlClient);
});

it.effect("the pruner publishes its boundary and the fixture applies upstream's keep rule", () =>
  Effect.gen(function* () {
    assert.strictEqual(yield* prunedThrough, 0);
    yield* insertTestEvent({
      sequence: 1,
      eventId: "e1",
      streamId: thread,
      type: "thread.created",
    });
    yield* insertTestEvent({
      sequence: 2,
      eventId: "e2",
      streamId: thread,
      type: "thread.updated",
    });
    yield* insertTestEvent({
      sequence: 3,
      eventId: "e3",
      streamId: thread,
      type: "thread.updated",
    });
    yield* insertTestEvent({
      sequence: 4,
      eventId: "e4",
      streamId: otherThread,
      type: "thread.created",
    });
    yield* insertTestEvent({
      sequence: 5,
      eventId: "e5",
      streamId: otherThread,
      type: "thread.updated",
    });
    assert.deepEqual(yield* pruneEventLogLikeUpstream(4), [2]);
    assert.strictEqual(yield* prunedThrough, 4);
    // A later, lower pass never moves the boundary back.
    yield* pruneEventLogLikeUpstream(1);
    assert.strictEqual(yield* prunedThrough, 4);
  }).pipe(Effect.provide(database)),
);

it.effect("selection revision never regresses when upstream prunes a newer selection event", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* insertTestEvent({
      sequence: 1,
      eventId: "s1",
      streamId: thread,
      type: "thread.created",
    });
    yield* insertTestEvent({
      sequence: 2,
      eventId: "s2",
      streamId: thread,
      type: "thread.model-selection-updated",
    });
    yield* insertTestEvent({
      sequence: 3,
      eventId: "s3",
      streamId: thread,
      type: "thread.updated",
    });
    // A client that read revision 1 must not match again once revision 2's event is pruned away.
    assert.strictEqual(yield* readSelectionEventRevision(sql, thread), 2);
    assert.deepEqual(yield* pruneEventLogLikeUpstream(3), [2]);
    const raw = yield* sql<{ readonly revision: number }>`
      SELECT MAX(sequence) AS revision FROM orchestration_events
      WHERE stream_id = ${thread} AND event_type IN ('thread.created', 'thread.model-selection-updated')`;
    assert.strictEqual(raw[0]!.revision, 1, "the raw maximum falls back to the thread's birth");
    assert.strictEqual(yield* readSelectionEventRevision(sql, thread), 3);
    // A real later selection event still wins; the boundary only sets a floor.
    yield* insertTestEvent({
      sequence: 7,
      eventId: "s7",
      streamId: thread,
      type: "thread.provider-switched",
    });
    assert.strictEqual(yield* readSelectionEventRevision(sql, thread), 7);
  }).pipe(Effect.provide(database)),
);

it.effect("selection revision is unchanged when nothing was pruned or the thread is newer", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    assert.strictEqual(yield* readSelectionEventRevision(sql, thread), 0);
    yield* insertTestEvent({
      sequence: 1,
      eventId: "n1",
      streamId: thread,
      type: "thread.created",
    });
    yield* insertTestEvent({
      sequence: 2,
      eventId: "n2",
      streamId: thread,
      type: "thread.model-selection-updated",
    });
    assert.strictEqual(yield* readSelectionEventRevision(sql, thread), 2);
    yield* insertTestEvent({
      sequence: 10,
      eventId: "n10",
      streamId: otherThread,
      type: "thread.created",
    });
    yield* insertTestEvent({
      sequence: 11,
      eventId: "n11",
      streamId: otherThread,
      type: "thread.model-selection-updated",
    });
    yield* pruneEventLogLikeUpstream(5);
    assert.strictEqual(yield* readSelectionEventRevision(sql, otherThread), 11);
  }).pipe(Effect.provide(database)),
);

it.effect("the failover step's selection revision stays raw so a pruned switch is ineligible", () =>
  Effect.gen(function* () {
    const store = yield* CodexFailoverStore.make;
    const sql = yield* SqlClient.SqlClient;
    yield* insertTestEvent({
      sequence: 1,
      eventId: "f1",
      streamId: thread,
      type: "thread.created",
    });
    yield* insertTestEvent({
      sequence: 2,
      eventId: "f2",
      streamId: thread,
      type: "thread.provider-switched",
    });
    yield* insertTestEvent({
      sequence: 3,
      eventId: "f3",
      streamId: thread,
      type: "thread.updated",
    });
    assert.strictEqual(yield* store.selectionRevision(thread), 2);
    assert.deepEqual(yield* pruneEventLogLikeUpstream(3), [2]);
    // The switch event is gone, so it can never equal this revision; the raw value is only ever lower.
    assert.strictEqual(yield* store.selectionRevision(thread), 1);
    assert.strictEqual(yield* readSelectionEventRevision(sql, thread), 3);
  }).pipe(Effect.provide(database)),
);

const birthPayload = (id: ThreadId) => ({ id, projectId: "project:pruned", createdAt: day });
const seedBirth = (id: ThreadId, sequence: number) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const payload = encodeJson(birthPayload(id));
    yield* sql`INSERT INTO projection_projects(project_id,title,workspace_root,scripts_json,created_at,updated_at)
      VALUES('project:pruned','synthetic','/repo','[]',${day},${day}) ON CONFLICT(project_id) DO NOTHING`;
    yield* sql`INSERT INTO orchestration_v2_projection_threads(thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,deleted_at,payload_json)
      VALUES(${id},'project:pruned','synthetic','codex','local','default',${day},${day},NULL,${payload})`;
    yield* insertTestEvent({
      sequence,
      eventId: `birth:${id}`,
      streamId: id,
      type: "thread.created",
      payload: birthPayload(id),
    });
  });

it.effect("the application birth survives pruning, and its absence is a refusal, not a guess", () =>
  Effect.gen(function* () {
    yield* seedBirth(thread, 1);
    yield* insertTestEvent({
      sequence: 2,
      eventId: "b2",
      streamId: thread,
      type: "thread.updated",
    });
    yield* insertTestEvent({
      sequence: 3,
      eventId: "b3",
      streamId: thread,
      type: "thread.updated",
    });
    const before = yield* readApplicationBirthRecord(thread);
    assert.deepEqual(yield* pruneEventLogLikeUpstream(3), [2]);
    assert.deepEqual(yield* readApplicationBirthRecord(thread), before);
    assert.strictEqual(before?.sequence, 1);
    const sql = yield* SqlClient.SqlClient;
    yield* sql`DELETE FROM orchestration_events WHERE event_type = 'thread.created'`;
    assert.isNull(yield* readApplicationBirthRecord(thread));
  }).pipe(Effect.provide(database)),
);

it.effect("high-water readers read the same retained stream-head maximum after pruning", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* insertTestEvent({
      sequence: 1,
      eventId: "h1",
      streamId: thread,
      type: "thread.created",
    });
    yield* insertTestEvent({
      sequence: 2,
      eventId: "h2",
      streamId: thread,
      type: "thread.updated",
    });
    yield* insertTestEvent({
      sequence: 3,
      eventId: "h3",
      streamId: otherThread,
      type: "thread.created",
    });
    yield* insertTestEvent({
      sequence: 4,
      eventId: "h4",
      streamId: otherThread,
      type: "thread.updated",
    });
    yield* insertTestEvent({
      sequence: 5,
      eventId: "h5",
      streamId: thread,
      type: "thread.updated",
    });
    yield* insertTestEvent({
      sequence: 6,
      eventId: "h6",
      streamId: "project:pruned",
      aggregateKind: "project",
      type: "project.meta-updated",
    });
    const dependencies = makeDependencyStore(sql, makeJonesTablePresence(sql));
    const metadata = yield* ThreadMetadataReads.ThreadMetadataReads;
    // The global maximum the runtime-use installation records inside its migration.
    const globalMaximum = Effect.map(
      sql<{
        readonly maximum: number | null;
      }>`SELECT max(sequence) AS maximum FROM orchestration_events`,
      (rows) => rows[0]?.maximum,
    );
    assert.strictEqual(yield* dependencies.sequence, 6);
    assert.strictEqual((yield* metadata.list()).snapshotSequence, 5);
    assert.strictEqual(yield* globalMaximum, 6);
    assert.deepEqual(yield* pruneEventLogLikeUpstream(6), [2]);
    assert.strictEqual(yield* dependencies.sequence, 6);
    assert.strictEqual((yield* metadata.list()).snapshotSequence, 5);
    assert.strictEqual(yield* globalMaximum, 6);
  }).pipe(Effect.provide(Layer.provideMerge(ThreadMetadataReads.layer, database))),
);

it.effect("the Jones queries that name an upstream index still find it", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const names = (yield* sql<{ readonly name: string }>`
      SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'orchestration_events'
        AND name IN ('idx_orchestration_events_application_high_water',
          'idx_orchestration_events_agent_stream_sequence')
      ORDER BY name`).map((row) => row.name);
    assert.deepEqual(names, [
      "idx_orchestration_events_agent_stream_sequence",
      "idx_orchestration_events_application_high_water",
    ]);
  }).pipe(Effect.provide(database)),
);

const receipt = (commandId: string, resultSequence: number) => ({
  commandId,
  aggregateKind: "thread" as const,
  aggregateId: thread,
  commandType: "thread.settle",
  acceptedAt: day,
  resultSequence,
  status: "accepted" as const,
});
const commandEvents = (commandId: string, first: number, types: ReadonlyArray<string>) =>
  types.map((type, index) => ({
    eventId: `${commandId}:${index}`,
    commandId,
    aggregateKind: "thread" as const,
    aggregateId: thread,
    sequence: first + index,
    type,
    occurredAt: day,
    applicationEventVersion: 2,
  }));

it.effect(
  "native settlement evidence survives pruning only where the proof was captured at commit",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* insertTestEvent({
        sequence: 1,
        eventId: "p1",
        streamId: thread,
        type: "thread.created",
      });
      // The first command had a reserved proof; the second never did.
      yield* sql`INSERT INTO jones_workstreams_settlement_proofs(native_command_id, thread_id, native_action, proof_json)
        VALUES ('command:proved', ${thread}, 'settle', NULL)`;
      for (const [commandId, first] of [
        ["command:proved", 2],
        ["command:bare", 4],
      ] as const) {
        const events = commandEvents(commandId, first, ["thread.settled", "thread.unpinned"]);
        const written = receipt(commandId, first + 1);
        yield* sql`INSERT INTO orchestration_command_receipts
          (command_id, aggregate_kind, aggregate_id, command_type, accepted_at, result_sequence, status, error)
          VALUES (${commandId}, 'thread', ${thread}, 'thread.settle', ${day}, ${first + 1}, 'accepted', NULL)`;
        for (const event of events)
          yield* insertTestEvent({
            sequence: event.sequence,
            eventId: event.eventId,
            streamId: thread,
            type: event.type,
            commandId,
            occurredAt: day,
          });
        yield* captureNativeProviderSettlementProof({ receipt: written, events });
      }
      yield* insertTestEvent({
        sequence: 6,
        eventId: "p6",
        streamId: thread,
        type: "thread.updated",
      });
      const reader = yield* Evidence.NativeProviderEvidence;
      const proved = yield* reader.readSnapshotByCommandId("command:proved", thread);
      const bareBefore = yield* reader.readSnapshotByCommandId("command:bare", thread);
      assert.lengthOf(proved.events, 2);
      assert.lengthOf(bareBefore.events, 2);
      assert.deepEqual(yield* pruneEventLogLikeUpstream(6), [2, 3, 4, 5]);
      assert.deepEqual(yield* reader.readSnapshotByCommandId("command:proved", thread), proved);
      // Receipts are retained, so the consumer sees an accepted command with no events and refuses.
      const bareAfter = yield* reader.readSnapshotByCommandId("command:bare", thread);
      assert.isTrue(Option.isSome(bareAfter.receipt));
      assert.deepEqual(bareAfter.events, []);
    }).pipe(
      Effect.provide(Evidence.makeNativeProviderEvidenceLayer().pipe(Layer.provideMerge(database))),
    ),
);
