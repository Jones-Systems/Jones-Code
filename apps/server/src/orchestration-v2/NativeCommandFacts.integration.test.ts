import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import {
  SqlitePersistenceMemory,
  makeSqlitePersistenceLive,
} from "../persistence/Layers/Sqlite.ts";
import { runMigrations, jonesMigrationEntries } from "../persistence/Migrations.ts";
import { runJonesMigrations } from "../persistence/JonesMigrationGuard.ts";
import ForeignV2DDL from "../persistence/Migrations/142_JonesV2NativeAcceptance.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as LegacyOwner from "./legacy/LegacyV1ThreadImporter.ts";
import { makeCommitTransaction } from "./CommitTransaction.ts";
import {
  nativeCreationCanonicalJson,
  nativeCreationSha256,
} from "../nativeCreation/NativeCreationPreparation.ts";
import {
  NativeWorkstreamSettlementWitnessV2,
  nativeWorkstreamSettlementWitnessBindingDigestV2,
} from "../nativeCreation/NativeCreationExecutionTypes.ts";
import * as Schema from "effect/Schema";

const withReader = <E, R>(database: Layer.Layer<SqlClient.SqlClient, E, R>, available = true) => {
  const owner = Layer.effect(
    EventSink.LegacyCurrentSourceReader,
    LegacyOwner.makeLegacyCurrentSourceReader,
  ).pipe(Layer.provide(database));
  const stores = Layer.mergeAll(
    database,
    EventStore.layer.pipe(Layer.provide(database)),
    ProjectionStore.layer.pipe(Layer.provide(database)),
  );
  return EventSink.layer.pipe(Layer.provideMerge(available ? Layer.merge(stores, owner) : stores));
};
const persistence = withReader(SqlitePersistenceMemory);
const now = DateTime.makeUnsafe("2026-10-05T00:00:00.000Z");
class OwnedRollbackFixtureError extends Schema.TaggedError<OwnedRollbackFixtureError>()(
  "OwnedRollbackFixtureError",
  {},
) {}
const seed = Effect.fnUntraced(function* () {
  const sink = yield* EventSink.EventSinkV2;
  const sql = yield* SqlClient.SqlClient;
  const events = yield* EventStore.EventStoreV2;
  const threadId = ThreadId.make("thread:private-native-facts");
  const commandId = CommandId.make("command:private-native-facts:birth");
  const instance = ProviderInstanceId.make("codex");
  const thread: OrchestrationV2AppThread = {
    id: threadId,
    projectId: ProjectId.make("project:private-native-facts"),
    title: "Private facts",
    providerInstanceId: instance,
    modelSelection: { instanceId: instance, model: "gpt-5.4" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { rootThreadId: threadId, parentThreadId: null, relationshipToParent: null },
    forkedFrom: null,
    createdBy: "user",
    creationSource: "web",
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  const birth = {
    id: EventId.make("event:private-native-facts:birth"),
    type: "thread.created" as const,
    threadId,
    occurredAt: now,
    payload: thread,
  };
  const committed = yield* sink.commitCommand({
    commandId,
    commandType: "thread.create",
    threadId,
    acceptedAt: now,
    events: [birth],
    effects: [],
  });
  const read = sink.readNativeCommandFacts;
  if (read === undefined) return yield* Effect.die("Production private fact reader missing");
  return { sql, sink, events, threadId, commandId, thread, birth, committed, read };
});

it.effect(
  "reads the committed receipt, unique birth and every original contributor group without writes",
  () =>
    Effect.gen(function* () {
      const f = yield* seed();
      const before = yield* f.sql`SELECT * FROM orchestration_events ORDER BY sequence`;
      const facts = yield* f.read({ threadId: f.threadId, commandId: f.commandId });
      assert.deepEqual(facts.receipt, f.committed.receipt);
      assert.equal(facts.creationProvenance, "native_created");
      assert.deepEqual(facts.incarnation, {
        eventId: f.birth.id,
        sequence: f.committed.storedEvents[0]!.sequence,
      });
      assert.equal(facts.events[0]?.commandId, f.commandId);
      assert.equal(facts.eventMetadata[0]?.eventId, f.birth.id);
      assert.equal(facts.projection?.thread.id, f.threadId);
      assert.equal(facts.workstreamWitness, null);
      assert.equal(facts.nativeCreationHistory, null);
      assert.deepEqual(
        Object.keys(facts.commitSnapshot.records).sort(),
        [
          "threads",
          "runs",
          "run_attempts",
          "nodes",
          "provider_threads",
          "provider_turns",
          "runtime_requests",
          "messages",
          "plans",
          "turn_items",
          "checkpoint_scopes",
          "checkpoints",
          "context_handoffs",
          "subagents",
          "context_transfers",
          "project",
          "projection_schema",
          "source_runtime",
          "turn_item_positions",
          "provider_sessions",
          "session_bindings",
          "effects",
          "thread_deletion_commands",
          "cleanup_task_bindings",
          "cleanup_task_outcomes",
          "worktree_path_admissions",
          "unknown_effect_holds",
          "launch_workflows",
          "runtime_evidence",
          "continuation_sources",
          "restart_continuations",
          "legacy_continuation",
          "native_import_seals",
          "legacy_import_markers",
          "legacy_source_threads",
          "legacy_source_messages",
          "imported_source_events",
          "native_confirmations",
          "imported_choices",
          "imported_outcomes",
          "stop_intents",
          "stop_fences",
          "start_reservations",
          "workstream_witnesses",
        ].sort(),
      );
      assert.deepEqual(
        Object.keys(facts.commitSnapshot.authorityRecords).sort(),
        [
          "sessions",
          "automation_enrollment",
          "provider_enrollment",
          "attempts",
          "claims",
          "normalized_commands",
          "reserved_commands",
          "reserved_command_identities",
          "effect_facts",
          "leases",
        ].sort(),
      );
      assert.deepEqual(yield* f.sql`SELECT * FROM orchestration_events ORDER BY sequence`, before);
      assert.deepEqual(yield* f.read({ threadId: f.threadId, commandId: f.commandId }), facts);
    }).pipe(Effect.provide(Layer.fresh(persistence))),
);

it.effect("filters the exact command before decoding unrelated malformed persisted events", () =>
  Effect.gen(function* () {
    const f = yield* seed();
    const unrelated = CommandId.make("command:private-native-facts:unrelated");
    const other = EventId.make("event:private-native-facts:unrelated");
    yield* f.events.append({ commandId: unrelated, events: [{ ...f.birth, id: other }] });
    yield* f.sql`UPDATE orchestration_events SET payload_json = '{' WHERE event_id = ${other}`;
    const facts = yield* f.read({ threadId: f.threadId, commandId: f.commandId });
    assert.deepEqual(
      facts.events.map((x) => x.event.id),
      [f.birth.id],
    );
    assert.deepEqual(
      facts.eventMetadata.map((x) => x.commandId),
      [f.commandId],
    );
    const invalid = yield* f.events
      .read({ commandId: unrelated, limit: 1 })
      .pipe(Stream.runCollect, Effect.result);
    assert.equal(invalid._tag, "Failure");
  }).pipe(Effect.provide(Layer.fresh(persistence))),
);

it.effect("retains the 257th actual metadata and event overflow sentinel before more rows", () =>
  Effect.gen(function* () {
    const f = yield* seed();
    const commandId = CommandId.make("command:private-native-facts:overflow");
    yield* f.events.append({
      commandId,
      events: Array.from({ length: 259 }, (_, index) => ({
        id: EventId.make(`event:private-native-facts:overflow:${index}`),
        type: "thread.visited" as const,
        threadId: f.threadId,
        occurredAt: now,
        payload: { ...f.thread, lastVisitedAt: now },
      })),
    });
    const facts = yield* f.read({ threadId: f.threadId, commandId });
    assert.equal(facts.eventMetadata.length, 257);
    assert.equal(facts.events.length, 257);
    assert.isTrue(facts.eventMetadataOverflow);
    assert.equal(facts.events.at(-1)?.event.id, "event:private-native-facts:overflow:256");
    assert.equal(
      (yield* f.sql<{
        count: number;
      }>`SELECT count(*) AS count FROM orchestration_events WHERE command_id = ${commandId}`)[0]!
        .count,
      259,
    );
    assert.isAbove(facts.snapshotSequence, facts.events.at(-1)!.sequence);
  }).pipe(Effect.provide(Layer.fresh(persistence))),
);

it.effect("keeps missing, duplicated and copied birth provenance unavailable", () =>
  Effect.gen(function* () {
    const f = yield* seed();
    const absent = yield* f.read({
      threadId: ThreadId.make("thread:private-native-facts:absent"),
      commandId: CommandId.make("command:private-native-facts:absent"),
    });
    assert.equal(absent.creationProvenance, "unavailable");
    assert.equal(absent.incarnation, null);
    yield* f.sql`UPDATE orchestration_events SET payload_json = json_set(payload_json, '$.id', 'thread:copied') WHERE event_id = ${f.birth.id}`;
    const copied = yield* f.read({ threadId: f.threadId, commandId: f.commandId });
    assert.equal(copied.incarnation, null);
    assert.equal(copied.creationProvenance, "unavailable");
    yield* f.events.append({
      commandId: CommandId.make("command:private-native-facts:duplicate"),
      events: [{ ...f.birth, id: EventId.make("event:private-native-facts:duplicate") }],
    });
    assert.equal(
      (yield* f.read({ threadId: f.threadId, commandId: f.commandId })).incarnation,
      null,
    );
  }).pipe(Effect.provide(Layer.fresh(persistence))),
);

it.effect(
  "denies malformed persisted contributor JSON instead of replacing it with an empty group",
  () =>
    Effect.gen(function* () {
      const f = yield* seed();
      yield* f.sql`UPDATE orchestration_v2_projection_threads SET payload_json = '{' WHERE thread_id = ${f.threadId}`;
      const before = yield* f.sql`SELECT * FROM orchestration_v2_projection_threads`;
      const result = yield* f
        .read({ threadId: f.threadId, commandId: f.commandId })
        .pipe(Effect.result);
      assert.equal(result._tag, "Failure");
      assert.deepEqual(yield* f.sql`SELECT * FROM orchestration_v2_projection_threads`, before);
    }).pipe(Effect.provide(Layer.fresh(persistence))),
);

it.effect(
  "reads the outer same-SQL pending commit and observes its rollback without publishing authority",
  () =>
    Effect.gen(function* () {
      const f = yield* seed();
      const owner = yield* makeCommitTransaction();
      const id = CommandId.make("command:private-native-facts:rollback");
      const before = yield* f.read({ threadId: f.threadId, commandId: id });
      const result = yield* owner
        .withTransaction(
          Effect.gen(function* () {
            yield* f.sink.commitCommand({
              commandId: id,
              commandType: "thread.mark-unread",
              threadId: f.threadId,
              acceptedAt: now,
              effects: [],
              events: [
                {
                  ...f.birth,
                  id: EventId.make("event:private-native-facts:rollback"),
                  type: "thread.marked-unread",
                },
              ],
            });
            const pending = yield* f.read({ threadId: f.threadId, commandId: id });
            assert.equal(pending.receipt?.commandId, id);
            assert.lengthOf(pending.events, 1);
            return yield* Effect.fail(new OwnedRollbackFixtureError());
          }),
        )
        .pipe(Effect.result);
      assert.equal(result._tag, "Failure");
      assert.deepEqual(yield* f.read({ threadId: f.threadId, commandId: id }), before);
    }).pipe(Effect.provide(Layer.fresh(persistence))),
);

it.effect("denies a missing captured legacy owner even when the current native birth matches", () =>
  Effect.gen(function* () {
    const f = yield* seed();
    const failure = yield* f
      .read({ threadId: f.threadId, commandId: f.commandId })
      .pipe(Effect.result);
    assert.equal(failure._tag, "Failure");
    if (failure._tag === "Failure")
      assert.equal(failure.failure._tag, "NativeCommandPreconditionError");
  }).pipe(Effect.provide(Layer.fresh(withReader(SqlitePersistenceMemory, false)))),
);

it.effect("refuses a different SQL identity and a finalized reader before any source read", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const threadId = ThreadId.make("thread:private-native-facts:identity");
    const live = yield* LegacyOwner.makeLegacyCurrentSourceReader;
    const otherResult = yield* Effect.gen(function* () {
      const other = yield* SqlClient.SqlClient;
      assert.notStrictEqual(other, sql);
      return yield* live.read({ sql: other, threadId });
    }).pipe(Effect.provide(Layer.fresh(SqlitePersistenceMemory)));
    assert.deepEqual(otherResult, {
      status: "unavailable",
      reason: "legacy_current_source_connection_unavailable",
    });
    const closed = yield* Effect.scoped(LegacyOwner.makeLegacyCurrentSourceReader);
    assert.deepEqual(yield* closed.read({ sql, threadId }), {
      status: "unavailable",
      reason: "legacy_current_source_connection_unavailable",
    });
  }).pipe(Effect.scoped, Effect.provide(Layer.fresh(SqlitePersistenceMemory))),
);

it.effect(
  "refuses recognized foreign source history before excluded native tables and leaves it inert",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 54 });
      yield* runJonesMigrations(jonesMigrationEntries.slice(0, 6));
      yield* runMigrations({ toMigrationInclusive: 56 });
      // The original foreign007 templates equal receiving142; these are foreign fixture bytes, never own142 history.
      yield* ForeignV2DDL;
      yield* sql`INSERT INTO jones_sql_migrations (migration_id, name) VALUES (7, 'V2NativeAcceptance')`;
      yield* runMigrations();
      yield* sql`INSERT INTO orchestration_v2_provider_runtime_evidence VALUES (
      'thread:foreign', 'provider:foreign', 'session:foreign', 'codex', 'codex', 'native:foreign',
      'generation:foreign', 1, '{}', '2026-10-05T00:00:00.000Z')`;
      const ledger = yield* sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`;
      const rows = yield* sql`SELECT * FROM orchestration_v2_provider_runtime_evidence`;
      const schema = yield* sql`SELECT * FROM sqlite_master ORDER BY type, name`;
      const result = yield* Effect.gen(function* () {
        const sink = yield* EventSink.EventSinkV2;
        if (sink.readNativeCommandFacts === undefined)
          return yield* Effect.die("Missing actual reader");
        return yield* sink
          .readNativeCommandFacts({
            threadId: ThreadId.make("thread:foreign"),
            commandId: CommandId.make("command:foreign"),
          })
          .pipe(Effect.result);
      }).pipe(
        Effect.provide(
          EventSink.layer.pipe(Layer.provide(Layer.merge(EventStore.layer, ProjectionStore.layer))),
        ),
      );
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure")
        assert.equal(result.failure._tag, "NativeCommandPreconditionError");
      assert.deepEqual(
        yield* sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`,
        ledger,
      );
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_provider_runtime_evidence`, rows);
      assert.deepEqual(yield* sql`SELECT * FROM sqlite_master ORDER BY type, name`, schema);
    }).pipe(Effect.provide(Layer.fresh(NodeSqliteClient.layer({ filename: ":memory:" })))),
);

it.effect(
  "reopens registered file SQL with the exact same private receipt, incarnation and contributors",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        ...(process.env.TMPDIR === undefined ? {} : { directory: process.env.TMPDIR }),
        prefix: "native-facts-",
      });
      const database = makeSqlitePersistenceLive(path.join(directory, "fixture.sqlite"));
      const first = yield* Effect.scoped(
        Effect.gen(function* () {
          const f = yield* seed();
          return yield* f.read({ threadId: f.threadId, commandId: f.commandId });
        }).pipe(Effect.provide(Layer.fresh(withReader(database)))),
      );
      const reopened = yield* Effect.scoped(
        Effect.gen(function* () {
          const sink = yield* EventSink.EventSinkV2;
          if (sink.readNativeCommandFacts === undefined)
            return yield* Effect.die("Missing actual reopened reader");
          return yield* sink.readNativeCommandFacts({
            threadId: first.threadId,
            commandId: first.commandId,
          });
        }).pipe(Effect.provide(Layer.fresh(withReader(database)))),
      );
      assert.deepEqual(reopened, first);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect.each([
  "synthetic",
  "digest",
  "copied",
  "generation",
  "native_id",
  "driver",
  "driver_carrier",
  "extra",
] as const)(
  "never authenticates a persisted %s workstream witness as a producer capability",
  (mode) =>
    Effect.gen(function* () {
      const f = yield* seed();
      const commandId = CommandId.make("command:private-native-facts:settle");
      const command = { type: "thread.settle" as const, threadId: f.threadId, commandId };
      yield* f.sink.commitCommand({
        commandId,
        commandType: command.type,
        threadId: f.threadId,
        acceptedAt: now,
        effects: [],
        events: [
          {
            ...f.birth,
            id: EventId.make("event:private-native-facts:settle"),
            type: "thread.settled",
            payload: { ...f.thread, settledAt: now, settledOverride: "settled" as const },
          },
        ],
      });
      const original = {
        version: 2,
        command,
        attemptKey: { owner_id: "owner", principal_id: "principal", command_id: "attempt" },
        dispatchStartedAt: "2026-10-05T00:00:00.000Z",
        actorSessionId: "session:witness",
        enrollmentSha256: "a".repeat(64),
        requestBytesSha256: "b".repeat(64),
        authority: {
          environmentId: "environment",
          authorityNamespace: "namespace",
          storeGeneration: 1,
        },
        incarnation: { eventId: f.birth.id, sequence: f.committed.storedEvents[0]!.sequence },
        targetEventSequence: f.committed.storedEvents[0]!.sequence,
        provider: {
          binding: {
            threadId: f.threadId,
            providerThreadId: "provider-thread:witness",
            providerSessionId: "provider-session:witness",
            instanceId: "codex",
            driver: "codex",
            nativeThreadId: "native:witness",
            runtimeGeneration: "generation:witness",
          },
          evidenceRevision: 1,
        },
      };
      const qualifiedCodec = yield* Schema.decodeUnknownEffect(NativeWorkstreamSettlementWitnessV2)(
        original,
      );
      const witness = {
        ...original,
        ...(mode === "extra" ? { copiedAuthority: true } : {}),
        provider: {
          ...original.provider,
          binding: {
            ...original.provider.binding,
            ...(mode === "copied" ? { threadId: "thread:copied-witness" } : {}),
            ...(mode === "generation" ? { runtimeGeneration: null } : {}),
            ...(mode === "native_id" ? { nativeThreadId: null } : {}),
            ...(mode === "driver" ? { driver: "not-a-driver" } : {}),
            ...(mode === "driver_carrier" ? { driver: 42 } : {}),
          },
        },
      };
      yield* f.sql`INSERT INTO orchestration_v2_native_command_identities
      (command_id, kind, version, command_type, aggregate_kind, aggregate_id, normalized_command_digest, binding_digest)
      VALUES (${commandId}, 'workstream_settlement', 2, 'thread.settle', 'thread', ${f.threadId},
        ${mode === "digest" ? "c".repeat(64) : nativeCreationSha256(nativeCreationCanonicalJson(command))},
        ${nativeWorkstreamSettlementWitnessBindingDigestV2(qualifiedCodec)})`;
      yield* f.sql`INSERT INTO orchestration_v2_workstream_settlement_witnesses
      (command_id, thread_id, witness_json, recorded_at)
      VALUES (${commandId}, ${f.threadId}, ${nativeCreationCanonicalJson(witness)}, '2026-10-05T00:00:00.000Z')`;
      const before = yield* f.sql`SELECT * FROM orchestration_v2_workstream_settlement_witnesses`;
      const result = yield* f.read({ threadId: f.threadId, commandId }).pipe(Effect.result);
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure")
        assert.equal(
          result.failure._tag,
          mode === "driver_carrier" || mode === "extra"
            ? "EventSinkWriteError"
            : "NativeCommandPreconditionError",
        );
      assert.deepEqual(
        yield* f.sql`SELECT * FROM orchestration_v2_workstream_settlement_witnesses`,
        before,
      );
    }).pipe(Effect.provide(Layer.fresh(persistence))),
);
