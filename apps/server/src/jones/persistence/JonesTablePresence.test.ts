import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";

import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import { migrationManifest, runMigrations } from "../../persistence/Migrations.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import { ImportedHistoryChoiceError } from "../importedHistory/ImportedHistoryChoice.ts";
import { runJonesMigrations } from "./JonesMigrationGuard.ts";
import { jonesMigrationEntries } from "./JonesMigrations.ts";
import { makeJonesTablePresence } from "./JonesTablePresence.ts";

const threadId = ThreadId.make("thread:presence");
const digest = "0".repeat(64);

const recording = () => {
  const queries: Array<string> = [];
  const record: Statement.Transformer = (statement) =>
    Effect.sync(() => {
      queries.push(statement.compile()[0]);
      return statement;
    });
  return { queries, record };
};
const schemaReads = (queries: ReadonlyArray<string>) =>
  queries.filter((query) => query.includes("sqlite_master")).length;

const reserveChoice = (commandId: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO jones_imported_history_choices(command_id,thread_id,actor_session_id,actor_digest,command_digest,delivery_digest,reviewed_basis,command_json,basis_json)
      VALUES(${commandId},${threadId},'actor:presence',${digest},${digest},${digest},${digest},'{}','{}')`;
  });

const commit = (commandId: string) =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const now = yield* DateTime.now;
    const id = ThreadId.make(`thread:${commandId}`);
    return yield* sink.commitCommand({
      commandId: CommandId.make(commandId),
      threadId: id,
      commandType: "thread.create",
      acceptedAt: now,
      events: [
        {
          id: EventId.make(`event:${commandId}`),
          type: "thread.created",
          threadId: id,
          occurredAt: now,
          payload: {
            id,
            projectId: ProjectId.make("project:presence"),
            title: "Synthetic presence",
            providerInstanceId: ProviderInstanceId.make("codex"),
            modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "synthetic" },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            activeProviderThreadId: null,
            lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
            forkedFrom: null,
            createdAt: now,
            updatedAt: now,
            archivedAt: null,
            settledOverride: null,
            settledAt: null,
            lastVisitedAt: null,
            deletedAt: null,
            createdBy: "user",
            creationSource: "web",
          },
        },
      ],
      effects: [],
    });
  });

// The reserved-choice gate rejects with this exact code inside the sink error.
const rejectedAsReserved = (commandId: string) =>
  commit(commandId).pipe(
    Effect.flip,
    Effect.map((error) => {
      assert.instanceOf(error.cause, ImportedHistoryChoiceError);
      assert.strictEqual(error.cause.code, "private_imported_lineage_required");
    }),
  );

const sinkOn = <E, R>(database: Layer.Layer<SqlClient.SqlClient, E, R>) =>
  EventSink.layer.pipe(
    Layer.provideMerge(Layer.merge(EventStore.layer, ProjectionStore.layer)),
    Layer.provideMerge(database),
  );
const productionSink = sinkOn(SqlitePersistence.layerMemory);
const upstreamOnly = Layer.effectDiscard(
  runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] }),
).pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })));
const upstreamOnlySink = sinkOn(upstreamOnly);

it.effect("presence remembers a found table per client and rereads an absent one", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const { queries, record } = recording();
    const presence = makeJonesTablePresence(sql);
    const check = presence("jones_presence_probe").pipe(
      Effect.provideService(Statement.CurrentTransformer, record),
    );
    assert.isFalse(yield* check);
    assert.isFalse(yield* check);
    assert.strictEqual(schemaReads(queries), 2);
    yield* sql`CREATE TABLE jones_presence_probe(id TEXT PRIMARY KEY)`;
    assert.isTrue(yield* check);
    assert.isTrue(yield* check);
    assert.isTrue(yield* check);
    assert.strictEqual(schemaReads(queries), 3);
    // Another client, or another service built on this one, reads for itself.
    assert.isTrue(
      yield* makeJonesTablePresence(sql)("jones_presence_probe").pipe(
        Effect.provideService(Statement.CurrentTransformer, record),
      ),
    );
    assert.strictEqual(schemaReads(queries), 4);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect(
  "production layer enforces reservations from the first command, then reads no schema per command or start",
  () =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      yield* reserveChoice("command:presence:reserved");
      yield* rejectedAsReserved("command:presence:reserved");
      yield* commit("command:presence:first");
      yield* sink.assertRuntimeStopStartAllowed!({ threadId, runId: RunId.make("run:first") });
      yield* sink.assertImportedHistoryStartAllowed!({
        threadId,
        runId: RunId.make("run:first"),
        providerThreadId: "provider-thread:presence",
      });

      const { queries, record } = recording();
      yield* Effect.gen(function* () {
        yield* commit("command:presence:second");
        yield* sink.assertRuntimeStopStartAllowed!({
          threadId,
          runId: RunId.make("run:second"),
          providerThreadId: "provider-thread:presence",
          runtimeGeneration: "generation:presence",
        });
        yield* sink.assertImportedHistoryStartAllowed!({
          threadId,
          runId: RunId.make("run:second"),
          providerThreadId: "provider-thread:presence",
        });
        yield* rejectedAsReserved("command:presence:reserved");
      }).pipe(Effect.provideService(Statement.CurrentTransformer, record));
      assert.strictEqual(schemaReads(queries), 0);
      // Each bounded lookup still runs on its own key.
      for (const lookup of [
        "FROM jones_imported_history_choices WHERE command_id",
        "FROM jones_runtime_stop_fences WHERE thread_id",
        "FROM jones_runtime_stop_intents WHERE thread_id",
        "FROM jones_imported_history_start_reservations WHERE thread_id",
      ])
        assert.isTrue(
          queries.some((query) => query.includes(lookup)),
          lookup,
        );
    }).pipe(Effect.provide(productionSink)),
);

it.effect(
  "upstream-only harness commits ordinary commands and observes Jones schema created later",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const sink = yield* EventSink.EventSinkV2;
      const accepted = yield* commit("command:upstream:ordinary");
      assert.isTrue(accepted.committed);
      yield* sink.assertRuntimeStopStartAllowed!({
        threadId,
        runId: RunId.make("run:upstream"),
        providerThreadId: "provider-thread:upstream",
      });
      yield* sink.assertImportedHistoryStartAllowed!({
        threadId,
        runId: RunId.make("run:upstream"),
        providerThreadId: "provider-thread:upstream",
      });
      assert.deepEqual(yield* sql`SELECT name FROM sqlite_master WHERE name LIKE 'jones_%'`, []);

      // The sink was built before this schema existed; it must not keep a negative answer.
      yield* runJonesMigrations(jonesMigrationEntries);
      yield* reserveChoice("command:upstream:reserved");
      yield* rejectedAsReserved("command:upstream:reserved");
      assert.deepEqual(
        yield* sql`SELECT command_id FROM orchestration_command_receipts ORDER BY command_id`,
        [{ command_id: "command:upstream:ordinary" }],
      );
    }).pipe(Effect.provide(upstreamOnlySink)),
);
