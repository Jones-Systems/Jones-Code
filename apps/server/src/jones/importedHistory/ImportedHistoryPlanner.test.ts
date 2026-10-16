import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentAuthenticatedPrincipal,
  EventId,
  RunId,
  MessageId,
  NodeId,
  ProviderDriverKind,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as ServerConfig from "../../config.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import type { ProviderAdapterV2Shape } from "@t3tools/provider-core/server/ProviderAdapter";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as ProviderReplayHarness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import {
  principal,
  threadId,
  runId,
  providerInstanceId,
  providerThreadId,
  now,
  delivery,
  run,
  setup,
} from "./ImportedHistoryFixture.testkit.ts";

const ownedConfig = ServerConfig.layerTest(process.cwd(), {
  prefix: "jones-imported-planner-",
}).pipe(Layer.provide(NodeServices.layer), Layer.orDie);
const adapter: ProviderAdapterV2Shape = {
  instanceId: providerInstanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
  openSession: () =>
    Effect.die("Physical provider invocation is prohibited in imported planner fixture"),
};
const planner = ProviderReplayHarness.layerWithRegistry(
  { name: "jones-imported-planner", runtimePolicyOverride: { cwd: "/fixture" } },
  ProviderAdapterRegistry.layerFromAdapters([adapter]),
  {
    databaseLayer: SqlitePersistence.layerMemory,
    serverConfigLayer: ownedConfig,
    runEffectWorker: false,
  },
);
const receiving = Layer.mergeAll(
  ownedConfig,
  SqlitePersistence.layerMemory,
  planner,
  ProjectionStore.layer.pipe(Layer.provide(SqlitePersistence.layerMemory)),
);

it.effect.each(["start", "invalid reviewed target"] as const)(
  "reviewed held queue head retains exact identity ahead of newer eligible work: %s",
  (scenario) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      let scratch: string | undefined;
      yield* Effect.gen(function* () {
        scratch = (yield* ServerConfig.ServerConfig).baseDir;
        yield* setup;
        const sink = yield* EventSink.EventSinkV2;
        yield* sink.write({
          events: [
            {
              id: EventId.make("event:planner:node"),
              type: "node.updated",
              threadId,
              runId,
              occurredAt: now,
              payload: {
                id: run.rootNodeId,
                threadId,
                runId,
                parentNodeId: null,
                rootNodeId: run.rootNodeId,
                kind: "root_turn",
                status: "pending",
                countsForRun: true,
                providerThreadId,
                providerTurnId: null,
                nativeItemRef: null,
                runtimeRequestId: null,
                checkpointScopeId: null,
                startedAt: null,
                completedAt: null,
              },
            },
            {
              id: EventId.make("event:planner:attempt"),
              type: "run-attempt.created",
              threadId,
              runId,
              occurredAt: now,
              payload: {
                id: run.activeAttemptId,
                runId,
                attemptOrdinal: 1,
                rootNodeId: run.rootNodeId,
                providerInstanceId,
                providerThreadId,
                providerTurnId: null,
                reason: "initial",
                status: "pending",
                startedAt: null,
                completedAt: null,
              },
            },
          ],
        });
        const eligibleId = RunId.make("run:planner:newer-eligible");
        const eligibleMessageId = MessageId.make("message:planner:newer-eligible");
        yield* sink.write({
          events: [
            {
              id: EventId.make("event:planner:newer-run"),
              type: "run.created",
              threadId,
              runId: eligibleId,
              occurredAt: now,
              payload: {
                ...run,
                id: eligibleId,
                ordinal: 2,
                queueHeld: false,
                userMessageId: eligibleMessageId,
              },
            },
            {
              id: EventId.make("event:planner:newer-message"),
              type: "message.updated",
              threadId,
              runId: eligibleId,
              occurredAt: now,
              payload: {
                id: eligibleMessageId,
                threadId,
                runId: eligibleId,
                nodeId: null,
                role: "user",
                text: "Newer eligible work",
                attachments: [],
                streaming: false,
                createdBy: "user",
                creationSource: "web",
                createdAt: now,
                updatedAt: now,
              },
            },
            ...(scenario === "start"
              ? []
              : [
                  {
                    id: EventId.make("event:planner:invalid-reviewed"),
                    type: "run.updated" as const,
                    threadId,
                    runId,
                    occurredAt: now,
                    payload: {
                      ...run,
                      rootNodeId: NodeId.make("node:planner:missing-reviewed-root"),
                    },
                  },
                ]),
          ],
        });
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        assert.isDefined(orchestrator.reviewImportedHistory);
        assert.isDefined(orchestrator.startWithImportedHistory);
        const review = yield* orchestrator.reviewImportedHistory!({ threadId, delivery }).pipe(
          Effect.provideService(EnvironmentAuthenticatedPrincipal, principal),
        );
        assert.strictEqual(review.status, "available");
        const commandId = CommandId.make("command:planner:reviewed");
        const command = {
          type: "thread.imported-history.start" as const,
          commandId,
          threadId,
          delivery,
          reviewedBasis: review.reviewedBasis ?? "unavailable",
        };
        if (scenario === "invalid reviewed target") {
          const failure = yield* orchestrator.startWithImportedHistory!(command).pipe(
            Effect.provideService(EnvironmentAuthenticatedPrincipal, principal),
            Effect.flip,
          );
          assert.equal(failure._tag, "OrchestratorProjectionError");
          const records = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadRecords(
            threadId,
            ["runs"],
          );
          assert.equal(records.runs.find((candidate) => candidate.id === runId)?.status, "queued");
          assert.isTrue(records.runs.find((candidate) => candidate.id === runId)?.queueHeld);
          assert.equal(
            records.runs.find((candidate) => candidate.id === eligibleId)?.status,
            "queued",
          );
          const sql = yield* SqlClient.SqlClient;
          assert.isEmpty(
            yield* sql`SELECT * FROM orchestration_v2_effect_outbox WHERE command_id=${commandId}`,
          );
          return;
        }
        const accepted = yield* orchestrator.startWithImportedHistory!(command).pipe(
          Effect.provideService(EnvironmentAuthenticatedPrincipal, principal),
        );
        assert.strictEqual(accepted.status, "accepted");
        assert.strictEqual(accepted.runId, runId);
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const records = yield* projections.getThreadRecords(threadId, [
          "runs",
          "contextHandoffs",
          "providerThreads",
        ]);
        const started = records.runs.find((candidate) => candidate.id === runId);
        assert.strictEqual(started?.status, "starting");
        assert.strictEqual(
          records.runs.find((candidate) => candidate.id === eligibleId)?.status,
          "queued",
        );
        assert.strictEqual(started?.queueHeld, false);
        assert.isNotNull(started?.contextHandoffId);
        const handoff = records.contextHandoffs.find(
          (candidate) => candidate.id === started?.contextHandoffId,
        );
        assert.isDefined(handoff);
        assert.include(handoff?.summaryText ?? "", "Imported transcript");
        assert.isNull(
          records.providerThreads.find((candidate) => candidate.id === providerThreadId)
            ?.nativeThreadRef,
        );
        const sql = yield* SqlClient.SqlClient;
        const receipts =
          yield* sql`SELECT * FROM orchestration_command_receipts WHERE command_id=${commandId} AND status='accepted'`;
        const outcomes =
          yield* sql`SELECT * FROM jones_imported_history_outcomes WHERE command_id=${commandId}`;
        const reservations =
          yield* sql`SELECT * FROM jones_imported_history_start_reservations WHERE command_id=${commandId} AND run_id=${runId}`;
        const effects =
          yield* sql`SELECT * FROM orchestration_v2_effect_outbox WHERE command_id=${commandId}`;
        assert.strictEqual(receipts.length, 1);
        assert.strictEqual(outcomes.length, 1);
        assert.strictEqual(reservations.length, 1);
        assert.strictEqual(effects.length, 1);
        assert.deepEqual(yield* sql`SELECT * FROM jones_imported_history_execution_starts`, []);
        const duplicate = yield* orchestrator.startWithImportedHistory!(command).pipe(
          Effect.provideService(EnvironmentAuthenticatedPrincipal, principal),
        );
        assert.deepEqual(duplicate, accepted);
        assert.strictEqual(
          (yield* sql`SELECT * FROM orchestration_v2_effect_outbox WHERE command_id=${commandId}`)
            .length,
          1,
        );
      }).pipe(Effect.provide(receiving));
      assert.isDefined(scratch);
      assert.isFalse(yield* fs.exists(scratch!));
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect.each(["failure", "cancellation"] as const)(
  "owned planner config scratch closes on %s",
  (outcome) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      let scratch: string | undefined;
      const exited = yield* Effect.gen(function* () {
        scratch = (yield* ServerConfig.ServerConfig).baseDir;
        assert.isTrue(yield* fs.exists(scratch));
        return yield* outcome === "failure" ? Effect.fail("fixture failure") : Effect.interrupt;
      }).pipe(Effect.provide(ownedConfig), Effect.exit);
      assert.strictEqual(exited._tag, "Failure");
      assert.isDefined(scratch);
      assert.isFalse(yield* fs.exists(scratch!));
    }).pipe(Effect.provide(NodeServices.layer)),
);
