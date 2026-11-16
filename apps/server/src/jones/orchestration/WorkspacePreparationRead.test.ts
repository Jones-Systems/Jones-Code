import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { SqliteReaderAdmissionError } from "@t3tools/shared/jones/sqliteReaders/admissionFailure";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import { vi } from "vite-plus/test";

import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ServerConfig from "../../config.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import { retryWorkspacePreparationRead } from "./WorkspacePreparationRead.ts";

const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "synthetic" };
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider process needed for preparation reads"),
} as ProviderAdapter.ProviderAdapterV2["Service"];
const database = SqlitePersistence.layerMemory;
const layerTest = Layer.mergeAll(
  ProjectionStore.layer.pipe(Layer.provide(database)),
  EffectOutbox.layer.pipe(Layer.provide(database)),
  ProviderReplayHarness.layerWithRegistry(
    { name: "workspace-preparation-read" },
    ProviderAdapterRegistry.layerFromAdapters([adapter]),
    {
      databaseLayer: database,
      runEffectWorker: false,
      serverConfigLayer: ServerConfig.layerTest(process.cwd(), {
        prefix: "workspace-preparation-read-",
      }).pipe(Layer.provide(NodeServices.layer), Layer.orDie),
    },
  ),
);

it.effect("does not retry admission failures for unrelated commands", () =>
  Effect.gen(function* () {
    const failure = new ProjectionStore.ProjectionStoreReadError({
      threadId: ThreadId.make("thread:preparation-read-bounds"),
      cause: new SqliteReaderAdmissionError({ reason: "queue-timeout" }),
    });
    let attempts = 0;
    const result = yield* Effect.suspend(() => {
      attempts++;
      return Effect.fail(failure);
    }).pipe(retryWorkspacePreparationRead("thread.metadata.update"), Effect.result);
    assert.equal(attempts, 1);
    assert.isTrue(Result.isFailure(result));
    if (Result.isFailure(result)) assert.strictEqual(result.failure, failure);
  }),
);

it.effect.each([
  { scenario: "transient congestion", failures: 1, admission: true, attempts: 2 },
  { scenario: "persistent congestion", failures: Infinity, admission: true, attempts: 3 },
  { scenario: "permanent read failure", failures: Infinity, admission: false, attempts: 1 },
])("handles $scenario before releasing a prepared run", (test) =>
  Effect.gen(function* () {
    const engine = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const threadId = ThreadId.make("thread:preparation-read");
    yield* engine.dispatch({
      type: "thread.create",
      commandId: CommandId.make("create:preparation-read"),
      threadId,
      projectId: ProjectId.make("project:preparation-read"),
      title: "Preparation read",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: "task/preparation-read",
      worktreePath: "/synthetic/worktree",
      createdBy: "user",
      creationSource: "web",
    });
    yield* engine.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("message:preparation-read"),
      threadId,
      messageId: MessageId.make("message:preparation-read"),
      text: "Continue on this branch",
      attachments: [],
      dispatchMode: { type: "defer_start" },
      createdBy: "user",
      creationSource: "web",
    });
    const run = (yield* projections.getThreadProjection(threadId)).runs[0]!;
    const read = projections.getThreadRecords;
    const failure = new ProjectionStore.ProjectionStoreReadError({
      threadId,
      cause: test.admission
        ? new SqliteReaderAdmissionError({ reason: "queue-timeout" })
        : new Error("projection decode failed"),
    });
    let attempts = 0;
    const spy = vi.spyOn(projections, "getThreadRecords").mockImplementation((id, fields, filter) =>
      Effect.suspend(() => {
        if (id === threadId && filter?.turnItemRunId === run.id && ++attempts <= test.failures) {
          return Effect.fail(failure);
        }
        return read(id, fields, filter);
      }),
    );
    const command = {
      type: "prepared-run.release" as const,
      commandId: CommandId.make("release:preparation-read"),
      threadId,
      runId: run.id,
    };
    const result = yield* engine
      .dispatch(command)
      .pipe(Effect.result, Effect.ensuring(Effect.sync(() => spy.mockRestore())));
    assert.equal(attempts, test.attempts);
    if (test.failures === Infinity) {
      assert.isTrue(Result.isFailure(result));
      if (Result.isFailure(result)) {
        assert.equal(result.failure._tag, "OrchestratorProjectionError");
        if (result.failure._tag === "OrchestratorProjectionError")
          assert.strictEqual(result.failure.cause, failure);
      }
      assert.deepEqual(yield* outbox.listByCommandId(command.commandId), []);
      assert.equal((yield* projections.getThreadProjection(threadId)).runs[0]?.status, "preparing");
      return;
    }
    assert.isTrue(Result.isSuccess(result));
    yield* engine.dispatch(command);
    assert.equal((yield* projections.getThreadProjection(threadId)).runs[0]?.status, "starting");
    assert.deepEqual(
      (yield* outbox.listByCommandId(command.commandId)).map((effect) => effect.request),
      [{ type: "provider-turn.start", runId: run.id }],
    );
  }).pipe(Effect.provide(layerTest)),
);
