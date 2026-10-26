import * as NodeURL from "node:url";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  CodexSettings,
  CommandId,
  MessageId,
  type ModelSelection,
  type OrchestrationV2ThreadProjection,
  ProjectId,
  ProviderInstanceId,
  type ServerProvider,
  ThreadId,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as CodexSchema from "effect-codex-app-server/schema";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as ServerConfig from "../../config.ts";
import * as Codex from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Worker from "../../orchestration-v2/EffectWorker.ts";
import * as Sink from "../../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Adapters from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Harness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Catalog from "../../provider/ProviderRegistry.ts";
import { resolveCodexHomeLayout } from "../../provider/Drivers/CodexHomeLayout.ts";
import * as Settings from "../../serverSettings.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as Evidence from "./CodexFailoverEvidence.ts";
import * as SqlClient from "effect/sql/SqlClient";
import * as ControlAdmission from "../orchestration/ControlAdmission.ts";
import * as ProcessAttribution from "../resourceTelemetry/ProcessAttribution.ts";

const accounts = ["A", "B", "C", "D", "E"] as const;
const instanceId = (account: string) => ProviderInstanceId.make(`synthetic-codex-${account}`);
const initialSelection: ModelSelection = {
  instanceId: instanceId("A"),
  model: "synthetic-codex-model",
  options: [
    { id: "reasoningEffort", value: "high" },
    { id: "serviceTier", value: "priority" },
  ],
};
const peerPath = NodeURL.fileURLToPath(
  new URL("../../../../../scripts/jones/e2e/codex-failover/peer.mjs", import.meta.url),
);
const Trace = Schema.Struct({
  synthetic: Schema.Literal(true),
  processId: Schema.String,
  instance: Schema.String,
  sequence: Schema.Number,
  method: Schema.String,
  nativeThreadId: Schema.NullOr(Schema.String),
  nativeTurnId: Schema.NullOr(Schema.String),
  model: Schema.optional(Schema.NullOr(Schema.String)),
  effort: Schema.optional(Schema.NullOr(Schema.String)),
  serviceTier: Schema.optional(Schema.NullOr(Schema.String)),
  inputMarker: Schema.optional(Schema.String),
  status: Schema.optional(Schema.String),
});
const decodeSettings = Schema.decodeEffect(CodexSettings);
const decodeTrace = Schema.decodeUnknownEffect(Schema.fromJsonString(Trace));
const decodeHistory = Schema.decodeUnknownEffect(
  Schema.fromJsonString(CodexSchema.V2ThreadReadResponse__Thread),
);

const catalogs: ReadonlyArray<ServerProvider> = accounts.map((account): ServerProvider => ({
  instanceId: instanceId(account),
  driver: Codex.CODEX_DRIVER_KIND,
  enabled: true,
  installed: true,
  version: "synthetic",
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: "2026-10-26T00:00:00.000Z",
  models: [
    {
      slug: initialSelection.model,
      name: "Synthetic Codex",
      isCustom: true,
      capabilities: {
        optionDescriptors: [
          {
            id: "reasoningEffort",
            label: "Effort",
            type: "select",
            options: [{ id: "high", label: "High" }],
          },
          {
            id: "serviceTier",
            label: "Tier",
            type: "select",
            options: [{ id: "priority", label: "Priority" }],
          },
        ],
      },
    },
  ],
  slashCommands: [],
  skills: [],
}));

const isIdle = (projection: OrchestrationV2ThreadProjection) =>
  projection.runs.every(
    (run) => !["queued", "starting", "running", "waiting"].includes(run.status),
  );

const awaitMilestone = Effect.fn("CodexFailoverTest.awaitMilestone")(function* (
  threadId: ThreadId,
  afterSequence: number,
  predicate: (projection: OrchestrationV2ThreadProjection) => boolean,
) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const sink = yield* Sink.EventSinkV2;
  const worker = yield* Worker.OrchestrationEffectWorkerV2;
  const read = worker.drain(100).pipe(Effect.andThen(orchestrator.getThreadProjection(threadId)));
  const current = yield* read;
  if (predicate(current)) return current;
  const [reached] = yield* sink.stream({ threadId, afterSequence }).pipe(
    Stream.mapEffect(() => read),
    Stream.filter(predicate),
    Stream.take(1),
    Stream.runCollect,
    Effect.timeout("10 seconds"),
    Effect.catch((error) =>
      Effect.gen(function* () {
        const projection = yield* orchestrator.getThreadProjection(threadId);
        const sql = yield* SqlClient.SqlClient;
        const decisions =
          yield* sql`SELECT e.run_id, e.provider_thread_id, e.provider_instance_id, d.state, d.decision FROM jones_codex_failover_evidence e LEFT JOIN jones_codex_failover_decisions d USING(provider_turn_id) WHERE e.thread_id = ${threadId}`;
        return yield* Effect.die(
          new Error(
            `Automatic Codex failover milestone missing: ${JSON.stringify({
              runs: projection.runs.map(({ ordinal, status, modelSelection }) => ({
                ordinal,
                status,
                modelSelection,
              })),
              providerTurns: projection.providerTurns.map(({ status }) => status),
              failures: projection.turnItems.flatMap((item) =>
                item.type === "error" ? [item.failure] : [],
              ),
              decisions,
            })}`,
            { cause: error },
          ),
        );
      }),
    ),
  );
  assert.isDefined(reached);
  yield* worker.drain(100);
  return yield* orchestrator.getThreadProjection(threadId);
});

const scenario = (mode: "chain" | "all-unavailable") =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const config = yield* Effect.acquireRelease(
      Harness.makeReplayServerConfig(`codex-failover-${mode}`),
      (value) =>
        fs.remove(value.baseDir, { recursive: true }).pipe(
          Effect.andThen(fs.exists(value.baseDir)),
          Effect.tap((exists) => Effect.sync(() => assert.isFalse(exists))),
          Effect.orDie,
        ),
    );
    const stateDir = path.join(config.baseDir, "peer-state");
    const homePath = path.join(config.baseDir, "codex-home");
    const cwd = path.join(config.baseDir, "workspace");
    for (const directory of [stateDir, homePath, cwd]) yield* fs.makeDirectory(directory);
    const realNode = yield* fs.realPath(process.execPath);
    const delimiter = (yield* HostProcess.Platform) === "win32" ? ";" : ":";
    const environment = {
      HOME: homePath,
      PATH: `${path.dirname(realNode)}${delimiter}/usr/bin${delimiter}/bin`,
    };
    const entries = yield* Effect.forEach(accounts, (account) =>
      decodeSettings({
        binaryPath: peerPath,
        homePath,
        launchArgs: `--state-dir ${JSON.stringify(stateDir)} --account ${account} --mode ${mode}${mode === "chain" && account === "A" ? " --hold" : ""}`,
      }).pipe(Effect.map((config) => ({ account, config }))),
    );
    const clientFactory = yield* Codex.CodexAppServerClientFactory.pipe(
      Effect.provide(Codex.layerAppServerClientFactory),
    );
    const adapterDependencies = yield* Effect.context<
      | Crypto.Crypto
      | FileSystem.FileSystem
      | Path.Path
      | IdAllocator.IdAllocatorV2
      | ControlAdmission.ControlAdmission
    >();
    const database = SqlitePersistence.layerMemory;
    const registry = Layer.effect(
      Adapters.ProviderAdapterRegistryV2,
      Effect.gen(function* () {
        const adapters = yield* Effect.forEach(entries, ({ account, config: settings }) =>
          Effect.gen(function* () {
            const adapter = yield* Codex.createCodexAdapterV2({
              instanceId: instanceId(account),
              displayName: undefined,
              enabled: true,
              environment: [],
              config: settings,
            });
            const layout = yield* resolveCodexHomeLayout(settings);
            return { adapter, continuationKey: layout.continuationKey };
          }),
        );
        assert.equal(new Set(adapters.map(({ continuationKey }) => continuationKey)).size, 1);
        const find = (
          id: ProviderInstanceId,
        ): Effect.Effect<
          (typeof adapters)[number],
          Adapters.ProviderAdapterRegistryLookupError
        > => {
          const entry = adapters.find(({ adapter }) => adapter.instanceId === id);
          return entry === undefined
            ? Effect.fail(new Adapters.ProviderAdapterRegistryLookupError({ instanceId: id }))
            : Effect.succeed(entry);
        };
        return Adapters.ProviderAdapterRegistryV2.of({
          get: (id) => find(id).pipe(Effect.map(({ adapter }) => adapter)),
          list: () => Effect.succeed(adapters.map(({ adapter }) => adapter.instanceId)),
          getMetadata: (id) =>
            find(id).pipe(
              Effect.map(({ adapter, continuationKey }) => ({
                driver: adapter.driver,
                continuationKey,
                enabled: true,
                capabilities: Codex.CodexProviderCapabilitiesV2,
              })),
            ),
        });
      }),
    ).pipe(
      Layer.provide(Layer.provide(Evidence.layer, database)),
      Layer.provide(Layer.succeed(Codex.CodexAppServerClientFactory, clientFactory)),
      Layer.provide(Layer.succeedContext(adapterDependencies)),
      Layer.provide(Layer.succeed(ServerConfig.ServerConfig, { ...config, cwd })),
      Layer.provide(Layer.succeed(HostProcess.Environment, environment)),
      Layer.provide(Layer.succeed(HostProcess.HomeDirectory, homePath)),
    );
    const runtime = Harness.layerWithRegistryAndServices(
      { name: `codex-failover-${mode}`, runtimePolicyOverride: { cwd } },
      registry,
      {
        serverConfigLayer: Layer.succeed(ServerConfig.ServerConfig, { ...config, cwd }),
        databaseLayer: database,
        runEffectWorker: false,
        runContinuationWorker: true,
      },
    ).pipe(
      Layer.provide(
        Layer.mock(Catalog.ProviderRegistry)({ getProviders: Effect.succeed(catalogs) }),
      ),
    );
    const attribution = yield* ProcessAttribution.ProcessAttribution;
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* Sink.EventSinkV2;
      const worker = yield* Worker.OrchestrationEffectWorkerV2;
      const settings = yield* Settings.ServerSettingsService;
      const defaults = yield* settings.getSettings;
      for (const id of Object.keys(defaults.providerInstances)) {
        yield* settings.updateProviderInstance({
          operation: "remove",
          instanceId: ProviderInstanceId.make(id),
        });
      }
      for (const { account, config } of entries) {
        yield* settings.updateProviderInstance({
          operation: "create",
          instanceId: instanceId(account),
          instance: { driver: Codex.CODEX_DRIVER_KIND, enabled: true, config },
        });
      }
      assert.deepEqual(
        Object.keys((yield* settings.getSettings).providerInstances).sort(),
        accounts.map(instanceId),
      );
      const threadId = ThreadId.make(`synthetic-failover-${mode}`);
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("synthetic:create"),
        threadId,
        projectId: ProjectId.make("synthetic-project"),
        title: "Synthetic Codex automatic failover",
        modelSelection: initialSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: cwd,
        createdBy: "user",
        creationSource: "web",
      });
      const dispatch = (marker: string, modelSelection?: ModelSelection, queued = false) =>
        orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(marker),
          threadId,
          messageId: MessageId.make(marker),
          text: marker,
          ...(modelSelection === undefined ? {} : { modelSelection, modelSelectionExplicit: true }),
          attachments: [],
          createdBy: "user",
          creationSource: "web",
          dispatchMode: { type: queued ? "queue_after_active" : "start_immediately" },
        });
      const awaitHeldA = Effect.fn("CodexFailoverTest.awaitHeldA")(function* (
        afterSequence: number,
        turnCount: number,
      ) {
        const held = yield* awaitMilestone(
          threadId,
          afterSequence,
          (projection) =>
            projection.providerTurns.length >= turnCount &&
            projection.providerTurns.some(
              (turn) => turn.status === "running" && turn.nativeTurnRef?.nativeId != null,
            ),
        );
        const turn = held.providerTurns.find(
          (turn) => turn.status === "running" && turn.nativeTurnRef?.nativeId != null,
        )!;
        assert.equal(
          held.providerThreads.find((row) => row.id === held.thread.activeProviderThreadId)
            ?.providerInstanceId,
          instanceId("A"),
        );
        assert.isString(turn.nativeTurnRef?.nativeId);
        return { held, turn };
      });
      const afterSequence = yield* sink.latestSequence();
      yield* dispatch("SYNTHETIC_ORIGINAL");
      let queuedProviderThreadId: OrchestrationV2ThreadProjection["thread"]["activeProviderThreadId"] =
        null;
      if (mode === "chain") {
        const { held, turn } = yield* awaitHeldA(afterSequence, 1);
        queuedProviderThreadId = held.thread.activeProviderThreadId;
        yield* dispatch("SYNTHETIC_QUEUED_INHERITED", undefined, true);
        const queued = yield* orchestrator.getThreadProjection(threadId);
        const follower = queued.runs.find(
          (run) => run.userMessageId === "SYNTHETIC_QUEUED_INHERITED",
        )!;
        assert.equal(follower.status, "queued");
        assert.equal(follower.providerThreadId, queuedProviderThreadId);
        yield* fs.writeFileString(
          path.join(stateDir, `release-${turn.nativeTurnRef!.nativeId}`),
          "SYNTHETIC_RELEASE\n",
        );
      }
      const expected = mode === "chain" ? ["A", "B", "C"] : [...accounts];
      const first = yield* awaitMilestone(
        threadId,
        afterSequence,
        (projection) =>
          projection.providerTurns.length >= expected.length + (mode === "chain" ? 1 : 0) &&
          isIdle(projection),
      );
      assert.lengthOf(first.providerTurns, expected.length + (mode === "chain" ? 1 : 0));
      assert.equal(first.runs.at(-1)?.status, mode === "chain" ? "completed" : "failed");
      const providerThread = first.providerThreads.find(
        ({ id }) => id === first.thread.activeProviderThreadId,
      );
      assert.isDefined(providerThread?.nativeThreadRef);
      const nativeRef = providerThread!.nativeThreadRef!;
      assert.deepEqual(
        new Set(first.providerThreads.map(({ nativeThreadRef }) => nativeThreadRef?.nativeId)),
        new Set([nativeRef.nativeId]),
      );
      const historyPath = path.join(stateDir, `thread-${nativeRef.nativeId}.json`);
      const before = yield* fs.readFileString(historyPath).pipe(Effect.flatMap(decodeHistory));
      assert.equal(before.cwd, cwd);
      assert.deepEqual(
        before.turns.map(({ status }) => status),
        mode === "chain"
          ? ["failed", "failed", "completed", "completed"]
          : accounts.map(() => "failed"),
      );
      if (mode === "chain") {
        const follower = first.runs.find(
          (run) => run.userMessageId === "SYNTHETIC_QUEUED_INHERITED",
        )!;
        assert.equal(follower.status, "completed");
        assert.deepEqual(follower.modelSelection, {
          ...initialSelection,
          instanceId: instanceId("C"),
        });
        assert.notEqual(follower.providerThreadId, queuedProviderThreadId);
        const chainRuns = first.runs.filter(
          (run) => run.userMessageId !== "SYNTHETIC_QUEUED_INHERITED",
        );
        assert.equal(new Set(chainRuns.map((run) => run.providerThreadId)).size, 3);
        const sql = yield* SqlClient.SqlClient;
        const acceptances = yield* sql<{
          status: string;
        }>`SELECT r.status FROM jones_codex_failover_steps s JOIN orchestration_command_receipts r ON r.command_id = s.continue_command_id WHERE s.thread_id = ${threadId}`;
        assert.deepEqual(
          acceptances.map((row) => row.status),
          ["accepted", "accepted"],
        );
        const saved = { ...initialSelection, instanceId: instanceId("C") };
        assert.deepEqual(first.thread.modelSelection, saved);
        assert.equal(first.thread.providerInstanceId, saved.instanceId);
        const nextSequence = yield* sink.latestSequence();
        yield* dispatch("SYNTHETIC_FRESH_INHERITED");
        const next = yield* awaitMilestone(
          threadId,
          nextSequence,
          (projection) => projection.providerTurns.length >= 5 && isIdle(projection),
        );
        assert.lengthOf(next.providerTurns, 5);
        assert.equal(next.runs.at(-1)?.status, "completed");
        assert.deepEqual(next.runs.at(-1)?.modelSelection, saved);
        assert.deepEqual(next.thread.modelSelection, saved);
        assert.deepEqual(
          next.providerThreads.find(({ id }) => id === next.thread.activeProviderThreadId)
            ?.nativeThreadRef,
          nativeRef,
        );
        const returnSequence = yield* sink.latestSequence();
        yield* dispatch("SYNTHETIC_EXPLICIT_RETURN", initialSelection);
        const { turn } = yield* awaitHeldA(returnSequence, 6);
        yield* fs.writeFileString(
          path.join(stateDir, `release-${turn.nativeTurnRef!.nativeId}`),
          "SYNTHETIC_RELEASE\n",
        );
        const returned = yield* awaitMilestone(
          threadId,
          returnSequence,
          (projection) => projection.providerTurns.length >= 8 && isIdle(projection),
        );
        assert.lengthOf(returned.providerTurns, 8);
        assert.equal(returned.runs.at(-1)?.status, "completed");
        assert.deepEqual(returned.thread.modelSelection, saved);
      }
      yield* worker.drain(100);
      const history = yield* fs.readFileString(historyPath).pipe(Effect.flatMap(decodeHistory));
      assert.equal(history.id, nativeRef.nativeId);
      assert.equal(history.cwd, cwd);
      assert.deepEqual(history.turns.slice(0, before.turns.length), before.turns);
      const visited = mode === "chain" ? [...expected, "C", "C", ...expected] : expected;
      assert.lengthOf(history.turns, visited.length);
      const markers = [
        "SYNTHETIC_ORIGINAL",
        ...expected.slice(1).map(() => "SYNTHETIC_CONTINUATION"),
        ...(mode === "chain" ? ["SYNTHETIC_QUEUED_INHERITED", "SYNTHETIC_FRESH_INHERITED"] : []),
        ...(mode === "chain"
          ? ["SYNTHETIC_EXPLICIT_RETURN", "SYNTHETIC_CONTINUATION", "SYNTHETIC_CONTINUATION"]
          : []),
      ];
      assert.deepEqual(
        history.turns.map((turn) =>
          turn.items.flatMap((item) =>
            item.type === "userMessage"
              ? item.content.flatMap((part) =>
                  part.type === "text"
                    ? [
                        part.text === "Continue where you left off."
                          ? "SYNTHETIC_CONTINUATION"
                          : part.text,
                      ]
                    : [],
                )
              : [],
          ),
        ),
        markers.map((marker) => [marker]),
      );
      const traces = (yield* Effect.forEach(
        (yield* fs.readDirectory(stateDir)).filter((name) => /^trace-.*\.ndjson$/.test(name)),
        (name) =>
          fs
            .readFileString(path.join(stateDir, name))
            .pipe(
              Effect.flatMap((text) =>
                Effect.forEach(text.trim().split("\n"), (line) => decodeTrace(line)),
              ),
            ),
      )).flat();
      const starts = traces.filter(({ method }) => method === "turn/start");
      assert.lengthOf(starts, visited.length);
      assert.lengthOf(
        traces.filter(({ method }) => method === "thread/start"),
        1,
      );
      assert.lengthOf(
        traces.filter(({ method }) => method === "synthetic/terminal"),
        visited.length,
      );
      for (const [index, turn] of history.turns.entries()) {
        const matching = starts.filter(({ nativeTurnId }) => nativeTurnId === turn.id);
        assert.lengthOf(matching, 1);
        assert.deepEqual(
          {
            instance: matching[0]!.instance,
            nativeThreadId: matching[0]!.nativeThreadId,
            model: matching[0]!.model,
            effort: matching[0]!.effort,
            serviceTier: matching[0]!.serviceTier,
            inputMarker: matching[0]!.inputMarker,
          },
          {
            instance: visited[index],
            nativeThreadId: nativeRef.nativeId,
            model: initialSelection.model,
            effort: "high",
            serviceTier: "priority",
            inputMarker: markers[index],
          },
        );
      }
      for (const account of expected.slice(1)) {
        assert.isTrue(
          traces.some(
            (entry) =>
              entry.instance === account &&
              entry.method === "thread/resume" &&
              entry.nativeThreadId === nativeRef.nativeId,
          ),
        );
      }
      assert.isFalse(yield* fs.exists(path.join(homePath, "auth.json")));
    }).pipe(
      Effect.provide(runtime),
      Effect.scoped,
      Effect.ensuring(
        attribution.snapshot.pipe(
          Effect.tap((entries) => Effect.sync(() => assert.equal(entries.size, 0))),
        ),
      ),
    );
  }).pipe(
    Effect.scoped,
    Effect.provide(
      Layer.mergeAll(
        NodeServices.layer,
        IdAllocator.layer,
        ControlAdmission.layer,
        ProcessAttribution.layer,
      ),
    ),
    Effect.provideService(
      ProviderEventLoggers.ProviderEventLoggers,
      ProviderEventLoggers.NoOpProviderEventLoggers,
    ),
  );

describe("Codex automatic account failover through the real server and subprocess adapter", () => {
  it.live(
    "cascades A → B → C across distinct provider threads and starts an inherited queued follower on C",
    () => scenario("chain"),
  );
  it.live(
    "exhausts A/B/C/D/E exactly once and terminates failed without duplicate continuation",
    () => scenario("all-unavailable"),
  );
});
