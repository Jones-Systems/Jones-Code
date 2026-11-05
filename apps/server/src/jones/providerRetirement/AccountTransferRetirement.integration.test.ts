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
import * as Crypto from "effect/Crypto";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as ServerConfig from "../../config.ts";
import * as Codex from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import * as Sessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as Sink from "../../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Adapters from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Harness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Catalog from "../../provider/ProviderRegistry.ts";
import { resolveCodexHomeLayout } from "../../provider/Drivers/CodexHomeLayout.ts";
import * as Settings from "../../serverSettings.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as Evidence from "../codexFailover/CodexFailoverEvidence.ts";
import * as Option from "effect/Option";
import * as ControlAdmission from "../orchestration/ControlAdmission.ts";
import * as ProcessAttribution from "../resourceTelemetry/ProcessAttribution.ts";

type DetachInput = Parameters<Sessions.ProviderSessionManagerV2Shape["detach"]>[0];

const accounts = ["A", "B"] as const;
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
    (run) => !["queued", "preparing", "starting", "running", "waiting"].includes(run.status),
  );
const failureState = Effect.fn("AccountTransferTest.failureState")(function* (threadId: ThreadId) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const outbox = yield* Outbox.EffectOutboxV2;
  const projection = yield* orchestrator.getThreadProjection(threadId);
  const effects = yield* outbox.listByThreadId(threadId);
  return {
    runs: projection.runs.map(({ ordinal, status, providerInstanceId }) => ({
      ordinal,
      status,
      providerInstanceId,
    })),
    failures: projection.turnItems.flatMap((item) => (item.type === "error" ? [item.failure] : [])),
    effects: effects.map(({ id, status, attemptCount, lastError }) => ({
      id,
      status,
      attemptCount,
      lastError,
    })),
  };
});
const awaitProjection = Effect.fn("AccountTransferTest.awaitProjection")(function* (
  threadId: ThreadId,
  afterSequence: number,
  predicate: (projection: OrchestrationV2ThreadProjection) => boolean,
  stage = "projection",
) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const sink = yield* Sink.EventSinkV2;
  const read = orchestrator.getThreadProjection(threadId);
  const current = yield* read;
  if (predicate(current)) return current;
  return Option.getOrThrow(
    yield* sink.stream({ threadId, afterSequence }).pipe(
      Stream.mapEffect(() => read),
      Stream.filter(predicate),
      Stream.runHead,
      Effect.timeout("45 seconds"),
      Effect.catchCause((cause) =>
        Effect.gen(function* () {
          const state = yield* failureState(threadId);
          return yield* Effect.die(
            new Error(`Account transfer ${stage} missing: ${JSON.stringify(state)}`, {
              cause: Cause.pretty(cause),
            }),
          );
        }),
      ),
    ),
  );
});

const scenario = (
  mode:
    | "selection"
    | "direct"
    | "queued"
    | "failover"
    | "cancel-return"
    | "exclusive"
    | "foreign"
    | "retry",
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const config = yield* Effect.acquireRelease(
      Harness.makeReplayServerConfig(`account-transfer-${mode}`),
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
    const held = mode === "queued" || mode === "failover";
    const entries = yield* Effect.forEach(accounts, (account) =>
      decodeSettings({
        binaryPath: peerPath,
        homePath,
        launchArgs: `--state-dir ${JSON.stringify(stateDir)} --account ${account} --mode chain${account !== "A" || mode !== "failover" ? " --outcome success" : ""}${account === "A" ? `${held ? " --hold" : ""} --defer-close${mode === "exclusive" ? "" : " --shared-producer"}` : ""}`,
      }).pipe(Effect.map((settings) => ({ account, settings }))),
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
        const adapters = yield* Effect.forEach(entries, ({ account, settings }) =>
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
        const find = (id: ProviderInstanceId) =>
          Effect.gen(function* () {
            const entry = adapters.find(({ adapter }) => adapter.instanceId === id);
            if (entry === undefined)
              return yield* new Adapters.ProviderAdapterRegistryLookupError({ instanceId: id });
            return entry;
          });
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
      Layer.provide(
        Layer.succeed(HostProcess.Environment, {
          HOME: homePath,
          PATH: `${path.dirname(realNode)}${delimiter}/usr/bin${delimiter}/bin`,
        }),
      ),
      Layer.provide(Layer.succeed(HostProcess.HomeDirectory, homePath)),
    );
    const secondAttempt = yield* Deferred.make<Outbox.OrchestrationEffectV2>();
    const queuedDetach = yield* Deferred.make<DetachInput>();
    const runtime = Harness.layerWithRegistryAndServices(
      { name: `account-transfer-${mode}`, runtimePolicyOverride: { cwd } },
      registry,
      {
        serverConfigLayer: Layer.succeed(ServerConfig.ServerConfig, { ...config, cwd }),
        databaseLayer: database,
        runEffectWorker: true,
        runContinuationWorker: true,
        ...(mode === "queued"
          ? {
              onSessionDetached: (input: DetachInput) =>
                input.requireWriterRetirement === true
                  ? Deferred.succeed(queuedDetach, input).pipe(Effect.asVoid)
                  : Effect.void,
            }
          : {}),
        ...(mode === "retry"
          ? {
              beforeEffectExecution: (effect: Outbox.OrchestrationEffectV2) =>
                effect.request.type === "provider-turn.start" && effect.attemptCount >= 2
                  ? Deferred.succeed(secondAttempt, effect).pipe(Effect.asVoid)
                  : Effect.void,
            }
          : {}),
      },
    ).pipe(
      Layer.provide(
        Layer.mock(Catalog.ProviderRegistry)({ getProviders: Effect.succeed(catalogs) }),
      ),
    );
    const readTraces = Effect.fn("AccountTransferTest.readTraces")(function* () {
      const files = (yield* fs.readDirectory(stateDir)).filter((name) =>
        /^trace-.*\.ndjson$/.test(name),
      );
      return (yield* Effect.forEach(files, (name) =>
        fs
          .readFileString(path.join(stateDir, name))
          .pipe(
            Effect.flatMap((contents) =>
              Effect.forEach(contents.split("\n").slice(0, -1), (line) => decodeTrace(line)),
            ),
          ),
      )).flat();
    });
    const awaitTrace = Effect.fn("AccountTransferTest.awaitTrace")(function* (
      stage: string,
      predicate: (traces: ReadonlyArray<typeof Trace.Type>) => boolean,
    ) {
      return yield* Effect.gen(function* () {
        const crypto = yield* Crypto.Crypto;
        const probe = path.join(stateDir, `watch-ready-${yield* crypto.randomUUIDv4}`);
        yield* Effect.acquireRelease(
          fs.writeFileString(probe, "", { flag: "wx" }),
          () => fs.remove(probe).pipe(Effect.orDie),
        );
        const changes = yield* Stream.toQueue(fs.watch(stateDir), {
          capacity: 1,
          strategy: "sliding",
        });
        // FileSystem.watch registers asynchronously; observe an event before the first snapshot.
        yield* Effect.raceFirst(
          Queue.take(changes),
          Stream.tick("10 millis").pipe(
            Stream.runForEach(() => fs.writeFileString(probe, "SYNTHETIC_WATCH_READY\n")),
          ),
        );
        while (true) {
          const traces = yield* readTraces();
          if (predicate(traces)) return traces;
          yield* Queue.take(changes);
        }
      }).pipe(
        Effect.timeout("45 seconds"),
        Effect.catchCause((cause) =>
          Effect.gen(function* () {
            const traces = (yield* readTraces()).map(
              ({ instance, processId, sequence, method }) => ({
                instance,
                processId,
                sequence,
                method,
              }),
            );
            return yield* Effect.die(
              new Error(`Account transfer trace ${stage} missing: ${JSON.stringify(traces)}`, {
                cause: Cause.pretty(cause),
              }),
            );
          }),
        ),
      );
    }, Effect.scoped);
    const attribution = yield* ProcessAttribution.ProcessAttribution;
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* Sink.EventSinkV2;
      const outbox = yield* Outbox.EffectOutboxV2;
      const sessions = yield* Sessions.ProviderSessionManagerV2;
      const settings = yield* Settings.ServerSettingsService;
      for (const id of Object.keys((yield* settings.getSettings).providerInstances))
        yield* settings.updateProviderInstance({
          operation: "remove",
          instanceId: ProviderInstanceId.make(id),
        });
      for (const { account, settings: config } of entries)
        yield* settings.updateProviderInstance({
          operation: "create",
          instanceId: instanceId(account),
          instance: { driver: Codex.CODEX_DRIVER_KIND, enabled: true, config },
        });
      const threadId = ThreadId.make(`synthetic-account-transfer-${mode}`);
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("synthetic:create"),
        threadId,
        projectId: ProjectId.make("synthetic-project"),
        title: "Synthetic writer transfer",
        modelSelection: initialSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: cwd,
        createdBy: "user",
        creationSource: "web",
      });
      const after = yield* sink.latestSequence();
      const dispatch = (marker: string, selection?: ModelSelection, queued = false) =>
        orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(marker),
          messageId: MessageId.make(marker),
          threadId,
          text: marker,
          ...(selection === undefined
            ? {}
            : { modelSelection: selection, modelSelectionExplicit: true }),
          attachments: [],
          createdBy: "user",
          creationSource: "web",
          dispatchMode: { type: queued ? "queue_after_active" : "start_immediately" },
        });
      const select = (account: string, key: string) =>
        orchestrator.dispatch({
          type: "thread.model-selection.set",
          commandId: CommandId.make(key),
          threadId,
          modelSelection: { ...initialSelection, instanceId: instanceId(account) },
        });
      yield* dispatch("SYNTHETIC_INITIAL");
      const source = yield* awaitProjection(threadId, after, (projection) =>
        held
          ? projection.providerTurns.some(
              (turn) => turn.status === "running" && turn.nativeTurnRef?.nativeId != null,
            )
          : projection.runs.length === 1 && isIdle(projection),
      );
      const sourceThread = source.providerThreads.find(
        (row) => row.id === source.thread.activeProviderThreadId,
      )!;
      const nativeId = sourceThread.nativeThreadRef!.nativeId!;
      assert.equal(sourceThread.providerInstanceId, instanceId("A"));
      const sourceProcess = (yield* readTraces()).find(
        (trace) => trace.method === "thread/start",
      )!.processId;
      const target = { ...initialSelection, instanceId: instanceId("B") };
      if (mode === "foreign") {
        // An ordinary unsubscribe intentionally leaves the synthetic holder alive until closure.
        yield* sessions.detach({ providerSessionId: sourceThread.providerSessionId!, threadId });
        yield* awaitTrace("source unsubscribe", (traces) =>
          traces.some(
            (trace) =>
              trace.method === "synthetic/unsubscribe" && trace.processId === sourceProcess,
          ),
        );
        const lockPath = path.join(stateDir, `lock-${nativeId}`);
        const holder = yield* fs.readFileString(lockPath);
        yield* select("B", "synthetic:foreign-B");
        yield* dispatch("SYNTHETIC_FOREIGN");
        yield* awaitProjection(
          threadId,
          after,
          (projection) =>
            projection.runs.some(
              (run) => run.userMessageId === "SYNTHETIC_FOREIGN" && run.status === "failed",
            ) && isIdle(projection),
          "foreign writer refusal",
        );
        assert.equal(yield* fs.readFileString(lockPath), holder);
        assert.isTrue(
          (yield* readTraces()).some((trace) => trace.method === "synthetic/writer-refused"),
        );
        yield* fs.writeFileString(path.join(stateDir, `close-${nativeId}`), "SYNTHETIC_CLOSE\n");
        yield* awaitTrace("foreign writer closed", (traces) =>
          traces.some(
            (trace) => trace.method === "synthetic/closed" && trace.processId === sourceProcess,
          ),
        );
        yield* dispatch("SYNTHETIC_RETRY");
        const recovered = yield* awaitProjection(
          threadId,
          after,
          (projection) =>
            projection.runs.some(
              (run) => run.userMessageId === "SYNTHETIC_RETRY" && run.status === "completed",
            ) && isIdle(projection),
          "foreign writer recovery",
        );
        assert.equal(
          recovered.providerThreads.find(
            (row) => row.id === recovered.thread.activeProviderThreadId,
          )!.nativeThreadRef!.nativeId,
          nativeId,
        );
        assert.lengthOf(
          (yield* readTraces()).filter((trace) => trace.method === "thread/start"),
          1,
        );
        return;
      }
      if (mode === "queued") {
        yield* dispatch("SYNTHETIC_TARGET", target, true);
        assert.equal(
          (yield* orchestrator.getThreadProjection(threadId)).runs.at(-1)!.status,
          "queued",
        );
      } else if (mode !== "failover" && mode !== "direct") {
        const selectionCommand = CommandId.make("synthetic:select-B");
        yield* select("B", selectionCommand);
        const effects = (yield* outbox.listByCommandId(selectionCommand)).filter(
          (effect) => effect.request.type === "provider-session.detach",
        );
        assert.lengthOf(effects, 1);
        assert.equal(
          effects[0]!.request.type === "provider-session.detach" && effects[0]!.request.detail,
          "Provider or model selection changed.",
        );
        yield* outbox.awaitCompletion(effects.map((effect) => effect.id));
        assert.equal(Option.getOrThrow(yield* outbox.get(effects[0]!.id)).status, "succeeded");
      }
      if (held) {
        const turn = source.providerTurns.find((turn) => turn.status === "running")!;
        yield* fs.writeFileString(
          path.join(stateDir, `release-${turn.nativeTurnRef!.nativeId}`),
          "SYNTHETIC_RELEASE\n",
        );
      } else yield* dispatch("SYNTHETIC_TARGET", mode === "direct" ? target : undefined);
      if (mode !== "exclusive") {
        yield* awaitTrace("transfer unsubscribe", (traces) =>
          traces.some(
            (trace) =>
              trace.method === "synthetic/unsubscribe" && trace.processId === sourceProcess,
          ),
        );
        const traces = yield* readTraces();
        assert.isFalse(
          traces.some(
            (trace) =>
              trace.instance === "B" && ["thread/resume", "turn/start"].includes(trace.method),
          ),
        );
        assert.isFalse(
          traces.some(
            (trace) => trace.method === "synthetic/closed" && trace.processId === sourceProcess,
          ),
        );
        const detaches = (yield* outbox.listByThreadId(threadId)).filter(
          (effect) => effect.request.type === "provider-session.detach",
        );
        if (mode === "queued") {
          assert.lengthOf(detaches, 0);
          assert.isFalse(yield* Deferred.isDone(queuedDetach));
        } else {
          yield* outbox.awaitCompletion(detaches.map((effect) => effect.id));
          assert.isTrue(detaches.length > 0);
        }
        if (mode === "direct" || mode === "failover")
          assert.isTrue(
            detaches.some(
              (effect) =>
                effect.request.type === "provider-session.detach" &&
                effect.request.detail === "Provider account changed; continuing the native thread.",
            ),
          );
        if (mode === "retry") {
          const attempt = yield* Deferred.await(secondAttempt).pipe(Effect.timeout("45 seconds"));
          assert.isAtLeast(attempt.attemptCount, 2);
          const current = Option.getOrThrow(yield* outbox.get(attempt.id));
          assert.equal(current.status, "running");
          assert.isAtLeast(current.attemptCount, 2);
          assert.isFalse(
            (yield* readTraces()).some(
              (trace) => trace.instance === "B" && trace.method === "thread/resume",
            ),
          );
        }
        if (mode === "cancel-return") {
          yield* orchestrator.dispatch({
            type: "thread.stop",
            commandId: CommandId.make("synthetic:cancel-B"),
            threadId,
          });
        }
        yield* fs.writeFileString(path.join(stateDir, `close-${nativeId}`), "SYNTHETIC_CLOSE\n");
        yield* sessions.awaitThreadRetirement!({ threadId, instanceId: instanceId("B") });
        if (mode === "queued") {
          const detached = yield* Deferred.await(queuedDetach).pipe(Effect.timeout("45 seconds"));
          assert.equal(detached.providerSessionId, sourceThread.providerSessionId);
          assert.equal(detached.threadId, threadId);
          assert.isTrue(detached.requireWriterRetirement);
          assert.equal(detached.detail, "Selection changed at normal run admission.");
        }
      }
      if (mode === "cancel-return") {
        yield* awaitProjection(
          threadId,
          after,
          (projection) => isIdle(projection),
          "cancelled B idle",
        );
        yield* select("A", "synthetic:return-A");
        yield* dispatch("SYNTHETIC_RETURN");
        const finished = yield* awaitProjection(
          threadId,
          after,
          (projection) =>
            projection.runs.some(
              (run) => run.userMessageId === "SYNTHETIC_RETURN" && run.status === "completed",
            ) && isIdle(projection),
          "cancelled transfer return to A",
        );
        assert.equal(finished.runs.at(-1)!.modelSelection.instanceId, instanceId("A"));
      } else {
        const finished = yield* awaitProjection(
          threadId,
          after,
          (projection) =>
            projection.runs.at(-1)!.modelSelection.instanceId === instanceId("B") &&
            projection.runs.at(-1)!.status === "completed" &&
            isIdle(projection),
          "target B completion",
        );
        if (mode === "queued")
          assert.isFalse(
            finished.providerSessions.some(
              (session) => session.id === sourceThread.providerSessionId,
            ),
          );
        assert.equal(
          finished.providerThreads.find((row) => row.id === finished.thread.activeProviderThreadId)!
            .nativeThreadRef!.nativeId,
          nativeId,
        );
        if (mode === "selection") {
          yield* select("A", "synthetic:return-A");
          yield* dispatch("SYNTHETIC_RETURN");
          yield* awaitProjection(
            threadId,
            after,
            (projection) =>
              projection.runs.some(
                (run) => run.userMessageId === "SYNTHETIC_RETURN" && run.status === "completed",
              ) && isIdle(projection),
            "selection return to A",
          );
        }
      }
      const traces = yield* readTraces();
      assert.lengthOf(
        traces.filter((trace) => trace.method === "thread/start"),
        1,
      );
      assert.isFalse(traces.some((trace) => trace.method === "synthetic/writer-refused"));
      for (const trace of traces.filter((trace) => trace.method === "thread/resume"))
        assert.equal(trace.nativeThreadId, nativeId);
      if (mode === "cancel-return")
        assert.isFalse(
          traces.some((trace) => trace.instance === "B" && trace.method === "thread/resume"),
        );
      else
        assert.isTrue(
          traces.some((trace) => trace.instance === "B" && trace.method === "thread/resume"),
        );
      if (mode === "selection" || mode === "cancel-return")
        assert.isTrue(
          traces.some(
            (trace) =>
              trace.instance === "A" &&
              trace.method === "thread/resume" &&
              trace.processId !== sourceProcess,
          ),
        );
      if (mode === "exclusive")
        assert.isTrue(
          traces.some(
            (trace) => trace.method === "synthetic/exit" && trace.processId === sourceProcess,
          ),
        );
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

describe("Codex account transfer with synthetic producers and a shared writer registry", () => {
  it.live.each([
    "selection",
    "direct",
    "queued",
    "failover",
    "cancel-return",
    "exclusive",
    "foreign",
    "retry",
  ] as const)("%s preserves native identity behind exact writer release", (mode) => scenario(mode));
});
