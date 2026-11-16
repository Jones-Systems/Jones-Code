import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as ThreadLaunch from "../../orchestration-v2/ThreadLaunchService.ts";
import * as Questions from "../../orchestration-v2/DelegatedTaskQuestionReactor.ts";
import { DEFAULT_SERVER_SETTINGS, EnvironmentId } from "@t3tools/contracts";
import { HttpServer } from "effect/http";
import * as NetAddress from "effect/net/NetAddress";
import * as Startup from "../../serverRuntimeStartup.ts";
import { ServerActivation } from "../../serverActivation.ts";
import * as Keybindings from "../../keybindings.ts";
import * as Legacy from "../../orchestration-v2/legacy/LegacyV1ThreadImporter.ts";
import * as Recovery from "../../orchestration-v2/ProviderRuntimeRecoveryService.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Sessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as Worker from "../../orchestration-v2/EffectWorker.ts";
import * as Relay from "../../relay/AgentAwarenessRelay.ts";
import * as Lifecycle from "../../serverLifecycleEvents.ts";
import * as Settings from "../../serverSettings.ts";
import * as Environment from "../../environment/ServerEnvironment.ts";
import * as Launcher from "../../cloud/serviceLauncherClient.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import * as ThreadMetadata from "../orchestration/threadMetadataReads.ts";
import * as Analytics from "../../telemetry/AnalyticsService.ts";
import * as Secrets from "../../auth/ServerSecretStore.ts";
import * as Auth from "../../auth/EnvironmentAuth.ts";
import * as External from "../../process/externalLauncher.ts";
import * as Git from "../../vcs/GitVcsDriver.ts";
import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { importedHistoryCanonicalJson as canonical } from "@t3tools/shared/jones/importedHistoryCanonical";
import {
  EventId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  MessageId,
  RunId,
  NodeId,
  RuntimeRequestId,
  OrchestrationV2DomainEvent,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Digest from "effect/crypto/Digest";
import * as Hex from "effect/encoding/Hex";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Config from "../../config.ts";
import * as Ownership from "../../serverOwnership.ts";
import * as ProcessRunner from "../../processRunner.ts";
import * as Projects from "../../orchestration-v2/ProjectStore.ts";
import * as Projection from "../../orchestration-v2/ProjectionStore.ts";
import { runMigrations } from "../../persistence/Migrations.ts";
import { workModeCandidate } from "../workMode/Policy.ts";
import { HeldRuntimeOwner } from "../cleanup/CheckedRuntimeOwner.ts";
import {
  SyntheticPresentation,
  installSelectedSyntheticPresentation,
  type SyntheticPresentationCapability,
} from "./SyntheticPresentation.ts";

const platform = ProcessRunner.layer.pipe(
  Layer.provideMerge(Config.layerTest(process.cwd(), { prefix: "private-presentation-" })),
  Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
);
const namespace = "l8-private-ui77-fixture";
const projectId = ProjectId.make(namespace + ":project");
const provider = ProviderInstanceId.make("synthetic-presentation-only");
const at = DateTime.makeUnsafe("2026-11-04T12:00:00Z");
const encodeEvent = Schema.encodeEffect(OrchestrationV2DomainEvent);
const fixtures = (): readonly OrchestrationV2DomainEvent[] => {
  const result: OrchestrationV2DomainEvent[] = [];
  const thread = (
    suffix: string,
    extra: Partial<Extract<OrchestrationV2DomainEvent, { type: "thread.created" }>["payload"]> = {},
  ) => {
    const id = ThreadId.make(namespace + ":" + suffix);
    result.push({
      id: EventId.make(id + ":birth"),
      type: "thread.created",
      threadId: id,
      occurredAt: at,
      payload: {
        id,
        projectId,
        title: "Synthetic " + suffix,
        createdBy: "user",
        creationSource: "web",
        providerInstanceId: provider,
        modelSelection: { instanceId: provider, model: "synthetic-model" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        activeProviderThreadId: null,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
        forkedFrom: null,
        createdAt: at,
        updatedAt: at,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
        deletedAt: null,
        autoSettleDisabledAt: at,
        ...extra,
      },
    });
    return id;
  };
  const parent = thread("running-question");
  const runId = RunId.make(parent + ":run");
  result.push({
    id: EventId.make(parent + ":run-created"),
    type: "run.created",
    threadId: parent,
    runId,
    occurredAt: at,
    payload: {
      id: runId,
      threadId: parent,
      ordinal: 1,
      providerInstanceId: provider,
      modelSelection: { instanceId: provider, model: "synthetic-model" },
      providerThreadId: null,
      userMessageId: MessageId.make(namespace + ":synthetic-message"),
      rootNodeId: null,
      activeAttemptId: null,
      status: "running",
      requestedAt: at,
      startedAt: at,
      completedAt: null,
      checkpointId: null,
      contextHandoffId: null,
    },
  });
  const question = (owner: ThreadId, approval = false) => {
    const nodeId = NodeId.make(owner + ":node"),
      requestId = RuntimeRequestId.make(owner + ":request");
    result.push({
      id: EventId.make(owner + ":node-event"),
      type: "node.updated",
      threadId: owner,
      nodeId,
      occurredAt: at,
      payload: {
        id: nodeId,
        threadId: owner,
        runId: null,
        parentNodeId: null,
        rootNodeId: nodeId,
        kind: approval ? "approval_request" : "user_input_request",
        status: "waiting",
        countsForRun: false,
        providerThreadId: null,
        providerTurnId: null,
        nativeItemRef: null,
        runtimeRequestId: requestId,
        checkpointScopeId: null,
        startedAt: at,
        completedAt: null,
      },
    });
    result.push({
      id: EventId.make(owner + ":request-event"),
      type: "runtime-request.updated",
      threadId: owner,
      nodeId,
      occurredAt: at,
      payload: {
        id: requestId,
        nodeId,
        providerTurnId: null,
        nativeRequestRef: null,
        kind: approval ? "command" : "user_input",
        status: "pending",
        responseCapability: approval
          ? { type: "not_resumable", reason: "Synthetic presentation has no native callback" }
          : { type: "message" },
        createdAt: at,
        resolvedAt: null,
      },
    });
    const common = {
      id: TurnItemId.make(owner + ":item"),
      threadId: owner,
      runId: null,
      nodeId,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: 1,
      status: "waiting" as const,
      title: null,
      startedAt: at,
      completedAt: null,
      updatedAt: at,
      requestId,
    };
    result.push({
      id: EventId.make(owner + ":item-event"),
      type: "turn-item.updated",
      threadId: owner,
      nodeId,
      occurredAt: at,
      payload: approval
        ? {
            ...common,
            type: "approval_request",
            requestKind: "command",
            prompt: "Synthetic approval priority",
          }
        : {
            ...common,
            type: "user_input_request",
            responseMode: "message",
            questions: [
              {
                id: "route",
                header: "Route",
                question: "Which synthetic route?",
                options: [{ label: "Isolated", description: "Task-owned fixture" }],
                allowCustomAnswer: true,
              },
            ],
          },
    });
  };
  question(parent);
  const child = thread("approval-child", {
    lineage: { parentThreadId: parent, relationshipToParent: "subagent", rootThreadId: parent },
  });
  question(child, true);
  thread("clear-idle");
  thread("snoozed", { snoozedAt: at, snoozedUntil: DateTime.makeUnsafe("2026-12-04T12:00:00Z") });
  const history = thread("started-history");
  result.push({
    id: EventId.make(history + ":run-event"),
    type: "run.created",
    threadId: history,
    runId: RunId.make(history + ":run"),
    occurredAt: at,
    payload: {
      id: RunId.make(history + ":run"),
      threadId: history,
      ordinal: 1,
      providerInstanceId: provider,
      modelSelection: { instanceId: provider, model: "synthetic-model" },
      providerThreadId: null,
      userMessageId: MessageId.make(namespace + ":synthetic-message"),
      rootNodeId: null,
      activeAttemptId: null,
      status: "completed",
      requestedAt: at,
      startedAt: at,
      completedAt: at,
      checkpointId: null,
      contextHandoffId: null,
    },
  });
  return result;
};
type Services =
  | SqlClient.SqlClient
  | Projection.ProjectionStoreV2
  | Config.ServerConfig
  | FileSystem.FileSystem
  | Path.Path
  | Digest.Digest
  | Projects.ProjectStoreV2
  | Outbox.EffectOutboxV2
  | SecureRandom.SecureRandom;
const fixture = <A, E>(
  test: (input: {
    readonly capability: SyntheticPresentationCapability;
    readonly install: Effect.Effect<
      void,
      import("./SyntheticPresentation.ts").SyntheticPresentationError,
      Config.ServerConfig | FileSystem.FileSystem | Path.Path | Digest.Digest
    >;
    readonly projections: Projection.ProjectionStoreV2["Service"];
    readonly sql: SqlClient.SqlClient;
    readonly sourceFile: string;
  }) => Effect.Effect<A, E, Services>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const config = yield* Config.ServerConfig;
      const crypto = yield* Digest.Digest;
      yield* fs.makeDirectory(config.stateDir, { recursive: true });
      const owner = yield* Ownership.acquireServerOwnership(config.serverRuntimeStatePath);
      const checkout = yield* fs.realPath(
        yield* path.fromFileUrl(new URL("../../../../../", import.meta.url)),
      );
      const sourceFile = path.join(
        checkout,
        "apps/server/src/jones/testing/SyntheticPresentation.ts",
      );
      const sourcePaths = [
        "apps/server/src/jones/testing/SyntheticPresentation.ts",
        "apps/server/src/serverRuntimeStartup.ts",
      ];
      const source = yield* Effect.forEach(sourcePaths, (file) =>
        fs.readFile(path.join(checkout, file)).pipe(
          Effect.flatMap((bytes) => crypto.digest(Digest.sha256, bytes)),
          Effect.map((bytes) => ({ path: file, sha256: Hex.encode(bytes) })),
        ),
      );
      const workspace = path.join(config.baseDir, "synthetic-workspace");
      yield* fs.makeDirectory(workspace);
      const persistence = Layer.effectDiscard(runMigrations()).pipe(
        Layer.provideMerge(NodeSqliteClient.layer({ filename: config.dbPath })),
      );
      const stores = Layer.mergeAll(Projection.layer, Projects.layer, Outbox.layer).pipe(
        Layer.provideMerge(persistence),
      );
      return yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const projections = yield* Projection.ProjectionStoreV2;
        const projects = yield* Projects.ProjectStoreV2;
        yield* projects.apply({
          sequence: 1,
          eventId: EventId.make(namespace + ":project-event"),
          aggregateKind: "project",
          aggregateId: projectId,
          occurredAt: DateTime.formatIso(at),
          commandId: null,
          causationEventId: null,
          correlationId: null,
          metadata: {},
          type: "project.created",
          payload: {
            projectId,
            title: "Synthetic presentation",
            workspaceRoot: workspace,
            defaultModelSelection: null,
            scripts: [],
            createdAt: DateTime.formatIso(at),
            updatedAt: DateTime.formatIso(at),
          },
        });
        const events = fixtures();
        const encoded = yield* Effect.forEach(events, (event) => encodeEvent(event));
        const capability: SyntheticPresentationCapability = {
          descriptor: {
            schema: "jones.private-synthetic-presentation/v1",
            namespace,
            checkout,
            privateHome: config.baseDir,
            database: config.dbPath,
            projectId,
            threadIds: events
              .filter((event) => event.type === "thread.created")
              .map((event) => event.threadId),
            eventIds: events.map((event) => event.id),
            providerIds: [provider],
            source,
            eventsSha256: Hex.encode(
              yield* crypto.digest(Digest.sha256, new TextEncoder().encode(canonical(encoded))),
            ),
          },
          events,
        };
        const marker = canonical({
          schema: "jones.private-synthetic-presentation-marker/v1",
          descriptorSha256: Hex.encode(
            yield* crypto.digest(
              Digest.sha256,
              new TextEncoder().encode(canonical(capability.descriptor)),
            ),
          ),
        });
        yield* fs.writeFileString(
          path.join(config.baseDir, ".jones-synthetic-presentation.json"),
          marker + "\n",
          { mode: 0o600 },
        );
        return yield* test({
          capability,
          projections,
          sql,
          sourceFile,
          install: installSelectedSyntheticPresentation().pipe(
            Effect.provideService(SyntheticPresentation, capability),
          ),
        });
      }).pipe(
        Effect.provide(stores),
        Effect.provideService(HeldRuntimeOwner, { token: owner.heldRuntimeOwner }),
        Effect.provideService(Config.ServerConfig, {
          ...config,
          devUrl: new URL("http://localhost:5511"),
        }),
      );
    }).pipe(Effect.provide(platform)),
  );

it.effect("absent capability performs no fixture filesystem reads", () =>
  fixture((input) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      let reads = 0;
      yield* installSelectedSyntheticPresentation().pipe(
        Effect.provideService(FileSystem.FileSystem, {
          ...fs,
          readFile: (path) => {
            reads++;
            return fs.readFile(path);
          },
          realPath: (path) => {
            reads++;
            return fs.realPath(path);
          },
        }),
      );
      assert.strictEqual(reads, 0);
      assert.deepEqual(yield* input.sql`SELECT * FROM orchestration_v2_projection_threads`, []);
    }),
  ),
);
it.effect(
  "one atomic complete fixture is visible through real shell/detail readers without executable history",
  () =>
    fixture((input) =>
      Effect.gen(function* () {
        assert.strictEqual(
          (yield* input.projections.getShellSnapshot({ location: "active" })).threads.length,
          0,
        );
        yield* input.install;
        const snapshot = yield* input.projections.getShellSnapshot({ location: "active" });
        assert.strictEqual(snapshot.threads.length, 5);
        assert.strictEqual(snapshot.snapshotSequence, 0);
        const parent = snapshot.threads.find(
          (thread) => thread.id === namespace + ":running-question",
        )!;
        assert.strictEqual(parent.status, "running");
        assert.strictEqual(parent.pendingRuntimeRequest?.blocking, false);
        const child = snapshot.threads.find(
          (thread) => thread.id === namespace + ":approval-child",
        )!;
        assert.strictEqual(child.lineage.parentThreadId, parent.id);
        assert.strictEqual(child.pendingRuntimeRequest?.kind, "command");
        for (const thread of snapshot.threads)
          assert.isNull(workModeCandidate(thread, DateTime.toEpochMillis(at)));
        assert.deepEqual(yield* input.projections.getSettlementCandidates(), []);
        assert.deepEqual(
          yield* input.projections.getLimitRecoveryCandidates({
            now: at,
            autoResume: true,
            snooze: true,
          }),
          [],
        );
        const detail = yield* input.projections.getThreadRecords(parent.id, [
          "runs",
          "runtimeRequests",
          "turnItems",
        ]);
        assert.strictEqual(detail.runs[0]?.status, "running");
        assert.strictEqual(detail.runtimeRequests[0]?.responseCapability.type, "message");
        assert.deepEqual(yield* input.sql`SELECT * FROM orchestration_events`, []);
        assert.deepEqual(yield* input.sql`SELECT * FROM orchestration_command_receipts`, []);
        assert.deepEqual(yield* input.sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
        assert.strictEqual((yield* input.install.pipe(Effect.result))._tag, "Failure");
        assert.deepEqual(
          yield* input.projections.getShellSnapshot({ location: "active" }),
          snapshot,
        );
      }),
    ),
);
it.effect("source variance consumes and refuses the capability before any projection write", () =>
  fixture((input) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const changed = {
        ...fs,
        readFile: (file: string) =>
          file === input.sourceFile
            ? Effect.succeed(new TextEncoder().encode("changed source bytes"))
            : fs.readFile(file),
      };
      assert.strictEqual(
        (yield* input.install.pipe(
          Effect.provideService(FileSystem.FileSystem, changed),
          Effect.result,
        ))._tag,
        "Failure",
      );
      assert.deepEqual(yield* input.sql`SELECT * FROM orchestration_v2_projection_threads`, []);
      assert.strictEqual((yield* input.install.pipe(Effect.result))._tag, "Failure");
    }),
  ),
);
it.effect("a late projection failure rolls the entire fixture back without retry authority", () =>
  fixture((input) =>
    Effect.gen(function* () {
      let calls = 0;
      const broken = {
        ...input.projections,
        apply: (event: Parameters<Projection.ProjectionStoreV2["Service"]["apply"]>[0]) =>
          ++calls === 2
            ? Effect.fail(
                new Projection.ProjectionStoreApplyEventError({
                  eventType: event.type,
                  cause: "Injected synthetic failure",
                }),
              )
            : input.projections.apply(event),
      };
      assert.strictEqual(
        (yield* input.install.pipe(
          Effect.provideService(Projection.ProjectionStoreV2, broken),
          Effect.result,
        ))._tag,
        "Failure",
      );
      assert.strictEqual(calls, 2);
      assert.deepEqual(yield* input.sql`SELECT * FROM orchestration_v2_projection_threads`, []);
      assert.strictEqual((yield* input.install.pipe(Effect.result))._tag, "Failure");
    }),
  ),
);
it.effect(
  "interruption and concurrent invocation cannot publish partial fixtures or reuse a consumed capability",
  () =>
    fixture((input) =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        let calls = 0;
        const gated = {
          ...input.projections,
          apply: (event: Parameters<Projection.ProjectionStoreV2["Service"]["apply"]>[0]) =>
            input.projections
              .apply(event)
              .pipe(
                Effect.andThen(
                  ++calls === 1
                    ? Deferred.succeed(entered, undefined).pipe(
                        Effect.andThen(Deferred.await(release)),
                      )
                    : Effect.void,
                ),
              ),
        };
        const pending = yield* input.install.pipe(
          Effect.provideService(Projection.ProjectionStoreV2, gated),
          Effect.forkChild,
        );
        yield* Deferred.await(entered);
        assert.strictEqual((yield* input.install.pipe(Effect.result))._tag, "Failure");
        yield* Fiber.interrupt(pending);
        yield* Deferred.succeed(release, undefined);
        assert.deepEqual(yield* input.sql`SELECT * FROM orchestration_v2_projection_threads`, []);
        assert.strictEqual((yield* input.install.pipe(Effect.result))._tag, "Failure");
      }),
    ),
);

it.effect.each([
  "success",
  "projection-failure",
  "shell-read-failure",
  "detail-read-failure",
  "shell-mismatch",
  "detail-mismatch",
] as const)(
  "real startup awaits private fixture before activation/readiness and propagates abort: %s",
  (mode) =>
    fixture((input) =>
      Effect.scoped(
        Effect.gen(function* () {
          const calls: string[] = [];
          const record = (label: string) =>
            Effect.sync(() => {
              calls.push(label);
            });
          const entered = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const shellEntered = yield* Deferred.make<void>();
          const shellRelease = yield* Deferred.make<void>();
          const detailEntered = yield* Deferred.make<void>();
          const detailRelease = yield* Deferred.make<void>();
          const readiness = yield* Deferred.make<"ready" | "failed">();
          let shellReads = 0,
            detailReads = 0;
          const activated = yield* Deferred.make<void>();
          const aborted = yield* Deferred.make<void>();
          const fs = yield* FileSystem.FileSystem;
          const config = yield* Config.ServerConfig;
          const gatedFs = {
            ...fs,
            readFile: (file: string) =>
              file === input.sourceFile
                ? record("source-file-validation").pipe(
                    Effect.andThen(Deferred.succeed(entered, undefined)),
                    Effect.andThen(Deferred.await(release)),
                    Effect.andThen(fs.readFile(file)),
                  )
                : fs.readFile(file),
          };
          let applied = 0;
          const readRecords: Projection.ProjectionStoreV2["Service"]["getThreadRecords"] = (
            threadId,
            fields,
            filter,
          ) =>
            Effect.gen(function* () {
              if (++detailReads === 1) {
                yield* record("committed-detail-read");
                yield* Deferred.succeed(detailEntered, undefined);
                yield* Deferred.await(detailRelease);
                if (mode === "detail-read-failure")
                  return yield* new Projection.ProjectionStoreReadError({ threadId });
              }
              const detail = yield* input.projections.getThreadRecords(threadId, fields, filter);
              return mode === "detail-mismatch" && detailReads === 1
                ? {
                    ...detail,
                    thread: {
                      ...detail.thread,
                      lineage: {
                        ...detail.thread.lineage,
                        parentThreadId: ThreadId.make(namespace + ":foreign"),
                      },
                    },
                  }
                : detail;
            });
          const projected = {
            ...input.projections,
            getThreadShell: (threadId: ThreadId) =>
              Effect.gen(function* () {
                if (++shellReads === 1) {
                  yield* record("committed-shell-read");
                  yield* Deferred.succeed(shellEntered, undefined);
                  yield* Deferred.await(shellRelease);
                  if (mode === "shell-read-failure")
                    return yield* new Projection.ProjectionStoreReadError({ threadId });
                }
                const shell = yield* input.projections.getThreadShell(threadId);
                return mode === "shell-mismatch" && shellReads === 1 && shell !== null
                  ? { ...shell, title: "Mismatched committed shell" }
                  : shell;
              }),
            getThreadRecords: readRecords,
            apply: (event: Parameters<Projection.ProjectionStoreV2["Service"]["apply"]>[0]) =>
              ++applied === 2 && mode === "projection-failure"
                ? Effect.fail(
                    new Projection.ProjectionStoreApplyEventError({
                      eventType: event.type,
                      cause: "Synthetic startup rollback",
                    }),
                  )
                : input.projections.apply(event),
          };
          const recovery = {
            terminalizedRuns: 0,
            stoppedSessions: 0,
            closedRequests: 0,
            retiredEffects: 0,
            requeuedEffects: 0,
          };
          const dependencies = Layer.mergeAll(
            Layer.mock(EventSink.EventSinkV2)({}),
            Layer.mock(ThreadLaunch.ThreadLaunchService)({}),
            Layer.succeed(Projects.ProjectStoreV2, yield* Projects.ProjectStoreV2),
            Layer.succeed(Outbox.EffectOutboxV2, yield* Outbox.EffectOutboxV2),
            Layer.succeed(SecureRandom.SecureRandom, yield* SecureRandom.SecureRandom),
            Layer.mock(Keybindings.Keybindings)({ start: record("keybindings") }),
            Layer.mock(Legacy.LegacyV1ThreadImporter)({
              pendingThreadCount: Effect.succeed(0),
              reconcileShells: record("legacy").pipe(
                Effect.as({ importedThreadCount: 0, importedMessageCount: 0 }),
              ),
              importPendingTranscripts: Effect.succeed({
                importedThreadCount: 0,
                importedMessageCount: 0,
              }),
            }),
            Layer.mock(Recovery.ProviderRuntimeRecoveryService)({
              recover: record("recovery").pipe(Effect.as(recovery)),
              prepareForShutdown: Effect.void,
              reconcile: () => Effect.succeed(recovery),
            }),
            Layer.mock(Orchestrator.OrchestratorV2)({
              recoverCodexFailover: record("failover"),
              recoverDelegatedTasks: record("delegation"),
              recoverPlannedUpdateQueues: record("planned"),
            }),
            Layer.mock(Sessions.ProviderSessionManagerV2)({ shutdown: Effect.void }),
            Layer.mock(Worker.OrchestrationEffectWorkerV2)({ runOnce: Effect.never }),
            Layer.mock(Relay.AgentAwarenessRelay)({ start: () => record("relay-registration") }),
            Layer.mock(Lifecycle.ServerLifecycleEvents)({
              publish: (event) =>
                record(event.type).pipe(Effect.as({ ...event, sequence: calls.length + 1 })),
            }),
            Layer.mock(Settings.ServerSettingsService)({
              start: record("settings"),
              getSettings: Effect.succeed(DEFAULT_SERVER_SETTINGS),
            }),
            Layer.mock(Environment.ServerEnvironment)({
              getDescriptor: Effect.succeed({
                environmentId: EnvironmentId.make(namespace + ":environment"),
                label: "Synthetic fixture",
                platform: { os: "linux", arch: "x64" },
                serverVersion: "0.0.0-synthetic",
                capabilities: { repositoryIdentity: true },
              }),
            }),
            Layer.mock(Launcher.ServiceLauncherClient)({
              managed: false,
              requiresQualifiedTrialGate: false,
              prepareTrial: record("trial").pipe(Effect.as(undefined)),
            }),
            Layer.mock(ProjectService.ProjectService)({
              snapshot: Effect.succeed({ projects: [], updatedAt: "2026-11-04T12:00:00Z" }),
            }),
            Layer.mock(ThreadMetadata.ThreadMetadataReads)({ count: Effect.succeed(5) }),
            Layer.mock(Analytics.AnalyticsService)({
              record: () => Effect.void,
              recordOwners: () => Effect.void,
            }),
            Layer.mock(Secrets.ServerSecretStore)({ get: () => Effect.succeedNone }),
            Layer.mock(Auth.EnvironmentAuth)({}),
            Layer.mock(External.ExternalLauncher)({}),
            Layer.mock(Git.GitVcsDriver)({}),
            Layer.mock(HttpServer.HttpServer)({
              address: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 5511),
            }),
          );
          yield* Effect.gen(function* () {
            const startup = yield* Startup.ServerRuntimeStartup;
            const command = yield* startup.enqueueCommand(record("command")).pipe(Effect.forkChild);
            yield* startup.awaitCommandReady.pipe(
              Effect.match({
                onSuccess: () => "ready" as const,
                onFailure: () => "failed" as const,
              }),
              Effect.flatMap((outcome) => Deferred.succeed(readiness, outcome)),
              Effect.forkChild,
            );
            yield* startup.markHttpListening(
              NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 5511),
            );
            yield* Deferred.await(entered);
            assert.deepEqual(calls, [
              "keybindings",
              "settings",
              "legacy",
              "recovery",
              "failover",
              "delegation",
              "planned",
              "relay-registration",
              "auxiliary",
              "trial",
              "source-file-validation",
            ]);
            assert.deepEqual(
              yield* input.sql`SELECT * FROM orchestration_v2_projection_threads`,
              [],
            );
            yield* Deferred.succeed(release, undefined);
            if (mode !== "projection-failure") {
              yield* Deferred.await(shellEntered);
              assert.strictEqual(
                (yield* input.sql`SELECT thread_id FROM orchestration_v2_projection_threads`)
                  .length,
                5,
              );
              assert.isFalse(yield* Deferred.isDone(activated));
              assert.isFalse(yield* Deferred.isDone(readiness));
              assert.isFalse(calls.includes("command"));
              yield* Deferred.succeed(shellRelease, undefined);
              if (mode !== "shell-read-failure") {
                yield* Deferred.await(detailEntered);
                assert.isFalse(yield* Deferred.isDone(activated));
                assert.isFalse(yield* Deferred.isDone(readiness));
                assert.isFalse(calls.includes("command"));
                yield* Deferred.succeed(detailRelease, undefined);
              }
            }
            if (mode === "success") {
              yield* startup.awaitCommandReady;
              yield* Fiber.join(command);
              yield* Deferred.await(activated);
              assert.isTrue(calls.indexOf("activate") > calls.indexOf("source-file-validation"));
              assert.isTrue(calls.indexOf("activate") > calls.indexOf("committed-shell-read"));
              assert.isTrue(calls.indexOf("activate") > calls.indexOf("committed-detail-read"));
              assert.strictEqual(yield* Deferred.await(readiness), "ready");
              assert.strictEqual(shellReads, 5);
              assert.strictEqual(detailReads, 5);
              assert.isTrue(calls.indexOf("command") > calls.indexOf("activate"));
              assert.strictEqual(
                (yield* input.projections.getShellSnapshot({ location: "active" })).threads.length,
                5,
              );
            } else {
              yield* Deferred.await(aborted);
              assert.strictEqual(
                (yield* startup.awaitCommandReady.pipe(Effect.result))._tag,
                "Failure",
              );
              assert.strictEqual((yield* Fiber.join(command).pipe(Effect.result))._tag, "Failure");
              assert.isFalse(calls.includes("activate"));
              assert.isFalse(calls.includes("ready"));
              assert.isFalse(calls.includes("welcome"));
              assert.strictEqual(yield* Deferred.await(readiness), "failed");
              const committed =
                yield* input.sql`SELECT thread_id FROM orchestration_v2_projection_threads`;
              if (mode === "projection-failure") {
                assert.deepEqual(committed, []);
                assert.strictEqual(shellReads, 0);
                assert.strictEqual(detailReads, 0);
              } else {
                assert.strictEqual(committed.length, 5);
                // The transaction already committed: failed readback aborts
                // startup and never claims rollback or grants retry authority.
                assert.strictEqual((yield* input.install.pipe(Effect.result))._tag, "Failure");
                assert.strictEqual(
                  (yield* input.sql`SELECT thread_id FROM orchestration_v2_projection_threads`)
                    .length,
                  5,
                );
              }
              assert.deepEqual(yield* input.sql`SELECT * FROM orchestration_events`, []);
              assert.deepEqual(yield* input.sql`SELECT * FROM orchestration_command_receipts`, []);
              assert.deepEqual(yield* input.sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
            }
          }).pipe(
            Effect.provide(
              Startup.layerWithOptions({
                awaitAuxiliaryParked: record("auxiliary"),
                activate: record("activate").pipe(
                  Effect.andThen(Deferred.succeed(activated, undefined)),
                ),
                abort: () =>
                  record("abort").pipe(Effect.andThen(Deferred.succeed(aborted, undefined))),
              }).pipe(Layer.provide(dependencies)),
            ),
            Effect.provideService(SyntheticPresentation, input.capability),
            Effect.provideService(Projection.ProjectionStoreV2, projected),
            Effect.provideService(FileSystem.FileSystem, gatedFs),
            Effect.provideService(Config.ServerConfig, {
              ...config,
              mode: "desktop",
              autoBootstrapProjectFromCwd: false,
              noBrowser: true,
              startupPresentation: "browser",
            }),
            Effect.provideService(ServerActivation, Deferred.await(activated)),
            Effect.ensuring(
              Effect.all([
                Deferred.succeed(release, undefined),
                Deferred.succeed(shellRelease, undefined),
                Deferred.succeed(detailRelease, undefined),
              ]),
            ),
          );
        }),
      ),
    ),
);

it.effect(
  "real empty outbox worker and question reactor cannot dispatch fixture presentation",
  () =>
    fixture((input) =>
      Effect.gen(function* () {
        yield* input.install;
        const outbox = yield* Outbox.EffectOutboxV2;
        let executions = 0,
          commands = 0;
        const worker = Worker.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              Layer.succeed(Outbox.EffectOutboxV2, outbox),
              Layer.mock(Worker.OrchestrationEffectExecutorV2)({
                execute: () => {
                  executions++;
                  return Effect.die("Presentation must not execute provider work");
                },
              }),
            ),
          ),
        );
        const claimed = yield* Worker.OrchestrationEffectWorkerV2.use(
          (service) => service.runOnce,
        ).pipe(Effect.provide(worker));
        assert.isFalse(claimed);
        assert.strictEqual(executions, 0);
        const reactor = Questions.layer.pipe(
          Layer.provide(
            Layer.mock(Orchestrator.OrchestratorV2)({
              dispatch: () => {
                commands++;
                return Effect.die("Presentation must not dispatch commands");
              },
            }),
          ),
        );
        yield* Questions.DelegatedTaskQuestionReactor.use((service) =>
          Effect.forEach(input.capability.events, (event) => service.handle(event), {
            discard: true,
          }),
        ).pipe(Effect.provide(reactor));
        assert.strictEqual(commands, 0);
        assert.deepEqual(yield* input.sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
      }),
    ),
);

it.effect(
  "wrong database binding consumes original owner and a cloned capability cannot retry",
  () =>
    fixture((input) =>
      Effect.gen(function* () {
        const wrong = {
          ...input.capability,
          descriptor: {
            ...input.capability.descriptor,
            database: input.capability.descriptor.database + "-foreign",
          },
        };
        const first = yield* installSelectedSyntheticPresentation().pipe(
          Effect.provideService(SyntheticPresentation, wrong),
          Effect.flip,
        );
        assert.strictEqual(first.reason, "fixture_binding_invalid");
        const again = yield* installSelectedSyntheticPresentation().pipe(
          Effect.provideService(SyntheticPresentation, { ...input.capability }),
          Effect.flip,
        );
        assert.strictEqual(again.reason, "fixture_owner_already_consumed");
        assert.deepEqual(yield* input.sql`SELECT * FROM orchestration_v2_projection_threads`, []);
      }),
    ),
);
it.effect(
  "an altered complete payload fails its exact descriptor hash without event, receipt or projection writes",
  () =>
    fixture((input) =>
      Effect.gen(function* () {
        const changed = {
          ...input.capability,
          events: input.capability.events.map((event) =>
            event.type === "thread.created"
              ? { ...event, payload: { ...event.payload, title: "Unapproved replacement title" } }
              : event,
          ),
        };
        const error = yield* installSelectedSyntheticPresentation().pipe(
          Effect.provideService(SyntheticPresentation, changed),
          Effect.flip,
        );
        assert.strictEqual(error.reason, "fixture_descriptor_changed");
        assert.deepEqual(yield* input.sql`SELECT * FROM orchestration_v2_projection_threads`, []);
        assert.deepEqual(yield* input.sql`SELECT * FROM orchestration_events`, []);
        assert.deepEqual(yield* input.sql`SELECT * FROM orchestration_command_receipts`, []);
      }),
    ),
);
