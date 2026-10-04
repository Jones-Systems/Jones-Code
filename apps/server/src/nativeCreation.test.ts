import {
  AuthSessionId,
  CommandId,
  EnvironmentAuthenticatedPrincipal,
  EventId,
  NativeCreationHistoricalBinding,
  NATIVE_BOOTSTRAP_MAX_SUBMISSION_BYTES,
  ORCHESTRATION_V2_WS_METHODS,
  OrchestrationDispatchCommandError,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  WsOrchestrationV2DispatchNativeBootstrapRpc,
} from "@t3tools/contracts";
import { it as effectIt } from "@effect/vitest";
import { DEFAULT_SIGNAL_EXPORT } from "@t3tools/shared/observability";
import * as OtelEnvironment from "@t3tools/shared/otelEnvironment";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as RpcGroup from "effect/unstable/rpc/RpcGroup";
import type { FromClientEncoded, FromServerEncoded } from "effect/unstable/rpc/RpcMessage";
import * as RpcServer from "effect/unstable/rpc/RpcServer";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { describe, expect, it } from "vite-plus/test";

import * as ServerConfig from "./config.ts";
import * as GitWorkflow from "./git/GitWorkflowService.ts";
import { CodexProviderCapabilitiesV2 } from "./orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as CommandReceiptStore from "./orchestration-v2/CommandReceiptStore.ts";
import * as IdAllocator from "./orchestration-v2/IdAllocator.ts";
import {
  NativeCreationAuthority,
  NativeCreationAuthorityError,
  NativeCreationAuthorityLive,
  NativeCreationBindingResolver,
  NativeCreationGrantResolver,
  type NativeCreationGrant,
} from "./orchestration-v2/NativeCreationAuthority.ts";
import {
  NativePreparationBinding,
  nativePreparationCommand,
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  validateNativeCreationPreparation,
} from "./orchestration-v2/NativeCreationPreparation.ts";
import * as ProjectStore from "./orchestration-v2/ProjectStore.ts";
import * as ProviderAdapterRegistry from "./orchestration-v2/ProviderAdapterRegistry.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as ThreadLaunch from "./orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagement from "./orchestration-v2/ThreadManagementService.ts";
import { AuthSessionRepository, make as makeAuthSessions } from "./persistence/AuthSessions.ts";
import * as NativeCreationRepositoryLayer from "./persistence/Layers/NativeCreationRepository.ts";
import { SqlitePersistenceMemory } from "./persistence/Layers/Sqlite.ts";
import {
  NativeCreationRepository,
  NativeCreationRepositoryError,
} from "./persistence/Services/NativeCreationRepository.ts";
import * as ManagedProjectFolders from "./project/ManagedProjectFolders.ts";
import * as ProjectCloneTracker from "./project/ProjectCloneTracker.ts";
import * as ProjectService from "./project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "./project/ProjectSetupScriptRunner.ts";
import * as WorktreeSetupTracker from "./project/WorktreeSetupTracker.ts";
import { makeProviderRegistryLayer } from "./provider/testUtils/providerRegistryMock.ts";
import * as ServerSettings from "./serverSettings.ts";
import * as TerminalManager from "./terminal/Manager.ts";
import * as TextGeneration from "./textGeneration/TextGeneration.ts";
import { nativeWorktreePath } from "./vcs/worktreePath.ts";
import { nativeBootstrapRpcSerialization } from "./ws.ts";

const encodeWire = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const validSubmission = {
  schema: "t3.native-bootstrap-submission/v1" as const,
  preparationBase64: "e30=",
  creationGuard: {
    schema: "t3.native-creation-guard/v1" as const,
    grantId: "synthetic-grant",
    grantRevision: 1,
  },
};
const request = (
  tag: string = ORCHESTRATION_V2_WS_METHODS.dispatchNativeBootstrap,
  payload: unknown = validSubmission,
) => ({ _tag: "Request", id: "1", tag, payload, headers: [] });
const oversize = (text: string) => `${" ".repeat(NATIVE_BOOTSTRAP_MAX_SUBMISSION_BYTES)}${text}`;

for (const bootstrapTag of [
  "orchestration.dispatchBootstrap",
  ORCHESTRATION_V2_WS_METHODS.dispatchNativeBootstrap,
]) {
  describe(`native guarded received-message bound: ${bootstrapTag}`, () => {
    const bootstrapRequest = () => request(bootstrapTag);
    for (const binary of [false, true]) {
      it(`rejects original ${binary ? "binary" : "text"} whitespace bytes before returning requests`, () => {
        const parser = nativeBootstrapRpcSerialization.makeUnsafe();
        const text = oversize(encodeWire(bootstrapRequest()));
        expect(() => parser.decode(binary ? new TextEncoder().encode(text) : text)).toThrow(
          RpcSerialization.MaxBufferSizeExceeded,
        );
      });
    }
    it("counts envelope headers and Unicode bytes", () => {
      const parser = nativeBootstrapRpcSerialization.makeUnsafe();
      const input = {
        ...bootstrapRequest(),
        headers: [["synthetic", "é".repeat(NATIVE_BOOTSTRAP_MAX_SUBMISSION_BYTES / 2)]],
      };
      expect(() => parser.decode(encodeWire(input))).toThrow(
        RpcSerialization.MaxBufferSizeExceeded,
      );
    });
    it("rejects all siblings in an oversized bootstrap batch with zero dispatches", () => {
      let effects = 0;
      const parser = nativeBootstrapRpcSerialization.makeUnsafe();
      expect(() => {
        for (const _request of parser.decode(
          oversize(encodeWire([request("server.getConfig"), bootstrapRequest()])),
        ))
          effects++;
      }).toThrow(RpcSerialization.MaxBufferSizeExceeded);
      expect(effects).toBe(0);
    });
    it("keeps ordinary and legacy RPC serialization unchanged", () => {
      for (const tag of [
        "server.getConfig",
        "orchestration.dispatchCommand",
        ORCHESTRATION_V2_WS_METHODS.dispatchCommand,
      ]) {
        const text = oversize(encodeWire(request(tag)));
        expect(nativeBootstrapRpcSerialization.makeUnsafe().decode(text)).toEqual(
          RpcSerialization.json.makeUnsafe().decode(text),
        );
      }
    });
    it("counts the exact Uint8Array view rather than its backing buffer", () => {
      const bytes = new TextEncoder().encode(encodeWire(bootstrapRequest()));
      const backing = new Uint8Array(NATIVE_BOOTSTRAP_MAX_SUBMISSION_BYTES + bytes.length);
      backing.set(bytes, NATIVE_BOOTSTRAP_MAX_SUBMISSION_BYTES);
      expect(
        nativeBootstrapRpcSerialization
          .makeUnsafe()
          .decode(backing.subarray(NATIVE_BOOTSTRAP_MAX_SUBMISSION_BYTES)),
      ).toEqual([bootstrapRequest()]);
    });
    it("returns the native decoder result for an in-bound batch", () => {
      const text = `  ${encodeWire([bootstrapRequest(), request("server.getConfig")])}  `;
      expect(nativeBootstrapRpcSerialization.makeUnsafe().decode(text)).toEqual(
        RpcSerialization.json.makeUnsafe().decode(text),
      );
    });
  });
}

const group = RpcGroup.make(WsOrchestrationV2DispatchNativeBootstrapRpc);
const nativeWire = (payload: unknown) =>
  Effect.gen(function* () {
    const requests = yield* Queue.unbounded<FromClientEncoded>();
    const responses = yield* Queue.unbounded<FromServerEncoded>();
    const disconnects = yield* Queue.unbounded<number>();
    let effects = 0;
    const protocol = RpcServer.Protocol.of({
      run: (receive) =>
        Queue.take(requests).pipe(
          Effect.flatMap((message) => receive(1, message)),
          Effect.forever,
        ),
      disconnects,
      send: (_clientId, message) => Queue.offer(responses, message).pipe(Effect.asVoid),
      end: () => Effect.void,
      clientIds: Effect.succeed(new Set([1])),
      initialMessage: Effect.succeed(Option.none()),
      supportsAck: false,
      supportsTransferables: false,
      supportsSpanPropagation: false,
      supportsNotifications: false,
      codecFor: nativeBootstrapRpcSerialization.codecFor,
    });
    yield* RpcServer.make(group).pipe(
      Effect.provideService(RpcServer.Protocol, protocol),
      Effect.provide(
        group.toLayer({
          [ORCHESTRATION_V2_WS_METHODS.dispatchNativeBootstrap]: () =>
            Effect.sync(() => {
              effects++;
            }).pipe(
              Effect.andThen(
                Effect.fail(
                  new OrchestrationDispatchCommandError({
                    message: "Codec fixture stops before native dispatch",
                    creationRejectionCode: "unsupported_authority",
                  }),
                ),
              ),
            ),
        }),
      ),
      Effect.forkScoped,
    );
    const decoded = nativeBootstrapRpcSerialization
      .makeUnsafe()
      .decode(encodeWire(request(ORCHESTRATION_V2_WS_METHODS.dispatchNativeBootstrap, payload)));
    for (const message of decoded) yield* Queue.offer(requests, message as FromClientEncoded);
    const response = yield* Queue.take(responses);
    return { response, effects };
  });
effectIt.effect("actual native RPC codecs accept the guarded outer schema", () =>
  nativeWire(request().payload).pipe(
    Effect.tap(({ response, effects }) =>
      Effect.sync(() => {
        expect(effects).toBe(1);
        expect(response._tag).toBe("Exit");
      }),
    ),
    Effect.scoped,
  ),
);
effectIt.effect(
  "actual native RPC codecs reject an excess outer field before handler effects",
  () =>
    nativeWire({ ...validSubmission, unexpected: true }).pipe(
      Effect.tap(({ effects }) =>
        Effect.sync(() => {
          expect(effects).toBe(0);
        }),
      ),
      Effect.scoped,
    ),
);

const producerFixture = (text = "Synthetic immutable text") => {
  const binding = Schema.decodeUnknownSync(NativePreparationBinding)({
    backend_instance: "synthetic-backend",
    environment_id: "synthetic-env",
    project_id: "synthetic-project",
    project_cwd: "/synthetic/project",
    account_ref: "synthetic-account",
    runtime_mode: "full-access",
    interaction_mode: "default",
    base_branch: "main",
    start_from_origin: false,
    run_setup_script: false,
    provider_model_selection: { instanceId: "codex", model: "synthetic-model" },
  });
  const command = nativePreparationCommand(
    "synthetic-operation",
    binding,
    text,
    "Synthetic thread",
    "2026-10-02T12:00:00Z",
  );
  const preparation = nativeCreationCanonicalJson({
    schema: "voice.t3-bootstrap-preparation/v1",
    operation_id: "synthetic-operation",
    preparation_id: command.commandId.replace("voice-command-", "voice-bootstrap-"),
    binding,
    command,
    binding_digest: nativeCreationSha256(nativeCreationCanonicalJson(binding)),
    prompt_digest: nativeCreationSha256(text),
    command_digest: nativeCreationSha256(nativeCreationCanonicalJson(command)),
  });
  const historical = Schema.decodeUnknownSync(NativeCreationHistoricalBinding)({
    backendInstance: binding.backend_instance,
    environmentId: binding.environment_id,
    projectId: binding.project_id,
    projectCwd: binding.project_cwd,
    accountRef: binding.account_ref,
    accountBindingId: "synthetic-account-binding",
    accountBindingRevision: 1,
    providerModelSelection: binding.provider_model_selection,
    runtimeMode: binding.runtime_mode,
    interactionMode: binding.interaction_mode,
    baseBranch: binding.base_branch,
    startFromOrigin: false,
    runSetupScript: false,
    requestedBranch: command.bootstrap.prepareWorktree.branch,
  });
  return {
    historical,
    command,
    submission: {
      schema: "t3.native-bootstrap-submission/v1" as const,
      preparationBase64: Buffer.from(preparation).toString("base64"),
      creationGuard: {
        schema: "t3.native-creation-guard/v1" as const,
        grantId: "synthetic-grant",
        grantRevision: 1,
      },
    },
  };
};
const preparedFixture = producerFixture();

// Launch reads only `worktreesDir`; the remaining fields are a complete synthetic config.
const syntheticServerConfig = (worktreesDir: string) =>
  Layer.effect(
    ServerConfig.ServerConfig,
    Effect.gen(function* () {
      const baseDir = "/synthetic/native-creation";
      return ServerConfig.make({
        ...(yield* ServerConfig.deriveServerPaths(baseDir, undefined)),
        worktreesDir,
        logLevel: "Error",
        traceMinLevel: "Info",
        traceTimingEnabled: true,
        traceBatchWindowMs: 200,
        traceMaxBytes: 10 * 1024 * 1024,
        traceMaxFiles: 10,
        otlpTracesUrl: undefined,
        otlpMetricsUrl: undefined,
        otlpLogsUrl: undefined,
        otlpTracesExport: DEFAULT_SIGNAL_EXPORT,
        otlpMetricsExport: DEFAULT_SIGNAL_EXPORT,
        otlpLogsExport: DEFAULT_SIGNAL_EXPORT,
        otelEnvironment: OtelEnvironment.none,
        cwd: preparedFixture.historical.projectCwd,
        baseDir,
        mode: "web",
        autoBootstrapProjectFromCwd: false,
        logWebSocketEvents: false,
        tailscaleServeEnabled: false,
        tailscaleServePort: 443,
        port: 0,
        host: undefined,
        desktopBootstrapToken: undefined,
        staticDir: undefined,
        devUrl: undefined,
        devAllowedOrigins: [],
        noBrowser: false,
        startupPresentation: "browser",
      });
    }),
  ).pipe(Layer.provide(Path.layer));

function makeNativeFixture(mode: "accepted" | "revoked" | "unavailable" | "lost-normalization") {
  const database = SqlitePersistenceMemory;
  const repositoryLayer = NativeCreationRepositoryLayer.layer.pipe(Layer.provide(database));
  const projectStore = ProjectStore.layer.pipe(Layer.provide(database));
  const projectId = ProjectId.make(preparedFixture.historical.projectId);
  const worktreesDir = "/synthetic/worktrees";
  let grant: NativeCreationGrant | undefined;
  let normalizations = 0;
  let dispatches = 0;
  let worktrees = 0;
  let setupCalls = 0;
  const createdPaths = new Set<string>();
  const baseAuthority = NativeCreationAuthorityLive.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.effect(AuthSessionRepository, makeAuthSessions).pipe(Layer.provide(database)),
        repositoryLayer,
        Layer.succeed(NativeCreationGrantResolver, {
          resolveCurrent: () =>
            grant === undefined || mode === "unavailable"
              ? Effect.fail(
                  new NativeCreationAuthorityError({
                    code: "unsupported_authority",
                    message: "Synthetic unavailable authority",
                  }),
                )
              : Effect.succeed({
                  enrolledSessionId: grant.actorSessionId,
                  trustedIssuerId: grant.issuerId,
                  grant,
                }),
        }),
        Layer.succeed(NativeCreationBindingResolver, {
          resolveCurrent: () => Effect.succeed(preparedFixture.historical),
        }),
      ),
    ),
  );
  const authority = Layer.effect(
    NativeCreationAuthority,
    Effect.gen(function* () {
      const actual = yield* NativeCreationAuthority;
      const sql = yield* SqlClient.SqlClient;
      return NativeCreationAuthority.of({
        ...actual,
        authorize: (input) =>
          Effect.gen(function* () {
            if (input.stage === "normalization") {
              expect(
                yield* sql`SELECT claim_id FROM native_creation_intents`.pipe(Effect.orDie),
              ).toHaveLength(1);
              expect(
                yield* sql`SELECT command_id FROM native_creation_reserved_command_identities`.pipe(
                  Effect.orDie,
                ),
              ).toHaveLength(0);
              if (mode === "revoked")
                return yield* new NativeCreationAuthorityError({
                  code: "stale_grant",
                  message: "Synthetic revoked grant",
                });
            }
            return yield* actual.authorize(input);
          }),
      });
    }),
  ).pipe(Layer.provide(Layer.mergeAll(baseAuthority, database)));
  const registry = ProviderAdapterRegistry.makeLayer([
    {
      instanceId: ProviderInstanceId.make("codex"),
      driver: ProviderDriverKind.make("codex"),
      getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
      planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
      openSession: () => Effect.die("Native fixture must not start a provider"),
    },
  ]);
  const orchestrator = makeOrchestratorV2ReplayLayerWithRegistry(
    { name: "native-creation-entry" },
    registry,
    { databaseLayer: database, runEffectWorker: false },
  ).pipe(Layer.provide(Layer.mergeAll(database, repositoryLayer, authority)));
  const originalThreads = ThreadManagement.layer.pipe(Layer.provide(orchestrator));
  const threads = Layer.effect(
    ThreadManagement.ThreadManagementService,
    Effect.gen(function* () {
      const actual = yield* ThreadManagement.ThreadManagementService;
      const repository = yield* NativeCreationRepository;
      const sql = yield* SqlClient.SqlClient;
      return ThreadManagement.ThreadManagementService.of({
        ...actual,
        dispatchNativeCreationStage: (command, input) =>
          Effect.gen(function* () {
            const ids = yield* sql<{
              command_id: string;
            }>`SELECT command_id FROM native_creation_reserved_command_identities ORDER BY command_id`.pipe(
              Effect.orDie,
            );
            expect(ids.map((row) => row.command_id)).toEqual(
              [
                preparedFixture.command.commandId,
                `${preparedFixture.command.commandId}:native:v2:create`,
                `${preparedFixture.command.commandId}:native:v2:message`,
              ].sort(),
            );
            expect(
              yield* sql`SELECT claim_id FROM native_creation_intents`.pipe(Effect.orDie),
            ).toHaveLength(1);
            const history = yield* repository.readHistoryByClaim(input.claimId).pipe(Effect.orDie);
            expect(history.normalizedCommandDigest).not.toBeNull();
            expect(history.normalizedCommandDigest).not.toBe(history.intent.commandDigest);
            expect(
              history.effects.some(
                (fact) =>
                  fact.kind === "lifecycle" &&
                  fact.action === "normalization" &&
                  fact.phase === "completed" &&
                  fact.result === "succeeded",
              ),
            ).toBe(true);
            dispatches++;
            return yield* actual.dispatchNativeCreationStage(command, input);
          }),
      });
    }),
  ).pipe(Layer.provide(Layer.mergeAll(originalThreads, repositoryLayer, database)));
  const external = Layer.mergeAll(
    syntheticServerConfig(worktreesDir),
    FileSystem.layerNoop({
      realPath: (path) => Effect.succeed(path),
      exists: (path) => Effect.succeed(createdPaths.has(path)),
    }),
    WorktreeSetupTracker.layer,
    Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({ get: () => Effect.succeed(null) }),
    Layer.mock(TerminalManager.TerminalManager)({ close: () => Effect.void }),
    Layer.mock(ProjectService.ProjectService)({
      getById: (id) =>
        Effect.succeed(
          id === projectId
            ? Option.some({
                id: projectId,
                title: "Synthetic project",
                workspaceRoot: preparedFixture.historical.projectCwd,
                repositoryIdentity: null,
                faviconPath: null,
                defaultModelSelection: preparedFixture.historical.providerModelSelection,
                defaultThreadEnvMode: null,
                scripts: [],
                createdAt: "2026-10-02T12:00:00Z",
                updatedAt: "2026-10-02T12:00:00Z",
                deletedAt: null,
              })
            : Option.none(),
        ),
    }),
    Layer.mock(GitWorkflow.GitWorkflowService)({
      createWorktree: (input) =>
        Effect.sync(() => {
          if (input.path === undefined || input.path === null || input.newRefName === undefined)
            throw new Error("Native fixture requires exact worktree resources");
          worktrees++;
          createdPaths.add(input.path);
          return { worktree: { path: input.path, refName: input.newRefName } };
        }),
    }),
    Layer.mock(ProjectSetupScriptRunner.ProjectSetupScriptRunner)({
      runForThread: () =>
        Effect.sync(() => {
          setupCalls++;
          return { status: "no-script" as const };
        }),
    }),
    Layer.mock(TextGeneration.TextGeneration)({}),
    ServerSettings.layerTest(),
    makeProviderRegistryLayer(),
    Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
      namedProjectsRoot: "/synthetic/native-creation/projects",
    }),
  );
  const launch = ThreadLaunch.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        CommandReceiptStore.layer.pipe(Layer.provide(database)),
        IdAllocator.layer,
        orchestrator,
        threads,
        external,
      ),
    ),
  );
  const layer = Layer.mergeAll(
    launch,
    orchestrator,
    threads,
    projectStore,
    repositoryLayer,
    authority,
    database,
    external,
  );
  const prepare = Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const sessions = yield* makeAuthSessions;
    const projects = yield* ProjectStore.ProjectStoreV2;
    const now = yield* DateTime.now;
    const timestamp = DateTime.formatIso(now);
    yield* projects.apply({
      sequence: 0,
      eventId: EventId.make("native-fixture-project"),
      aggregateKind: "project",
      aggregateId: projectId,
      occurredAt: timestamp,
      commandId: null,
      causationEventId: null,
      correlationId: null,
      metadata: {},
      type: "project.created",
      payload: {
        projectId,
        title: "Synthetic project",
        workspaceRoot: preparedFixture.historical.projectCwd,
        defaultModelSelection: preparedFixture.historical.providerModelSelection,
        scripts: [],
        createdAt: timestamp,
        updatedAt: timestamp,
      },
    });
    const sessionId = AuthSessionId.make("synthetic-session");
    const principal: EnvironmentAuthenticatedPrincipal["Service"] = {
      sessionId,
      subject: "synthetic-native-fixture",
      method: "bearer-access-token",
      scopes: new Set(["orchestration:read", "orchestration:operate"]),
    };
    yield* sessions.create({
      sessionId,
      subject: principal.subject,
      method: principal.method,
      scopes: [...principal.scopes],
      issuedAt: now,
      expiresAt: DateTime.add(now, { days: 1 }),
      client: {
        label: null,
        ipAddress: null,
        userAgent: null,
        deviceType: "bot",
        os: null,
        browser: null,
      },
    });
    yield* sql`INSERT INTO native_creation_automation_enrollments (session_id, enrolled_at) VALUES (${sessionId}, ${timestamp})`;
    const preparation = yield* validateNativeCreationPreparation(
      Buffer.from(preparedFixture.submission.preparationBase64, "base64"),
    );
    grant = {
      grantId: "synthetic-grant",
      revision: 1,
      actorSessionId: sessionId,
      issuerId: "synthetic-issuer",
      expiresAt: DateTime.add(now, { days: 1 }),
      revoked: false,
      operationId: preparation.operationId,
      preparationId: preparation.preparationId,
      preparationSha256: preparation.preparationSha256,
      bindingDigest: preparation.bindingDigest,
      binding: preparedFixture.historical,
      resources: {
        projectCwd: preparation.binding.project_cwd,
        branch: preparation.command.bootstrap.prepareWorktree.branch,
        worktreePath: nativeWorktreePath({
          worktreesDir,
          cwd: preparation.binding.project_cwd,
          branch: preparation.command.bootstrap.prepareWorktree.branch,
        }),
      },
      allowedStages: [
        "claim",
        "normalization",
        "tracker_registration",
        "bootstrap_detachment",
        "worktree_ownership",
        "worktree",
        "setup_detachment",
        "native_command",
      ],
      recoveryScopes: [],
    };
    const repository = yield* NativeCreationRepository;
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const observedRepository = NativeCreationRepository.of({
      ...repository,
      recordNormalizedCommand: (claimId, command) =>
        repository.recordNormalizedCommand(claimId, command).pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              normalizations++;
            }),
          ),
          Effect.andThen(() =>
            mode === "lost-normalization"
              ? Effect.fail(
                  new NativeCreationRepositoryError({
                    code: "unresolved_claim",
                    message: "Synthetic lost response after durable normalization",
                  }),
                )
              : Effect.void,
          ),
        ),
    });
    const dispatch = launches
      .dispatchNativeBootstrap(preparedFixture.submission, {
        nativeCreationBootId: "synthetic-boot",
      })
      .pipe(
        Effect.provideService(EnvironmentAuthenticatedPrincipal, principal),
        Effect.provideService(NativeCreationRepository, observedRepository),
      );
    return { dispatch, repository, sql };
  });
  return { layer, prepare, counts: () => ({ normalizations, dispatches, worktrees, setupCalls }) };
}

effectIt.effect(
  "real SQL claim and all V2 command identities commit before native dispatch",
  () => {
    const harness = makeNativeFixture("accepted");
    return Effect.gen(function* () {
      const { dispatch, repository, sql } = yield* harness.prepare;
      const result = yield* dispatch;
      expect(result.commandAcceptance).toBe("accepted");
      expect(result.creation?.stageCommands).toHaveLength(3);
      expect(
        result.creation?.stageCommands.every(
          (stage) =>
            stage.receipt?.status === "accepted" &&
            stage.event?.sequence === stage.receipt.resultSequence,
        ),
      ).toBe(true);
      expect(harness.counts()).toEqual({
        normalizations: 1,
        dispatches: 3,
        worktrees: 1,
        setupCalls: 0,
      });
      expect(yield* dispatch).toEqual(result);
      expect(harness.counts()).toEqual({
        normalizations: 1,
        dispatches: 3,
        worktrees: 1,
        setupCalls: 0,
      });
      const history = yield* repository.readHistory(preparedFixture.command.commandId);
      expect(Option.isSome(history)).toBe(true);
      if (Option.isNone(history)) return;
      const ids = [
        CommandId.make(preparedFixture.command.commandId),
        CommandId.make(`${preparedFixture.command.commandId}:native:v2:create`),
        CommandId.make(`${preparedFixture.command.commandId}:native:v2:message`),
      ];
      const invalidInventory = yield* repository
        .reserveCommandIdentities(history.value.intent.claimId, [
          ...ids,
          CommandId.make("unrelated-native-command"),
        ])
        .pipe(Effect.result);
      expect(invalidInventory._tag).toBe("Failure");
      expect(
        yield* sql`SELECT command_id FROM native_creation_reserved_command_identities`,
      ).toHaveLength(3);
      expect(
        yield* sql`SELECT command_id FROM native_creation_thread_recovery_commands`,
      ).toHaveLength(0);
    }).pipe(Effect.provide(harness.layer));
  },
);

effectIt.effect(
  "unresolved normalization retains its committed history and rejects replay without dispatch",
  () => {
    const harness = makeNativeFixture("lost-normalization");
    return Effect.gen(function* () {
      const { dispatch, repository } = yield* harness.prepare;
      const first = yield* dispatch.pipe(Effect.flip);
      expect(first._tag === "NativeCreationAuthorityError" && first.code).toBe("unresolved_claim");
      const duplicate = yield* dispatch.pipe(Effect.flip);
      expect(duplicate._tag === "NativeCreationAuthorityError" && duplicate.code).toBe(
        "unresolved_claim",
      );
      expect(harness.counts()).toEqual({
        normalizations: 1,
        dispatches: 0,
        worktrees: 0,
        setupCalls: 0,
      });
      const history = yield* repository.readHistory(preparedFixture.command.commandId);
      expect(Option.isSome(history)).toBe(true);
      if (Option.isSome(history)) {
        expect(history.value.normalizedCommandDigest).not.toBeNull();
        expect(
          history.value.effects.some(
            (fact) =>
              fact.kind === "lifecycle" &&
              fact.action === "normalization" &&
              fact.phase === "completed" &&
              fact.result === "unknown",
          ),
        ).toBe(true);
      }
    }).pipe(Effect.provide(harness.layer));
  },
);

effectIt.effect(
  "authority revocation between claim and normalization preserves claimed history with zero bootstrap effects",
  () => {
    const harness = makeNativeFixture("revoked");
    return Effect.gen(function* () {
      const { dispatch, repository } = yield* harness.prepare;
      const rejected = yield* dispatch.pipe(Effect.flip);
      expect(rejected._tag === "NativeCreationAuthorityError" && rejected.code).toBe("stale_grant");
      expect(harness.counts()).toEqual({
        normalizations: 0,
        dispatches: 0,
        worktrees: 0,
        setupCalls: 0,
      });
      const history = yield* repository.readHistory(preparedFixture.command.commandId);
      expect(Option.isSome(history)).toBe(true);
      if (Option.isSome(history)) {
        expect(history.value.normalizedCommandDigest).toBeNull();
        expect(history.value.effects).toHaveLength(0);
      }
    }).pipe(Effect.provide(harness.layer));
  },
);

effectIt.effect(
  "unavailable native authority rejects before durable claims, normalization or dispatch",
  () => {
    const harness = makeNativeFixture("unavailable");
    return Effect.gen(function* () {
      const { dispatch, sql } = yield* harness.prepare;
      const rejected = yield* dispatch.pipe(Effect.flip);
      expect(rejected._tag === "NativeCreationAuthorityError" && rejected.code).toBe(
        "unsupported_authority",
      );
      expect(harness.counts()).toEqual({
        normalizations: 0,
        dispatches: 0,
        worktrees: 0,
        setupCalls: 0,
      });
      expect(yield* sql`SELECT claim_id FROM native_creation_intents`).toHaveLength(0);
    }).pipe(Effect.provide(harness.layer));
  },
);
