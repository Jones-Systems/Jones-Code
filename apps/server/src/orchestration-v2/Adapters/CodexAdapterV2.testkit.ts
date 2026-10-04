import * as OtelEnvironment from "@t3tools/shared/otelEnvironment";
import { DEFAULT_SIGNAL_EXPORT } from "@t3tools/shared/observability";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AuthSessionId,
  NativeCommandIdentityV2,
  NativeCreationHistoricalBinding,
  OrchestrationV2Command,
  type ProviderReplayTranscript,
} from "@t3tools/contracts";
import * as CodexClient from "effect-codex-app-server/client";
import * as CodexReplay from "effect-codex-app-server/replay";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

import * as ServerConfig from "../../config.ts";
import * as IdAllocator from "../IdAllocator.ts";
import { AuthSessionRecord, AuthSessionRepository } from "../../persistence/AuthSessions.ts";
import {
  NativeCreationRepository,
  NativeCreationRepositoryError,
  type NativeCreationResolvedExecutionV2,
} from "../../persistence/Services/NativeCreationRepository.ts";
import {
  makeNativeCreationAuthority,
  NativeCreationBindingResolver,
  NativeCreationExecutionReferenceV2,
  NativeCreationGrantResolver,
  type NativeCreationGrant,
} from "../NativeCreationAuthority.ts";
import {
  NativePreparationBinding,
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  nativeCreationV2CommandDigest,
  nativePreparationCommand,
  validateNativeCreationPreparation,
} from "../NativeCreationPreparation.ts";
import { ProviderAdapterOpenSessionError } from "../ProviderAdapter.ts";
import { ProviderAdapterDriverCreateError } from "../ProviderAdapterDriver.ts";
import * as ProviderAdapterRegistry from "../ProviderAdapterRegistry.ts";
import type { OrchestratorV2ProviderReplayHarness } from "../testkit/ProviderReplayHarness.ts";
import type { ProviderReplayGate } from "../testkit/ProviderReplayGate.testkit.ts";
import * as CodexAdapterV2 from "./CodexAdapterV2.ts";

export const makeCodexNativeCreationFixture = (
  scenario: string,
  hooks: { readonly beforeGrantRead?: () => Effect.Effect<void> } = {},
) =>
  Effect.gen(function* () {
    const actorSessionId = AuthSessionId.make(`codex-fixture-actor-${scenario}`);
    const binding = yield* Schema.decodeUnknownEffect(NativePreparationBinding)({
      backend_instance: "synthetic-backend",
      environment_id: "synthetic-environment",
      project_id: "synthetic-project",
      project_cwd: "/synthetic/project",
      account_ref: "synthetic-account",
      runtime_mode: "full-access",
      interaction_mode: "default",
      base_branch: "main",
      start_from_origin: false,
      run_setup_script: false,
      provider_model_selection: { instanceId: "codex", model: "gpt-5.4" },
    });
    const preparedCommand = nativePreparationCommand(
      `codex-authority-${scenario}`,
      binding,
      "Synthetic prompt",
      "Synthetic thread",
      "2026-10-03T12:00:00Z",
    );
    const preparation = yield* validateNativeCreationPreparation(
      new TextEncoder().encode(
        nativeCreationCanonicalJson({
          schema: "voice.t3-bootstrap-preparation/v1",
          operation_id: `codex-authority-${scenario}`,
          binding,
          command: preparedCommand,
          preparation_id: preparedCommand.commandId.replace("voice-command-", "voice-bootstrap-"),
          binding_digest: nativeCreationSha256(nativeCreationCanonicalJson(binding)),
          prompt_digest: nativeCreationSha256(preparedCommand.message.text),
          command_digest: nativeCreationSha256(nativeCreationCanonicalJson(preparedCommand)),
        }),
      ),
    );
    const historical = yield* Schema.decodeUnknownEffect(NativeCreationHistoricalBinding)({
      backendInstance: binding.backend_instance,
      environmentId: binding.environment_id,
      projectId: binding.project_id,
      projectCwd: binding.project_cwd,
      accountRef: binding.account_ref,
      accountBindingId: "synthetic-qualified-account",
      accountBindingRevision: 1,
      providerModelSelection: binding.provider_model_selection,
      runtimeMode: binding.runtime_mode,
      interactionMode: binding.interaction_mode,
      baseBranch: binding.base_branch,
      startFromOrigin: false,
      runSetupScript: false,
      requestedBranch: preparedCommand.bootstrap.prepareWorktree.branch,
    });
    const resources = Object.freeze({
      projectCwd: historical.projectCwd,
      branch: historical.requestedBranch,
      worktreePath: "/synthetic/worktree",
    });
    const reference = yield* Schema.decodeUnknownEffect(NativeCreationExecutionReferenceV2)({
      version: 2,
      claimId: `synthetic-claim-${scenario}`,
      stage: "native_command",
      stageCommandId: `${preparedCommand.commandId}:native:v2:create`,
      effectId: `synthetic-effect-${scenario}`,
    });
    const command = yield* Schema.decodeUnknownEffect(OrchestrationV2Command)({
      type: "thread.create",
      commandId: reference.stageCommandId,
      threadId: preparedCommand.threadId,
      ...preparedCommand.bootstrap.createThread,
      createdBy: "user",
      creationSource: "server",
    });
    if (command.type !== "thread.create")
      return yield* Effect.die("Synthetic native creation command was not a thread create");
    const commandDigest = nativeCreationV2CommandDigest(command);
    const nativeIdentity = yield* Schema.decodeUnknownEffect(NativeCommandIdentityV2)({
      kind: "native_creation_stage",
      version: 2,
      commandId: reference.stageCommandId,
      commandType: command.type,
      aggregateKind: "thread",
      aggregateId: command.threadId,
      normalizedCommandDigest: commandDigest,
      bindingDigest: preparation.bindingDigest,
    });
    const grantId = `synthetic-grant-${scenario}`;
    let grant: NativeCreationGrant = {
      grantId,
      revision: 1,
      actorSessionId,
      issuerId: "synthetic-issuer",
      expiresAt: DateTime.makeUnsafe("2099-01-01T00:00:00Z"),
      revoked: false,
      operationId: preparation.operationId,
      preparationId: preparation.preparationId,
      preparationSha256: preparation.preparationSha256,
      bindingDigest: preparation.bindingDigest,
      binding: historical,
      resources,
      allowedStages: ["native_command"],
      recoveryScopes: [],
    };
    const session = yield* Schema.decodeUnknownEffect(AuthSessionRecord)({
      sessionId: actorSessionId,
      subject: "Synthetic actor",
      scopes: ["orchestration:operate"],
      method: "bearer-access-token",
      client: {
        label: null,
        ipAddress: null,
        userAgent: null,
        deviceType: "bot",
        os: null,
        browser: null,
      },
      issuedAt: "2026-01-01T00:00:00Z",
      expiresAt: "2099-01-01T00:00:00Z",
      revokedAt: null,
      lastConnectedAt: null,
    });
    const resolved: NativeCreationResolvedExecutionV2 = {
      reference,
      command,
      nativeIdentity,
      preparation,
      history: {
        intent: {
          claimId: reference.claimId,
          claimedBootId: "synthetic-boot",
          claimedAt: "2026-10-03T12:00:00Z",
          actorSessionId,
          grantId,
          grantRevision: 1,
          preparationId: preparation.preparationId,
          operationId: preparation.operationId,
          preparationSha256: preparation.preparationSha256,
          bindingDigest: preparation.bindingDigest,
          promptDigest: preparation.promptDigest,
          commandDigest: preparation.commandDigest,
          commandId: preparedCommand.commandId,
          threadId: preparedCommand.threadId,
          messageId: preparedCommand.message.messageId,
          canonicalPreparation: preparation.canonicalText,
          binding: historical,
          resources,
        },
        normalizedCommandDigest: null,
        effects: [],
        effectsV2: [],
        effectOverflow: false,
      },
    };
    let starts = 0;
    let authorizationReads = 0;
    const unexpected = () =>
      Effect.die("Unexpected synthetic native-creation repository operation");
    const ledger = NativeCreationRepository.of({
      hasAutomationEnrollment: () => Effect.succeed(true),
      claim: unexpected,
      readHistory: unexpected,
      readHistoryByClaim: unexpected,
      readBoundedHistoryByThread: unexpected,
      validateCommandAcceptanceV2: unexpected,
      readExecutionReference: () => Effect.succeed(resolved),
      startEffectV2: (actual, timestamp, authorize) =>
        Effect.gen(function* () {
          yield* authorize;
          if (starts > 0)
            return yield* new NativeCreationRepositoryError({
              code: "unresolved_claim",
              message: "Synthetic native effect already started",
            });
          starts++;
          return {
            status: "started" as const,
            fact: {
              version: 2 as const,
              kind: "native_command" as const,
              phase: "started" as const,
              effectId: actual.effectId,
              ordinal: 0,
              timestamp,
              commandId: command.commandId,
              threadId: command.threadId,
              commandType: command.type,
              commandDigest,
            },
          };
        }),
      completeEffectV2: unexpected,
      reserveCommandIdentities: unexpected,
      getReservedCommandIdentity: unexpected,
      recordNormalizedCommand: unexpected,
      reserveCommand: unexpected,
      getReservedCommand: unexpected,
      reserveThreadRecoveryCommand: unexpected,
      readThreadRecoveryCommand: unexpected,
      recordNativeEffectConfirmation: unexpected,
      readNativeEffectConfirmation: unexpected,
      startEffect: unexpected,
      completeEffect: unexpected,
    });
    const sessions = AuthSessionRepository.of({
      create: unexpected,
      createReplacingActive: unexpected,
      createIfAbsent: unexpected,
      getById: () => Effect.succeed(Option.some(session)),
      listActive: unexpected,
      revoke: unexpected,
      revokeAllExcept: unexpected,
      setLastConnectedAt: unexpected,
      setClientConnection: unexpected,
    });
    const authority = yield* makeNativeCreationAuthority.pipe(
      Effect.provideService(NativeCreationRepository, ledger),
      Effect.provideService(AuthSessionRepository, sessions),
      Effect.provideService(NativeCreationBindingResolver, {
        resolveCurrent: () => Effect.succeed(historical),
      }),
      Effect.provideService(NativeCreationGrantResolver, {
        resolveCurrent: () =>
          (hooks.beforeGrantRead?.() ?? Effect.void).pipe(
            Effect.andThen(
              Effect.sync(() => {
                authorizationReads++;
                return {
                  enrolledSessionId: actorSessionId,
                  trustedIssuerId: "synthetic-issuer",
                  grant,
                };
              }),
            ),
          ),
      }),
    );
    const context = yield* authority.issueExecution({
      reference,
      timestamp: "2026-10-03T12:00:00Z",
    });
    return {
      execution: Object.freeze({ context, resources }),
      revoke: Effect.sync(() => {
        grant = { ...grant, revoked: true };
      }),
      get startCount() {
        return starts;
      },
      get authorizationReads() {
        return authorizationReads;
      },
    };
  });

export class CodexReplayTranscriptDecodeError extends Schema.TaggedError<CodexReplayTranscriptDecodeError>()(
  "CodexReplayTranscriptDecodeError",
  {
    driver: Schema.optional(Schema.String),
    protocol: Schema.optional(Schema.String),
    scenario: Schema.optional(Schema.String),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to decode Codex app-server replay transcript for scenario ${this.scenario ?? "<unknown>"}.`;
  }
}

export const CodexOrchestratorReplayHarnessError = Schema.Union([
  CodexReplayTranscriptDecodeError,
  CodexReplay.CodexAppServerReplayError,
  ProviderAdapterDriverCreateError,
]);
export type CodexOrchestratorReplayHarnessError = typeof CodexOrchestratorReplayHarnessError.Type;

export function withCodexReplayChildMetadata(
  client: CodexClient.CodexAppServerClient["Service"],
  transcript: CodexReplay.CodexAppServerReplayTranscript,
  readMetadata: (threadId: string) => Effect.Effect<unknown> = (threadId) =>
    Effect.succeed({ thread: { id: threadId }, model: null }),
): CodexClient.CodexAppServerClient["Service"] {
  const childThreadIds = new Set(
    transcript.entries.flatMap((entry) => {
      if (entry.type !== "emit_inbound" || !Predicate.isObject(entry.frame)) return [];
      const params = entry.frame.params;
      if (!Predicate.isObject(params) || !Predicate.isObject(params.item)) return [];
      const item = params.item;
      if (item.type === "subAgentActivity" && typeof item.agentThreadId === "string") {
        return [item.agentThreadId];
      }
      return item.type === "collabAgentToolCall" && Array.isArray(item.receiverThreadIds)
        ? item.receiverThreadIds.filter(Predicate.isString)
        : [];
    }),
  );
  return {
    ...client,
    raw: {
      ...client.raw,
      request: (method, params) =>
        method === "thread/resume" &&
        Predicate.isObject(params) &&
        params.excludeTurns === true &&
        typeof params.threadId === "string" &&
        childThreadIds.has(params.threadId)
          ? readMetadata(params.threadId)
          : client.raw.request(method, params),
    },
  };
}

function metadataFromTranscript(transcript: ProviderReplayTranscript): {
  readonly provider?: string;
  readonly protocol?: string;
  readonly scenario?: string;
} {
  return {
    provider: transcript.provider,
    protocol: transcript.protocol,
    scenario: transcript.scenario,
  };
}

export function makeReplayServerConfig(
  scenario: string,
): Effect.Effect<
  ServerConfig.ServerConfig["Service"],
  PlatformError.PlatformError,
  FileSystem.FileSystem | Path.Path | Scope.Scope
> {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const baseDir = yield* fs.makeTempDirectoryScoped({
      prefix: `t3-orchestration-v2-codex-${scenario}-`,
    });
    const stateDir = path.join(baseDir, "userdata");
    const logsDir = path.join(stateDir, "logs");
    const providerLogsDir = path.join(logsDir, "provider");
    const terminalLogsDir = path.join(logsDir, "terminals");
    const attachmentsDir = path.join(stateDir, "attachments");
    const environmentThemesDir = path.join(stateDir, "themes");
    const worktreesDir = path.join(baseDir, "worktrees");
    const providerStatusCacheDir = path.join(baseDir, "caches");

    for (const directory of [
      stateDir,
      logsDir,
      providerLogsDir,
      terminalLogsDir,
      attachmentsDir,
      environmentThemesDir,
      worktreesDir,
      providerStatusCacheDir,
    ]) {
      yield* fs.makeDirectory(directory, { recursive: true });
    }

    return {
      logLevel: "Error",
      traceMinLevel: "Info",
      traceTimingEnabled: true,
      traceBatchWindowMs: 200,
      traceMaxBytes: 10 * 1024 * 1024,
      traceMaxFiles: 10,
      otelEnvironment: OtelEnvironment.none,
      otlpTracesUrl: undefined,
      otlpMetricsUrl: undefined,
      otlpLogsUrl: undefined,
      otlpTracesExport: DEFAULT_SIGNAL_EXPORT,
      otlpMetricsExport: DEFAULT_SIGNAL_EXPORT,
      otlpLogsExport: DEFAULT_SIGNAL_EXPORT,
      mode: "web",
      port: 0,
      host: undefined,
      cwd: process.cwd(),
      baseDir,
      staticDir: undefined,
      devUrl: undefined,
      devAllowedOrigins: [],
      noBrowser: false,
      startupPresentation: "browser",
      tailscaleServeEnabled: false,
      tailscaleServePort: 443,
      desktopBootstrapToken: undefined,
      autoBootstrapProjectFromCwd: false,
      logWebSocketEvents: false,
      stateDir,
      authorityStateDir: path.join(baseDir, "native-store-authority"),
      dbPath: path.join(stateDir, "state.sqlite"),
      keybindingsConfigPath: path.join(stateDir, "keybindings.json"),
      settingsPath: path.join(stateDir, "settings.json"),
      providerStatusCacheDir,
      worktreesDir,
      attachmentsDir,
      browserArtifactsDir: path.join(stateDir, "browser-artifacts"),
      environmentThemesDir,
      logsDir,
      serverLogPath: path.join(logsDir, "server.log"),
      serverTracePath: path.join(logsDir, "server.trace.ndjson"),
      providerLogsDir,
      providerEventLogPath: path.join(providerLogsDir, "events.log"),
      terminalLogsDir,
      anonymousIdPath: path.join(stateDir, "anonymous-id"),
      environmentIdPath: path.join(stateDir, "environment-id"),
      serverRuntimeStatePath: path.join(stateDir, "server-runtime.json"),
      secretsDir: path.join(stateDir, "secrets"),
    };
  });
}

export function makeCodexProviderAdapterRegistryReplayLayer(input: {
  readonly transcript: CodexReplay.CodexAppServerReplayTranscript;
  readonly driver?: CodexReplay.CodexAppServerReplayDriver;
  readonly goalResponses?: ReadonlyMap<
    string,
    import("effect-codex-app-server/schema").V2ThreadGoalGetResponse
  >;
}) {
  const replayLayer =
    input.driver === undefined
      ? CodexReplay.layerReplay(input.transcript)
      : CodexReplay.layerReplayWithDriver(input.driver);
  const replayClientFactoryLayer = Layer.succeed(CodexAdapterV2.CodexAppServerClientFactory, {
    open: (openInput) =>
      Effect.gen(function* () {
        const context = yield* Layer.build(replayLayer).pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapterOpenSessionError({
                driver: CodexAdapterV2.CODEX_DRIVER_KIND,
                providerSessionId: openInput.providerSessionId,
                cause,
              }),
          ),
        );
        return yield* Effect.service(CodexClient.CodexAppServerClient).pipe(
          Effect.map((service) => {
            const client = withCodexReplayChildMetadata(service, input.transcript);
            return {
              ...client,
              raw: {
                ...client.raw,
                request: (method, params) =>
                  method === "thread/goal/get" &&
                  Predicate.isObject(params) &&
                  typeof params.threadId === "string" &&
                  input.goalResponses?.has(params.threadId)
                    ? Effect.succeed(input.goalResponses.get(params.threadId))
                    : client.raw.request(method, params),
              },
            } satisfies CodexClient.CodexAppServerClient["Service"];
          }),
          Effect.provide(context),
        );
      }),
  });
  const serverConfigLayer = Layer.effect(
    ServerConfig.ServerConfig,
    makeReplayServerConfig(input.transcript.scenario).pipe(Effect.orDie),
  ).pipe(Layer.provide(NodeServices.layer));
  const registryLayer = ProviderAdapterRegistry.makeDriverLayer({
    drivers: [CodexAdapterV2.CodexAdapterV2Driver],
    configMap: {
      [CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID]: {
        driver: CodexAdapterV2.CODEX_DRIVER_KIND,
      },
    },
  }).pipe(
    Layer.provide(
      Layer.mergeAll(
        replayClientFactoryLayer,
        serverConfigLayer,
        NodeServices.layer,
        IdAllocator.layer,
      ),
    ),
  );

  return registryLayer;
}

const decodeCodexAppServerReplayTranscript = Schema.decodeUnknownEffect(
  CodexReplay.CodexAppServerReplayTranscript,
);

export const CodexOrchestratorReplayHarness: OrchestratorV2ProviderReplayHarness<
  CodexReplay.CodexAppServerReplayTranscript,
  CodexOrchestratorReplayHarnessError
> = {
  driver: CodexAdapterV2.CODEX_DRIVER_KIND,
  decodeTranscript: (transcript) =>
    decodeCodexAppServerReplayTranscript(transcript).pipe(
      Effect.mapError(
        (cause) =>
          new CodexReplayTranscriptDecodeError({
            ...metadataFromTranscript(transcript),
            cause,
          }),
      ),
    ),
  makeProviderAdapterRegistryLayer: (
    transcript,
    options: { readonly replayGate?: ProviderReplayGate } = {},
  ) => {
    return Layer.effectContext(
      Effect.gen(function* () {
        const replayGate = options.replayGate;
        if (replayGate !== undefined) {
          yield* Effect.addFinalizer(() => Effect.sync(() => replayGate.releaseAll()));
        }
        const driver = yield* CodexReplay.makeReplayDriver(
          transcript,
          replayGate === undefined
            ? {}
            : {
                beforeEmitInbound: (entry) =>
                  Effect.promise((signal) => replayGate.beforeEmit(entry.label, signal)),
              },
        );
        return yield* Layer.build(
          makeCodexProviderAdapterRegistryReplayLayer({ transcript, driver }),
        );
      }),
    );
  },
};
