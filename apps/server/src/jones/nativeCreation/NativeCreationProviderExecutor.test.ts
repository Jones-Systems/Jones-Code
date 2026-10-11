import { materializeCodexReplayAppIdentity } from "../runtimeIdentity/CodexReplayIdentity.ts";
import * as Guard from "./NativeCreationProviderGuard.ts";
import * as CodexClient from "effect-codex-app-server/client";
import * as Predicate from "effect/Predicate";
import * as CodexAdapterV2 from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import {
  makeCodexReplayClientFactory,
  makeReplayServerConfig,
} from "../../orchestration-v2/Adapters/CodexAdapterV2.testkit.ts";
import * as CodexReplay from "effect-codex-app-server/replay";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as FileSystem from "effect/FileSystem";
import * as IdAllocator from "../../orchestration-v2/IdAllocator.ts";
import {
  runtimeBinding,
  ProviderRuntimeBindingError,
  ProviderAdapterV2RuntimePolicy,
} from "../../orchestration-v2/ProviderAdapter.ts";
import { CodexSettings, OrchestrationV2AppThread, NodeId, MessageId } from "@t3tools/contracts";
import packageJson from "../../../package.json" with { type: "json" };
import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { NativeCreationExecutionReferenceV2 } from "./NativeCreationExecutionTypes.ts";
const memory = NodeSqliteClient.layer({ filename: ":memory:" });
import {
  AuthSessionId,
  EventId,
  NativeCreationHistoricalBinding,
  OrchestrationV2Command,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as Crypto from "effect/Crypto";
import * as Option from "effect/Option";
import * as TestClock from "effect/testing/TestClock";
import * as AuthSessions from "../../persistence/AuthSessions.ts";
import { runMigrations } from "../../persistence/Migrations.ts";
import * as Repository from "./NativeCreationRepository.ts";
import * as RepositorySqlite from "./NativeCreationRepositorySqlite.ts";
import * as Authority from "./NativeCreationAuthority.ts";
import * as Execution from "./NativeCreationProviderExecution.ts";
import * as Concrete from "./NativeCreationProviderExecutor.ts";
import * as Start from "../../orchestration-v2/ProviderTurnStartService.ts";
import * as Registry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import { ProviderAdapterV2 } from "../../orchestration-v2/ProviderAdapter.ts";
import { NativeWorkspaceVerified } from "./NativeCreationWorkspaceTypes.ts";
import {
  ProviderRuntimeBinding,
  OrchestrationV2ProviderThread,
  RunAttemptId,
} from "@t3tools/contracts";
import {
  NativePreparationBinding,
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  nativePreparationCommand,
  validateNativeCreationPreparation,
} from "./NativeCreationPreparation.ts";
import { NativeCreationWholeOperationEvidence } from "./NativeCreationExecutionTypes.ts";
import * as Outbox from "../../orchestration-v2/EffectOutbox.ts";

const now = "2026-10-02T12:34:56Z";
const deadline = "2099-01-01T00:00:00.000Z";
const actor = AuthSessionId.make("sql-fixture-actor");
const database = Layer.effectDiscard(runMigrations().pipe(Effect.map(() => undefined))).pipe(
  Layer.provideMerge(memory),
);
const realRepository = RepositorySqlite.layer.pipe(Layer.provideMerge(database));
const fixture = Effect.gen(function* () {
  yield* TestClock.setTime(DateTime.toEpochMillis(DateTime.makeUnsafe(now)));
  const repository = yield* Repository.NativeCreationRepository;
  const sql = yield* SqlClient.SqlClient;
  const binding = yield* Schema.decodeUnknownEffect(NativePreparationBinding)({
    backend_instance: "sql-fixture-backend",
    environment_id: "sql-fixture-environment",
    project_id: "sql-fixture-project",
    project_cwd: "/synthetic/project",
    account_ref: "sql-fixture-account",
    runtime_mode: "full-access",
    interaction_mode: "default",
    base_branch: "main",
    start_from_origin: false,
    run_setup_script: false,
    provider_model_selection: { instanceId: "codex", model: "synthetic-model" },
  });
  const prepared = nativePreparationCommand(
    "sql-native-execution",
    binding,
    "Synthetic prompt",
    "Synthetic title",
    now,
  );
  const preparation = yield* validateNativeCreationPreparation(
    new TextEncoder().encode(
      nativeCreationCanonicalJson({
        schema: "voice.t3-bootstrap-preparation/v1",
        operation_id: "sql-native-execution",
        binding,
        command: prepared,
        preparation_id: prepared.commandId.replace("voice-command-", "voice-bootstrap-"),
        binding_digest: nativeCreationSha256(nativeCreationCanonicalJson(binding)),
        prompt_digest: nativeCreationSha256(prepared.message.text),
        command_digest: nativeCreationSha256(nativeCreationCanonicalJson(prepared)),
      }),
    ),
  );
  const historical = yield* Schema.decodeUnknownEffect(NativeCreationHistoricalBinding)({
    backendInstance: binding.backend_instance,
    environmentId: binding.environment_id,
    projectId: binding.project_id,
    projectCwd: binding.project_cwd,
    accountRef: binding.account_ref,
    accountBindingId: "sql-qualified-account",
    accountBindingRevision: 1,
    providerModelSelection: binding.provider_model_selection,
    runtimeMode: binding.runtime_mode,
    interactionMode: binding.interaction_mode,
    baseBranch: binding.base_branch,
    startFromOrigin: false,
    runSetupScript: false,
    requestedBranch: prepared.bootstrap.prepareWorktree.branch,
  });
  const resources = {
    projectCwd: binding.project_cwd,
    branch: historical.requestedBranch,
    worktreePath: "/synthetic/worktree",
  };
  const claimId = "sql-native-claim";
  yield* repository.claim(
    {
      preparation,
      resources,
      claimId,
      claimedBootId: "sql-boot",
      claimedAt: now,
      actorSessionId: actor,
      grantId: "sql-grant",
      grantRevision: 1,
    },
    Effect.succeed(historical),
  );
  yield* sql`INSERT INTO native_creation_automation_enrollments(session_id,enrolled_at) VALUES(${actor},${now})`;
  const command = yield* Schema.decodeUnknownEffect(OrchestrationV2Command)({
    type: "prepared-run.release",
    commandId: `${prepared.commandId}:native:v2:message`,
    threadId: prepared.threadId,
    runId: "sql-run",
  });
  if (command.type !== "prepared-run.release")
    return yield* Effect.die(new Error("Synthetic release has wrong type"));
  yield* repository.reserveExecutionCommandIdentities!(claimId, [
    prepared.commandId,
    command.commandId,
  ]);
  const normalized = yield* Schema.decodeUnknownEffect(OrchestrationV2Command)({
    type: "message.dispatch",
    createdBy: "user",
    creationSource: "server",
    commandId: prepared.commandId,
    threadId: prepared.threadId,
    messageId: prepared.message.messageId,
    text: prepared.message.text,
    attachments: [],
    modelSelection: binding.provider_model_selection,
    dispatchMode: { type: "start_immediately" },
  });
  yield* repository.recordNormalizedCommand(claimId, normalized);
  yield* repository.reserveCommand(claimId, command);
  const reference = yield* Schema.decodeUnknownEffect(NativeCreationExecutionReferenceV2)({
    version: 2,
    claimId,
    stageCommandId: command.commandId,
    effectId: `effect:${command.commandId}:provider-turn.start:${command.runId}`,
    stage: "native_command",
  });
  const eventId = EventId.make("sql-release-event");
  const appendReceipt = Effect.gen(function* () {
    const events = yield* sql<{
      sequence: number;
    }>`INSERT INTO orchestration_events(event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version)
      VALUES(${eventId},'thread',${command.threadId},1,'run.updated',${now},${command.commandId},'system','{}','{}',2) RETURNING sequence`;
    const sequence = events[0]!.sequence;
    yield* sql`INSERT INTO orchestration_command_receipts(command_id,aggregate_kind,aggregate_id,accepted_at,result_sequence,status,error,command_type)
      VALUES(${command.commandId},'thread',${command.threadId},${now},${sequence},'accepted',NULL,${command.type})`;
    return sequence;
  });
  const accept = (actual: OrchestrationV2Command = command) =>
    sql.withTransaction(
      Effect.gen(function* () {
        const sequence = yield* appendReceipt;
        yield* repository.recordExecutionAcceptance!({
          claimId,
          command: actual,
          eventId,
          sequence,
        });
        return sequence;
      }),
    );
  const enqueue = Effect.gen(function* () {
    const payload = yield* Schema.encodeEffect(
      Schema.fromJsonString(Outbox.NativeOrchestrationEffectPayloadV2),
    )({
      request: { type: "provider-turn.start", runId: command.runId },
      nativeCreationExecutionReference: reference,
    });
    yield* sql`INSERT INTO orchestration_v2_effect_outbox(effect_id,command_id,thread_id,effect_type,payload_json,status,attempt_count,available_at,lease_owner,lease_expires_at,created_at,updated_at)
      VALUES(${reference.effectId},${command.commandId},${command.threadId},'provider-turn.start',${payload},'running',1,${now},'sql-worker',${deadline},${now},${now})`;
  });
  const seedRuntime = Effect.gen(function* () {
    const providerPayload = nativeCreationCanonicalJson({
      runtimeIdentity: { runtimeGeneration: "sql-generation" },
    });
    yield* sql`INSERT INTO orchestration_v2_projection_threads(thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,active_provider_thread_id,created_at,updated_at,payload_json)
      VALUES(${command.threadId},'sql-fixture-project','Synthetic','codex','full-access','default','sql-provider-thread',${now},${now},'{}')`;
    yield* sql`INSERT INTO orchestration_v2_projection_runs(run_id,thread_id,ordinal,provider,provider_thread_id,status,requested_at,payload_json)
      VALUES(${command.runId},${command.threadId},1,'codex','sql-provider-thread','running',${now},'{}')`;
    yield* sql`INSERT INTO orchestration_v2_projection_run_attempts(attempt_id,thread_id,run_id,attempt_ordinal,root_node_id,provider,provider_thread_id,status,payload_json)
      VALUES('sql-attempt',${command.threadId},${command.runId},1,'sql-root','codex','sql-provider-thread','running','{}')`;
    yield* sql`INSERT INTO orchestration_v2_projection_provider_threads(provider_thread_id,thread_id,provider,provider_session_id,status,updated_at,payload_json)
      VALUES('sql-provider-thread',${command.threadId},'codex','sql-provider-session','active',${now},${providerPayload})`;
    yield* sql`INSERT INTO orchestration_v2_projection_provider_session_bindings(provider_session_id,thread_id) VALUES('sql-provider-session',${command.threadId})`;
  });
  const evidence = yield* Schema.decodeUnknownEffect(NativeCreationWholeOperationEvidence)({
    version: 1,
    outcome: "confirmed_success",
    effectId: reference.effectId,
    threadId: command.threadId,
    commandId: command.commandId,
    providerSessionId: "sql-provider-session",
    providerThreadId: "sql-provider-thread",
    runtimeGeneration: "sql-generation",
    runId: command.runId,
    attemptId: "sql-attempt",
    coverage: "whole_operation",
  });
  const session = yield* Schema.decodeUnknownEffect(AuthSessions.AuthSessionRecord)({
    sessionId: actor,
    subject: "Synthetic native actor",
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
    issuedAt: now,
    expiresAt: deadline,
    revokedAt: null,
    lastConnectedAt: null,
  });
  const grant: Authority.NativeCreationGrant = {
    grantId: "sql-grant",
    revision: 1,
    actorSessionId: actor,
    issuerId: "sql-issuer",
    expiresAt: DateTime.makeUnsafe(deadline),
    revoked: false,
    operationId: preparation.operationId,
    preparationId: preparation.preparationId,
    preparationSha256: preparation.preparationSha256,
    bindingDigest: preparation.bindingDigest,
    binding: historical,
    resources,
    allowedStages: ["native_command", "fetch"],
    recoveryScopes: [],
  };
  let revoked = false;
  const revoke = Effect.sync(() => {
    revoked = true;
  });
  const authorityLayer = Authority.NativeCreationAuthorityLive.pipe(
    Layer.provide(Layer.succeed(Repository.NativeCreationRepository, repository)),
    Layer.provide(
      Layer.succeed(
        AuthSessions.AuthSessionRepository,
        AuthSessions.AuthSessionRepository.of({
          getById: () => Effect.succeed(Option.some(session)),
          create: () => Effect.void,
          createReplacingActive: () => Effect.succeed([]),
          createIfAbsent: () => Effect.void,
          listActive: () => Effect.succeed([]),
          revoke: () => Effect.succeed(false),
          revokeAllExcept: () => Effect.succeed([]),
          setLastConnectedAt: () => Effect.void,
          setClientConnection: () => Effect.void,
        }),
      ),
    ),
    Layer.provide(
      Layer.succeed(Authority.NativeCreationGrantResolver, {
        resolveCurrent: () =>
          Effect.succeed({
            enrolledSessionId: actor,
            trustedIssuerId: "sql-issuer",
            grant: { ...grant, revoked },
          }),
      }),
    ),
    Layer.provide(
      Layer.succeed(Authority.NativeCreationBindingResolver, {
        resolveCurrent: () => Effect.succeed(historical),
      }),
    ),
  );
  const issue = Effect.gen(function* () {
    const authority = yield* Authority.NativeCreationAuthority;
    return yield* authority.issueExecution!({ reference, timestamp: now });
  }).pipe(Effect.provide(authorityLayer));
  const confirmation = {
    reference,
    workerId: "sql-worker",
    expectedAttempt: 1,
    leaseExpiresAt: deadline,
    evidence,
  };
  const effect: Outbox.OrchestrationEffectV2 = {
    id: reference.effectId,
    commandId: command.commandId,
    threadId: command.threadId,
    request: { type: "provider-turn.start", runId: command.runId },
    nativeCreationExecutionReference: reference,
    status: "running",
    attemptCount: 1,
    availableAt: now,
    leaseOwner: "sql-worker",
    leaseExpiresAt: deadline,
    createdAt: now,
    updatedAt: now,
    completedAt: null,
    lastError: null,
  };
  return {
    sql,
    repository,
    command,
    reference,
    eventId,
    accept,
    enqueue,
    seedRuntime,
    evidence,
    issue,
    confirmation,
    authorityLayer,
    revoke,
    effect,
  };
});

function makeCodexReplayTurn(input: {
  readonly id: string;
  readonly status: "inProgress" | "completed" | "interrupted" | "failed";
}): Record<string, unknown> {
  const terminal =
    input.status === "completed" || input.status === "interrupted" || input.status === "failed";
  return {
    id: input.id,
    items: [],
    itemsView: "notLoaded",
    status: input.status,
    error: null,
    startedAt: 1782622440,
    completedAt: terminal ? 1782622450 : null,
    durationMs: null,
  };
}

function codexReplayPreamble(input: {
  readonly nativeThreadId: string;
  readonly nativeTurnId: string;
  readonly prompt: string;
  /** Text the adapter should send, when it differs from what the user typed. */
  readonly sentPrompt?: string;
}): Array<CodexReplay.CodexAppServerReplayEntry> {
  return [
    {
      type: "expect_outbound",
      label: "initialize",
      frame: {
        id: 1,
        method: "initialize",
        params: {
          clientInfo: { name: "T3 Code", title: "T3 Code", version: packageJson.version },
          capabilities: {
            experimentalApi: true,
            optOutNotificationMethods: ["turn/diff/updated"],
            extensions: {
              "io.modelcontextprotocol/ui": { mimeTypes: ["text/html;profile=mcp-app"] },
            },
          },
        },
      },
    },
    {
      type: "emit_inbound",
      label: "initialize",
      frame: {
        id: 1,
        result: {
          userAgent: "T3 Code/0.156.1",
          codexHome: "/tmp/codex-home",
          platformFamily: "unix",
          platformOs: "macos",
        },
      },
    },
    { type: "expect_outbound", label: "initialized", frame: { method: "initialized" } },
    {
      type: "expect_outbound",
      label: "thread/start",
      frame: {
        id: 2,
        method: "thread/start",
        params: { config: CodexAdapterV2.CODEX_THREAD_CONFIG },
      },
    },
    {
      type: "emit_inbound",
      label: "thread/start",
      frame: {
        id: 2,
        result: {
          thread: {
            id: input.nativeThreadId,
            sessionId: input.nativeThreadId,
            forkedFromId: null,
            preview: "",
            ephemeral: false,
            modelProvider: "openai",
            createdAt: 1782622440,
            updatedAt: 1782622440,
            status: { type: "idle" },
            path: `/tmp/${input.nativeThreadId}.jsonl`,
            cwd: "/synthetic/worktree",
            cliVersion: "0.144.0",
            source: "vscode",
            threadSource: null,
            agentNickname: null,
            agentRole: null,
            gitInfo: null,
            name: null,
            turns: [],
          },
          model: "synthetic-model",
          modelProvider: "openai",
          serviceTier: null,
          cwd: "/synthetic/worktree",
          instructionSources: [],
          approvalPolicy: "on-request",
          approvalsReviewer: "user",
          sandbox: { type: "workspaceWrite", writableRoots: [], networkAccess: false },
          reasoningEffort: "medium",
        },
      },
    },
    {
      type: "expect_outbound",
      label: "turn/start",
      frame: {
        id: 3,
        method: "turn/start",
        params: {
          threadId: input.nativeThreadId,
          input: [{ type: "text", text: input.sentPrompt ?? input.prompt }],
          cwd: "/synthetic/worktree",
          model: "synthetic-model",
          approvalPolicy: "never",
          approvalsReviewer: "user",
          sandboxPolicy: { type: "dangerFullAccess" },
          summary: "detailed",
        },
      },
    },
    {
      type: "emit_inbound",
      label: "turn/start",
      frame: {
        id: 3,
        result: { turn: makeCodexReplayTurn({ id: input.nativeTurnId, status: "inProgress" }) },
      },
    },
    {
      type: "emit_inbound",
      label: "turn/started",
      frame: {
        method: "turn/started",
        params: {
          threadId: input.nativeThreadId,
          turn: makeCodexReplayTurn({ id: input.nativeTurnId, status: "inProgress" }),
        },
      },
    },
  ];
}

const decodeMaterializedTranscript = Schema.decodeUnknownSync(
  CodexReplay.CodexAppServerReplayTranscript,
);

function makeCodexReplayTranscript(input: {
  readonly scenario: string;
  readonly entries: ReadonlyArray<CodexReplay.CodexAppServerReplayEntry>;
}): CodexReplay.CodexAppServerReplayTranscript {
  return decodeMaterializedTranscript(
    materializeCodexReplayAppIdentity({
      provider: "codex",
      protocol: "codex.app-server",
      version: "0.144.0",
      scenario: input.scenario,
      entries: input.entries,
    }),
  );
}

type Mode =
  | "success"
  | "void"
  | "generation"
  | "failure"
  | "transport-success"
  | "transport-spawn-revoked"
  | "transport-history-revoked"
  | "transport-send-revoked"
  | "transport-generation"
  | "transport-resume"
  | "transport-failure";
const scenario = (mode: Mode) =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.accept(f.command);
    yield* f.enqueue;
    const context = yield* f.issue;
    let calls = 0;
    const original = yield* f.repository.readExecutionReference!(f.reference);
    const resources = original.history.intent.resources;
    const verified = yield* Schema.decodeUnknownEffect(NativeWorkspaceVerified)({
      claimId: f.reference.claimId,
      basis: {
        bootId: "synthetic-boot",
        projectId: original.history.intent.binding.projectId,
        projectCwd: resources.projectCwd,
        projectBirth: "synthetic-project-birth",
        gitCommonDirectory: "/synthetic/git",
        physicalGitIdentity: "synthetic-git-birth",
        worktreePath: resources.worktreePath,
        parentBirth: "synthetic-parent-birth",
        producerId: "synthetic-producer",
        baseRef: "main",
        setupDefinition: null,
        configuredSubmodulesDefinition: "",
        baseConfigurationDefinition: "",
      },
      proof: {
        worktreePath: resources.worktreePath,
        pathBirth: "synthetic-worktree-birth",
        gitCommonDirectory: "/synthetic/git",
        physicalGitIdentity: "synthetic-git-birth",
        branch: resources.branch,
        baseRef: "main",
        configuredSubmodulesDigest: "synthetic-submodules",
        baseConfigurationDigest: "synthetic-config",
      },
      setupTerminalId: null,
    });
    const binding = yield* Schema.decodeUnknownEffect(ProviderRuntimeBinding)({
      threadId: f.command.threadId,
      providerThreadId: "sql-provider-thread",
      providerSessionId: "sql-provider-session",
      providerInstanceId: "codex",
      driver: "codex",
      nativeThreadId: "synthetic-native",
      runtimeGeneration: "sql-generation",
    });
    const providerThread = yield* Schema.decodeUnknownEffect(OrchestrationV2ProviderThread)({
      id: binding.providerThreadId,
      appThreadId: binding.threadId,
      ownerNodeId: null,
      providerSessionId: binding.providerSessionId,
      providerInstanceId: binding.providerInstanceId,
      driver: "codex",
      nativeThreadRef: { driver: "codex", nativeId: binding.nativeThreadId, strength: "strong" },
      nativeMetadata: null,
      modelSelection: original.history.intent.binding.providerModelSelection,
      status: "idle",
      nativeConversationHeadRef: null,
      firstRunOrdinal: null,
      lastRunOrdinal: null,
      handoffIds: [],
      forkedFrom: null,
      contextUsage: null,
      createdAt: DateTime.makeUnsafe(now),
      updatedAt: DateTime.makeUnsafe(now),
    });
    const transport = (guard: Guard.NativeProviderExecutionGuard) =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const allocator = yield* IdAllocator.IdAllocatorV2;
          const config = yield* Effect.acquireRelease(
            makeReplayServerConfig(`native-${mode}`).pipe(Effect.orDie),
            (value) => fs.remove(value.baseDir, { recursive: true }).pipe(Effect.orDie),
          );
          const preamble = codexReplayPreamble({
            nativeThreadId: "synthetic-native",
            nativeTurnId: "synthetic-turn",
            prompt: "Synthetic prompt",
          });
          const startIndex = preamble.findIndex(
            (entry) => entry.type === "expect_outbound" && entry.label === "turn/start",
          );
          const entries: Array<CodexReplay.CodexAppServerReplayEntry> = [
            ...preamble.slice(0, startIndex),
            {
              type: "expect_outbound",
              frame: {
                id: 3,
                method: "thread/inject_items",
                params: {
                  threadId: "synthetic-native",
                  items: [
                    { type: "message", role: "user", content: [{ type: "input_text", text: "" }] },
                  ],
                },
              },
            },
            { type: "emit_inbound", frame: { id: 3, result: {} } },
            ...preamble.slice(startIndex).map((entry) =>
              (entry.type === "expect_outbound" || entry.type === "emit_inbound") &&
              Predicate.isObject(entry.frame)
                ? {
                    ...entry,
                    frame: { ...entry.frame, ...(entry.label === "turn/start" ? { id: 4 } : {}) },
                  }
                : entry,
            ),
          ];
          if (mode === "transport-failure") {
            const index = entries.findIndex(
              (entry) => entry.type === "emit_inbound" && entry.label === "turn/start",
            );
            entries[index] = {
              type: "emit_inbound",
              label: "turn/start",
              frame: { id: 4, error: { code: -32000, message: "Synthetic start rejected" } },
            };
          }
          const factory = makeCodexReplayClientFactory({
            transcript: makeCodexReplayTranscript({ scenario: `native-${mode}`, entries }),
          });
          let opened = 0;
          const requests: Array<string> = [];
          const settings = yield* Schema.decodeUnknownEffect(CodexSettings)({});
          const adapter = CodexAdapterV2.makeCodexAdapterV2({
            instanceId: binding.providerInstanceId,
            settings,
            crypto: yield* Crypto.Crypto,
            environment: {},
            fileSystem: fs,
            idAllocator: allocator,
            serverConfig: config,
            clientFactory: {
              open: (openInput) =>
                Effect.sync(() => {
                  opened += 1;
                }).pipe(
                  Effect.andThen(factory.open(openInput)),
                  Effect.map(
                    (client) =>
                      ({
                        ...client,
                        request: (method, params) =>
                          Effect.sync(() => {
                            requests.push(method);
                          }).pipe(Effect.andThen(client.request(method, params))),
                      }) satisfies CodexClient.CodexAppServerClient["Service"],
                  ),
                ),
            },
          });
          if (mode === "transport-spawn-revoked") yield* f.revoke;
          const policy = ProviderAdapterV2RuntimePolicy.make({
            cwd: resources.worktreePath,
            runtimeMode: "full-access",
            interactionMode: "default",
          });
          const actual = Effect.gen(function* () {
            const runtime = yield* adapter.openSession({
              threadId: f.command.threadId,
              providerSessionId: binding.providerSessionId,
              modelSelection: original.history.intent.binding.providerModelSelection,
              runtimePolicy: policy,
              nativeCreationGuard: guard,
              runtimeLifecycle: {
                admit: () => Effect.void,
                reserve: () => Effect.succeed("sql-generation"),
                abandon: () => Effect.void,
                invalidate: () => Effect.void,
                bind: (input) =>
                  Effect.gen(function* () {
                    const bound = {
                      ...input.providerThread,
                      runtimeIdentity: {
                        runtimeGeneration: input.runtimeGeneration,
                        evidenceRevision: 1,
                        requested: input.requested,
                        observed: input.observed,
                      },
                    };
                    const captured = runtimeBinding(bound, input.runtimeGeneration);
                    if (captured === undefined)
                      return yield* new ProviderRuntimeBindingError({
                        driver: binding.driver,
                        detail: "Synthetic native binding missing",
                      });
                    yield* Guard.bindNativeProviderGuard(guard, {
                      ...captured,
                      runtimeGeneration:
                        mode === "transport-generation"
                          ? "original-old-generation"
                          : captured.runtimeGeneration,
                    });
                    return bound;
                  }).pipe(
                    Effect.mapError(
                      (cause) =>
                        new ProviderRuntimeBindingError({
                          driver: binding.driver,
                          detail: "Synthetic runtime binding rejected",
                          cause,
                        }),
                    ),
                  ),
              },
            });
            const loaded = yield* runtime.ensureThread({
              threadId: f.command.threadId,
              providerSessionId: binding.providerSessionId,
              modelSelection: original.history.intent.binding.providerModelSelection,
              runtimePolicy: policy,
              existingProviderThread: {
                ...providerThread,
                nativeThreadRef: null,
                status: "not_loaded",
              },
              nativeCreationGuard: guard,
            });
            if (mode === "transport-resume")
              yield* runtime.resumeThread({
                providerThread: loaded,
                modelSelection: original.history.intent.binding.providerModelSelection,
                runtimePolicy: policy,
              });
            if (mode === "transport-history-revoked") yield* f.revoke;
            if (runtime.injectHistory === undefined)
              return yield* new Start.ProviderTurnStartError({
                runId: f.command.runId,
                cause: "Actual Codex history owner is unavailable",
              });
            yield* runtime.injectHistory({ providerThread: loaded, messages: [], context: "" });
            if (mode === "transport-send-revoked") yield* f.revoke;
            const appThread = yield* Schema.decodeUnknownEffect(OrchestrationV2AppThread)({
              createdBy: "system",
              creationSource: "server",
              id: f.command.threadId,
              projectId: original.history.intent.binding.projectId,
              title: "Synthetic",
              providerInstanceId: binding.providerInstanceId,
              modelSelection: original.history.intent.binding.providerModelSelection,
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: resources.branch,
              worktreePath: resources.worktreePath,
              activeProviderThreadId: loaded.id,
              lineage: {
                parentThreadId: null,
                relationshipToParent: null,
                rootThreadId: f.command.threadId,
              },
              forkedFrom: null,
              createdAt: DateTime.makeUnsafe(now),
              updatedAt: DateTime.makeUnsafe(now),
              archivedAt: null,
              settledOverride: null,
              settledAt: null,
              lastVisitedAt: null,
              deletedAt: null,
            });
            yield* runtime.startTurn({
              appThread,
              threadId: f.command.threadId,
              runId: f.command.runId,
              runOrdinal: 1,
              providerTurnOrdinal: 1,
              attemptId: RunAttemptId.make("sql-attempt"),
              rootNodeId: NodeId.make("sql-root"),
              providerThread: loaded,
              message: {
                createdBy: "system",
                creationSource: "server",
                messageId: MessageId.make("synthetic-message"),
                text: "Synthetic prompt",
                attachments: [],
              },
              modelSelection: original.history.intent.binding.providerModelSelection,
              runtimePolicy: policy,
              nativeCreationGuard: guard,
            });
            const acknowledgement = Guard.readNativeProviderAcknowledgement(guard);
            if (acknowledgement === undefined)
              return yield* new Start.ProviderTurnStartError({
                runId: f.command.runId,
                cause: "Actual Codex send lacks direct acknowledgement",
              });
            return acknowledgement;
          });
          const result = yield* actual.pipe(Effect.result);
          assert.strictEqual(opened, mode === "transport-spawn-revoked" ? 0 : 1);
          const expected = mode === "transport-spawn-revoked" ? [] : ["initialize", "thread/start"];
          if (
            mode === "transport-send-revoked" ||
            mode === "transport-success" ||
            mode === "transport-failure"
          )
            expected.push("thread/inject_items");
          if (mode === "transport-success" || mode === "transport-failure")
            expected.push("turn/start");
          assert.deepEqual(requests, expected);
          if (mode !== "transport-success")
            assert.isUndefined(Guard.readNativeProviderAcknowledgement(guard));
          if (result._tag === "Failure") return yield* result.failure;
          return result.success;
        }),
      ).pipe(Effect.provide(Layer.merge(NodeServices.layer, IdAllocator.layer)));
    const start = Layer.succeed(
      Start.ProviderTurnStartServiceV2,
      Start.ProviderTurnStartServiceV2.of({
        start: () => Effect.void,
        startNative: (input) =>
          Effect.gen(function* () {
            calls += 1;
            if (mode.startsWith("transport-")) return yield* transport(input.guard);
            yield* Guard.revalidateNativeProviderGuard(input.guard, {
              threadId: input.threadId,
              cwd: resources.worktreePath,
            });
            yield* Guard.bindNativeProviderGuard(input.guard, binding);
            if (mode === "failure")
              return yield* new Start.ProviderTurnStartError({
                runId: input.runId,
                cause: "Synthetic lost acknowledgement",
              });
            if (mode !== "void")
              yield* Guard.acknowledgeNativeProviderGuard(input.guard, {
                threadId: input.threadId,
                runId: input.runId,
                providerInstanceId: binding.providerInstanceId,
                attemptId: RunAttemptId.make("sql-attempt"),
                providerThread,
                runtimeGeneration:
                  mode === "generation" ? "replaced-generation" : binding.runtimeGeneration,
              });
            return f.evidence;
          }).pipe(
            Effect.mapError(
              (cause) => new Start.ProviderTurnStartError({ runId: input.runId, cause }),
            ),
          ),
      }),
    );
    const repository = Layer.succeed(
      Repository.NativeCreationRepository,
      Repository.NativeCreationRepository.of({
        ...f.repository,
        readWorkspaceVerified: () => Effect.succeed(Option.some(verified)),
      }),
    );
    const result = yield* Effect.gen(function* () {
      const executor = yield* Execution.NativeCreationProviderExecutor;
      return yield* executor
        .executeWholeOperation({ effect: f.effect, context })
        .pipe(Effect.result);
    }).pipe(
      Effect.provide(
        Concrete.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              repository,
              start,
              Layer.succeed(Registry.ProviderAdapterRegistryV2, {
                get: () =>
                  Effect.succeed(
                    ProviderAdapterV2.of({
                      instanceId: binding.providerInstanceId,
                      driver: binding.driver,
                      nativeCreationExecution: true,
                      getCapabilities: () => Effect.die("Not called by executor"),
                      planSelectionTransition: () => Effect.die("Not called by executor"),
                      openSession: () =>
                        Effect.die("Synthetic start owner supplies the physical acknowledgement"),
                    }),
                  ),
                list: () => Effect.succeed([]),
              }),
            ),
          ),
        ),
      ),
    );
    assert.strictEqual(calls, 1);
    if (mode === "success" || mode === "transport-success") {
      assert.isTrue(result._tag === "Success");
      if (result._tag === "Success") assert.deepEqual(result.success, f.evidence);
    } else assert.isTrue(result._tag === "Failure");
  });
it.effect.each(["success", "void", "generation", "failure"] satisfies ReadonlyArray<Mode>)(
  "concrete native executor requires exact direct acknowledgement (%s)",
  (mode) => scenario(mode).pipe(Effect.provide(realRepository)),
);

it.effect.each([
  "transport-success",
  "transport-spawn-revoked",
  "transport-history-revoked",
  "transport-send-revoked",
  "transport-generation",
  "transport-resume",
  "transport-failure",
] satisfies ReadonlyArray<Mode>)(
  "actual synthetic Codex transport preserves native boundary (%s)",
  (mode) => scenario(mode).pipe(Effect.provide(realRepository)),
);
