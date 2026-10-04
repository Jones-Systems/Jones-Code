import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import {
  assertOwnedDatabase,
  createOwnedRoot,
  observeSyntheticClose,
  sealSyntheticFixture,
  syntheticFixtureReceiptSha256,
} from "./guard.mjs";

import { assertSyntheticDatabaseSource } from "./sources.mjs";

const requestLimit = 49 * 1024;
const receiptLimit = 24 * 1024;
const contexts = new WeakMap();
const timestampBase = Date.parse("2026-10-02T12:00:00.000Z");
const nativeTables = [
  "native_creation_automation_enrollments",
  "native_creation_intents",
  "native_creation_normalized_commands",
  "native_creation_reserved_commands",
  "native_creation_reserved_command_identities",
  "native_creation_effect_facts",
];
const capturedTables = [
  "effect_sql_migrations",
  "jones_sql_migrations",
  "orchestration_events",
  "orchestration_command_receipts",
  "projection_projects",
  "projection_threads",
  "projection_thread_messages",
  "projection_thread_activities",
  "projection_thread_sessions",
  "projection_turns",
  "projection_pending_approvals",
  "projection_thread_proposed_plans",
  "projection_state",
  "worktree_ownership_leases",
  "provider_session_runtime",
  "checkpoint_diff_blobs",
  ...nativeTables,
];

function refuse(code, message, evidence) {
  const error = new Error(message);
  error.code = code;
  if (evidence !== undefined) error.evidence = evidence;
  throw error;
}

function freeze(value) {
  if (value && typeof value === "object" && !Object.isFrozen(value)) {
    Object.values(value).forEach(freeze);
    Object.freeze(value);
  }
  return value;
}

function canonical(value) {
  if (typeof value === "bigint") return ["integer", value.toString()];
  if (value instanceof Uint8Array) return ["bytes", Buffer.from(value).toString("hex")];
  if (value === null) return ["null"];
  if (Array.isArray(value)) return ["array", value.map(canonical)];
  if (typeof value === "object")
    return [
      "object",
      Object.keys(value)
        .sort()
        .map((key) => [key, canonical(value[key])]),
    ];
  if (typeof value === "number" && !Number.isFinite(value))
    refuse("invalid_capture", "nonfinite numbers cannot enter a canonical capture");
  return [typeof value, value];
}

function digest(value) {
  return NodeCrypto.createHash("sha256")
    .update(JSON.stringify(canonical(value)))
    .digest("hex");
}

function checkedOptions(options) {
  if (!options || typeof options !== "object")
    refuse("invalid_options", "fixture options required");
  if (Object.hasOwn(options, "profile"))
    refuse("unsupported_profile", "core fixtures use observed production defaults only");
  if (options.signal !== undefined && !(options.signal instanceof AbortSignal))
    refuse("invalid_options", "signal must be an AbortSignal");
  const databaseSource = assertSyntheticDatabaseSource(options.databaseSource);
  const recipe = { kind: "coherent-v1", historyTurns: 3, payloadBytes: 256, ...options.recipe };
  if (
    recipe.kind !== "coherent-v1" ||
    !Number.isSafeInteger(recipe.historyTurns) ||
    recipe.historyTurns < 3 ||
    recipe.historyTurns > 256 ||
    !Number.isSafeInteger(recipe.payloadBytes) ||
    recipe.payloadBytes < 1 ||
    recipe.payloadBytes > 64 * 1024
  )
    refuse("invalid_recipe", "coherent-v1 requires 3–256 turns and 1–65536 payload bytes");
  const policy = {
    ...options.policy,
    maxFiles: Math.min(options.policy?.maxFiles ?? 32, 32),
    maxReceiptBytes: Math.min(options.policy?.maxReceiptBytes ?? receiptLimit, receiptLimit),
  };
  return { ...options, databaseSource, recipe: freeze(recipe), policy };
}

async function loadSource(source) {
  const root = source.worktreePath;
  const require = NodeModule.createRequire(NodePath.join(root, "apps/server/package.json"));
  const dependencies = {
    Effect: "effect/Effect",
    Layer: "effect/Layer",
    ManagedRuntime: "effect/ManagedRuntime",
    Schema: "effect/Schema",
    Option: "effect/Option",
    Stream: "effect/Stream",
    Logger: "effect/Logger",
    SqlClient: "effect/unstable/sql/SqlClient",
    NodeServices: "@effect/platform-node/NodeServices",
  };
  const files = {
    Contracts: "packages/contracts/src/index.ts",
    Config: "apps/server/src/config.ts",
    Sqlite: "apps/server/src/persistence/Layers/Sqlite.ts",
    EngineLayer: "apps/server/src/orchestration/Layers/OrchestrationEngine.ts",
    EngineService: "apps/server/src/orchestration/Services/OrchestrationEngine.ts",
    SnapshotLayer: "apps/server/src/orchestration/Layers/ProjectionSnapshotQuery.ts",
    SnapshotService: "apps/server/src/orchestration/Services/ProjectionSnapshotQuery.ts",
    Pipeline: "apps/server/src/orchestration/Layers/ProjectionPipeline.ts",
    EventStore: "apps/server/src/persistence/Layers/OrchestrationEventStore.ts",
    Receipts: "apps/server/src/persistence/Layers/OrchestrationCommandReceipts.ts",
    Liveness: "apps/server/src/orchestration/ThreadBackgroundLiveness.ts",
    PlanProgress: "apps/server/src/orchestration/ThreadPlanProgress.ts",
    Resolver: "apps/server/src/project/RepositoryIdentityResolver.ts",
    Leases: "apps/server/src/orchestration/WorktreeOwnershipLease.ts",
    ProviderRuntime: "apps/server/src/persistence/ProviderSessionRuntime.ts",
    Projector: "apps/server/src/orchestration/projector.ts",
    Settings: "packages/contracts/src/settings.ts",
    Keybindings: "packages/contracts/src/keybindings.ts",
    Attachments: "apps/server/src/attachmentStore.ts",
  };
  if (source.sourceRevision === "e5a31aceec91484b64315c63dcce80f6e7581604") {
    files.Native = "apps/server/src/persistence/Layers/NativeCreationRepository.ts";
    files.NativePreparation = "apps/server/src/orchestration/NativeCreationPreparation.ts";
  }
  return Object.fromEntries(
    await Promise.all([
      ...Object.entries(dependencies).map(async ([key, specifier]) => [
        key,
        await import(NodeURL.pathToFileURL(require.resolve(specifier)).href),
      ]),
      ...Object.entries(files).map(async ([key, relative]) => [
        key,
        await import(NodeURL.pathToFileURL(NodePath.join(root, relative)).href),
      ]),
    ]),
  );
}

function orchestrationLayer(modules, paths, workspace, root) {
  const { Effect, Layer } = modules;
  return Layer.mergeAll(
    modules.EngineLayer.OrchestrationEngineLive.pipe(
      Layer.provide(modules.SnapshotLayer.OrchestrationProjectionSnapshotQueryLive),
      Layer.provide(modules.Pipeline.OrchestrationProjectionPipelineLive),
    ),
    modules.SnapshotLayer.OrchestrationProjectionSnapshotQueryLive,
  ).pipe(
    Layer.provideMerge(modules.Liveness.layer),
    Layer.provide(modules.PlanProgress.layer),
    Layer.provide(modules.EventStore.OrchestrationEventStoreLive),
    Layer.provideMerge(modules.Receipts.OrchestrationCommandReceiptRepositoryLive),
    Layer.provide(
      Layer.succeed(modules.Resolver.RepositoryIdentityResolver, {
        resolve: () => Effect.succeed(null),
      }),
    ),
    Layer.provideMerge(modules.Sqlite.makeSqlitePersistenceLive(paths.dbPath)),
    Layer.provideMerge(modules.Config.ServerConfig.layerTest(workspace, root)),
    Layer.provideMerge(modules.NodeServices.layer),
    Layer.provide(modules.Logger.layer([])),
  );
}

function requireContext(context) {
  const state = context && contexts.get(context);
  if (!state || state.status !== "open")
    refuse("invalid_context", "capture requires the original open in-process fixture context");
  return state;
}

function some(modules, value, label) {
  if (modules.Option.isNone(value)) refuse("incoherent_fixture", `${label} was absent`);
  return value.value;
}

async function produceFiles(state) {
  const { modules, paths, workspace, worktree } = state;
  const { Schema } = modules;
  const settings = Schema.encodeSync(modules.Settings.ServerSettings)({
    ...modules.Settings.DEFAULT_SERVER_SETTINGS,
    providerInstances: { codex: { driver: "codex", displayName: "Synthetic offline", config: {} } },
  });
  const keybindings = Schema.decodeUnknownSync(modules.Keybindings.KeybindingsConfig)([
    { key: "mod+shift+k", command: "thread.stop" },
  ]);
  const attachmentId = modules.Attachments.createAttachmentId("fixture-history", ".png");
  const attachment = Buffer.from(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a4WQAAAAASUVORK5CYII=",
    "base64",
  );
  if (!attachmentId) refuse("incoherent_fixture", "synthetic attachment identity was refused");
  const attachmentPath = NodePath.join(paths.attachmentsDir, `${attachmentId}.png`);
  const files = [
    [paths.settingsPath, `${JSON.stringify(settings)}\n`],
    [paths.keybindingsConfigPath, `${JSON.stringify(keybindings)}\n`],
    [paths.environmentIdPath, `${NodeCrypto.randomUUID()}\n`],
    [NodePath.join(workspace, "fixture.txt"), "Synthetic workspace\n"],
    [NodePath.join(worktree, "fixture.txt"), "Synthetic worktree\n"],
    [attachmentPath, attachment],
  ];
  for (const [path, bytes] of files) {
    await NodeFSP.mkdir(NodePath.dirname(path), { recursive: true, mode: 0o700 });
    await NodeFSP.writeFile(path, bytes, { flag: "wx", mode: 0o600 });
  }
  state.files = files.map(([path]) => path);
  if (
    modules.Attachments.resolveAttachmentPathById({
      attachmentsDir: paths.attachmentsDir,
      attachmentId,
    }) !== attachmentPath
  )
    refuse("incoherent_fixture", "synthetic attachment path was refused");
  state.attachment = {
    type: "image",
    id: attachmentId,
    name: "fixture.png",
    mimeType: "image/png",
    sizeBytes: attachment.length,
  };
}

async function produceNative(state, dispatch) {
  const { modules, workspace, worktree } = state;
  if (!modules.Native) return;
  const { Effect, Schema, Contracts, NativePreparation: preparationModule } = modules;
  const repository = await state.run(modules.Native.make);
  const binding = Schema.decodeUnknownSync(preparationModule.NativePreparationBinding)({
    backend_instance: "synthetic-backend",
    environment_id: "synthetic-environment",
    project_id: "fixture-project",
    project_cwd: workspace,
    account_ref: "synthetic-account",
    runtime_mode: "full-access",
    interaction_mode: "default",
    base_branch: "main",
    start_from_origin: false,
    run_setup_script: false,
    provider_model_selection: { instanceId: "codex", model: "synthetic-model" },
  });
  const raw = preparationModule.nativePreparationCommand(
    "fixture-native-operation",
    binding,
    "Synthetic native prompt",
    "Synthetic native thread",
    state.now(),
  );
  const preparation = await state.run(
    preparationModule.validateNativeCreationPreparation(
      new TextEncoder().encode(
        preparationModule.nativeCreationCanonicalJson({
          schema: "voice.t3-bootstrap-preparation/v1",
          operation_id: "fixture-native-operation",
          preparation_id: raw.commandId.replace("voice-command-", "voice-bootstrap-"),
          binding,
          command: raw,
          binding_digest: preparationModule.nativeCreationSha256(
            preparationModule.nativeCreationCanonicalJson(binding),
          ),
          prompt_digest: preparationModule.nativeCreationSha256(raw.message.text),
          command_digest: preparationModule.nativeCreationSha256(
            preparationModule.nativeCreationCanonicalJson(raw),
          ),
        }),
      ),
    ),
  );
  const historical = Schema.decodeUnknownSync(Contracts.NativeCreationHistoricalBinding)({
    backendInstance: binding.backend_instance,
    environmentId: binding.environment_id,
    projectId: binding.project_id,
    projectCwd: workspace,
    accountRef: binding.account_ref,
    accountBindingId: "synthetic-account-binding",
    accountBindingRevision: 1,
    providerModelSelection: binding.provider_model_selection,
    runtimeMode: binding.runtime_mode,
    interactionMode: binding.interaction_mode,
    baseBranch: binding.base_branch,
    startFromOrigin: false,
    runSetupScript: false,
    requestedBranch: raw.bootstrap.prepareWorktree.branch,
  });
  const claimId = "fixture-native-claim";
  const authorize = Effect.succeed(historical);
  await state.run(
    repository.claim(
      {
        preparation,
        resources: {
          projectCwd: workspace,
          branch: historical.requestedBranch,
          worktreePath: worktree,
        },
        claimId,
        claimedBootId: "synthetic-boot",
        claimedAt: state.now(),
        actorSessionId: "synthetic-session",
        grantId: "synthetic-grant",
        grantRevision: 1,
      },
      authorize,
    ),
  );
  await state.run(
    repository.reserveCommandIdentities(claimId, [
      raw.commandId,
      `${raw.commandId}:bootstrap-thread-create`,
    ]),
  );
  await state.run(
    repository.recordNormalizedCommand(
      claimId,
      Schema.decodeUnknownSync(Contracts.OrchestrationCommand)({
        ...raw,
        modelSelection: binding.provider_model_selection,
      }),
    ),
  );
  const command = Schema.decodeUnknownSync(Contracts.OrchestrationCommand)({
    type: "thread.create",
    commandId: `${raw.commandId}:bootstrap-thread-create`,
    threadId: raw.threadId,
    ...raw.bootstrap.createThread,
  });
  await state.run(repository.reserveCommand(claimId, command));
  const start = await state.run(
    repository.startEffect(
      claimId,
      {
        effectId: "fixture-native-create",
        kind: "native_command",
        phase: "started",
        timestamp: state.now(),
        commandId: command.commandId,
        threadId: command.threadId,
        commandType: command.type,
        commandDigest: preparationModule.nativeCreationCommandDigest(command),
      },
      authorize,
    ),
  );
  await dispatch(command, { bootstrapEffect: { claimId, effectId: start.effectId } });
  state.native = { repository, claimId, threadId: command.threadId };
}

async function populate(state) {
  const { modules, recipe } = state;
  const decode = modules.Schema.decodeUnknownSync(modules.Contracts.OrchestrationCommand);
  const dispatch = async (command, options) => {
    const decoded = decode(command);
    const result = await state.run(state.engine.dispatch(decoded, options));
    return result;
  };
  await dispatch({
    type: "project.create",
    commandId: "fixture-project-create",
    projectId: "fixture-project",
    title: "Synthetic project",
    workspaceRoot: state.workspace,
    createdAt: state.now(),
  });
  for (const name of ["empty", "history", "leased"]) {
    await dispatch({
      type: "thread.create",
      commandId: `fixture-${name}-create`,
      threadId: `fixture-${name}`,
      projectId: "fixture-project",
      title: `Synthetic ${name}`,
      modelSelection: { instanceId: "codex", model: "synthetic-model" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: name === "leased" ? "synthetic-branch" : null,
      worktreePath: name === "leased" ? state.worktree : null,
      createdAt: state.now(),
    });
  }
  const payload = "x".repeat(recipe.payloadBytes);
  for (let index = 1; index <= recipe.historyTurns; index += 1) {
    const turnId = `fixture-turn-${index}`;
    const userId = `fixture-user-${index}`;
    const assistantId = `fixture-assistant-${index}`;
    const requestedAt = state.now();
    await dispatch({
      type: "thread.turn.start",
      commandId: `fixture-turn-start-${index}`,
      threadId: "fixture-history",
      message: {
        messageId: userId,
        role: "user",
        text: `Synthetic ${index} ${payload}`,
        attachments: index === 1 ? [state.attachment] : [],
      },
      runtimeMode: "full-access",
      interactionMode: "default",
      createdAt: requestedAt,
    });
    await dispatch({
      type: "thread.session.set",
      commandId: `fixture-session-${index}`,
      threadId: "fixture-history",
      session: {
        threadId: "fixture-history",
        status: "running",
        providerName: "codex",
        providerInstanceId: "codex",
        runtimeMode: "full-access",
        activeTurnId: turnId,
        lastError: null,
        updatedAt: requestedAt,
      },
      createdAt: requestedAt,
    });
    await dispatch({
      type: "thread.message.assistant.delta",
      commandId: `fixture-assistant-delta-${index}`,
      threadId: "fixture-history",
      messageId: assistantId,
      turnId,
      delta: `Synthetic answer ${index} ${payload}`,
      createdAt: state.now(),
    });
    await dispatch({
      type: "thread.message.assistant.complete",
      commandId: `fixture-assistant-complete-${index}`,
      threadId: "fixture-history",
      messageId: assistantId,
      turnId,
      createdAt: state.now(),
    });
    await dispatch({
      type: "thread.activity.append",
      commandId: `fixture-activity-${index}`,
      threadId: "fixture-history",
      activity: {
        id: `fixture-activity-${index}`,
        kind: "tool.completed",
        summary: "Synthetic offline work",
        tone: "info",
        turnId,
        payload: { synthetic: true },
        createdAt: state.now(),
      },
      createdAt: state.now(),
    });
    const completedAt = state.now();
    await dispatch({
      type: "thread.turn.diff.complete",
      commandId: `fixture-checkpoint-${index}`,
      threadId: "fixture-history",
      turnId,
      completedAt,
      checkpointRef: `refs/t3/checkpoints/fixture-history/turn/${index}`,
      status: "ready",
      files: [{ path: "fixture.txt", kind: "modified", additions: 1, deletions: 0 }],
      assistantMessageId: assistantId,
      checkpointTurnCount: index,
      createdAt: completedAt,
    });
    await dispatch({
      type: "thread.session.set",
      commandId: `fixture-turn-complete-${index}`,
      threadId: "fixture-history",
      session: {
        threadId: "fixture-history",
        status: "ready",
        providerName: "codex",
        providerInstanceId: "codex",
        runtimeMode: "full-access",
        activeTurnId: null,
        lastError: null,
        updatedAt: completedAt,
      },
      turnSettlement: { turnId, state: "completed", completedAt },
      createdAt: completedAt,
    });
  }
  await dispatch({
    type: "thread.session.set",
    commandId: "fixture-session-stopped",
    threadId: "fixture-history",
    session: {
      threadId: "fixture-history",
      status: "stopped",
      providerName: "codex",
      providerInstanceId: "codex",
      runtimeMode: "full-access",
      activeTurnId: null,
      lastError: null,
      updatedAt: state.now(),
    },
    createdAt: state.now(),
  });
  await dispatch({
    type: "thread.activity.append",
    commandId: "fixture-activity-unlinked",
    threadId: "fixture-history",
    activity: {
      id: "fixture-activity-unlinked",
      kind: "fixture.note",
      summary: "Synthetic thread note",
      tone: "info",
      turnId: null,
      payload: { synthetic: true },
      createdAt: state.now(),
    },
    createdAt: state.now(),
  });
  const provider = await state.run(modules.ProviderRuntime.make);
  await state.run(
    provider.upsert({
      threadId: "fixture-history",
      providerName: "codex",
      providerInstanceId: "codex",
      adapterKey: "synthetic-offline",
      runtimeMode: "full-access",
      status: "stopped",
      lastSeenAt: state.now(),
      resumeCursor: { synthetic: true, turn: recipe.historyTurns },
      runtimePayload: { synthetic: true, payload },
    }),
  );
  const sql = await state.run(modules.Effect.service(modules.SqlClient.SqlClient));
  await state.run(sql`INSERT INTO checkpoint_diff_blobs (thread_id, from_turn_count, to_turn_count, diff, created_at)
    VALUES (${"fixture-history"}, ${0}, ${recipe.historyTurns}, ${"Synthetic diff\n"}, ${state.now()})`);
  const leases = await state.run(modules.Leases.makeWorktreeOwnershipLeaseStore());
  const incarnation = some(
    modules,
    await state.run(leases.getThreadIncarnation("fixture-leased")),
    "lease incarnation",
  );
  const leaseInput = {
    resourcePath: state.worktree,
    leaseId: "fixture-lease-first",
    ownerThreadId: "fixture-leased",
    ownerIncarnation: incarnation,
    branch: "synthetic-branch",
    nowMs: timestampBase,
    expiresAtMs: timestampBase + 300000,
  };
  const first = some(modules, await state.run(leases.acquire(leaseInput)), "first lease");
  const current = some(
    modules,
    await state.run(
      leases.acquire({ ...leaseInput, leaseId: "fixture-lease-current", nowMs: timestampBase + 1 }),
    ),
    "rotated lease",
  );
  const staleRenewed = await state.run(
    leases.renew({ ...first, nowMs: timestampBase + 2, expiresAtMs: timestampBase + 300002 }),
  );
  await state.run(leases.release(first));
  const afterStaleRelease = await state.run(leases.listAll());
  const foreign = await state.run(
    leases.acquire({
      ...leaseInput,
      ownerThreadId: "fixture-empty",
      leaseId: "fixture-lease-foreign",
    }),
  );
  if (
    staleRenewed ||
    !afterStaleRelease.some((lease) => lease.leaseId === current.leaseId) ||
    modules.Option.isSome(foreign)
  )
    refuse("incoherent_fixture", "production lease generation fencing failed");
  state.leaseFencing = freeze({
    incarnation,
    currentLeaseId: current.leaseId,
    staleRenewed,
    staleReleasePreservedCurrent: true,
    foreignAcquired: false,
  });
  await produceNative(state, dispatch);
}

async function quiesceEngineLeases(state) {
  const leases = await state.run(state.engine.listWorktreeOwnershipLeases);
  for (const lease of leases) {
    if (lease.leaseId !== state.leaseFencing.currentLeaseId)
      await state.run(state.engine.releaseWorktreeOwnership(lease));
  }
}

async function query(state, text, values = []) {
  const { Effect, SqlClient } = state.modules;
  return state.run(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      return yield* sql.unsafe(text, values);
    }).pipe(Effect.provideService(SqlClient.SafeIntegers, true)),
  );
}

const identifier = (value) => {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(value))
    refuse("invalid_capture", "invalid observed SQL identifier");
  return `"${value}"`;
};

async function tableSummary(state, table) {
  const info = await query(state, `PRAGMA table_info(${identifier(table)})`);
  if (info.length === 0) return { status: "absent" };
  let keys = info
    .filter((column) => Number(column.pk) > 0)
    .sort((left, right) => Number(left.pk) - Number(right.pk))
    .map((column) => column.name);
  if (keys.length === 0) {
    const indexes = await query(state, `PRAGMA index_list(${identifier(table)})`);
    const unique = indexes.find(
      (index) => Number(index.unique) === 1 && Number(index.partial) === 0,
    );
    if (!unique) refuse("invalid_capture", `${table} has no observed unique paging key`);
    keys = (await query(state, `PRAGMA index_info(${identifier(unique.name)})`))
      .sort((left, right) => Number(left.seqno) - Number(right.seqno))
      .map((column) => column.name);
  }
  const hash = NodeCrypto.createHash("sha256");
  const order = keys.map(identifier).join(",");
  hash.update(`${JSON.stringify(info.map((column) => column.name).sort())}\n`);
  let count = 0;
  let last;
  while (true) {
    const values = last ? keys.map((key) => last[key]) : [];
    const where = last ? ` WHERE (${order}) > (${keys.map(() => "?").join(",")})` : "";
    const rows = await query(
      state,
      `SELECT * FROM ${identifier(table)}${where} ORDER BY ${order} LIMIT 128`,
      values,
    );
    for (const row of rows) hash.update(`${JSON.stringify(canonical(row))}\n`);
    count += rows.length;
    if (rows.length < 128) break;
    last = rows.at(-1);
  }
  return { status: "present", count, sha256: hash.digest("hex"), pagingKey: keys };
}

function publicModel(model) {
  return {
    snapshotSequence: model.snapshotSequence,
    projects: [...model.projects]
      .sort((left, right) => left.id.localeCompare(right.id))
      .map((project) => ({ ...project, repositoryIdentity: project.repositoryIdentity ?? null })),
    threads: [...model.threads]
      .sort((left, right) => left.id.localeCompare(right.id))
      .map((thread) => ({
        ...thread,
        pinnedAt: thread.pinnedAt ?? null,
        pinOrderKey: thread.pinOrderKey ?? null,
        titleRegeneration: thread.titleRegeneration ?? null,
        titleState: thread.titleState ?? null,
        messages: [...thread.messages].sort((left, right) => left.id.localeCompare(right.id)),
        activities: [...thread.activities].sort((left, right) => left.id.localeCompare(right.id)),
        checkpoints: [...thread.checkpoints].sort(
          (left, right) => left.checkpointTurnCount - right.checkpointTurnCount,
        ),
      })),
  };
}

function structuralModelDifferences(snapshot, replay) {
  const differences = [];
  const valueType = (value) =>
    value === null
      ? "null"
      : Array.isArray(value)
        ? "array"
        : value instanceof Uint8Array
          ? "bytes"
          : typeof value;
  const describe = (value, present) => ({
    present,
    type: present ? valueType(value) : "absent",
    sha256: digest(value),
    ...(Array.isArray(value) ? { length: value.length } : {}),
  });
  const visit = (left, right, path, leftPresent = true, rightPresent = true) => {
    if (differences.length === 8 || (leftPresent === rightPresent && Object.is(left, right)))
      return;
    const type = valueType(left);
    if (
      leftPresent &&
      rightPresent &&
      type === valueType(right) &&
      (type === "object" || type === "array")
    ) {
      if (type !== "array" || left.length === right.length) {
        const keys =
          type === "array"
            ? Array.from({ length: left.length }, (_, index) => String(index))
            : [...new Set([...Object.keys(left), ...Object.keys(right)])].sort();
        for (const key of keys) {
          const segment = key.replaceAll("~", "~0").replaceAll("/", "~1");
          visit(
            left[key],
            right[key],
            `${path}/${segment}`,
            Object.hasOwn(left, key),
            Object.hasOwn(right, key),
          );
          if (differences.length === 8) return;
        }
        return;
      }
    }
    const leftSummary = describe(left, leftPresent);
    const rightSummary = describe(right, rightPresent);
    if (leftPresent !== rightPresent || leftSummary.sha256 !== rightSummary.sha256)
      differences.push({ path, snapshot: leftSummary, replay: rightSummary });
  };
  visit(snapshot, replay, "");
  return differences;
}

export async function captureFixture(context) {
  const state = requireContext(context);
  const { modules } = state;
  const tables = {};
  for (const table of capturedTables) tables[table] = await tableSummary(state, table);
  const snapshot = await state.run(state.snapshotQuery.getSnapshot());
  let replay = modules.Projector.createEmptyReadModel(new Date(timestampBase).toISOString());
  let eventCursor = 0;
  while (true) {
    const events = await state.run(
      modules.Stream.runCollect(state.engine.readEvents(eventCursor, 128)),
    );
    for (const event of events) {
      replay = await state.run(modules.Projector.projectEvent(replay, event));
      eventCursor = event.sequence;
    }
    if (events.length < 128) break;
  }
  const snapshotModel = publicModel(snapshot);
  const replayModel = publicModel(replay);
  const snapshotDigest = digest(snapshotModel);
  const replayDigest = digest(replayModel);
  const recent = some(
    modules,
    await state.run(
      state.snapshotQuery.getThreadDetailSnapshot("fixture-history", { turnLimit: 2 }),
    ),
    "recent page",
  );
  const cursor = recent.page?.beforeCursor;
  if (!cursor) refuse("incoherent_fixture", "history recipe did not create an older-page cursor");
  const older = some(
    modules,
    await state.run(
      state.snapshotQuery.getThreadDetailSnapshot("fixture-history", {
        turnLimit: 1,
        beforeCursor: cursor,
      }),
    ),
    "older page",
  );
  const recentIds = recent.thread.messages.map((message) => message.id);
  const olderIds = older.thread.messages.map((message) => message.id);
  const overlap = olderIds.filter((id) => recentIds.includes(id)).length;
  const integrityRows = await query(state, "PRAGMA integrity_check");
  const integrity = integrityRows.map((row) => Object.values(row)[0]);
  const foreignKeys = await query(state, "PRAGMA foreign_key_check");
  const links = await query(
    state,
    `SELECT
    (SELECT COUNT(*) FROM orchestration_command_receipts r LEFT JOIN orchestration_events e ON e.sequence=r.result_sequence WHERE r.status='accepted' AND
      (e.sequence IS NULL OR e.command_id IS NOT r.command_id OR e.aggregate_kind IS NOT r.aggregate_kind OR e.stream_id IS NOT r.aggregate_id)) AS missing_receipt_events,
    (SELECT COUNT(*) FROM projection_threads t LEFT JOIN projection_projects p ON p.project_id=t.project_id WHERE p.project_id IS NULL) AS missing_thread_projects,
    (SELECT COUNT(*) FROM projection_thread_messages m LEFT JOIN projection_threads t ON t.thread_id=m.thread_id WHERE t.thread_id IS NULL) AS missing_message_threads,
    (SELECT COUNT(*) FROM (SELECT aggregate_kind,stream_id,COUNT(*) AS n,MAX(stream_version) AS v,MIN(stream_version) AS first FROM orchestration_events GROUP BY aggregate_kind,stream_id) WHERE first<>0 OR n<>v+1) AS noncontiguous_streams`,
  );
  const coupling = Object.fromEntries(
    Object.entries(links[0]).map(([key, value]) => [key, Number(value)]),
  );
  const maxSequence = Number(
    (await query(state, "SELECT COALESCE(MAX(sequence),0) AS n FROM orchestration_events"))[0].n,
  );
  const projectionCursors = (
    await query(
      state,
      "SELECT projector,last_applied_sequence FROM projection_state ORDER BY projector",
    )
  ).map((row) => ({ projector: row.projector, sequence: Number(row.last_applied_sequence) }));
  const liveProjectorNames = new Set(Object.values(modules.Pipeline.ORCHESTRATION_PROJECTOR_NAMES));
  const liveCursors = projectionCursors.filter((row) => liveProjectorNames.has(row.projector));
  // Attachment cleanup advances at bootstrap; normal traffic advances the nine exported projectors.
  const bootstrapCursors = projectionCursors.filter(
    (row) => row.projector === "projection.attachment-cleanup",
  );
  const pragmas = {};
  for (const name of [
    "journal_mode",
    "synchronous",
    "foreign_keys",
    "busy_timeout",
    "journal_size_limit",
    "page_size",
    "user_version",
  ])
    pragmas[name] = Object.values((await query(state, `PRAGMA ${name}`))[0])[0];
  for (const name of Object.keys(pragmas))
    if (typeof pragmas[name] === "bigint") pragmas[name] = Number(pragmas[name]);
  const files = [];
  for (const path of state.files) {
    const info = await NodeFSP.lstat(path);
    if (!info.isFile() || info.isSymbolicLink() || info.nlink !== 1)
      refuse("invalid_capture", "synthetic file identity changed");
    const bytes = await NodeFSP.readFile(path);
    files.push({
      relativePath: NodePath.relative(state.root, path),
      sizeBytes: bytes.length,
      sha256: NodeCrypto.createHash("sha256").update(bytes).digest("hex"),
    });
  }
  const reference = (path) => {
    const relative = NodePath.relative(state.root, path);
    if (
      relative === ".." ||
      relative.startsWith(`..${NodePath.sep}`) ||
      NodePath.isAbsolute(relative)
    )
      refuse("incoherent_fixture", "synthetic reference escapes the owned root");
    return relative;
  };
  const checkpointFiles = snapshot.threads.flatMap((thread) =>
    thread.checkpoints.flatMap((checkpoint) => checkpoint.files),
  );
  for (const file of checkpointFiles) {
    const relative = reference(NodePath.resolve(state.worktree, file.path));
    if (!files.some((entry) => entry.relativePath === relative))
      refuse("incoherent_fixture", "checkpoint metadata names a missing synthetic file");
  }
  const blobs = (
    await query(
      state,
      "SELECT thread_id,from_turn_count,to_turn_count,diff FROM checkpoint_diff_blobs ORDER BY thread_id,from_turn_count,to_turn_count",
    )
  ).map((row) => ({
    threadId: row.thread_id,
    fromTurnCount: Number(row.from_turn_count),
    toTurnCount: Number(row.to_turn_count),
    sizeBytes: Buffer.byteLength(row.diff),
    sha256: NodeCrypto.createHash("sha256").update(row.diff).digest("hex"),
  }));
  const ledgers = {};
  for (const table of ["effect_sql_migrations", "jones_sql_migrations"])
    ledgers[table] = (
      await query(state, `SELECT migration_id,name FROM ${identifier(table)} ORDER BY migration_id`)
    ).map((row) => ({ id: Number(row.migration_id), name: row.name }));
  const attachmentRefs = snapshot.threads.flatMap((thread) =>
    thread.messages.flatMap((message) => message.attachments ?? []),
  );
  const attachmentFiles = attachmentRefs.map((attachment) => {
    const path = modules.Attachments.resolveAttachmentPathById({
      attachmentsDir: state.paths.attachmentsDir,
      attachmentId: attachment.id,
    });
    const file = files.find((entry) => NodePath.join(state.root, entry.relativePath) === path);
    if (!file || file.sizeBytes !== attachment.sizeBytes)
      refuse("incoherent_fixture", "attachment metadata differs from owned file");
    return { id: attachment.id, relativePath: file.relativePath, sha256: file.sha256 };
  });
  let native = { status: "absent" };
  if (state.native) {
    const history = await state.run(
      state.native.repository.readHistoryByClaim(state.native.claimId),
    );
    native = {
      status: "present",
      claimId: history.intent.claimId,
      normalizedCommandDigest: history.normalizedCommandDigest,
      effectPhases: history.effects.map((fact) => fact.phase),
      historySha256: digest(history),
    };
  }
  const capture = {
    schema: "jones-performance-capture/v1",
    databaseSource: state.databaseSource,
    recipe: state.recipe,
    runtime: {
      nodeVersion: process.versions.node,
      sqliteVersion: (await query(state, "SELECT sqlite_version() AS version"))[0].version,
      pragmas,
      profile: "observed-production-defaults",
    },
    tables,
    ledgers,
    coupling: {
      ...coupling,
      maxSequence,
      snapshotSequence: snapshot.snapshotSequence,
      projectionCursors,
    },
    readModel: {
      projectCount: snapshot.projects.length,
      threadCount: snapshot.threads.length,
      historyMessages: snapshot.threads.find((thread) => thread.id === "fixture-history")?.messages
        .length,
      snapshotSha256: snapshotDigest,
      replaySha256: replayDigest,
      equivalent: snapshotDigest === replayDigest,
    },
    pages: {
      cursor,
      recentMessageIds: recentIds,
      olderMessageIds: olderIds,
      overlap,
      recentSha256: digest(recent),
      olderSha256: digest(older),
      snapshotSequence: recent.snapshotSequence,
    },
    leaseFencing: state.leaseFencing,
    native,
    files,
    attachmentFiles,
    blobs,
    references: {
      workspaces: snapshot.projects.map((project) => reference(project.workspaceRoot)),
      worktrees: snapshot.threads
        .filter((thread) => thread.worktreePath !== null)
        .map((thread) => reference(thread.worktreePath)),
      checkpointFiles: [
        ...new Set(
          checkpointFiles.map((file) => reference(NodePath.resolve(state.worktree, file.path))),
        ),
      ],
    },
    integrity: { results: integrity, ok: integrity.length === 1 && integrity[0] === "ok" },
    foreignKeys: { violations: foreignKeys.length, sha256: digest(foreignKeys) },
  };
  const failedPredicates = {
    readModel: !capture.readModel.equivalent,
    pageOverlap: overlap !== 0,
    integrity: !capture.integrity.ok,
    foreignKeys: foreignKeys.length !== 0,
    coupling: Object.values(coupling).some((value) => value !== 0),
    snapshotCursor: snapshot.snapshotSequence !== maxSequence,
    liveProjectorSet:
      liveProjectorNames.size !== 9 ||
      liveCursors.length !== 9 ||
      new Set(liveCursors.map((row) => row.projector)).size !== 9,
    liveProjectorCursor: liveCursors.some(
      (row) => !Number.isSafeInteger(row.sequence) || row.sequence !== maxSequence,
    ),
    bootstrapCursor:
      bootstrapCursors.length !== 1 ||
      bootstrapCursors.some(
        (row) =>
          !Number.isSafeInteger(row.sequence) || row.sequence < 0 || row.sequence > maxSequence,
      ),
    unknownProjector: projectionCursors.some(
      (row) =>
        !liveProjectorNames.has(row.projector) && row.projector !== "projection.attachment-cleanup",
    ),
    native: Boolean(
      state.native &&
      (!native.normalizedCommandDigest || !native.effectPhases.includes("completed")),
    ),
  };
  if (Object.values(failedPredicates).some(Boolean)) {
    const message = "production state failed replay, cursor, coupling or integrity checks";
    const diagnostic = JSON.stringify({
      failedPredicates,
      structuralDifferencesLimit: 8,
      structuralDifferences: failedPredicates.readModel
        ? structuralModelDifferences(snapshotModel, replayModel)
        : [],
    });
    const boundedDiagnostic =
      Buffer.byteLength(message) + 1 + Buffer.byteLength(diagnostic) <= 4 * 1024
        ? diagnostic
        : JSON.stringify({
            failedPredicates,
            structuralDifferencesOmitted: "4 KiB message budget",
          });
    refuse("incoherent_fixture", `${message}\n${boundedDiagnostic}`, freeze(capture));
  }
  return freeze(capture);
}

// Internal producer results carry the genuine owner only within this audited process.
export async function produceFixture(input, use) {
  const options = checkedOptions(input);
  if (typeof use !== "function") refuse("invalid_callback", "fixture callback required");
  const owner = createOwnedRoot(options);
  const root = owner.creationReceipt.canonicalRootPath;
  const result = { owner, closeKnown: true };
  let state;
  let runtime;
  let permit;
  try {
    const modules = await loadSource(options.databaseSource);
    const paths = await modules.Effect.runPromise(
      modules.Config.deriveServerPaths(root, undefined).pipe(
        modules.Effect.provide(modules.NodeServices.layer),
        modules.Effect.provide(modules.Logger.layer([])),
      ),
    );
    const workspace = NodePath.join(root, "workspace");
    const worktree = NodePath.join(paths.worktreesDir, "fixture");
    for (const path of [paths.stateDir, workspace, worktree])
      await NodeFSP.mkdir(path, { recursive: true, mode: 0o700 });
    permit = assertOwnedDatabase(owner, {
      databaseRelativePath: NodePath.relative(root, paths.dbPath),
      access: "create",
    });
    result.closeKnown = false;
    runtime = modules.ManagedRuntime.make(orchestrationLayer(modules, paths, workspace, root));
    const cancellation = new AbortController();
    const signal = options.signal
      ? AbortSignal.any([options.signal, cancellation.signal])
      : cancellation.signal;
    const active = new Set();
    let clock = 0;
    state = {
      ...options,
      owner,
      root,
      modules,
      paths,
      workspace,
      worktree,
      runtime,
      cancellation,
      active,
      status: "open",
      now: () => new Date(timestampBase + clock++ * 1000).toISOString(),
    };
    state.run = (effect) => {
      if (state.status !== "open") refuse("closed_context", "fixture runtime is closing or closed");
      const promise = runtime.runPromise(effect, { signal });
      active.add(promise);
      void promise.then(
        () => active.delete(promise),
        () => active.delete(promise),
      );
      return promise;
    };
    state.engine = await state.run(
      modules.Effect.service(modules.EngineService.OrchestrationEngineService),
    );
    state.snapshotQuery = await state.run(
      modules.Effect.service(modules.SnapshotService.ProjectionSnapshotQuery),
    );
    const context = Object.freeze({
      owner,
      paths: freeze(paths),
      databaseSource: options.databaseSource,
      recipe: options.recipe,
      engine: state.engine,
      snapshotQuery: state.snapshotQuery,
      run: state.run,
    });
    contexts.set(context, state);
    await produceFiles(state);
    await populate(state);
    await quiesceEngineLeases(state);
    result.value = await use(context);
    await quiesceEngineLeases(state);
    result.capture = await captureFixture(context);
  } catch (error) {
    result.error = error;
  } finally {
    if (runtime && permit) {
      if (state) {
        state.status = "closing";
        state.cancellation.abort();
        await Promise.allSettled([...state.active]);
      }
      let closedProof;
      try {
        closedProof = await observeSyntheticClose(owner, {
          permit,
          producerStep: options.binding.taskRef,
          resource: runtime,
          close: (resource) => resource.dispose(),
        });
        result.closeKnown = true;
      } catch (error) {
        result.closeKnown = false;
        result.error ??= error;
      }
      if (closedProof && !result.error) {
        try {
          assertSyntheticDatabaseSource(options.databaseSource);
          result.receipt = await sealSyntheticFixture(owner, {
            databaseRelativePath: permit.relativePath,
            producerStep: options.binding.taskRef,
            closedProof,
          });
          result.receiptSha256 = syntheticFixtureReceiptSha256(result.receipt);
          result.captureSha256 = NodeCrypto.createHash("sha256")
            .update(`${JSON.stringify(result.capture)}\n`)
            .digest("hex");
        } catch (error) {
          result.retainReason = error.code ?? "seal_failed";
          result.error = error;
        }
      }
      if (state) state.status = "closed";
    }
  }
  return result;
}

async function workerMain() {
  if (process.argv.length !== 3 || Buffer.byteLength(process.argv[2]) > requestLimit)
    refuse("request_limit", "worker requires exactly one bounded JSON request");
  const input = JSON.parse(process.argv[2]);
  if (input.schema !== "jones-performance-fixture-request/v1")
    refuse("invalid_request", "worker request schema differs");
  const controller = new AbortController();
  const abort = () => controller.abort();
  process.once("SIGTERM", abort);
  process.once("SIGINT", abort);
  try {
    const produced = await produceFixture(
      { ...input.options, signal: controller.signal },
      () => undefined,
    );
    if (produced.error) {
      process.exitCode = 1;
      process.stdout.write(
        `${JSON.stringify({
          schema: "jones-performance-fixture-failure/v1",
          code: produced.error.code ?? "producer_failed",
          closeKnown: produced.closeKnown,
          retainReason: produced.retainReason,
          creationReceipt: produced.owner.creationReceipt,
        })}\n`,
      );
      return;
    }
    const envelope = {
      schema: "jones-performance-fixture-envelope/v1",
      databaseSource: input.options.databaseSource,
      capture: produced.capture,
      captureSha256: produced.captureSha256,
      receipt: produced.receipt,
      receiptSha256: produced.receiptSha256,
    };
    if (Buffer.byteLength(`${JSON.stringify(produced.receipt)}\n`) > receiptLimit)
      refuse("receipt_limit", "complete sealed receipt exceeds 24 KiB");
    const bytes = `${JSON.stringify(envelope)}\n`;
    if (Buffer.byteLength(bytes) > 49 * 1024)
      refuse("envelope_limit", "complete worker envelope exceeds 49 KiB");
    process.stdout.write(bytes);
  } finally {
    process.removeListener("SIGTERM", abort);
    process.removeListener("SIGINT", abort);
  }
}

if (
  process.argv[1] &&
  NodeURL.pathToFileURL(NodePath.resolve(process.argv[1])).href === import.meta.url
) {
  await workerMain().catch((error) => {
    process.stderr.write(`${error.code ?? "worker_failed"}\n`);
    process.exitCode = 1;
  });
}
