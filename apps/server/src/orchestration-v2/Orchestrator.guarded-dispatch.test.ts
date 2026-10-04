import { assert, it } from "@effect/vitest";
import { inspect } from "node:util";
import {
  AuthSessionId,
  CommandId,
  CheckpointRef,
  EnvironmentAuthenticatedPrincipal,
  EventId,
  MessageId,
  NativeCreationHistoricalBinding,
  OrchestrationV2Command,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  ThreadId,
  WorktreeOwnershipConflictError,
  type ThreadTurnDispatchGuardV2,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { AuthSessionRepository, make as makeAuthSessions } from "../persistence/AuthSessions.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import { layer as nativeRepositoryLayer } from "../persistence/Layers/NativeCreationRepository.ts";
import { NativeCreationRepository, NativeCreationRepositoryError } from "../persistence/Services/NativeCreationRepository.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import { DispatchGuardRejectedError } from "./DispatchGuard.ts";
import {
  makeNativeCreationAuthority,
  NativeCreationAuthority,
  NativeCreationBindingResolver,
  NativeCreationGrantResolver,
  type NativeCreationGrant,
} from "./NativeCreationAuthority.ts";
import {
  NativePreparationBinding,
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  nativeCreationV2CommandDigest,
  nativePreparationCommand,
  validateNativeCreationPreparation,
} from "./NativeCreationPreparation.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as EventSink from "./EventSink.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as LegacyV1ThreadImporter from "./legacy/LegacyV1ThreadImporter.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";
import type { ProviderAdapterV2Shape } from "./ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./testkit/ProviderReplayHarness.ts";

const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "gpt-5.1-codex" };
const makeAdapter = (getCapabilities: ProviderAdapterV2Shape["getCapabilities"]) => ({
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities,
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("Guard acceptance must not open a provider process"),
}) as ProviderAdapterV2Shape;
const makeResidentStopAdapter = (opened: () => void, closed: () => void, interrupted: () => void): ProviderAdapterV2Shape => ({
  ...makeAdapter(() => Effect.succeed(CodexProviderCapabilitiesV2)),
  openSession: (input) => Effect.gen(function* () {
    opened();
    yield* Effect.addFinalizer(() => Effect.sync(closed));
    const now = yield* DateTime.now;
    return {
      runtimeGeneration: input.nativeOperation?.runtimeGeneration,
      instanceId,
      driver: ProviderDriverKind.make("codex"),
      providerSessionId: input.providerSessionId,
      providerSession: {
        id: input.providerSessionId, driver: ProviderDriverKind.make("codex"), providerInstanceId: instanceId,
        status: "ready" as const, cwd: input.runtimePolicy.cwd ?? process.cwd(), model: modelSelection.model,
        capabilities: CodexProviderCapabilitiesV2, createdAt: now, updatedAt: now, lastError: null,
      },
      events: Stream.never,
      ensureThread: () => Effect.die("STOP acceptance must not ensure a native thread"),
      resumeThread: () => Effect.die("STOP acceptance must not resume a native thread"),
      startTurn: () => Effect.die("STOP acceptance must not start a native turn"),
      steerTurn: () => Effect.die("STOP acceptance must not steer a native turn"),
      interruptTurn: () => Effect.sync(interrupted),
      respondToRuntimeRequest: () => Effect.die("STOP acceptance must not respond to native requests"),
      readThreadSnapshot: () => Effect.die("STOP acceptance must not read a native snapshot"),
      rollbackThread: () => Effect.die("STOP acceptance must not roll back a native thread"),
      forkThread: () => Effect.die("STOP acceptance must not fork a native thread"),
    };
  }),
});
const makeTestLayer = (adapter = makeAdapter(() => Effect.succeed(CodexProviderCapabilitiesV2))) => {
  const repository = nativeRepositoryLayer.pipe(Layer.provide(SqlitePersistenceMemory));
  const nativeAuthority = Layer.mock(NativeCreationAuthority)({
    authorize: () => Effect.die("Ordinary guards must not invoke native creation authority"),
    isAutomationEnrolled: () => Effect.die("Ordinary guards must not inspect native enrollment"),
    issueExecution: () => Effect.die("The disabled guard worker must not issue native execution"),
    authorizeExecution: () => Effect.die("Ordinary guards must not authorize native execution"),
  });
  return Layer.mergeAll(
    SqlitePersistenceMemory,
    repository,
    IdAllocator.layer,
    ProjectStore.layer.pipe(Layer.provide(SqlitePersistenceMemory)),
    makeOrchestratorV2ReplayLayerWithRegistry(
      { name: "guarded-dispatch" },
      ProviderAdapterRegistry.makeLayer([adapter]),
      { databaseLayer: SqlitePersistenceMemory, runEffectWorker: false },
    ).pipe(Layer.provide(Layer.mergeAll(SqlitePersistenceMemory, repository, nativeAuthority))),
  );
};

const fixture = Effect.fn("Orchestrator.guardedDispatch.fixture")(function* (name: string) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const sessions = yield* makeAuthSessions;
  const now = yield* DateTime.now;
  const threadId = ThreadId.make(`thread:guard:${name}`);
  const sessionId = AuthSessionId.make(`session:guard:${name}`);
  const projectId = ProjectId.make(`project:guard:${name}`);
  const principal: EnvironmentAuthenticatedPrincipal["Service"] = {
    sessionId,
    subject: `user:${name}`,
    method: "browser-session-cookie",
    scopes: new Set(["orchestration:read", "orchestration:operate"]),
  };
  yield* sessions.create({
    sessionId,
    subject: principal.subject,
    method: principal.method,
    scopes: [...principal.scopes],
    issuedAt: now,
    expiresAt: DateTime.add(now, { days: 1 }),
    client: { label: null, ipAddress: null, userAgent: null, deviceType: "unknown", os: null, browser: null },
  });
  const projects = yield* ProjectStore.ProjectStoreV2;
  const timestamp = DateTime.formatIso(now);
  yield* projects.apply({
    sequence: 0, eventId: EventId.make(`project-created:guard:${name}`), aggregateKind: "project", aggregateId: projectId,
    occurredAt: timestamp, commandId: null, causationEventId: null, correlationId: null, metadata: {}, type: "project.created",
    payload: { projectId, title: "Guard fixture", workspaceRoot: `/synthetic/guard/${name}`,
      defaultModelSelection: modelSelection, scripts: [], createdAt: timestamp, updatedAt: timestamp },
  });
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make(`create:guard:${name}`),
    threadId,
    projectId,
    title: "Guard fixture",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "user",
    creationSource: "web",
  });
  const command = {
    type: "message.dispatch" as const,
    commandId: CommandId.make(`dispatch:guard:${name}`),
    threadId,
    messageId: MessageId.make(`message:guard:${name}`),
    text: "Run once",
    attachments: [],
    dispatchMode: { type: "start_immediately" as const },
    createdBy: "user" as const,
    creationSource: "web" as const,
  };
  const observation = yield* orchestrator.observeCommand(command);
  const target = observation.target;
  if (target === null || target.incarnation === null) return yield* Effect.die("Missing native birth target");
  assert.isTrue(target.complete);
  assert.isTrue(target.idle);
  const guard: ThreadTurnDispatchGuardV2 = {
    version: 2,
    observedSnapshotSequence: target.snapshotSequence,
    expectedIncarnation: target.incarnation,
    expectedModelSelection: target.modelSelection,
    expectedActiveRunId: target.activeRunId,
    expectedLatestRunId: target.latestRunId,
    expectedActiveRunAttemptId: target.activeRunAttemptId,
    expectedActiveProviderThreadId: target.activeProviderThreadId,
    expectedProviderSessionId: target.providerSessionId,
    expectedProviderSessionStatus: target.providerSessionStatus,
    ...(target.runtimeGeneration === undefined ? {} : { expectedRuntimeGeneration: target.runtimeGeneration }),
    requireIdle: true,
  };
  const dispatch = (value = command) => orchestrator.dispatchGuarded(value, guard).pipe(
    Effect.provideService(EnvironmentAuthenticatedPrincipal, principal),
  );
  return { orchestrator, sessions, principal, command, guard, dispatch };
});

const nativeCreationStageFixture = Effect.fn("Orchestrator.nativeCreationStage.fixture")(function* (name: string) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const repository = yield* NativeCreationRepository;
  const sink = yield* EventSink.EventSinkV2;
  const sessions = yield* makeAuthSessions;
  const sql = yield* SqlClient.SqlClient;
  const now = yield* DateTime.now;
  const timestamp = DateTime.formatIso(now);
  const sessionId = AuthSessionId.make(`session:native-recovery:${name}`);
  const principal: EnvironmentAuthenticatedPrincipal["Service"] = {
    sessionId, subject: `native-recovery:${name}`, method: "bearer-access-token",
    scopes: new Set(["orchestration:read", "orchestration:operate"]),
  };
  yield* sessions.create({
    sessionId, subject: principal.subject, method: principal.method, scopes: [...principal.scopes],
    issuedAt: now, expiresAt: DateTime.add(now, { days: 1 }),
    client: { label: null, ipAddress: null, userAgent: null, deviceType: "bot", os: null, browser: null },
  });
  yield* sql`INSERT INTO native_creation_automation_enrollments (session_id, enrolled_at) VALUES (${sessionId}, ${timestamp})`;
  const binding = Schema.decodeUnknownSync(NativePreparationBinding)({
    backend_instance: "synthetic-backend", environment_id: "synthetic-environment",
    project_id: `project:native-recovery:${name}`, project_cwd: `/synthetic/recovery/${name}`, account_ref: "synthetic-account",
    runtime_mode: "full-access", interaction_mode: "default", base_branch: "main",
    start_from_origin: false, run_setup_script: false, provider_model_selection: modelSelection,
  });
  const original = nativePreparationCommand(`native-recovery-${name}`, binding, "Run once", "Recovery fixture", timestamp);
  const preparation = yield* validateNativeCreationPreparation(new TextEncoder().encode(nativeCreationCanonicalJson({
    schema: "voice.t3-bootstrap-preparation/v1", operation_id: `native-recovery-${name}`, binding, command: original,
    preparation_id: original.commandId.replace("voice-command-", "voice-bootstrap-"),
    binding_digest: nativeCreationSha256(nativeCreationCanonicalJson(binding)),
    prompt_digest: nativeCreationSha256(original.message.text), command_digest: nativeCreationSha256(nativeCreationCanonicalJson(original)),
  })));
  const historical = Schema.decodeUnknownSync(NativeCreationHistoricalBinding)({
    backendInstance: binding.backend_instance, environmentId: binding.environment_id, projectId: binding.project_id,
    projectCwd: binding.project_cwd, accountRef: binding.account_ref, accountBindingId: "synthetic-qualified-account",
    accountBindingRevision: 1, providerModelSelection: binding.provider_model_selection,
    runtimeMode: binding.runtime_mode, interactionMode: binding.interaction_mode, baseBranch: binding.base_branch,
    startFromOrigin: false, runSetupScript: false, requestedBranch: original.bootstrap.prepareWorktree.branch,
  });
  const resources = { projectCwd: binding.project_cwd, branch: historical.requestedBranch, worktreePath: `${binding.project_cwd}/worktree` };
  const guard = { schema: "t3.native-creation-guard/v1" as const, grantId: `synthetic-recovery-grant:${name}`, grantRevision: 1 };
  let grant: NativeCreationGrant = {
    grantId: guard.grantId, revision: 1, actorSessionId: sessionId, issuerId: "synthetic-issuer",
    expiresAt: DateTime.add(now, { days: 1 }), revoked: false, operationId: preparation.operationId,
    preparationId: preparation.preparationId, preparationSha256: preparation.preparationSha256,
    bindingDigest: preparation.bindingDigest, binding: historical, resources,
    allowedStages: ["claim", "native_command"], recoveryScopes: [],
  };
  const authority = yield* makeNativeCreationAuthority.pipe(
    Effect.provideService(AuthSessionRepository, sessions),
    Effect.provideService(NativeCreationGrantResolver, {
      resolveCurrent: () => Effect.sync(() => ({ enrolledSessionId: sessionId, trustedIssuerId: grant.issuerId, grant })),
    }),
    Effect.provideService(NativeCreationBindingResolver, { resolveCurrent: () => Effect.succeed(historical) }),
  );
  const projects = yield* ProjectStore.ProjectStoreV2;
  yield* projects.apply({
    sequence: 0, eventId: EventId.make(`project-created:native-recovery:${name}`), aggregateKind: "project", aggregateId: historical.projectId,
    occurredAt: timestamp, commandId: null, causationEventId: null, correlationId: null, metadata: {}, type: "project.created",
    payload: { projectId: historical.projectId, title: "Recovery fixture", workspaceRoot: binding.project_cwd,
      defaultModelSelection: modelSelection, scripts: [], createdAt: timestamp, updatedAt: timestamp },
  });
  const claimId = `synthetic-recovery-claim:${name}`;
  const input = { actorSessionId: sessionId, preparation, guard, resources };
  yield* repository.claim({ ...input, claimId, claimedBootId: "synthetic-boot", claimedAt: timestamp,
    grantId: guard.grantId, grantRevision: guard.grantRevision }, authority.authorize({ ...input, stage: "claim" }));
  const create = yield* Schema.decodeUnknownEffect(OrchestrationV2Command)({
    type: "thread.create", commandId: `${original.commandId}:native:v2:create`, threadId: original.threadId,
    projectId: binding.project_id, title: original.bootstrap.createThread.title, modelSelection,
    runtimeMode: binding.runtime_mode, interactionMode: binding.interaction_mode, branch: resources.branch,
    worktreePath: resources.worktreePath, createdBy: "user", creationSource: "server",
  });
  if (create.type !== "thread.create") return yield* Effect.die("Expected native recovery create fixture");
  const ids = yield* IdAllocator.IdAllocatorV2;
  const message = yield* Schema.decodeUnknownEffect(OrchestrationV2Command)({
    type: "message.dispatch", commandId: `${original.commandId}:native:v2:message`, threadId: original.threadId,
    messageId: original.message.messageId, text: original.message.text, attachments: [], modelSelection,
    dispatchMode: { type: "defer_start" }, createdBy: "user", creationSource: "server",
  });
  const release = yield* Schema.decodeUnknownEffect(OrchestrationV2Command)({
    type: "prepared-run.release", commandId: original.commandId, threadId: original.threadId,
    runId: ids.derive.run({ threadId: create.threadId, ordinal: 1 }),
  });
  if (message.type !== "message.dispatch" || release.type !== "prepared-run.release") return yield* Effect.die("Expected exact native message and release fixture stages");
  yield* repository.reserveCommandIdentities(claimId, [create.commandId, message.commandId, release.commandId]);
  for (const stage of [create, message, release]) yield* repository.reserveCommand(claimId, stage);
  yield* repository.recordNormalizedCommand(claimId, release);
  const enableRecovery = (recoveryScopeId: string, resource: NativeCreationGrant["recoveryScopes"][number]["resource"]) => {
    grant = { ...grant, allowedStages: ["claim", "native_command", "cleanup"], recoveryScopes: [{ scopeId: recoveryScopeId, resource }] };
  };
  return { orchestrator, repository, sink, sql, claimId, create, message, release, input, authority, principal, timestamp, enableRecovery };
});

const nativeThreadRecoveryFixture = Effect.fn("Orchestrator.nativeThreadRecovery.fixture")(function* (name: string) {
  const fixture = yield* nativeCreationStageFixture(name);
  const { orchestrator, repository, sink, sql, claimId, create, input, authority, principal, timestamp } = fixture;
  yield* orchestrator.dispatchNativeCreationStage(create, { ...input, claimId, stage: "native_command" }).pipe(
    Effect.provideService(EnvironmentAuthenticatedPrincipal, principal), Effect.provideService(NativeCreationAuthority, authority),
  );
  const incarnation = yield* sink.getThreadIncarnation(create.threadId);
  if (incarnation === null) return yield* Effect.die("Expected accepted V2 native create birth");
  const resource = { kind: "thread" as const, threadId: create.threadId, incarnation };
  const recoveryScopeId = `synthetic-thread-recovery:${name}`;
  fixture.enableRecovery(recoveryScopeId, resource);
  const authorization = { ...input, stage: "cleanup" as const, recoveryScopeId, recoveryResource: resource };
  const cleanupStartEffectId = `native-recovery-cleanup:${name}`;
  const commandStartEffectId = `native-recovery-command:${name}`;
  const cleanup = yield* repository.startEffect(claimId, {
    kind: "cleanup", phase: "started", effectId: cleanupStartEffectId, timestamp, recoveryScopeId, resource,
  }, authority.authorize(authorization));
  const command = { type: "thread.delete" as const, commandId: CommandId.make(`${input.preparation.command.commandId}:bootstrap-thread-delete`), threadId: create.threadId };
  const commandDigest = nativeCreationV2CommandDigest(command);
  yield* repository.reserveThreadRecoveryCommand({
    version: 2, claimId, commandId: command.commandId, threadId: command.threadId, commandType: command.type,
    canonicalCommand: command, commandDigest, commandStartEffectId, cleanupStartEffectId,
    cleanupStartOrdinal: cleanup.ordinal, recoveryScopeId, resource,
  });
  yield* repository.startEffect(claimId, {
    kind: "native_command", phase: "started", effectId: commandStartEffectId, timestamp,
    commandId: command.commandId, threadId: command.threadId, commandType: command.type, commandDigest,
  }, authority.authorize({ ...input, stage: "native_command" }));
  const context = yield* authority.issueThreadRecovery({ claimId, commandStartEffectId, cleanupStartEffectId, authorization });
  return { orchestrator, repository, sink, sql, claimId, command, context, create, commandStartEffectId, cleanupStartEffectId };
});

it.effect("rejects freshly reserved native creation before ordinary preparation or command acceptance", () =>
  Effect.gen(function* () {
    for (const type of ["thread.create", "message.dispatch", "prepared-run.release"] as const) {
      const { orchestrator, repository, sink, sql, claimId, create, message, release, input, authority, principal } =
        yield* nativeCreationStageFixture(`reserved-${type}`);
      const nativeDispatch = (command: Parameters<Orchestrator.OrchestratorV2["Service"]["dispatchNativeCreationStage"]>[0]) => orchestrator.dispatchNativeCreationStage(command, {
        ...input, claimId, stage: "native_command",
      }).pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal), Effect.provideService(NativeCreationAuthority, authority));
      if (type !== "thread.create") yield* nativeDispatch(create);
      if (type === "prepared-run.release") {
        yield* nativeDispatch(message);
        const projection = yield* orchestrator.getThreadProjection(create.threadId);
        const workspace = projection.turnItems.find((item) => item.type === "command_execution" && item.input === "Preparing workspace");
        if (workspace?.type !== "command_execution") return yield* Effect.die("Missing native preparing workspace fixture");
        const ids = yield* IdAllocator.IdAllocatorV2;
        yield* sink.write({ events: [{ id: yield* ids.allocate.event({ threadId: create.threadId }), type: "turn-item.updated",
          threadId: create.threadId, occurredAt: yield* DateTime.now, payload: { ...workspace, status: "completed", exitCode: 0 } }] });
      }
      const command = type === "thread.create" ? create : type === "message.dispatch" ? message : release;
      assert.isTrue(Option.isSome(yield* repository.getReservedCommandIdentity(command.commandId)));
      const history = yield* repository.readHistoryByClaim(claimId);
      const events = yield* sql`SELECT event_id, sequence FROM orchestration_events WHERE stream_id = ${create.threadId} ORDER BY sequence`;
      const threads = yield* sql`SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id = ${create.threadId}`;
      const effects = yield* sql`SELECT effect_id, status, payload_json FROM orchestration_v2_effect_outbox WHERE thread_id = ${create.threadId} ORDER BY effect_id`;
      let preparations = 0;
      const outcome = yield* Effect.exit(orchestrator.dispatch(command, Effect.sync(() => { preparations += 1; })));
      assert.isTrue(Exit.isFailure(outcome), `Ordinary dispatch accepted fresh reserved ${type}`);
      assert.equal(preparations, 0);
      assert.equal((yield* sink.readCommandReceiptIdentity(command.commandId)).receipt, null);
      assert.deepEqual(yield* sql`SELECT event_id, sequence FROM orchestration_events WHERE stream_id = ${create.threadId} ORDER BY sequence`, events);
      assert.deepEqual(yield* sql`SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id = ${create.threadId}`, threads);
      assert.deepEqual(yield* sql`SELECT effect_id, status, payload_json FROM orchestration_v2_effect_outbox WHERE thread_id = ${create.threadId} ORDER BY effect_id`, effects);
      assert.deepEqual(yield* repository.readHistoryByClaim(claimId), history);
      const ordinary = { ...create, commandId: CommandId.make(`ordinary-unreserved:${type}`),
        threadId: ThreadId.make(`ordinary-unreserved-thread:${type}`) };
      yield* orchestrator.dispatch(ordinary, Effect.sync(() => { preparations += 1; }));
      assert.equal(preparations, 1);
      assert.equal((yield* sink.readCommandReceiptIdentity(ordinary.commandId)).receipt?.status, "accepted");
    }
  }).pipe(Effect.provide(makeTestLayer())),
);

it.effect("ordinary dispatch atomically captures checkout ownership and its real start effect", () =>
  Effect.gen(function* () {
    const { orchestrator, command } = yield* fixture("ordinary-admission");
    const sink = yield* EventSink.EventSinkV2;
    const sql = yield* SqlClient.SqlClient;
    const accepted = yield* orchestrator.dispatch(command);
    const admission = yield* sink.readOrdinaryCheckoutAdmission({ commandId: command.commandId, threadId: command.threadId });
    assert.isNotNull(admission);
    if (admission === null) return yield* Effect.die("Missing ordinary checkout admission");
    assert.equal(admission.capture.threadId, command.threadId);
    assert.equal(admission.capture.canonicalCheckoutPath, "/synthetic/guard/ordinary-admission");
    assert.equal(admission.receipt.status, "accepted");
    const effects = yield* sql<{ readonly effect_id: string }>`SELECT effect_id FROM orchestration_v2_effect_outbox
      WHERE command_id = ${command.commandId} AND effect_type = 'provider-turn.start'`;
    assert.lengthOf(effects, 1);
    const link = yield* sink.readOrdinaryCheckoutEffectLink(effects[0]!.effect_id);
    assert.equal(link?.admission.admissionId, admission.admissionId);
    assert.deepEqual(yield* orchestrator.dispatch(command), accepted);
    assert.deepEqual(yield* sink.readOrdinaryCheckoutAdmission({ commandId: command.commandId, threadId: command.threadId }), admission);
    assert.lengthOf(yield* orchestrator.listWorktreeOwnershipLeases, 1);
  }).pipe(Effect.provide(makeTestLayer())),
);

it.effect("rejects an ordinary message on another thread's checkout before preparation or durable acceptance", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sink = yield* EventSink.EventSinkV2;
    const projects = yield* ProjectStore.ProjectStoreV2;
    const sql = yield* SqlClient.SqlClient;
    const projectId = ProjectId.make("project:ordinary-checkout-conflict");
    const checkout = "/synthetic/ordinary-checkout-conflict";
    const now = DateTime.formatIso(yield* DateTime.now);
    yield* projects.apply({ sequence: 0, eventId: EventId.make("project-created:ordinary-checkout-conflict"),
      aggregateKind: "project", aggregateId: projectId, occurredAt: now, commandId: null,
      causationEventId: null, correlationId: null, metadata: {}, type: "project.created",
      payload: { projectId, title: "Checkout conflict", workspaceRoot: checkout,
        defaultModelSelection: modelSelection, scripts: [], createdAt: now, updatedAt: now } });
    const owner = ThreadId.make("thread:ordinary-checkout-owner");
    const contender = ThreadId.make("thread:ordinary-checkout-contender");
    for (const threadId of [owner, contender]) {
      yield* orchestrator.dispatch({ type: "thread.create", commandId: CommandId.make(`create:${threadId}`),
        threadId, projectId, title: "Shared checkout", modelSelection, runtimeMode: "full-access",
        interactionMode: "default", branch: "main", worktreePath: null, createdBy: "user", creationSource: "web" });
    }
    const lease = yield* orchestrator.acquireOrdinaryWorktreeOwnership(owner);
    const command = { type: "message.dispatch" as const, commandId: CommandId.make("dispatch:ordinary-checkout-contender"),
      threadId: contender, messageId: MessageId.make("message:ordinary-checkout-contender"), text: "Must not enter this checkout",
      attachments: [], modelSelection, dispatchMode: { type: "start_immediately" as const },
      createdBy: "user" as const, creationSource: "web" as const };
    const projection = yield* orchestrator.getThreadProjection(contender);
    const events = yield* sql`SELECT event_id, sequence FROM orchestration_events ORDER BY sequence`;
    const effects = yield* sql`SELECT effect_id, status, payload_json FROM orchestration_v2_effect_outbox ORDER BY effect_id`;
    let preparations = 0;
    const result = yield* Effect.exit(orchestrator.dispatch(command, Effect.sync(() => { preparations += 1; })));
    assert.isTrue(Exit.isFailure(result), "Ordinary message was accepted on another thread's owned checkout");
    if (Exit.isFailure(result)) assert.instanceOf(Cause.squash(result.cause), WorktreeOwnershipConflictError);
    assert.equal(preparations, 0);
    assert.equal((yield* sink.readCommandReceiptIdentity(command.commandId)).receipt, null);
    assert.deepEqual(yield* orchestrator.getThreadProjection(contender), projection);
    assert.deepEqual(yield* sql`SELECT event_id, sequence FROM orchestration_events ORDER BY sequence`, events);
    assert.deepEqual(yield* sql`SELECT effect_id, status, payload_json FROM orchestration_v2_effect_outbox ORDER BY effect_id`, effects);
    assert.deepEqual(yield* orchestrator.listWorktreeOwnershipLeases, [lease]);
  }).pipe(Effect.provide(makeTestLayer())),
);

it.effect("preserves deletion consent on replay and observes the same command without cleanup effects", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const sink = yield* EventSink.EventSinkV2;
    const projects = yield* ProjectStore.ProjectStoreV2;
    const sql = yield* SqlClient.SqlClient;
    const now = DateTime.formatIso(yield* DateTime.now);
    const projectId = ProjectId.make("project:deletion-observation");
    const projectRoot = "/synthetic/deletion-observation";
    yield* projects.apply({ sequence: 0, eventId: EventId.make("project-created:deletion-observation"),
      aggregateKind: "project", aggregateId: projectId, occurredAt: now, commandId: null,
      causationEventId: null, correlationId: null, metadata: {}, type: "project.created",
      payload: { projectId, title: "Deletion observation", workspaceRoot: projectRoot,
        defaultModelSelection: modelSelection, scripts: [], createdAt: now, updatedAt: now } });
    const threadId = ThreadId.make("thread:deletion-observation");
    const noConsentThreadId = ThreadId.make("thread:deletion-without-consent");
    for (const id of [threadId, noConsentThreadId]) {
      yield* orchestrator.dispatch({ type: "thread.create", commandId: CommandId.make(`create:${id}`),
        threadId: id, projectId, title: "Deletion fixture", modelSelection, runtimeMode: "full-access",
        interactionMode: "default", branch: "feature/deletion", worktreePath: `${projectRoot}/${id === threadId ? "worktree" : "other-worktree"}`,
        createdBy: "user", creationSource: "web" });
    }
    const lease = yield* orchestrator.acquireWorktreeOwnership(threadId);
    const command = { type: "thread.delete" as const, commandId: CommandId.make("delete:with-worktree-consent"), threadId,
      worktreeRemoval: { projectId, path: lease.resourcePath, branch: lease.branch, force: true as const } };
    const accepted = yield* threads.dispatch(command);
    const original = yield* sink.readThreadDeletionCommand(command.commandId);
    assert.deepEqual(original?.command, command);
    assert.isNotNull((yield* orchestrator.getThreadProjection(threadId)).thread.deletedAt);
    const observationInput = { threadId, commandId: command.commandId };
    const observed = yield* threads.observeThreadDeletionCleanup(observationInput);
    assert.equal(observed.state, "pending");
    assert.equal(observed.receipt?.status, "accepted");
    assert.equal(observed.receipt?.resultSequence, accepted.sequence);
    assert.equal(observed.deletion?.eventId, original?.deletion.eventId);
    assert.equal(observed.deletion?.sequence, original?.deletion.sequence);
    assert.equal(observed.deletion?.resultSequence, accepted.sequence);
    assert.deepEqual(observed.worktree, { projectId, path: lease.resourcePath, branch: lease.branch });
    assert.equal(observed.currentLease, "original");
    assert.equal(observed.removalOutcome, null);
    const before = yield* sql`SELECT event_id, sequence FROM orchestration_events ORDER BY sequence`;
    const effects = yield* sql`SELECT effect_id, status, payload_json FROM orchestration_v2_effect_outbox ORDER BY effect_id`;
    const leases = yield* orchestrator.listWorktreeOwnershipLeases;
    assert.isTrue(effects.some((effect) => effect.status === "pending" && JSON.parse(String(effect.payload_json)).type === "worktree.cleanup"));
    assert.deepEqual(yield* threads.dispatch(command), accepted);
    assert.deepEqual(yield* threads.observeThreadDeletionCleanup(observationInput), observed);
    const stripped = { type: "thread.delete" as const, commandId: command.commandId, threadId };
    let preparations = 0;
    for (const replay of [stripped, { ...command, worktreeRemoval: { ...command.worktreeRemoval, path: `${projectRoot}/changed` } }]) {
      const rejected = yield* Effect.flip(orchestrator.dispatch(replay, Effect.sync(() => { preparations += 1; })));
      assert.equal(rejected._tag === "DispatchGuardRejectedError" ? rejected.reason : undefined, "identity_conflict");
    }
    assert.equal(preparations, 0);
    assert.deepEqual(yield* threads.observeThreadDeletionCleanup({ threadId: noConsentThreadId, commandId: command.commandId }), {
      threadId: noConsentThreadId, commandId: command.commandId, receipt: null, deletion: null, worktree: null,
      state: "not_found", removalOutcome: null, currentLease: "unavailable", reason: null,
    });
    assert.deepEqual(yield* sql`SELECT event_id, sequence FROM orchestration_events ORDER BY sequence`, before);
    assert.deepEqual(yield* sql`SELECT effect_id, status, payload_json FROM orchestration_v2_effect_outbox ORDER BY effect_id`, effects);
    assert.deepEqual(yield* orchestrator.listWorktreeOwnershipLeases, leases);
    const ordinary = { type: "thread.delete" as const, commandId: CommandId.make("delete:without-worktree-consent"), threadId: noConsentThreadId };
    yield* threads.dispatch(ordinary);
    const ordinaryOriginal = yield* sink.readThreadDeletionCommand(ordinary.commandId);
    assert.deepEqual(ordinaryOriginal?.command, ordinary);
    const ordinaryObservationInput = { threadId: noConsentThreadId, commandId: ordinary.commandId };
    assert.equal((yield* threads.observeThreadDeletionCleanup(ordinaryObservationInput)).state, "not_requested");
    assert.isFalse((yield* sql`SELECT payload_json FROM orchestration_v2_effect_outbox WHERE command_id = ${ordinary.commandId}`)
      .some((effect) => JSON.parse(String(effect.payload_json)).type === "worktree.cleanup"));
    const immutable = yield* Effect.flip(sql`DELETE FROM orchestration_v2_thread_deletion_commands WHERE command_id = ${ordinary.commandId}`);
    assert.include(inspect(immutable, { depth: 10 }), "thread deletion commands are permanent");
    assert.deepEqual(yield* sink.readThreadDeletionCommand(ordinary.commandId), ordinaryOriginal);
    const historicalCommand = { ...ordinary, commandId: CommandId.make("delete:historical-without-original-capture") };
    yield* sql`INSERT INTO orchestration_command_receipts
      (command_id, command_type, aggregate_kind, aggregate_id, accepted_at, result_sequence, status, error)
      SELECT ${historicalCommand.commandId}, command_type, aggregate_kind, aggregate_id, accepted_at, result_sequence, status, error
      FROM orchestration_command_receipts WHERE command_id = ${ordinary.commandId}`;
    assert.equal(yield* sink.readThreadDeletionCommand(historicalCommand.commandId), null);
    const historical = yield* threads.observeThreadDeletionCleanup({ threadId: noConsentThreadId, commandId: historicalCommand.commandId });
    assert.equal(historical.state, "unknown");
    assert.equal(historical.reason, "original_worktree_consent_unavailable");
    assert.equal(historical.receipt?.status, "accepted");
    const historicalEvents = yield* sql`SELECT event_id, sequence FROM orchestration_events ORDER BY sequence`;
    const historicalEffects = yield* sql`SELECT effect_id, status, payload_json FROM orchestration_v2_effect_outbox ORDER BY effect_id`;
    const unbound = yield* Effect.flip(orchestrator.dispatch(historicalCommand, Effect.sync(() => { preparations += 1; })));
    assert.equal(unbound._tag === "DispatchGuardRejectedError" ? unbound.reason : undefined, "unbound_receipt");
    assert.equal(preparations, 0);
    assert.deepEqual(yield* sql`SELECT event_id, sequence FROM orchestration_events ORDER BY sequence`, historicalEvents);
    assert.deepEqual(yield* sql`SELECT effect_id, status, payload_json FROM orchestration_v2_effect_outbox ORDER BY effect_id`, historicalEffects);
    assert.deepEqual(yield* sink.readThreadDeletionCommand(ordinary.commandId), ordinaryOriginal);
  }).pipe(Effect.provide(makeTestLayer())),
);

it.effect("captures project deletion policy without changing consent and replays the original policy after settings change", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const sink = yield* EventSink.EventSinkV2;
    const projects = yield* ProjectStore.ProjectStoreV2;
    const settings = yield* ServerSettingsService;
    const sql = yield* SqlClient.SqlClient;
    const now = DateTime.formatIso(yield* DateTime.now);
    const policyProjectId = ProjectId.make("project:deletion-policy");
    const offProjectId = ProjectId.make("project:deletion-policy-off");
    const rules = { worktreeAfterDays: 7, worktreeOnMerge: true, worktreeOnDelete: true, worktreeUnchanged: true };
    yield* settings.updateSettings({ storageCleanup: { worktreeOnDelete: false }, projectSettingsOverrides: {
      [policyProjectId]: { worktreeCleanup: { mode: "custom", rules } },
      [offProjectId]: { worktreeCleanup: { mode: "off" } },
    } });
    for (const projectId of [policyProjectId, offProjectId]) {
      yield* projects.apply({ sequence: 0, eventId: EventId.make(`project-created:${projectId}`),
        aggregateKind: "project", aggregateId: projectId, occurredAt: now, commandId: null,
        causationEventId: null, correlationId: null, metadata: {}, type: "project.created",
        payload: { projectId, title: "Deletion policy", workspaceRoot: `/synthetic/${projectId}`,
          defaultModelSelection: modelSelection, scripts: [], createdAt: now, updatedAt: now } });
    }
    const policyThreads = [ThreadId.make("thread:deletion-policy-leased"), ThreadId.make("thread:deletion-policy-unleased")];
    const offThreadId = ThreadId.make("thread:deletion-policy-off");
    const explicitThreadId = ThreadId.make("thread:deletion-policy-explicit");
    for (const threadId of [...policyThreads, offThreadId, explicitThreadId]) {
      const projectId = policyThreads.includes(threadId) ? policyProjectId : offProjectId;
      yield* orchestrator.dispatch({ type: "thread.create", commandId: CommandId.make(`create:${threadId}`),
        threadId, projectId, title: "Policy fixture", modelSelection, runtimeMode: "full-access",
        interactionMode: "default", branch: "feature/policy", worktreePath: `/synthetic/${projectId}/${threadId}`,
        createdBy: "user", creationSource: "web" });
    }
    const lease = yield* orchestrator.acquireWorktreeOwnership(policyThreads[0]!);
    for (const threadId of policyThreads) {
      const command = { type: "thread.delete" as const, commandId: CommandId.make(`delete:${threadId}`), threadId };
      const accepted = yield* threads.dispatch(command);
      const original = yield* sink.readThreadDeletionCommand(command.commandId);
      const task = yield* sink.readDeletionWorktreeTask(EventSink.deletionWorktreeEffectIdV1(command.commandId, threadId));
      assert.deepEqual(original?.command, command);
      assert.equal(original?.commandDigest, nativeCreationV2CommandDigest(command));
      assert.deepEqual(original?.inventory.request, { origin: "policy", projectId: policyProjectId,
        path: `/synthetic/${policyProjectId}/${threadId}`, branch: "feature/policy", force: false, rules });
      assert.deepEqual(task?.task.request, original?.inventory.request);
      assert.equal(task?.task.consent, undefined);
      assert.equal(task?.task.captureStatus, "captured");
      assert.equal(task?.leaseInventory.status, threadId === policyThreads[0] ? "original" : "absent");
      if (task?.leaseInventory.status === "original") assert.deepEqual(task.leaseInventory.lease, lease);
      const observed = yield* threads.observeThreadDeletionCleanup({ threadId, commandId: command.commandId });
      assert.equal(observed.state, "pending");
      assert.equal(observed.removalOutcome, null);
      const before = yield* sql`SELECT event_id, sequence FROM orchestration_events ORDER BY sequence`;
      const effects = yield* sql`SELECT effect_id, status, payload_json FROM orchestration_v2_effect_outbox ORDER BY effect_id`;
      yield* settings.updateSettings({ projectSettingsOverrides: {
        [policyProjectId]: { worktreeCleanup: { mode: "custom", rules: { ...rules, worktreeAfterDays: 14, worktreeOnDelete: false } } },
      } });
      assert.deepEqual(yield* threads.dispatch(command), accepted);
      assert.deepEqual(yield* sink.readThreadDeletionCommand(command.commandId), original);
      assert.deepEqual(yield* sink.readDeletionWorktreeTask(EventSink.deletionWorktreeEffectIdV1(command.commandId, threadId)), task);
      assert.deepEqual(yield* threads.observeThreadDeletionCleanup({ threadId, commandId: command.commandId }), observed);
      assert.deepEqual(yield* sql`SELECT event_id, sequence FROM orchestration_events ORDER BY sequence`, before);
      assert.deepEqual(yield* sql`SELECT effect_id, status, payload_json FROM orchestration_v2_effect_outbox ORDER BY effect_id`, effects);
      yield* settings.updateSettings({ projectSettingsOverrides: {
        [policyProjectId]: { worktreeCleanup: { mode: "custom", rules } },
      } });
    }
    yield* settings.updateSettings({ storageCleanup: { worktreeOnDelete: true }, projectSettingsOverrides: {
      [offProjectId]: { worktreeCleanup: { mode: "off" } },
    } });
    const off = { type: "thread.delete" as const, commandId: CommandId.make("delete:policy-off"), threadId: offThreadId };
    yield* threads.dispatch(off);
    assert.equal((yield* sink.readThreadDeletionCommand(off.commandId))?.inventory.request, undefined);
    assert.equal((yield* threads.observeThreadDeletionCleanup({ threadId: offThreadId, commandId: off.commandId })).state, "not_requested");
    const explicit = { type: "thread.delete" as const, commandId: CommandId.make("delete:policy-explicit"), threadId: explicitThreadId,
      worktreeRemoval: { projectId: offProjectId, path: `/synthetic/${offProjectId}/${explicitThreadId}`, branch: "feature/policy", force: true as const } };
    yield* threads.dispatch(explicit);
    assert.deepEqual((yield* sink.readThreadDeletionCommand(explicit.commandId))?.inventory.request,
      { origin: "explicit", consent: explicit.worktreeRemoval });
    assert.deepEqual((yield* sink.readDeletionWorktreeTask(EventSink.deletionWorktreeEffectIdV1(explicit.commandId, explicitThreadId)))?.task.consent,
      explicit.worktreeRemoval);
    assert.equal((yield* threads.observeThreadDeletionCleanup({ threadId: explicitThreadId, commandId: explicit.commandId })).state, "pending");
  }).pipe(Effect.provide(makeTestLayer())),
);

it.effect("accepts unchanged native thread recovery with atomic original command completion and exact replay", () =>
  Effect.gen(function* () {
    const { orchestrator, repository, sink, sql, claimId, command, context, commandStartEffectId, cleanupStartEffectId } =
      yield* nativeThreadRecoveryFixture("unchanged");
    const strippedFresh = yield* Effect.flip(orchestrator.dispatch(command));
    assert.instanceOf(strippedFresh, DispatchGuardRejectedError);
    const accepted = yield* orchestrator.dispatchNativeCreationRecovery(command, context);
    assert.isTrue(accepted.storedEvents.some((stored) => stored.event.type === "thread.deleted"));
    assert.isNotNull((yield* orchestrator.getThreadProjection(command.threadId)).thread.deletedAt);
    const receipt = (yield* sink.readCommandReceiptIdentity(command.commandId)).receipt;
    assert.equal(receipt?.status, "accepted");
    assert.equal(receipt?.commandType, "thread.delete");
    const history = yield* repository.readHistoryByClaim(claimId);
    const completions = history.effects.filter((fact) => fact.effectId === commandStartEffectId && fact.phase === "completed");
    assert.equal(completions.length, 1);
    const completion = completions[0]!;
    assert.equal(completion.kind, "native_command");
    if (completion.kind !== "native_command" || completion.phase !== "completed") return yield* Effect.die("Missing original recovery command completion");
    assert.equal(completion.sequence, receipt?.resultSequence);
    assert.equal(completion.eventId, accepted.storedEvents.at(-1)?.event.id);
    assert.equal(completion.sequence, accepted.storedEvents.at(-1)?.sequence);
    assert.isFalse(history.effects.some((fact) => fact.effectId === cleanupStartEffectId && fact.phase === "completed"));
    assert.equal(history.effectsV2.length, 0);
    const effects = yield* sql`SELECT effect_id, status, payload_json FROM orchestration_v2_effect_outbox ORDER BY effect_id`;
    assert.isTrue(effects.length > 0);
    assert.isTrue(effects.every((row) => row.status === "pending"));
    const before = yield* sql`SELECT sequence, event_id FROM orchestration_events ORDER BY sequence`;
    assert.deepEqual(yield* orchestrator.dispatchNativeCreationRecovery(command, context), accepted);
    assert.instanceOf(yield* Effect.flip(orchestrator.dispatch(command)), DispatchGuardRejectedError);
    assert.instanceOf(yield* Effect.flip(orchestrator.dispatchNativeCreationRecovery(command, structuredClone(context))), DispatchGuardRejectedError);
    assert.deepEqual(yield* sql`SELECT sequence, event_id FROM orchestration_events ORDER BY sequence`, before);
    assert.deepEqual(yield* sql`SELECT effect_id, status, payload_json FROM orchestration_v2_effect_outbox ORDER BY effect_id`, effects);
    assert.deepEqual(yield* repository.readHistoryByClaim(claimId), history);
  }).pipe(Effect.provide(makeTestLayer())),
);

it.effect("rejects native thread recovery after replacement without changing the replacement or original history", () =>
  Effect.gen(function* () {
    const { orchestrator, repository, sink, sql, claimId, command, context, create } = yield* nativeThreadRecoveryFixture("replacement");
    yield* orchestrator.dispatch({ type: "thread.delete", commandId: CommandId.make("ordinary-delete:native-recovery"), threadId: command.threadId });
    yield* orchestrator.dispatch({ ...create, commandId: CommandId.make("ordinary-recreate:native-recovery"), title: "Replacement survives" });
    const recovery = yield* repository.readThreadRecoveryCommand(command.commandId);
    const replacementBirth = yield* sink.readApplicationBirthRecord(command.threadId);
    assert.isNotNull(recovery);
    assert.isNotNull(replacementBirth);
    assert.notEqual(replacementBirth!.eventId, recovery!.resource.incarnation.eventId);
    assert.isAbove(replacementBirth!.sequence, recovery!.resource.incarnation.sequence);
    const projection = yield* orchestrator.getThreadProjection(command.threadId);
    const history = yield* repository.readHistoryByClaim(claimId);
    const before = yield* sql`SELECT sequence, event_id FROM orchestration_events ORDER BY sequence`;
    const effects = yield* sql`SELECT effect_id, status, payload_json FROM orchestration_v2_effect_outbox ORDER BY effect_id`;
    assert.isTrue(effects.some((effect) => effect.effect_id === "effect:ordinary-delete:native-recovery:terminal.cleanup" && effect.status === "pending"));
    const rejected = yield* Effect.flip(orchestrator.dispatchNativeCreationRecovery(command, context));
    assert.instanceOf(rejected, DispatchGuardRejectedError);
    assert.equal(rejected._tag === "DispatchGuardRejectedError" ? rejected.reason : undefined, "unknown_evidence");
    assert.deepEqual(yield* orchestrator.getThreadProjection(command.threadId), projection);
    assert.deepEqual(yield* repository.readHistoryByClaim(claimId), history);
    assert.equal((yield* sink.readCommandReceiptIdentity(command.commandId)).receipt, null);
    assert.deepEqual(yield* sql`SELECT sequence, event_id FROM orchestration_events ORDER BY sequence`, before);
    assert.deepEqual(yield* sql`SELECT effect_id, status, payload_json FROM orchestration_v2_effect_outbox ORDER BY effect_id`, effects);
  }).pipe(Effect.provide(makeTestLayer())),
);

it.effect("rolls back native thread recovery when original command completion cannot be persisted", () =>
  Effect.gen(function* () {
    const { orchestrator, repository, sink, sql, claimId, command, context, commandStartEffectId } =
      yield* nativeThreadRecoveryFixture("completion-rollback");
    const projection = yield* orchestrator.getThreadProjection(command.threadId);
    const history = yield* repository.readHistoryByClaim(claimId);
    const before = yield* sql`SELECT sequence, event_id FROM orchestration_events ORDER BY sequence`;
    const effects = yield* sql`SELECT effect_id, status, payload_json FROM orchestration_v2_effect_outbox ORDER BY effect_id`;
    yield* sql.unsafe(`CREATE TRIGGER recovery_completion_failure BEFORE INSERT ON native_creation_effect_facts
      WHEN NEW.effect_id = '${commandStartEffectId}' AND NEW.phase = 'completed'
      BEGIN SELECT RAISE(ABORT, 'injected recovery command completion failure'); END`);
    const outcome = yield* Effect.exit(orchestrator.dispatchNativeCreationRecovery(command, context));
    assert.isTrue(Exit.isFailure(outcome));
    if (Exit.isFailure(outcome)) {
      const cause = Cause.squash(outcome.cause);
      assert.isTrue(Schema.is(Orchestrator.OrchestratorDispatchError)(cause));
      if (!Schema.is(Orchestrator.OrchestratorDispatchError)(cause)) return yield* Effect.die("Expected recovery acceptance failure");
      assert.isTrue(Schema.is(EventSink.EventSinkWriteError)(cause.cause));
      if (!Schema.is(EventSink.EventSinkWriteError)(cause.cause)) return yield* Effect.die("Expected transactional recovery write failure");
      const persistence = cause.cause.cause;
      assert.isTrue(Schema.is(NativeCreationRepositoryError)(persistence));
      if (!Schema.is(NativeCreationRepositoryError)(persistence)) return yield* Effect.die("Expected original recovery completion write failure");
      assert.equal(persistence.code, "conflict");
      assert.equal(persistence.message, "Native creation persistence rejected the operation");
    }
    assert.deepEqual(yield* orchestrator.getThreadProjection(command.threadId), projection);
    assert.deepEqual(yield* repository.readHistoryByClaim(claimId), history);
    assert.equal((yield* sink.readCommandReceiptIdentity(command.commandId)).receipt, null);
    assert.deepEqual(yield* sql`SELECT sequence, event_id FROM orchestration_events ORDER BY sequence`, before);
    assert.deepEqual(yield* sql`SELECT effect_id, status, payload_json FROM orchestration_v2_effect_outbox ORDER BY effect_id`, effects);
    yield* sql.unsafe("DROP TRIGGER recovery_completion_failure");
    const accepted = yield* orchestrator.dispatchNativeCreationRecovery(command, context);
    assert.isTrue(accepted.storedEvents.some((stored) => stored.event.type === "thread.deleted"));
    const completions = (yield* repository.readHistoryByClaim(claimId)).effects.filter((fact) =>
      fact.effectId === commandStartEffectId && fact.phase === "completed");
    assert.equal(completions.length, 1);
    assert.equal((yield* sink.readCommandReceiptIdentity(command.commandId)).receipt?.resultSequence, accepted.sequence);
  }).pipe(Effect.provide(makeTestLayer())),
);

it.effect("replays the exact native identity before a now-busy guard and rejects guard stripping", () =>
  Effect.gen(function* () {
    const { orchestrator, command, dispatch } = yield* fixture("replay");
    const sql = yield* SqlClient.SqlClient;
    const first = yield* dispatch();
    const before = yield* sql`SELECT effect_id, status FROM orchestration_v2_effect_outbox ORDER BY effect_id`;
    assert.deepEqual(yield* dispatch(), first);
    const changed = yield* Effect.flip(dispatch({ ...command, text: "Changed effect-bearing input" }));
    assert.instanceOf(changed, DispatchGuardRejectedError);
    assert.equal(changed._tag === "DispatchGuardRejectedError" ? changed.reason : undefined, "identity_conflict");
    const stripped = yield* Effect.flip(orchestrator.dispatch(command));
    assert.instanceOf(stripped, DispatchGuardRejectedError);
    assert.equal(stripped._tag === "DispatchGuardRejectedError" ? stripped.reason : undefined, "identity_conflict");
    assert.deepEqual(yield* sql`SELECT effect_id, status FROM orchestration_v2_effect_outbox ORDER BY effect_id`, before);
    const observation = yield* orchestrator.observeCommand(command);
    assert.equal(observation.commandStatus, "accepted");
    assert.equal(observation.correlation, "exact");
  }).pipe(Effect.provide(makeTestLayer())),
);

it.effect("rejects guard stripping before transcript preparation and prepares fresh ordinary commands once", () => {
  let hydrations = 0;
  const orchestratorLayer = makeTestLayer();
  const layer = Layer.mergeAll(
    orchestratorLayer,
    ThreadManagement.layerWithLegacyImporter.pipe(Layer.provide(Layer.mergeAll(
      orchestratorLayer,
      Layer.mock(LegacyV1ThreadImporter.LegacyV1ThreadImporter)({
        ensureTranscript: () => Effect.sync(() => { hydrations += 1; }),
      }),
    ))),
  );
  return Effect.gen(function* () {
    const { command, dispatch } = yield* fixture("preparation");
    const threads = yield* ThreadManagement.ThreadManagementService;
    yield* dispatch();
    const stripped = yield* threads.dispatch(command).pipe(Effect.flip);
    assert.equal(stripped._tag === "DispatchGuardRejectedError" ? stripped.reason : undefined, "identity_conflict");
    assert.equal(hydrations, 0);
    const ordinary = {
      type: "thread.metadata.update" as const,
      commandId: CommandId.make("ordinary:guard:preparation"),
      threadId: command.threadId,
      title: "Prepared ordinary command",
    };
    const accepted = yield* threads.dispatch(ordinary);
    assert.equal(hydrations, 1);
    assert.deepEqual(yield* threads.dispatch(ordinary), accepted);
    assert.equal(hydrations, 1);
  }).pipe(Effect.provide(layer));
});

it.effect("holds an unknown imported ordinary message before server transcript preparation", () => {
  let hydrations = 0;
  return Effect.gen(function* () {
    const { orchestrator, command } = yield* fixture("unknown-import-preparation");
    const sink = yield* EventSink.EventSinkV2;
    const sql = yield* SqlClient.SqlClient;
    yield* sink.recordLegacyContinuationDisposition({
      threadId: command.threadId,
      provenance: "native_import",
      qualification: { type: "unknown", reason: "source_not_stopped" },
      evidence: null,
      importedAt: DateTime.formatIso(yield* DateTime.now),
    });
    const before = yield* sql`SELECT event_id, sequence FROM orchestration_events ORDER BY sequence`;
    const failure = yield* Effect.flip(orchestrator.dispatch(command,
      Effect.sync(() => { hydrations += 1; }),
    ));
    assert.equal(failure._tag, "OrchestratorImportedContinuationHeldError");
    assert.equal(failure._tag === "OrchestratorImportedContinuationHeldError" ? failure.reason : undefined, "continuation_unknown");
    assert.equal(hydrations, 0);
    assert.deepEqual(yield* sql`SELECT event_id, sequence FROM orchestration_events ORDER BY sequence`, before);
    for (const table of ["orchestration_v2_projection_runs", "orchestration_v2_projection_provider_threads", "orchestration_v2_effect_outbox"]) {
      const rows = yield* sql`SELECT COUNT(*) AS count FROM ${sql(table)} WHERE thread_id = ${command.threadId}`;
      assert.equal(rows[0]?.count, 0);
    }
  }).pipe(Effect.provide(makeTestLayer()));
});

it.effect("holds a runtime stop without a current resident target before receipts fences or effects", () =>
  Effect.gen(function* () {
    const { orchestrator, command, principal } = yield* fixture("unknown-runtime-stop");
    const sql = yield* SqlClient.SqlClient;
    const input = { commandId: CommandId.make("stop:unknown-runtime-stop"), threadId: command.threadId,
      target: { driver: ProviderDriverKind.make("codex"), evidenceRevision: 1,
        binding: { threadId: command.threadId, providerThreadId: ProviderThreadId.make("provider-thread:unknown-runtime-stop"),
          providerSessionId: ProviderSessionId.make("provider-session:unknown-runtime-stop"), instanceId,
          runtimeGeneration: "generation:unknown-runtime-stop" } } };
    const before = yield* sql`SELECT event_id, sequence FROM orchestration_events ORDER BY sequence`;
    const threadBefore = yield* sql`SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id = ${command.threadId}`;
    const result = yield* orchestrator.stopCurrentThreadRuntime(input).pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal));
    assert.equal(result.commandStatus, "unknown");
    assert.isNull(result.receipt);
    assert.isNull(result.target);
    assert.equal(result.queueFence.status, "unknown");
    assert.equal(result.runtimeStop.status, "not_started");
    assert.deepEqual(yield* sql`SELECT event_id, sequence FROM orchestration_events ORDER BY sequence`, before);
    assert.deepEqual(yield* sql`SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id = ${command.threadId}`, threadBefore);
    assert.deepEqual(yield* sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id = ${input.commandId}`, []);
    assert.deepEqual(yield* sql`SELECT command_id FROM orchestration_v2_current_runtime_stop_intents WHERE command_id = ${input.commandId}`, []);
    assert.deepEqual(yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox`, []);
  }).pipe(Effect.provide(makeTestLayer())),
);

it.effect("dispatches a captured settled restart through the private origin and rejects stripped or changed replay", () => {
  let opens = 0;
  const adapter = makeResidentStopAdapter(() => { opens += 1; }, () => {}, () => {});
  const runtime = makeTestLayer(adapter);
  const layer = Layer.mergeAll(runtime, ThreadManagement.layerWithLegacyImporter.pipe(Layer.provide(Layer.mergeAll(
    runtime, Layer.mock(LegacyV1ThreadImporter.LegacyV1ThreadImporter)({
      ensureTranscript: () => Effect.die("A captured restart must not hydrate an imported transcript"),
    }),
  ))));
  return Effect.gen(function* () {
    for (const status of ["completed", "waiting"] as const) {
      const { orchestrator, command } = yield* fixture(`captured-restart-${status}`);
      const sink = yield* EventSink.EventSinkV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const sql = yield* SqlClient.SqlClient;
      const ids = yield* IdAllocator.IdAllocatorV2;
      yield* orchestrator.dispatch(command);
      const initial = yield* orchestrator.getThreadProjection(command.threadId);
      const provider = initial.providerThreads[0]!;
      const source = initial.runs[0]!;
      const root = initial.nodes.find((node) => node.id === source.rootNodeId)!;
      const attempt = initial.attempts.find((candidate) => candidate.id === source.activeAttemptId)!;
      assert.isNotNull(root.checkpointScopeId);
      yield* outbox.cancelUnsettled({ threadId: command.threadId, effectTypes: ["provider-turn.start"], reason: "Fixture supplies completed foreground state without native execution" });
      const now = yield* DateTime.now;
      const providerSessionId = provider.providerSessionId!;
      yield* sink.write({ events: [{ id: yield* ids.allocate.event({ threadId: command.threadId }),
        type: "provider-thread.updated", threadId: command.threadId, occurredAt: now,
        payload: { ...provider, nativeThreadRef: { driver: provider.driver, nativeId: `native:${status}:captured-restart`, strength: "strong" }, status: "idle" },
      }] });
      yield* manager.open({ threadId: command.threadId, providerSessionId, modelSelection,
        runtimePolicy: { runtimeMode: "full-access", interactionMode: "default", cwd: process.cwd() } });
      yield* manager.registerRuntimeBinding({ threadId: command.threadId, providerSessionId, providerThreadId: provider.id });
      yield* sink.write({ events: [
        { id: yield* ids.allocate.event({ threadId: command.threadId }), type: "run.updated", threadId: command.threadId,
          occurredAt: now, payload: { ...source, status, completedAt: status === "waiting" ? null : now } },
        { id: yield* ids.allocate.event({ threadId: command.threadId }), type: "run-attempt.updated", threadId: command.threadId,
          occurredAt: now, payload: { ...attempt, status: "completed", completedAt: now } },
        { id: yield* ids.allocate.event({ threadId: command.threadId }), type: "node.updated", threadId: command.threadId,
          occurredAt: now, payload: { ...root, status: "completed", completedAt: now } },
        { id: yield* ids.allocate.event({ threadId: command.threadId }), type: "provider-turn.updated", threadId: command.threadId,
          occurredAt: now, payload: { id: ProviderTurnId.make(`turn:captured-restart:${status}`), providerThreadId: provider.id,
            nodeId: root.id, runAttemptId: attempt.id, nativeTurnRef: null, ordinal: 1, status: "completed", startedAt: now, completedAt: now } },
      ] });
      const settledSource = (yield* orchestrator.getThreadProjection(command.threadId)).runs.find((run) => run.id === source.id)!;
      const checkpointCommandId = CommandId.make(`command:effect:checkpoint.capture:${source.id}`);
      const checkpointEffectId = `effect:${checkpointCommandId}:checkpoint.capture:${source.id}`;
      const checkpointWorkerId = `worker:captured-checkpoint:${status}`;
      const owner = yield* sink.readCurrentProviderRuntimeOwner(command.threadId);
      assert.isNotNull(owner);
      const marker = yield* sink.prepareRestartContinuation({ markerId: `marker:captured-restart:${status}`,
        threadId: command.threadId, projectId: initial.thread.projectId, sourceRunId: source.id,
        sourceRunAttemptId: source.activeAttemptId!, expectedBinding: owner!.binding, expectedEvidenceRevision: owner!.evidenceRevision });
      const release = yield* sink.readNativeCommandFacts({ threadId: command.threadId,
        commandId: CommandId.make(`release:captured-restart:${status}`) });
      assert.isTrue(yield* sink.releaseRestartContinuation({ marker, currentSnapshot: release.commitSnapshot,
        revalidateAfterTrial: Effect.gen(function* () {
          assert.deepEqual(yield* sink.readCurrentProviderRuntimeOwner(command.threadId), owner);
        }),
      }));
      const workerId = `worker:captured-restart:${status}`;
      const claim = Option.getOrThrow(yield* outbox.claimNext({ workerId, leaseDurationMs: 60_000 }));
      if (status === "waiting") {
        yield* outbox.enqueue([{ id: checkpointEffectId, commandId: checkpointCommandId, threadId: command.threadId,
          request: { type: "checkpoint.capture", runId: source.id, scopeId: root.checkpointScopeId! } }]);
        assert.isTrue(Option.isNone(yield* outbox.claimNext({ workerId: checkpointWorkerId, leaseDurationMs: 60_000 })));
      }
      const context = { effectId: claim.id, marker, workerId, expectedAttempt: claim.attemptCount };
      const continuation = { ...command, ...EventSink.capturedRestartContinuationIdsV1(context),
        text: "Continue where you left off.", modelSelection, createdBy: "agent" as const, creationSource: "server" as const,
        restartContinuationOfRunId: source.id };
      const accepted = yield* threads.dispatchRestartContinuation(continuation, context);
      const after = yield* orchestrator.getThreadProjection(command.threadId);
      const created = after.runs.find((run) => run.restartContinuationOfRunId === source.id);
      assert.isDefined(created);
      assert.equal(created?.status, status === "waiting" ? "queued" : "starting");
      assert.deepEqual(after.runs.find((run) => run.id === source.id), settledSource);
      assert.equal(after.messages.find((message) => message.id === continuation.messageId)?.text, continuation.text);
      const origin = (yield* sink.readCommandReceiptIdentity(continuation.commandId)).capturedRestartOrigin;
      assert.deepEqual(origin?.marker, marker);
      assert.equal(origin?.originalClaim.workerId, workerId);
      const before = yield* sql`SELECT event_id, sequence FROM orchestration_events WHERE stream_id = ${command.threadId} ORDER BY sequence`;
      const effects = yield* sql`SELECT effect_id, status FROM orchestration_v2_effect_outbox WHERE command_id = ${continuation.commandId}`;
      assert.deepEqual(effects, status === "waiting" ? [] : [{ effect_id: `effect:${continuation.commandId}:provider-turn.start:${created!.id}`, status: "pending" }]);
      assert.deepEqual(yield* threads.dispatchRestartContinuation(continuation, context), accepted);
      const stripped = yield* threads.dispatch(continuation).pipe(Effect.flip);
      assert.equal(stripped._tag === "DispatchGuardRejectedError" ? stripped.reason : undefined, "identity_conflict");
      const changed = yield* threads.dispatchRestartContinuation({ ...continuation, text: "Different restart" }, context).pipe(Effect.flip);
      assert.equal(changed._tag === "DispatchGuardRejectedError" ? changed.reason : undefined, "identity_conflict");
      assert.deepEqual(yield* sql`SELECT event_id, sequence FROM orchestration_events WHERE stream_id = ${command.threadId} ORDER BY sequence`, before);
      assert.deepEqual(yield* sql`SELECT effect_id, status FROM orchestration_v2_effect_outbox WHERE command_id = ${continuation.commandId}`, effects);
      yield* outbox.succeed({ effectId: claim.id, workerId });
      if (status === "waiting") {
        const checkpointClaim = Option.getOrThrow(yield* outbox.claimNext({ workerId: checkpointWorkerId, leaseDurationMs: 60_000 }));
        assert.equal(checkpointClaim.id, checkpointEffectId);
        const completedAt = yield* DateTime.now;
        const checkpoint = { id: yield* ids.allocate.checkpoint({ checkpointScopeId: root.checkpointScopeId!, name: "captured-waiting-completion" }),
          threadId: command.threadId, scopeId: root.checkpointScopeId!, runId: source.id, nodeId: root.id,
          parentCheckpointId: null, ordinalWithinScope: source.ordinal, appRunOrdinal: source.ordinal,
          ref: CheckpointRef.make("refs/t3/fixture/captured-waiting-completion"), status: "ready" as const, files: [], capturedAt: completedAt };
        yield* sink.commitCommand({ commandId: checkpointCommandId, commandType: "checkpoint.capture", threadId: command.threadId,
          acceptedAt: completedAt, effects: [], events: [
            { id: yield* ids.allocate.event({ threadId: command.threadId, commandId: checkpointCommandId }), type: "checkpoint.captured",
              threadId: command.threadId, runId: source.id, nodeId: root.id, occurredAt: completedAt, payload: checkpoint },
            { id: yield* ids.allocate.event({ threadId: command.threadId, commandId: checkpointCommandId }), type: "run.updated",
              threadId: command.threadId, runId: source.id, nodeId: root.id, occurredAt: completedAt,
              payload: { ...settledSource, status: "completed", checkpointId: checkpoint.id, completedAt } },
          ] });
        yield* outbox.succeed({ effectId: checkpointEffectId, workerId: checkpointWorkerId });
        yield* orchestrator.dispatch({ type: "queue.resume", commandId: CommandId.make("resume:captured-waiting-completion"), threadId: command.threadId });
        const resumed = (yield* orchestrator.getThreadProjection(command.threadId)).runs.find((run) => run.id === created!.id)!;
        assert.equal(resumed.status, "starting");
        assert.equal(resumed.userMessageId, continuation.messageId);
        assert.equal(resumed.restartContinuationOfRunId, source.id);
        const startId = `effect:command:system:start-queued:${created!.id}:provider-turn.start:${created!.id}`;
        assert.deepEqual(yield* sql`SELECT effect_id, status FROM orchestration_v2_effect_outbox WHERE effect_id = ${startId}`,
          [{ effect_id: startId, status: "pending" }]);
      }
      yield* outbox.cancelUnsettled({ threadId: command.threadId, effectTypes: ["provider-turn.start"], reason: "Fixture ends before native execution" });
    }
    assert.equal(opens, 1);
  }).pipe(Effect.provide(layer));
});

it.effect("accepts and replays a pinned runtime stop with original queued fences and no native detach", () => {
  let opens = 0;
  let closes = 0;
  let interrupts = 0;
  const adapter = makeResidentStopAdapter(() => { opens += 1; }, () => { closes += 1; }, () => { interrupts += 1; });
  return Effect.gen(function* () {
    const { orchestrator, command, principal } = yield* fixture("accepted-runtime-stop");
    const sink = yield* EventSink.EventSinkV2;
    const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
    const sql = yield* SqlClient.SqlClient;
    const ids = yield* IdAllocator.IdAllocatorV2;
    yield* orchestrator.dispatch({ ...command, dispatchMode: { type: "defer_start" } });
    const prepared = yield* orchestrator.getThreadProjection(command.threadId);
    const providerThread = prepared.providerThreads[0]!;
    assert.isNotNull(providerThread.providerSessionId);
    const providerSessionId = providerThread.providerSessionId!;
    const now = yield* DateTime.now;
    yield* sink.write({ events: [
      { id: yield* ids.allocate.event({ threadId: command.threadId }), type: "provider-thread.updated",
        threadId: command.threadId, occurredAt: now, payload: { ...providerThread,
          nativeThreadRef: { driver: providerThread.driver, nativeId: "native:accepted-runtime-stop", strength: "strong" }, status: "idle" } },
      { id: yield* ids.allocate.event({ threadId: command.threadId }), type: "thread.metadata-updated",
        threadId: command.threadId, occurredAt: now, payload: { ...prepared.thread,
          pinnedAt: now, pinOrderKey: "a12", activeOrderKey: "a8", snoozedAt: now, snoozedUntil: DateTime.add(now, { days: 1 }) } },
    ] });
    yield* manager.open({ threadId: command.threadId, providerSessionId, modelSelection,
      runtimePolicy: { runtimeMode: "full-access", interactionMode: "default", cwd: process.cwd() } });
    yield* manager.registerRuntimeBinding({ threadId: command.threadId, providerSessionId, providerThreadId: providerThread.id });
    yield* orchestrator.dispatch({ ...command, commandId: CommandId.make("queue:accepted-runtime-stop"),
      messageId: MessageId.make("message:queued-runtime-stop"), text: "Preserve this queued payload", dispatchMode: { type: "queue_after_active" } });
    const projection = yield* orchestrator.getThreadProjection(command.threadId);
    const queued = projection.runs.find((run) => run.status === "queued")!;
    assert.isDefined(queued);
    const attachment = yield* manager.readCurrentThreadRuntimeAttachment(command.threadId);
    assert.equal(attachment.status, "attached");
    if (attachment.status !== "attached") return yield* Effect.die("Expected the actual current resident attachment");
    const input = { commandId: CommandId.make("stop:accepted-runtime-stop"), threadId: command.threadId,
      target: { binding: attachment.binding, driver: attachment.driver, evidenceRevision: attachment.evidenceRevision } };
    const beforeThread = yield* sql`SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id = ${command.threadId}`;
    const beforeRuns = yield* sql`SELECT payload_json FROM orchestration_v2_projection_runs WHERE thread_id = ${command.threadId} ORDER BY run_id`;
    const beforeProviders = yield* sql`SELECT payload_json FROM orchestration_v2_projection_provider_threads WHERE thread_id = ${command.threadId} ORDER BY provider_thread_id`;
    const stop = orchestrator.stopCurrentThreadRuntime(input).pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal));
    yield* sql.unsafe(`CREATE TRIGGER stop_intent_failure BEFORE INSERT ON orchestration_v2_current_runtime_stop_intents
      WHEN NEW.command_id = '${input.commandId}' BEGIN SELECT RAISE(ABORT, 'injected stop intent failure'); END`);
    const failed = yield* Effect.exit(stop);
    assert.isTrue(Exit.isFailure(failed));
    if (Exit.isFailure(failed)) assert.include(inspect(Cause.squash(failed.cause), { depth: 8 }), "injected stop intent failure");
    assert.deepEqual(yield* sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id = ${input.commandId}`, []);
    assert.deepEqual(yield* sql`SELECT event_id FROM orchestration_events WHERE command_id = ${input.commandId}`, []);
    assert.deepEqual(yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE command_id = ${input.commandId}`, []);
    assert.deepEqual(yield* sql`SELECT command_id FROM orchestration_v2_current_runtime_stop_intents WHERE command_id = ${input.commandId}`, []);
    assert.deepEqual(yield* sql`SELECT stop_command_id FROM orchestration_v2_queued_runtime_stop_fences WHERE stop_command_id = ${input.commandId}`, []);
    yield* sql.unsafe("DROP TRIGGER stop_intent_failure");
    const accepted = yield* stop;
    assert.equal(accepted.commandStatus, "accepted");
    assert.equal(accepted.receipt?.commandType, "provider-session.detach");
    assert.equal(accepted.queueFence.status, "installed");
    assert.deepEqual(accepted.queueFence.affectedRunIds, [queued.id]);
    assert.equal(accepted.runtimeStop.status, "pending");
    assert.deepEqual(accepted.target, input.target);
    const intent = yield* sink.readCurrentThreadRuntimeStopIntent(input);
    assert.equal(intent?.queuedBases.length, 1);
    assert.equal(intent?.queuedBases[0]?.executionIntent?.runAttemptId, queued.activeAttemptId);
    assert.equal(intent?.queuedBases[0]?.queuedProviderThreadId, queued.providerThreadId);
    const fences = yield* sink.readQueuedRunRuntimeStopFences({ threadId: command.threadId, runId: queued.id, incarnation: intent!.incarnation });
    assert.equal(fences.length, 1);
    assert.equal(fences[0]?.executionIntent.effectId, `effect:command:system:start-queued:${queued.id}:provider-turn.start:${queued.id}`);
    assert.deepEqual(yield* sql`SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id = ${command.threadId}`, beforeThread);
    assert.deepEqual(yield* sql`SELECT payload_json FROM orchestration_v2_projection_runs WHERE thread_id = ${command.threadId} ORDER BY run_id`, beforeRuns);
    assert.deepEqual(yield* sql`SELECT payload_json FROM orchestration_v2_projection_provider_threads WHERE thread_id = ${command.threadId} ORDER BY provider_thread_id`, beforeProviders);
    const events = yield* sql`SELECT event_id, sequence, event_type FROM orchestration_events WHERE command_id = ${input.commandId}`;
    assert.equal(events.length, 1);
    assert.equal(events[0]?.event_type, "provider-session.detach-requested");
    const outbox = yield* sql`SELECT effect_id, status FROM orchestration_v2_effect_outbox WHERE command_id = ${input.commandId}`;
    assert.deepEqual(outbox, [{ effect_id: `effect:${input.commandId}:provider-session.detach:${providerSessionId}`, status: "pending" }]);
    assert.deepEqual(yield* stop, accepted);
    const stale = yield* Effect.flip(orchestrator.stopCurrentThreadRuntime({ ...input,
      target: { ...input.target, evidenceRevision: input.target.evidenceRevision + 1 } }).pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal)));
    assert.equal(stale._tag === "DispatchGuardRejectedError" ? stale.reason : undefined, "identity_conflict");
    const stripped = yield* Effect.flip(orchestrator.dispatch({ type: "provider-session.detach", commandId: input.commandId,
      threadId: command.threadId, providerSessionId }));
    assert.equal(stripped._tag === "DispatchGuardRejectedError" ? stripped.reason : undefined, "identity_conflict");
    assert.deepEqual(yield* sql`SELECT event_id, sequence, event_type FROM orchestration_events WHERE command_id = ${input.commandId}`, events);
    assert.deepEqual(yield* sql`SELECT effect_id, status FROM orchestration_v2_effect_outbox WHERE command_id = ${input.commandId}`, outbox);
    assert.equal(opens, 1);
    assert.equal(interrupts, 0);
    assert.equal(closes, 0);
  }).pipe(Effect.provide(makeTestLayer(adapter)));
});

it.effect("observes the original pinned runtime stop after actual claimed execution succeeds", () => {
  let opens = 0;
  let closes = 0;
  const adapter = makeResidentStopAdapter(() => { opens += 1; }, () => { closes += 1; }, () => {});
  return Effect.gen(function* () {
    const { orchestrator, command, principal } = yield* fixture("completed-runtime-stop");
    const sink = yield* EventSink.EventSinkV2;
    const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
    const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const ids = yield* IdAllocator.IdAllocatorV2;
    yield* orchestrator.dispatch({ ...command, dispatchMode: { type: "defer_start" } });
    const prepared = yield* orchestrator.getThreadProjection(command.threadId);
    const provider = prepared.providerThreads[0]!;
    assert.isNotNull(provider.providerSessionId);
    const providerSessionId = provider.providerSessionId!;
    const now = yield* DateTime.now;
    yield* sink.write({ events: [{ id: yield* ids.allocate.event({ threadId: command.threadId }),
      type: "provider-thread.updated", threadId: command.threadId, occurredAt: now,
      payload: { ...provider, nativeThreadRef: { driver: provider.driver,
        nativeId: "native:completed-runtime-stop", strength: "strong" }, status: "idle" } }] });
    yield* manager.open({ threadId: command.threadId, providerSessionId, modelSelection,
      runtimePolicy: { runtimeMode: "full-access", interactionMode: "default", cwd: process.cwd() } });
    yield* manager.registerRuntimeBinding({ threadId: command.threadId, providerSessionId, providerThreadId: provider.id });
    const attachment = yield* manager.readCurrentThreadRuntimeAttachment(command.threadId);
    assert.equal(attachment.status, "attached");
    if (attachment.status !== "attached") return yield* Effect.die("Expected the original resident stop target");
    const input = { commandId: CommandId.make("stop:completed-runtime-stop"), threadId: command.threadId,
      target: { binding: attachment.binding, driver: attachment.driver, evidenceRevision: attachment.evidenceRevision } };
    const accepted = yield* orchestrator.stopCurrentThreadRuntime(input).pipe(
      Effect.provideService(EnvironmentAuthenticatedPrincipal, principal),
    );
    assert.equal(accepted.runtimeStop.status, "pending");
    const workerId = "worker:completed-runtime-stop";
    const claim = Option.getOrThrow(yield* outbox.claimNext({ workerId, leaseDurationMs: 60_000 }));
    assert.equal(claim.id, `effect:${input.commandId}:provider-session.detach:${providerSessionId}`);
    assert.equal(claim.commandId, input.commandId);
    assert.equal(claim.threadId, input.threadId);
    assert.equal(claim.request.type, "provider-session.detach");
    yield* executor.execute(claim, { willRetry: false });
    assert.equal((yield* manager.readCurrentThreadRuntimeAttachment(command.threadId)).status, "stopped");
    assert.isTrue(yield* outbox.succeed({ effectId: claim.id, workerId }));
    assert.deepEqual(yield* outbox.listHeldByThreadId(command.threadId), []);
    const completed = yield* orchestrator.observeCurrentThreadRuntimeStop(input);
    assert.equal(completed.runtimeStop.status, "stopped", "Successful pinned provider-session stop stayed unknown");
    assert.equal(completed.reason, null);
    assert.deepEqual(completed.target, input.target);
    assert.deepEqual(completed.receipt, accepted.receipt);
    assert.deepEqual(completed.queueFence, accepted.queueFence);
    assert.equal(opens, 1);
    assert.equal(closes, 1);
  }).pipe(Effect.provide(makeTestLayer(adapter)));
});

it.effect("fences the original reserved queued intent after its run becomes starting", () => {
  const adapter = makeResidentStopAdapter(() => {}, () => {}, () => {});
  return Effect.gen(function* () {
    const { orchestrator, command, principal } = yield* fixture("starting-runtime-stop");
    const sink = yield* EventSink.EventSinkV2;
    const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
    const ids = yield* IdAllocator.IdAllocatorV2;
    const sql = yield* SqlClient.SqlClient;
    yield* orchestrator.dispatch({ ...command, dispatchMode: { type: "defer_start" } });
    const prepared = yield* orchestrator.getThreadProjection(command.threadId);
    const providerThread = prepared.providerThreads[0]!;
    const providerSessionId = providerThread.providerSessionId;
    if (providerSessionId === null) return yield* Effect.die("Expected the ordinary planned provider session ID");
    const now = yield* DateTime.now;
    yield* sink.write({ events: [{ id: yield* ids.allocate.event({ threadId: command.threadId }),
      type: "provider-thread.updated", threadId: command.threadId, occurredAt: now,
      payload: { ...providerThread, nativeThreadRef: { driver: providerThread.driver,
        nativeId: "native:starting-runtime-stop", strength: "strong" }, status: "idle" } }] });
    yield* manager.open({ threadId: command.threadId, providerSessionId, modelSelection,
      runtimePolicy: { runtimeMode: "full-access", interactionMode: "default", cwd: process.cwd() } });
    yield* manager.registerRuntimeBinding({ threadId: command.threadId, providerSessionId, providerThreadId: providerThread.id });
    yield* orchestrator.dispatch({ ...command, commandId: CommandId.make("queue:starting-runtime-stop"),
      messageId: MessageId.make("message:starting-runtime-stop"), text: "Original queued payload", dispatchMode: { type: "queue_after_active" } });
    const projection = yield* orchestrator.getThreadProjection(command.threadId);
    const queued = projection.runs.find((run) => run.status === "queued")!;
    const owner = yield* sink.readCurrentProviderRuntimeOwner(command.threadId);
    const incarnation = yield* sink.readApplicationThreadBirth(command.threadId);
    if (owner === null || owner.binding.runtimeGeneration === null || incarnation === null || queued.activeAttemptId === null) {
      return yield* Effect.die("Expected the actual registered source, application birth and queued attempt");
    }
    const commandId = CommandId.make(`command:system:start-queued:${queued.id}`);
    const executionIntent: Orchestrator.QueuedRunExecutionIntentV2 = { kind: "queued", commandId, runId: queued.id,
      runAttemptId: queued.activeAttemptId, effectId: `effect:${commandId}:provider-turn.start:${queued.id}`, reviewedBasis: null };
    const basisFields = { runId: queued.id, messageId: queued.userMessageId, queuedProviderThreadId: queued.providerThreadId,
      runAttemptId: queued.activeAttemptId, executionIntent, switchPlan: null, sourceMode: "queued_thread" as const,
      sourceBinding: { ...owner.binding, runtimeGeneration: owner.binding.runtimeGeneration }, sourceEvidenceRevision: owner.evidenceRevision };
    const basis: Orchestrator.QueuedRunContinuationBasisV2 = { ...basisFields,
      basisDigest: EventSink.queuedRunContinuationBasisDigestV2(basisFields) };
    yield* sink.withTransaction(Effect.gen(function* () {
      const facts = yield* sink.readNativeCommandFacts({ threadId: command.threadId, commandId });
      const reserved = yield* sink.reserveQueuedRunStart({ snapshot: facts.commitSnapshot, incarnation, basis, executionIntent,
        pendingEffect: { id: executionIntent.effectId, commandId, threadId: command.threadId,
          request: { type: "provider-turn.start", runId: queued.id } },
        revalidateCurrentSource: Effect.gen(function* () {
          assert.deepEqual(yield* sink.readCurrentProviderRuntimeOwner(command.threadId), owner);
        }) });
      assert.equal(reserved.status, "reserved");
      yield* sink.write({ events: [
        { id: yield* ids.allocate.event({ threadId: command.threadId }), type: "run.updated", threadId: command.threadId,
          runId: prepared.runs[0]!.id, occurredAt: now, payload: { ...prepared.runs[0]!, status: "completed", completedAt: now } },
        { id: yield* ids.allocate.event({ threadId: command.threadId }), type: "run.updated", threadId: command.threadId,
          runId: queued.id, occurredAt: now, payload: { ...queued, status: "starting", queuePosition: null } },
      ] });
    }));
    const attachment = yield* manager.readCurrentThreadRuntimeAttachment(command.threadId);
    if (attachment.status !== "attached") return yield* Effect.die("Expected the current resident target after queued preparation");
    const input = { commandId: CommandId.make("stop:starting-runtime-stop"), threadId: command.threadId,
      target: { binding: attachment.binding, driver: attachment.driver, evidenceRevision: attachment.evidenceRevision } };
    const before = yield* sql`SELECT payload_json FROM orchestration_v2_projection_runs WHERE run_id = ${queued.id}`;
    const accepted = yield* orchestrator.stopCurrentThreadRuntime(input).pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal));
    assert.equal(accepted.commandStatus, "accepted");
    assert.deepEqual(accepted.queueFence.affectedRunIds, [queued.id]);
    const intent = yield* sink.readCurrentThreadRuntimeStopIntent(input);
    assert.deepEqual(intent?.queuedBases, [basis]);
    const fences = yield* sink.readQueuedRunRuntimeStopFences({ threadId: command.threadId, runId: queued.id, incarnation });
    assert.equal(fences.length, 1);
    assert.deepEqual(fences[0]?.executionIntent, executionIntent);
    assert.equal(fences[0]?.basisDigest, basis.basisDigest);
    assert.deepEqual(yield* sql`SELECT payload_json FROM orchestration_v2_projection_runs WHERE run_id = ${queued.id}`, before);
    assert.deepEqual(yield* sql`SELECT status FROM orchestration_v2_effect_outbox WHERE effect_id = ${executionIntent.effectId}`, [{ status: "cancelled" }]);
  }).pipe(Effect.provide(makeTestLayer(adapter)));
});

it.effect("reviews and observes imported choices without hydration, command writes or provider allocation", () => {
  let hydrations = 0;
  let snapshotReads = 0;
  const orchestratorLayer = makeTestLayer();
  const layer = Layer.mergeAll(
    orchestratorLayer,
    ThreadManagement.layerWithLegacyImporter.pipe(Layer.provide(Layer.mergeAll(
      orchestratorLayer,
      Layer.mock(LegacyV1ThreadImporter.LegacyV1ThreadImporter)({
        ensureTranscript: () => Effect.sync(() => { hydrations += 1; }),
        readTranscriptSnapshotEvidence: () => Effect.sync(() => { snapshotReads += 1; return null; }),
      }),
    ))),
  );
  return Effect.gen(function* () {
    const { command, principal } = yield* fixture("imported-read");
    const threads = yield* ThreadManagement.ThreadManagementService;
    const sql = yield* SqlClient.SqlClient;
    const before = yield* sql`SELECT event_id, command_id FROM orchestration_events ORDER BY sequence`;
    const review = yield* threads.reviewImportedHistoryStart({
      threadId: command.threadId,
      delivery: {
        type: "message", messageId: command.messageId, text: command.text, attachments: [],
        runtimeMode: "full-access", interactionMode: "default", dispatchMode: { type: "start_immediately" },
      },
    }).pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal));
    assert.equal(review.applicability, "not_imported");
    assert.isNull(review.reviewedBasis);
    const observed = yield* threads.observeImportedHistoryStart({ threadId: command.threadId, commandId: command.commandId });
    assert.equal(observed.intentStatus, "not_found");
    assert.isNull(observed.target);
    assert.equal(hydrations, 0);
    assert.equal(snapshotReads, 1);
    assert.deepEqual(yield* sql`SELECT event_id, command_id FROM orchestration_events ORDER BY sequence`, before);
    const effects = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count FROM orchestration_v2_effect_outbox`;
    const choices = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count FROM orchestration_v2_imported_history_start_choices`;
    assert.equal(effects[0]?.count, 0);
    assert.equal(choices[0]?.count, 0);
  }).pipe(Effect.provide(layer));
});

it.effect("persists an identity-bound stale rejection without events and preserves exact rejected replay", () =>
  Effect.gen(function* () {
    const { orchestrator, command, dispatch } = yield* fixture("stale");
    const sql = yield* SqlClient.SqlClient;
    yield* orchestrator.dispatch({
      type: "thread.metadata.update", commandId: CommandId.make("stale:rename"), threadId: command.threadId, title: "Changed",
    });
    const stale = yield* Effect.flip(dispatch());
    assert.equal(stale._tag === "DispatchGuardRejectedError" ? stale.reason : undefined, "stale_target");
    const again = yield* Effect.flip(dispatch());
    assert.instanceOf(again, Orchestrator.OrchestratorCommandPreviouslyRejectedError);
    const changed = yield* Effect.flip(dispatch({ ...command, text: "Different" }));
    assert.equal(changed._tag === "DispatchGuardRejectedError" ? changed.reason : undefined, "identity_conflict");
    const events = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count FROM orchestration_events WHERE command_id = ${command.commandId}`;
    const effects = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count FROM orchestration_v2_effect_outbox WHERE command_id = ${command.commandId}`;
    assert.equal(events[0]?.count, 0);
    assert.equal(effects[0]?.count, 0);
    const observation = yield* orchestrator.observeCommand(command);
    assert.equal(observation.commandStatus, "rejected");
  }).pipe(Effect.provide(makeTestLayer())),
);

it.effect("serializes two guarded starts to one accepted message", () =>
  Effect.gen(function* () {
    const { command, dispatch } = yield* fixture("concurrent");
    const sql = yield* SqlClient.SqlClient;
    const results = yield* Effect.all([
      Effect.exit(dispatch()),
      Effect.exit(dispatch({ ...command, commandId: CommandId.make("dispatch:guard:concurrent:other"), messageId: MessageId.make("message:guard:concurrent:other") })),
    ], { concurrency: 2 });
    assert.equal(results.filter(Exit.isSuccess).length, 1);
    assert.equal(results.filter(Exit.isFailure).length, 1);
    const messages = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count FROM orchestration_v2_projection_messages WHERE thread_id = ${command.threadId} AND role = 'user'`;
    assert.equal(messages[0]?.count, 1);
  }).pipe(Effect.provide(makeTestLayer())),
);

it.effect("rechecks an actor contributor changed during planning before queuing provider effects", () => {
  let capabilities: ReturnType<ProviderAdapterV2Shape["getCapabilities"]> = Effect.succeed(CodexProviderCapabilitiesV2);
  const adapter = makeAdapter(() => capabilities);
  return Effect.gen(function* () {
    const { orchestrator, sessions, principal, command, dispatch } = yield* fixture("actor-cas");
    const sql = yield* SqlClient.SqlClient;
    const reached = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    capabilities = Effect.gen(function* () {
      yield* Deferred.succeed(reached, undefined);
      yield* Deferred.await(release);
      return CodexProviderCapabilitiesV2;
    });
    const pending = yield* dispatch().pipe(Effect.forkChild);
    yield* Deferred.await(reached);
    yield* sessions.revoke({ sessionId: principal.sessionId, revokedAt: yield* DateTime.now });
    yield* Deferred.succeed(release, undefined);
    const failure = yield* Effect.flip(Fiber.join(pending));
    assert.equal(failure._tag === "DispatchGuardRejectedError" ? failure.reason : undefined, "stale_target");
    const effects = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count FROM orchestration_v2_effect_outbox WHERE command_id = ${command.commandId}`;
    const events = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count FROM orchestration_events WHERE command_id = ${command.commandId}`;
    assert.equal(effects[0]?.count, 0);
    assert.equal(events[0]?.count, 0);
    assert.equal((yield* orchestrator.observeCommand(command)).commandStatus, "rejected");
  }).pipe(Effect.provide(makeTestLayer(adapter)));
});

it.effect("rolls back native stage acceptance when its durable event association cannot be written", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const repository = yield* NativeCreationRepository;
    const sessions = yield* makeAuthSessions;
    const sql = yield* SqlClient.SqlClient;
    const now = yield* DateTime.now;
    const sessionId = AuthSessionId.make("session:native-stage-rollback");
    const principal: EnvironmentAuthenticatedPrincipal["Service"] = {
      sessionId, subject: "native-stage-rollback", method: "bearer-access-token",
      scopes: new Set(["orchestration:read", "orchestration:operate"]),
    };
    yield* sessions.create({
      sessionId, subject: principal.subject, method: principal.method, scopes: [...principal.scopes],
      issuedAt: now, expiresAt: DateTime.add(now, { days: 1 }),
      client: { label: null, ipAddress: null, userAgent: null, deviceType: "bot", os: null, browser: null },
    });
    yield* sql`INSERT INTO native_creation_automation_enrollments (session_id, enrolled_at)
      VALUES (${sessionId}, ${DateTime.formatIso(now)})`;
    const binding = Schema.decodeUnknownSync(NativePreparationBinding)({
      backend_instance: "synthetic-backend", environment_id: "synthetic-environment",
      project_id: "project:native-stage-rollback", project_cwd: "/synthetic/project", account_ref: "synthetic-account",
      runtime_mode: "full-access", interaction_mode: "default", base_branch: "main",
      start_from_origin: false, run_setup_script: false, provider_model_selection: modelSelection,
    });
    const original = nativePreparationCommand("native-stage-rollback", binding, "Run once", "Native stage", DateTime.formatIso(now));
    const preparation = yield* validateNativeCreationPreparation(new TextEncoder().encode(nativeCreationCanonicalJson({
      schema: "voice.t3-bootstrap-preparation/v1", operation_id: "native-stage-rollback", binding, command: original,
      preparation_id: original.commandId.replace("voice-command-", "voice-bootstrap-"),
      binding_digest: nativeCreationSha256(nativeCreationCanonicalJson(binding)),
      prompt_digest: nativeCreationSha256(original.message.text), command_digest: nativeCreationSha256(nativeCreationCanonicalJson(original)),
    })));
    const historical = Schema.decodeUnknownSync(NativeCreationHistoricalBinding)({
      backendInstance: binding.backend_instance, environmentId: binding.environment_id, projectId: binding.project_id,
      projectCwd: binding.project_cwd, accountRef: binding.account_ref, accountBindingId: "synthetic-qualified-account",
      accountBindingRevision: 1, providerModelSelection: binding.provider_model_selection,
      runtimeMode: binding.runtime_mode, interactionMode: binding.interaction_mode, baseBranch: binding.base_branch,
      startFromOrigin: false, runSetupScript: false, requestedBranch: original.bootstrap.prepareWorktree.branch,
    });
    const resources = { projectCwd: binding.project_cwd, branch: historical.requestedBranch, worktreePath: "/synthetic/worktree" };
    const guard = { schema: "t3.native-creation-guard/v1" as const, grantId: "synthetic-stage-grant", grantRevision: 1 };
    const grant: NativeCreationGrant = {
      grantId: guard.grantId, revision: 1, actorSessionId: sessionId, issuerId: "synthetic-issuer",
      expiresAt: DateTime.add(now, { days: 1 }), revoked: false,
      operationId: preparation.operationId, preparationId: preparation.preparationId,
      preparationSha256: preparation.preparationSha256, bindingDigest: preparation.bindingDigest,
      binding: historical, resources, allowedStages: ["claim", "native_command"], recoveryScopes: [],
    };
    const authority = yield* makeNativeCreationAuthority.pipe(
      Effect.provideService(AuthSessionRepository, sessions),
      Effect.provideService(NativeCreationGrantResolver, {
        resolveCurrent: () => Effect.succeed({ enrolledSessionId: sessionId, trustedIssuerId: grant.issuerId, grant }),
      }),
      Effect.provideService(NativeCreationBindingResolver, { resolveCurrent: () => Effect.succeed(historical) }),
    );
    const claimId = "synthetic-stage-rollback-claim";
    const input = { actorSessionId: sessionId, preparation, guard, resources };
    yield* repository.claim({
      ...input, claimId, claimedBootId: "synthetic-boot", claimedAt: DateTime.formatIso(now),
      grantId: guard.grantId, grantRevision: guard.grantRevision,
    }, authority.authorize({ ...input, stage: "claim" }));
    const commandId = CommandId.make(`${original.commandId}:native:v2:create`);
    const messageStageId = CommandId.make(`${original.commandId}:native:v2:message`);
    const command = yield* Schema.decodeUnknownEffect(OrchestrationV2Command)({
      type: "thread.create", commandId, threadId: original.threadId, projectId: binding.project_id,
      title: original.bootstrap.createThread.title, modelSelection, runtimeMode: binding.runtime_mode,
      interactionMode: binding.interaction_mode, branch: resources.branch, worktreePath: resources.worktreePath,
      createdBy: "user", creationSource: "server",
    });
    if (command.type !== "thread.create") return yield* Effect.die("Expected native create fixture");
    const ids = yield* IdAllocator.IdAllocatorV2;
    const message = yield* Schema.decodeUnknownEffect(OrchestrationV2Command)({
      type: "message.dispatch", commandId: messageStageId, threadId: original.threadId,
      messageId: original.message.messageId, text: original.message.text, attachments: [], modelSelection,
      dispatchMode: { type: "defer_start" }, createdBy: "user", creationSource: "server",
    });
    const release = yield* Schema.decodeUnknownEffect(OrchestrationV2Command)({
      type: "prepared-run.release", commandId: original.commandId, threadId: original.threadId,
      runId: ids.derive.run({ threadId: ThreadId.make(original.threadId), ordinal: 1 }),
    });
    yield* repository.reserveCommandIdentities(claimId, [commandId, messageStageId, CommandId.make(original.commandId)]);
    for (const stage of [command, message, release]) yield* repository.reserveCommand(claimId, stage);
    yield* repository.recordNormalizedCommand(claimId, release);
    const reserved = yield* repository.getReservedCommand(commandId);
    yield* sql.unsafe(`CREATE TRIGGER native_stage_event_failure BEFORE INSERT ON orchestration_events
      WHEN NEW.command_id = '${commandId}' BEGIN SELECT RAISE(ABORT, 'injected native stage association failure'); END`);
    const outcome = yield* Effect.exit(orchestrator.dispatchNativeCreationStage(command, {
      ...input, claimId, stage: "native_command",
    }).pipe(
      Effect.provideService(EnvironmentAuthenticatedPrincipal, principal),
      Effect.provideService(NativeCreationAuthority, authority),
    ));
    assert.isTrue(Exit.isFailure(outcome));
    if (Exit.isFailure(outcome)) {
      assert.include(inspect(Cause.squash(outcome.cause), { depth: 8 }), "injected native stage association failure");
    }
    assert.deepEqual(yield* repository.getReservedCommand(commandId), reserved);
    assert.isTrue(Option.isSome(yield* repository.readHistory(original.commandId)));
    for (const table of ["orchestration_events", "orchestration_command_receipts", "orchestration_v2_native_command_identities", "orchestration_v2_effect_outbox"]) {
      const count = yield* sql.unsafe<{ count: number }>(`SELECT COUNT(*) AS count FROM ${table} WHERE command_id = '${commandId}'`);
      assert.equal(count[0]?.count, 0);
    }
    const projection = yield* sql<{ count: number }>`SELECT COUNT(*) AS count FROM orchestration_v2_projection_threads WHERE thread_id = ${command.threadId}`;
    assert.equal(projection[0]?.count, 0);
    assert.equal((yield* repository.readHistoryByClaim(claimId)).effectsV2.length, 0);
  }).pipe(Effect.provide(Layer.mergeAll(makeTestLayer(), nativeRepositoryLayer.pipe(Layer.provide(SqlitePersistenceMemory))))),
);
