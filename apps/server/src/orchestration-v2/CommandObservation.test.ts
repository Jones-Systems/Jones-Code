import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  ThreadId,
  OrchestrationV2ThreadProjection,
  OrchestrationV2ConversationMessage,
  NativeCommandObservationV2,
  NativeCommandObservationV2Json,
  NativeCreationObservationV2,
  type OrchestrationV2Run,
  type OrchestrationV2StoredEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { expect } from "vite-plus/test";
import {
  commandObservationFromNativeFactsV2,
  dispatchTargetFromNativeFactsV2,
  makeCommandObservationQuery,
  nativeCreationObservationFromHistoryV2,
} from "./CommandObservation.ts";
import { EventSinkV2, type NativeCommandFactsV2 } from "./EventSink.ts";
import { ProviderSessionManagerV2 } from "./ProviderSessionManager.ts";
import { ProviderRuntimeObservation } from "./ProviderAdapter.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import type { NativeCreationBoundedHistoryV2 } from "../persistence/Services/NativeCreationRepository.ts";

const now = DateTime.makeUnsafe("2026-10-02T12:00:00Z");
const threadId = ThreadId.make("native-observation-thread");
const commandId = CommandId.make("native-observation-command");
const messageId = MessageId.make("native-observation-message");
const runId = RunId.make("native-observation-run");
const instanceId = ProviderInstanceId.make("codex");
const input = { threadId, commandId, messageId };
const makeFacts = (): NativeCommandFactsV2 => {
  const projection = Schema.decodeUnknownSync(OrchestrationV2ThreadProjection)({
    thread: {
      id: threadId,
      projectId: ProjectId.make("native-observation-project"),
      createdBy: "user",
      creationSource: "web",
      title: "Observation",
      providerInstanceId: instanceId,
      modelSelection: { instanceId, model: "synthetic-model" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: null,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    },
    runs: [],
    attempts: [],
    nodes: [],
    subagents: [],
    providerSessions: [],
    providerThreads: [],
    providerTurns: [],
    runtimeRequests: [],
    messages: [],
    plans: [],
    turnItems: [],
    checkpointScopes: [],
    checkpoints: [],
    contextHandoffs: [],
    contextTransfers: [],
    visibleTurnItems: [],
    updatedAt: now,
  });
  const records: Record<string, ReadonlyArray<Readonly<Record<string, unknown>>>> = {};
  for (const name of [
    "threads",
    "runs",
    "run_attempts",
    "nodes",
    "provider_threads",
    "provider_turns",
    "runtime_requests",
    "messages",
    "plans",
    "turn_items",
    "checkpoint_scopes",
    "checkpoints",
    "context_handoffs",
    "context_transfers",
    "subagents",
    "provider_sessions",
    "session_bindings",
    "effects",
    "unknown_effect_holds",
    "launch_workflows",
    "runtime_evidence",
    "restart_continuations",
    "legacy_continuation",
    "source_runtime",
  ])
    records[name] = [];
  records.threads = [{ thread_id: threadId }];
  records.project = [{ project_id: "native-observation-project", deleted_at: null }];
  records.projection_schema = [{ schema_version: 2 }];
  const authorityRecords: Record<string, ReadonlyArray<Readonly<Record<string, unknown>>>> = {};
  for (const name of [
    "claims",
    "attempts",
    "effect_facts",
    "normalized_commands",
    "reserved_commands",
    "reserved_command_identities",
  ])
    authorityRecords[name] = [];
  const incarnation = { eventId: EventId.make("native-observation-birth"), sequence: 1 };
  return {
    ...input,
    receipt: null,
    identity: null,
    events: [],
    eventMetadata: [],
    eventMetadataOverflow: false,
    snapshotSequence: 5,
    targetEventSequence: 1,
    incarnation,
    creationProvenance: "native_created",
    projection,
    creationHistory: [],
    nativeCreationHistory: null,
    workstreamWitness: null,
    commitSnapshot: {
      commandId,
      threadId,
      targetEventSequence: 1,
      incarnation,
      creationProvenance: "native_created",
      records,
      authority: {},
      authorityRecords,
    },
  };
};
const makeRun = (id = runId): OrchestrationV2Run => ({
  id,
  threadId,
  ordinal: 1,
  providerInstanceId: instanceId,
  modelSelection: { instanceId, model: "synthetic-model" },
  providerThreadId: null,
  userMessageId: messageId,
  rootNodeId: null,
  activeAttemptId: null,
  status: "queued",
  requestedAt: now,
  startedAt: null,
  completedAt: null,
  checkpointId: null,
  contextHandoffId: null,
});
const acceptedFacts = (): NativeCommandFactsV2 => {
  const f = makeFacts();
  const message = Schema.decodeUnknownSync(OrchestrationV2ConversationMessage)({
    id: messageId,
    threadId,
    runId,
    nodeId: null,
    role: "user",
    createdBy: "user",
    creationSource: "web",
    text: "SYNTHETIC_PRIVATE_TEXT_NOT_HISTORY",
    attachments: [],
    streaming: false,
    createdAt: now,
    updatedAt: now,
  });
  const events: ReadonlyArray<OrchestrationV2StoredEvent> = [
    {
      sequence: 2,
      commandId,
      event: {
        id: EventId.make("native-message-event"),
        threadId,
        type: "message.updated",
        occurredAt: now,
        payload: message,
      },
    },
  ];
  return {
    ...f,
    receipt: {
      commandId,
      threadId,
      commandType: "message.dispatch",
      acceptedAt: now,
      resultSequence: 2,
      status: "accepted",
      error: null,
    },
    events,
    eventMetadata: [
      {
        eventId: events[0]!.event.id,
        commandId,
        aggregateKind: "thread",
        aggregateId: threadId,
        sequence: 2,
        type: "message.updated",
        occurredAt: DateTime.formatIso(now),
        applicationEventVersion: 2,
      },
    ],
    projection: { ...f.projection!, messages: [message], runs: [makeRun()] },
  };
};
const observe = (facts: NativeCommandFactsV2) =>
  commandObservationFromNativeFactsV2(input, facts, dispatchTargetFromNativeFactsV2(facts).target);

const historyCarrier = (): NativeCreationBoundedHistoryV2 => {
  const stageSeeds = [
    [
      CommandId.make(`${commandId}:native:v2:create`),
      "thread.create",
      EventId.make("native-observation-birth"),
      1,
    ],
    [
      CommandId.make(`${commandId}:native:v2:message`),
      "message.dispatch",
      EventId.make("native-message-event"),
      2,
    ],
    [commandId, "prepared-run.release", EventId.make("native-release-event"), 3],
  ] as const;
  const stageCommands: Array<NativeCreationBoundedHistoryV2["stageCommands"][number]> =
    stageSeeds.map(([id, type, eventId, sequence]) => ({
      claimId: "claim",
      commandId: id,
      threadId,
      commandType: type,
      commandDigest: "d".repeat(64),
      event: { eventId, sequence },
      receipt: {
        commandId: id,
        threadId,
        commandType: type,
        acceptedAt: now,
        resultSequence: sequence,
        status: "accepted" as const,
        error: null,
      },
    }));
  const lifecycle = ["normalization"].flatMap((action, index) => [
    {
      effectId: `lifecycle-${action}`,
      ordinal: index * 2,
      timestamp: DateTime.formatIso(now),
      kind: "lifecycle",
      phase: "started",
      threadId,
      action,
    },
    {
      effectId: `lifecycle-${action}`,
      ordinal: index * 2 + 1,
      timestamp: DateTime.formatIso(now),
      kind: "lifecycle",
      phase: "completed",
      threadId,
      action,
      result: "succeeded",
    },
  ]);
  const worktree = {
    effectId: "worktree",
    timestamp: DateTime.formatIso(now),
    kind: "worktree",
    projectCwd: "/synthetic/project",
    worktreePath: "/synthetic/worktree",
    branch: "native-branch",
    baseRef: "main",
    ownership: "created",
  };
  const decoded = Schema.decodeUnknownSync(NativeCreationObservationV2)({
    version: 2,
    schema: "t3.native-creation-observation/v2",
    preparationId: "preparation",
    operationId: "operation",
    preparationSha256: "a".repeat(64),
    bindingDigest: "b".repeat(64),
    promptDigest: "c".repeat(64),
    commandDigest: "e".repeat(64),
    normalizedCommandDigest: "d".repeat(64),
    claimId: "claim",
    claimedBootId: "boot",
    claimedAt: DateTime.formatIso(now),
    actorSessionId: "actor",
    grantId: "grant",
    grantRevision: 1,
    binding: {
      backendInstance: "backend",
      environmentId: "environment",
      projectId: "native-observation-project",
      projectCwd: "/synthetic/project",
      accountRef: "account",
      accountBindingId: "account-binding",
      accountBindingRevision: 1,
      providerModelSelection: { instanceId, model: "synthetic-model" },
      runtimeMode: "full-access",
      interactionMode: "default",
      baseBranch: "main",
      startFromOrigin: false,
      runSetupScript: false,
      requestedBranch: "native-branch",
    },
    incarnation: null,
    outcome: "unknown",
    overflow: false,
    unresolvedEffects: [],
    stageCommands,
    finalReceipt: stageCommands[2]!.receipt,
    effectsV1: [
      ...lifecycle,
      { ...worktree, ordinal: 2, phase: "started" },
      { ...worktree, ordinal: 3, phase: "completed", result: "succeeded" },
    ],
    effectsV2: stageCommands
      .filter((stage) => stage.commandType === "prepared-run.release")
      .flatMap((stage, index) => [
        {
          version: 2,
          kind: "native_command",
          effectId: `effect:${commandId}:provider-turn.start:${runId}`,
          ordinal: 4 + index * 2,
          timestamp: DateTime.formatIso(now),
          commandId: stage.commandId,
          threadId,
          commandType: stage.commandType,
          commandDigest: stage.commandDigest,
          phase: "started",
        },
        {
          version: 2,
          kind: "native_command",
          effectId: `effect:${commandId}:provider-turn.start:${runId}`,
          ordinal: 5 + index * 2,
          timestamp: DateTime.formatIso(now),
          commandId: stage.commandId,
          threadId,
          commandType: stage.commandType,
          commandDigest: stage.commandDigest,
          phase: "completed",
          ...stage.event,
        },
      ]),
  });
  const {
    version: _version,
    schema: _schema,
    incarnation: _incarnation,
    outcome: _outcome,
    ...safe
  } = decoded;
  return { ...safe, originalCommandId: commandId, threadId, messageId };
};

const historyFacts = (history = historyCarrier()): NativeCommandFactsV2 => {
  const f = makeFacts();
  const event = {
    id: EventId.make("native-release-event"),
    threadId,
    type: "run.updated" as const,
    occurredAt: now,
    payload: { ...makeRun(), status: "starting" as const },
  };
  return {
    ...f,
    receipt: history.finalReceipt,
    nativeCreationHistory: history,
    identity:
      history.finalReceipt === null
        ? null
        : {
            kind: "native_creation_stage",
            version: 2,
            commandId,
            commandType: "prepared-run.release",
            aggregateKind: "thread",
            aggregateId: threadId,
            normalizedCommandDigest: history.normalizedCommandDigest ?? "d".repeat(64),
            bindingDigest: history.bindingDigest,
          },
    creationHistory: [{ claim_id: "claim" }],
    events: [{ sequence: 3, commandId, event }],
    eventMetadata: [
      {
        eventId: event.id,
        commandId,
        aggregateKind: "thread",
        aggregateId: threadId,
        sequence: 3,
        type: event.type,
        occurredAt: DateTime.formatIso(now),
        applicationEventVersion: 2,
      },
    ],
  };
};

it("proves idle only for a unique native birth with complete empty attachment and start contributors", () => {
  const f = makeFacts();
  expect(dispatchTargetFromNativeFactsV2(f).target).toMatchObject({
    complete: true,
    idle: true,
    blockers: [],
  });
  const { runtime_evidence: _omitted, ...records } = f.commitSnapshot.records;
  const missing = dispatchTargetFromNativeFactsV2({
    ...f,
    commitSnapshot: { ...f.commitSnapshot, records },
  });
  expect(missing.target).toMatchObject({ complete: false, idle: false });
  expect(missing.target?.blockers).toContain("unknown_evidence");
  expect(
    dispatchTargetFromNativeFactsV2({ ...f, incarnation: null, creationProvenance: "unavailable" })
      .target?.idle,
  ).toBe(false);
  expect(
    dispatchTargetFromNativeFactsV2({ ...f, creationProvenance: "legacy_import" }).target?.idle,
  ).toBe(false);
});

it("holds persisted settlement until unsettle and blocks archived and deleted targets", () => {
  for (const [field, value, blocker] of [
    ["settledOverride", "settled", "settled"],
    ["archivedAt", now, "archived"],
    ["deletedAt", now, "deleted"],
  ] as const) {
    const f = makeFacts();
    const target = dispatchTargetFromNativeFactsV2({
      ...f,
      projection: { ...f.projection!, thread: { ...f.projection!.thread, [field]: value } },
    }).target;
    expect(target?.idle).toBe(false);
    expect(target?.blockers).toContain(blocker);
  }
  const f = makeFacts();
  expect(
    dispatchTargetFromNativeFactsV2({
      ...f,
      projection: {
        ...f.projection!,
        thread: { ...f.projection!.thread, settledOverride: "active" },
      },
    }).target?.idle,
  ).toBe(true);
});

it("blocks queued, held, preparing, starting, running and waiting runs", () => {
  for (const status of ["queued", "preparing", "starting", "running", "waiting"] as const) {
    const f = makeFacts();
    const target = dispatchTargetFromNativeFactsV2({
      ...f,
      projection: { ...f.projection!, runs: [{ ...makeRun(), status, queueHeld: true }] },
      commitSnapshot: {
        ...f.commitSnapshot,
        records: { ...f.commitSnapshot.records, runs: [{ status }] },
      },
    }).target;
    expect(target?.blockers).toContain(status === "queued" ? "queued_run" : "active_run");
    expect(target?.blockers).toContain("held_run");
    expect(target?.idle).toBe(false);
  }
});

it("keeps durable approval, user input, tool, auth and actionable plan blockers", () => {
  const f = makeFacts();
  const requests = ["user_input", "dynamic_tool_call", "auth_refresh", "command"].map(
    (kind, index) => ({
      id: `request-${index}`,
      nodeId: NodeId.make("request-node"),
      providerTurnId: null,
      nativeRequestRef: null,
      kind,
      status: "pending",
      responseCapability: { type: "not_resumable", reason: "fixture" },
      createdAt: now,
      resolvedAt: null,
    }),
  );
  const projection = Schema.decodeUnknownSync(OrchestrationV2ThreadProjection)({
    ...f.projection,
    runtimeRequests: requests,
    plans: [
      {
        id: "plan",
        threadId,
        runId: null,
        nodeId: "plan-node",
        kind: "proposed_plan",
        status: "active",
        markdown: "Plan",
      },
    ],
  });
  const target = dispatchTargetFromNativeFactsV2({ ...f, projection }).target;
  expect(target?.blockers).toEqual(
    expect.arrayContaining([
      "pending_approval",
      "pending_user_input",
      "pending_tool",
      "pending_auth_refresh",
      "actionable_plan",
    ]),
  );
  expect(target?.idle).toBe(false);
});

it("blocks native effects, unresolved starts, unknown holds and dormant or released restart claims", () => {
  const f = makeFacts();
  const target = dispatchTargetFromNativeFactsV2({
    ...f,
    commitSnapshot: {
      ...f.commitSnapshot,
      records: {
        ...f.commitSnapshot.records,
        effects: [{ status: "running" }],
        launch_workflows: [{ status: "preparing" }],
        unknown_effect_holds: [{ effect_id: "lost-effect" }],
        restart_continuations: [{ status: "dormant" }],
      },
    },
  }).target;
  expect(target?.blockers).toEqual(
    expect.arrayContaining([
      "pending_native_effect",
      "unresolved_start",
      "unknown_resume",
      "unknown_evidence",
    ]),
  );
});

it("blocks attempts, execution nodes, provider turns, subagents, background rosters and undelivered wakes", () => {
  const f = makeFacts();
  const projection = Schema.decodeUnknownSync(OrchestrationV2ThreadProjection)({
    ...f.projection,
    runs: [
      {
        ...makeRun(),
        status: "running",
        activeAttemptId: "attempt",
        delegatedCompletion: {
          disposition: "open",
          nextGeneration: 2,
          delivery: { generation: 1, messageId: "completion-message", taskIds: ["agent-node"] },
        },
      },
    ],
    attempts: [
      {
        id: "attempt",
        runId,
        attemptOrdinal: 1,
        rootNodeId: "root-node",
        providerInstanceId: instanceId,
        providerThreadId: "provider-thread",
        providerTurnId: "provider-turn",
        reason: "initial",
        status: "running",
        startedAt: now,
        completedAt: null,
      },
    ],
    nodes: [
      {
        id: "root-node",
        threadId,
        runId,
        parentNodeId: null,
        rootNodeId: "root-node",
        kind: "root_turn",
        status: "running",
        countsForRun: true,
        providerThreadId: "provider-thread",
        providerTurnId: "provider-turn",
        nativeItemRef: null,
        runtimeRequestId: null,
        checkpointScopeId: null,
        startedAt: now,
        completedAt: null,
      },
    ],
    providerTurns: [
      {
        id: "provider-turn",
        providerThreadId: "provider-thread",
        nodeId: "root-node",
        runAttemptId: "attempt",
        nativeTurnRef: null,
        ordinal: 1,
        status: "pending",
        startedAt: null,
        completedAt: null,
      },
    ],
    providerThreads: [
      {
        id: "provider-thread",
        driver: "codex",
        providerInstanceId: instanceId,
        providerSessionId: null,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: null,
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        pendingBackgroundTasks: [{ taskId: "background-command", kind: "command" }],
        createdAt: now,
        updatedAt: now,
      },
    ],
    subagents: [
      {
        id: "agent-node",
        threadId,
        runId,
        parentNodeId: "root-node",
        origin: "app_owned",
        createdBy: "user",
        driver: "codex",
        providerInstanceId: instanceId,
        providerThreadId: null,
        childThreadId: null,
        nativeTaskRef: null,
        prompt: "Task",
        title: null,
        model: null,
        status: "running",
        completionDelivery: { state: "claimed", observedByRunId: null },
        result: null,
        startedAt: now,
        completedAt: null,
        updatedAt: now,
      },
    ],
  });
  const current = dispatchTargetFromNativeFactsV2({ ...f, projection });
  expect(current.target?.blockers).toEqual(
    expect.arrayContaining([
      "active_attempt",
      "execution_node",
      "provider_turn",
      "subagent_work",
      "background_work",
      "completion_delivery",
      "wake_delivery",
      "unknown_evidence",
    ]),
  );
  expect(current.target?.blockers).not.toContain("provider_activity");
  expect(current.target?.idle).toBe(false);
});

it("retains positive runtime work but never treats a resumed root-only idle probe as complete coverage", () => {
  const f = makeFacts();
  const binding = {
    threadId,
    providerThreadId: ProviderThreadId.make("provider-thread"),
    providerSessionId: "provider-session",
    instanceId,
    runtimeGeneration: "current-generation",
  };
  const projection = Schema.decodeUnknownSync(OrchestrationV2ThreadProjection)({
    ...f.projection,
    thread: { ...f.projection!.thread, activeProviderThreadId: binding.providerThreadId },
    providerSessions: [
      {
        id: binding.providerSessionId,
        driver: "codex",
        providerInstanceId: instanceId,
        status: "ready",
        cwd: "/synthetic",
        model: "synthetic-model",
        capabilities: CodexProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      },
    ],
    providerThreads: [
      {
        id: binding.providerThreadId,
        driver: "codex",
        providerInstanceId: instanceId,
        providerSessionId: binding.providerSessionId,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: null,
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: null,
        lastRunOrdinal: null,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      },
    ],
  });
  const resumed = {
    ...f,
    projection,
    commitSnapshot: {
      ...f.commitSnapshot,
      records: {
        ...f.commitSnapshot.records,
        runtime_evidence: [
          {
            thread_id: threadId,
            provider_thread_id: binding.providerThreadId,
            provider_session_id: binding.providerSessionId,
            provider_instance_id: instanceId,
            driver: "codex",
            native_thread_id: null,
            runtime_generation: "current-generation",
            evidence_revision: 1,
            registered_at: DateTime.formatIso(now),
          },
        ],
      },
    },
  };
  for (const status of ["working", "busy", "monitoring", "idle"] as const) {
    const observed = Schema.decodeUnknownSync(ProviderRuntimeObservation)({
      status,
      binding,
      observedAt: "2026-10-02T12:00:01Z",
    });
    const current = dispatchTargetFromNativeFactsV2(resumed, observed);
    expect(current.target).toMatchObject({ complete: false, idle: false });
    expect(current.target?.blockers).toContain("unknown_evidence");
    if (status === "working" || status === "busy")
      expect(current.target?.blockers).toContain("provider_activity");
    if (status === "monitoring") expect(current.target?.blockers).toContain("background_work");
    expect(current.runtimeReason).toBe("native_activity_coverage_incomplete");
  }
  const unrelated = Schema.decodeUnknownSync(ProviderRuntimeObservation)({
    status: "working",
    binding: { ...binding, threadId: "unrelated-thread" },
    observedAt: "2026-10-02T12:00:01Z",
  });
  const mismatched = dispatchTargetFromNativeFactsV2(resumed, unrelated);
  expect(mismatched.target?.blockers).not.toContain("provider_activity");
  expect(mismatched.runtimeReason).toBe("native_activity_binding_mismatch");
});

it("correlates an ordinary accepted message without native identity or settlement, even after a newer run", () => {
  const f = acceptedFacts();
  const newer = {
    ...makeRun(RunId.make("newer-run")),
    ordinal: 2,
    userMessageId: MessageId.make("newer-message"),
  };
  const observation = observe({
    ...f,
    projection: { ...f.projection!, runs: [...f.projection!.runs, newer] },
  });
  expect(observation).toMatchObject({
    commandStatus: "accepted",
    identityVerification: "unbound",
    correlation: "exact",
    correlatedMessageId: messageId,
    run: { runId, runAttemptId: null },
  });
  expect(observation.target?.latestRunId).toBe(newer.id);
  Schema.decodeUnknownSync(NativeCommandObservationV2)(observation);
  const wire = Schema.encodeSync(NativeCommandObservationV2Json)(observation);
  expect(wire.receipt?.acceptedAt).toBe("2026-10-02T12:00:00.000Z");
});

it("requires original command event binding even when message and run projections exist", () => {
  const f = acceptedFacts();
  expect(observe({ ...f, receipt: null, events: [], eventMetadata: [] })).toMatchObject({
    commandStatus: "not_found",
    correlation: "missing",
    run: null,
  });
  expect(observe({ ...f, events: [], eventMetadata: [] })).toMatchObject({
    commandStatus: "accepted",
    correlation: "pending",
    run: null,
  });
  expect(observe({ ...f, projection: { ...f.projection!, runs: [] } })).toMatchObject({
    commandStatus: "accepted",
    correlation: "pending",
    run: null,
  });
});

it("uses actual run-attempt/provider identities and holds missing or contradictory attempt ownership", () => {
  const f = acceptedFacts();
  const projection = Schema.decodeUnknownSync(OrchestrationV2ThreadProjection)({
    ...f.projection,
    runs: [{ ...makeRun(), activeAttemptId: "attempt", providerThreadId: "provider-thread" }],
    attempts: [
      {
        id: "attempt",
        runId,
        attemptOrdinal: 1,
        rootNodeId: "root-node",
        providerInstanceId: instanceId,
        providerThreadId: "provider-thread",
        providerTurnId: "provider-turn",
        reason: "initial",
        status: "completed",
        startedAt: now,
        completedAt: now,
      },
    ],
  });
  expect(observe({ ...f, projection })).toMatchObject({
    correlation: "exact",
    run: {
      runId,
      runAttemptId: "attempt",
      providerThreadId: "provider-thread",
      providerTurnId: "provider-turn",
    },
  });
  expect(observe({ ...f, projection: { ...projection, attempts: [] } })).toMatchObject({
    correlation: "pending",
    run: null,
  });
  expect(
    observe({
      ...f,
      projection: { ...projection, attempts: [projection.attempts[0]!, projection.attempts[0]!] },
    }).correlation,
  ).toBe("ambiguous");
  expect(
    observe({
      ...f,
      projection: {
        ...projection,
        attempts: [
          {
            ...projection.attempts[0]!,
            providerThreadId: ProviderThreadId.make("foreign-provider-thread"),
          },
        ],
      },
    }).correlation,
  ).toBe("mismatched");
});

it("detects foreign domains, wrong messages, duplicate matches and non-final or overflowing event evidence", () => {
  const f = acceptedFacts();
  expect(
    observe({ ...f, receipt: { ...f.receipt!, threadId: ThreadId.make("foreign") } }).correlation,
  ).toBe("mismatched");
  expect(
    commandObservationFromNativeFactsV2({ ...input, messageId: MessageId.make("wrong") }, f, null)
      .correlation,
  ).toBe("mismatched");
  expect(
    observe({
      ...f,
      projection: { ...f.projection!, runs: [makeRun(), makeRun(RunId.make("duplicate-run"))] },
    }).correlation,
  ).toBe("ambiguous");
  expect(observe({ ...f, receipt: { ...f.receipt!, resultSequence: 3 } }).correlation).not.toBe(
    "exact",
  );
  expect(observe({ ...f, eventMetadataOverflow: true })).toMatchObject({
    correlation: "pending",
    snapshot: { complete: false },
  });
});

it("separates native companion verification from command acceptance and message correlation", () => {
  const f = acceptedFacts();
  const identity = {
    kind: "guarded_message_dispatch" as const,
    version: 2 as const,
    commandId,
    commandType: "message.dispatch",
    aggregateKind: "thread" as const,
    aggregateId: threadId,
    normalizedCommandDigest: "a".repeat(64),
    bindingDigest: "b".repeat(64),
  };
  expect(observe({ ...f, identity }).identityVerification).toBe("verified");
  expect(observe({ ...f, identity, receipt: null }).identityVerification).toBe("unknown");
  expect(
    observe({ ...f, identity: { ...identity, aggregateId: ThreadId.make("foreign") } })
      .identityVerification,
  ).toBe("mismatched");
});

it("keeps a missing normalized V2 digest null and reports claimed metadata as unknown", () => {
  const history = {
    ...historyCarrier(),
    normalizedCommandDigest: null,
    stageCommands: [],
    effectsV1: [],
    effectsV2: [],
    finalReceipt: null,
  };
  const observation = observe({ ...makeFacts(), nativeCreationHistory: history });
  expect(observation).toMatchObject({
    commandStatus: "not_found",
    creation: {
      commandDigest: "e".repeat(64),
      normalizedCommandDigest: null,
      outcome: "unknown",
      incarnation: null,
    },
  });
});

it("normalized intent with no creation chain stays in progress and never exposes original bytes", () => {
  const history = {
    ...historyCarrier(),
    stageCommands: [],
    effectsV1: [],
    effectsV2: [],
    finalReceipt: null,
    canonicalPreparation: "PRIVATE_PREPARATION",
    canonicalCommand: "PRIVATE_COMMAND",
    intent: { text: "PRIVATE_INTENT" },
  };
  const observation = observe({
    ...makeFacts(),
    projection: acceptedFacts().projection,
    nativeCreationHistory: history,
  });
  expect(observation.creation).toMatchObject({
    outcome: "in_progress",
    incarnation: null,
    effectsV1: [],
    effectsV2: [],
  });
  const encoded = JSON.stringify(Schema.encodeSync(NativeCommandObservationV2Json)(observation));
  for (const privateValue of [
    "SYNTHETIC_PRIVATE_TEXT_NOT_HISTORY",
    "PRIVATE_PREPARATION",
    "PRIVATE_COMMAND",
    "PRIVATE_INTENT",
    "canonicalPreparation",
    "canonicalCommand",
    "originalCommandId",
  ])
    expect(encoded).not.toContain(privateValue);
});

it("unresolved external starts stay unknown without receipts, replay, or regenerated identity", () => {
  const history: NativeCreationBoundedHistoryV2 = {
    ...historyCarrier(),
    finalReceipt: null,
    stageCommands: [],
    effectsV2: [],
    effectsV1: [
      {
        effectId: "lost-fetch",
        ordinal: 0,
        timestamp: DateTime.formatIso(now),
        kind: "fetch",
        phase: "started",
        projectCwd: "/synthetic/project",
        baseRef: "main",
      },
    ],
    unresolvedEffects: ["lost-fetch"],
  };
  const observation = observe({ ...makeFacts(), nativeCreationHistory: history });
  expect(observation).toMatchObject({
    commandStatus: "not_found",
    identityVerification: "missing",
    correlation: "missing",
    run: null,
    creation: { outcome: "unknown", unresolvedEffects: ["lost-fetch"] },
  });
  expect(observation.target).toMatchObject({ complete: false, idle: false });
});

it.effect(
  "command completion facts without matching native event and receipt cannot attest creation",
  () => {
    const history = historyCarrier();
    return Effect.gen(function* () {
      const query = yield* makeCommandObservationQuery();
      const observation = yield* query.observe(input);
      expect(observation).toMatchObject({
        commandStatus: "not_found",
        identity: null,
        creation: { outcome: "unknown" },
      });
      const legacy = yield* query.observeLegacy(input).pipe(Effect.flip);
      expect(legacy).toMatchObject({ reason: "observation_unsupported" });
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.mock(EventSinkV2)({
            readNativeCommandFacts: () =>
              Effect.succeed({
                ...historyFacts(history),
                receipt: null,
                identity: null,
                events: [],
                eventMetadata: [],
                nativeCreationHistory: {
                  ...history,
                  finalReceipt: null,
                  stageCommands: history.stageCommands.map((stage) => ({
                    ...stage,
                    receipt: null,
                    event: null,
                  })),
                },
              }),
          }),
          Layer.mock(ProviderSessionManagerV2)({
            observeThreadRuntime: () =>
              Effect.die("Historical reads must not start or recover a runtime."),
          }),
        ),
      ),
    );
  },
);

it("retains accepted stage and final receipt metadata without treating them as complete preparation", () => {
  const history = historyCarrier();
  const observation = observe(historyFacts(history));
  expect(observation.creation).toMatchObject({
    stageCommands: history.stageCommands,
    finalReceipt: history.finalReceipt,
  });
  expect(observation.creation?.outcome).not.toBe("complete");
  expect(
    observation.creation?.effectsV2.every(
      (effect) => effect.commandType === "prepared-run.release",
    ),
  ).toBe(true);
  const wire = Schema.encodeSync(NativeCommandObservationV2Json)(observation);
  expect(wire.creation?.finalReceipt?.acceptedAt).toBe("2026-10-02T12:00:00.000Z");
  expect(wire.creation?.stageCommands[2]?.receipt?.acceptedAt).toBe("2026-10-02T12:00:00.000Z");
  expect(
    observe({ ...historyFacts(history), commandId: CommandId.make("cross-query") }).creation
      ?.outcome,
  ).toBe("unknown");
  expect(
    observe({
      ...historyFacts(history),
      incarnation: { eventId: EventId.make("replacement-birth"), sequence: 4 },
    }).creation?.outcome,
  ).toBe("unknown");
  const unstarted = { ...history, effectsV2: [] };
  expect(observe(historyFacts(unstarted)).creation).toMatchObject({
    outcome: "in_progress",
    effectsV2: [],
  });
  const started = {
    ...history,
    effectsV2: history.effectsV2.filter((effect) => effect.phase === "started"),
  };
  expect(observe(historyFacts(started)).creation?.outcome).toBe("unknown");
});

it("associates only the original command or verified stage with the exact thread and message", () => {
  const history = historyCarrier();
  const f = historyFacts(history);
  expect(
    commandObservationFromNativeFactsV2(
      { ...input, commandId: CommandId.make("unrelated") },
      f,
      null,
    ),
  ).not.toHaveProperty("creation");
  expect(
    commandObservationFromNativeFactsV2(
      { ...input, messageId: MessageId.make("unrelated") },
      f,
      null,
    ),
  ).not.toHaveProperty("creation");
  expect(
    commandObservationFromNativeFactsV2(
      { ...input, threadId: ThreadId.make("unrelated") },
      f,
      null,
    ),
  ).not.toHaveProperty("creation");
  const stage = history.stageCommands[1]!;
  const stageInput = { ...input, commandId: stage.commandId };
  const stageFacts = {
    ...f,
    commandId: stage.commandId,
    receipt: stage.receipt,
    identity: {
      ...f.identity!,
      commandId: stage.commandId,
      commandType: stage.commandType,
      normalizedCommandDigest: stage.commandDigest,
    },
    events: acceptedFacts().events.map((stored) => ({ ...stored, commandId: stage.commandId })),
    eventMetadata: [
      {
        eventId: stage.event!.eventId,
        sequence: stage.event!.sequence,
        commandId: stage.commandId,
        aggregateKind: "thread" as const,
        aggregateId: threadId,
        type: "message.updated",
        occurredAt: DateTime.formatIso(now),
        applicationEventVersion: 2,
      },
    ],
  };
  expect(nativeCreationObservationFromHistoryV2(stageInput, stageFacts, history)).toMatchObject({
    stageCommands: history.stageCommands,
    finalReceipt: history.finalReceipt,
  });
  const foreignStage = { ...stage, commandId: CommandId.make("unverified-stage") };
  expect(
    nativeCreationObservationFromHistoryV2({ ...input, commandId: foreignStage.commandId }, f, {
      ...history,
      stageCommands: [foreignStage],
    }),
  ).toBeUndefined();
});

it("preserves V1 command facts separately and rejects incomplete or conflicting V2 attribution", () => {
  const history = historyCarrier();
  const f = historyFacts(history);
  const v1 = {
    kind: "native_command" as const,
    commandId,
    threadId,
    commandType: "thread.turn.start" as const,
    commandDigest: "e".repeat(64),
    timestamp: DateTime.formatIso(now),
    effectId: "v1-final",
  };
  const legacyHistory: NativeCreationBoundedHistoryV2 = {
    ...history,
    effectsV2: [],
    effectsV1: [
      { ...v1, ordinal: 0, phase: "started" },
      { ...v1, ordinal: 1, phase: "completed", eventId: EventId.make("v1-event"), sequence: 3 },
    ],
  };
  expect(observe({ ...f, nativeCreationHistory: legacyHistory }).creation).toMatchObject({
    effectsV1: legacyHistory.effectsV1,
    effectsV2: [],
    outcome: "unknown",
  });
  for (const variant of [
    { ...history, effectsV2: history.effectsV2.filter((effect) => effect.phase === "completed") },
    { ...history, finalReceipt: { ...history.finalReceipt!, resultSequence: 4 } },
    { ...history, normalizedCommandDigest: "a".repeat(64) },
    {
      ...history,
      stageCommands: history.stageCommands.map((stage) =>
        stage.commandType === "prepared-run.release"
          ? { ...stage, event: { eventId: EventId.make("unmatched-event"), sequence: 3 } }
          : stage,
      ),
    },
    {
      ...history,
      effectsV2: history.effectsV2.map((effect) => ({ ...effect, commandDigest: "a".repeat(64) })),
    },
  ])
    expect(observe({ ...f, nativeCreationHistory: variant }).creation?.outcome).toBe("unknown");
  expect(observe({ ...f, identity: null }).creation?.outcome).toBe("unknown");
  expect(
    observe({ ...f, identity: { ...f.identity!, bindingDigest: "a".repeat(64) } }).creation
      ?.outcome,
  ).toBe("unknown");
});

it("bounds public history and makes every actual or defensive overflow unknown", () => {
  const history = historyCarrier();
  const f = historyFacts(history);
  for (const variant of [
    { ...history, overflow: true },
    { ...history, effectsV1: Array.from({ length: 257 }, () => history.effectsV1[0]!) },
    { ...history, effectsV2: Array.from({ length: 257 }, () => history.effectsV2[0]!) },
    {
      ...history,
      unresolvedEffects: Array.from({ length: 257 }, (_, index) => `unresolved-${index}`),
    },
    { ...history, stageCommands: [...history.stageCommands, history.stageCommands[0]!] },
  ]) {
    const creation = observe({ ...f, nativeCreationHistory: variant }).creation!;
    expect(creation).toMatchObject({ overflow: true, outcome: "unknown" });
    expect(creation.effectsV1.length).toBeLessThanOrEqual(256);
    expect(creation.effectsV2.length).toBeLessThanOrEqual(256);
    expect(creation.unresolvedEffects.length).toBeLessThanOrEqual(256);
    expect(creation.stageCommands.length).toBeLessThanOrEqual(3);
    Schema.decodeUnknownSync(NativeCreationObservationV2)(creation);
  }
});

it.effect(
  "legacy adapters retain only genuine original not-found or rejected facts and fabricate no V1 target",
  () =>
    Effect.gen(function* () {
      const query = yield* makeCommandObservationQuery();
      const observation = yield* query.observeLegacy(input);
      assert.deepEqual(observation, {
        ...input,
        snapshotSequence: 5,
        commandStatus: "not_found",
        acceptedSequence: null,
        correlation: "missing",
        turn: null,
        target: null,
      });
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.mock(EventSinkV2)({
            readNativeCommandFacts: () => {
              const facts = makeFacts();
              return Effect.succeed({
                ...facts,
                creationProvenance: "legacy_import",
                commitSnapshot: {
                  ...facts.commitSnapshot,
                  creationProvenance: "legacy_import",
                },
              });
            },
          }),
          Layer.mock(ProviderSessionManagerV2)({}),
        ),
      ),
    ),
);

it.effect(
  "retains a genuinely imported original rejected V1 receipt without producing a V1 target",
  () =>
    Effect.gen(function* () {
      const query = yield* makeCommandObservationQuery();
      expect(yield* query.observeLegacy(input)).toEqual({
        ...input,
        snapshotSequence: 5,
        commandStatus: "rejected",
        acceptedSequence: null,
        correlation: "missing",
        turn: null,
        target: null,
      });
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.mock(EventSinkV2)({
            readNativeCommandFacts: () => {
              const facts = makeFacts();
              return Effect.succeed({
                ...facts,
                creationProvenance: "legacy_import",
                commitSnapshot: {
                  ...facts.commitSnapshot,
                  creationProvenance: "legacy_import",
                },
                receipt: {
                  commandId,
                  threadId,
                  commandType: "thread.turn.start",
                  acceptedAt: now,
                  resultSequence: 5,
                  status: "rejected",
                  error: "dispatch_guard_rejected",
                },
              });
            },
          }),
          Layer.mock(ProviderSessionManagerV2)({}),
        ),
      ),
    ),
);
