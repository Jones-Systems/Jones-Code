import { it } from "@effect/vitest";
import {
  AuthSessionId, CommandId, EventId, MessageId, ProviderInstanceId, ProviderThreadId,
  ProviderSessionId, RunId, RunAttemptId, ThreadId,
  OrchestrationV2Command, ThreadTurnDispatchGuardV2,
  type OrchestrationDispatchTargetV2,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { expect } from "vite-plus/test";
import {
  assertNativeCommandReplayV2, makeDispatchGuard, makeGuardedCommandIdentityV2, nativeCommandCanonicalJsonV2,
  validateDispatchGuardTargetV2,
} from "./DispatchGuard.ts";
import { EventSinkV2, type NativeCommandFactsV2 } from "./EventSink.ts";
import { ProviderSessionManagerV2 } from "./ProviderSessionManager.ts";

const now = DateTime.makeUnsafe("2026-10-02T12:00:00Z");
const threadId = ThreadId.make("guard-target");
const commandId = CommandId.make("guard-command");
const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "synthetic-model" };
const incarnation = { eventId: EventId.make("guard-birth"), sequence: 3 };
const guard: ThreadTurnDispatchGuardV2 = {
  version: 2, observedSnapshotSequence: 10, expectedIncarnation: incarnation,
  expectedModelSelection: modelSelection, expectedActiveRunId: null, expectedLatestRunId: null,
  expectedActiveRunAttemptId: null, expectedActiveProviderThreadId: null,
  expectedProviderSessionId: null, expectedProviderSessionStatus: null, requireIdle: true,
};
const command = (overrides: Readonly<Record<string, unknown>> = {}) => {
  const decoded = Schema.decodeUnknownSync(OrchestrationV2Command)({
    type: "message.dispatch", commandId, threadId, messageId: MessageId.make("guard-message"),
    text: "Bounded task", attachments: [], createdBy: "user", creationSource: "web",
    dispatchMode: { type: "start_immediately" }, ...overrides,
  });
  if (decoded.type !== "message.dispatch") throw new Error("Invalid message fixture.");
  return decoded;
};
const target: OrchestrationDispatchTargetV2 = {
  incarnation, modelSelection, activeRunId: null, latestRunId: null, activeRunAttemptId: null,
  activeProviderThreadId: null, providerSessionId: null, providerSessionStatus: null,
  snapshotSequence: 10, targetEventSequence: 3, complete: true, requireIdle: true, idle: true, blockers: [],
};
const facts = (): NativeCommandFactsV2 => ({
  commandId, threadId, receipt: null, identity: null, events: [], eventMetadata: [], eventMetadataOverflow: false,
  snapshotSequence: 10, targetEventSequence: 3, incarnation, creationProvenance: "native_created",
  projection: null, creationHistory: [], nativeCreationHistory: null,
  commitSnapshot: { commandId, threadId, targetEventSequence: 3, incarnation,
    creationProvenance: "native_created", records: {}, authority: {}, authorityRecords: {} },
});
const binding = { actorSessionId: AuthSessionId.make("guard-actor"), incarnation };
const rejection = (nextTarget: OrchestrationDispatchTargetV2, nextGuard = guard, nextCommand = command()) =>
  validateDispatchGuardTargetV2(nextCommand, nextGuard, { facts: facts(), target: nextTarget }).pipe(Effect.flip);

it.effect("allows unrelated global watermark changes and rejects only a newer target watermark", () =>
  Effect.gen(function* () {
    yield* validateDispatchGuardTargetV2(command(), guard, {
      facts: { ...facts(), snapshotSequence: 15 }, target: { ...target, snapshotSequence: 15 },
    });
    const stale = yield* validateDispatchGuardTargetV2(command(), guard, {
      facts: { ...facts(), snapshotSequence: 15, targetEventSequence: 11 }, target,
    }).pipe(Effect.flip);
    expect(stale.reason).toBe("stale_target");
    const future = yield* validateDispatchGuardTargetV2(command(), { ...guard, observedSnapshotSequence: 16 }, {
      facts: { ...facts(), snapshotSequence: 15 }, target,
    }).pipe(Effect.flip);
    expect(future.reason).toBe("future_snapshot");
  }),
);

it.effect("rejects missing targets and unsupported bootstrap or deferred operations", () =>
  Effect.gen(function* () {
    expect((yield* validateDispatchGuardTargetV2(command(), guard, { facts: facts(), target: null }).pipe(Effect.flip)).reason)
      .toBe("missing_target");
    const create = Schema.decodeUnknownSync(OrchestrationV2Command)({
      type: "thread.create", commandId, threadId, projectId: "project", title: "Bootstrap",
      modelSelection, runtimeMode: "full-access", interactionMode: "default", branch: null, worktreePath: null,
      createdAt: now, createdBy: "user", creationSource: "web",
    });
    expect((yield* validateDispatchGuardTargetV2(create, guard, { facts: facts(), target }).pipe(Effect.flip)).reason)
      .toBe("unsupported_operation");
    expect((yield* rejection(target, guard, command({ dispatchMode: { type: "defer_start" } }))).reason)
      .toBe("unsupported_operation");
    expect(Schema.decodeUnknownResult(ThreadTurnDispatchGuardV2)({ ...guard, requireIdle: false })._tag)
      .toBe("Failure");
  }),
);

it.effect("reads guarded targets through the coherent store port without opening or recovering a provider", () =>
  Effect.gen(function* () {
    const validate = yield* makeDispatchGuard();
    const error = yield* validate(command(), guard).pipe(Effect.flip);
    expect(error).toMatchObject({ reason: "missing_target" });
  }).pipe(Effect.provide(Layer.mergeAll(
    Layer.mock(EventSinkV2)({ readNativeCommandFacts: () => Effect.succeed(facts()) }),
    Layer.mock(ProviderSessionManagerV2)({ observeThreadRuntime: () => Effect.die("A missing target must not probe a provider.") }),
  ))),
);

it.effect("compares full current model and options while allowing a requested model on the observed instance", () =>
  Effect.gen(function* () {
    expect((yield* rejection({ ...target, modelSelection: { ...modelSelection, model: "changed" } })).reason).toBe("binding_mismatch");
    expect((yield* rejection({ ...target, modelSelection: { ...modelSelection,
      options: [{ id: "reasoningEffort", value: "high" }] } })).reason).toBe("binding_mismatch");
    yield* validateDispatchGuardTargetV2(command({ modelSelection: { ...modelSelection,
      model: "new-model", options: [{ id: "reasoningEffort", value: "medium" }] } }), guard, { facts: facts(), target });
    expect((yield* rejection(target, guard, command({ modelSelection: {
      ...modelSelection, instanceId: ProviderInstanceId.make("other-account"),
    } }))).reason).toBe("binding_mismatch");
  }),
);

it.effect("compares incarnation, active/latest run, attempt, provider thread, session/status and exact string generation", () =>
  Effect.gen(function* () {
    const alternatives: ReadonlyArray<OrchestrationDispatchTargetV2> = [
      { ...target, incarnation: { ...incarnation, eventId: EventId.make("replacement-birth") } },
      { ...target, incarnation: { ...incarnation, sequence: 4 } },
      { ...target, incarnation: null },
      { ...target, activeRunId: RunId.make("active-run") },
      { ...target, latestRunId: RunId.make("latest-run") },
      { ...target, activeRunAttemptId: RunAttemptId.make("attempt") },
      { ...target, activeProviderThreadId: ProviderThreadId.make("provider-thread") },
      { ...target, providerSessionId: ProviderSessionId.make("provider-session") },
      { ...target, providerSessionStatus: "starting" },
      { ...target, runtimeGeneration: "generation-a" },
    ];
    for (const candidate of alternatives) expect((yield* rejection(candidate)).reason).toBe("binding_mismatch");
    const nextGuard = { ...guard, expectedRuntimeGeneration: "generation-a" };
    yield* validateDispatchGuardTargetV2(command(), nextGuard, { facts: facts(), target: { ...target, runtimeGeneration: "generation-a" } });
    expect((yield* rejection({ ...target, runtimeGeneration: "generation-b" }, nextGuard)).reason).toBe("binding_mismatch");
    expect((yield* rejection(target, nextGuard)).reason).toBe("binding_mismatch");
  }),
);

it.effect("rejects every positive idle blocker and incomplete current runtime evidence", () =>
  Effect.gen(function* () {
    const blockers: OrchestrationDispatchTargetV2["blockers"] = [
      "archived", "deleted", "settled", "queued_run", "held_run", "active_run", "active_attempt",
      "provider_turn", "execution_node", "provider_activity", "pending_approval", "pending_user_input",
      "pending_tool", "pending_auth_refresh", "actionable_plan", "subagent_work", "background_work",
      "completion_delivery", "wake_delivery", "pending_native_effect", "unknown_resume", "unresolved_start",
    ];
    for (const blocker of blockers) {
      const rejected = yield* rejection({ ...target, idle: false, blockers: [blocker] });
      expect(rejected.reason).toBe("busy");
      expect(rejected.detail).toContain(blocker);
    }
    const unknown = yield* validateDispatchGuardTargetV2(command(), guard, {
      facts: facts(), target: { ...target, complete: false, idle: false, blockers: ["unknown_evidence"] },
      runtimeReason: "native_activity_coverage_incomplete",
    }).pipe(Effect.flip);
    expect(unknown).toMatchObject({ reason: "unknown_evidence", detail: "native_activity_coverage_incomplete" });
  }),
);

it("canonicalizes property order and UTC values without losing array order or omitted/null/undefined bindings", () => {
  expect(nativeCommandCanonicalJsonV2({ z: 1, a: now })).toBe(nativeCommandCanonicalJsonV2({ a: DateTime.makeUnsafe("2026-10-02T08:00:00-04:00"), z: 1 }));
  expect(nativeCommandCanonicalJsonV2({})).not.toBe(nativeCommandCanonicalJsonV2({ value: undefined }));
  expect(nativeCommandCanonicalJsonV2({ value: undefined })).not.toBe(nativeCommandCanonicalJsonV2({ value: null }));
  expect(nativeCommandCanonicalJsonV2(["a", "b"])).not.toBe(nativeCommandCanonicalJsonV2(["b", "a"]));
});

it("identifies every effect-bearing decoded command and guard input plus trusted resource/association bindings", () => {
  const original = makeGuardedCommandIdentityV2(command(), guard, binding);
  const variants = [
    command({ text: "Changed" }),
    command({ messageId: MessageId.make("changed-message") }),
    command({ attachments: [{ type: "file", id: "file-1", name: "fixture.txt", mimeType: "text/plain", sizeBytes: 1 }] }),
    command({ context: { version: 1, records: [] } }),
    command({ titleSeed: "Changed title" }),
    command({ modelSelection: { ...modelSelection, options: [{ id: "reasoningEffort", value: "high" }] } }),
    command({ sourcePlanRef: { threadId: "source-thread", planId: "source-plan" } }),
    command({ senderThreadId: "sender-thread" }),
    command({ deliveryIntent: "auto" }),
    command({ dispatchMode: { type: "queue_after_active" } }),
    command({ creationSource: "mcp" }),
  ];
  for (const candidate of variants)
    expect(makeGuardedCommandIdentityV2(candidate, guard, binding).normalizedCommandDigest).not.toBe(original.normalizedCommandDigest);
  expect(makeGuardedCommandIdentityV2(command(), { ...guard, observedSnapshotSequence: 9 }, binding).normalizedCommandDigest)
    .not.toBe(original.normalizedCommandDigest);
  for (const nextBinding of [
    { ...binding, actorSessionId: AuthSessionId.make("other-actor") },
    { ...binding, incarnation: { ...incarnation, sequence: 4 } },
    { ...binding, resourcePaths: ["/one", "/two"] },
    { ...binding, association: { namespace: "qualified", ownerId: "owner" } },
  ]) expect(makeGuardedCommandIdentityV2(command(), guard, nextBinding).bindingDigest).not.toBe(original.bindingDigest);
});

it.effect("replays exact native identity before checking a now-busy guard and rejects changed or stripped retries", () =>
  Effect.gen(function* () {
    const identity = makeGuardedCommandIdentityV2(command(), guard, binding);
    const accepted = { ...facts(), identity,
      receipt: { commandId, threadId, commandType: "message.dispatch", acceptedAt: now,
        resultSequence: 11, status: "accepted" as const, error: null } };
    yield* assertNativeCommandReplayV2(accepted, identity);
    expect((yield* rejection({ ...target, idle: false, blockers: ["queued_run"] })).reason).toBe("busy");
    expect((yield* assertNativeCommandReplayV2(accepted).pipe(Effect.flip)).reason).toBe("identity_conflict");
    expect((yield* assertNativeCommandReplayV2(accepted,
      makeGuardedCommandIdentityV2(command({ text: "Changed" }), guard, binding)).pipe(Effect.flip)).reason).toBe("identity_conflict");
    expect((yield* assertNativeCommandReplayV2({ ...accepted, receipt: { ...accepted.receipt,
      status: "rejected" } }, { ...identity, bindingDigest: "f".repeat(64) }).pipe(Effect.flip)).reason).toBe("identity_conflict");
  }),
);

it.effect("preserves ordinary unbound replay but never backfills native ownership onto its receipt", () =>
  Effect.gen(function* () {
    const original = { ...facts(), receipt: { commandId, threadId, commandType: "message.dispatch",
      acceptedAt: now, resultSequence: 11, status: "accepted" as const, error: null } };
    yield* assertNativeCommandReplayV2(original);
    expect((yield* assertNativeCommandReplayV2(original,
      makeGuardedCommandIdentityV2(command(), guard, binding)).pipe(Effect.flip)).reason).toBe("unbound_receipt");
    expect((yield* assertNativeCommandReplayV2({ ...original, identity: makeGuardedCommandIdentityV2(command(), guard, binding),
      receipt: null }, makeGuardedCommandIdentityV2(command(), guard, binding)).pipe(Effect.flip)).reason).toBe("identity_conflict");
  }),
);
