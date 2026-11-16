import {
  CommandId,
  MessageId,
  NodeId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";

import {
  codexFailoverChain,
  codexFailoverEligibility,
  codexFailoverFollowerSelection,
  codexFailoverPostSwitchEligible,
  latestCodexExecutedRun,
  type CodexFailoverChainStep,
  type CodexFailoverSkipReason,
} from "./CodexFailoverEligibility.ts";

const threadId = ThreadId.make("failover-policy-thread");
const otherThreadId = ThreadId.make("other-thread");
const source = ProviderInstanceId.make("codex_A");
const target = ProviderInstanceId.make("codex_B");
const providerThreadId = ProviderThreadId.make("failover-native-thread");
const runId = RunId.make("failed-run");
const attemptId = RunAttemptId.make("failed-attempt");
const providerTurnId = ProviderTurnId.make("failed-turn");
const now = DateTime.makeUnsafe("2026-10-26T20:00:00.000Z");
const epoch = DateTime.toEpochMillis(now);
const driver = ProviderDriverKind.make("codex");

type Pre = Parameters<typeof codexFailoverEligibility>[0];
type Post = Parameters<typeof codexFailoverPostSwitchEligible>[0];

function run(overrides: Partial<OrchestrationV2Run> = {}): OrchestrationV2Run {
  return {
    id: runId,
    threadId,
    ordinal: 1,
    providerInstanceId: source,
    modelSelection: {
      instanceId: source,
      model: "origin-model",
      options: [{ id: "fast", value: true }],
    },
    providerThreadId,
    userMessageId: MessageId.make("original-user-message"),
    rootNodeId: NodeId.make("root-node"),
    activeAttemptId: attemptId,
    status: "failed",
    requestedAt: now,
    startedAt: now,
    completedAt: now,
    checkpointId: null,
    contextHandoffId: null,
    ...overrides,
  };
}

function pre(): Pre {
  return {
    projection: {
      thread: {
        id: threadId,
        modelSelection: { instanceId: source, model: "saved-model" },
        activeProviderThreadId: providerThreadId,
        archivedAt: null,
        deletedAt: null,
        settledAt: null,
        settledOverride: null,
      },
      runs: [run()],
      attempts: [
        {
          id: attemptId,
          runId,
          providerInstanceId: source,
          providerThreadId,
          providerSettlement: {
            runAttemptId: attemptId,
            providerTurnId,
            status: "failed",
            completedAt: now,
          },
        },
      ],
      providerThreads: [
        {
          id: providerThreadId,
          appThreadId: threadId,
          ownerNodeId: null,
          driver,
          providerInstanceId: source,
          nativeThreadRef: { driver, nativeId: "native-conversation", strength: "strong" },
        },
      ],
    },
    evidence: {
      providerTurnId,
      threadId,
      runId,
      attemptId,
      providerThreadId,
      providerInstanceId: source,
      disposition: "usage_limit",
      occurredAt: "2026-10-26T20:00:00.000Z",
    },
    facts: {
      driver: "codex",
      stopped: false,
      pendingRequest: false,
      sourceQuiet: true,
      worktreePresent: true,
      now: epoch,
    },
  };
}

describe("Codex failover pre-switch eligibility", () => {
  it("bounds rounded native completion times by server admission without changing stored history", () => {
    const previous = run({
      id: RunId.make("previous"),
      ordinal: 1,
      status: "completed",
      requestedAt: DateTime.makeUnsafe("2026-10-26T12:00:00.100Z"),
      startedAt: DateTime.makeUnsafe("2026-10-26T12:00:00.100Z"),
      completedAt: DateTime.makeUnsafe("2026-10-26T12:00:00.500Z"),
    });
    const failure = run({
      ordinal: 2,
      requestedAt: DateTime.makeUnsafe("2026-10-26T12:00:00.600Z"),
      startedAt: DateTime.makeUnsafe("2026-10-26T12:00:00.600Z"),
      completedAt: DateTime.makeUnsafe("2026-10-26T12:00:00Z"),
    });
    expect(latestCodexExecutedRun([previous, failure])?.id).toBe(failure.id);
    expect(DateTime.formatIso(failure.completedAt!)).toBe("2026-10-26T12:00:00.000Z");
    expect(
      latestCodexExecutedRun([
        failure,
        run({
          id: RunId.make("newer-active"),
          ordinal: 3,
          status: "running",
          completedAt: null,
        }),
      ])?.id,
    ).toBe(RunId.make("newer-active"));
    expect(
      latestCodexExecutedRun([
        previous,
        {
          ...failure,
          ordinal: 0,
          requestedAt: previous.requestedAt,
          startedAt: previous.startedAt,
          completedAt: DateTime.makeUnsafe("2026-10-26T12:00:01Z"),
        },
      ])?.id,
    ).toBe(failure.id);
  });
  it("allows an attributed failure even when saved and captured models differ", () => {
    expect(codexFailoverEligibility(pre())).toEqual({ type: "eligible" });
  });

  it("allows an app-owned delegated child to continue through the same policy", () => {
    const input = pre();
    expect(
      codexFailoverEligibility({
        ...input,
        projection: {
          ...input.projection,
          runs: [run({ delegatedTaskId: NodeId.make("delegated-task") })],
        },
      }),
    ).toEqual({ type: "eligible" });
  });

  const cases: Array<{
    name: string;
    reason: CodexFailoverSkipReason;
    change: (input: Pre) => Pre;
  }> = [
    {
      name: "another thread's evidence",
      reason: "not_failed",
      change: (i) => ({ ...i, evidence: { ...i.evidence, threadId: otherThreadId } }),
    },
    {
      name: "missing origin",
      reason: "not_failed",
      change: (i) => ({ ...i, projection: { ...i.projection, runs: [] } }),
    },
    {
      name: "non-failed run",
      reason: "not_failed",
      change: (i) => ({
        ...i,
        projection: { ...i.projection, runs: [run({ status: "completed" })] },
      }),
    },
    {
      name: "superseded attempt",
      reason: "not_failed",
      change: (i) => ({
        ...i,
        projection: {
          ...i.projection,
          runs: [run({ activeAttemptId: RunAttemptId.make("new-attempt") })],
        },
      }),
    },
    {
      name: "missing settlement",
      reason: "not_failed",
      change: (i) => ({
        ...i,
        projection: {
          ...i.projection,
          attempts: [{ ...i.projection.attempts[0]!, providerSettlement: null }],
        },
      }),
    },
    {
      name: "non-failed settlement",
      reason: "not_failed",
      change: (i) => ({
        ...i,
        projection: {
          ...i.projection,
          attempts: [
            {
              ...i.projection.attempts[0]!,
              providerSettlement: {
                ...i.projection.attempts[0]!.providerSettlement!,
                status: "interrupted",
              },
            },
          ],
        },
      }),
    },
    {
      name: "another turn's settlement",
      reason: "not_failed",
      change: (i) => ({
        ...i,
        evidence: { ...i.evidence, providerTurnId: ProviderTurnId.make("other-turn") },
      }),
    },
    {
      name: "settlement bound to another attempt",
      reason: "not_failed",
      change: (i) => ({
        ...i,
        projection: {
          ...i.projection,
          attempts: [
            {
              ...i.projection.attempts[0]!,
              providerSettlement: {
                ...i.projection.attempts[0]!.providerSettlement!,
                runAttemptId: RunAttemptId.make("other-attempt"),
              },
            },
          ],
        },
      }),
    },
    {
      name: "local failure",
      reason: "local_failure",
      change: (i) => ({ ...i, facts: { ...i.facts, localFailure: true } }),
    },
    {
      name: "newer executed run",
      reason: "not_latest",
      change: (i) => ({
        ...i,
        projection: {
          ...i.projection,
          runs: [
            ...i.projection.runs,
            run({ id: RunId.make("new-run"), ordinal: 2, status: "running", completedAt: null }),
          ],
        },
      }),
    },
    {
      name: "another driver",
      reason: "not_codex",
      change: (i) => ({ ...i, facts: { ...i.facts, driver: "claudeAgent" } }),
    },
    {
      name: "unknown driver",
      reason: "not_codex",
      change: (i) => ({ ...i, facts: { ...i.facts, driver: undefined } }),
    },
    {
      name: "saved selection moved",
      reason: "selection_moved",
      change: (i) => ({
        ...i,
        projection: {
          ...i.projection,
          thread: {
            ...i.projection.thread,
            modelSelection: { instanceId: target, model: "saved-model" },
          },
        },
      }),
    },
    {
      name: "attempt on another instance",
      reason: "selection_moved",
      change: (i) => ({
        ...i,
        projection: {
          ...i.projection,
          attempts: [{ ...i.projection.attempts[0]!, providerInstanceId: target }],
        },
      }),
    },
    {
      name: "active provider thread changed",
      reason: "provider_thread_changed",
      change: (i) => ({
        ...i,
        projection: {
          ...i.projection,
          thread: { ...i.projection.thread, activeProviderThreadId: null },
        },
      }),
    },
    {
      name: "attempt on another provider thread",
      reason: "provider_thread_changed",
      change: (i) => ({
        ...i,
        projection: {
          ...i.projection,
          attempts: [
            {
              ...i.projection.attempts[0]!,
              providerThreadId: ProviderThreadId.make("another-native-thread"),
            },
          ],
        },
      }),
    },
    {
      name: "missing native reference",
      reason: "no_native_ref",
      change: (i) => ({
        ...i,
        projection: {
          ...i.projection,
          providerThreads: [{ ...i.projection.providerThreads[0]!, nativeThreadRef: null }],
        },
      }),
    },
    {
      name: "blank native identity",
      reason: "no_native_ref",
      change: (i) => ({
        ...i,
        projection: {
          ...i.projection,
          providerThreads: [
            {
              ...i.projection.providerThreads[0]!,
              nativeThreadRef: { driver, nativeId: " ", strength: "strong" },
            },
          ],
        },
      }),
    },
    {
      name: "foreign owned provider thread",
      reason: "no_native_ref",
      change: (i) => ({
        ...i,
        projection: {
          ...i.projection,
          providerThreads: [{ ...i.projection.providerThreads[0]!, appThreadId: otherThreadId }],
        },
      }),
    },
    {
      name: "native child provider thread",
      reason: "no_native_ref",
      change: (i) => ({
        ...i,
        projection: {
          ...i.projection,
          providerThreads: [
            { ...i.projection.providerThreads[0]!, ownerNodeId: NodeId.make("native-child") },
          ],
        },
      }),
    },
    {
      name: "Stop marker",
      reason: "stopped",
      change: (i) => ({ ...i, facts: { ...i.facts, stopped: true, sourceQuiet: false } }),
    },
    {
      name: "pending request",
      reason: "pending_request",
      change: (i) => ({ ...i, facts: { ...i.facts, pendingRequest: true } }),
    },
    {
      name: "archived thread",
      reason: "inactive_thread",
      change: (i) => ({
        ...i,
        projection: { ...i.projection, thread: { ...i.projection.thread, archivedAt: now } },
      }),
    },
    {
      name: "deleted thread",
      reason: "inactive_thread",
      change: (i) => ({
        ...i,
        projection: { ...i.projection, thread: { ...i.projection.thread, deletedAt: now } },
      }),
    },
    {
      name: "settled thread",
      reason: "inactive_thread",
      change: (i) => ({
        ...i,
        projection: { ...i.projection, thread: { ...i.projection.thread, settledAt: now } },
      }),
    },
    {
      name: "settled override",
      reason: "inactive_thread",
      change: (i) => ({
        ...i,
        projection: {
          ...i.projection,
          thread: { ...i.projection.thread, settledOverride: "settled" },
        },
      }),
    },
    {
      name: "snoozed thread",
      reason: "snoozed",
      change: (i) => ({
        ...i,
        projection: {
          ...i.projection,
          thread: { ...i.projection.thread, snoozedUntil: DateTime.add(now, { seconds: 1 }) },
        },
      }),
    },
    {
      name: "missing worktree",
      reason: "worktree_missing",
      change: (i) => ({ ...i, facts: { ...i.facts, worktreePresent: false } }),
    },
  ];
  it.each(cases)("skips $name", ({ change, reason }) => {
    expect(codexFailoverEligibility(change(pre()))).toEqual({ type: "skip", reason });
  });

  it("defers active native children or background work, then rechecks Stop before retrying", () => {
    const input = pre();
    expect(
      codexFailoverEligibility({ ...input, facts: { ...input.facts, sourceQuiet: false } }),
    ).toEqual({ type: "defer", reason: "source_busy" });
    expect(
      codexFailoverEligibility({ ...input, facts: { ...input.facts, stopped: true } }),
    ).toEqual({ type: "skip", reason: "stopped" });
    expect(codexFailoverEligibility(input)).toEqual({ type: "eligible" });
  });

  it("does not let queued or never-started cancelled messages displace the latest executed run", () => {
    const input = pre();
    const projection = {
      ...input.projection,
      runs: [
        run({
          id: RunId.make("queued"),
          ordinal: 10,
          status: "queued",
          startedAt: null,
          completedAt: null,
        }),
        run({ id: RunId.make("cancelled"), ordinal: 11, status: "cancelled", startedAt: null }),
        run(),
      ],
    };
    expect(codexFailoverEligibility({ ...input, projection })).toEqual({ type: "eligible" });
  });

  it("expires startup evidence after 24 hours, while ordinary reactions and the exact boundary remain eligible", () => {
    const input = pre();
    const evidence = { ...input.evidence, occurredAt: "2026-10-25T20:00:00.000Z" };
    expect(
      codexFailoverEligibility({ ...input, evidence, facts: { ...input.facts, startup: true } }),
    ).toEqual({ type: "eligible" });
    expect(
      codexFailoverEligibility({
        ...input,
        evidence,
        facts: { ...input.facts, startup: true, now: epoch + 1 },
      }),
    ).toEqual({ type: "skip", reason: "stale" });
    expect(
      codexFailoverEligibility({ ...input, evidence, facts: { ...input.facts, now: epoch + 1 } }),
    ).toEqual({ type: "eligible" });
  });
});

function post(): Post {
  const input = pre();
  const savedSelection = {
    instanceId: target,
    model: "saved-model",
    options: [{ id: "effort", value: "high" }],
  };
  return {
    projection: {
      ...input.projection,
      thread: { ...input.projection.thread, modelSelection: savedSelection },
    },
    step: { threadId, originRunId: runId, status: "switched", savedSelection },
    selectionRevision: 42,
    switchSequence: 42,
    facts: { ...input.facts, blockingRun: false },
    targetAdmitted: true,
  };
}

describe("Codex failover post-switch eligibility", () => {
  it("recovers a committed switch even though the saved instance is already the target", () => {
    const input = post();
    expect(codexFailoverPostSwitchEligible(input)).toBe(true);
    expect(codexFailoverEligibility({ ...pre(), projection: input.projection })).toEqual({
      type: "skip",
      reason: "selection_moved",
    });
  });

  const cases: Array<{ name: string; change: (input: Post) => Post }> = [
    { name: "continued step", change: (i) => ({ ...i, step: { ...i.step, status: "continued" } }) },
    { name: "abandoned step", change: (i) => ({ ...i, step: { ...i.step, status: "abandoned" } }) },
    {
      name: "another thread",
      change: (i) => ({ ...i, step: { ...i.step, threadId: otherThreadId } }),
    },
    {
      name: "later model selection",
      change: (i) => ({
        ...i,
        projection: {
          ...i.projection,
          thread: {
            ...i.projection.thread,
            modelSelection: { ...i.step.savedSelection, model: "new-model" },
          },
        },
      }),
    },
    {
      name: "later option selection",
      change: (i) => ({
        ...i,
        projection: {
          ...i.projection,
          thread: {
            ...i.projection.thread,
            modelSelection: { ...i.step.savedSelection, options: [{ id: "effort", value: "max" }] },
          },
        },
      }),
    },
    {
      name: "later selection event even with identical selection",
      change: (i) => ({ ...i, selectionRevision: 43 }),
    },
    { name: "missing switch event", change: (i) => ({ ...i, switchSequence: null }) },
    {
      name: "newer run",
      change: (i) => ({
        ...i,
        projection: {
          ...i.projection,
          runs: [run(), run({ id: RunId.make("new-run"), ordinal: 2 })],
        },
      }),
    },
    { name: "Stop", change: (i) => ({ ...i, facts: { ...i.facts, stopped: true } }) },
    { name: "blocking work", change: (i) => ({ ...i, facts: { ...i.facts, blockingRun: true } }) },
    {
      name: "pending request",
      change: (i) => ({ ...i, facts: { ...i.facts, pendingRequest: true } }),
    },
    {
      name: "archived thread",
      change: (i) => ({
        ...i,
        projection: { ...i.projection, thread: { ...i.projection.thread, archivedAt: now } },
      }),
    },
    {
      name: "deleted thread",
      change: (i) => ({
        ...i,
        projection: { ...i.projection, thread: { ...i.projection.thread, deletedAt: now } },
      }),
    },
    {
      name: "settled thread",
      change: (i) => ({
        ...i,
        projection: { ...i.projection, thread: { ...i.projection.thread, settledAt: now } },
      }),
    },
    {
      name: "snoozed thread",
      change: (i) => ({
        ...i,
        projection: {
          ...i.projection,
          thread: { ...i.projection.thread, snoozedUntil: DateTime.add(now, { seconds: 1 }) },
        },
      }),
    },
    {
      name: "missing worktree",
      change: (i) => ({ ...i, facts: { ...i.facts, worktreePresent: false } }),
    },
    {
      name: "busy source runtime",
      change: (i) => ({ ...i, facts: { ...i.facts, sourceQuiet: false } }),
    },
    { name: "target no longer admitted", change: (i) => ({ ...i, targetAdmitted: false }) },
  ];
  it.each(cases)("refuses $name", ({ change }) => {
    expect(codexFailoverPostSwitchEligible(change(post()))).toBe(false);
  });
});

describe("Codex failover chain identity and reset memory", () => {
  const step: CodexFailoverChainStep = {
    threadId,
    chainRootRunId: runId,
    originRunId: runId,
    continueMessageId: MessageId.make("continue-A"),
    sourceInstanceId: source,
  };

  it("joins by continuation message ID only and includes all sources in that chain", () => {
    const origin = run({
      id: RunId.make("B-run"),
      userMessageId: step.continueMessageId,
      providerInstanceId: target,
    });
    const steps = [
      step,
      {
        ...step,
        originRunId: origin.id,
        sourceInstanceId: target,
        continueMessageId: MessageId.make("continue-B"),
      },
      { ...step, threadId: otherThreadId, sourceInstanceId: ProviderInstanceId.make("foreign") },
      {
        ...step,
        chainRootRunId: RunId.make("another-chain"),
        sourceInstanceId: ProviderInstanceId.make("unrelated"),
      },
    ];
    const result = codexFailoverChain({ threadId, origin, steps, usageResets: [], now: epoch });
    expect(result.chainRootRunId).toBe(runId);
    expect([...result.visited]).toEqual([target, source]);
    const fresh = codexFailoverChain({
      threadId,
      origin: { ...origin, userMessageId: MessageId.make("new-user-message") },
      steps,
      usageResets: [],
      now: epoch,
    });
    expect(fresh.chainRootRunId).toBe(origin.id);
    expect([...fresh.visited]).toEqual([target]);
  });

  it("excludes future usage resets across chains on this thread, but ignores elapsed, unavailable and foreign records", () => {
    const reset = {
      threadId,
      sourceInstanceId: target,
      sourceFailure: "usage_limit" as const,
      sourceResetAt: "2026-10-26T21:00:00.000Z",
    };
    const usageResets = [
      reset,
      {
        ...reset,
        sourceInstanceId: ProviderInstanceId.make("elapsed"),
        sourceResetAt: "2026-10-26T20:00:00.000Z",
      },
      { ...reset, sourceInstanceId: ProviderInstanceId.make("unknown"), sourceResetAt: null },
      {
        ...reset,
        sourceInstanceId: ProviderInstanceId.make("malformed"),
        sourceResetAt: "invalid",
      },
      {
        ...reset,
        sourceInstanceId: ProviderInstanceId.make("unavailable"),
        sourceFailure: "unavailable" as const,
      },
      { ...reset, sourceInstanceId: ProviderInstanceId.make("foreign"), threadId: otherThreadId },
    ];
    const input = { threadId, origin: run(), steps: [], usageResets, now: epoch };
    expect([...codexFailoverChain(input).visited]).toEqual([source, target]);
    expect([...codexFailoverChain({ ...input, now: epoch + 3_600_000 }).visited]).toEqual([source]);
  });
});

describe("Codex failover queued followers", () => {
  type Follower = Parameters<typeof codexFailoverFollowerSelection>[0];
  const follower = (): Follower => ({
    run: run({ status: "queued" }),
    marker: { threadId, runId },
    threadId,
    activeProviderThreadId: providerThreadId,
    sourceInstanceId: source,
    targetInstanceId: target,
    effectiveSelection: run().modelSelection,
    targetAdmissible: () => true,
  });

  it("admits and returns the follower's own model and options on the target", () => {
    const input = follower();
    const expected = { ...input.effectiveSelection, instanceId: target };
    const checked: Array<unknown> = [];
    expect(
      codexFailoverFollowerSelection({
        ...input,
        targetAdmissible: (selection) => {
          checked.push(selection);
          return true;
        },
      }),
    ).toEqual(expected);
    expect(checked).toEqual([expected]);
    expect(input.effectiveSelection.instanceId).toBe(source);
  });

  const continuedFollower = (): Follower => {
    const input = follower();
    const effectiveSelection = { ...input.effectiveSelection, instanceId: target };
    const switchCommandId = CommandId.make("previous-switch");
    const continueMessageId = MessageId.make("previous-continuation");
    return {
      ...input,
      activeProviderThreadId: ProviderThreadId.make("account-B-thread"),
      sourceInstanceId: target,
      targetInstanceId: ProviderInstanceId.make("codex_C"),
      effectiveSelection,
      marker: {
        threadId,
        runId,
        overrideSelection: effectiveSelection,
        overrideSwitchCommandId: switchCommandId,
      },
      continuation: {
        chainRootRunId: runId,
        origin: {
          ...run(),
          userMessageId: continueMessageId,
          providerThreadId: ProviderThreadId.make("account-B-thread"),
          providerInstanceId: target,
        },
        previousStep: {
          switchCommandId,
          threadId,
          chainRootRunId: runId,
          continueMessageId,
          targetInstanceId: target,
          status: "continued",
        },
      },
    };
  };

  it("follows a durable prior override across distinct account-owned provider threads", () => {
    const input = continuedFollower();
    expect(input.run.providerThreadId).not.toBe(input.activeProviderThreadId);
    expect(codexFailoverFollowerSelection(input)).toEqual({
      ...input.effectiveSelection,
      instanceId: input.targetInstanceId,
    });
  });

  const continuationCases: Array<{ name: string; change: (input: Follower) => Follower }> = [
    { name: "explicit or markerless run", change: (i) => ({ ...i, marker: null }) },
    {
      name: "another run's marker",
      change: (i) => ({ ...i, marker: { ...i.marker!, runId: RunId.make("other-run") } }),
    },
    {
      name: "another thread's marker",
      change: (i) => ({ ...i, marker: { ...i.marker!, threadId: otherThreadId } }),
    },
    {
      name: "missing durable override",
      change: (i) => ({ ...i, marker: { ...i.marker!, overrideSelection: null } }),
    },
    {
      name: "unrelated override command",
      change: (i) => ({
        ...i,
        marker: { ...i.marker!, overrideSwitchCommandId: CommandId.make("unrelated") },
      }),
    },
    {
      name: "changed effective selection",
      change: (i) => ({
        ...i,
        effectiveSelection: { ...i.effectiveSelection, model: "other-model" },
      }),
    },
    {
      name: "missing prior switch",
      change: (i) => ({ ...i, continuation: { ...i.continuation!, previousStep: null } }),
    },
    {
      name: "different chain",
      change: (i) => ({
        ...i,
        continuation: { ...i.continuation!, chainRootRunId: RunId.make("other-chain") },
      }),
    },
    {
      name: "different continuation",
      change: (i) => ({
        ...i,
        continuation: {
          ...i.continuation!,
          origin: { ...i.continuation!.origin, userMessageId: MessageId.make("unrelated") },
        },
      }),
    },
    {
      name: "unrelated active provider thread",
      change: (i) => ({ ...i, activeProviderThreadId: ProviderThreadId.make("unrelated") }),
    },
    {
      name: "another source account",
      change: (i) => ({
        ...i,
        continuation: {
          ...i.continuation!,
          origin: { ...i.continuation!.origin, providerInstanceId: source },
        },
      }),
    },
    {
      name: "another app thread",
      change: (i) => ({
        ...i,
        continuation: {
          ...i.continuation!,
          origin: { ...i.continuation!.origin, threadId: otherThreadId },
        },
      }),
    },
    {
      name: "abandoned prior switch",
      change: (i) => ({
        ...i,
        continuation: {
          ...i.continuation!,
          previousStep: { ...i.continuation!.previousStep!, status: "abandoned" },
        },
      }),
    },
    {
      name: "prior switch targeting another source",
      change: (i) => ({
        ...i,
        continuation: {
          ...i.continuation!,
          previousStep: { ...i.continuation!.previousStep!, targetInstanceId: source },
        },
      }),
    },
    {
      name: "prior switch on another thread",
      change: (i) => ({
        ...i,
        continuation: {
          ...i.continuation!,
          previousStep: { ...i.continuation!.previousStep!, threadId: otherThreadId },
        },
      }),
    },
    { name: "inadmissible follower", change: (i) => ({ ...i, targetAdmissible: () => false }) },
  ];
  it.each(continuationCases)("preserves $name across provider-thread changes", ({ change }) => {
    expect(codexFailoverFollowerSelection(change(continuedFollower()))).toBeNull();
  });

  const cases: Array<{ name: string; change: (input: Follower) => Follower }> = [
    {
      name: "explicit selection without an inheritance marker",
      change: (i) => ({ ...i, marker: null }),
    },
    { name: "legacy run without a marker", change: (i) => ({ ...i, marker: null }) },
    {
      name: "marker for another run",
      change: (i) => ({ ...i, marker: { threadId, runId: RunId.make("other-run") } }),
    },
    {
      name: "marker for another thread",
      change: (i) => ({ ...i, marker: { threadId: otherThreadId, runId } }),
    },
    {
      name: "another thread's run",
      change: (i) => ({ ...i, run: { ...i.run, threadId: otherThreadId } }),
    },
    {
      name: "already started run",
      change: (i) => ({ ...i, run: { ...i.run, status: "running" } }),
    },
    {
      name: "another provider thread",
      change: (i) => ({
        ...i,
        run: { ...i.run, providerThreadId: ProviderThreadId.make("other-provider-thread") },
      }),
    },
    {
      name: "unbound active provider thread",
      change: (i) => ({ ...i, activeProviderThreadId: null }),
    },
    {
      name: "another effective instance",
      change: (i) => ({
        ...i,
        effectiveSelection: { ...i.effectiveSelection, instanceId: target },
      }),
    },
    {
      name: "inadmissible own selection",
      change: (i) => ({ ...i, targetAdmissible: () => false }),
    },
  ];
  it.each(cases)("preserves $name", ({ change }) => {
    expect(codexFailoverFollowerSelection(change(follower()))).toBeNull();
  });
});
