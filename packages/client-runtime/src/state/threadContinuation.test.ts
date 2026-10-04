import { describe, expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  MessageId,
  OrchestrationV2ImportedHistoryReviewBasis,
  ORCHESTRATION_V2_WS_METHODS,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  type OrchestrationV2ImportedHistoryReviewResult,
  type OrchestrationV2ImportedHistoryStartReceipt,
  type OrchestrationV2ThreadRuntimeAttachmentResult,
  type OrchestrationV2CurrentThreadRuntimeTarget,
  type OrchestrationV2StopCurrentThreadRuntimeInput,
  type OrchestrationV2StopCurrentThreadRuntimeResult,
  type OrchestrationV2ThreadRuntimeObservationResult,
  type OrchestrationV2ThreadRuntimeObservation,
  type OrchestrationV2OperatingCountsResult,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/unstable/reactivity";

import { v2Now, v2ThreadId, v2ThreadShell } from "./orchestrationV2TestFixtures.ts";
import { presentThreadShell } from "./models.ts";
import {
  resolveCurrentThreadRuntimeAttachment,
  resolveImportedContinuationReceipt,
  resolveImportedContinuationReview,
  captureCurrentThreadRuntimeStopTarget,
  resolveCurrentThreadRuntimeStop,
  resolveThreadRuntimeObservation,
  resolveThreadOperatingState,
  resolveOperatingCounts,
} from "./threadContinuation.ts";

const TARGET = { type: "message", messageId: MessageId.make("imported-message") } as const;
const BASIS = OrchestrationV2ImportedHistoryReviewBasis.make("reviewed-basis");
const COMMAND_ID = CommandId.make("imported-start");
const REVIEW: OrchestrationV2ImportedHistoryReviewResult = {
  version: 2,
  threadId: v2ThreadId,
  target: TARGET,
  capability: { startWithImportedHistory: true },
  applicability: "imported",
  qualification: { type: "unknown", reason: "Resume has not been qualified." },
  restoredBinding: { type: "missing", reason: "No exact restored binding." },
  nativeEffects: { type: "clear" },
  transcriptEligibility: { type: "eligible" },
  reviewedBasis: BASIS,
};
const EXPECTED = {
  threadId: v2ThreadId,
  commandId: COMMAND_ID,
  target: TARGET,
  reviewedBasis: BASIS,
};
const RECEIPT: OrchestrationV2ImportedHistoryStartReceipt = {
  version: 2,
  ...EXPECTED,
  intentStatus: "accepted",
  receipt: {
    commandId: COMMAND_ID,
    threadId: v2ThreadId,
    commandType: "thread.imported-history.start",
    acceptedAt: v2Now,
    resultSequence: 4,
    status: "accepted",
    error: null,
  },
  rejectionReason: null,
  execution: {
    status: "not_started",
    runId: null,
    providerThreadId: null,
    providerSessionId: null,
    nativeThreadId: null,
    effectOutcome: null,
    error: null,
  },
};
const STARTED: OrchestrationV2ImportedHistoryStartReceipt = {
  ...RECEIPT,
  execution: {
    ...RECEIPT.execution,
    status: "started",
    runId: RunId.make("imported-run"),
    providerThreadId: ProviderThreadId.make("imported-provider-thread"),
    providerSessionId: ProviderSessionId.make("imported-session"),
    nativeThreadId: "native-imported-thread",
    effectOutcome: "confirmed_success",
  },
};

describe("imported continuation review", () => {
  it.each(["unknown", "unsupported"] as const)(
    "offers explicit imported start for positively eligible %s qualification",
    (type) => {
      expect(
        resolveImportedContinuationReview(
          {
            ...REVIEW,
            qualification: { type, reason: "Not qualified." },
          },
          EXPECTED,
        ),
      ).toEqual({ status: "available", reviewedBasis: BASIS, reason: null });
    },
  );

  it("keeps qualified exact-binding continuation on ordinary Send", () => {
    expect(
      resolveImportedContinuationReview(
        {
          ...REVIEW,
          qualification: { type: "qualified" },
          restoredBinding: {
            type: "ready",
            providerThreadId: ProviderThreadId.make("restored-provider-thread"),
            providerInstanceId: ProviderInstanceId.make("codex"),
            driver: ProviderDriverKind.make("codex"),
            nativeThreadId: "restored-native-thread",
            providerSessionId: null,
          },
          capability: { startWithImportedHistory: false },
          reviewedBasis: null,
        },
        EXPECTED,
      ),
    ).toEqual({ status: "native", reason: null });
  });

  it("holds qualified continuation when its exact binding is missing", () => {
    expect(
      resolveImportedContinuationReview(
        {
          ...REVIEW,
          qualification: { type: "qualified" },
          reviewedBasis: null,
        },
        EXPECTED,
      ),
    ).toEqual({ status: "held", reason: "No exact restored binding." });
  });

  it.each(["imported", "not_imported"] as const)(
    "does not override known unknown native effects for %s applicability",
    (applicability) => {
      expect(
        resolveImportedContinuationReview(
          {
            ...REVIEW,
            applicability,
            reviewedBasis: null,
            nativeEffects: { type: "unknown", reason: "A native effect needs reconciliation." },
          },
          EXPECTED,
        ),
      ).toEqual({ status: "unknown", reason: "A native effect needs reconciliation." });
    },
  );

  it("does not turn non-imported conversation reads into consent", () => {
    expect(
      resolveImportedContinuationReview(
        {
          ...REVIEW,
          applicability: "not_imported",
          reviewedBasis: null,
        },
        EXPECTED,
      ),
    ).toEqual({ status: "ordinary", reason: null });
  });

  it.each([
    ["capability", { capability: { startWithImportedHistory: false } }, "unavailable"],
    ["applicability", { applicability: "unknown" }, "unknown"],
    [
      "eligibility",
      { transcriptEligibility: { type: "ineligible", reason: "No bounded transcript." } },
      "unavailable",
    ],
    [
      "eligibility evidence",
      { transcriptEligibility: { type: "unknown", reason: "Transcript is unknown." } },
      "unknown",
    ],
    ["basis", { reviewedBasis: null }, "unavailable"],
  ] satisfies ReadonlyArray<
    readonly [string, Partial<OrchestrationV2ImportedHistoryReviewResult>, string]
  >)("requires positive %s before explicit start", (_name, change, status) => {
    expect(resolveImportedContinuationReview({ ...REVIEW, ...change }, EXPECTED).status).toBe(
      status,
    );
  });

  it("rejects absent, another-thread, and another-queued-run reviews", () => {
    expect(resolveImportedContinuationReview(null, EXPECTED).status).toBe("unavailable");
    expect(
      resolveImportedContinuationReview(
        {
          ...REVIEW,
          threadId: ThreadId.make("other-thread"),
        },
        EXPECTED,
      ).status,
    ).toBe("unavailable");
    expect(
      resolveImportedContinuationReview(
        {
          ...REVIEW,
          target: {
            type: "queued_run",
            runId: RunId.make("other-run"),
            messageId: TARGET.messageId,
          },
        },
        {
          threadId: v2ThreadId,
          target: {
            type: "queued_run",
            runId: RunId.make("held-run"),
            messageId: TARGET.messageId,
          },
        },
      ).status,
    ).toBe("unavailable");
  });
});

describe("imported continuation receipt", () => {
  it.each(["not_started", "pending"] as const)(
    "shows accepted intent with %s execution as pending",
    (status) => {
      expect(
        resolveImportedContinuationReceipt(
          {
            ...RECEIPT,
            execution: { ...RECEIPT.execution, status },
          },
          EXPECTED,
        ),
      ).toEqual({ status: "pending", intentAccepted: true, reason: null });
    },
  );

  it("presents started only from correlated confirmed execution", () => {
    expect(resolveImportedContinuationReceipt(STARTED, EXPECTED)).toEqual({
      status: "started",
      intentAccepted: true,
      reason: null,
    });
    expect(
      resolveImportedContinuationReceipt(
        {
          ...STARTED,
          execution: { ...STARTED.execution, providerSessionId: null },
        },
        EXPECTED,
      ).status,
    ).toBe("unknown");
  });

  it("keeps uncertain execution reconciliation-only after accepted intent", () => {
    expect(
      resolveImportedContinuationReceipt(
        {
          ...RECEIPT,
          execution: { ...RECEIPT.execution, status: "unknown", effectOutcome: "unknown" },
        },
        EXPECTED,
      ),
    ).toMatchObject({ status: "unknown", intentAccepted: true });
    expect(resolveImportedContinuationReceipt(null, EXPECTED)).toMatchObject({
      status: "unknown",
      intentAccepted: false,
    });
    expect(
      resolveImportedContinuationReceipt(
        {
          ...RECEIPT,
          intentStatus: "not_found",
          receipt: null,
        },
        EXPECTED,
      ).status,
    ).toBe("unknown");
  });

  it.each(["not_found", "unknown"] as const)(
    "keeps a %s observation with no target reconciliation-only",
    (intentStatus) => {
      expect(
        resolveImportedContinuationReceipt(
          {
            ...RECEIPT,
            intentStatus,
            target: null,
            reviewedBasis: null,
            receipt: null,
          },
          EXPECTED,
        ),
      ).toMatchObject({ status: "unknown", intentAccepted: false });
    },
  );

  it("preserves the draft on correlated preadmission rejection", () => {
    expect(
      resolveImportedContinuationReceipt(
        {
          ...RECEIPT,
          intentStatus: "rejected",
          receipt: { ...RECEIPT.receipt!, status: "rejected", error: "Review is stale." },
          rejectionReason: "Review is stale.",
        },
        EXPECTED,
      ),
    ).toEqual({ status: "rejected", intentAccepted: false, reason: "Review is stale." });
  });

  it("does not classify rejection with unknown effects as a safe retry", () => {
    expect(
      resolveImportedContinuationReceipt(
        {
          ...RECEIPT,
          intentStatus: "rejected",
          receipt: null,
          execution: { ...RECEIPT.execution, effectOutcome: "unknown" },
        },
        EXPECTED,
      ).status,
    ).toBe("unknown");
  });

  it.each([
    ["command", { commandId: CommandId.make("other-command") }],
    ["thread", { threadId: ThreadId.make("other-thread") }],
    ["basis", { reviewedBasis: OrchestrationV2ImportedHistoryReviewBasis.make("new-basis") }],
    ["message", { target: { type: "message", messageId: MessageId.make("other-message") } }],
    [
      "observation",
      { receipt: { ...RECEIPT.receipt!, commandId: CommandId.make("other-command") } },
    ],
  ] satisfies ReadonlyArray<
    readonly [string, Partial<OrchestrationV2ImportedHistoryStartReceipt>]
  >)("ignores another %s response without retiring the reviewed draft", (_name, change) => {
    expect(resolveImportedContinuationReceipt({ ...STARTED, ...change }, EXPECTED)).toMatchObject({
      status: "unknown",
      intentAccepted: false,
    });
  });

  it("requires the selected queued run to own the execution", () => {
    const target = {
      type: "queued_run",
      runId: RunId.make("held-run"),
      messageId: TARGET.messageId,
    } as const;
    expect(
      resolveImportedContinuationReceipt(
        { ...STARTED, target },
        {
          ...EXPECTED,
          target,
        },
      ),
    ).toMatchObject({ status: "unknown", intentAccepted: true });
    expect(
      resolveImportedContinuationReceipt(
        {
          ...STARTED,
          target,
          execution: { ...STARTED.execution, runId: target.runId },
        },
        { ...EXPECTED, target },
      ).status,
    ).toBe("started");
  });
});

describe("current runtime attachment", () => {
  it("uses only the exact current thread read and preserves unknown versus stopped", () => {
    const stopped: OrchestrationV2ThreadRuntimeAttachmentResult = {
      threadId: v2ThreadId,
      attachment: {
        status: "stopped",
        reason: "runtime_not_resident",
        observedAt: "2026-10-03T00:00:00Z",
      },
    };
    expect(resolveCurrentThreadRuntimeAttachment(stopped, v2ThreadId)).toEqual(stopped.attachment);
    expect(
      resolveCurrentThreadRuntimeAttachment(stopped, ThreadId.make("other-thread")),
    ).toBeNull();
    const unknown: OrchestrationV2ThreadRuntimeAttachmentResult = {
      ...stopped,
      attachment: {
        status: "unknown",
        reason: "Observation is unavailable.",
        observedAt: stopped.attachment.observedAt,
      },
    };
    expect(resolveCurrentThreadRuntimeAttachment(unknown, v2ThreadId)?.status).toBe("unknown");
    expect(resolveCurrentThreadRuntimeAttachment(null, v2ThreadId)).toBeNull();
  });
});

const CURRENT_TARGET: OrchestrationV2CurrentThreadRuntimeTarget = {
  binding: {
    threadId: v2ThreadId,
    providerThreadId: ProviderThreadId.make("current-provider-thread"),
    providerSessionId: ProviderSessionId.make("current-provider-session"),
    instanceId: ProviderInstanceId.make("codex"),
    runtimeGeneration: "current-generation",
    nativeThreadId: "current-native-thread",
  },
  driver: ProviderDriverKind.make("codex"),
  evidenceRevision: 8,
};

const CURRENT_OBSERVATION: OrchestrationV2ThreadRuntimeObservationResult = {
  threadId: v2ThreadId,
  observation: {
    status: "working",
    binding: CURRENT_TARGET.binding,
    observedAt: "2026-10-03T02:28:34Z",
  },
};
const CURRENT_THREAD: OrchestrationV2ThreadShell = {
  ...v2ThreadShell,
  activeProviderThreadId: CURRENT_TARGET.binding.providerThreadId,
  modelSelection: { instanceId: ProviderInstanceId.make("claude-next"), model: "claude-next" },
};
const COUNTS: OrchestrationV2OperatingCountsResult = {
  total: 7,
  operating: 3,
  foregroundWaitingApproval: 1,
  foregroundWaitingInput: 1,
  foregroundWaitingPlan: 1,
  backgroundOperating: 2,
  backgroundUnknown: 1,
  snapshotSequence: 14,
  observedAt: "2026-10-03T02:28:34Z",
  backgroundSampledAt: "2026-10-03T02:28:30Z",
};

describe("current runtime observations and Operating", () => {
  it.each(["working", "monitoring", "busy", "idle"] as const)(
    "preserves current %s binding, generation, and time despite a different selected next model",
    (status) => {
      const observation: OrchestrationV2ThreadRuntimeObservation = {
        status,
        binding: CURRENT_TARGET.binding,
        observedAt: COUNTS.observedAt,
      };
      expect(
        resolveThreadRuntimeObservation(
          AsyncResult.success({
            threadId: v2ThreadId,
            observation,
          }),
          CURRENT_THREAD,
        ),
      ).toBe(observation);
    },
  );

  it("keeps unavailable, refreshing, and failed observations unknown without a fabricated timestamp", () => {
    const previous = AsyncResult.success(CURRENT_OBSERVATION);
    for (const query of [
      AsyncResult.initial<OrchestrationV2ThreadRuntimeObservationResult>(),
      AsyncResult.success(CURRENT_OBSERVATION, { waiting: true }),
      AsyncResult.failure(Cause.fail("Observation transport failed."), {
        previousSuccess: Option.some(previous),
      }),
    ]) {
      const observation = resolveThreadRuntimeObservation(query, CURRENT_THREAD);
      expect(observation.status).toBe("unknown");
      expect(observation).not.toHaveProperty("observedAt");
    }
  });

  it("rejects another thread and a former provider-thread owner", () => {
    expect(
      resolveThreadRuntimeObservation(
        AsyncResult.success({
          ...CURRENT_OBSERVATION,
          threadId: ThreadId.make("another-thread"),
        }),
        CURRENT_THREAD,
      ),
    ).toEqual({ status: "unknown", reason: "runtime_binding_changed" });
    expect(
      resolveThreadRuntimeObservation(AsyncResult.success(CURRENT_OBSERVATION), {
        ...CURRENT_THREAD,
        activeProviderThreadId: ProviderThreadId.make("replacement-owner"),
      }),
    ).toEqual({ status: "unknown", reason: "runtime_binding_changed" });
  });

  it("preserves authoritative unknown reasons and never promotes their optional binding to activity", () => {
    const observation: OrchestrationV2ThreadRuntimeObservation = {
      status: "unknown",
      binding: CURRENT_TARGET.binding,
      reason: "Registration cannot be reconciled.",
    };
    expect(
      resolveThreadRuntimeObservation(
        AsyncResult.success({
          threadId: v2ThreadId,
          observation,
        }),
        CURRENT_THREAD,
      ),
    ).toBe(observation);
    expect(resolveThreadOperatingState(CURRENT_THREAD, observation).operating).toBe(false);
    expect(
      resolveThreadRuntimeObservation(
        AsyncResult.success({
          threadId: v2ThreadId,
          observation: {
            ...observation,
            binding: {
              ...CURRENT_TARGET.binding,
              providerThreadId: ProviderThreadId.make("old-owner"),
            },
          },
        }),
        CURRENT_THREAD,
      ).status,
    ).toBe("unknown");
  });

  it.each([
    ["working", true, true],
    ["monitoring", true, false],
    ["busy", false, false],
    ["idle", false, false],
  ] as const)(
    "classifies live background %s separately from the workstream running count",
    (status, operating, workstreamRunning) => {
      expect(
        resolveThreadOperatingState(CURRENT_THREAD, {
          status,
          binding: CURRENT_TARGET.binding,
          observedAt: COUNTS.observedAt,
        }),
      ).toMatchObject({ operating, workstreamRunning, backgroundStatus: status });
    },
  );

  it.each(["starting", "running"] as const)(
    "keeps foreground %s Operating when background is unknown",
    (status) => {
      expect(
        resolveThreadOperatingState(
          { ...CURRENT_THREAD, status },
          {
            status: "unknown",
            reason: "No current background sample.",
          },
        ),
      ).toMatchObject({ operating: true, workstreamRunning: true });
    },
  );

  it.each([
    ["approval", { id: RuntimeRequestId.make("approval"), kind: "command", createdAt: v2Now }],
    ["input", { id: RuntimeRequestId.make("input"), kind: "user_input", createdAt: v2Now }],
    ["plan", null],
  ] satisfies ReadonlyArray<
    readonly [string, OrchestrationV2ThreadShell["pendingRuntimeRequest"]]
  >)(
    "preserves foreground %s display precedence independently of background monitoring",
    (foregroundAttention, pendingRuntimeRequest) => {
      const thread = { ...CURRENT_THREAD, pendingRuntimeRequest, hasActionableProposedPlan: true };
      const observation: OrchestrationV2ThreadRuntimeObservation = {
        status: "monitoring",
        binding: CURRENT_TARGET.binding,
        observedAt: COUNTS.observedAt,
      };
      const expected = {
        foregroundAttention,
        backgroundDisplay: "monitoring",
        operating: true,
        workstreamRunning: false,
      };
      expect(resolveThreadOperatingState(thread, observation)).toMatchObject(expected);
      expect(
        resolveThreadOperatingState(
          presentThreadShell(EnvironmentId.make("environment"), thread),
          observation,
        ),
      ).toMatchObject(expected);
    },
  );

  it("excludes archived threads but retains settled and snoozed live work", () => {
    const observation = CURRENT_OBSERVATION.observation;
    const retained = {
      ...CURRENT_THREAD,
      settledOverride: "settled" as const,
      settledAt: v2Now,
      snoozedAt: v2Now,
      snoozedUntil: v2Now,
    };
    expect(resolveThreadOperatingState(retained, observation)).toMatchObject({
      operating: true,
      workstreamRunning: true,
    });
    expect(
      resolveThreadOperatingState({ ...retained, archivedAt: v2Now }, observation),
    ).toMatchObject({ operating: false, workstreamRunning: false });
  });

  it("does not treat a retained pending task roster as live work", () => {
    const thread = {
      ...CURRENT_THREAD,
      pendingBackgroundTasks: [{ kind: "monitor" as const, taskId: "historical-monitor" }],
    };
    expect(
      resolveThreadOperatingState(thread, {
        status: "idle",
        binding: CURRENT_TARGET.binding,
        observedAt: COUNTS.observedAt,
      }),
    ).toMatchObject({ operating: false, workstreamRunning: false, backgroundDisplay: null });
  });

  it.each(["starting", "running"] as const)(
    "preserves authoritative foreground %s with a retained historical roster",
    (status) => {
      const presented = presentThreadShell(EnvironmentId.make("environment"), {
        ...CURRENT_THREAD,
        status,
        activityRunStatus: status,
        pendingBackgroundTasks: [{ kind: "monitor", taskId: "historical-monitor" }],
      });
      expect(presented.runtime?.status).toBe(status);
      expect(
        resolveThreadOperatingState(presented, {
          status: "unknown",
          reason: "No current background observation.",
        }),
      ).toMatchObject({ operating: true, workstreamRunning: true });
    },
  );
});

describe("Operating counts availability", () => {
  it("retains the complete authoritative count and timestamp tuple", () => {
    expect(resolveOperatingCounts(AsyncResult.success(COUNTS), 14)).toEqual({
      status: "available",
      counts: COUNTS,
    });
    expect(resolveOperatingCounts(AsyncResult.success(COUNTS), 14).counts).toBe(COUNTS);
  });

  it("keeps initial and failed reads unavailable instead of returning healthy zero counts", () => {
    expect(resolveOperatingCounts(AsyncResult.initial())).toEqual({
      status: "unavailable",
      counts: null,
    });
    expect(
      resolveOperatingCounts(
        AsyncResult.failure(Cause.fail("Counts manager failed."), {
          previousSuccess: Option.some(AsyncResult.success(COUNTS)),
        }),
      ),
    ).toEqual({ status: "unavailable", counts: null });
  });

  it("exposes refreshing and older-snapshot counts as stale", () => {
    expect(resolveOperatingCounts(AsyncResult.success(COUNTS, { waiting: true }))).toEqual({
      status: "stale",
      counts: null,
    });
    expect(resolveOperatingCounts(AsyncResult.success(COUNTS), 15)).toEqual({
      status: "stale",
      counts: null,
    });
  });
});
const STOP_INPUT: OrchestrationV2StopCurrentThreadRuntimeInput = {
  commandId: CommandId.make("current-runtime-stop"),
  threadId: v2ThreadId,
  target: CURRENT_TARGET,
};
const STOP_RESULT: OrchestrationV2StopCurrentThreadRuntimeResult = {
  version: 2,
  ...STOP_INPUT,
  commandStatus: "accepted",
  receipt: {
    commandId: STOP_INPUT.commandId,
    threadId: v2ThreadId,
    commandType: "provider-session.detach",
    acceptedAt: v2Now,
    resultSequence: 8,
    status: "accepted",
    error: null,
  },
  queueFence: { status: "installed", affectedRunIds: [RunId.make("held-before-stop")] },
  runtimeStop: { status: "pending" },
  reason: null,
};

describe("current runtime stop", () => {
  it("captures only the exact attached current tuple without historical session inference", () => {
    const attached: OrchestrationV2ThreadRuntimeAttachmentResult = {
      threadId: v2ThreadId,
      attachment: {
        status: "attached",
        ...CURRENT_TARGET,
        runtimeStatus: "running",
        observedAt: "2026-10-03T01:55:00Z",
      },
    };
    const target = captureCurrentThreadRuntimeStopTarget(attached, v2ThreadId);
    expect(target).toEqual(CURRENT_TARGET);
    expect(target?.binding).not.toBe(CURRENT_TARGET.binding);
    expect(
      captureCurrentThreadRuntimeStopTarget(attached, ThreadId.make("other-thread")),
    ).toBeNull();
    expect(
      captureCurrentThreadRuntimeStopTarget(
        {
          ...attached,
          attachment: {
            status: "unknown",
            reason: "Current binding is unknown.",
            observedAt: attached.attachment.observedAt,
          },
        },
        v2ThreadId,
      ),
    ).toBeNull();
    expect(
      captureCurrentThreadRuntimeStopTarget(
        {
          threadId: v2ThreadId,
          attachment: {
            status: "stopped",
            reason: "runtime_not_resident",
            observedAt: attached.attachment.observedAt,
          },
        },
        v2ThreadId,
      ),
    ).toBeNull();
    expect(captureCurrentThreadRuntimeStopTarget(null, v2ThreadId)).toBeNull();
    expect(
      captureCurrentThreadRuntimeStopTarget(
        {
          ...attached,
          attachment: {
            status: "attached",
            ...CURRENT_TARGET,
            binding: { ...CURRENT_TARGET.binding, threadId: ThreadId.make("other-thread") },
            runtimeStatus: "running",
            observedAt: attached.attachment.observedAt,
          },
        },
        v2ThreadId,
      ),
    ).toBeNull();
  });

  it("keeps command acceptance and queue fencing distinct from runtime stop", () => {
    expect(resolveCurrentThreadRuntimeStop(STOP_RESULT, STOP_INPUT)).toEqual({
      status: "pending",
      commandAccepted: true,
      queueFenceInstalled: true,
      reason: null,
    });
    expect(
      resolveCurrentThreadRuntimeStop(
        {
          ...STOP_RESULT,
          queueFence: { status: "not_installed", affectedRunIds: [] },
          runtimeStop: { status: "not_started" },
        },
        STOP_INPUT,
      ),
    ).toEqual({
      status: "pending",
      commandAccepted: true,
      queueFenceInstalled: false,
      reason: null,
    });
    expect(
      resolveCurrentThreadRuntimeStop(
        {
          ...STOP_RESULT,
          runtimeStop: { status: "stopped" },
        },
        STOP_INPUT,
      ),
    ).toEqual({
      status: "stopped",
      commandAccepted: true,
      queueFenceInstalled: true,
      reason: null,
    });
  });

  it.each(["thread.imported-history.start", ORCHESTRATION_V2_WS_METHODS.stopCurrentThreadRuntime])(
    "does not infer a stopped runtime from a %s receipt kind",
    (commandType) => {
      expect(
        resolveCurrentThreadRuntimeStop(
          {
            ...STOP_RESULT,
            receipt: { ...STOP_RESULT.receipt!, commandType },
            runtimeStop: { status: "stopped" },
          },
          STOP_INPUT,
        ),
      ).toMatchObject({
        status: "unknown",
        commandAccepted: false,
        queueFenceInstalled: false,
      });
    },
  );

  it("keeps installed-fence unknown effects reconciliation-only", () => {
    expect(
      resolveCurrentThreadRuntimeStop(
        {
          ...STOP_RESULT,
          runtimeStop: { status: "unknown" },
          reason: "Native stop observation is uncertain.",
        },
        STOP_INPUT,
      ),
    ).toEqual({
      status: "unknown",
      commandAccepted: true,
      queueFenceInstalled: true,
      reason: "Native stop observation is uncertain.",
    });
    expect(
      resolveCurrentThreadRuntimeStop(
        {
          ...STOP_RESULT,
          queueFence: { status: "unknown", affectedRunIds: [] },
        },
        STOP_INPUT,
      ).status,
    ).toBe("unknown");
  });

  it.each(["not_found", "unknown"] as const)(
    "does not infer a stop or safe retry from %s with no target",
    (commandStatus) => {
      expect(
        resolveCurrentThreadRuntimeStop(
          {
            ...STOP_RESULT,
            commandStatus,
            target: null,
            receipt: null,
            queueFence: { status: "not_installed", affectedRunIds: [] },
            runtimeStop: { status: "not_started" },
          },
          STOP_INPUT,
        ),
      ).toMatchObject({
        status: "unknown",
        commandAccepted: false,
        queueFenceInstalled: false,
      });
    },
  );

  it("presents a correlated preadmission rejection without claiming a fence or stopped runtime", () => {
    expect(
      resolveCurrentThreadRuntimeStop(
        {
          ...STOP_RESULT,
          commandStatus: "rejected",
          receipt: { ...STOP_RESULT.receipt!, status: "rejected", error: "Binding changed." },
          queueFence: { status: "not_installed", affectedRunIds: [] },
          runtimeStop: { status: "not_started" },
          reason: "Binding changed.",
        },
        STOP_INPUT,
      ),
    ).toEqual({
      status: "rejected",
      commandAccepted: false,
      queueFenceInstalled: false,
      reason: "Binding changed.",
    });
  });

  it.each([
    ["driver", { ...CURRENT_TARGET, driver: ProviderDriverKind.make("claude") }],
    ["evidence revision", { ...CURRENT_TARGET, evidenceRevision: 9 }],
    [
      "provider thread",
      {
        ...CURRENT_TARGET,
        binding: {
          ...CURRENT_TARGET.binding,
          providerThreadId: ProviderThreadId.make("replacement-thread"),
        },
      },
    ],
    [
      "provider session",
      {
        ...CURRENT_TARGET,
        binding: {
          ...CURRENT_TARGET.binding,
          providerSessionId: ProviderSessionId.make("replacement-session"),
        },
      },
    ],
    [
      "instance",
      {
        ...CURRENT_TARGET,
        binding: {
          ...CURRENT_TARGET.binding,
          instanceId: ProviderInstanceId.make("replacement-instance"),
        },
      },
    ],
    [
      "generation",
      {
        ...CURRENT_TARGET,
        binding: { ...CURRENT_TARGET.binding, runtimeGeneration: "replacement-generation" },
      },
    ],
    [
      "native thread",
      {
        ...CURRENT_TARGET,
        binding: { ...CURRENT_TARGET.binding, nativeThreadId: "replacement-native-thread" },
      },
    ],
  ] satisfies ReadonlyArray<readonly [string, OrchestrationV2CurrentThreadRuntimeTarget]>)(
    "does not apply a late %s stop result to the captured runtime",
    (_name, target) => {
      expect(
        resolveCurrentThreadRuntimeStop(
          {
            ...STOP_RESULT,
            target,
            runtimeStop: { status: "stopped" },
          },
          STOP_INPUT,
        ),
      ).toMatchObject({
        status: "unknown",
        commandAccepted: false,
        queueFenceInstalled: false,
      });
    },
  );

  it("ignores mismatched command, thread, and inner receipt identities", () => {
    for (const change of [
      { commandId: CommandId.make("another-stop-command") },
      { threadId: ThreadId.make("another-thread") },
      { receipt: { ...STOP_RESULT.receipt!, commandId: CommandId.make("another-stop-command") } },
    ]) {
      expect(
        resolveCurrentThreadRuntimeStop({ ...STOP_RESULT, ...change }, STOP_INPUT),
      ).toMatchObject({
        status: "unknown",
        commandAccepted: false,
        queueFenceInstalled: false,
      });
    }
    expect(resolveCurrentThreadRuntimeStop(null, STOP_INPUT).status).toBe("unknown");
  });
});
