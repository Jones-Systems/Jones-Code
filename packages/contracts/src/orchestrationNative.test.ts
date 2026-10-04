import { assert, it } from "@effect/vitest";
import * as Option from "effect/Option";
import * as DateTime from "effect/DateTime";
import { createHash } from "node:crypto";
import * as Schema from "effect/Schema";
import { NativeCreationEffect } from "./nativeCreation.ts";
import {
  LegacyNativeBootstrapCommandV1,
  NativeCommandIdentityV2,
  NativeBootstrapDispatchResultV2,
  NativeBootstrapDispatchResultV2Json,
  NativeCommandObservationV2,
  NativeCommandObservationV2Json,
  NativeCreationEffectV2,
  NativeCreationObservationV2,
  NativeCreationObservationV2Json,
  NativeThreadIncarnationV2,
  OrchestrationCommandObservation,
  OrchestrationDispatchTargetV2,
  OrchestrationV2ThreadRuntimeAttachment,
  OrchestrationV2ThreadRuntimeAttachmentResult,
  OrchestrationV2ThreadRuntimeObservation,
  OrchestrationV2ThreadRuntimeObservationResult,
  OrchestrationV2OperatingCountsResult,
  OrchestrationV2CurrentThreadRuntimeTarget,
  OrchestrationV2StopCurrentThreadRuntimeResult,
  OrchestrationV2StopCurrentThreadRuntimeResultJson,
  OrchestrationV2ThreadDeletionCleanupObservation,
  OrchestrationV2ThreadDeletionCleanupObservationJson,
  OrchestrationV2ImportedHistoryReviewResult,
  OrchestrationV2ImportedHistoryStartReceipt,
  OrchestrationV2ImportedHistoryStartReceiptJson,
  ThreadTurnDispatchGuardV2,
  ThreadTurnDispatchGuard,
  ThreadTurnStartCommand,
} from "./orchestrationNative.ts";

const deletionCleanupWire = {
  threadId: "thread-1",
  commandId: "delete-1",
  receipt: {
    commandId: "delete-1",
    threadId: "thread-1",
    commandType: "thread.delete",
    acceptedAt: "2026-10-03T12:00:00.000Z",
    resultSequence: 24,
    status: "accepted",
    error: null,
  },
  deletion: { eventId: "deletion-event-1", sequence: 21, resultSequence: 24 },
  worktree: { projectId: "project-1", path: "/project/task", branch: "task-branch" },
  state: "completed",
  removalOutcome: { result: "succeeded", effect: "confirmed" },
  currentLease: "absent",
  reason: null,
};

it("round-trips deletion cleanup runtime and JSON metadata while keeping historical event and final receipt sequences separate", () => {
  for (const wire of [
    deletionCleanupWire,
    { ...deletionCleanupWire, currentLease: "replacement" },
    {
      ...deletionCleanupWire,
      state: "unknown",
      removalOutcome: { result: "succeeded", effect: "unknown" },
      currentLease: "unavailable",
      reason: "readback_unavailable",
    },
  ]) {
    const decoded = Schema.decodeUnknownSync(OrchestrationV2ThreadDeletionCleanupObservationJson)(
      wire,
    );
    assert.isTrue(DateTime.isDateTime(decoded.receipt?.acceptedAt));
    assert.strictEqual(decoded.deletion?.sequence, 21);
    assert.strictEqual(decoded.receipt?.resultSequence, 24);
    assert.deepEqual<unknown>(
      Schema.encodeSync(OrchestrationV2ThreadDeletionCleanupObservationJson)(decoded),
      wire,
    );
    const runtime = Schema.decodeUnknownSync(OrchestrationV2ThreadDeletionCleanupObservation)(
      decoded,
    );
    assert.deepEqual<unknown>(
      Schema.encodeSync(Schema.toCodecJson(OrchestrationV2ThreadDeletionCleanupObservation))(
        runtime,
      ),
      wire,
    );
  }
});

it("preserves unavailable deletion facts and independent removal/readback states without manufacturing successful cleanup", () => {
  const missing = {
    threadId: "thread-1",
    commandId: "delete-1",
    receipt: null,
    deletion: null,
    worktree: null,
    state: "not_found",
    removalOutcome: null,
    currentLease: "unavailable",
    reason: null,
  };
  for (const state of ["not_found", "unknown"]) {
    const wire = { ...missing, state };
    assert.deepEqual<unknown>(
      Schema.encodeSync(OrchestrationV2ThreadDeletionCleanupObservationJson)(
        Schema.decodeUnknownSync(OrchestrationV2ThreadDeletionCleanupObservationJson)(wire),
      ),
      wire,
    );
  }
  for (const state of ["not_requested", "pending", "removing", "retained"]) {
    const wire = { ...deletionCleanupWire, state, removalOutcome: null, currentLease: "original" };
    assert.deepEqual<unknown>(
      Schema.encodeSync(OrchestrationV2ThreadDeletionCleanupObservationJson)(
        Schema.decodeUnknownSync(OrchestrationV2ThreadDeletionCleanupObservationJson)(wire),
      ),
      wire,
    );
  }
  for (const effect of ["confirmed", "absent", "no_effect", "unknown"]) {
    const wire = {
      ...deletionCleanupWire,
      state: "retained",
      removalOutcome: { result: null, effect },
      currentLease: "original",
    };
    assert.deepEqual<unknown>(
      Schema.encodeSync(OrchestrationV2ThreadDeletionCleanupObservationJson)(
        Schema.decodeUnknownSync(OrchestrationV2ThreadDeletionCleanupObservationJson)(wire),
      ),
      wire,
    );
  }
});

it("rejects private deletion authority and malformed sequence/outcome fields in runtime and JSON codecs", () => {
  const runtime = Schema.decodeUnknownSync(OrchestrationV2ThreadDeletionCleanupObservationJson)(
    deletionCleanupWire,
  );
  for (const [schema, base] of [
    [OrchestrationV2ThreadDeletionCleanupObservationJson, deletionCleanupWire],
    [OrchestrationV2ThreadDeletionCleanupObservation, runtime],
  ] as const) {
    const accepts = Schema.decodeUnknownOption(schema);
    for (const changed of [
      { lease: {} },
      { birth: {} },
      { proof: undefined },
      { deletion: { ...base.deletion, sequence: -1 } },
      { deletion: { ...base.deletion, resultSequence: 1.5 } },
      { deletion: { ...base.deletion, birth: "private" } },
      { worktree: { ...base.worktree, lease: {} } },
      { receipt: { ...base.receipt, claimId: "private" } },
      { removalOutcome: { result: "succeeded", effect: "success" } },
      { removalOutcome: { result: "succeeded", effect: "confirmed", retry: true } },
      { state: "removed" },
      { currentLease: "held" },
    ])
      assert.isTrue(Option.isNone(accepts({ ...base, ...changed })));
  }
});

const attachedRuntime = {
  status: "attached",
  binding: {
    threadId: "thread-1",
    providerThreadId: "provider-thread-1",
    providerSessionId: "provider-session-1",
    instanceId: "codex_work",
    runtimeGeneration: "generation-current-1",
    nativeThreadId: "native-thread-1",
  },
  driver: "codex",
  runtimeStatus: "ready",
  evidenceRevision: 4,
  observedAt: "2026-10-03T01:00:00Z",
};

it("round-trips exact current runtime attachment without inferring historical or operational state", () => {
  for (const attachment of [
    attachedRuntime,
    { status: "stopped", reason: "runtime_not_resident", observedAt: attachedRuntime.observedAt },
    { status: "unknown", reason: "unconfigured", observedAt: attachedRuntime.observedAt },
  ]) {
    const wire = { threadId: "thread-1", attachment };
    const decoded = Schema.decodeUnknownSync(OrchestrationV2ThreadRuntimeAttachmentResult)(wire);
    assert.deepEqual<unknown>(
      Schema.encodeSync(OrchestrationV2ThreadRuntimeAttachmentResult)(decoded),
      wire,
    );
  }
  const { nativeThreadId: _nativeThreadId, ...binding } = attachedRuntime.binding;
  const withoutNativeThread = { ...attachedRuntime, binding };
  const decoded = Schema.decodeUnknownSync(OrchestrationV2ThreadRuntimeAttachment)(
    withoutNativeThread,
  );
  assert.deepEqual<unknown>(
    Schema.encodeSync(OrchestrationV2ThreadRuntimeAttachment)(decoded),
    withoutNativeThread,
  );
});

it("requires current binding string generation and nonnegative integral runtime evidence revisions", () => {
  const accepts = Schema.decodeUnknownOption(OrchestrationV2ThreadRuntimeAttachment);
  for (const changed of [
    { binding: { ...attachedRuntime.binding, runtimeGeneration: 4 } },
    { binding: { ...attachedRuntime.binding, runtimeGeneration: undefined } },
    { binding: { ...attachedRuntime.binding, nativeThreadId: null } },
    { runtimeStatus: "idle" },
    { evidenceRevision: -1 },
    { evidenceRevision: 1.5 },
  ])
    assert.isTrue(Option.isNone(accepts({ ...attachedRuntime, ...changed })));
  assert.isTrue(Option.isSome(accepts({ ...attachedRuntime, evidenceRevision: 0 })));
});

it("preserves explicit optional STOP feature availability without inferring authorization or attachment proof", () => {
  const schema = OrchestrationV2ThreadRuntimeAttachmentResult;
  const base = {
    threadId: "thread-1",
    attachment: {
      status: "unknown",
      reason: "unconfigured",
      observedAt: attachedRuntime.observedAt,
    },
  };
  for (const wire of [
    base,
    { ...base, stopCapability: null },
    { ...base, stopCapability: { version: 2 } },
  ]) {
    const decoded = Schema.decodeUnknownSync(schema)(wire);
    assert.deepEqual<unknown>(Schema.encodeSync(schema)(decoded), wire);
    assert.deepEqual<unknown>(Schema.encodeSync(Schema.toCodecJson(schema))(decoded), wire);
  }
  for (const stopCapability of [
    { version: 1 },
    { version: 3 },
    { version: "2" },
    { version: 2, authorized: true },
    { version: 2, stopped: true },
  ]) {
    assert.isTrue(Option.isNone(Schema.decodeUnknownOption(schema)({ ...base, stopCapability })));
  }
});

it("rejects projection history, invented operating and stop-success fields in current attachment results", () => {
  const accepts = Schema.decodeUnknownOption(OrchestrationV2ThreadRuntimeAttachmentResult);
  for (const wire of [
    { threadId: "thread-1", attachment: attachedRuntime, history: [] },
    { threadId: "thread-1", attachment: { ...attachedRuntime, operating: true } },
    {
      threadId: "thread-1",
      attachment: {
        ...attachedRuntime,
        binding: { ...attachedRuntime.binding, canonicalCommand: "{}" },
      },
    },
    {
      threadId: "thread-1",
      attachment: {
        status: "stopped",
        reason: "runtime_not_resident",
        observedAt: attachedRuntime.observedAt,
        stopSucceeded: true,
      },
    },
    { threadId: "thread-1", attachment: null },
  ])
    assert.isTrue(Option.isNone(accepts(wire)));
});

const guard = {
  observedSnapshotSequence: 12,
  expectedModelSelection: {
    instanceId: "codex_work",
    model: "model-1",
    options: [{ id: "effort", value: "high" }],
  },
  expectedSessionStatus: "ready",
  expectedActiveTurnId: null,
  expectedLatestTurnId: "historical-turn-1",
  requireIdle: true,
};

it("retains V1 guard bindings and requires explicit nullable historical turn evidence", () => {
  const decoded = Schema.decodeUnknownSync(ThreadTurnDispatchGuard)(guard);
  assert.deepEqual<unknown>(Schema.encodeSync(ThreadTurnDispatchGuard)(decoded), guard);
  const decode = Schema.decodeUnknownOption(ThreadTurnDispatchGuard);
  assert.isTrue(Option.isNone(decode({ ...guard, requireIdle: false })));
  assert.isTrue(Option.isNone(decode({ ...guard, expectedActiveTurnId: undefined })));
  assert.isTrue(Option.isNone(decode({ ...guard, expectedSessionStatus: "unknown" })));
});

it("retains original V1 bootstrap and message metadata without translating turn IDs into runs", () => {
  const wire = {
    type: "thread.turn.start",
    commandId: "command-1",
    threadId: "thread-1",
    message: { messageId: "message-1", role: "user", text: "original prompt", attachments: [] },
    modelSelection: { instanceId: "codex_work", model: "model-1" },
    titleSeed: "original title",
    runtimeMode: "full-access",
    interactionMode: "default",
    bootstrap: {
      createThread: {
        projectId: "project-1",
        title: "thread",
        modelSelection: { instanceId: "codex_work", model: "model-1" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdAt: "2026-10-02T12:00:00Z",
      },
      prepareWorktree: {
        projectCwd: "/workspace/project",
        baseBranch: "main",
        startFromOrigin: false,
      },
      runSetupScript: false,
    },
    sourceProposedPlan: { threadId: "source-thread", planId: "plan-1" },
    dispatchGuard: guard,
    createdAt: "2026-10-02T12:00:00Z",
  };
  const decoded = Schema.decodeUnknownSync(ThreadTurnStartCommand)(wire);
  assert.deepEqual<unknown>(Schema.encodeSync(ThreadTurnStartCommand)(decoded), wire);
});

it("retains V1 observation omission, correlation and exact string timestamps", () => {
  const wire = {
    threadId: "thread-1",
    commandId: "command-1",
    messageId: "message-1",
    snapshotSequence: 12,
    commandStatus: "accepted",
    acceptedSequence: 11,
    correlation: "exact",
    turn: {
      turnId: "historical-turn-1",
      state: "completed",
      requestedAt: "2026-10-02T12:00:00Z",
      startedAt: "2026-10-02T12:00:01.123Z",
      completedAt: "2026-10-02T12:00:02Z",
      assistantMessageId: null,
    },
    target: null,
  };
  const decoded = Schema.decodeUnknownSync(OrchestrationCommandObservation)(wire);
  assert.strictEqual(decoded.creation, undefined);
  assert.strictEqual(decoded.turn?.turnId, "historical-turn-1");
  assert.deepEqual<unknown>(Schema.encodeSync(OrchestrationCommandObservation)(decoded), wire);
});

// Voice e35a1974 producer vectors retained from the parent preparation conformance suite.
const producerCommands = [
  {
    command: {
      bootstrap: {
        createThread: {
          branch: null,
          createdAt: "2026-10-02T12:34:56Z",
          interactionMode: "default",
          modelSelection: {
            instanceId: "codex",
            model: "fixture-model",
          },
          projectId: "fixture-project",
          runtimeMode: "full-access",
          title: "Synthetic thread",
          worktreePath: null,
        },
        prepareWorktree: {
          baseBranch: "main",
          branch: "t3code/voice-aa4c25ff545a65b500bd7830",
          projectCwd: "/fixture/project",
          requireWorktree: true,
          startFromOrigin: false,
        },
        runSetupScript: false,
      },
      commandId: "voice-command-aa4c25ff545a65b500bd783049ea3587e2e7e1e9f2df8b104493d0d95eb20af0",
      createdAt: "2026-10-02T12:34:56Z",
      interactionMode: "default",
      message: {
        attachments: [],
        messageId: "voice-message-aa4c25ff545a65b500bd783049ea3587e2e7e1e9f2df8b104493d0d95eb20af0",
        role: "user",
        text: "Create a test thread",
      },
      runtimeMode: "full-access",
      threadId: "voice-thread-aa4c25ff545a65b500bd783049ea3587e2e7e1e9f2df8b104493d0d95eb20af0",
      type: "thread.turn.start",
    },
    commandDigest: "1b0f82e7cfe4a3ef0e23c846464b329039a8b85229b4a712b80d63ce85136998",
  },
  {
    command: {
      bootstrap: {
        createThread: {
          branch: null,
          createdAt: "2026-10-02T08:34:56.123456-04:00",
          interactionMode: "default",
          modelSelection: {
            instanceId: "codex",
            model: "fixture-model",
            options: [
              {
                id: "z",
                value: true,
              },
              {
                id: "a",
                value: "high",
              },
            ],
          },
          projectId: "fixture-project",
          runtimeMode: "full-access",
          title: "Unicode 雪",
          worktreePath: null,
        },
        prepareWorktree: {
          baseBranch: "main",
          branch: "t3code/voice-ed9ca90d0042c1268996a855",
          projectCwd: "/fixture/project",
          requireWorktree: true,
          startFromOrigin: false,
        },
        runSetupScript: false,
      },
      commandId: "voice-command-ed9ca90d0042c1268996a8551a5b5029b1a6c27e60c1def1bce176e849c5d11b",
      createdAt: "2026-10-02T08:34:56.123456-04:00",
      interactionMode: "default",
      message: {
        attachments: [],
        messageId: "voice-message-ed9ca90d0042c1268996a8551a5b5029b1a6c27e60c1def1bce176e849c5d11b",
        role: "user",
        text: "雪 🧪 é\n\t\b\f\r\u0000\u001f ",
      },
      runtimeMode: "full-access",
      threadId: "voice-thread-ed9ca90d0042c1268996a8551a5b5029b1a6c27e60c1def1bce176e849c5d11b",
      type: "thread.turn.start",
    },
    commandDigest: "f1204c723d5f98d3fd47e10a339b0a5fa845cd6e95036fc6332793ae9b955ed2",
  },
  {
    command: {
      bootstrap: {
        createThread: {
          branch: null,
          createdAt: "2026-10-02T12:34:56+00:00",
          interactionMode: "default",
          modelSelection: {
            instanceId: "codex",
            model: "fixture-model",
            options: [
              {
                id: "a",
                value: "high",
              },
              {
                id: "z",
                value: false,
              },
            ],
          },
          projectId: "fixture-project",
          runtimeMode: "full-access",
          title: "Sorted object keys",
          worktreePath: null,
        },
        prepareWorktree: {
          baseBranch: "main",
          branch: "t3code/voice-449c049ec4eec031daba9c09",
          projectCwd: "/fixture/project",
          requireWorktree: true,
          startFromOrigin: false,
        },
        runSetupScript: false,
      },
      commandId: "voice-command-449c049ec4eec031daba9c0948a3ef09d734f03519e7e88c8d3c788ab39d9089",
      createdAt: "2026-10-02T12:34:56+00:00",
      interactionMode: "default",
      message: {
        attachments: [],
        messageId: "voice-message-449c049ec4eec031daba9c0948a3ef09d734f03519e7e88c8d3c788ab39d9089",
        role: "user",
        text: "Options with reverse IDs",
      },
      runtimeMode: "full-access",
      threadId: "voice-thread-449c049ec4eec031daba9c0948a3ef09d734f03519e7e88c8d3c788ab39d9089",
      type: "thread.turn.start",
    },
    commandDigest: "de42b4bf7f8a1ccda235f68d410285210d65809721aaa383896fc8c93fcc38fa",
  },
  {
    command: {
      bootstrap: {
        createThread: {
          branch: null,
          createdAt: "20261002T123456Z",
          interactionMode: "default",
          modelSelection: {
            instanceId: "codex",
            model: "fixture-model",
          },
          projectId: "fixture-project",
          runtimeMode: "full-access",
          title: "Basic timestamp",
          worktreePath: null,
        },
        prepareWorktree: {
          baseBranch: "main",
          branch: "t3code/voice-f4993fe3682795c161932b71",
          projectCwd: "/fixture/project",
          requireWorktree: true,
          startFromOrigin: false,
        },
        runSetupScript: false,
      },
      commandId: "voice-command-f4993fe3682795c161932b71d42efd03b223fbbf21eb4ed3fa06253667245cb2",
      createdAt: "20261002T123456Z",
      interactionMode: "default",
      message: {
        attachments: [],
        messageId: "voice-message-f4993fe3682795c161932b71d42efd03b223fbbf21eb4ed3fa06253667245cb2",
        role: "user",
        text: "Basic time",
      },
      runtimeMode: "full-access",
      threadId: "voice-thread-f4993fe3682795c161932b71d42efd03b223fbbf21eb4ed3fa06253667245cb2",
      type: "thread.turn.start",
    },
    commandDigest: "c12bd874f9d5d3c5b96eb80b3a4991470969e62e48b8adaae5ca4ec5a0db94cf",
  },
  {
    command: {
      bootstrap: {
        createThread: {
          branch: null,
          createdAt: "2026-W40-5T12:34:56+00:00",
          interactionMode: "default",
          modelSelection: {
            instanceId: "codex",
            model: "fixture-model",
          },
          projectId: "fixture-project",
          runtimeMode: "full-access",
          title: "Week timestamp",
          worktreePath: null,
        },
        prepareWorktree: {
          baseBranch: "main",
          branch: "t3code/voice-71cf8b0fc216d903bf9cacfc",
          projectCwd: "/fixture/project",
          requireWorktree: true,
          startFromOrigin: false,
        },
        runSetupScript: false,
      },
      commandId: "voice-command-71cf8b0fc216d903bf9cacfc071918afddddfbcbf130b735096558b3d7cfaaf8",
      createdAt: "2026-W40-5T12:34:56+00:00",
      interactionMode: "default",
      message: {
        attachments: [],
        messageId: "voice-message-71cf8b0fc216d903bf9cacfc071918afddddfbcbf130b735096558b3d7cfaaf8",
        role: "user",
        text: "Week time",
      },
      runtimeMode: "full-access",
      threadId: "voice-thread-71cf8b0fc216d903bf9cacfc071918afddddfbcbf130b735096558b3d7cfaaf8",
      type: "thread.turn.start",
    },
    commandDigest: "0c3e84d9661217b0d203dbd4d92271260439b93615e50c08812d7c7e5f01ab19",
  },
  {
    command: {
      bootstrap: {
        createThread: {
          branch: null,
          createdAt: "2026-10-02T12:34:56+00:00:30.123456",
          interactionMode: "default",
          modelSelection: {
            instanceId: "codex",
            model: "fixture-model",
          },
          projectId: "fixture-project",
          runtimeMode: "full-access",
          title: "Offset timestamp",
          worktreePath: null,
        },
        prepareWorktree: {
          baseBranch: "main",
          branch: "t3code/voice-0bc8df814f12afbc689cea1e",
          projectCwd: "/fixture/project",
          requireWorktree: true,
          startFromOrigin: false,
        },
        runSetupScript: false,
      },
      commandId: "voice-command-0bc8df814f12afbc689cea1ee5069d237500e8e1fb49c3377a83f3a2e5da8e55",
      createdAt: "2026-10-02T12:34:56+00:00:30.123456",
      interactionMode: "default",
      message: {
        attachments: [],
        messageId: "voice-message-0bc8df814f12afbc689cea1ee5069d237500e8e1fb49c3377a83f3a2e5da8e55",
        role: "user",
        text: "Offset seconds",
      },
      runtimeMode: "full-access",
      threadId: "voice-thread-0bc8df814f12afbc689cea1ee5069d237500e8e1fb49c3377a83f3a2e5da8e55",
      type: "thread.turn.start",
    },
    commandDigest: "569f7a5853737ca0d57e03100278f0f5e28c7d3762e973e4f2d3485c42158ceb",
  },
  {
    command: {
      bootstrap: {
        createThread: {
          branch: null,
          createdAt: "2026-10-02T12:34:56,123456+02",
          interactionMode: "default",
          modelSelection: {
            instanceId: "codex",
            model: "fixture-model",
          },
          projectId: "fixture-project",
          runtimeMode: "full-access",
          title: "Comma timestamp",
          worktreePath: null,
        },
        prepareWorktree: {
          baseBranch: "main",
          branch: "t3code/voice-e17fa5c9bb7169b6b33e59b0",
          projectCwd: "/fixture/project",
          requireWorktree: true,
          startFromOrigin: false,
        },
        runSetupScript: false,
      },
      commandId: "voice-command-e17fa5c9bb7169b6b33e59b0b86491e81bf998b82cdca851eb62205f0a7a17ae",
      createdAt: "2026-10-02T12:34:56,123456+02",
      interactionMode: "default",
      message: {
        attachments: [],
        messageId: "voice-message-e17fa5c9bb7169b6b33e59b0b86491e81bf998b82cdca851eb62205f0a7a17ae",
        role: "user",
        text: "Comma fraction",
      },
      runtimeMode: "full-access",
      threadId: "voice-thread-e17fa5c9bb7169b6b33e59b0b86491e81bf998b82cdca851eb62205f0a7a17ae",
      type: "thread.turn.start",
    },
    commandDigest: "bf84eebafafcf50b8ce70609fa5ea6b129a195441c3e47d106d0560b1759e442",
  },
  {
    command: {
      bootstrap: {
        createThread: {
          branch: null,
          createdAt: "2026-10-02T12:34:56Z",
          interactionMode: "default",
          modelSelection: {
            instanceId: "codex",
            model: "fixture-model",
          },
          projectId: "fixture-project",
          runtimeMode: "full-access",
          title: "﻿Synthetic title﻿",
          worktreePath: null,
        },
        prepareWorktree: {
          baseBranch: "main",
          branch: "t3code/voice-053a14ad96571aa95e4bb95f",
          projectCwd: "/fixture/project",
          requireWorktree: true,
          startFromOrigin: false,
        },
        runSetupScript: false,
      },
      commandId: "voice-command-053a14ad96571aa95e4bb95fd0dc37066e1b533c5ae928c4d5e9716a3f7129ac",
      createdAt: "2026-10-02T12:34:56Z",
      interactionMode: "default",
      message: {
        attachments: [],
        messageId: "voice-message-053a14ad96571aa95e4bb95fd0dc37066e1b533c5ae928c4d5e9716a3f7129ac",
        role: "user",
        text: " ﻿ ",
      },
      runtimeMode: "full-access",
      threadId: "voice-thread-053a14ad96571aa95e4bb95fd0dc37066e1b533c5ae928c4d5e9716a3f7129ac",
      type: "thread.turn.start",
    },
    commandDigest: "f1b5260f71c1ced0eefed180ac62d7c91f4d523cf95e697a118ca3fd98d6b087",
  },
];

it("retains regenerated V1 bootstrap fields, timestamps, option order and canonical command digests", () => {
  for (const vector of producerCommands) {
    const decoded = Schema.decodeUnknownSync(LegacyNativeBootstrapCommandV1)(vector.command);
    const encoded = Schema.encodeSync(LegacyNativeBootstrapCommandV1)(decoded);
    assert.deepEqual<unknown>(encoded, vector.command);
    const canonical = JSON.stringify(encoded, (_key, child: unknown) => {
      if (child !== null && typeof child === "object" && !Array.isArray(child)) {
        return Object.fromEntries(
          Object.entries(child).sort(([left], [right]) =>
            left < right ? -1 : left > right ? 1 : 0,
          ),
        );
      }
      return child;
    });
    assert.strictEqual(createHash("sha256").update(canonical).digest("hex"), vector.commandDigest);
    const decode = Schema.decodeUnknownOption(LegacyNativeBootstrapCommandV1);
    assert.isTrue(Option.isNone(decode({ ...vector.command, bootstrap: undefined })));
    assert.isTrue(
      Option.isNone(
        decode({
          ...vector.command,
          bootstrap: { ...vector.command.bootstrap, prepareWorktree: undefined },
        }),
      ),
    );
    assert.isTrue(
      Option.isNone(
        decode({
          ...vector.command,
          bootstrap: { ...vector.command.bootstrap, runSetupScript: undefined },
        }),
      ),
    );
  }
});

it("keeps the producer carrier closed while ordinary historical title decoding still trims", () => {
  const vector = producerCommands.at(-1);
  if (vector === undefined)
    throw new Error("The retained producer vectors must include the Python strip fixture");
  const command = vector.command;
  const decode = Schema.decodeUnknownOption(LegacyNativeBootstrapCommandV1);
  for (const wire of [
    { ...command, extra: true },
    { ...command, bootstrap: { ...command.bootstrap, extra: true } },
    {
      ...command,
      bootstrap: {
        ...command.bootstrap,
        prepareWorktree: { ...command.bootstrap.prepareWorktree, extra: true },
      },
    },
  ])
    assert.isTrue(Option.isNone(decode(wire)));
  const historical = Schema.decodeUnknownSync(ThreadTurnStartCommand)(command);
  assert.strictEqual(historical.bootstrap?.createThread?.title, "Synthetic title");
});

it("requires a closed V2 command identity with exact kind, version, aggregate and SHA-256 digests", () => {
  const wire = {
    kind: "guarded_message_dispatch",
    version: 2,
    commandId: "command-1",
    commandType: "message.dispatch",
    aggregateKind: "thread",
    aggregateId: "thread-1",
    normalizedCommandDigest: "a".repeat(64),
    bindingDigest: "b".repeat(64),
  };
  const decoded = Schema.decodeUnknownSync(NativeCommandIdentityV2)(wire);
  assert.deepEqual<unknown>(Schema.encodeSync(NativeCommandIdentityV2)(decoded), wire);
  const accepts = Schema.decodeUnknownOption(NativeCommandIdentityV2);
  for (const changed of [
    { version: 1 },
    { kind: "native_command" },
    { aggregateKind: "project" },
    { normalizedCommandDigest: "A".repeat(64) },
    { bindingDigest: "a".repeat(63) },
    { extra: true },
  ])
    assert.isTrue(Option.isNone(accepts({ ...wire, ...changed })));
  assert.isTrue(
    Option.isNone(
      Schema.decodeUnknownOption(NativeThreadIncarnationV2)({
        eventId: "birth-event",
        sequence: 1,
        guessed: true,
      }),
    ),
  );
});

const guardV2 = {
  version: 2,
  observedSnapshotSequence: 12,
  expectedIncarnation: { eventId: "thread-created-event", sequence: 1 },
  expectedModelSelection: { instanceId: "codex_work", model: "model-1" },
  expectedActiveRunId: null,
  expectedLatestRunId: "run-1",
  expectedActiveRunAttemptId: null,
  expectedActiveProviderThreadId: "provider-thread-1",
  expectedProviderSessionId: "provider-session-1",
  expectedProviderSessionStatus: "ready",
  expectedRuntimeGeneration: "launch-generation-1",
  requireIdle: true,
};

it("requires closed V2 idle guard bindings with actual run and provider IDs", () => {
  const decode = Schema.decodeUnknownSync(ThreadTurnDispatchGuardV2);
  assert.deepEqual<unknown>(Schema.encodeSync(ThreadTurnDispatchGuardV2)(decode(guardV2)), guardV2);
  const accepts = Schema.decodeUnknownOption(ThreadTurnDispatchGuardV2);
  for (const changed of [
    { version: 1 },
    { requireIdle: false },
    { expectedActiveRunId: undefined },
    { expectedIncarnation: null },
    { expectedRuntimeGeneration: 1 },
    { extra: true },
    { expectedIncarnation: { ...guardV2.expectedIncarnation, guessed: true } },
  ])
    assert.isTrue(Option.isNone(accepts({ ...guardV2, ...changed })));
  const { expectedRuntimeGeneration: _generation, ...withoutGeneration } = guardV2;
  assert.deepEqual<unknown>(
    Schema.encodeSync(ThreadTurnDispatchGuardV2)(decode(withoutGeneration)),
    withoutGeneration,
  );
});

it("cannot attest incomplete V2 target evidence as idle and rejects contradictory blockers", () => {
  const target = {
    incarnation: guardV2.expectedIncarnation,
    modelSelection: guardV2.expectedModelSelection,
    activeRunId: null,
    latestRunId: "run-1",
    activeRunAttemptId: null,
    activeProviderThreadId: "provider-thread-1",
    providerSessionId: "provider-session-1",
    providerSessionStatus: "ready",
    runtimeGeneration: "launch-generation-1",
    snapshotSequence: 12,
    targetEventSequence: 11,
    complete: true,
    requireIdle: true,
    idle: true,
    blockers: [],
  };
  const decode = Schema.decodeUnknownSync(OrchestrationDispatchTargetV2);
  assert.deepEqual<unknown>(
    Schema.encodeSync(OrchestrationDispatchTargetV2)(decode(target)),
    target,
  );
  const accepts = Schema.decodeUnknownOption(OrchestrationDispatchTargetV2);
  assert.isTrue(Option.isNone(accepts({ ...target, complete: false })));
  assert.isTrue(Option.isNone(accepts({ ...target, complete: false, idle: false })));
  assert.isTrue(Option.isNone(accepts({ ...target, blockers: ["active_run"] })));
  const unknown = {
    ...target,
    incarnation: null,
    complete: false,
    idle: false,
    blockers: ["unknown_evidence"],
  };
  assert.deepEqual<unknown>(
    Schema.encodeSync(OrchestrationDispatchTargetV2)(decode(unknown)),
    unknown,
  );
});

it("keeps V2 receipt, identity and message correlation separate from current execution and settlement", () => {
  const acceptedAt = DateTime.makeUnsafe("2026-10-02T12:00:00.123Z");
  const receipt = {
    commandId: "command-1",
    threadId: "thread-1",
    commandType: "message.dispatch",
    acceptedAt,
    resultSequence: 11,
    status: "accepted",
    error: null,
  };
  const observation = {
    version: 2,
    threadId: "thread-1",
    commandId: "command-1",
    messageId: "message-1",
    commandStatus: "accepted",
    receipt,
    identity: null,
    identityVerification: "unbound",
    correlation: "exact",
    snapshot: { snapshotSequence: 12, targetEventSequence: 11, complete: true },
    correlatedMessageId: "message-1",
    run: {
      runId: "run-1",
      runAttemptId: "attempt-1",
      providerThreadId: "provider-thread-1",
      providerTurnId: "provider-turn-1",
      status: "running",
    },
    target: null,
  };
  const runtime = Schema.decodeUnknownSync(NativeCommandObservationV2)(observation);
  assert.strictEqual(runtime.correlation, "exact");
  assert.strictEqual(runtime.run?.status, "running");
  assert.strictEqual(runtime.identityVerification, "unbound");
  const wire = {
    ...observation,
    receipt: { ...receipt, acceptedAt: DateTime.formatIso(acceptedAt) },
  };
  const decoded = Schema.decodeUnknownSync(NativeCommandObservationV2Json)(wire);
  assert.deepEqual<unknown>(Schema.encodeSync(NativeCommandObservationV2Json)(decoded), wire);
  const missing = {
    ...wire,
    commandStatus: "not_found",
    receipt: null,
    correlation: "missing",
    correlatedMessageId: null,
    run: null,
  };
  assert.deepEqual<unknown>(
    Schema.encodeSync(NativeCommandObservationV2Json)(
      Schema.decodeUnknownSync(NativeCommandObservationV2Json)(missing),
    ),
    missing,
  );
  assert.isTrue(
    Option.isNone(
      Schema.decodeUnknownOption(NativeCommandObservationV2Json)({
        ...wire,
        receipt: { ...wire.receipt, resultSequence: -1 },
      }),
    ),
  );
});

const nativeEffectV2 = {
  version: 2,
  kind: "native_command",
  phase: "started",
  effectId: "effect-v2-1",
  ordinal: 2,
  timestamp: "2026-10-02T12:00:00.123456Z",
  commandId: "logical-command-1",
  threadId: "thread-1",
  commandType: "prepared-run.release",
  commandDigest: "b".repeat(64),
};
const nativeEffectV1 = {
  effectId: "effect-v1-1",
  ordinal: 0,
  timestamp: "2026-10-02T12:00:00Z",
  kind: "fetch",
  phase: "started",
  projectCwd: "/workspace/project",
  baseRef: "main",
};
const creationReceipt = {
  commandId: "logical-command-1",
  threadId: "thread-1",
  commandType: "prepared-run.release",
  acceptedAt: "2026-10-02T12:00:01.123Z",
  resultSequence: 5,
  status: "accepted",
  error: null,
};
const publicCreation = {
  version: 2,
  schema: "t3.native-creation-observation/v2",
  preparationId: "preparation-1",
  operationId: "operation-1",
  preparationSha256: "c".repeat(64),
  bindingDigest: "d".repeat(64),
  promptDigest: "e".repeat(64),
  commandDigest: "a".repeat(64),
  normalizedCommandDigest: null,
  claimId: "claim-1",
  claimedBootId: "boot-1",
  claimedAt: "2026-10-02T12:00:00Z",
  actorSessionId: "session-1",
  grantId: "grant-1",
  grantRevision: 1,
  binding: {
    backendInstance: "backend-1",
    environmentId: "environment-1",
    projectId: "project-1",
    projectCwd: "/workspace/project",
    accountRef: "account-ref-1",
    accountBindingId: "account-binding-1",
    accountBindingRevision: 1,
    providerModelSelection: { instanceId: "codex", model: "model-1" },
    runtimeMode: "full-access",
    interactionMode: "default",
    baseBranch: "main",
    startFromOrigin: true,
    runSetupScript: false,
    requestedBranch: "t3code/voice-branch",
  },
  incarnation: { eventId: "thread-created-event", sequence: 1 },
  stageCommands: [
    {
      claimId: "claim-1",
      commandId: "logical-command-1",
      threadId: "thread-1",
      commandType: "prepared-run.release",
      commandDigest: "b".repeat(64),
      receipt: creationReceipt,
      event: { eventId: "release-event", sequence: 5 },
    },
  ],
  finalReceipt: creationReceipt,
  effectsV1: [nativeEffectV1],
  effectsV2: [nativeEffectV2],
  unresolvedEffects: ["effect-v1-1", "effect-v2-1"],
  outcome: "unknown",
  overflow: false,
};

it("keeps exactly three V2 native command types separate from original V1 effects", () => {
  const decode = Schema.decodeUnknownSync(NativeCreationEffectV2);
  const accepts = Schema.decodeUnknownOption(NativeCreationEffectV2);
  for (const commandType of ["thread.create", "message.dispatch", "prepared-run.release"]) {
    const started = { ...nativeEffectV2, commandType };
    const completed = { ...started, phase: "completed", eventId: "stage-event", sequence: 5 };
    for (const fact of [started, completed]) {
      assert.deepEqual<unknown>(Schema.encodeSync(NativeCreationEffectV2)(decode(fact)), fact);
      assert.isTrue(Option.isNone(Schema.decodeUnknownOption(NativeCreationEffect)(fact)));
    }
  }
  for (const changed of [
    { version: undefined },
    { version: 1 },
    { commandType: "thread.turn.start" },
    { commandType: "thread.delete" },
    { commandType: "run.cancel" },
    { commandDigest: "A".repeat(64) },
    { phase: "completed" },
    { eventId: "unexpected-start-event", sequence: 5 },
    { canonicalCommand: "{}" },
  ])
    assert.isTrue(Option.isNone(accepts({ ...nativeEffectV2, ...changed })));
  const original = {
    effectId: "original-command-effect",
    ordinal: 0,
    timestamp: "2026-10-02T12:00:00Z",
    kind: "native_command",
    phase: "started",
    commandId: "original-command",
    threadId: "thread-1",
    commandType: "thread.turn.start",
    commandDigest: "a".repeat(64),
  };
  const v1 = Schema.decodeUnknownSync(NativeCreationEffect)(original);
  assert.deepEqual<unknown>(Schema.encodeSync(NativeCreationEffect)(v1), original);
});

it("round-trips public V2 creation receipts while retaining separate original and stage digests", () => {
  const creation = Schema.decodeUnknownSync(NativeCreationObservationV2Json)(publicCreation);
  assert.deepEqual<unknown>(
    Schema.encodeSync(NativeCreationObservationV2Json)(creation),
    publicCreation,
  );
  assert.strictEqual(creation.commandDigest, "a".repeat(64));
  assert.strictEqual(creation.normalizedCommandDigest, null);
  assert.strictEqual(creation.stageCommands[0]?.commandDigest, "b".repeat(64));
  assert.deepEqual<unknown>(creation.effectsV1, [nativeEffectV1]);
  assert.deepEqual<unknown>(creation.effectsV2, [nativeEffectV2]);
  const runtime = Schema.decodeUnknownSync(NativeCreationObservationV2)(creation);
  assert.deepEqual<unknown>(Schema.encodeSync(NativeCreationObservationV2)(runtime), creation);
  const recordedCompatibility = { ...publicCreation, normalizedCommandDigest: "f".repeat(64) };
  assert.deepEqual<unknown>(
    Schema.encodeSync(NativeCreationObservationV2Json)(
      Schema.decodeUnknownSync(NativeCreationObservationV2Json)(recordedCompatibility),
    ),
    recordedCompatibility,
  );
  const observation = {
    version: 2,
    threadId: "thread-1",
    commandId: "logical-command-1",
    messageId: "message-1",
    commandStatus: "accepted",
    identity: null,
    identityVerification: "unbound",
    correlation: "exact",
    snapshot: { snapshotSequence: 6, targetEventSequence: 5, complete: true },
    correlatedMessageId: "message-1",
    run: null,
    target: null,
    receipt: creationReceipt,
    creation: publicCreation,
  };
  const decoded = Schema.decodeUnknownSync(NativeCommandObservationV2Json)(observation);
  assert.strictEqual(decoded.creation?.outcome, "unknown");
  assert.deepEqual<unknown>(
    Schema.encodeSync(NativeCommandObservationV2Json)(decoded),
    observation,
  );
  assert.deepEqual<unknown>(
    Schema.encodeSync(Schema.toCodecJson(NativeCommandObservationV2))(decoded),
    observation,
  );
  assert.isTrue(
    Option.isNone(
      Schema.decodeUnknownOption(NativeCommandObservationV2Json)({
        ...observation,
        creation: null,
      }),
    ),
  );
});

it("bounds V2 creation facts and stages and requires unknown outcomes on overflow", () => {
  const decode = Schema.decodeUnknownSync(NativeCreationObservationV2Json);
  const accepts = Schema.decodeUnknownOption(NativeCreationObservationV2Json);
  const releaseStage = publicCreation.stageCommands[0];
  if (releaseStage === undefined)
    throw new Error("The public fixture must retain its release attribution");
  const stageCommands = [
    {
      ...releaseStage,
      commandId: "logical-command-1:native:v2:create",
      commandType: "thread.create",
      commandDigest: "c".repeat(64),
      receipt: {
        ...creationReceipt,
        commandId: "logical-command-1:native:v2:create",
        commandType: "thread.create",
        resultSequence: 1,
      },
      event: { eventId: "thread-created-event", sequence: 1 },
    },
    {
      ...releaseStage,
      commandId: "logical-command-1:native:v2:message",
      commandType: "message.dispatch",
      commandDigest: "d".repeat(64),
      receipt: {
        ...creationReceipt,
        commandId: "logical-command-1:native:v2:message",
        commandType: "message.dispatch",
        resultSequence: 3,
      },
      event: { eventId: "message-dispatched-event", sequence: 3 },
    },
    releaseStage,
  ];
  const bounded = {
    ...publicCreation,
    stageCommands,
    effectsV1: Array.from({ length: 256 }, (_, ordinal) => ({
      ...nativeEffectV1,
      ordinal,
      effectId: `v1-${ordinal}`,
    })),
    effectsV2: Array.from({ length: 256 }, (_, ordinal) => ({
      ...nativeEffectV2,
      ordinal,
      effectId: `v2-${ordinal}`,
    })),
    unresolvedEffects: Array.from({ length: 256 }, (_, ordinal) => `unresolved-${ordinal}`),
  };
  assert.deepEqual<unknown>(
    Schema.encodeSync(NativeCreationObservationV2Json)(decode(bounded)),
    bounded,
  );
  for (const changed of [
    { effectsV1: [...bounded.effectsV1, nativeEffectV1] },
    { effectsV2: [...bounded.effectsV2, nativeEffectV2] },
    { unresolvedEffects: [...bounded.unresolvedEffects, "overflow-effect"] },
    { stageCommands: [...stageCommands, releaseStage] },
  ])
    assert.isTrue(Option.isNone(accepts({ ...bounded, ...changed })));
  const overflow = { ...publicCreation, overflow: true };
  assert.deepEqual<unknown>(
    Schema.encodeSync(NativeCreationObservationV2Json)(decode(overflow)),
    overflow,
  );
  for (const outcome of ["complete", "in_progress", "incomplete"]) {
    assert.isTrue(Option.isNone(accepts({ ...overflow, outcome })));
  }
});

it("rejects internal canonical bodies and cross-version effects in public V2 creation history", () => {
  const accepts = Schema.decodeUnknownOption(NativeCreationObservationV2Json);
  for (const changed of [
    { canonicalPreparation: "{}" },
    { canonicalCommand: "{}" },
    { intent: { canonicalPreparation: "{}" } },
    { binding: { ...publicCreation.binding, canonicalPreparation: "{}" } },
    { stageCommands: [{ ...publicCreation.stageCommands[0], canonicalCommand: "{}" }] },
    { effectsV1: [nativeEffectV2] },
    { effectsV2: [nativeEffectV1] },
  ])
    assert.isTrue(Option.isNone(accepts({ ...publicCreation, ...changed })));
});

const importedReview = {
  version: 2,
  threadId: "thread-1",
  target: { type: "queued_run", runId: "held-run-1", messageId: "held-message-1" },
  capability: { startWithImportedHistory: true },
  applicability: "imported",
  qualification: { type: "unsupported", reason: "native_resume_unsupported" },
  restoredBinding: { type: "missing", reason: "binding_missing" },
  nativeEffects: { type: "clear" },
  transcriptEligibility: { type: "eligible" },
  reviewedBasis: "reviewed-basis-1",
};
const importedReceipt = {
  version: 2,
  commandId: "imported-start-1",
  threadId: "thread-1",
  target: importedReview.target,
  reviewedBasis: importedReview.reviewedBasis,
  intentStatus: "accepted",
  receipt: {
    commandId: "imported-start-1",
    threadId: "thread-1",
    commandType: "thread.imported-history.start",
    acceptedAt: "2026-10-03T01:00:00.000Z",
    resultSequence: 11,
    status: "accepted",
    error: null,
  },
  rejectionReason: null,
  execution: {
    status: "pending",
    runId: "held-run-1",
    providerThreadId: null,
    providerSessionId: null,
    nativeThreadId: null,
    effectOutcome: null,
    error: null,
  },
};

it("preserves separate imported qualification, restored binding, eligibility and target facts", () => {
  const decode = Schema.decodeUnknownSync(OrchestrationV2ImportedHistoryReviewResult);
  for (const target of [importedReview.target, { type: "message", messageId: "new-message-1" }]) {
    const wire = { ...importedReview, target };
    assert.deepEqual<unknown>(
      Schema.encodeSync(OrchestrationV2ImportedHistoryReviewResult)(decode(wire)),
      wire,
    );
  }
  const qualified = {
    ...importedReview,
    qualification: { type: "qualified" },
    reviewedBasis: null,
    restoredBinding: {
      type: "ready",
      providerThreadId: "provider-thread-1",
      providerInstanceId: "codex_work",
      driver: "codex",
      nativeThreadId: "native-thread-1",
      providerSessionId: "provider-session-1",
      runtimeGeneration: "current-generation-1",
    },
  };
  assert.deepEqual<unknown>(
    Schema.encodeSync(OrchestrationV2ImportedHistoryReviewResult)(decode(qualified)),
    qualified,
  );
  const held = {
    ...qualified,
    restoredBinding: { type: "unknown", reason: "current_binding_unproved" },
  };
  assert.deepEqual<unknown>(
    Schema.encodeSync(OrchestrationV2ImportedHistoryReviewResult)(decode(held)),
    held,
  );
  for (const qualification of [
    { type: "unknown", reason: "source_unproved" },
    { type: "unsupported", reason: "native_resume_unsupported" },
  ])
    assert.isTrue(
      Option.isSome(
        Schema.decodeUnknownOption(OrchestrationV2ImportedHistoryReviewResult)({
          ...importedReview,
          qualification,
        }),
      ),
    );
});

it("rejects a reviewed basis unless imported capability, eligibility and clear native effects are positive", () => {
  const accepts = Schema.decodeUnknownOption(OrchestrationV2ImportedHistoryReviewResult);
  for (const changed of [
    { applicability: "unknown" },
    { applicability: "not_imported" },
    { capability: { startWithImportedHistory: false } },
    { qualification: { type: "qualified" } },
    { nativeEffects: { type: "unknown", reason: "effect_unresolved" } },
    { transcriptEligibility: { type: "ineligible", reason: "transcript_absent" } },
    { transcriptEligibility: { type: "unknown", reason: "coverage_incomplete" } },
  ]) {
    assert.isTrue(Option.isNone(accepts({ ...importedReview, ...changed })));
    assert.isTrue(Option.isSome(accepts({ ...importedReview, ...changed, reviewedBasis: null })));
  }
});

it("keeps intent acceptance separate from unknown or pending execution and preserves receipt DateTime JSON parity", () => {
  const decode = Schema.decodeUnknownSync(OrchestrationV2ImportedHistoryStartReceiptJson);
  const runtime = decode(importedReceipt);
  assert.isTrue(DateTime.isDateTime(runtime.receipt?.acceptedAt));
  assert.strictEqual(runtime.intentStatus, "accepted");
  assert.strictEqual(runtime.execution.status, "pending");
  assert.deepEqual<unknown>(
    Schema.encodeSync(OrchestrationV2ImportedHistoryStartReceiptJson)(runtime),
    importedReceipt,
  );
  assert.deepEqual<unknown>(
    Schema.encodeSync(Schema.toCodecJson(OrchestrationV2ImportedHistoryStartReceipt))(runtime),
    importedReceipt,
  );
  const uncertain = {
    ...importedReceipt,
    intentStatus: "rejected",
    rejectionReason: "native_effect_unknown",
    receipt: { ...importedReceipt.receipt, status: "rejected", error: "native_effect_unknown" },
    execution: {
      ...importedReceipt.execution,
      status: "unknown",
      effectOutcome: "unknown",
      error: "native_effect_unknown",
    },
  };
  assert.deepEqual<unknown>(
    Schema.encodeSync(OrchestrationV2ImportedHistoryStartReceiptJson)(decode(uncertain)),
    uncertain,
  );
  const missing = {
    ...importedReceipt,
    reviewedBasis: null,
    intentStatus: "not_found",
    receipt: null,
    execution: { ...importedReceipt.execution, status: "not_started" },
  };
  assert.deepEqual<unknown>(
    Schema.encodeSync(OrchestrationV2ImportedHistoryStartReceiptJson)(decode(missing)),
    missing,
  );
});

it("requires complete success and actual binding identifiers to represent started imported execution", () => {
  const execution = {
    status: "started",
    runId: "held-run-1",
    providerThreadId: "fresh-provider-thread-1",
    providerSessionId: "fresh-provider-session-1",
    nativeThreadId: "fresh-native-thread-1",
    runtimeGeneration: "fresh-generation-1",
    effectOutcome: "confirmed_success",
    error: null,
  };
  const wire = { ...importedReceipt, execution };
  const schema = OrchestrationV2ImportedHistoryStartReceiptJson;
  assert.deepEqual<unknown>(
    Schema.encodeSync(schema)(Schema.decodeUnknownSync(schema)(wire)),
    wire,
  );
  for (const changed of [
    { effectOutcome: "unknown" },
    { effectOutcome: "known_no_effect" },
    { effectOutcome: null },
    { runId: null },
    { providerThreadId: null },
    { providerSessionId: null },
    { nativeThreadId: null },
    { runtimeGeneration: 4 },
  ])
    assert.isTrue(
      Option.isNone(
        Schema.decodeUnknownOption(schema)({ ...wire, execution: { ...execution, ...changed } }),
      ),
    );
});

it("excludes raw transcript, source keys and canonical payloads from imported review and receipt metadata", () => {
  const review = Schema.decodeUnknownOption(OrchestrationV2ImportedHistoryReviewResult);
  const receipt = Schema.decodeUnknownOption(OrchestrationV2ImportedHistoryStartReceiptJson);
  for (const extra of [
    { transcript: "private transcript" },
    { canonicalPreparation: "{}" },
    { sourceStoreKey: "private store" },
    { snapshot: {} },
  ]) {
    assert.isTrue(Option.isNone(review({ ...importedReview, ...extra })));
    assert.isTrue(Option.isNone(receipt({ ...importedReceipt, ...extra })));
  }
  assert.isTrue(
    Option.isNone(
      review({ ...importedReview, target: { ...importedReview.target, text: "replacement" } }),
    ),
  );
  assert.isTrue(
    Option.isNone(
      receipt({
        ...importedReceipt,
        execution: { ...importedReceipt.execution, canonicalCommand: "{}" },
      }),
    ),
  );
});

const acceptedBootstrapStages = [
  {
    claimId: publicCreation.claimId,
    commandId: "logical-command-1:native:v2:create",
    threadId: "thread-1",
    commandType: "thread.create",
    commandDigest: "1".repeat(64),
    event: publicCreation.incarnation,
    receipt: {
      ...creationReceipt,
      commandId: "logical-command-1:native:v2:create",
      commandType: "thread.create",
      resultSequence: 1,
    },
  },
  {
    claimId: publicCreation.claimId,
    commandId: "logical-command-1:native:v2:message",
    threadId: "thread-1",
    commandType: "message.dispatch",
    commandDigest: "2".repeat(64),
    event: { eventId: "message-event", sequence: 3 },
    receipt: {
      ...creationReceipt,
      commandId: "logical-command-1:native:v2:message",
      commandType: "message.dispatch",
      resultSequence: 3,
    },
  },
  publicCreation.stageCommands[0]!,
];
const acceptedBootstrapResult = {
  version: 2,
  commandId: "logical-command-1",
  threadId: "thread-1",
  messageId: "message-1",
  commandAcceptance: "accepted",
  creation: { ...publicCreation, stageCommands: acceptedBootstrapStages },
};

it("separates three-stage native command acceptance from unresolved external creation work", () => {
  const schema = NativeBootstrapDispatchResultV2Json;
  for (const outcome of ["unknown", "in_progress", "incomplete", "complete"]) {
    const wire = {
      ...acceptedBootstrapResult,
      creation: { ...acceptedBootstrapResult.creation, outcome },
    };
    const decoded = Schema.decodeUnknownSync(schema)(wire);
    assert.strictEqual(decoded.commandAcceptance, "accepted");
    assert.strictEqual(decoded.creation?.outcome, outcome);
    assert.deepEqual<unknown>(Schema.encodeSync(schema)(decoded), wire);
    assert.deepEqual<unknown>(
      Schema.encodeSync(Schema.toCodecJson(NativeBootstrapDispatchResultV2))(decoded),
      wire,
    );
  }
  for (const commandAcceptance of ["pending", "rejected", "unknown"]) {
    const wire = { ...acceptedBootstrapResult, commandAcceptance, creation: null };
    assert.deepEqual<unknown>(
      Schema.encodeSync(schema)(Schema.decodeUnknownSync(schema)(wire)),
      wire,
    );
  }
});

it("requires exact native stage IDs, command types, claim attribution and accepted matching receipts", () => {
  const accepts = Schema.decodeUnknownOption(NativeBootstrapDispatchResultV2Json);
  for (const changed of [
    { stageCommands: acceptedBootstrapStages.slice(0, 2) },
    {
      stageCommands: [
        acceptedBootstrapStages[1],
        acceptedBootstrapStages[0],
        acceptedBootstrapStages[2],
      ],
    },
    {
      stageCommands: acceptedBootstrapStages.map((stage, index) =>
        index === 0 ? { ...stage, commandId: "ordinary-create" } : stage,
      ),
    },
    {
      stageCommands: acceptedBootstrapStages.map((stage, index) =>
        index === 1 ? { ...stage, commandType: "thread.create" } : stage,
      ),
    },
    {
      stageCommands: acceptedBootstrapStages.map((stage, index) =>
        index === 1 ? { ...stage, claimId: "other-claim" } : stage,
      ),
    },
    {
      stageCommands: acceptedBootstrapStages.map((stage, index) =>
        index === 1 ? { ...stage, threadId: "other-thread" } : stage,
      ),
    },
    {
      stageCommands: acceptedBootstrapStages.map((stage, index) =>
        index === 1 ? { ...stage, commandDigest: "A".repeat(64) } : stage,
      ),
    },
    {
      stageCommands: acceptedBootstrapStages.map((stage, index) =>
        index === 1 ? { ...stage, receipt: null } : stage,
      ),
    },
    {
      stageCommands: acceptedBootstrapStages.map((stage, index) =>
        index === 1
          ? { ...stage, receipt: { ...stage.receipt, commandId: "unrelated-command" } }
          : stage,
      ),
    },
    {
      stageCommands: acceptedBootstrapStages.map((stage, index) =>
        index === 1 ? { ...stage, receipt: { ...stage.receipt, threadId: "other-thread" } } : stage,
      ),
    },
    {
      stageCommands: acceptedBootstrapStages.map((stage, index) =>
        index === 1
          ? { ...stage, receipt: { ...stage.receipt, commandType: "thread.create" } }
          : stage,
      ),
    },
    {
      stageCommands: acceptedBootstrapStages.map((stage, index) =>
        index === 1
          ? { ...stage, receipt: { ...stage.receipt, status: "rejected", error: "conflict" } }
          : stage,
      ),
    },
  ])
    assert.isTrue(
      Option.isNone(
        accepts({
          ...acceptedBootstrapResult,
          creation: { ...acceptedBootstrapResult.creation, ...changed },
        }),
      ),
    );
});

it("requires increasing result-event sequences, actual birth and final release receipt attribution", () => {
  const accepts = Schema.decodeUnknownOption(NativeBootstrapDispatchResultV2Json);
  for (const changed of [
    {
      stageCommands: acceptedBootstrapStages.map((stage, index) =>
        index === 1 ? { ...stage, event: null } : stage,
      ),
    },
    {
      stageCommands: acceptedBootstrapStages.map((stage, index) =>
        index === 1 ? { ...stage, event: { eventId: "message-event", sequence: 2 } } : stage,
      ),
    },
    {
      stageCommands: acceptedBootstrapStages.map((stage, index) =>
        index === 1 ? { ...stage, event: { eventId: "thread-created-event", sequence: 3 } } : stage,
      ),
    },
    {
      stageCommands: acceptedBootstrapStages.map((stage, index) =>
        index === 1
          ? {
              ...stage,
              event: { eventId: "message-event", sequence: 1 },
              receipt: { ...stage.receipt, resultSequence: 1 },
            }
          : stage,
      ),
    },
    { incarnation: { eventId: "unrelated-birth", sequence: 1 } },
    { incarnation: null },
    { finalReceipt: null },
    { finalReceipt: { ...creationReceipt, commandId: "unrelated-final-command" } },
    { finalReceipt: { ...creationReceipt, resultSequence: 6 } },
    { finalReceipt: { ...creationReceipt, acceptedAt: "2026-10-03T01:00:01.000Z" } },
    { overflow: true },
  ])
    assert.isTrue(
      Option.isNone(
        accepts({
          ...acceptedBootstrapResult,
          creation: { ...acceptedBootstrapResult.creation, ...changed },
        }),
      ),
    );
  assert.isTrue(Option.isNone(accepts({ ...acceptedBootstrapResult, creation: null })));
});

it("keeps the native bootstrap result closed and rejects raw canonical or scalar completion fields", () => {
  const accepts = Schema.decodeUnknownOption(NativeBootstrapDispatchResultV2Json);
  for (const extra of [
    { providerCompleted: true },
    { sequence: 5 },
    { canonicalCommand: "{}" },
    { preparationBase64: "e30=" },
  ]) {
    assert.isTrue(Option.isNone(accepts({ ...acceptedBootstrapResult, ...extra })));
  }
});

const currentStopTarget = {
  binding: attachedRuntime.binding,
  driver: attachedRuntime.driver,
  evidenceRevision: attachedRuntime.evidenceRevision,
};
const currentStopResult = {
  version: 2,
  commandId: "stop-current-1",
  threadId: "thread-1",
  target: currentStopTarget,
  commandStatus: "accepted",
  receipt: {
    commandId: "stop-current-1",
    threadId: "thread-1",
    commandType: "provider-session.detach",
    acceptedAt: "2026-10-03T01:00:00.000Z",
    resultSequence: 11,
    status: "accepted",
    error: null,
  },
  queueFence: { status: "installed", affectedRunIds: [] },
  runtimeStop: { status: "pending" },
  reason: null,
};

it("preserves current stop target and receipt JSON while keeping installed acceptance separate from actual stopped", () => {
  const schema = OrchestrationV2StopCurrentThreadRuntimeResultJson;
  for (const runtimeStop of [{ status: "pending" }, { status: "unknown" }, { status: "stopped" }]) {
    const wire = { ...currentStopResult, runtimeStop };
    const decoded = Schema.decodeUnknownSync(schema)(wire);
    assert.deepEqual<unknown>(Schema.encodeSync(schema)(decoded), wire);
    assert.deepEqual<unknown>(
      Schema.encodeSync(Schema.toCodecJson(OrchestrationV2StopCurrentThreadRuntimeResult))(decoded),
      wire,
    );
    assert.strictEqual(decoded.queueFence.affectedRunIds.length, 0);
  }
  const affected = {
    ...currentStopResult,
    queueFence: { status: "installed", affectedRunIds: ["affected-run-1"] },
  };
  assert.deepEqual<unknown>(
    Schema.encodeSync(schema)(Schema.decodeUnknownSync(schema)(affected)),
    affected,
  );
});

it("requires exact nonnull target for accepted, installed or stopped and installed fence for stopped", () => {
  const accepts = Schema.decodeUnknownOption(OrchestrationV2StopCurrentThreadRuntimeResultJson);
  for (const wire of [
    { ...currentStopResult, target: null },
    { ...currentStopResult, commandStatus: "unknown", target: null },
    {
      ...currentStopResult,
      commandStatus: "unknown",
      target: null,
      queueFence: { status: "unknown", affectedRunIds: [] },
      runtimeStop: { status: "stopped" },
    },
    {
      ...currentStopResult,
      queueFence: { status: "unknown", affectedRunIds: [] },
      runtimeStop: { status: "stopped" },
    },
    {
      ...currentStopResult,
      queueFence: { status: "not_installed", affectedRunIds: [] },
      runtimeStop: { status: "stopped" },
    },
    {
      ...currentStopResult,
      target: {
        ...currentStopTarget,
        binding: { ...currentStopTarget.binding, threadId: "other-thread" },
      },
    },
  ])
    assert.isTrue(Option.isNone(accepts(wire)));
});

it("represents absent stop observation without fabricating a historical target", () => {
  const schema = OrchestrationV2StopCurrentThreadRuntimeResultJson;
  for (const commandStatus of ["not_found", "unknown"]) {
    const wire = {
      ...currentStopResult,
      commandStatus,
      target: null,
      receipt: null,
      queueFence: { status: "unknown", affectedRunIds: [] },
      runtimeStop: { status: "not_started" },
    };
    assert.deepEqual<unknown>(
      Schema.encodeSync(schema)(Schema.decodeUnknownSync(schema)(wire)),
      wire,
    );
  }
  assert.isTrue(
    Option.isNone(
      Schema.decodeUnknownOption(schema)({
        ...currentStopResult,
        commandStatus: "not_found",
        receipt: null,
        queueFence: { status: "not_installed", affectedRunIds: [] },
        runtimeStop: { status: "not_started" },
      }),
    ),
  );
});

it("keeps stop targets and result metadata closed with existing string generation and revision", () => {
  const target = Schema.decodeUnknownOption(OrchestrationV2CurrentThreadRuntimeTarget);
  for (const changed of [
    { evidenceRevision: -1 },
    { evidenceRevision: 1.5 },
    { grantId: "caller-grant" },
    { binding: { ...currentStopTarget.binding, runtimeGeneration: 4 } },
    { binding: { ...currentStopTarget.binding, transition: "caller-transition" } },
  ])
    assert.isTrue(Option.isNone(target({ ...currentStopTarget, ...changed })));
  const accepts = Schema.decodeUnknownOption(OrchestrationV2StopCurrentThreadRuntimeResultJson);
  for (const changed of [
    { epoch: 4 },
    { affectedRunIds: ["caller-run"] },
    { queueFence: { ...currentStopResult.queueFence, clearOtherHolds: true } },
    { runtimeStop: { status: "stopped", shutdownAll: true } },
  ])
    assert.isTrue(Option.isNone(accepts({ ...currentStopResult, ...changed })));
});

it("represents absent imported start targets only for unknown or not-found intent without fake identifiers", () => {
  const schema = OrchestrationV2ImportedHistoryStartReceiptJson;
  for (const intentStatus of ["not_found", "unknown"]) {
    const wire = {
      ...importedReceipt,
      intentStatus,
      target: null,
      receipt: null,
      reviewedBasis: null,
      execution: { ...importedReceipt.execution, runId: null, status: "not_started" },
    };
    const decoded = Schema.decodeUnknownSync(schema)(wire);
    assert.strictEqual(decoded.target, null);
    assert.deepEqual<unknown>(Schema.encodeSync(schema)(decoded), wire);
    assert.deepEqual<unknown>(
      Schema.encodeSync(Schema.toCodecJson(OrchestrationV2ImportedHistoryStartReceipt))(decoded),
      wire,
    );
  }
  for (const intentStatus of ["accepted", "rejected"]) {
    assert.isTrue(
      Option.isNone(
        Schema.decodeUnknownOption(schema)({ ...importedReceipt, intentStatus, target: null }),
      ),
    );
  }
});

const currentOperatingCounts = {
  total: 4,
  operating: 2,
  foregroundWaitingApproval: 1,
  foregroundWaitingInput: 1,
  foregroundWaitingPlan: 1,
  backgroundOperating: 2,
  backgroundUnknown: 1,
  snapshotSequence: 12,
  observedAt: "2026-10-03T02:25:30Z",
  backgroundSampledAt: "2026-10-03T02:25:29Z",
};

it("preserves all raw current runtime statuses and exact provider-owner binding without selected-instance projection", () => {
  const schema = OrchestrationV2ThreadRuntimeObservationResult;
  for (const status of ["working", "monitoring", "busy", "idle"]) {
    const wire = {
      threadId: "thread-1",
      observation: {
        status,
        binding: { ...attachedRuntime.binding, instanceId: "codex_owner" },
        observedAt: attachedRuntime.observedAt,
      },
    };
    const decoded = Schema.decodeUnknownSync(schema)(wire);
    assert.deepEqual<unknown>(Schema.encodeSync(schema)(decoded), wire);
    assert.deepEqual<unknown>(Schema.encodeSync(Schema.toCodecJson(schema))(decoded), wire);
  }
});

it("preserves unknown raw observation binding and reason without inventing an observation timestamp", () => {
  const schema = OrchestrationV2ThreadRuntimeObservationResult;
  for (const observation of [
    { status: "unknown", reason: "runtime_not_resident" },
    { status: "unknown", binding: attachedRuntime.binding, reason: "runtime_binding_changed" },
  ]) {
    const wire = { threadId: "thread-1", observation };
    const decoded = Schema.decodeUnknownSync(schema)(wire);
    assert.deepEqual<unknown>(Schema.encodeSync(schema)(decoded), wire);
    assert.deepEqual<unknown>(Schema.encodeSync(Schema.toCodecJson(schema))(decoded), wire);
    assert.isFalse(Object.hasOwn(decoded.observation, "observedAt"));
  }
  const accepts = Schema.decodeUnknownOption(OrchestrationV2ThreadRuntimeObservation);
  assert.isTrue(
    Option.isNone(
      accepts({
        status: "unknown",
        reason: "runtime_not_resident",
        observedAt: attachedRuntime.observedAt,
      }),
    ),
  );
});

it("rejects incomplete, cross-thread or invented current runtime observation evidence", () => {
  const schema = OrchestrationV2ThreadRuntimeObservationResult;
  const observation = {
    status: "working",
    binding: attachedRuntime.binding,
    observedAt: attachedRuntime.observedAt,
  };
  for (const changed of [
    { status: "attached" },
    { binding: null },
    { binding: undefined },
    { observedAt: undefined },
    { binding: { ...attachedRuntime.binding, runtimeGeneration: 4 } },
    { binding: { ...attachedRuntime.binding, threadId: "other-thread" } },
    { selectedInstanceId: "codex_next" },
    { stopped: true },
  ])
    assert.isTrue(
      Option.isNone(
        Schema.decodeUnknownOption(schema)({
          threadId: "thread-1",
          observation: { ...observation, ...changed },
        }),
      ),
    );
});

it("preserves aggregate operating and independent foreground waiting counts with native string sample times", () => {
  const schema = OrchestrationV2OperatingCountsResult;
  const decoded = Schema.decodeUnknownSync(schema)(currentOperatingCounts);
  assert.deepEqual<unknown>(Schema.encodeSync(schema)(decoded), currentOperatingCounts);
  assert.deepEqual<unknown>(
    Schema.encodeSync(Schema.toCodecJson(schema))(decoded),
    currentOperatingCounts,
  );
  assert.strictEqual(decoded.backgroundOperating, 2);
  assert.strictEqual(decoded.foregroundWaitingApproval, 1);
  for (const changed of [
    { total: -1 },
    { operating: 1.5 },
    { snapshotSequence: -1 },
    { backgroundUnknown: undefined },
    { observedAt: 4 },
    { backgroundSampledAt: 4 },
    { healthy: true },
    { selectedInstanceId: "codex_next" },
  ])
    assert.isTrue(
      Option.isNone(Schema.decodeUnknownOption(schema)({ ...currentOperatingCounts, ...changed })),
    );
});
