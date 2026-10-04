import { describe, expect, it } from "vite-plus/test";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import { EnvironmentAuthorizationError } from "./auth.ts";
import {
  NativeBootstrapSubmission,
  NATIVE_BOOTSTRAP_MAX_PREPARATION_BYTES,
  NATIVE_BOOTSTRAP_MAX_SUBMISSION_BYTES,
} from "./nativeCreation.ts";

import { ORCHESTRATION_V2_WS_METHODS, OrchestrationV2ClientCommand } from "./orchestrationV2.ts";
import {
  OrchestrationV2GuardedDispatchInput,
  OrchestrationV2DispatchNativeBootstrapInput,
  WsOrchestrationV2DispatchNativeBootstrapRpc,
  OrchestrationV2GetThreadRuntimeAttachmentInput,
  OrchestrationV2GetThreadRuntimeObservationInput,
  OrchestrationV2GetOperatingCountsInput,
  WsOrchestrationV2GetThreadRuntimeObservationRpc,
  WsOrchestrationV2GetOperatingCountsRpc,
  OrchestrationV2StopCurrentThreadRuntimeInput,
  OrchestrationV2ObserveCurrentThreadRuntimeStopInput,
  WsOrchestrationV2StopCurrentThreadRuntimeRpc,
  WsOrchestrationV2ObserveCurrentThreadRuntimeStopRpc,
  OrchestrationV2ObserveThreadDeletionCleanupInput,
  WsOrchestrationV2ObserveThreadDeletionCleanupRpc,
  OrchestrationV2ReviewImportedHistoryStartInput,
  OrchestrationV2ObserveImportedHistoryStartInput,
  WsOrchestrationV2ReviewImportedHistoryStartRpc,
  WsOrchestrationV2StartWithImportedHistoryRpc,
  WsOrchestrationV2ObserveImportedHistoryStartRpc,
  WsOrchestrationV2DispatchGuardedRpc,
  WsOrchestrationV2GetThreadRuntimeAttachmentRpc,
  WsRpcGroup,
  WsSubscribeServerConfigRpc,
} from "./rpc.ts";

describe("deletion cleanup observation transport", () => {
  it("registers the strict same-command read without accepting removal authority or a mutation request", () => {
    expect(ORCHESTRATION_V2_WS_METHODS.observeThreadDeletionCleanup).toBe(
      "orchestration.observeThreadDeletionCleanup",
    );
    expect(WsRpcGroup.requests.get(ORCHESTRATION_V2_WS_METHODS.observeThreadDeletionCleanup)).toBe(
      WsOrchestrationV2ObserveThreadDeletionCleanupRpc,
    );
    expect(WsOrchestrationV2ObserveThreadDeletionCleanupRpc.errorSchema).toBe(
      EnvironmentAuthorizationError,
    );
    const wire = { threadId: "thread-1", commandId: "delete-1" };
    for (const schema of [
      OrchestrationV2ObserveThreadDeletionCleanupInput,
      WsOrchestrationV2ObserveThreadDeletionCleanupRpc.payloadSchema,
    ]) {
      expect(Schema.encodeSync(schema)(Schema.decodeUnknownSync(schema)(wire))).toEqual(wire);
      for (const changed of [
        { commandId: undefined },
        { threadId: undefined },
        { worktreeRemoval: {} },
        { force: true },
        { lease: {} },
        { path: "/project/task" },
        { proof: undefined },
      ])
        expect(() => Schema.decodeUnknownSync(schema)({ ...wire, ...changed })).toThrow();
    }
  });

  it("carries historical completion, replacement and unavailable facts through the registered JSON response", () => {
    const schema = Schema.toCodecJson(
      WsOrchestrationV2ObserveThreadDeletionCleanupRpc.successSchema,
    );
    const complete = {
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
      deletion: { eventId: "delete-event-1", sequence: 21, resultSequence: 24 },
      worktree: { projectId: "project-1", path: "/project/task", branch: null },
      state: "completed",
      removalOutcome: { result: "succeeded", effect: "confirmed" },
      currentLease: "replacement",
      reason: null,
    };
    const missing = {
      threadId: "thread-1",
      commandId: "delete-1",
      receipt: null,
      deletion: null,
      worktree: null,
      state: "unknown",
      removalOutcome: null,
      currentLease: "unavailable",
      reason: "inventory_unavailable",
    };
    for (const wire of [complete, missing]) {
      expect(Schema.encodeSync(schema)(Schema.decodeUnknownSync(schema)(wire))).toEqual(wire);
      expect(() => Schema.decodeUnknownSync(schema)({ ...wire, lease: {} })).toThrow();
    }
  });
});

describe("current runtime attachment read transport", () => {
  it("carries missing, null or explicit version2 STOP availability through the registered read response", () => {
    const registered = WsRpcGroup.requests.get(
      ORCHESTRATION_V2_WS_METHODS.getThreadRuntimeAttachment,
    );
    if (registered === undefined) throw new Error("getThreadRuntimeAttachment is not registered");
    const schema = Schema.toCodecJson(registered.successSchema);
    const base = {
      threadId: "thread-1",
      attachment: { status: "unknown", reason: "unconfigured", observedAt: "2026-10-03T01:00:00Z" },
    };
    for (const wire of [
      base,
      { ...base, stopCapability: null },
      { ...base, stopCapability: { version: 2 } },
    ]) {
      expect(Schema.encodeSync(schema)(Schema.decodeUnknownSync(schema)(wire))).toEqual(wire);
    }
    expect(() =>
      Schema.decodeUnknownSync(schema)({ ...base, stopCapability: { version: 1 } }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(schema)({
        ...base,
        stopCapability: { version: 2, authorized: true },
      }),
    ).toThrow();
  });

  it("registers the dedicated thread read and preserves stopped and unknown responses", () => {
    expect(ORCHESTRATION_V2_WS_METHODS.getThreadRuntimeAttachment).toBe(
      "orchestration.getThreadRuntimeAttachment",
    );
    const registered = WsRpcGroup.requests.get(
      ORCHESTRATION_V2_WS_METHODS.getThreadRuntimeAttachment,
    );
    if (registered === undefined) throw new Error("getThreadRuntimeAttachment is not registered");
    expect(registered.errorSchema).toBe(EnvironmentAuthorizationError);
    for (const schema of [
      OrchestrationV2GetThreadRuntimeAttachmentInput,
      registered.payloadSchema,
    ]) {
      expect(
        Schema.encodeSync(schema)(Schema.decodeUnknownSync(schema)({ threadId: "thread-1" })),
      ).toEqual({ threadId: "thread-1" });
      expect(() => Schema.decodeUnknownSync(schema)({})).toThrow();
    }
    for (const attachment of [
      { status: "stopped", reason: "runtime_not_resident", observedAt: "2026-10-03T01:00:00Z" },
      { status: "unknown", reason: "unconfigured", observedAt: "2026-10-03T01:00:00Z" },
    ]) {
      const wire = { threadId: "thread-1", attachment };
      const schema = WsOrchestrationV2GetThreadRuntimeAttachmentRpc.successSchema;
      expect(Schema.encodeSync(schema)(Schema.decodeUnknownSync(schema)(wire))).toEqual(wire);
    }
  });
});

/**
 * The client always sends `environmentThemes`, including to servers built
 * before the field existed, whose payload schema was an empty struct. What
 * makes that safe is that such a schema accepts the request rather than
 * rejecting it -- an error here would take down the config subscription.
 */
describe("subscribeServerConfig payload compatibility", () => {
  it("is accepted by a server whose schema predates the field", () => {
    const oldServerPayload = Schema.Struct({});
    const decoded = Schema.decodeExit(oldServerPayload)({ environmentThemes: true });
    expect(Exit.isSuccess(decoded)).toBe(true);
  });

  it("is carried by a server that declares it", () => {
    const decoded = Schema.decodeSync(WsSubscribeServerConfigRpc.payloadSchema)({
      environmentThemes: true,
    });
    expect(decoded).toEqual({ environmentThemes: true });
  });

  it("stays optional, so a client that never sends it still subscribes", () => {
    const decoded = Schema.decodeSync(WsSubscribeServerConfigRpc.payloadSchema)({});
    expect(decoded).toEqual({});
  });
});

const guardedMessage = {
  type: "message.dispatch",
  createdBy: "user",
  creationSource: "web",
  commandId: "command-guarded-message",
  threadId: "thread-1",
  messageId: "message-1",
  text: "Preserve the message and delivery intent",
  attachments: [
    {
      type: "image",
      id: "pending-00000000-0000-4000-8000-000000000001",
      name: "context.png",
      mimeType: "image/png",
      sizeBytes: 4,
    },
  ],
  modelSelection: { instanceId: "codex_work", model: "model-1" },
  deliveryIntent: "auto",
  dispatchMode: { type: "start_immediately" },
};
const guardedBasis = {
  version: 2,
  observedSnapshotSequence: 12,
  expectedIncarnation: { eventId: "thread-created-event", sequence: 1 },
  expectedModelSelection: guardedMessage.modelSelection,
  expectedActiveRunId: null,
  expectedLatestRunId: "latest-run-1",
  expectedActiveRunAttemptId: null,
  expectedActiveProviderThreadId: "provider-thread-1",
  expectedProviderSessionId: "provider-session-1",
  expectedProviderSessionStatus: "ready",
  expectedRuntimeGeneration: "runtime-generation-1",
  requireIdle: true,
};

describe("guarded V2 dispatch transport", () => {
  it("registers the separate method and round-trips the existing message payload with its exact guard", () => {
    expect(ORCHESTRATION_V2_WS_METHODS.dispatchGuarded).toBe("orchestration.dispatchGuarded");
    const registered = WsRpcGroup.requests.get(ORCHESTRATION_V2_WS_METHODS.dispatchGuarded);
    if (registered === undefined) throw new Error("dispatchGuarded is not registered");
    const payload = { command: guardedMessage, guard: guardedBasis };
    for (const schema of [
      OrchestrationV2GuardedDispatchInput,
      registered.payloadSchema,
      WsOrchestrationV2DispatchGuardedRpc.payloadSchema,
    ]) {
      const decoded = Schema.decodeUnknownSync(schema)(payload);
      expect(Schema.encodeSync(schema)(decoded)).toEqual(payload);
    }
  });

  it("keeps ordinary messages valid but rejects original guard keys before ordinary decoding can discard them", () => {
    const registered = WsRpcGroup.requests.get(ORCHESTRATION_V2_WS_METHODS.dispatchCommand);
    if (registered === undefined) throw new Error("dispatchCommand is not registered");
    for (const schema of [OrchestrationV2ClientCommand, registered.payloadSchema]) {
      const decode = Schema.decodeUnknownSync(schema);
      expect(Schema.encodeSync(schema)(decode(guardedMessage))).toEqual(guardedMessage);
      for (const extra of [
        { guard: guardedBasis },
        { dispatchGuard: guardedBasis },
        { guard: null },
        { dispatchGuard: undefined },
      ]) {
        expect(() => decode({ ...guardedMessage, ...extra })).toThrow();
      }
    }
  });

  it("rejects misplaced guards, unknown wrapper keys and other otherwise valid client commands", () => {
    const decode = Schema.decodeUnknownSync(OrchestrationV2GuardedDispatchInput);
    const payload = { command: guardedMessage, guard: guardedBasis };
    expect(() =>
      Schema.decodeUnknownSync(OrchestrationV2ClientCommand)({
        type: "thread.archive",
        commandId: "archive-command",
        threadId: "thread-1",
      }),
    ).not.toThrow();
    for (const input of [
      { ...payload, dispatchGuard: guardedBasis },
      { ...payload, messageId: "misplaced-message-id" },
      { ...payload, command: { ...guardedMessage, guard: guardedBasis } },
      { ...payload, command: { ...guardedMessage, dispatchGuard: guardedBasis } },
      { ...payload, guard: { ...guardedBasis, command: guardedMessage } },
      { ...payload, guard: { ...guardedBasis, dispatchGuard: guardedBasis } },
      { ...payload, guard: { ...guardedBasis, expectedRuntimeGeneration: 1 } },
      {
        ...payload,
        command: { type: "thread.archive", commandId: "archive-command", threadId: "thread-1" },
      },
      { command: guardedMessage },
      { ...payload, guard: null },
    ])
      expect(() => decode(input)).toThrow();
  });
});

describe("WebSocket RPC contracts", () => {
  it("exposes only the V2 orchestration transport surface", () => {
    const methods = [...WsRpcGroup.requests.keys()];

    expect(methods).toEqual(expect.arrayContaining(Object.values(ORCHESTRATION_V2_WS_METHODS)));
    expect(methods.filter((method) => method.startsWith("orchestrationV1."))).toEqual([]);
  });

  it("rejects server-internal commands sent to dispatchCommand", () => {
    const dispatchCommand = WsRpcGroup.requests.get(ORCHESTRATION_V2_WS_METHODS.dispatchCommand);
    if (dispatchCommand === undefined) throw new Error("dispatchCommand is not registered");
    const decode = Schema.decodeUnknownExit(dispatchCommand.payloadSchema);

    expect(
      Exit.isFailure(
        decode({
          type: "checkpoint.rollback.fail",
          commandId: "forged-rollback-failure",
          threadId: "thread-1",
          requestId: "rollback-1",
          message: "Forged failure.",
        }),
      ),
    ).toBe(true);
    expect(
      Exit.isSuccess(
        decode({
          type: "checkpoint.rollback",
          commandId: "rollback-1",
          threadId: "thread-1",
          scopeId: "scope-1",
          checkpointId: "checkpoint-1",
        }),
      ),
    ).toBe(true);
  });
});

const importedQueueDelivery = {
  type: "queued_run",
  runId: "held-run-1",
  messageId: "held-message-1",
};
const importedQueueStart = {
  type: "thread.imported-history.start",
  commandId: "imported-start-1",
  threadId: "thread-1",
  reviewedBasis: "reviewed-basis-1",
  delivery: importedQueueDelivery,
};

describe("explicit imported history transport", () => {
  it("registers one deliberate start plus separate review and observation reads with exact payloads", () => {
    for (const [method, rpc, payload] of [
      [
        ORCHESTRATION_V2_WS_METHODS.reviewImportedHistoryStart,
        WsOrchestrationV2ReviewImportedHistoryStartRpc,
        { threadId: "thread-1", delivery: importedQueueDelivery },
      ],
      [
        ORCHESTRATION_V2_WS_METHODS.startWithImportedHistory,
        WsOrchestrationV2StartWithImportedHistoryRpc,
        importedQueueStart,
      ],
      [
        ORCHESTRATION_V2_WS_METHODS.observeImportedHistoryStart,
        WsOrchestrationV2ObserveImportedHistoryStartRpc,
        { threadId: "thread-1", commandId: "imported-start-1" },
      ],
    ] as const) {
      const registered = WsRpcGroup.requests.get(method);
      if (registered === undefined) throw new Error(`${method} is not registered`);
      expect(registered).toBe(rpc);
      const decoded = Schema.decodeUnknownSync(registered.payloadSchema)(payload);
      expect(Schema.encodeSync(registered.payloadSchema)(decoded)).toEqual(payload);
    }
    expect(WsOrchestrationV2ReviewImportedHistoryStartRpc.errorSchema).toBe(
      EnvironmentAuthorizationError,
    );
    expect(WsOrchestrationV2ObserveImportedHistoryStartRpc.errorSchema).toBe(
      EnvironmentAuthorizationError,
    );
  });

  it("accepts both explicit targets while ordinary and guarded dispatch cannot grant imported-history consent", () => {
    const {
      type: _type,
      createdBy: _createdBy,
      creationSource: _creationSource,
      commandId: _commandId,
      threadId: _threadId,
      ...message
    } = guardedMessage;
    const immediate = {
      ...importedQueueStart,
      delivery: {
        ...message,
        type: "message",
        runtimeMode: "full-access",
        interactionMode: "default",
      },
    };
    for (const wire of [importedQueueStart, immediate]) {
      const schema = WsOrchestrationV2StartWithImportedHistoryRpc.payloadSchema;
      expect(Schema.encodeSync(schema)(Schema.decodeUnknownSync(schema)(wire))).toEqual(wire);
      const ordinary = WsRpcGroup.requests.get(ORCHESTRATION_V2_WS_METHODS.dispatchCommand);
      if (ordinary === undefined) throw new Error("dispatchCommand is not registered");
      expect(() => Schema.decodeUnknownSync(ordinary.payloadSchema)(wire)).toThrow();
      expect(() =>
        Schema.decodeUnknownSync(WsOrchestrationV2DispatchGuardedRpc.payloadSchema)({
          command: wire,
          guard: guardedBasis,
        }),
      ).toThrow();
    }
  });

  it("rejects basis or effect fields on review and rejects queued body overrides at every public start decoder", () => {
    for (const schema of [
      OrchestrationV2ReviewImportedHistoryStartInput,
      WsOrchestrationV2ReviewImportedHistoryStartRpc.payloadSchema,
    ]) {
      for (const extra of [
        { reviewedBasis: "caller-basis" },
        { nativeEffects: { type: "clear" } },
        { commandId: "caller-command" },
      ]) {
        expect(() =>
          Schema.decodeUnknownSync(schema)({
            threadId: "thread-1",
            delivery: importedQueueDelivery,
            ...extra,
          }),
        ).toThrow();
      }
    }
    for (const schema of [
      OrchestrationV2ObserveImportedHistoryStartInput,
      WsOrchestrationV2ObserveImportedHistoryStartRpc.payloadSchema,
    ]) {
      expect(() =>
        Schema.decodeUnknownSync(schema)({
          threadId: "thread-1",
          commandId: "imported-start-1",
          delivery: importedQueueDelivery,
        }),
      ).toThrow();
    }
    const decode = Schema.decodeUnknownSync(
      WsOrchestrationV2StartWithImportedHistoryRpc.payloadSchema,
    );
    expect(() =>
      decode({
        ...importedQueueStart,
        delivery: { ...importedQueueDelivery, text: "replacement" },
      }),
    ).toThrow();
    expect(() =>
      decode({ ...importedQueueStart, delivery: { ...importedQueueDelivery, clearHolds: true } }),
    ).toThrow();
    expect(() => decode({ ...importedQueueStart, choice: "resume" })).toThrow();
  });

  it("preserves intent acceptance and uncertain execution through the actual start and observation JSON codecs", () => {
    const wire = {
      version: 2,
      commandId: "imported-start-1",
      threadId: "thread-1",
      target: importedQueueDelivery,
      reviewedBasis: "reviewed-basis-1",
      intentStatus: "accepted",
      rejectionReason: null,
      receipt: {
        commandId: "imported-start-1",
        threadId: "thread-1",
        commandType: "thread.imported-history.start",
        acceptedAt: "2026-10-03T01:00:00.000Z",
        resultSequence: 11,
        status: "accepted",
        error: null,
      },
      execution: {
        status: "unknown",
        runId: "held-run-1",
        providerThreadId: null,
        providerSessionId: null,
        nativeThreadId: null,
        effectOutcome: "unknown",
        error: "native_effect_unknown",
      },
    };
    for (const rpc of [
      WsOrchestrationV2StartWithImportedHistoryRpc,
      WsOrchestrationV2ObserveImportedHistoryStartRpc,
    ]) {
      const schema = Schema.toCodecJson(rpc.successSchema);
      const decoded = Schema.decodeUnknownSync(schema)(wire);
      expect(decoded.intentStatus).toBe("accepted");
      expect(decoded.execution.status).toBe("unknown");
      expect(Schema.encodeSync(schema)(decoded)).toEqual(wire);
    }
  });
});

const nativeBootstrapSubmission = {
  schema: "t3.native-bootstrap-submission/v1",
  preparationBase64: "e30=",
  creationGuard: { schema: "t3.native-creation-guard/v1", grantId: "grant-1", grantRevision: 1 },
};

describe("native bootstrap V2 compatibility transport", () => {
  it("registers the new method with the exact existing V1 compatibility input and preserves old route encoding", () => {
    expect(ORCHESTRATION_V2_WS_METHODS.dispatchNativeBootstrap).toBe(
      "orchestration.dispatchNativeBootstrap",
    );
    expect(OrchestrationV2DispatchNativeBootstrapInput).toBe(NativeBootstrapSubmission);
    const registered = WsRpcGroup.requests.get(ORCHESTRATION_V2_WS_METHODS.dispatchNativeBootstrap);
    if (registered === undefined) throw new Error("dispatchNativeBootstrap is not registered");
    expect(registered).toBe(WsOrchestrationV2DispatchNativeBootstrapRpc);
    for (const schema of [OrchestrationV2DispatchNativeBootstrapInput, registered.payloadSchema]) {
      expect(
        Schema.encodeSync(schema)(Schema.decodeUnknownSync(schema)(nativeBootstrapSubmission)),
      ).toEqual(nativeBootstrapSubmission);
    }
    const old = WsRpcGroup.requests.get("orchestration.dispatchBootstrap");
    if (old === undefined) throw new Error("Historical bootstrap codec is missing");
    expect(
      Schema.encodeSync(old.payloadSchema)(
        Schema.decodeUnknownSync(old.payloadSchema)(nativeBootstrapSubmission),
      ),
    ).toEqual(nativeBootstrapSubmission);
  });

  it("rejects original wire principal, boot, grant issuance and resource overrides rather than dropping them", () => {
    const decode = Schema.decodeUnknownSync(
      WsOrchestrationV2DispatchNativeBootstrapRpc.payloadSchema,
    );
    for (const extra of [
      { actor: "caller" },
      { principal: {} },
      { bootId: "caller-boot" },
      { nativeCreationBootId: "caller-boot" },
      { resources: {} },
      { grantOverride: "caller-grant" },
      { command: {} },
      { authority: undefined },
    ])
      expect(() => decode({ ...nativeBootstrapSubmission, ...extra })).toThrow();
    expect(() =>
      decode({
        ...nativeBootstrapSubmission,
        creationGuard: { ...nativeBootstrapSubmission.creationGuard, issueGrant: true },
      }),
    ).toThrow();
    for (const preparationBase64 of ["{}", "e30", "e30===", " e30="]) {
      expect(() => decode({ ...nativeBootstrapSubmission, preparationBase64 })).toThrow();
    }
  });

  it("keeps the exact preparation and total-submission size limits on the registered codec", () => {
    const decode = Schema.decodeUnknownSync(
      WsOrchestrationV2DispatchNativeBootstrapRpc.payloadSchema,
    );
    const maxLength = 4 * Math.ceil(NATIVE_BOOTSTRAP_MAX_PREPARATION_BYTES / 3);
    const atLimit = "A".repeat(maxLength - 2) + "==";
    expect(() =>
      decode({ ...nativeBootstrapSubmission, preparationBase64: atLimit }),
    ).not.toThrow();
    expect(() =>
      decode({ ...nativeBootstrapSubmission, preparationBase64: "A".repeat(maxLength) }),
    ).toThrow();
    expect(() =>
      decode({
        ...nativeBootstrapSubmission,
        creationGuard: {
          ...nativeBootstrapSubmission.creationGuard,
          grantId: "g".repeat(NATIVE_BOOTSTRAP_MAX_SUBMISSION_BYTES),
        },
      }),
    ).toThrow();
  });

  it("returns a versioned unresolved result through the actual JSON success codec without scalar completion", () => {
    const schema = Schema.toCodecJson(WsOrchestrationV2DispatchNativeBootstrapRpc.successSchema);
    const wire = {
      version: 2,
      commandId: "logical-command-1",
      threadId: "thread-1",
      messageId: "message-1",
      commandAcceptance: "unknown",
      creation: null,
    };
    expect(Schema.encodeSync(schema)(Schema.decodeUnknownSync(schema)(wire))).toEqual(wire);
    expect(() => Schema.decodeUnknownSync(schema)({ sequence: 5 })).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(schema)({ ...wire, commandAcceptance: "accepted" }),
    ).toThrow();
  });
});

const stopCurrentRuntimeRequest = {
  commandId: "stop-current-1",
  threadId: "thread-1",
  target: {
    binding: {
      threadId: "thread-1",
      providerThreadId: "provider-thread-1",
      providerSessionId: "provider-session-1",
      instanceId: "codex_work",
      runtimeGeneration: "current-generation-1",
      nativeThreadId: "native-thread-1",
    },
    driver: "codex",
    evidenceRevision: 4,
  },
};

describe("current target runtime stop transport", () => {
  it("registers exact target mutation and read-only observation inputs separately", () => {
    expect(ORCHESTRATION_V2_WS_METHODS.stopCurrentThreadRuntime).toBe(
      "orchestration.stopCurrentThreadRuntime",
    );
    expect(ORCHESTRATION_V2_WS_METHODS.observeCurrentThreadRuntimeStop).toBe(
      "orchestration.observeCurrentThreadRuntimeStop",
    );
    for (const [method, rpc, payload] of [
      [
        ORCHESTRATION_V2_WS_METHODS.stopCurrentThreadRuntime,
        WsOrchestrationV2StopCurrentThreadRuntimeRpc,
        stopCurrentRuntimeRequest,
      ],
      [
        ORCHESTRATION_V2_WS_METHODS.observeCurrentThreadRuntimeStop,
        WsOrchestrationV2ObserveCurrentThreadRuntimeStopRpc,
        { threadId: "thread-1", commandId: "stop-current-1" },
      ],
    ] as const) {
      const registered = WsRpcGroup.requests.get(method);
      if (registered === undefined) throw new Error(`${method} is not registered`);
      expect(registered).toBe(rpc);
      expect(
        Schema.encodeSync(registered.payloadSchema)(
          Schema.decodeUnknownSync(registered.payloadSchema)(payload),
        ),
      ).toEqual(payload);
    }
    expect(WsOrchestrationV2ObserveCurrentThreadRuntimeStopRpc.errorSchema).toBe(
      EnvironmentAuthorizationError,
    );
  });

  it("rejects mismatched current target, caller queue mapping, grants and epochs on original wire", () => {
    for (const schema of [
      OrchestrationV2StopCurrentThreadRuntimeInput,
      WsOrchestrationV2StopCurrentThreadRuntimeRpc.payloadSchema,
    ]) {
      const decode = Schema.decodeUnknownSync(schema);
      for (const extra of [
        { affectedRunIds: [] },
        { queuedBases: [] },
        { grant: "caller" },
        { epoch: 4 },
        { transition: undefined },
      ]) {
        expect(() => decode({ ...stopCurrentRuntimeRequest, ...extra })).toThrow();
      }
      expect(() => decode({ ...stopCurrentRuntimeRequest, target: null })).toThrow();
      expect(() =>
        decode({
          ...stopCurrentRuntimeRequest,
          target: {
            ...stopCurrentRuntimeRequest.target,
            binding: { ...stopCurrentRuntimeRequest.target.binding, threadId: "other-thread" },
          },
        }),
      ).toThrow();
      expect(() =>
        decode({
          ...stopCurrentRuntimeRequest,
          target: { ...stopCurrentRuntimeRequest.target, reason: "caller" },
        }),
      ).toThrow();
    }
    for (const schema of [
      OrchestrationV2ObserveCurrentThreadRuntimeStopInput,
      WsOrchestrationV2ObserveCurrentThreadRuntimeStopRpc.payloadSchema,
    ]) {
      expect(() =>
        Schema.decodeUnknownSync(schema)({
          threadId: "thread-1",
          commandId: "stop-current-1",
          target: stopCurrentRuntimeRequest.target,
        }),
      ).toThrow();
    }
  });

  it("round-trips installed pending and not-found target absence through actual JSON success codecs", () => {
    const accepted = {
      version: 2,
      commandId: "stop-current-1",
      threadId: "thread-1",
      target: stopCurrentRuntimeRequest.target,
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
    const notFound = {
      ...accepted,
      commandStatus: "not_found",
      target: null,
      receipt: null,
      queueFence: { status: "unknown", affectedRunIds: [] },
      runtimeStop: { status: "not_started" },
    };
    for (const rpc of [
      WsOrchestrationV2StopCurrentThreadRuntimeRpc,
      WsOrchestrationV2ObserveCurrentThreadRuntimeStopRpc,
    ]) {
      const schema = Schema.toCodecJson(rpc.successSchema);
      for (const wire of [accepted, notFound]) {
        expect(Schema.encodeSync(schema)(Schema.decodeUnknownSync(schema)(wire))).toEqual(wire);
      }
    }
  });

  it("keeps absent imported start observation targets null through the actual observation response codec", () => {
    const schema = Schema.toCodecJson(
      WsOrchestrationV2ObserveImportedHistoryStartRpc.successSchema,
    );
    const wire = {
      version: 2,
      commandId: "missing-start-1",
      threadId: "thread-1",
      target: null,
      reviewedBasis: null,
      intentStatus: "not_found",
      receipt: null,
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
    expect(Schema.encodeSync(schema)(Schema.decodeUnknownSync(schema)(wire))).toEqual(wire);
  });
});

describe("raw IF-LIVE observation and aggregate read transports", () => {
  it("registers read-only exact thread and optional-project inputs without authority or selection fields", () => {
    expect(ORCHESTRATION_V2_WS_METHODS.getThreadRuntimeObservation).toBe(
      "orchestration.getThreadRuntimeObservation",
    );
    expect(ORCHESTRATION_V2_WS_METHODS.getOperatingCounts).toBe("orchestration.getOperatingCounts");
    for (const rpc of [
      WsOrchestrationV2GetThreadRuntimeObservationRpc,
      WsOrchestrationV2GetOperatingCountsRpc,
    ]) {
      expect(WsRpcGroup.requests.get(rpc._tag)).toBe(rpc);
      expect(rpc.errorSchema).toBe(EnvironmentAuthorizationError);
    }
    for (const schema of [
      OrchestrationV2GetThreadRuntimeObservationInput,
      WsOrchestrationV2GetThreadRuntimeObservationRpc.payloadSchema,
    ]) {
      expect(
        Schema.encodeSync(schema)(Schema.decodeUnknownSync(schema)({ threadId: "thread-1" })),
      ).toEqual({ threadId: "thread-1" });
      expect(() => Schema.decodeUnknownSync(schema)({})).toThrow();
      expect(() =>
        Schema.decodeUnknownSync(schema)({ threadId: "thread-1", instanceId: "selected-next" }),
      ).toThrow();
    }
    for (const schema of [
      OrchestrationV2GetOperatingCountsInput,
      WsOrchestrationV2GetOperatingCountsRpc.payloadSchema,
    ]) {
      for (const wire of [{}, { projectId: "project-1" }]) {
        expect(Schema.encodeSync(schema)(Schema.decodeUnknownSync(schema)(wire))).toEqual(wire);
      }
      expect(() => Schema.decodeUnknownSync(schema)({ projectId: null })).toThrow();
      expect(() => Schema.decodeUnknownSync(schema)({ settled: false })).toThrow();
      expect(() => Schema.decodeUnknownSync(schema)({ snoozed: false })).toThrow();
    }
  });

  it("preserves raw known and unknown observations through the actual registered JSON success codec", () => {
    const schema = Schema.toCodecJson(
      WsOrchestrationV2GetThreadRuntimeObservationRpc.successSchema,
    );
    for (const observation of [
      {
        status: "working",
        binding: stopCurrentRuntimeRequest.target.binding,
        observedAt: "2026-10-03T02:25:30Z",
      },
      {
        status: "monitoring",
        binding: stopCurrentRuntimeRequest.target.binding,
        observedAt: "2026-10-03T02:25:30Z",
      },
      {
        status: "busy",
        binding: stopCurrentRuntimeRequest.target.binding,
        observedAt: "2026-10-03T02:25:30Z",
      },
      {
        status: "idle",
        binding: stopCurrentRuntimeRequest.target.binding,
        observedAt: "2026-10-03T02:25:30Z",
      },
      { status: "unknown", reason: "runtime_not_resident" },
      {
        status: "unknown",
        binding: stopCurrentRuntimeRequest.target.binding,
        reason: "runtime_binding_changed",
      },
    ]) {
      const wire = { threadId: "thread-1", observation };
      expect(Schema.encodeSync(schema)(Schema.decodeUnknownSync(schema)(wire))).toEqual(wire);
    }
  });

  it("preserves independent waiting and background counts without a healthy-zero or selection fallback shape", () => {
    const schema = Schema.toCodecJson(WsOrchestrationV2GetOperatingCountsRpc.successSchema);
    const wire = {
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
    expect(Schema.encodeSync(schema)(Schema.decodeUnknownSync(schema)(wire))).toEqual(wire);
    expect(() => Schema.decodeUnknownSync(schema)({ error: "counts_unavailable" })).toThrow();
    expect(() => Schema.decodeUnknownSync(schema)({ ...wire, healthy: true })).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(schema)({ ...wire, selectedInstanceId: "selected-next" }),
    ).toThrow();
  });
});
