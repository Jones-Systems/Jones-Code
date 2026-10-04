import { assert, it } from "@effect/vitest";
import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  AuthSessionId,
  EnvironmentAuthenticatedPrincipal,
  ORCHESTRATION_PROTOCOL_VERSION,
  OrchestrationV2GuardedDispatchInput,
  OrchestrationV2ReviewImportedHistoryStartInput,
  OrchestrationV2StartWithImportedHistoryCommand,
  OrchestrationV2ImportedHistoryReviewResult,
  OrchestrationV2ImportedHistoryStartReceipt,
  CommandId,
  ProviderInstanceId,
  ProviderDriverKind,
  ProviderSessionId,
  ProviderThreadId,
  ProjectId,
  OrchestrationV2ThreadRuntimeObservationResult,
  OrchestrationV2StopCurrentThreadRuntimeResult,
  NativeBootstrapSubmission,
  NATIVE_BOOTSTRAP_MAX_SUBMISSION_BYTES,
  ThreadId,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import * as Layer from "effect/Layer";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { DEFAULT_SIGNAL_EXPORT } from "@t3tools/shared/observability";
import * as OtelEnvironment from "@t3tools/shared/otelEnvironment";
import {
  NativeCreationAuthority,
  NativeCreationAuthorityError,
} from "./orchestration-v2/NativeCreationAuthority.ts";
import { NativeCreationRepository } from "./persistence/Services/NativeCreationRepository.ts";
import * as ServerConfig from "./config.ts";
import * as ServerRuntimeStartup from "./serverRuntimeStartup.ts";
import { RpcSerialization } from "effect/unstable/rpc";
import { ProviderOperatingCountsError } from "./orchestration-v2/ProviderThreadRuntimeObservation.ts";
import { OrchestratorProjectionError } from "./orchestration-v2/Orchestrator.ts";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";

import {
  dispatchGuardedRpcCommand,
  dispatchNativeBootstrapRpc,
  nativeBootstrapRpcSerialization,
  stopCurrentThreadRuntimeRpc,
  reviewImportedHistoryRpc,
  startWithImportedHistoryRpc,
  observeImportedHistoryRpc,
  readThreadRuntimeAttachment,
  readThreadRuntimeObservation,
  readOperatingCounts,
  observeCurrentThreadRuntimeStopRpc,
  hasCompatibleOrchestrationProtocol,
  resolveAvailableEditorsForConfig,
  shouldUseBoundedThreadSnapshot,
} from "./ws.ts";

it("accepts only the current orchestration protocol before websocket RPC setup", () => {
  assert.isTrue(
    hasCompatibleOrchestrationProtocol(
      new URL(`https://host.test/ws?orchestrationProtocol=${ORCHESTRATION_PROTOCOL_VERSION}`),
    ),
  );
  assert.isFalse(hasCompatibleOrchestrationProtocol(new URL("https://host.test/ws")));
  assert.isFalse(
    hasCompatibleOrchestrationProtocol(
      new URL(`https://host.test/ws?orchestrationProtocol=${ORCHESTRATION_PROTOCOL_VERSION - 1}`),
    ),
  );
});

it("keeps full thread snapshot fallback unless the client opts into bounded history", () => {
  assert.isFalse(shouldUseBoundedThreadSnapshot({}));
  assert.isFalse(shouldUseBoundedThreadSnapshot({ acceptBoundedSnapshot: false }));
  assert.isTrue(shouldUseBoundedThreadSnapshot({ acceptBoundedSnapshot: true }));
});

it.effect("does not block server config when editor discovery never resolves", () =>
  Effect.gen(function* () {
    const discoveryInterrupted = yield* Deferred.make<void>();
    const responseFiber = yield* resolveAvailableEditorsForConfig(
      Effect.never.pipe(
        Effect.onInterrupt(() => Deferred.succeed(discoveryInterrupted, undefined)),
      ),
    ).pipe(Effect.forkChild);

    yield* TestClock.adjust(Duration.seconds(5));

    const availableEditors = yield* Fiber.join(responseFiber);
    yield* Deferred.await(discoveryInterrupted);
    assert.deepEqual(availableEditors, []);
  }),
);

const guardedInput = Schema.decodeUnknownEffect(OrchestrationV2GuardedDispatchInput)({
  command: {
    type: "message.dispatch",
    commandId: "guarded-rpc-command",
    threadId: "guarded-rpc-thread",
    messageId: "guarded-rpc-message",
    text: "Continue",
    attachments: [],
    createdBy: "user",
    creationSource: "web",
    dispatchMode: { type: "start_immediately" },
  },
  guard: {
    version: 2,
    observedSnapshotSequence: 12,
    expectedIncarnation: { eventId: "guarded-rpc-birth", sequence: 4 },
    expectedModelSelection: { instanceId: "codex", model: "gpt-5.3-codex" },
    expectedActiveRunId: null,
    expectedLatestRunId: null,
    expectedActiveRunAttemptId: null,
    expectedActiveProviderThreadId: null,
    expectedProviderSessionId: null,
    expectedProviderSessionStatus: null,
    requireIdle: true,
  },
});

it.effect("gates one guarded dispatch and provides only the verified session principal", () =>
  Effect.gen(function* () {
    const input = yield* guardedInput;
    const queued = yield* Deferred.make<void>();
    const ready = yield* Deferred.make<void>();
    const session = {
      sessionId: AuthSessionId.make("verified-rpc-session"),
      subject: "verified-user",
      method: "dpop-access-token" as const,
      scopes: [AuthOrchestrationOperateScope],
      proofKeyThumbprint: "verified-proof-key",
      expiresAt: DateTime.makeUnsafe("2026-10-04T00:00:00Z"),
    };
    let calls = 0;
    const result = yield* dispatchGuardedRpcCommand(
      input,
      session,
      {
        dispatchGuarded: (command, guard) =>
          Effect.gen(function* () {
            calls++;
            assert.strictEqual(command, input.command);
            assert.strictEqual(guard, input.guard);
            const principal = yield* EnvironmentAuthenticatedPrincipal;
            assert.equal(principal.sessionId, session.sessionId);
            assert.equal(principal.subject, session.subject);
            assert.equal(principal.method, session.method);
            assert.equal(principal.proofKeyThumbprint, session.proofKeyThumbprint);
            assert.strictEqual(principal.expiresAt, session.expiresAt);
            assert.deepEqual(Array.from(principal.scopes), session.scopes);
            return { sequence: 13, storedEvents: [] };
          }),
      },
      {
        enqueueCommand: (effect) =>
          Deferred.succeed(queued, undefined).pipe(
            Effect.andThen(Deferred.await(ready)),
            Effect.andThen(effect),
          ),
      },
    ).pipe(Effect.forkChild);
    yield* Deferred.await(queued);
    assert.equal(calls, 0);
    yield* Deferred.succeed(ready, undefined);
    assert.equal((yield* Fiber.join(result)).sequence, 13);
    assert.equal(calls, 1);
  }),
);

it.effect("propagates guarded rejection without retry or ordinary intake fallback", () =>
  Effect.gen(function* () {
    const input = yield* guardedInput;
    const rejected = new OrchestratorProjectionError({
      threadId: input.command.threadId,
      cause: "guard rejected",
    });
    let calls = 0;
    const error = yield* dispatchGuardedRpcCommand(
      input,
      {
        sessionId: AuthSessionId.make("verified-rpc-session"),
        subject: "verified-user",
        method: "bearer-access-token",
        scopes: [AuthOrchestrationOperateScope],
      },
      {
        dispatchGuarded: () =>
          Effect.sync(() => {
            calls++;
          }).pipe(Effect.andThen(Effect.fail(rejected))),
      },
      { enqueueCommand: (effect) => effect },
    ).pipe(Effect.flip);
    assert.strictEqual(error, rejected);
    assert.equal(calls, 1);
  }),
);

it.effect("reads attachment facts once and rejects another thread's resident binding", () =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("attachment-target");
    const attachment = {
      status: "attached" as const,
      binding: {
        threadId,
        providerSessionId: ProviderSessionId.make("attachment-session"),
        providerThreadId: ProviderThreadId.make("attachment-provider-thread"),
        instanceId: ProviderInstanceId.make("codex"),
        runtimeGeneration: "attachment-generation",
      },
      driver: ProviderDriverKind.make("codex"),
      runtimeStatus: "ready" as const,
      evidenceRevision: 4,
      observedAt: "2026-10-03T12:00:00Z",
    };
    let reads = 0;
    const read = (value: typeof attachment) => ({
      readCurrentThreadRuntimeAttachment: (target: ThreadId) =>
        Effect.sync(() => {
          reads++;
          assert.equal(target, threadId);
          return value;
        }),
    });
    const matching = yield* readThreadRuntimeAttachment(threadId, read(attachment));
    assert.strictEqual(matching.attachment, attachment);
    const changed = yield* readThreadRuntimeAttachment(
      threadId,
      read({
        ...attachment,
        binding: { ...attachment.binding, threadId: ThreadId.make("other-thread") },
      }),
    );
    assert.deepEqual(changed, {
      threadId,
      attachment: {
        status: "unknown",
        reason: "runtime_binding_changed",
        observedAt: attachment.observedAt,
      },
    });
    assert.equal(reads, 2);
  }),
);

it.effect("preserves stopped and unknown attachment observations", () =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("attachment-target");
    for (const attachment of [
      {
        status: "stopped" as const,
        reason: "runtime_not_resident" as const,
        observedAt: "2026-10-03T12:00:00Z",
      },
      {
        status: "unknown" as const,
        reason: "orchestration_runtime_unavailable",
        observedAt: "2026-10-03T12:00:00Z",
      },
    ]) {
      const result = yield* readThreadRuntimeAttachment(threadId, {
        readCurrentThreadRuntimeAttachment: () => Effect.succeed(attachment),
      });
      assert.strictEqual(result.attachment, attachment);
    }
  }),
);

const importedSession = {
  sessionId: AuthSessionId.make("verified-imported-session"),
  subject: "verified-imported-user",
  method: "bearer-access-token" as const,
  scopes: [AuthOrchestrationReadScope, AuthOrchestrationOperateScope],
};
const importedReviewInput = Schema.decodeUnknownEffect(
  OrchestrationV2ReviewImportedHistoryStartInput,
)({
  threadId: "imported-rpc-thread",
  delivery: {
    type: "queued_run",
    runId: "existing-queued-run",
    messageId: "existing-queued-message",
  },
});
const importedStartInput = Schema.decodeUnknownEffect(
  OrchestrationV2StartWithImportedHistoryCommand,
)({
  type: "thread.imported-history.start",
  commandId: "imported-rpc-command",
  threadId: "imported-rpc-thread",
  reviewedBasis: "opaque-reviewed-basis",
  delivery: {
    type: "queued_run",
    runId: "existing-queued-run",
    messageId: "existing-queued-message",
  },
});
const importedPendingReceipt = Schema.decodeUnknownEffect(
  OrchestrationV2ImportedHistoryStartReceipt,
)({
  version: 2,
  commandId: "imported-rpc-command",
  threadId: "imported-rpc-thread",
  target: {
    type: "queued_run",
    runId: "existing-queued-run",
    messageId: "existing-queued-message",
  },
  reviewedBasis: "opaque-reviewed-basis",
  intentStatus: "accepted",
  rejectionReason: null,
  receipt: {
    commandId: "imported-rpc-command",
    threadId: "imported-rpc-thread",
    commandType: "thread.imported-history.start",
    acceptedAt: DateTime.makeUnsafe("2026-10-03T12:00:00Z"),
    resultSequence: 21,
    status: "accepted",
    error: null,
  },
  execution: {
    status: "pending",
    runId: "existing-queued-run",
    providerThreadId: null,
    providerSessionId: null,
    nativeThreadId: null,
    effectOutcome: null,
    error: null,
  },
});

it.effect(
  "reviews imported history with the verified principal and keeps unknown capability unavailable",
  () =>
    Effect.gen(function* () {
      const input = yield* importedReviewInput;
      let calls = 0;
      const result = yield* reviewImportedHistoryRpc(input, importedSession, {
        reviewImportedHistoryStart: (received) =>
          Effect.gen(function* () {
            calls++;
            assert.strictEqual(received, input);
            const principal = yield* EnvironmentAuthenticatedPrincipal;
            assert.equal(principal.sessionId, importedSession.sessionId);
            assert.equal(principal.subject, importedSession.subject);
            assert.deepEqual(Array.from(principal.scopes), importedSession.scopes);
            return yield* new OrchestratorProjectionError({
              threadId: input.threadId,
              cause: "review facts unavailable",
            });
          }),
      });
      yield* Schema.decodeUnknownEffect(OrchestrationV2ImportedHistoryReviewResult)(result);
      assert.equal(calls, 1);
      assert.isFalse(result.capability.startWithImportedHistory);
      assert.equal(result.applicability, "unknown");
      assert.isNull(result.reviewedBasis);
      assert.deepEqual(result.target, input.delivery);
    }),
);

it.effect("gates imported start and preserves the same pending receipt on repeated requests", () =>
  Effect.gen(function* () {
    const command = yield* importedStartInput;
    const pending = yield* importedPendingReceipt;
    const queued = yield* Deferred.make<void>();
    const ready = yield* Deferred.make<void>();
    let calls = 0;
    const service = {
      startWithImportedHistory: (received: typeof command) =>
        Effect.gen(function* () {
          calls++;
          assert.strictEqual(received, command);
          const principal = yield* EnvironmentAuthenticatedPrincipal;
          assert.equal(principal.sessionId, importedSession.sessionId);
          assert.equal(principal.subject, importedSession.subject);
          return pending;
        }),
    };
    const response = yield* startWithImportedHistoryRpc(command, importedSession, service, {
      enqueueCommand: (effect) =>
        Deferred.succeed(queued, undefined).pipe(
          Effect.andThen(Deferred.await(ready)),
          Effect.andThen(effect),
        ),
    }).pipe(Effect.forkChild);
    yield* Deferred.await(queued);
    assert.equal(calls, 0);
    yield* Deferred.succeed(ready, undefined);
    assert.strictEqual(yield* Fiber.join(response), pending);
    const repeated = yield* startWithImportedHistoryRpc(command, importedSession, service, {
      enqueueCommand: (effect) => effect,
    });
    assert.strictEqual(repeated, pending);
    assert.equal(repeated.intentStatus, "accepted");
    assert.equal(repeated.execution.status, "pending");
    assert.equal(calls, 2);
  }),
);

it.effect("maps imported start rejection once and preserves its cause", () =>
  Effect.gen(function* () {
    const command = yield* importedStartInput;
    const rejected = new OrchestratorProjectionError({
      threadId: command.threadId,
      cause: "review changed",
    });
    let calls = 0;
    const error = yield* startWithImportedHistoryRpc(
      command,
      importedSession,
      {
        startWithImportedHistory: () =>
          Effect.sync(() => {
            calls++;
          }).pipe(Effect.andThen(Effect.fail(rejected))),
      },
      { enqueueCommand: (effect) => effect },
    ).pipe(Effect.flip);
    assert.equal(error._tag, "OrchestrationV2DispatchCommandError");
    assert.equal(error.commandId, command.commandId);
    assert.equal(error.commandType, command.type);
    assert.strictEqual(error.cause, rejected);
    assert.equal(calls, 1);
  }),
);

it.effect("observes imported acceptance without upgrading pending execution", () =>
  Effect.gen(function* () {
    const pending = yield* importedPendingReceipt;
    const input = { threadId: pending.threadId, commandId: pending.commandId };
    let calls = 0;
    const result = yield* observeImportedHistoryRpc(input, {
      observeImportedHistoryStart: (received) =>
        Effect.sync(() => {
          calls++;
          assert.strictEqual(received, input);
          return pending;
        }),
    });
    assert.strictEqual(result, pending);
    assert.equal(result.execution.status, "pending");
    assert.equal(calls, 1);
  }),
);

it.effect("reports imported observation failure as unknown rather than absence or completion", () =>
  Effect.gen(function* () {
    const input = {
      threadId: ThreadId.make("imported-rpc-thread"),
      commandId: CommandId.make("imported-rpc-command"),
    };
    const result = yield* observeImportedHistoryRpc(input, {
      observeImportedHistoryStart: () =>
        Effect.fail(
          new OrchestratorProjectionError({
            threadId: input.threadId,
            cause: "receipt read unavailable",
          }),
        ),
    });
    yield* Schema.decodeUnknownEffect(OrchestrationV2ImportedHistoryStartReceipt)(result);
    assert.equal(result.commandId, input.commandId);
    assert.equal(result.intentStatus, "unknown");
    assert.equal(result.execution.status, "unknown");
    assert.isNull(result.receipt);
    assert.isNull(result.execution.nativeThreadId);
  }),
);

const observedRuntimeBinding = {
  threadId: ThreadId.make("observed-rpc-thread"),
  providerSessionId: ProviderSessionId.make("actual-owner-session"),
  providerThreadId: ProviderThreadId.make("actual-owner-provider-thread"),
  instanceId: ProviderInstanceId.make("actual-owner-instance"),
  runtimeGeneration: "actual-owner-generation",
};

it.effect("reads current native activity once and preserves the actual owner binding", () =>
  Effect.gen(function* () {
    let calls = 0;
    for (const status of ["working", "monitoring", "busy", "idle"] as const) {
      const observation = {
        status,
        binding: observedRuntimeBinding,
        observedAt: "2026-10-03T12:00:00Z",
      };
      const result = yield* readThreadRuntimeObservation(observedRuntimeBinding.threadId, {
        observeCurrentThreadRuntime: (target) =>
          Effect.sync(() => {
            calls++;
            assert.equal(target, observedRuntimeBinding.threadId);
            return observation;
          }),
      });
      assert.strictEqual(result.observation, observation);
      yield* Schema.decodeUnknownEffect(OrchestrationV2ThreadRuntimeObservationResult)(result);
    }
    assert.equal(calls, 4);
  }),
);

it.effect("preserves unknown native activity without inventing an observation timestamp", () =>
  Effect.gen(function* () {
    const unknown = { status: "unknown" as const, reason: "runtime_generation_unregistered" };
    const result = yield* readThreadRuntimeObservation(observedRuntimeBinding.threadId, {
      observeCurrentThreadRuntime: () => Effect.succeed(unknown),
    });
    assert.strictEqual(result.observation, unknown);
    assert.isFalse(Object.hasOwn(result.observation, "observedAt"));
    yield* Schema.decodeUnknownEffect(OrchestrationV2ThreadRuntimeObservationResult)(result);
  }),
);

it.effect("rejects mismatched known and unknown runtime observation bindings", () =>
  Effect.gen(function* () {
    const wrongBinding = {
      ...observedRuntimeBinding,
      threadId: ThreadId.make("another-runtime-thread"),
    };
    for (const observation of [
      { status: "monitoring" as const, binding: wrongBinding, observedAt: "2026-10-03T12:00:00Z" },
      { status: "unknown" as const, binding: wrongBinding, reason: "native_probe_incomplete" },
    ]) {
      const result = yield* readThreadRuntimeObservation(observedRuntimeBinding.threadId, {
        observeCurrentThreadRuntime: () => Effect.succeed(observation),
      });
      assert.deepEqual(result, {
        threadId: observedRuntimeBinding.threadId,
        observation: { status: "unknown", reason: "runtime_binding_changed" },
      });
      yield* Schema.decodeUnknownEffect(OrchestrationV2ThreadRuntimeObservationResult)(result);
    }
  }),
);

it.effect(
  "reads all native count fields directly and preserves project selection and incomplete coverage",
  () =>
    Effect.gen(function* () {
      const input = { projectId: ProjectId.make("counts-project") };
      const counts = {
        total: 7,
        operating: 2,
        foregroundWaitingApproval: 1,
        foregroundWaitingInput: 1,
        foregroundWaitingPlan: 1,
        backgroundOperating: 1,
        backgroundUnknown: 2,
        snapshotSequence: 31,
        observedAt: "2026-10-03T12:00:01Z",
        backgroundSampledAt: "2026-10-03T12:00:00Z",
      };
      let calls = 0;
      const result = yield* readOperatingCounts(input, {
        getOperatingCounts: (received) =>
          Effect.sync(() => {
            calls++;
            assert.strictEqual(received, input);
            return counts;
          }),
      });
      assert.strictEqual(result, counts);
      assert.equal(calls, 1);
    }),
);

it.effect(
  "preserves typed operating-count read failure without returning healthy zero values",
  () =>
    Effect.gen(function* () {
      const failure = new ProviderOperatingCountsError({
        cause: "actual runtime counts unavailable",
      });
      let calls = 0;
      const error = yield* readOperatingCounts(
        {},
        {
          getOperatingCounts: () =>
            Effect.sync(() => {
              calls++;
            }).pipe(Effect.andThen(Effect.fail(failure))),
        },
      ).pipe(Effect.flip);
      assert.strictEqual(error, failure);
      assert.equal(calls, 1);
    }),
);

const runtimeStopInput = {
  threadId: ThreadId.make("observed-stop-thread"),
  commandId: CommandId.make("observed-stop-command"),
};
const runtimeStopPending = Schema.decodeUnknownEffect(
  OrchestrationV2StopCurrentThreadRuntimeResult,
)({
  version: 2,
  ...runtimeStopInput,
  target: {
    binding: { ...observedRuntimeBinding, threadId: runtimeStopInput.threadId },
    driver: "codex",
    evidenceRevision: 4,
  },
  commandStatus: "accepted",
  receipt: {
    ...runtimeStopInput,
    commandType: "provider-session.detach",
    acceptedAt: DateTime.makeUnsafe("2026-10-03T12:00:00Z"),
    resultSequence: 41,
    status: "accepted",
    error: null,
  },
  queueFence: { status: "installed", affectedRunIds: ["fenced-queued-run"] },
  runtimeStop: { status: "pending" },
  reason: null,
});

it.effect("observes STOP once and preserves pending or unknown runtime completion", () =>
  Effect.gen(function* () {
    const pending = yield* runtimeStopPending;
    let calls = 0;
    for (const result of [
      pending,
      {
        ...pending,
        runtimeStop: { status: "unknown" as const },
        reason: "targeted_quiescence_unproven",
      },
    ]) {
      const observed = yield* observeCurrentThreadRuntimeStopRpc(runtimeStopInput, {
        observeCurrentThreadRuntimeStop: (received) =>
          Effect.sync(() => {
            calls++;
            assert.strictEqual(received, runtimeStopInput);
            return result;
          }),
      });
      assert.strictEqual(observed, result);
      assert.notEqual(observed.runtimeStop.status, "stopped");
      assert.isFalse(Object.hasOwn(observed, "stopCapability"));
    }
    assert.equal(calls, 2);
  }),
);

it.effect("preserves a missing STOP command as not found without a target", () =>
  Effect.gen(function* () {
    const missing = yield* Schema.decodeUnknownEffect(
      OrchestrationV2StopCurrentThreadRuntimeResult,
    )({
      version: 2,
      ...runtimeStopInput,
      target: null,
      commandStatus: "not_found",
      receipt: null,
      queueFence: { status: "not_installed", affectedRunIds: [] },
      runtimeStop: { status: "not_started" },
      reason: null,
    });
    const observed = yield* observeCurrentThreadRuntimeStopRpc(runtimeStopInput, {
      observeCurrentThreadRuntimeStop: () => Effect.succeed(missing),
    });
    assert.strictEqual(observed, missing);
  }),
);

it.effect(
  "keeps a STOP observation read failure unknown without claiming a fence or stopped runtime",
  () =>
    Effect.gen(function* () {
      const result = yield* observeCurrentThreadRuntimeStopRpc(runtimeStopInput, {
        observeCurrentThreadRuntimeStop: () =>
          Effect.fail(
            new OrchestratorProjectionError({
              threadId: runtimeStopInput.threadId,
              cause: "STOP receipt unavailable",
            }),
          ),
      });
      yield* Schema.decodeUnknownEffect(OrchestrationV2StopCurrentThreadRuntimeResult)(result);
      assert.equal(result.commandStatus, "unknown");
      assert.equal(result.queueFence.status, "unknown");
      assert.equal(result.runtimeStop.status, "unknown");
      assert.isNull(result.target);
      assert.isNull(result.receipt);
    }),
);

it.effect("rejects STOP proof returned for a different query identity", () =>
  Effect.gen(function* () {
    const pending = yield* runtimeStopPending;
    for (const result of [
      { ...pending, commandId: CommandId.make("different-stop-command") },
      { ...pending, threadId: ThreadId.make("different-stop-thread") },
      {
        ...pending,
        target: {
          ...pending.target!,
          binding: { ...pending.target!.binding, threadId: ThreadId.make("different-stop-thread") },
        },
      },
    ]) {
      const observed = yield* observeCurrentThreadRuntimeStopRpc(runtimeStopInput, {
        observeCurrentThreadRuntimeStop: () => Effect.succeed(result),
      });
      assert.equal(observed.commandId, runtimeStopInput.commandId);
      assert.equal(observed.threadId, runtimeStopInput.threadId);
      assert.equal(observed.commandStatus, "unknown");
      assert.equal(observed.reason, "current_runtime_stop_observation_unbound");
      assert.isNull(observed.target);
      assert.isNull(observed.receipt);
    }
  }),
);

const syntheticNativeSubmission = Schema.decodeUnknownEffect(NativeBootstrapSubmission)({
  schema: "t3.native-bootstrap-submission/v1",
  preparationBase64: "e30=",
  creationGuard: {
    schema: "t3.native-creation-guard/v1",
    grantId: "fixture-grant",
    grantRevision: 1,
  },
});
const nativeRpcConfig = ServerConfig.deriveServerPaths("/__t3_ws_native_fixture__", undefined).pipe(
  Effect.map((paths) =>
    ServerConfig.make({
      ...paths,
      logLevel: "Error",
      traceMinLevel: "Info",
      traceTimingEnabled: true,
      traceBatchWindowMs: 200,
      traceMaxBytes: 10 * 1024 * 1024,
      traceMaxFiles: 10,
      otlpTracesUrl: undefined,
      otlpMetricsUrl: undefined,
      otlpLogsUrl: undefined,
      otlpTracesExport: DEFAULT_SIGNAL_EXPORT,
      otlpMetricsExport: DEFAULT_SIGNAL_EXPORT,
      otlpLogsExport: DEFAULT_SIGNAL_EXPORT,
      otelEnvironment: OtelEnvironment.none,
      mode: "web",
      port: 0,
      host: undefined,
      cwd: "/__t3_ws_native_fixture__",
      baseDir: "/__t3_ws_native_fixture__",
      staticDir: undefined,
      devUrl: undefined,
      devAllowedOrigins: [],
      noBrowser: true,
      startupPresentation: "headless",
      desktopBootstrapToken: undefined,
      autoBootstrapProjectFromCwd: false,
      logWebSocketEvents: false,
      tailscaleServeEnabled: false,
      tailscaleServePort: 443,
    }),
  ),
  Effect.provide(Path.layer),
);
const nativeRpcContext = (
  lookup: Parameters<typeof NativeCreationAuthority.of>[0]["isAutomationEnrolled"],
) =>
  Layer.build(
    Layer.mergeAll(
      Layer.mock(NativeCreationAuthority)({ isAutomationEnrolled: lookup }),
      Layer.mock(NativeCreationRepository)({}),
      Layer.effect(ServerConfig.ServerConfig, nativeRpcConfig),
      FileSystem.layerNoop({}),
    ),
  );

it.effect("rejects missing native capability before enrollment, startup or producer effects", () =>
  Effect.gen(function* () {
    const submission = yield* syntheticNativeSubmission;
    let enrollmentReads = 0;
    let startupCalls = 0;
    let producerCalls = 0;
    const context = yield* nativeRpcContext(() =>
      Effect.sync(() => {
        enrollmentReads++;
        return true;
      }),
    );
    const error = yield* dispatchNativeBootstrapRpc(
      submission,
      importedSession,
      {
        dispatchNativeBootstrap: () =>
          Effect.sync(() => {
            producerCalls++;
          }).pipe(
            Effect.andThen(
              Effect.fail(
                new NativeCreationAuthorityError({
                  code: "unresolved_claim",
                  message: "must not enter",
                }),
              ),
            ),
          ),
      },
      {
        enqueueCommand: (effect) => {
          startupCalls++;
          return effect;
        },
      },
      context,
      Effect.succeed(false),
    ).pipe(Effect.flip);
    assert.equal(error.creationRejectionCode, "unsupported_authority");
    assert.deepEqual([enrollmentReads, startupCalls, producerCalls], [0, 0, 0]);
  }).pipe(Effect.scoped),
);

it.effect("requires the verified session's permanent native enrollment before startup", () =>
  Effect.gen(function* () {
    const submission = yield* syntheticNativeSubmission;
    let startupCalls = 0;
    let producerCalls = 0;
    const context = yield* nativeRpcContext((sessionId) =>
      Effect.sync(() => {
        assert.equal(sessionId, importedSession.sessionId);
        return false;
      }),
    );
    const error = yield* dispatchNativeBootstrapRpc(
      submission,
      importedSession,
      {
        dispatchNativeBootstrap: () =>
          Effect.sync(() => {
            producerCalls++;
          }).pipe(
            Effect.andThen(
              Effect.fail(
                new NativeCreationAuthorityError({
                  code: "unresolved_claim",
                  message: "must not enter",
                }),
              ),
            ),
          ),
      },
      {
        enqueueCommand: (effect) => {
          startupCalls++;
          return effect;
        },
      },
      context,
      Effect.succeed(true),
    ).pipe(Effect.flip);
    assert.equal(error.creationRejectionCode, "stale_grant");
    assert.deepEqual([startupCalls, producerCalls], [0, 0]);
  }).pipe(Effect.scoped),
);

it.effect(
  "passes the verified native principal and actual server boot to one gated producer call",
  () =>
    Effect.gen(function* () {
      const submission = yield* syntheticNativeSubmission;
      const context = yield* nativeRpcContext(() => Effect.succeed(true));
      const queued = yield* Deferred.make<void>();
      const ready = yield* Deferred.make<void>();
      let producerCalls = 0;
      const response = yield* dispatchNativeBootstrapRpc(
        submission,
        importedSession,
        {
          dispatchNativeBootstrap: (received, server) =>
            Effect.gen(function* () {
              producerCalls++;
              assert.strictEqual(received, submission);
              assert.equal(server.nativeCreationBootId, ServerRuntimeStartup.nativeCreationBootId);
              const principal = yield* EnvironmentAuthenticatedPrincipal;
              assert.equal(principal.sessionId, importedSession.sessionId);
              assert.equal(principal.subject, importedSession.subject);
              return yield* new NativeCreationAuthorityError({
                code: "binding_mismatch",
                message: "actual producer rejects fixture binding",
              });
            }),
        },
        {
          enqueueCommand: (effect) =>
            Deferred.succeed(queued, undefined).pipe(
              Effect.andThen(Deferred.await(ready)),
              Effect.andThen(effect),
            ),
        },
        context,
        Effect.succeed(true),
      ).pipe(Effect.flip, Effect.forkChild);
      yield* Deferred.await(queued);
      assert.equal(producerCalls, 0);
      yield* Deferred.succeed(ready, undefined);
      assert.equal((yield* Fiber.join(response)).creationRejectionCode, "binding_mismatch");
      assert.equal(producerCalls, 1);
    }).pipe(Effect.scoped),
);

it("enforces the native frame byte limit for both bootstrap method tags", () => {
  for (const tag of ["orchestration.dispatchBootstrap", "orchestration.dispatchNativeBootstrap"]) {
    const frame = JSON.stringify({
      _tag: "Request",
      id: "1",
      tag,
      payload: { padding: "x".repeat(NATIVE_BOOTSTRAP_MAX_SUBMISSION_BYTES) },
      headers: [],
    });
    assert.throws(
      () => nativeBootstrapRpcSerialization.makeUnsafe().decode(frame),
      RpcSerialization.MaxBufferSizeExceeded,
    );
  }
  const ordinary = JSON.stringify({
    _tag: "Request",
    id: "2",
    tag: "server.getConfig",
    payload: { padding: "x".repeat(NATIVE_BOOTSTRAP_MAX_SUBMISSION_BYTES) },
    headers: [],
  });
  assert.doesNotThrow(() => nativeBootstrapRpcSerialization.makeUnsafe().decode(ordinary));
});

it.effect(
  "gates STOP with the verified principal and preserves pending acceptance without advertising completion",
  () =>
    Effect.gen(function* () {
      const pending = yield* runtimeStopPending;
      assert.isNotNull(pending.target);
      const input = { ...runtimeStopInput, target: pending.target! };
      const queued = yield* Deferred.make<void>();
      const ready = yield* Deferred.make<void>();
      let calls = 0;
      const response = yield* stopCurrentThreadRuntimeRpc(
        input,
        importedSession,
        {
          stopCurrentThreadRuntime: (received) =>
            Effect.gen(function* () {
              calls++;
              assert.strictEqual(received, input);
              const principal = yield* EnvironmentAuthenticatedPrincipal;
              assert.equal(principal.sessionId, importedSession.sessionId);
              assert.equal(principal.subject, importedSession.subject);
              return pending;
            }),
        },
        {
          enqueueCommand: (effect) =>
            Deferred.succeed(queued, undefined).pipe(
              Effect.andThen(Deferred.await(ready)),
              Effect.andThen(effect),
            ),
        },
      ).pipe(Effect.forkChild);
      yield* Deferred.await(queued);
      assert.equal(calls, 0);
      yield* Deferred.succeed(ready, undefined);
      assert.strictEqual(yield* Fiber.join(response), pending);
      assert.equal(calls, 1);
      assert.equal(pending.runtimeStop.status, "pending");
    }),
);

it.effect("maps STOP rejection once without an ordinary detach fallback", () =>
  Effect.gen(function* () {
    const pending = yield* runtimeStopPending;
    const input = { ...runtimeStopInput, target: pending.target! };
    const rejected = new OrchestratorProjectionError({
      threadId: input.threadId,
      cause: "current stop target changed",
    });
    let calls = 0;
    const error = yield* stopCurrentThreadRuntimeRpc(
      input,
      importedSession,
      {
        stopCurrentThreadRuntime: () =>
          Effect.sync(() => {
            calls++;
          }).pipe(Effect.andThen(Effect.fail(rejected))),
      },
      { enqueueCommand: (effect) => effect },
    ).pipe(Effect.flip);
    assert.equal(error.commandType, "provider-session.detach");
    assert.strictEqual(error.cause, rejected);
    assert.equal(calls, 1);
  }),
);
