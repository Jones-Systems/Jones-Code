import { assert, it, vi } from "@effect/vitest";
import {
  NodeId,
  RunId,
  ProviderSessionId,
  ProviderInstanceId,
  RuntimeRequestId,
  ThreadId,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import * as EventSink from "./EventSink.ts";
import * as OrdinaryCheckout from "./OrdinaryCheckoutOwnership.ts";
import { OrchestrationEffectRequestV2 } from "./EffectOutbox.ts";
import { nativeCreationCanonicalJson, nativeCreationSha256 } from "./NativeCreationPreparation.ts";

import type { ProviderAdapterV2RuntimeRequestResponseInput } from "./ProviderAdapter.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as RuntimeRequestService from "./RuntimeRequestService.ts";

function runtimeContext(
  threadId: ThreadId,
  request?: OrchestrationV2RuntimeRequest,
  runId: RunId | null = null,
): ProjectionStore.ProjectionRuntimeResponseContext {
  return {
    request,
    item: undefined,
    session: undefined,
    node:
      request === undefined
        ? undefined
        : {
            id: request.nodeId,
            threadId,
            runId,
            parentNodeId: null,
            rootNodeId: request.nodeId,
            kind: "approval_request",
            status: "completed",
            countsForRun: false,
            providerThreadId: null,
            providerTurnId: request.providerTurnId,
            nativeItemRef: null,
            runtimeRequestId: request.id,
            checkpointScopeId: null,
            startedAt: request.createdAt,
            completedAt: request.resolvedAt,
          },
  };
}
const standaloneSink = Layer.mock(EventSink.EventSinkV2)({
  readOrdinaryCheckoutAdmissionForRun: () => Effect.succeed(null),
  revalidateOrdinaryCheckoutExecution: () =>
    Effect.die("Standalone response has no ordinary executor."),
});

function projectionWithRuntimeRequest(
  runtimeRequest?: OrchestrationV2RuntimeRequest,
): OrchestrationV2ThreadProjection {
  return {
    runtimeRequests: runtimeRequest === undefined ? [] : [runtimeRequest],
  } as unknown as OrchestrationV2ThreadProjection;
}

function resolvedRuntimeRequest(
  requestId: RuntimeRequestId,
  providerSessionId: ProviderSessionId,
): OrchestrationV2RuntimeRequest {
  return {
    id: requestId,
    nodeId: NodeId.make(`node-${requestId}`),
    providerTurnId: null,
    nativeRequestRef: null,
    kind: "command",
    status: "resolved",
    responseCapability: {
      type: "live",
      providerSessionId,
    },
    createdAt: DateTime.makeUnsafe("2026-07-29T00:00:00.000Z"),
    resolvedAt: DateTime.makeUnsafe("2026-07-29T00:00:01.000Z"),
  };
}

function runtimeRequestTestLayer(
  projection: OrchestrationV2ThreadProjection,
  getSession: ProviderSessionManager.ProviderSessionManagerV2Shape["get"],
) {
  return RuntimeRequestService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        standaloneSink,
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getRuntimeResponseContext: (threadId, requestId) =>
            Effect.succeed(
              runtimeContext(
                threadId,
                projection.runtimeRequests.find((request) => request.id === requestId),
              ),
            ),
        }),
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
          get: getSession,
        }),
      ),
    ),
  );
}

it.effect("forwards orchestrator-resolved runtime requests to the live adapter", () => {
  const threadId = ThreadId.make("thread-runtime-request-resolved");
  const providerSessionId = ProviderSessionId.make("provider-session-runtime-request-resolved");
  const requestId = RuntimeRequestId.make("request-resolved");
  const respondToRuntimeRequest = vi.fn(
    (_input: ProviderAdapterV2RuntimeRequestResponseInput) => Effect.void,
  );
  const getSession = vi.fn(() =>
    Effect.succeed(
      Option.some({
        instanceId: ProviderInstanceId.make("codex-response"),
        runtimeGeneration: "response-process",
        respondToRuntimeRequest,
      } as never),
    ),
  );
  const projection = {
    runtimeRequests: [
      {
        id: requestId,
        nodeId: NodeId.make("node-runtime-request-resolved"),
        providerTurnId: null,
        nativeRequestRef: null,
        kind: "command",
        status: "resolved",
        responseCapability: {
          type: "live",
          providerSessionId,
        },
        createdAt: DateTime.makeUnsafe("2026-07-29T00:00:00.000Z"),
        resolvedAt: DateTime.makeUnsafe("2026-07-29T00:00:01.000Z"),
      },
    ],
  } as unknown as OrchestrationV2ThreadProjection;
  const testLayer = RuntimeRequestService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        standaloneSink,
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getRuntimeResponseContext: (threadId, requestId) =>
            Effect.succeed(
              runtimeContext(
                threadId,
                projection.runtimeRequests.find((request) => request.id === requestId),
              ),
            ),
        }),
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
          get: getSession,
        }),
      ),
    ),
  );

  return Effect.gen(function* () {
    const service = yield* RuntimeRequestService.RuntimeRequestServiceV2;
    yield* service.respond({
      threadId,
      providerSessionId,
      requestId,
      decision: "accept",
    });

    assert.equal(getSession.mock.calls.length, 1);
    assert.equal(respondToRuntimeRequest.mock.calls.length, 1);
    const response = respondToRuntimeRequest.mock.calls[0]?.[0];
    const operationId = response?.nativeOperation?.operationId;
    assert.isDefined(operationId);
    if (operationId === undefined) return;
    assert.deepEqual(response, {
      requestId,
      decision: "accept",
      nativeOperation: {
        operationId,
        operation: "respond_to_request",
        instanceId: ProviderInstanceId.make("codex-response"),
        threadId,
        providerSessionId,
        runtimeGeneration: "response-process",
      },
    });
    assert.match(
      respondToRuntimeRequest.mock.calls[0]?.[0].nativeOperation?.operationId ?? "",
      /^runtime-response:/u,
    );
  }).pipe(Effect.provide(testLayer));
});

it.effect("rejects expired runtime requests before invoking the live adapter", () => {
  const threadId = ThreadId.make("thread-runtime-request-expired");
  const providerSessionId = ProviderSessionId.make("provider-session-runtime-request-expired");
  const requestId = RuntimeRequestId.make("request-expired");
  const respondToRuntimeRequest = vi.fn(
    (_input: ProviderAdapterV2RuntimeRequestResponseInput) => Effect.void,
  );
  const getSession = vi.fn(() =>
    Effect.succeed(
      Option.some({
        respondToRuntimeRequest,
      } as never),
    ),
  );
  const projection = {
    runtimeRequests: [
      {
        id: requestId,
        nodeId: NodeId.make("node-runtime-request-expired"),
        providerTurnId: null,
        nativeRequestRef: null,
        kind: "command",
        status: "expired",
        responseCapability: {
          type: "live",
          providerSessionId,
        },
        createdAt: DateTime.makeUnsafe("2026-07-29T00:00:00.000Z"),
        resolvedAt: DateTime.makeUnsafe("2026-07-29T00:00:01.000Z"),
      },
    ],
  } as unknown as OrchestrationV2ThreadProjection;
  const testLayer = RuntimeRequestService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        standaloneSink,
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getRuntimeResponseContext: (threadId, requestId) =>
            Effect.succeed(
              runtimeContext(
                threadId,
                projection.runtimeRequests.find((request) => request.id === requestId),
              ),
            ),
        }),
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
          get: getSession,
        }),
      ),
    ),
  );

  return Effect.gen(function* () {
    const service = yield* RuntimeRequestService.RuntimeRequestServiceV2;
    const error = yield* service
      .respond({
        threadId,
        providerSessionId,
        requestId,
        decision: "accept",
      })
      .pipe(Effect.flip);

    assert.equal(error.reason, "request-not-ready");
    assert.equal(
      error.message,
      `Runtime request ${requestId} on thread ${threadId} is not ready for response execution.`,
    );
    assert.isUndefined(error.cause);
    assert.equal(getSession.mock.calls.length, 0);
    assert.equal(respondToRuntimeRequest.mock.calls.length, 0);
  }).pipe(Effect.provide(testLayer));
});

it.effect("classifies runtime request response validation failures", () => {
  const threadId = ThreadId.make("thread-runtime-request-validation");
  const providerSessionId = ProviderSessionId.make("provider-session-runtime-request-validation");
  const otherProviderSessionId = ProviderSessionId.make(
    "provider-session-runtime-request-validation-other",
  );
  const requestId = RuntimeRequestId.make("request-validation");
  const unexpectedGetSession = vi.fn(() => Effect.succeed(Option.none()));
  const missingLayer = runtimeRequestTestLayer(
    projectionWithRuntimeRequest(),
    unexpectedGetSession,
  );
  const notResumableLayer = runtimeRequestTestLayer(
    projectionWithRuntimeRequest(resolvedRuntimeRequest(requestId, otherProviderSessionId)),
    unexpectedGetSession,
  );
  const inactiveLayer = runtimeRequestTestLayer(
    projectionWithRuntimeRequest(resolvedRuntimeRequest(requestId, providerSessionId)),
    () => Effect.succeed(Option.none()),
  );
  const respond = Effect.gen(function* () {
    const service = yield* RuntimeRequestService.RuntimeRequestServiceV2;
    return yield* service
      .respond({
        threadId,
        providerSessionId,
        requestId,
        decision: "accept",
      })
      .pipe(Effect.flip);
  });

  return Effect.gen(function* () {
    const missing = yield* respond.pipe(Effect.provide(missingLayer));
    assert.equal(missing.reason, "request-missing");
    assert.equal(
      missing.message,
      `Runtime request ${requestId} no longer exists on thread ${threadId}.`,
    );
    assert.isUndefined(missing.cause);

    const notResumable = yield* respond.pipe(Effect.provide(notResumableLayer));
    assert.equal(notResumable.reason, "request-not-resumable");
    assert.equal(
      notResumable.message,
      `Runtime request ${requestId} on thread ${threadId} is not resumable on provider session ${providerSessionId}.`,
    );
    assert.isUndefined(notResumable.cause);

    const inactive = yield* respond.pipe(Effect.provide(inactiveLayer));
    assert.equal(inactive.reason, "provider-session-not-active");
    assert.equal(
      inactive.message,
      `Provider session ${providerSessionId} is not active for runtime request ${requestId} on thread ${threadId}.`,
    );
    assert.isUndefined(inactive.cause);
    assert.equal(unexpectedGetSession.mock.calls.length, 0);
  });
});

it.effect("preserves genuine provider session lookup failures as the cause", () => {
  const threadId = ThreadId.make("thread-runtime-request-lookup-failure");
  const providerSessionId = ProviderSessionId.make(
    "provider-session-runtime-request-lookup-failure",
  );
  const requestId = RuntimeRequestId.make("request-lookup-failure");
  const lookupFailure = new ProviderSessionManager.ProviderSessionLookupError({
    providerSessionId,
    cause: "lookup failed",
  });
  const testLayer = runtimeRequestTestLayer(
    projectionWithRuntimeRequest(resolvedRuntimeRequest(requestId, providerSessionId)),
    () => Effect.fail(lookupFailure),
  );

  return Effect.gen(function* () {
    const service = yield* RuntimeRequestService.RuntimeRequestServiceV2;
    const error = yield* service
      .respond({
        threadId,
        providerSessionId,
        requestId,
        decision: "accept",
      })
      .pipe(Effect.flip);

    assert.instanceOf(error, RuntimeRequestService.RuntimeRequestResponseExecutionError);
    assert.equal(error.reason, "unexpected-failure");
    assert.equal(
      error.message,
      `Failed to respond to runtime request ${requestId} on thread ${threadId} via provider session ${providerSessionId}.`,
    );
    assert.strictEqual(error.cause, lookupFailure);
  }).pipe(Effect.provide(testLayer));
});

function ordinaryClaimFixture(
  threadId: ThreadId,
  runId: RunId,
  request: OrchestrationEffectRequestV2,
) {
  const timestamp = "2026-10-03T00:00:00.000Z";
  const command = { type: "thread.turn.start", commandId: "command:original-start", threadId };
  const birth = {
    kind: "application_v2_thread_birth",
    threadId,
    eventId: "event:original-birth",
    sequence: 1,
  };
  const lease = {
    resourcePath: "/fixture/checkout",
    leaseId: "lease:original-start",
    ownerThreadId: threadId,
    ownerIncarnation: OrdinaryCheckout.ordinaryApplicationIncarnationV1(
      birth as OrdinaryCheckout.OrdinaryApplicationBirthV1,
    ),
    branch: "fixture-branch",
    acquiredAtMs: 1,
    renewedAtMs: 1,
    expiresAtMs: 300001,
  };
  const capture = Schema.decodeUnknownSync(OrdinaryCheckout.OrdinaryCheckoutCaptureV1)({
    version: 1,
    commandId: command.commandId,
    commandType: command.type,
    canonicalCommand: command,
    commandDigest: OrdinaryCheckout.ordinaryCheckoutCommandDigestV1(command),
    origin: { kind: "command" },
    threadId,
    applicationBirth: birth,
    projectId: "project:original-start",
    canonicalProjectRoot: "/fixture/repo",
    canonicalCheckoutPath: lease.resourcePath,
    branch: lease.branch,
    lease,
  });
  const admission = Schema.decodeUnknownSync(OrdinaryCheckout.OrdinaryCheckoutAdmissionV1)({
    version: 1,
    admissionId: OrdinaryCheckout.ordinaryCheckoutAdmissionIdV1(capture),
    capture: Schema.encodeSync(OrdinaryCheckout.OrdinaryCheckoutCaptureV1)(capture),
    receipt: {
      commandId: command.commandId,
      threadId,
      commandType: command.type,
      acceptedAt: timestamp,
      resultSequence: 2,
      status: "accepted",
      error: null,
    },
    eventBasis: [
      {
        eventId: "event:original-run",
        sequence: 2,
        threadId,
        commandId: command.commandId,
        eventType: "run.created",
      },
    ],
    run: {
      runId,
      runAttemptId: "attempt:original-start",
      nodeId: "node:original-start",
      messageId: "message:original-start",
    },
    recordedAt: timestamp,
  });
  const reference = OrdinaryCheckout.ordinaryCheckoutAdmissionRefV1(admission);
  const originalSource = {
    kind: "outbox",
    link: {
      version: 1,
      effectId: "effect:original-start",
      commandId: command.commandId,
      threadId,
      requestSha256: "c".repeat(64),
      admission: reference,
      recordedAt: timestamp,
    },
    workerId: "worker:original-start",
    expectedAttempt: 1,
    leaseExpiresAt: "2026-10-03T00:05:00.000Z",
  };
  const use = Schema.decodeUnknownSync(OrdinaryCheckout.OrdinaryCheckoutUseV1)({
    version: 1,
    kind: "ordinary_checkout_use",
    operationId: "effect:original-start:ordinary-checkout:attempt:1",
    admission: reference,
    source: originalSource,
    lease,
  });
  const source = Schema.decodeUnknownSync(OrdinaryCheckout.OrdinaryCheckoutOutboxExecutionSourceV1)(
    {
      ...originalSource,
      link: {
        ...originalSource.link,
        effectId: "effect:joined-control",
        commandId: "command:joined-control",
        requestSha256: nativeCreationSha256(
          nativeCreationCanonicalJson(Schema.encodeSync(OrchestrationEffectRequestV2)(request)),
        ),
      },
      workerId: "worker:joined-control",
      expectedAttempt: 2,
    },
  );
  const execution = OrdinaryCheckout.makeOrdinaryCheckoutExecutionRefV1({
    originalUse: use,
    executor: { kind: "actual_outbox_claim", source },
  });
  return { admission, use, execution };
}

it.effect(
  "revalidates the exact joined original claim after runtime lookup before answering",
  () => {
    const threadId = ThreadId.make("thread:ordinary-answer");
    const runId = RunId.make("run:ordinary-answer");
    const providerSessionId = ProviderSessionId.make("session:ordinary-answer");
    const requestId = RuntimeRequestId.make("request:ordinary-answer");
    const request = resolvedRuntimeRequest(requestId, providerSessionId);
    const input = { threadId, providerSessionId, requestId, decision: "accept" as const };
    const fixture = ordinaryClaimFixture(threadId, runId, {
      type: "runtime-request.respond",
      providerSessionId,
      requestId,
      decision: "accept",
    });
    const calls: string[] = [];
    const failure = new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
      reason: "claim_mismatch",
      threadId,
      path: fixture.use.lease.resourcePath,
      message: "The joined claim expired during runtime lookup.",
    });
    let staleAfterLookup = false;
    const testLayer = RuntimeRequestService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getRuntimeResponseContext: () =>
              Effect.succeed(runtimeContext(threadId, request, runId)),
          }),
          Layer.mock(EventSink.EventSinkV2)({
            readOrdinaryCheckoutAdmissionForRun: (target) =>
              Effect.sync(() => {
                assert.deepEqual(target, { threadId, runId });
                return fixture.admission;
              }),
            revalidateOrdinaryCheckoutExecution: (ref) =>
              Effect.gen(function* () {
                assert.strictEqual(ref, fixture.execution);
                calls.push("revalidate");
                if (staleAfterLookup && calls.at(-2) === "lookup") return yield* failure;
                return ref;
              }),
          }),
          Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
            get: () =>
              Effect.sync(() => {
                calls.push("lookup");
                return Option.some({
                  instanceId: ProviderInstanceId.make("ordinary-answer"),
                  respondToRuntimeRequest: () =>
                    Effect.sync(() => {
                      calls.push("answer");
                    }),
                } as never);
              }),
          }),
        ),
      ),
    );
    return Effect.gen(function* () {
      const service = yield* RuntimeRequestService.RuntimeRequestServiceV2;
      yield* service.respond({
        ...input,
        ordinaryCheckoutUse: fixture.use,
        ordinaryCheckoutExecution: fixture.execution,
      });
      assert.deepEqual(calls, ["revalidate", "lookup", "revalidate", "answer"]);
      calls.length = 0;
      staleAfterLookup = true;
      const error = yield* service
        .respond({
          ...input,
          ordinaryCheckoutUse: fixture.use,
          ordinaryCheckoutExecution: fixture.execution,
        })
        .pipe(Effect.flip);
      assert.strictEqual(error.cause, failure);
      assert.deepEqual(calls, ["revalidate", "lookup", "revalidate"]);
    }).pipe(Effect.provide(testLayer));
  },
);

it.effect(
  "holds admitted runtime answers with missing or mismatched original actors and response bodies",
  () => {
    const threadId = ThreadId.make("thread:ordinary-answer-fences");
    const runId = RunId.make("run:ordinary-answer-fences");
    const providerSessionId = ProviderSessionId.make("session:ordinary-answer-fences");
    const requestId = RuntimeRequestId.make("request:ordinary-answer-fences");
    const request = resolvedRuntimeRequest(requestId, providerSessionId);
    const fixture = ordinaryClaimFixture(threadId, runId, {
      type: "runtime-request.respond",
      providerSessionId,
      requestId,
      decision: "accept",
    });
    const input = { threadId, providerSessionId, requestId, decision: "accept" as const };
    const testLayer = RuntimeRequestService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getRuntimeResponseContext: () =>
              Effect.succeed(runtimeContext(threadId, request, runId)),
          }),
          Layer.mock(EventSink.EventSinkV2)({
            readOrdinaryCheckoutAdmissionForRun: () => Effect.succeed(fixture.admission),
            revalidateOrdinaryCheckoutExecution: () =>
              Effect.die("Mismatched response must fail before SQL execution entry."),
          }),
          Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
            get: () => Effect.die("Mismatched response must not look up a runtime."),
          }),
        ),
      ),
    );
    return Effect.gen(function* () {
      const service = yield* RuntimeRequestService.RuntimeRequestServiceV2;
      const wrongUse = { ...fixture.use, operationId: "replacement-operation" };
      const wrongAdmission = OrdinaryCheckout.makeOrdinaryCheckoutExecutionRefV1({
        originalUse: {
          ...fixture.use,
          admission: { ...fixture.use.admission, admissionId: "d".repeat(64) },
        },
        executor: fixture.execution.executor,
      });
      for (const context of [
        {},
        { ordinaryCheckoutUse: fixture.use },
        { ordinaryCheckoutUse: wrongUse, ordinaryCheckoutExecution: fixture.execution },
        { ordinaryCheckoutExecution: wrongAdmission },
        {
          ordinaryCheckoutUse: fixture.use,
          ordinaryCheckoutExecution: fixture.execution,
          decision: "decline" as const,
        },
      ]) {
        const error = yield* service.respond({ ...input, ...context }).pipe(Effect.flip);
        assert.equal(error.reason, "request-not-resumable");
        assert.equal(error.cause, "The runtime response has no matching original checkout claim.");
      }
    }).pipe(Effect.provide(testLayer));
  },
);

it.effect("missing runtime request nodes cannot bypass checkout admission detection", () => {
  const threadId = ThreadId.make("thread:answer-missing-node");
  const providerSessionId = ProviderSessionId.make("session:answer-missing-node");
  const requestId = RuntimeRequestId.make("request:answer-missing-node");
  const request = resolvedRuntimeRequest(requestId, providerSessionId);
  const testLayer = RuntimeRequestService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getRuntimeResponseContext: () =>
            Effect.succeed({
              request,
              node: undefined,
              item: undefined,
              session: undefined,
            }),
        }),
        standaloneSink,
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
          get: () => Effect.die("Missing node must not select today's runtime."),
        }),
      ),
    ),
  );
  return Effect.gen(function* () {
    const service = yield* RuntimeRequestService.RuntimeRequestServiceV2;
    const error = yield* service
      .respond({ threadId, providerSessionId, requestId, decision: "accept" })
      .pipe(Effect.flip);
    assert.equal(error.reason, "request-not-resumable");
    assert.equal(error.cause, "The runtime response has no recorded request node.");
  }).pipe(Effect.provide(testLayer));
});
