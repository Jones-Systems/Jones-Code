import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  EnvironmentId,
  PreviewTabId,
  ProviderInstanceId,
  ThreadId,
  type PreviewAutomationOperation,
  type PreviewAutomationRequest,
  type PreviewAutomationResponse,
  type PreviewAutomationStreamEvent,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import * as Broker from "../../mcp/PreviewAutomationBroker.ts";
import { selectRecordingOperation } from "./projection.ts";

const makeBroker = Broker.make.pipe(Effect.provide(NodeServices.layer));
const scope = {
  environmentId: EnvironmentId.make("recording-environment"),
  thread: {
    threadId: ThreadId.make("recording-thread"),
    providerSessionId: "recording-session",
    providerInstanceId: ProviderInstanceId.make("codex"),
  },
  requestNamespace: "recording-session",
  issuedAt: 1,
  client: undefined,
  capabilities: new Set(["preview"] as const),
};
const tabId = PreviewTabId.make("recording-tab");
const identity = {
  schemaVersion: 1,
  runtimeKind: "electron",
  runtimeInstanceId: "dispatch-runtime",
  appVersion: "test",
  buildCommit: null,
} as const;
type Routed = PreviewAutomationRequest & { readonly connectionId: string };
type BrokerService = Broker.PreviewAutomationBroker["Service"];

const host = Effect.fn("test.recording.host")(function* (
  broker: BrokerService,
  options?: Broker.PreviewAutomationConnectOptions,
) {
  const connected = yield* Deferred.make<string>();
  const requests = yield* Queue.unbounded<Routed>();
  const seen: Routed[] = [];
  const events = yield* broker.connect(
    { clientId: "host", environmentId: scope.environmentId, runtimeIdentity: identity },
    options,
  );
  const fiber = yield* Stream.runForEach(events, (event: PreviewAutomationStreamEvent) => {
    if (event.type === "connected") return Deferred.succeed(connected, event.connectionId);
    const request = { ...event.request, connectionId: event.connectionId };
    seen.push(request);
    return Queue.offer(requests, request);
  }).pipe(Effect.forkScoped);
  const connectionId = yield* Deferred.await(connected);
  return { requests, seen, fiber, connectionId };
});
type Host = Effect.Success<ReturnType<typeof host>>;
const start = Effect.fn("test.recording.start")(function* (
  broker: BrokerService,
  target: Host,
  options: {
    readonly tabId?: PreviewTabId;
    readonly unbound?: boolean;
    readonly operation?: PreviewAutomationOperation;
  } = {},
) {
  const caller = yield* broker
    .invoke({
      scope,
      operation: options.operation ?? "recordingStart",
      input: {},
      ...(options.unbound ? {} : { tabId: options.tabId ?? tabId }),
      timeoutMs: 100,
      failurePolicy: "request_only",
    })
    .pipe(Effect.exit, Effect.forkScoped);
  const request = yield* Queue.take(target.requests);
  const handle = {
    requestId: request.requestId,
    clientId: "host",
    connectionId: request.connectionId,
  };
  return { caller, request, handle };
});
const read = (
  broker: BrokerService,
  handle?: { requestId: string; clientId: string; connectionId: string },
  target = tabId,
) =>
  broker.readRecordingOperation({
    scope,
    input: { tabId: target, ...(handle === undefined ? {} : { handle }) },
  });
const unknownReply = {
  ok: false,
  error: {
    _tag: "PreviewAutomationTimeoutError",
    message: "private-error-sentinel",
    outcome: "unknown" as const,
  },
};
const success = { ok: true, result: { tabId, recording: true, startedAt: "2026-11-02T12:00:00Z" } };
const fence = Effect.fn("test.recording.fence")(function* (broker: BrokerService, target = tabId) {
  expect(
    yield* broker
      .invoke({ scope, operation: "click", tabId: target, input: {} })
      .pipe(Effect.flip, Effect.orDie),
  ).toMatchObject({
    _tag: "PreviewAutomationTimeoutError",
    outcome: "not_started",
    unreconciled: true,
  });
});

it.effect.each(["expiry", "cap"] as const)(
  "retains exact recording evidence after tombstone %s without changing the fence",
  (loss) =>
    Effect.scoped(
      Effect.gen(function* () {
        const broker = yield* makeBroker;
        let resolutions = 0;
        const target = yield* host(broker, {
          resolveRuntimeEvidence: () =>
            Effect.sync(() => {
              resolutions++;
              return { runtimeIdentity: identity, attachmentGeneration: 7 };
            }),
        });
        const original = yield* start(broker, target);
        expect(yield* read(broker)).toMatchObject({
          result: {
            status: "pending",
            dispatched: true,
            handle: original.handle,
            dispatchEvidence: {
              basis: "dispatch_time",
              runtimeIdentity: identity,
              attachmentGeneration: 7,
            },
          },
        });
        yield* TestClock.adjust(1100);
        expect((yield* Fiber.join(original.caller))._tag).toBe("Failure");
        expect(yield* read(broker, original.handle)).toMatchObject({
          result: { status: "unknown" },
        });
        if (loss === "expiry") yield* TestClock.adjust(60_001);
        else {
          const overflow = yield* Effect.forEach(
            Array.from({ length: 256 }),
            () =>
              broker
                .invoke({
                  scope,
                  operation: "waitFor",
                  input: {},
                  tabId,
                  timeoutMs: 100,
                  failurePolicy: "request_only",
                })
                .pipe(Effect.exit),
            { concurrency: "unbounded" },
          ).pipe(Effect.forkScoped);
          for (let i = 0; i < 256; i++) yield* Queue.take(target.requests);
          yield* TestClock.adjust(1100);
          yield* Fiber.join(overflow);
        }
        const before = { requests: target.seen.length, resolutions };
        for (const mismatch of [
          { clientId: "foreign" },
          { connectionId: "replacement" },
          { requestId: "missing" },
        ]) {
          yield* broker.respond({ ...original.handle, ...success, ...mismatch });
          expect(yield* read(broker, original.handle)).toMatchObject({
            result: { status: "unknown" },
          });
        }
        for (const response of [{ ok: false }, unknownReply, { ...unknownReply, ok: true }]) {
          yield* broker.respond({ ...original.handle, ...response });
          expect(yield* read(broker, original.handle)).toMatchObject({
            result: { status: "unknown" },
          });
          yield* fence(broker);
        }
        expect({ requests: target.seen.length, resolutions }).toEqual(before);
        yield* broker.respond({
          ...original.handle,
          ...success,
          result: { ...success.result, ignored: "private-start-sentinel" },
        });
        const settled = yield* read(broker, original.handle);
        expect(JSON.stringify(settled)).not.toContain("private-start-sentinel");
        expect(settled).toMatchObject({
          result: {
            status: "settled",
            reportedOutcome: "success",
            metadata: { status: "valid", reportedStart: success.result },
            dispatchEvidence: {
              basis: "dispatch_time",
              runtimeIdentity: identity,
              attachmentGeneration: 7,
            },
          },
        });
        yield* TestClock.adjust(1000);
        yield* broker.respond({
          ...original.handle,
          ok: false,
          error: { _tag: "Error", message: "contradict duplicate", outcome: "not_started" },
        });
        expect(yield* read(broker, original.handle)).toEqual(settled);
        expect({ requests: target.seen.length, resolutions }).toEqual(before);
      }),
    ),
);

it.effect(
  "retains original dispatch evidence and exact authority through replacement and disconnect",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const broker = yield* makeBroker;
        const old = yield* host(broker);
        const original = yield* start(broker, old);
        const replacement = yield* host(broker);
        yield* Fiber.join(original.caller);
        expect(yield* read(broker, original.handle)).toMatchObject({
          result: { status: "unknown" },
        });
        yield* broker.respond({
          ...original.handle,
          ...success,
          connectionId: replacement.connectionId,
        });
        yield* fence(broker);
        yield* Fiber.interrupt(replacement.fiber);
        expect(yield* read(broker, original.handle)).toMatchObject({
          result: { status: "unknown" },
        });
        yield* broker.respond({ ...original.handle, ...success });
        expect(yield* read(broker, original.handle)).toMatchObject({
          result: {
            status: "settled",
            dispatchEvidence: { runtimeIdentity: identity, attachmentGeneration: null },
          },
        });
        expect(replacement.seen).toHaveLength(0);
      }),
    ),
);

it.effect(
  "matches all stable caller fields, excludes tab-unbound dispatch, and never falls back from a handle miss",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const broker = yield* makeBroker;
        const target = yield* host(broker);
        const original = yield* start(broker, target);
        const unavailable = {
          schemaVersion: 1,
          tabId,
          result: { status: "unavailable", reason: "not_retained" },
        };
        for (const foreign of [
          { ...scope, environmentId: EnvironmentId.make("foreign") },
          { ...scope, thread: { ...scope.thread, threadId: ThreadId.make("foreign") } },
          { ...scope, thread: { ...scope.thread, providerSessionId: "foreign" } },
          {
            ...scope,
            thread: { ...scope.thread, providerInstanceId: ProviderInstanceId.make("foreign") },
          },
        ]) {
          for (const handle of [undefined, original.handle])
            expect(
              yield* broker.readRecordingOperation({
                scope: foreign,
                input: { tabId, ...(handle === undefined ? {} : { handle }) },
              }),
            ).toEqual(unavailable);
        }
        expect(
          yield* broker.readRecordingOperation({
            scope: { ...scope, requestNamespace: "renewed", issuedAt: 99 },
            input: { tabId, handle: original.handle },
          }),
        ).toEqual(yield* read(broker, original.handle));
        expect(yield* read(broker, { ...original.handle, requestId: "missing" })).toEqual(
          unavailable,
        );
        yield* broker.respond({ ...original.handle, ...success });
        yield* Fiber.join(original.caller);
        expect(yield* read(broker, original.handle)).toEqual(unavailable);

        const fresh = yield* makeBroker;
        const freshHost = yield* host(fresh);
        const unbound = yield* start(fresh, freshHost, { unbound: true });
        yield* fresh.respond({ ...unbound.handle, ...unknownReply });
        yield* Fiber.join(unbound.caller);
        expect(yield* read(fresh, unbound.handle)).toEqual(unavailable);
        expect(yield* read(fresh)).toEqual(unavailable);
      }),
    ),
);

const payloadCases: ReadonlyArray<{
  name: string;
  response: Pick<PreviewAutomationResponse, "ok" | "result" | "error">;
  outcome: string;
  metadata: string;
}> = [
  { name: "missing success", response: { ok: true }, outcome: "success", metadata: "invalid" },
  {
    name: "wrong tab",
    response: { ok: true, result: { ...success.result, tabId: "foreign" } },
    outcome: "success",
    metadata: "invalid",
  },
  {
    name: "wrong type",
    response: { ok: true, result: { ...success.result, recording: "yes" } },
    outcome: "success",
    metadata: "invalid",
  },
  {
    name: "oversized timestamp",
    response: { ok: true, result: { ...success.result, startedAt: "x".repeat(129) } },
    outcome: "success",
    metadata: "invalid",
  },
  {
    name: "legacy failure",
    response: { ok: false, error: { _tag: "Legacy", message: "private-error-sentinel" } },
    outcome: "failure",
    metadata: "not_projected",
  },
  {
    name: "reported failure",
    response: {
      ok: false,
      error: { _tag: "Error", message: "private-error-sentinel", outcome: "reported" },
    },
    outcome: "failure",
    metadata: "not_projected",
  },
  {
    name: "explicit not started",
    response: {
      ok: false,
      error: { _tag: "Error", message: "private-error-sentinel", outcome: "not_started" },
    },
    outcome: "not_started",
    metadata: "not_projected",
  },
  {
    name: "contradictory accepted envelope",
    response: {
      ok: true,
      result: success.result,
      error: { _tag: "Error", message: "private-error-sentinel", outcome: "not_started" },
    },
    outcome: "unclassifiable",
    metadata: "invalid",
  },
];
it.effect.each(payloadCases)(
  "separates accepted settlement from metadata: $name",
  ({ response, outcome, metadata }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const broker = yield* makeBroker;
        const target = yield* host(broker);
        const original = yield* start(broker, target);
        yield* broker.respond({ ...original.handle, ...unknownReply });
        yield* Fiber.join(original.caller);
        yield* broker.respond({ ...original.handle, ...response });
        const result = yield* read(broker, original.handle);
        expect(result).toMatchObject({
          result: { status: "settled", reportedOutcome: outcome, metadata: { status: metadata } },
        });
        expect(JSON.stringify(result)).not.toContain("private-error-sentinel");
        const next = yield* start(broker, target);
        expect(next.request.operation).toBe("recordingStart");
      }),
    ),
);

it.effect(
  "never projects stop artifacts and never retains normal or non-recording completions",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const broker = yield* makeBroker;
        const target = yield* host(broker);
        for (const operation of ["recordingStart", "recordingStop", "click"] as const) {
          const normal = yield* start(broker, target, { operation });
          yield* broker.respond({
            ...normal.handle,
            ok: true,
            result: { ...success.result, path: "private-artifact-sentinel" },
          });
          yield* Fiber.join(normal.caller);
          expect(yield* read(broker, normal.handle)).toMatchObject({
            result: { status: "unavailable" },
          });
        }
        const normalFailure = yield* start(broker, target);
        yield* broker.respond({
          ...normalFailure.handle,
          ok: false,
          error: { _tag: "Error", message: "failure", outcome: "reported" },
        });
        yield* Fiber.join(normalFailure.caller);
        expect(yield* read(broker, normalFailure.handle)).toMatchObject({
          result: { status: "unavailable" },
        });
        const stop = yield* start(broker, target, { operation: "recordingStop" });
        yield* broker.respond({ ...stop.handle, ...unknownReply });
        yield* Fiber.join(stop.caller);
        yield* broker.respond({
          ...stop.handle,
          ok: true,
          result: {
            path: "private-artifact-sentinel",
            id: "private-artifact-sentinel",
            uploadedAttachmentId: "private-artifact-sentinel",
            url: "private-artifact-sentinel",
          },
        });
        const result = yield* read(broker, stop.handle);
        expect(result).toMatchObject({
          result: {
            status: "settled",
            operation: "recordingStop",
            metadata: { status: "not_projected" },
          },
        });
        expect(JSON.stringify(result)).not.toContain("private-artifact-sentinel");
        const nonrecording = yield* start(broker, target, { operation: "click" });
        yield* broker.respond({ ...nonrecording.handle, ...unknownReply });
        yield* Fiber.join(nonrecording.caller);
        yield* broker.respond({ ...nonrecording.handle, ok: true });
        expect(yield* read(broker, nonrecording.handle)).toMatchObject({
          result: { status: "unavailable" },
        });
      }),
    ),
);

it.effect(
  "discovers mixed retained states as bounded ambiguity rather than preferring latest",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const broker = yield* makeBroker;
        const target = yield* host(broker);
        const handles = [];
        let activeHost = target;
        for (let i = 0; i < 8; i++) {
          if (i === 4) activeHost = yield* host(broker);
          const original = yield* start(broker, activeHost);
          handles.push(original.handle);
          if (i === 1)
            expect(yield* read(broker)).toMatchObject({
              result: {
                status: "ambiguous",
                truncated: false,
                candidates: [{ status: "pending" }, { status: "settled" }],
              },
            });
          yield* broker.respond({ ...original.handle, ...unknownReply });
          yield* Fiber.join(original.caller);
          yield* broker.respond({ ...original.handle, ...success });
        }
        const pending = yield* start(broker, activeHost);
        expect(yield* read(broker)).toMatchObject({
          result: { status: "ambiguous", truncated: true },
        });
        const ambiguous = (yield* read(broker)).result;
        if (ambiguous.status === "ambiguous") expect(ambiguous.candidates).toHaveLength(8);
        expect(yield* read(broker, pending.handle)).toMatchObject({
          result: { status: "pending" },
        });
        yield* broker.respond({ ...pending.handle, ...unknownReply });
        yield* Fiber.join(pending.caller);
        expect(yield* read(broker)).toMatchObject({
          result: { status: "ambiguous", truncated: true },
        });
        expect(yield* read(broker, handles[0])).toMatchObject({ result: { status: "settled" } });
        expect(yield* read(broker, { ...pending.handle, connectionId: "missing" })).toMatchObject({
          result: { status: "unavailable" },
        });
      }),
    ),
);

it.effect(
  "evicts completed entries FIFO without renewing reads, recreating duplicates or weakening quarantine",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const broker = yield* makeBroker;
        const target = yield* host(broker);
        const unresolvedTab = PreviewTabId.make("unresolved-tab");
        const unresolved = yield* start(broker, target, { tabId: unresolvedTab });
        yield* broker.respond({ ...unresolved.handle, ...unknownReply });
        yield* Fiber.join(unresolved.caller);
        const handles = [];
        for (let i = 0; i < 257; i++) {
          const original = yield* start(broker, target);
          handles.push(original.handle);
          yield* broker.respond({ ...original.handle, ...unknownReply });
          yield* Fiber.join(original.caller);
          yield* broker.respond({ ...original.handle, ...success });
          if (i === 255) {
            expect(yield* read(broker, handles[0])).toMatchObject({
              result: { status: "settled" },
            });
            yield* broker.respond({ ...handles[0]!, ...success });
          }
        }
        expect(yield* read(broker, handles[0])).toMatchObject({
          result: { status: "unavailable", reason: "not_retained" },
        });
        yield* broker.respond({ ...handles[0]!, ...success });
        expect(yield* read(broker, handles[0])).toMatchObject({
          result: { status: "unavailable" },
        });
        expect(yield* read(broker, handles[1])).toMatchObject({ result: { status: "settled" } });
        expect(yield* read(broker, handles[256])).toMatchObject({ result: { status: "settled" } });
        expect(yield* read(broker, unresolved.handle, unresolvedTab)).toMatchObject({
          result: { status: "unknown" },
        });
        yield* fence(broker, unresolvedTab);
        const fresh = yield* makeBroker;
        expect(yield* read(fresh, handles[256])).toMatchObject({
          result: { status: "unavailable" },
        });
      }),
    ),
);

it.effect("omits evidence before dispatch and does not call the blocked resolver on read", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let resolutions = 0;
      const target = yield* host(broker, {
        resolveRuntimeEvidence: () =>
          Effect.gen(function* () {
            resolutions++;
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(release);
            return { runtimeIdentity: identity, attachmentGeneration: 1 };
          }),
      });
      const caller = yield* broker
        .invoke({
          scope,
          operation: "recordingStart",
          tabId,
          input: {},
          failurePolicy: "request_only",
        })
        .pipe(Effect.exit, Effect.forkScoped);
      yield* Deferred.await(entered);
      const result = yield* read(broker);
      expect(result).toMatchObject({ result: { status: "pending", dispatched: false } });
      expect(result.result).not.toHaveProperty("dispatchEvidence");
      expect(resolutions).toBe(1);
      yield* Deferred.succeed(release, undefined);
      const request = yield* Queue.take(target.requests);
      yield* broker.respond({
        clientId: "host",
        requestId: request.requestId,
        connectionId: request.connectionId,
        ...success,
      });
      yield* Fiber.join(caller);
    }),
  ),
);

it("deduplicates an inconsistent snapshot in favor of unresolved evidence", () => {
  const context = {
    ...scope.thread,
    environmentId: scope.environmentId,
    operation: "recordingStart" as const,
    requestId: "preview-1",
    clientId: "host",
    connectionId: "connection",
    tabId,
  };
  const entry = { context, dispatched: true };
  const result = selectRecordingOperation(
    {
      pending: new Map([[context.requestId, entry]]),
      quarantine: new Map([[context.requestId, entry]]),
      completedRecordingOperations: new Map([
        [
          context.requestId,
          {
            context,
            result: {
              status: "settled",
              handle: {
                requestId: context.requestId,
                clientId: context.clientId,
                connectionId: context.connectionId,
              },
              operation: "recordingStart",
              reportedOutcome: "success",
              metadata: { status: "invalid" },
              settledAt: "2026-11-02T12:00:00Z",
            },
          },
        ],
      ]),
    },
    scope,
    { tabId },
  );
  expect(result.result.status).toBe("unknown");
});
