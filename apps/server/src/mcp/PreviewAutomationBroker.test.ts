import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  PreviewAutomationClientDisconnectedError,
  PreviewAutomationInvalidSelectorError,
  PreviewAutomationMalformedResponseError,
  PreviewAutomationNoAvailableHostError,
  PreviewAutomationTargetNotEditableError,
  PreviewTabId,
  ProviderInstanceId,
  ThreadId,
  type PreviewAutomationHost,
  type PreviewAutomationRequest,
  type PreviewAutomationStreamEvent,
  SERVER_BROWSER_AUTOMATION_CLIENT_ID,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Result from "effect/Result";
import * as Scheduler from "effect/Scheduler";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import * as PreviewAutomationBroker from "./PreviewAutomationBroker.ts";

const makeBroker = PreviewAutomationBroker.make.pipe(Effect.provide(NodeServices.layer));

const scope = {
  environmentId: EnvironmentId.make("environment-1"),
  requestNamespace: "provider-session-1",
  thread: {
    threadId: ThreadId.make("thread-1"),
    providerSessionId: "provider-session-1",
    providerInstanceId: ProviderInstanceId.make("codex"),
  },
  client: undefined,
  capabilities: new Set(["preview"] as const),
  issuedAt: 1,
};

const makeHost = (overrides: Partial<PreviewAutomationHost> = {}): PreviewAutomationHost => ({
  clientId: "client-1",
  environmentId: scope.environmentId,
  ...overrides,
});

const testRuntimeIdentity = {
  schemaVersion: 1,
  runtimeKind: "electron",
  runtimeInstanceId: "test-runtime",
  appVersion: "test",
  buildCommit: null,
} as const;

type RoutedRequest = PreviewAutomationRequest & {
  readonly connectionId: PreviewAutomationStreamEvent["connectionId"];
};

const requestsFrom = (
  events: Stream.Stream<PreviewAutomationStreamEvent>,
  onConnected: (connectionId: PreviewAutomationStreamEvent["connectionId"]) => void = () => {},
): Stream.Stream<RoutedRequest> =>
  events.pipe(
    Stream.filterMap((event) => {
      if (event.type === "connected") {
        onConnected(event.connectionId);
        return Result.failVoid;
      }
      return Result.succeed({ ...event.request, connectionId: event.connectionId });
    }),
  );

it.effect("atomically registers a connected host and correlates its response", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const requests = requestsFrom(yield* broker.connect(makeHost()));
      yield* Stream.runForEach(requests, (request) =>
        broker.respond({
          clientId: "client-1",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: true,
          result: { available: true },
        }),
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      const result = yield* broker.invoke<{ available: boolean }>({
        scope,
        operation: "open",
        input: {},
      });

      expect(result).toEqual({ available: true });
    }),
  ),
);

it.effect("targets multiple tabs explicitly while retaining a default tab", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const appTabId = PreviewTabId.make("tab-web-app");
      const simulatorTabId = PreviewTabId.make("tab-ios-simulator");
      const openedTabIds = [appTabId, simulatorTabId];
      let openIndex = 0;
      const routedRequests: RoutedRequest[] = [];
      const requests = requestsFrom(yield* broker.connect(makeHost()));
      yield* Stream.runForEach(requests, (request) => {
        routedRequests.push(request);
        return broker.respond({
          clientId: "client-1",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: true,
          result:
            request.operation === "open"
              ? { available: true, tabId: openedTabIds[openIndex++] }
              : { url: "http://localhost:3200" },
        });
      }).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      yield* broker.invoke({ scope, operation: "open", input: { reuseExistingTab: false } });
      yield* broker.invoke({ scope, operation: "open", input: { reuseExistingTab: false } });
      yield* broker.invoke({ scope, operation: "snapshot", input: {} });
      yield* broker.invoke({ scope, operation: "snapshot", input: {}, tabId: appTabId });
      yield* broker.invoke({ scope, operation: "snapshot", input: {} });

      expect(routedRequests).toHaveLength(5);
      expect(routedRequests[0]?.tabId).toBeUndefined();
      expect(routedRequests[1]?.tabId).toBe(appTabId);
      expect(routedRequests[2]?.tabId).toBe(simulatorTabId);
      expect(routedRequests[2]?.tabIdExplicit).toBe(false);
      expect(routedRequests[3]?.tabId).toBe(appTabId);
      expect(routedRequests[3]?.tabIdExplicit).toBe(true);
      expect(routedRequests[4]?.tabId).toBe(appTabId);
    }),
  ),
);

it.effect.each([true, false])(
  "keeps an older target stable while a newer explicit tab responds (implicit: %s)",
  (implicit) =>
    Effect.scoped(
      Effect.gen(function* () {
        const broker = yield* makeBroker;
        const olderTabId = PreviewTabId.make("tab-older-request");
        const newerTabId = PreviewTabId.make("tab-newer-request");
        const releaseOlderResponse = yield* Deferred.make<void>();
        const routedRequests: RoutedRequest[] = [];
        const requests = requestsFrom(yield* broker.connect(makeHost()));
        yield* Stream.runForEach(requests, (request) => {
          routedRequests.push(request);
          const response = Effect.gen(function* () {
            if (request.tabId === olderTabId && request.operation === "snapshot") {
              yield* Deferred.await(releaseOlderResponse);
            }
            yield* broker.respond({
              clientId: "client-1",
              connectionId: request.connectionId,
              requestId: request.requestId,
              ok: true,
              result: { url: "http://localhost:3200" },
            });
            if (request.tabId === newerTabId) {
              yield* Deferred.succeed(releaseOlderResponse, undefined);
            }
          });
          return response.pipe(Effect.forkScoped, Effect.asVoid);
        }).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;

        yield* broker.invoke({ scope, operation: "status", input: {}, tabId: olderTabId });
        let capturedTabId: PreviewTabId | undefined;
        const older = yield* broker
          .invoke({
            scope,
            operation: "snapshot",
            input: {},
            ...(implicit ? {} : { tabId: olderTabId }),
            onTargetTab: (tabId) => {
              capturedTabId = tabId;
            },
          })
          .pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        const newer = yield* broker
          .invoke({ scope, operation: "snapshot", input: {}, tabId: newerTabId })
          .pipe(Effect.forkScoped);
        yield* Fiber.join(newer);
        yield* Fiber.join(older);
        yield* broker.invoke({
          scope,
          operation: "status",
          input: {},
          tabId: olderTabId,
          updateCurrentTab: false,
        });
        yield* broker.invoke({ scope, operation: "snapshot", input: {} });

        expect(routedRequests.at(-1)?.tabId).toBe(newerTabId);
        expect(capturedTabId).toBe(olderTabId);
      }),
    ),
);

it.effect("tracks the tab returned by a targeted recording stop", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const browsingTabId = PreviewTabId.make("tab-session-b");
      const recordingTabId = PreviewTabId.make("tab-session-a-recording");
      const routedRequests: RoutedRequest[] = [];
      const requests = requestsFrom(yield* broker.connect(makeHost()));
      yield* Stream.runForEach(requests, (request) => {
        routedRequests.push(request);
        return broker.respond({
          clientId: "client-1",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: true,
          result:
            request.operation === "open"
              ? { available: true, tabId: browsingTabId }
              : request.operation === "recordingStop"
                ? { id: "recording-1", tabId: recordingTabId }
                : { url: "http://localhost:3200" },
        });
      }).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      yield* broker.invoke({ scope, operation: "open", input: {} });
      yield* broker.invoke({ scope, operation: "recordingStop", input: {} });
      yield* broker.invoke({ scope, operation: "snapshot", input: {} });

      expect(routedRequests.at(-1)?.tabId).toBe(recordingTabId);
    }),
  ),
);

it.effect("does not let a no-tab response suppress an earlier tab decision", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const initialTabId = PreviewTabId.make("tab-initial");
      const openedTabId = PreviewTabId.make("tab-opened-late");
      const releaseOpenResponse = yield* Deferred.make<void>();
      const routedRequests: RoutedRequest[] = [];
      const requests = requestsFrom(yield* broker.connect(makeHost()));
      yield* Stream.runForEach(requests, (request) => {
        routedRequests.push(request);
        const marker =
          typeof request.input === "object" && request.input !== null && "marker" in request.input
            ? request.input.marker
            : undefined;
        const response = Effect.gen(function* () {
          if (marker === "older") {
            yield* Deferred.await(releaseOpenResponse);
          }
          yield* broker.respond({
            clientId: "client-1",
            connectionId: request.connectionId,
            requestId: request.requestId,
            ok: true,
            result:
              request.operation === "open"
                ? { available: true, tabId: marker === "older" ? openedTabId : initialTabId }
                : { url: "http://localhost:3200" },
          });
          if (marker === "newer") {
            yield* Deferred.succeed(releaseOpenResponse, undefined);
          }
        });
        return response.pipe(Effect.forkScoped, Effect.asVoid);
      }).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      yield* broker.invoke({ scope, operation: "open", input: {} });
      const older = yield* broker
        .invoke({
          scope,
          operation: "open",
          input: { marker: "older", reuseExistingTab: false },
        })
        .pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      const newer = yield* broker
        .invoke({ scope, operation: "snapshot", input: { marker: "newer" } })
        .pipe(Effect.forkScoped);
      yield* Fiber.join(newer);
      yield* Fiber.join(older);
      yield* broker.invoke({ scope, operation: "snapshot", input: {} });

      expect(routedRequests.at(-1)?.tabId).toBe(openedTabId);
    }),
  ),
);

it.effect("announces a live replacement stream before delivering requests", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const events = yield* broker.connect(makeHost());
      const receivedTypes: PreviewAutomationStreamEvent["type"][] = [];
      const consumer = yield* events.pipe(
        Stream.take(2),
        Stream.runForEach((event) => {
          receivedTypes.push(event.type);
          return event.type === "connected"
            ? Effect.void
            : broker.respond({
                clientId: "client-1",
                connectionId: event.connectionId,
                requestId: event.request.requestId,
                ok: true,
                result: "ready",
              });
        }),
        Effect.forkScoped,
      );
      yield* Effect.yieldNow;

      const result = yield* broker.invoke<string>({ scope, operation: "status", input: {} });
      yield* Fiber.join(consumer);

      expect(receivedTypes).toEqual(["connected", "request"]);
      expect(result).toBe("ready");
    }),
  ),
);

it.effect(
  "keeps a server-host open alive for installation without extending other operations",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const broker = yield* makeBroker;
        const received = yield* Deferred.make<RoutedRequest>();
        const requests = requestsFrom(yield* broker.connect(makeHost(), { preferred: true }));
        yield* Stream.runForEach(requests, (request) =>
          request.operation === "open"
            ? Deferred.succeed(received, request)
            : broker.respond({
                clientId: "client-1",
                connectionId: request.connectionId,
                requestId: request.requestId,
                ok: true,
                result: request.timeoutMs,
              }),
        ).pipe(Effect.forkScoped);
        const opening = yield* broker
          .invoke({ scope, operation: "open", input: {} })
          .pipe(Effect.forkScoped);
        const request = yield* Deferred.await(received);
        yield* TestClock.adjust(16_000);
        yield* broker.respond({
          clientId: "client-1",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: true,
          result: "opened",
        });
        expect(yield* Fiber.join(opening)).toBe("opened");
        expect(yield* broker.invoke({ scope, operation: "status", input: {} })).toBe(15_000);
        expect(yield* broker.invoke({ scope, operation: "navigate", input: {} })).toBe(15_000);
      }),
    ),
);

it.effect("preserves bounded request and remote selector diagnostics", () => {
  const locator = "role=button[name='request-secret']";
  const remoteMessage = "Unexpected token near remote-secret.";
  const remoteError = {
    _tag: "PreviewAutomationInvalidSelectorError",
    message: remoteMessage,
    detail: { selector: "role=button[name='remote-secret']" },
  } as const;

  return Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const requests = requestsFrom(yield* broker.connect(makeHost()));
      yield* Stream.runForEach(requests, (request) =>
        broker.respond({
          clientId: "client-1",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: false,
          error: remoteError,
        }),
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      const error = yield* broker
        .invoke<void>({
          scope,
          operation: "click",
          input: { locator },
          tabId: PreviewTabId.make("tab-1"),
          timeoutMs: 1_234,
        })
        .pipe(Effect.flip);

      expect(error).toBeInstanceOf(PreviewAutomationInvalidSelectorError);
      expect(error).toMatchObject({
        operation: "click",
        environmentId: scope.environmentId,
        threadId: scope.thread.threadId,
        providerSessionId: scope.thread.providerSessionId,
        providerInstanceId: scope.thread.providerInstanceId,
        clientId: "client-1",
        requestId: "preview-0",
        tabId: "tab-1",
        timeoutMs: 1_234,
        selectorKind: "locator",
        selectorLength: locator.length,
        remoteTag: "PreviewAutomationInvalidSelectorError",
        remoteMessageLength: remoteMessage.length,
        remoteDetailKind: "object",
      });
      expect(error.message).toBe(
        `Preview automation click received an invalid locator (${locator.length} characters).`,
      );
      expect(error.message).not.toContain("secret");
      expect(error.cause).toBe(remoteError);
      expect("selector" in error).toBe(false);
      expect("remoteMessage" in error).toBe(false);
      expect("remoteDetail" in error).toBe(false);
    }),
  );
});

it.effect("classifies a remote non-editable target without collapsing it to execution", () => {
  const remoteError = {
    _tag: "PreviewAutomationTargetNotEditableError",
    message: "remote target details",
    detail: { selectorKind: "focused-element" },
  } as const;

  return Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const requests = requestsFrom(yield* broker.connect(makeHost()));
      yield* Stream.runForEach(requests, (request) =>
        broker.respond({
          clientId: "client-1",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: false,
          error: remoteError,
        }),
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      const error = yield* broker
        .invoke<void>({
          scope,
          operation: "type",
          input: { text: "hello" },
          tabId: PreviewTabId.make("tab-1"),
        })
        .pipe(Effect.flip);

      expect(error).toBeInstanceOf(PreviewAutomationTargetNotEditableError);
      expect(error).toMatchObject({
        operation: "type",
        tabId: "tab-1",
        selectorKind: "focused-element",
        remoteTag: "PreviewAutomationTargetNotEditableError",
      });
      expect(error.message).toBe("Preview automation type requires an editable focused element.");
    }),
  );
});

it.effect.each([
  "PreviewAutomationRecordingTransferError",
  "PreviewAutomationRecordingDesktopUpdateRequiredError",
  "PreviewAutomationRecordingTooLargeError",
  "PreviewAutomationRecordingDeadlineExpiredError",
] as const)("preserves recording failure %s", (tag) =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const remoteError = {
        _tag: tag,
        message: "remote recording details",
        detail: { reason: "untrusted-reason", threadId: "untrusted-thread" },
      };
      const requests = requestsFrom(yield* broker.connect(makeHost()));
      yield* Stream.runForEach(requests, (request) =>
        broker.respond({
          clientId: "client-1",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: false,
          error: remoteError,
        }),
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      const error = yield* broker
        .invoke<void>({
          scope,
          operation: "recordingStop",
          input: {},
        })
        .pipe(Effect.flip);
      expect(error).toMatchObject({
        _tag: tag,
        threadId: scope.thread.threadId,
      });
      expect(error.cause).toBe(remoteError);
      expect(error.message).toContain("remains on the desktop");
      expect(error.message).not.toContain("remote recording details");
    }),
  ),
);

it.effect.each([
  { clientId: SERVER_BROWSER_AUTOMATION_CLIENT_ID, shown: true },
  { clientId: "client-1", shown: false },
])("tells the agent why its own server browser failed ($clientId)", ({ clientId, shown }) =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const requests = requestsFrom(yield* broker.connect(makeHost({ clientId })));
      yield* Stream.runForEach(requests, (request) =>
        broker.respond({
          clientId,
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: false,
          error: {
            _tag: "PreviewAutomationExecutionError",
            message: "page.goto: net::ERR_CONNECTION_REFUSED at http://localhost:4719/",
          },
        }),
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      const error = yield* broker
        .invoke<void>({ scope, operation: "open", input: {} })
        .pipe(Effect.flip);
      expect(error._tag).toBe("PreviewAutomationExecutionError");
      // A desktop or other remote host's text stays out of the agent's context.
      expect(error.message.includes("ERR_CONNECTION_REFUSED")).toBe(shown);
    }),
  ),
);

it.effect("distinguishes malformed remote failures", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const requests = requestsFrom(yield* broker.connect(makeHost()));
      yield* Stream.runForEach(requests, (request) =>
        broker.respond({
          clientId: "client-1",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: false,
        }),
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      const error = yield* broker
        .invoke<void>({ scope, operation: "status", input: {}, timeoutMs: 2_000 })
        .pipe(Effect.flip);

      expect(error).toBeInstanceOf(PreviewAutomationMalformedResponseError);
      expect(error).toMatchObject({
        operation: "status",
        environmentId: scope.environmentId,
        threadId: scope.thread.threadId,
        providerSessionId: scope.thread.providerSessionId,
        providerInstanceId: scope.thread.providerInstanceId,
        clientId: "client-1",
        requestId: "preview-0",
        timeoutMs: 2_000,
      });
    }),
  ),
);

it.effect("rejects calls when no connected host exists", () =>
  Effect.gen(function* () {
    const broker = yield* makeBroker;
    const error = yield* broker
      .invoke<void>({ scope, operation: "status", input: {} })
      .pipe(Effect.flip);

    expect(error).toBeInstanceOf(PreviewAutomationNoAvailableHostError);
    expect(error).toMatchObject({
      operation: "status",
      environmentId: scope.environmentId,
      threadId: scope.thread.threadId,
      providerSessionId: scope.thread.providerSessionId,
      providerInstanceId: scope.thread.providerInstanceId,
    });
  }),
);

it.effect("does not create host state from focus updates without a live stream", () =>
  Effect.gen(function* () {
    const broker = yield* makeBroker;
    yield* broker.focusHost({
      clientId: "client-1",
      environmentId: scope.environmentId,
      connectionId: "connection-missing",
      focused: true,
    });

    const error = yield* broker
      .invoke<void>({ scope, operation: "status", input: {} })
      .pipe(Effect.flip);
    expect(error).toBeInstanceOf(PreviewAutomationNoAvailableHostError);
  }),
);

it.effect("removes host availability when the authoritative request stream disconnects", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const requests = requestsFrom(yield* broker.connect(makeHost()));
      const beforeAcquisition = yield* broker
        .invoke<void>({ scope, operation: "status", input: {} })
        .pipe(Effect.flip);
      expect(beforeAcquisition).toBeInstanceOf(PreviewAutomationNoAvailableHostError);

      const consumer = yield* Stream.runDrain(requests).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      yield* Fiber.interrupt(consumer);

      const error = yield* broker
        .invoke<void>({ scope, operation: "status", input: {} })
        .pipe(Effect.flip);
      expect(error).toBeInstanceOf(PreviewAutomationNoAvailableHostError);
    }),
  ),
);

it.effect("routes requests for background threads through an environment-level host", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const backgroundThreadId = ThreadId.make("thread-background");
      const requests = requestsFrom(yield* broker.connect(makeHost()));
      let routedThreadId: string | undefined;
      yield* Stream.runForEach(requests, (request) => {
        routedThreadId = request.threadId;
        return broker.respond({
          clientId: "client-1",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: true,
          result: "background",
        });
      }).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      const result = yield* broker.invoke<string>({
        scope: {
          ...scope,
          thread: {
            ...scope.thread,
            threadId: backgroundThreadId,
            providerSessionId: "provider-session-background",
          },
        },
        operation: "status",
        input: {},
      });

      expect(result).toBe("background");
      expect(routedThreadId).toBe(backgroundThreadId);
    }),
  ),
);

it.effect("never routes a provider session to a host from another environment", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const matchingRequests = requestsFrom(
        yield* broker.connect(makeHost({ clientId: "client-matching" })),
      );
      const foreignRequests = requestsFrom(
        yield* broker.connect(
          makeHost({
            clientId: "client-foreign",
            environmentId: EnvironmentId.make("environment-foreign"),
          }),
        ),
      );
      yield* Stream.runForEach(matchingRequests, (request) =>
        broker.respond({
          clientId: "client-matching",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: true,
          result: "matching",
        }),
      ).pipe(Effect.forkScoped);
      yield* Stream.runForEach(foreignRequests, (request) =>
        broker.respond({
          clientId: "client-foreign",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: true,
          result: "foreign",
        }),
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      expect(yield* broker.invoke<string>({ scope, operation: "status", input: {} })).toBe(
        "matching",
      );
    }),
  ),
);

it.effect("pins a provider session to its initial host despite later focus changes", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      let firstConnectionId = "";
      let secondConnectionId = "";
      const firstRequests = requestsFrom(
        yield* broker.connect(makeHost({ clientId: "client-first" })),
        (connectionId) => {
          firstConnectionId = connectionId;
        },
      );
      const secondRequests = requestsFrom(
        yield* broker.connect(makeHost({ clientId: "client-second" })),
        (connectionId) => {
          secondConnectionId = connectionId;
        },
      );
      yield* Stream.runForEach(firstRequests, (request) =>
        broker.respond({
          clientId: "client-first",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: true,
          result: "first",
        }),
      ).pipe(Effect.forkScoped);
      yield* Stream.runForEach(secondRequests, (request) =>
        broker.respond({
          clientId: "client-second",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: true,
          result: "second",
        }),
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      yield* broker.focusHost({
        clientId: "client-first",
        environmentId: scope.environmentId,
        connectionId: "connection-stale",
        focused: true,
        liveTabs: [{ threadId: scope.thread.threadId, tabId: PreviewTabId.make("stale-tab") }],
      });
      expect(yield* broker.invoke<string>({ scope, operation: "status", input: {} })).toBe(
        "second",
      );
      yield* broker.focusHost({
        clientId: "client-first",
        environmentId: scope.environmentId,
        connectionId: firstConnectionId,
        focused: true,
      });

      const firstPinnedScope = {
        ...scope,
        thread: { ...scope.thread, providerSessionId: "provider-session-first-pinned" },
      };
      expect(
        yield* broker.invoke<string>({ scope: firstPinnedScope, operation: "status", input: {} }),
      ).toBe("first");

      yield* broker.focusHost({
        clientId: "client-second",
        environmentId: scope.environmentId,
        connectionId: secondConnectionId,
        focused: true,
      });

      expect(
        yield* broker.invoke<string>({ scope: firstPinnedScope, operation: "status", input: {} }),
      ).toBe("first");
      expect(
        yield* broker.invoke<string>({
          scope: {
            ...scope,
            thread: { ...scope.thread, providerSessionId: "provider-session-second-pinned" },
          },
          operation: "status",
          input: {},
        }),
      ).toBe("second");
    }),
  ),
);

it.effect("prefers the live tab owner for new sessions without moving existing leases", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const connections = new Map<string, string>();
      for (const clientId of ["owner", "other"]) {
        const requests = requestsFrom(
          yield* broker.connect(makeHost({ clientId })),
          (connectionId) => connections.set(clientId, connectionId),
        );
        yield* Stream.runForEach(requests, (request) =>
          broker.respond({
            clientId,
            connectionId: request.connectionId,
            requestId: request.requestId,
            ok: true,
            result: clientId,
          }),
        ).pipe(Effect.forkScoped);
      }
      yield* Effect.yieldNow;
      yield* broker.focusHost({
        clientId: "owner",
        environmentId: scope.environmentId,
        connectionId: connections.get("owner")!,
        focused: false,
        liveTabs: [
          { threadId: scope.thread.threadId, tabId: PreviewTabId.make("signed-in"), visible: true },
        ],
      });
      yield* broker.focusHost({
        clientId: "other",
        environmentId: scope.environmentId,
        connectionId: connections.get("other")!,
        focused: true,
        liveTabs: [
          {
            threadId: scope.thread.threadId,
            tabId: PreviewTabId.make("signed-in"),
            visible: false,
          },
          {
            threadId: ThreadId.make("another-thread"),
            tabId: PreviewTabId.make("different-tab"),
            visible: true,
          },
        ],
      });
      expect(yield* broker.invoke<string>({ scope, operation: "evaluate", input: {} })).toBe(
        "owner",
      );
      expect(
        yield* broker.invoke<string>({
          scope: { ...scope, thread: { ...scope.thread, providerSessionId: "explicit-owner" } },
          tabId: PreviewTabId.make("signed-in"),
          operation: "snapshot",
          input: {},
        }),
      ).toBe("owner");
      expect(
        yield* broker.invoke<string>({
          scope: { ...scope, thread: { ...scope.thread, providerSessionId: "other-tab" } },
          tabId: PreviewTabId.make("different-tab"),
          operation: "evaluate",
          input: {},
        }),
      ).toBe("other");

      yield* broker.focusHost({
        clientId: "owner",
        environmentId: scope.environmentId,
        connectionId: connections.get("owner")!,
        focused: false,
        liveTabs: [],
      });
      expect(yield* broker.invoke<string>({ scope, operation: "evaluate", input: {} })).toBe(
        "owner",
      );
      expect(
        yield* broker.invoke<string>({
          scope: { ...scope, thread: { ...scope.thread, providerSessionId: "after-tab-closed" } },
          operation: "evaluate",
          input: {},
        }),
      ).toBe("other");
    }),
  ),
);

it.effect("prefers a focused host over unrelated extra capabilities for a new session", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      let focusedConnectionId = "";
      for (const [clientId, supportedOperations] of [
        ["focused", ["status"]],
        ["background", ["status", "resize"]],
      ] as const) {
        const requests = requestsFrom(
          yield* broker.connect(makeHost({ clientId, supportedOperations })),
          (connectionId) => {
            if (clientId === "focused") focusedConnectionId = connectionId;
          },
        );
        yield* Stream.runForEach(requests, (request) =>
          broker.respond({
            clientId,
            connectionId: request.connectionId,
            requestId: request.requestId,
            ok: true,
            result: clientId,
          }),
        ).pipe(Effect.forkScoped);
      }
      yield* Effect.yieldNow;
      yield* broker.focusHost({
        clientId: "focused",
        environmentId: scope.environmentId,
        connectionId: focusedConnectionId,
        focused: true,
      });
      expect(yield* broker.invoke<string>({ scope, operation: "status", input: {} })).toBe(
        "focused",
      );
    }),
  ),
);

it.effect("does not route new operations to legacy hosts that did not advertise support", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const legacyEvents = yield* broker.connect(makeHost());
      yield* Stream.runDrain(legacyEvents).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      const error = yield* broker
        .invoke<void>({ scope, operation: "resize", input: { mode: "fill" } })
        .pipe(Effect.flip);

      expect(error).toBeInstanceOf(PreviewAutomationNoAvailableHostError);
      expect(error).toMatchObject({ operation: "resize", environmentId: scope.environmentId });
    }),
  ),
);

it.effect("routes resize to a capable host instead of a newer legacy connection", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const capableRequests = requestsFrom(
        yield* broker.connect(
          makeHost({ clientId: "client-capable", supportedOperations: ["resize"] }),
        ),
      );
      const legacyRequests = requestsFrom(
        yield* broker.connect(makeHost({ clientId: "client-legacy" })),
      );
      yield* Stream.runForEach(capableRequests, (request) =>
        broker.respond({
          clientId: "client-capable",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: true,
          result: "capable",
        }),
      ).pipe(Effect.forkScoped);
      yield* Stream.runForEach(legacyRequests, (request) =>
        broker.respond({
          clientId: "client-legacy",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: true,
          result: "legacy",
        }),
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      expect(
        yield* broker.invoke<string>({ scope, operation: "resize", input: { mode: "fill" } }),
      ).toBe("capable");
    }),
  ),
);

it.effect("does not move a live legacy assignment to another runtime for resize", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const legacyRequests = requestsFrom(
        yield* broker.connect(makeHost({ clientId: "client-legacy" })),
      );
      yield* Stream.runForEach(legacyRequests, (request) =>
        broker.respond({
          clientId: "client-legacy",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: true,
          result: "legacy",
        }),
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      expect(yield* broker.invoke<string>({ scope, operation: "status", input: {} })).toBe(
        "legacy",
      );

      const capableRequests = requestsFrom(
        yield* broker.connect(
          makeHost({ clientId: "client-capable", supportedOperations: ["resize"] }),
        ),
      );
      yield* Stream.runForEach(capableRequests, (request) =>
        broker.respond({
          clientId: "client-capable",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: true,
          result: "capable",
        }),
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      const error = yield* broker
        .invoke<void>({ scope, operation: "resize", input: { mode: "fill" } })
        .pipe(Effect.flip);
      expect(error).toBeInstanceOf(PreviewAutomationNoAvailableHostError);
      expect(yield* broker.invoke<string>({ scope, operation: "status", input: {} })).toBe(
        "legacy",
      );
    }),
  ),
);

it.effect("ignores stale focus updates for a different environment", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      let firstConnectionId = "";
      const firstRequests = requestsFrom(
        yield* broker.connect(makeHost({ clientId: "client-first" })),
        (connectionId) => {
          firstConnectionId = connectionId;
        },
      );
      const secondRequests = requestsFrom(
        yield* broker.connect(makeHost({ clientId: "client-second" })),
      );
      yield* Stream.runForEach(firstRequests, (request) =>
        broker.respond({
          clientId: "client-first",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: true,
          result: "first",
        }),
      ).pipe(Effect.forkScoped);
      yield* Stream.runForEach(secondRequests, (request) =>
        broker.respond({
          clientId: "client-second",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: true,
          result: "second",
        }),
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      yield* broker.focusHost({
        clientId: "client-first",
        environmentId: EnvironmentId.make("environment-stale"),
        connectionId: firstConnectionId,
        focused: true,
      });

      expect(yield* broker.invoke<string>({ scope, operation: "status", input: {} })).toBe(
        "second",
      );
    }),
  ),
);

it.effect("retains a disconnected provider session affinity until its hold expires", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const firstTabId = PreviewTabId.make("tab-on-first-host");
      let firstConnectionId = "";
      let secondRoutedTabId: PreviewTabId | undefined;
      const firstRequests = requestsFrom(
        yield* broker.connect(makeHost({ clientId: "client-first" })),
        (connectionId) => {
          firstConnectionId = connectionId;
        },
      );
      const secondRequests = requestsFrom(
        yield* broker.connect(makeHost({ clientId: "client-second" })),
      );
      const firstConsumer = yield* Stream.runForEach(firstRequests, (request) =>
        broker.respond({
          clientId: "client-first",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: true,
          result: request.operation === "open" ? { host: "first", tabId: firstTabId } : "first",
        }),
      ).pipe(Effect.forkScoped);
      yield* Stream.runForEach(secondRequests, (request) => {
        secondRoutedTabId = request.tabId;
        return broker.respond({
          clientId: "client-second",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: true,
          result: "second",
        });
      }).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      yield* broker.focusHost({
        clientId: "client-first",
        environmentId: scope.environmentId,
        connectionId: firstConnectionId,
        focused: true,
        liveTabs: [{ threadId: scope.thread.threadId, tabId: firstTabId }],
      });
      expect(yield* broker.invoke({ scope, operation: "open", input: {} })).toEqual({
        host: "first",
        tabId: firstTabId,
      });

      yield* Fiber.interrupt(firstConsumer);
      yield* Effect.yieldNow;

      expect(
        yield* broker
          .invoke({ scope, operation: "status", input: {} })
          .pipe(Effect.flip, Effect.orDie),
      ).toMatchObject({ reconnecting: true });
      yield* TestClock.adjust(30_000);
      expect(yield* broker.invoke<string>({ scope, operation: "status", input: {} })).toBe(
        "second",
      );
      expect(secondRoutedTabId).toBeUndefined();
    }),
  ),
);

it.effect("lets the browser host resolve an active tab locally", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const requests = requestsFrom(yield* broker.connect(makeHost()));
      let routedTabId: string | undefined;
      yield* Stream.runForEach(requests, (request) => {
        routedTabId = request.tabId;
        return broker.respond({
          clientId: "client-1",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: true,
        });
      }).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      yield* broker.invoke<void>({ scope, operation: "click", input: { x: 10, y: 10 } });

      expect(routedTabId).toBeUndefined();
    }),
  ),
);

it.effect("keeps a replacement stream authoritative when the old stream finalizes", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      let firstConnectionId = "";
      let replacementConnectionId = "";
      const firstRequests = requestsFrom(yield* broker.connect(makeHost()), (connectionId) => {
        firstConnectionId = connectionId;
      });
      yield* Stream.runDrain(firstRequests).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      const replacementRequests = requestsFrom(
        yield* broker.connect(makeHost()),
        (connectionId) => {
          replacementConnectionId = connectionId;
        },
      );
      yield* Stream.runForEach(replacementRequests, (request) =>
        broker.respond({
          clientId: "client-1",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: true,
          result: "replacement",
        }),
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      expect(replacementConnectionId).not.toBe(firstConnectionId);
      const result = yield* broker.invoke<string>({ scope, operation: "status", input: {} });
      expect(result).toBe("replacement");
    }),
  ),
);

it.effect("does not carry a tab id across a replacement automation stream", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const openedTabId = PreviewTabId.make("tab-first-webcontents");
      const firstRequests = requestsFrom(
        yield* broker.connect(makeHost({ runtimeIdentity: testRuntimeIdentity })),
      );
      yield* Stream.runForEach(firstRequests, (request) =>
        broker.respond({
          clientId: "client-1",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: true,
          result:
            request.operation === "open"
              ? { host: "first", tabId: openedTabId }
              : { host: "first" },
        }),
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      expect(yield* broker.invoke({ scope, operation: "open", input: {} })).toEqual({
        host: "first",
        tabId: openedTabId,
      });

      const routedRequests: RoutedRequest[] = [];
      const replacementRequests = requestsFrom(
        yield* broker.connect(makeHost({ runtimeIdentity: testRuntimeIdentity })),
      );
      yield* Stream.runForEach(replacementRequests, (request) => {
        routedRequests.push(request);
        return broker.respond({
          clientId: "client-1",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: true,
          result: "replacement",
        });
      }).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      expect(yield* broker.invoke<string>({ scope, operation: "status", input: {} })).toBe(
        "replacement",
      );
      expect(routedRequests.at(-1)?.tabId).toBeUndefined();
    }),
  ),
);

it.effect("fails requests assigned to the stream that is replaced", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const requests = requestsFrom(yield* broker.connect(makeHost()));
      yield* Stream.runDrain(requests).pipe(Effect.forkScoped);
      const pending = yield* broker
        .invoke<void>({ scope, operation: "status", input: {} })
        .pipe(Effect.flip, Effect.forkScoped);
      yield* Effect.yieldNow;

      const replacementRequests = requestsFrom(yield* broker.connect(makeHost()));
      yield* Stream.runDrain(replacementRequests).pipe(Effect.forkScoped);

      const error = yield* Fiber.join(pending);
      expect(error).toBeInstanceOf(PreviewAutomationClientDisconnectedError);
      expect(error).toMatchObject({
        operation: "status",
        environmentId: scope.environmentId,
        threadId: scope.thread.threadId,
        providerSessionId: scope.thread.providerSessionId,
        providerInstanceId: scope.thread.providerInstanceId,
        clientId: "client-1",
        requestId: "preview-0",
        timeoutMs: 15_000,
      });
    }),
  ),
);

it.effect("accepts responses only from the host that received the request", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const requests = requestsFrom(yield* broker.connect(makeHost()));
      yield* Stream.runForEach(requests, (request) =>
        Effect.gen(function* () {
          yield* broker.respond({
            clientId: "client-foreign",
            connectionId: request.connectionId,
            requestId: request.requestId,
            ok: true,
            result: "foreign",
          });
          yield* broker.respond({
            clientId: "client-1",
            connectionId: "connection-stale",
            requestId: request.requestId,
            ok: true,
            result: "stale",
          });
          yield* broker.respond({
            clientId: "client-1",
            connectionId: request.connectionId,
            requestId: request.requestId,
            ok: true,
            result: "owner",
          });
        }),
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      const result = yield* broker.invoke<string>({ scope, operation: "status", input: {} });
      expect(result).toBe("owner");
    }),
  ),
);

it.effect("evicts an unanswered host and lets later calls use a healthy runtime", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const connected = yield* Deferred.make<string>();
      const received = yield* Deferred.make<RoutedRequest>();
      const otherReceived = yield* Deferred.make<void>();
      const otherCompleted = yield* Deferred.make<void>();
      const oldTab = PreviewTabId.make("tab-on-frozen-host");
      const events = yield* broker.connect(makeHost());
      const consumer = yield* Stream.runForEach(events, (event) => {
        if (event.type === "connected") return Deferred.succeed(connected, event.connectionId);
        const request = { ...event.request, connectionId: event.connectionId };
        if (request.operation === "open") {
          return broker.respond({
            clientId: "client-1",
            connectionId: event.connectionId,
            requestId: request.requestId,
            ok: true,
            result: { tabId: oldTab },
          });
        }
        return request.operation === "snapshot"
          ? Deferred.succeed(received, request)
          : Deferred.succeed(otherReceived, undefined);
      }).pipe(Effect.forkScoped);
      const connectionId = yield* Deferred.await(connected);
      yield* broker.invoke({ scope, operation: "open", input: {} });

      const healthyConnected = yield* Deferred.make<void>();
      const healthyRequests: RoutedRequest[] = [];
      const healthy = yield* broker.connect(makeHost({ clientId: "healthy" }));
      yield* Stream.runForEach(healthy, (event) => {
        if (event.type === "connected") return Deferred.succeed(healthyConnected, undefined);
        healthyRequests.push({ ...event.request, connectionId: event.connectionId });
        return broker.respond({
          clientId: "healthy",
          connectionId: event.connectionId,
          requestId: event.request.requestId,
          ok: true,
          result: "healthy",
        });
      }).pipe(Effect.forkScoped);
      yield* Deferred.await(healthyConnected);

      const timedOut = yield* broker
        .invoke<void>({
          scope,
          operation: "snapshot",
          input: {},
          timeoutMs: 1_000,
        })
        .pipe(Effect.flip, Effect.forkScoped);
      const lateRequest = yield* Deferred.await(received);
      const other = yield* broker
        .invoke<void>({
          scope,
          operation: "evaluate",
          input: {},
          timeoutMs: 10_000,
        })
        .pipe(
          Effect.flip,
          Effect.tap(() => Deferred.succeed(otherCompleted, undefined)),
          Effect.forkScoped,
        );
      yield* Deferred.await(otherReceived);
      yield* TestClock.adjust(1_000);
      expect(yield* Fiber.join(timedOut)).toMatchObject({ _tag: "PreviewAutomationTimeoutError" });
      yield* Deferred.await(otherCompleted);
      expect(yield* Deferred.isDone(otherCompleted)).toBe(true);
      expect(yield* Fiber.join(other)).toMatchObject({
        _tag: "PreviewAutomationClientDisconnectedError",
      });
      const consumerExit = yield* Fiber.await(consumer);
      expect(Exit.isSuccess(consumerExit)).toBe(true);

      // Late traffic from the evicted connection cannot restore its assignment.
      yield* broker.respond({
        clientId: "client-1",
        connectionId,
        requestId: lateRequest.requestId,
        ok: true,
        result: { tabId: oldTab },
      });
      yield* broker.focusHost({
        clientId: "client-1",
        connectionId,
        environmentId: scope.environmentId,
        focused: true,
      });
      expect(
        yield* broker
          .invoke({ scope, operation: "status", input: {} })
          .pipe(Effect.flip, Effect.orDie),
      ).toMatchObject({ reconnecting: true });
      yield* TestClock.adjust(30_000);
      expect(yield* broker.invoke({ scope, operation: "status", input: {} })).toBe("healthy");
      expect(healthyRequests).toHaveLength(1);
      expect(healthyRequests[0]?.tabId).toBeUndefined();
    }),
  ),
);

it.effect("discards buffered actions before completing an evicted host stream", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const connected = yield* Deferred.make<void>();
      const received = yield* Deferred.make<void>();
      const actionRouted = yield* Deferred.make<void>();
      const releaseConsumer = yield* Deferred.make<void>();
      const operations: string[] = [];
      const consumer = yield* Stream.runForEach(yield* broker.connect(makeHost()), (event) => {
        if (event.type === "connected") return Deferred.succeed(connected, undefined);
        operations.push(event.request.operation);
        return Deferred.succeed(received, undefined).pipe(
          Effect.andThen(Deferred.await(releaseConsumer)),
        );
      }).pipe(Effect.forkScoped);
      yield* Deferred.await(connected);
      const timedOut = yield* broker
        .invoke<void>({ scope, operation: "snapshot", input: {}, timeoutMs: 1_000 })
        .pipe(Effect.flip, Effect.forkScoped);
      yield* Deferred.await(received);
      const buffered = yield* broker
        .invoke<void>({
          scope,
          operation: "click",
          input: {},
          timeoutMs: 10_000,
          onTargetTab: () => {
            Deferred.doneUnsafe(actionRouted, Effect.void);
          },
        })
        .pipe(Effect.flip, Effect.forkScoped);
      yield* Deferred.await(actionRouted);
      yield* TestClock.adjust(1_000);
      expect(yield* Fiber.join(timedOut)).toMatchObject({ _tag: "PreviewAutomationTimeoutError" });
      expect(yield* Fiber.join(buffered)).toMatchObject({
        _tag: "PreviewAutomationClientDisconnectedError",
      });
      yield* Deferred.succeed(releaseConsumer, undefined);
      expect(Exit.isSuccess(yield* Fiber.await(consumer))).toBe(true);
      expect(operations).toEqual(["snapshot"]);
    }),
  ),
);

it.effect("rejects a routed action when its generation is evicted before delivery", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const connected = yield* Deferred.make<void>();
      const received = yield* Deferred.make<void>();
      const actionRouted = yield* Deferred.make<void>();
      const operations: string[] = [];
      const consumer = yield* Stream.runForEach(yield* broker.connect(makeHost()), (event) => {
        if (event.type === "connected") return Deferred.succeed(connected, undefined);
        operations.push(event.request.operation);
        return Deferred.succeed(received, undefined);
      }).pipe(Effect.forkScoped);
      yield* Deferred.await(connected);
      const timedOut = yield* broker
        .invoke<void>({ scope, operation: "snapshot", input: {}, timeoutMs: 1_000 })
        .pipe(Effect.flip, Effect.forkScoped);
      yield* Deferred.await(received);

      // Suspend only this invocation in the gap between route selection and delivery.
      const tasks: Array<() => void> = [];
      let paused = false;
      const dispatcher: Scheduler.SchedulerDispatcher = {
        scheduleTask: (task) => tasks.push(task),
        flush: () => {
          let task: (() => void) | undefined;
          while ((task = tasks.shift()) !== undefined) task();
        },
      };
      const scheduler: Scheduler.Scheduler = {
        executionMode: "async",
        makeDispatcher: () => dispatcher,
        shouldYield: () => paused,
      };
      const action = yield* broker
        .invoke<void>({
          scope,
          operation: "click",
          input: {},
          onTargetTab: () => {
            paused = true;
            Deferred.doneUnsafe(actionRouted, Effect.void);
          },
        })
        .pipe(
          Effect.flip,
          Effect.provideService(Scheduler.Scheduler, scheduler),
          Effect.forkScoped,
        );
      yield* Deferred.await(actionRouted);
      yield* TestClock.adjust(1_000);
      expect(yield* Fiber.join(timedOut)).toMatchObject({ _tag: "PreviewAutomationTimeoutError" });
      expect(Exit.isSuccess(yield* Fiber.await(consumer))).toBe(true);

      paused = false;
      dispatcher.flush();
      expect(yield* Fiber.join(action)).toMatchObject({
        _tag: "PreviewAutomationClientDisconnectedError",
      });
      expect(operations).toEqual(["snapshot"]);
    }),
  ),
);

it.effect("keeps a host that responds with an operation timeout", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const connected = yield* Deferred.make<void>();
      const events = yield* broker.connect(makeHost());
      yield* Stream.runForEach(events, (event) => {
        if (event.type === "connected") return Deferred.succeed(connected, undefined);
        return broker.respond({
          clientId: "client-1",
          connectionId: event.connectionId,
          requestId: event.request.requestId,
          ...(event.request.operation === "waitFor"
            ? {
                ok: false,
                error: { _tag: "PreviewAutomationTimeoutError", message: "Selector timed out" },
              }
            : { ok: true, result: "responsive" }),
        });
      }).pipe(Effect.forkScoped);
      yield* Deferred.await(connected);
      expect(
        yield* broker.invoke<void>({ scope, operation: "waitFor", input: {} }).pipe(Effect.flip),
      ).toMatchObject({ _tag: "PreviewAutomationTimeoutError" });
      expect(yield* broker.invoke({ scope, operation: "status", input: {} })).toBe("responsive");
    }),
  ),
);

it.effect("keeps the host connected when a background status read times out", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const requests = requestsFrom(yield* broker.connect(makeHost()));
      // A busy host answers its actions but not the metadata read behind them.
      yield* Stream.runForEach(requests, (request) =>
        request.operation === "status"
          ? Effect.void
          : broker.respond({
              clientId: "client-1",
              connectionId: request.connectionId,
              requestId: request.requestId,
              ok: true,
              result: { operation: request.operation },
            }),
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      const status = yield* broker
        .invoke<void>({
          scope,
          operation: "status",
          input: {},
          timeoutMs: 500,
          updateCurrentTab: false,
          failurePolicy: "request_only",
        })
        .pipe(Effect.flip, Effect.forkScoped);
      yield* TestClock.adjust(500);
      expect(yield* Fiber.join(status)).toMatchObject({ _tag: "PreviewAutomationTimeoutError" });

      expect(yield* broker.invoke({ scope, operation: "snapshot", input: {} })).toEqual({
        operation: "snapshot",
      });
    }),
  ),
);

it.effect("authors status receipts only for successful object results", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      let responseResult: unknown = null;
      const requests = requestsFrom(yield* broker.connect(makeHost()));
      yield* Stream.runForEach(requests, (request) =>
        broker.respond({
          clientId: "client-1",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: true,
          result: responseResult,
        }),
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      for (const result of [null, [], "legacy-status", 42]) {
        responseResult = result;
        expect(yield* broker.invoke({ scope, operation: "status", input: {} })).toEqual(result);
      }
      for (const operation of ["open", "navigate"] as const) {
        responseResult = {
          available: true,
          selectedClient: { clientId: "client-authored-action" },
        };
        expect(yield* broker.invoke({ scope, operation, input: {} })).toEqual(responseResult);
      }
    }),
  ),
);

it.effect("never transfers a pending runtime receipt to a replacement connection", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const oldIdentity = {
        schemaVersion: 1,
        runtimeKind: "electron",
        runtimeInstanceId: "old-runtime",
        appVersion: "1.0.0",
        buildCommit: "a".repeat(40),
      } as const;
      const newIdentity = {
        ...oldIdentity,
        runtimeInstanceId: "new-runtime",
        buildCommit: "b".repeat(40),
      };
      const oldRequestReady = yield* Deferred.make<RoutedRequest>();
      yield* Stream.runForEach(
        requestsFrom(yield* broker.connect(makeHost({ runtimeIdentity: oldIdentity }))),
        (request) => Deferred.succeed(oldRequestReady, request),
      ).pipe(Effect.forkScoped);
      const pending = yield* broker
        .invoke<{
          selectedClient: { runtimeIdentity: typeof oldIdentity };
        }>({ scope, operation: "status", input: {} })
        .pipe(Effect.flip, Effect.forkScoped);
      const oldRequest = yield* Deferred.await(oldRequestReady);
      const newRequestReady = yield* Deferred.make<RoutedRequest>();
      yield* Stream.runForEach(
        requestsFrom(yield* broker.connect(makeHost({ runtimeIdentity: newIdentity }))),
        (request) => Deferred.succeed(newRequestReady, request),
      ).pipe(Effect.forkScoped);
      expect(yield* Fiber.join(pending)).toBeInstanceOf(PreviewAutomationClientDisconnectedError);
      expect(
        yield* broker
          .invoke({ scope, operation: "status", input: {} })
          .pipe(Effect.flip, Effect.orDie),
      ).toMatchObject({ reconnecting: true });
      yield* TestClock.adjust(30_000);
      const current = yield* broker
        .invoke<{
          selectedClient: {
            clientId: string;
            connectionId: string;
            requestId: string;
            runtimeIdentity: typeof newIdentity;
          };
        }>({ scope, operation: "status", input: {} })
        .pipe(Effect.forkScoped);
      const request = yield* Deferred.await(newRequestReady);
      expect(request.connectionId).not.toBe(oldRequest.connectionId);
      for (const mismatch of [
        { clientId: "foreign", connectionId: request.connectionId, requestId: request.requestId },
        {
          clientId: "client-1",
          connectionId: oldRequest.connectionId,
          requestId: request.requestId,
        },
        {
          clientId: "client-1",
          connectionId: request.connectionId,
          requestId: oldRequest.requestId,
        },
      ]) {
        yield* broker.respond({
          ...mismatch,
          ok: true,
          result: { selectedClient: { runtimeIdentity: oldIdentity } },
        });
      }
      yield* broker.respond({
        clientId: "client-1",
        connectionId: request.connectionId,
        requestId: request.requestId,
        ok: true,
        result: { selectedClient: { clientId: "forged", runtimeIdentity: oldIdentity } },
      });
      const result = yield* Fiber.join(current);
      expect(result.selectedClient).toMatchObject({
        clientId: "client-1",
        connectionId: request.connectionId,
        requestId: request.requestId,
        runtimeIdentity: newIdentity,
      });
    }),
  ),
);

it.effect("expires one request without evicting a generation with a newer valid reply", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const received = yield* Deferred.make<void>();
      let requests = 0;
      yield* Stream.runForEach(requestsFrom(yield* broker.connect(makeHost())), (request) => {
        requests++;
        return request.operation === "click"
          ? Deferred.succeed(received, undefined)
          : broker.respond({
              clientId: "client-1",
              connectionId: request.connectionId,
              requestId: request.requestId,
              ok: true,
              result: "alive",
            });
      }).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      const expired = yield* broker
        .invoke({
          scope,
          operation: "click",
          input: {},
          tabId: PreviewTabId.make("tab-a"),
          timeoutMs: 1000,
        })
        .pipe(Effect.flip, Effect.orDie, Effect.forkScoped);
      yield* Deferred.await(received);
      expect(yield* broker.invoke({ scope, operation: "status", input: {} })).toBe("alive");
      yield* TestClock.adjust(1000);
      const error = yield* Fiber.join(expired);
      expect(yield* broker.invoke({ scope, operation: "status", input: {} })).toBe("alive");
      expect(error).toMatchObject({ _tag: "PreviewAutomationTimeoutError", outcome: "unknown" });
      expect(requests).toBe(3);
    }),
  ),
);

it.effect("decorative status expiry never starts a health check or changes routing", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const received = yield* Deferred.make<void>();
      const operations: string[] = [];
      yield* Stream.runForEach(
        requestsFrom(yield* broker.connect(makeHost({ supportsPing: true }))),
        (request) => {
          operations.push(request.operation);
          return request.operation === "status"
            ? Deferred.succeed(received, undefined)
            : broker.respond({
                clientId: "client-1",
                connectionId: request.connectionId,
                requestId: request.requestId,
                ok: true,
                result: { tabId: PreviewTabId.make("tab-a") },
              });
        },
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      yield* broker.invoke({ scope, operation: "click", input: {} });
      const expired = yield* broker
        .invoke({
          scope,
          operation: "status",
          input: {},
          timeoutMs: 500,
          failurePolicy: "request_only",
          updateCurrentTab: false,
        })
        .pipe(Effect.flip, Effect.orDie, Effect.forkScoped);
      yield* Deferred.await(received);
      yield* TestClock.adjust(500);
      expect(yield* Fiber.join(expired)).toMatchObject({ outcome: "unknown" });
      yield* broker.invoke({ scope, operation: "click", input: {} });
      expect(operations).toEqual(["click", "status", "click"]);
    }),
  ),
);

it.effect("coalesces simultaneous expiries into one unanswered ping before eviction", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const routed = yield* Deferred.make<void>();
      let actions = 0;
      let pings = 0;
      yield* Stream.runForEach(
        requestsFrom(yield* broker.connect(makeHost({ supportsPing: true }))),
        (request) => {
          if (request.operation === "ping") pings++;
          else if (++actions === 2) return Deferred.succeed(routed, undefined);
          return Effect.void;
        },
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      const a = yield* broker
        .invoke({ scope, operation: "status", input: {}, timeoutMs: 100 })
        .pipe(Effect.flip, Effect.orDie, Effect.forkScoped);
      const b = yield* broker
        .invoke({ scope, operation: "snapshot", input: {}, timeoutMs: 100 })
        .pipe(Effect.flip, Effect.orDie, Effect.forkScoped);
      yield* Deferred.await(routed);
      yield* TestClock.adjust(100);
      expect(yield* Fiber.join(a)).toMatchObject({ outcome: "unknown" });
      expect(yield* Fiber.join(b)).toMatchObject({ outcome: "unknown" });
      expect(pings).toBe(1);
      yield* TestClock.adjust(1000);
      expect(
        yield* broker
          .invoke({ scope, operation: "status", input: {} })
          .pipe(Effect.flip, Effect.orDie),
      ).toMatchObject({
        _tag: "PreviewAutomationNoAvailableHostError",
        reconnecting: true,
      });
    }),
  ),
);

it.effect.each(["late reply", "snapshot"])(
  "reconciles a controlled mutation via %s without affecting another tab",
  (reconcile) =>
    Effect.scoped(
      Effect.gen(function* () {
        const broker = yield* makeBroker;
        const received = yield* Deferred.make<RoutedRequest>();
        const tabA = PreviewTabId.make("tab-a");
        const tabB = PreviewTabId.make("tab-b");
        let actions = 0;
        yield* Stream.runForEach(
          requestsFrom(yield* broker.connect(makeHost({ supportsSnapshotBarrier: true }))),
          (request) => {
            if (request.operation === "click" && actions++ === 0)
              return Deferred.succeed(received, request);
            return broker.respond({
              clientId: "client-1",
              connectionId: request.connectionId,
              requestId: request.requestId,
              ok: true,
              result: { tabId: request.tabId },
            });
          },
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        const a = yield* broker
          .invoke({ scope, operation: "click", tabId: tabA, input: {}, timeoutMs: 100 })
          .pipe(Effect.flip, Effect.orDie, Effect.forkScoped);
        const late = yield* Deferred.await(received);
        yield* broker.invoke({ scope, operation: "status", input: {} });
        yield* TestClock.adjust(100);
        yield* Fiber.join(a);
        expect(
          yield* broker
            .invoke({ scope, operation: "click", tabId: tabA, input: {} })
            .pipe(Effect.flip, Effect.orDie),
        ).toMatchObject({
          outcome: "not_started",
          unreconciled: true,
        });
        yield* broker.invoke({ scope, operation: "click", tabId: tabB, input: {} });
        if (reconcile === "snapshot") {
          yield* broker.invoke({ scope, operation: "snapshot", tabId: tabA, input: {} });
          yield* broker.respond({
            clientId: "client-1",
            connectionId: late.connectionId,
            requestId: late.requestId,
            ok: false,
            error: {
              _tag: "PreviewAutomationTimeoutError",
              message: "late unknown",
              outcome: "unknown",
            },
          });
        } else
          yield* broker.respond({
            clientId: "client-1",
            connectionId: late.connectionId,
            requestId: late.requestId,
            ok: true,
            result: {},
          });
        yield* broker.invoke({ scope, operation: "click", tabId: tabA, input: {} });
        expect(actions).toBe(3);
      }),
    ),
);

it.effect("snapshot cannot clear a mutation outside the controlled semaphore", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const received = yield* Deferred.make<void>();
      const tabId = PreviewTabId.make("tab-a");
      yield* Stream.runForEach(
        requestsFrom(yield* broker.connect(makeHost({ supportsSnapshotBarrier: true }))),
        (request) =>
          request.operation === "navigate"
            ? Deferred.succeed(received, undefined)
            : broker.respond({
                clientId: "client-1",
                connectionId: request.connectionId,
                requestId: request.requestId,
                ok: true,
                result: {},
              }),
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      const expired = yield* broker
        .invoke({ scope, operation: "navigate", tabId, input: {}, timeoutMs: 100 })
        .pipe(Effect.flip, Effect.orDie, Effect.forkScoped);
      yield* Deferred.await(received);
      yield* broker.invoke({ scope, operation: "status", input: {} });
      yield* TestClock.adjust(100);
      yield* Fiber.join(expired);
      yield* broker.invoke({ scope, operation: "snapshot", tabId, input: {} });
      expect(
        yield* broker
          .invoke({ scope, operation: "click", tabId, input: {} })
          .pipe(Effect.flip, Effect.orDie),
      ).toMatchObject({ outcome: "not_started" });
    }),
  ),
);

it.effect.each([true, false])(
  "restores only advertised remembered tabs on the identical runtime (%s)",
  (advertiseTab) =>
    Effect.scoped(
      Effect.gen(function* () {
        const broker = yield* makeBroker;
        const tabId = PreviewTabId.make("remembered-tab");
        const first = yield* Stream.runForEach(
          requestsFrom(yield* broker.connect(makeHost({ runtimeIdentity: testRuntimeIdentity }))),
          (request) =>
            broker.respond({
              clientId: "client-1",
              connectionId: request.connectionId,
              requestId: request.requestId,
              ok: true,
              result: { tabId },
            }),
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        yield* broker.invoke({ scope, operation: "open", input: {} });
        yield* Fiber.interrupt(first);
        yield* Effect.yieldNow;
        let foreignRequests = 0;
        yield* Stream.runForEach(
          requestsFrom(
            yield* broker.connect(
              makeHost({ clientId: "foreign", runtimeIdentity: testRuntimeIdentity }),
            ),
          ),
          () => {
            foreignRequests++;
            return Effect.void;
          },
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        expect(
          yield* broker
            .invoke({ scope, operation: "status", input: {} })
            .pipe(Effect.flip, Effect.orDie),
        ).toMatchObject({ reconnecting: true });
        expect(foreignRequests).toBe(0);
        let restoredTab: PreviewTabId | undefined;
        let connectionId = "";
        yield* Stream.runForEach(
          requestsFrom(
            yield* broker.connect(makeHost({ runtimeIdentity: testRuntimeIdentity })),
            (id) => {
              connectionId = id;
            },
          ),
          (request) => {
            restoredTab = request.tabId;
            return broker.respond({
              clientId: "client-1",
              connectionId: request.connectionId,
              requestId: request.requestId,
              ok: true,
              result: "restored",
            });
          },
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        yield* broker.focusHost({
          clientId: "client-1",
          connectionId,
          environmentId: scope.environmentId,
          focused: true,
          liveTabs: advertiseTab ? [{ threadId: scope.thread.threadId, tabId }] : [],
        });
        expect(yield* broker.invoke({ scope, operation: "status", input: {} })).toBe("restored");
        expect(restoredTab).toBe(advertiseTab ? tabId : undefined);
      }),
    ),
);

it.effect("a stale unanswered ping cannot evict a replacement generation", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const received = yield* Deferred.make<void>();
      const pingReady = yield* Deferred.make<void>();
      yield* Stream.runForEach(
        requestsFrom(
          yield* broker.connect(
            makeHost({ supportsPing: true, runtimeIdentity: testRuntimeIdentity }),
          ),
        ),
        (request) =>
          Deferred.succeed(request.operation === "ping" ? pingReady : received, undefined),
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      const expired = yield* broker
        .invoke({ scope, operation: "status", input: {}, timeoutMs: 100 })
        .pipe(Effect.flip, Effect.orDie, Effect.forkScoped);
      yield* Deferred.await(received);
      yield* TestClock.adjust(100);
      yield* Fiber.join(expired);
      yield* Deferred.await(pingReady);
      yield* Stream.runForEach(
        requestsFrom(
          yield* broker.connect(
            makeHost({ supportsPing: true, runtimeIdentity: testRuntimeIdentity }),
          ),
        ),
        (request) =>
          broker.respond({
            clientId: "client-1",
            connectionId: request.connectionId,
            requestId: request.requestId,
            ok: true,
            result: "replacement",
          }),
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      yield* TestClock.adjust(1000);
      expect(yield* broker.invoke({ scope, operation: "status", input: {} })).toBe("replacement");
    }),
  ),
);

it.effect(
  "keeps unrelated pending work and its lease when a request expires and ping succeeds",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const broker = yield* makeBroker;
        const aReady = yield* Deferred.make<void>();
        const bReady = yield* Deferred.make<RoutedRequest>();
        const tabId = PreviewTabId.make("lease-tab");
        yield* Stream.runForEach(
          requestsFrom(yield* broker.connect(makeHost({ supportsPing: true }))),
          (request) => {
            if (request.operation === "click") return Deferred.succeed(aReady, undefined);
            if (request.operation === "evaluate") return Deferred.succeed(bReady, request);
            return broker.respond({
              clientId: "client-1",
              connectionId: request.connectionId,
              requestId: request.requestId,
              ok: true,
              result: request.operation === "ping" ? { alive: true } : { tabId },
            });
          },
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        yield* broker.invoke({ scope, operation: "open", input: {} });
        const a = yield* broker
          .invoke({ scope, operation: "click", input: {}, timeoutMs: 100 })
          .pipe(Effect.flip, Effect.orDie, Effect.forkScoped);
        yield* Deferred.await(aReady);
        const b = yield* broker
          .invoke({ scope, operation: "evaluate", input: {}, timeoutMs: 5000 })
          .pipe(Effect.forkScoped);
        const bRequest = yield* Deferred.await(bReady);
        yield* TestClock.adjust(100);
        expect(yield* Fiber.join(a)).toMatchObject({ outcome: "unknown" });
        yield* broker.respond({
          clientId: "client-1",
          connectionId: bRequest.connectionId,
          requestId: bRequest.requestId,
          ok: true,
          result: "finished",
        });
        expect(yield* Fiber.join(b)).toBe("finished");
        expect(bRequest.tabId).toBe(tabId);
        expect(yield* broker.invoke({ scope, operation: "status", input: {} })).toMatchObject({
          tabId,
        });
      }),
    ),
);

it.effect("an exact late unknown reply retains quarantine until a settled reply arrives", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* makeBroker;
      const ready = yield* Deferred.make<RoutedRequest>();
      const tabId = PreviewTabId.make("tab-a");
      let clicks = 0;
      yield* Stream.runForEach(requestsFrom(yield* broker.connect(makeHost())), (request) => {
        if (request.operation === "click" && clicks++ === 0)
          return Deferred.succeed(ready, request);
        return broker.respond({
          clientId: "client-1",
          connectionId: request.connectionId,
          requestId: request.requestId,
          ok: true,
          result: {},
        });
      }).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      const a = yield* broker
        .invoke({ scope, operation: "click", tabId, input: {}, timeoutMs: 100 })
        .pipe(Effect.flip, Effect.orDie, Effect.forkScoped);
      const late = yield* Deferred.await(ready);
      yield* broker.invoke({ scope, operation: "status", input: {} });
      yield* TestClock.adjust(100);
      yield* Fiber.join(a);
      yield* broker.respond({
        clientId: "client-1",
        connectionId: late.connectionId,
        requestId: late.requestId,
        ok: false,
        error: { _tag: "PreviewAutomationTimeoutError", message: "unknown", outcome: "unknown" },
      });
      expect(
        yield* broker
          .invoke({ scope, operation: "click", tabId, input: {} })
          .pipe(Effect.flip, Effect.orDie),
      ).toMatchObject({ unreconciled: true });
      yield* broker.respond({
        clientId: "client-1",
        connectionId: late.connectionId,
        requestId: late.requestId,
        ok: true,
        result: {},
      });
      yield* broker.invoke({ scope, operation: "click", tabId, input: {} });
      expect(clicks).toBe(2);
    }),
  ),
);

it.effect.each(["stable", "reattached", "detached", "headless"] as const)(
  "captures request-target runtime evidence and invalidates a %s attachment",
  (mode) =>
    Effect.scoped(
      Effect.gen(function* () {
        const broker = yield* makeBroker;
        const identity = {
          schemaVersion: 1,
          runtimeKind: "electron",
          runtimeInstanceId: "desktop-one",
          appVersion: "0.1.0",
          buildCommit: null,
        } as const;
        let evidence: PreviewAutomationBroker.PreviewAutomationRuntimeEvidence | null =
          mode === "headless" ? null : { runtimeIdentity: identity, attachmentGeneration: 1 };
        const targets: string[] = [];
        const requests = requestsFrom(
          yield* broker.connect(makeHost(), {
            preferred: true,
            resolveRuntimeEvidence: (target) =>
              Effect.sync(() => {
                targets.push(`${target.threadId}/${target.tabId}`);
                return target.tabId === PreviewTabId.make("target-tab") ? evidence : null;
              }),
          }),
        );
        yield* Stream.runForEach(requests, (request) =>
          Effect.gen(function* () {
            if (mode === "reattached")
              evidence = { runtimeIdentity: identity, attachmentGeneration: 2 };
            if (mode === "detached") evidence = null;
            yield* broker.respond({
              clientId: "client-1",
              connectionId: request.connectionId,
              requestId: request.requestId,
              ok: true,
              result: { available: true, selectedClient: { runtimeIdentity: identity } },
            });
          }),
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        const result = yield* broker.invoke<{
          selectedClient: { runtimeIdentity: typeof identity | null };
        }>({ scope, operation: "status", tabId: PreviewTabId.make("target-tab"), input: {} });
        expect(result.selectedClient.runtimeIdentity).toEqual(mode === "stable" ? identity : null);
        expect(targets).toEqual(["thread-1/target-tab", "thread-1/target-tab"]);
      }),
    ),
);

it.effect(
  "retains targeted Electron affinity behind the aggregate native host until its hold expires",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const broker = yield* makeBroker;
        const identity = {
          schemaVersion: 1,
          runtimeKind: "electron",
          runtimeInstanceId: "physical-one",
          appVersion: "1.0.0",
          buildCommit: null,
        } as const;
        let evidence: PreviewAutomationBroker.PreviewAutomationRuntimeEvidence | null = {
          runtimeIdentity: identity,
          attachmentGeneration: 1,
        };
        let dispatched = 0;
        const tabId = PreviewTabId.make("remembered-tab");
        yield* Stream.runForEach(
          requestsFrom(
            yield* broker.connect(makeHost(), {
              preferred: true,
              resolveRuntimeEvidence: () => Effect.sync(() => evidence),
            }),
          ),
          (request) => {
            dispatched++;
            return broker.respond({
              clientId: "client-1",
              connectionId: request.connectionId,
              requestId: request.requestId,
              ok: true,
              result: { tabId },
            });
          },
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        yield* broker.invoke({ scope, operation: "status", tabId, input: {} });
        evidence = null;
        const detached = yield* broker
          .invoke({ scope, operation: "evaluate", input: {} })
          .pipe(Effect.flip, Effect.orDie);
        expect(detached).toMatchObject({
          _tag: "PreviewAutomationRequestQueueClosedError",
          outcome: "not_started",
        });
        expect(dispatched).toBe(1);
        evidence = {
          runtimeIdentity: { ...identity, runtimeInstanceId: "physical-two" },
          attachmentGeneration: 2,
        };
        const replaced = yield* broker
          .invoke({ scope, operation: "evaluate", input: {} })
          .pipe(Effect.flip, Effect.orDie);
        expect(replaced).toMatchObject({ outcome: "not_started" });
        expect(dispatched).toBe(1);
        evidence = { runtimeIdentity: identity, attachmentGeneration: 3 };
        yield* broker.invoke({ scope, operation: "status", tabId, input: {} });
        expect(dispatched).toBe(2);
        evidence = {
          runtimeIdentity: { ...identity, runtimeInstanceId: "physical-two" },
          attachmentGeneration: 4,
        };
        yield* broker
          .invoke({ scope, operation: "evaluate", input: {} })
          .pipe(Effect.flip, Effect.orDie);
        yield* TestClock.adjust(30_000);
        yield* broker.invoke({ scope, operation: "status", tabId, input: {} });
        expect(dispatched).toBe(3);
      }),
    ),
);
