import { EnvironmentId, PeerLinkError } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";

import * as PeerLinks from "../../peer/PeerLinks.ts";
import * as PeerMcpClient from "../../peer/PeerMcpClient.ts";
import * as PeerMonitor from "../remoteDelegation/PeerMonitor.ts";

const input = {
  environmentId: EnvironmentId.make("session-test"),
  tool: "mutate",
  arguments: { commandId: "key" },
  limits: { runtimeMode: "auto", interactionMode: "default" } as const,
  success: Schema.Struct({ ok: Schema.Boolean }),
};
const links = Layer.mock(PeerLinks.PeerLinks)({
  resolve: () =>
    Effect.succeed({
      url: "https://peer.invalid",
      token: "synthetic-token",
      generation: "generation-1",
      link: {},
    } as PeerLinks.ResolvedPeerLink),
  recordOutcome: () => Effect.void,
  currentGeneration: () => Effect.succeed(Option.some("generation-1")),
});
const initialization = (id: unknown, session: string) =>
  Response.json(
    {
      jsonrpc: "2.0",
      id,
      result: {
        protocolVersion: "2025-06-18",
        capabilities: {},
        serverInfo: { name: "test", version: "1" },
      },
    },
    { headers: { "mcp-session-id": session } },
  );

const successfulCall = (id: unknown) =>
  Response.json({
    jsonrpc: "2.0",
    id,
    result: { content: [{ type: "text", text: '{"ok":true}' }] },
  });
const clientLayer = (http: HttpClient.HttpClient, linkLayer = links) =>
  PeerMcpClient.layer.pipe(
    Layer.provide(linkLayer),
    Layer.provide(Layer.succeed(HttpClient.HttpClient, http)),
  );

it.effect("cold callers share one initialization while every call resolves its peer afresh", () =>
  Effect.gen(function* () {
    const allResolved = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    let resolves = 0;
    let opens = 0;
    let calls = 0;
    const concurrentLinks = Layer.mock(PeerLinks.PeerLinks)({
      resolve: () =>
        Effect.gen(function* () {
          if (++resolves === 8) yield* Deferred.succeed(allResolved, undefined);
          return {
            url: "https://peer.invalid",
            token: "synthetic-token",
            generation: "g1",
            link: {},
          } as PeerLinks.ResolvedPeerLink;
        }),
      currentGeneration: () => Effect.succeed(Option.some("g1")),
      recordOutcome: () => Effect.void,
    });
    const http = HttpClient.make((request) =>
      Effect.gen(function* () {
        if (request.body._tag !== "Uint8Array") return yield* Effect.die("Expected JSON request");
        const body = JSON.parse(new TextDecoder().decode(request.body.body));
        if (body.method === "initialize") {
          opens++;
          yield* Deferred.await(release);
          return HttpClientResponse.fromWeb(request, initialization(body.id, "shared"));
        }
        if (body.method === "notifications/initialized")
          return HttpClientResponse.fromWeb(request, new Response(null, { status: 202 }));
        calls++;
        return HttpClientResponse.fromWeb(request, successfulCall(body.id));
      }),
    );
    yield* Effect.gen(function* () {
      const client = yield* PeerMcpClient.PeerMcpClient;
      const waiting = yield* Effect.forEach(Array.from({ length: 8 }), () => client.call(input), {
        concurrency: "unbounded",
      }).pipe(Effect.forkChild);
      yield* Deferred.await(allResolved);
      yield* Deferred.succeed(release, undefined);
      expect(yield* Fiber.join(waiting)).toHaveLength(8);
      expect(opens).toBe(1);
      expect(calls).toBe(8);
      expect(resolves).toBe(8);
    }).pipe(Effect.provide(clientLayer(http, concurrentLinks)));
  }),
);

it.effect.each(["one", "all"] as const)(
  "cancelling %s opening waiter leaves service-owned initialization independent",
  (cancel) =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const secondResolved = yield* Deferred.make<void>();
      let resolves = 0;
      let opens = 0;
      let interrupted = 0;
      const cancellableLinks = Layer.mock(PeerLinks.PeerLinks)({
        resolve: () =>
          Effect.gen(function* () {
            if (++resolves === 2) yield* Deferred.succeed(secondResolved, undefined);
            return {
              url: "https://peer.invalid",
              token: "synthetic-token",
              generation: "g1",
              link: {},
            } as PeerLinks.ResolvedPeerLink;
          }),
        currentGeneration: () => Effect.succeed(Option.some("g1")),
        recordOutcome: () => Effect.void,
      });
      const http = HttpClient.make((request) =>
        Effect.gen(function* () {
          if (request.body._tag !== "Uint8Array") return yield* Effect.die("Expected JSON request");
          const body = JSON.parse(new TextDecoder().decode(request.body.body));
          if (body.method === "initialize") {
            opens++;
            yield* Deferred.succeed(started, undefined);
            yield* Deferred.await(release).pipe(
              Effect.onInterrupt(() => Effect.sync(() => interrupted++)),
            );
            return HttpClientResponse.fromWeb(request, initialization(body.id, "shared"));
          }
          return HttpClientResponse.fromWeb(
            request,
            body.method === "notifications/initialized"
              ? new Response(null, { status: 202 })
              : successfulCall(body.id),
          );
        }),
      );
      yield* Effect.gen(function* () {
        const client = yield* PeerMcpClient.PeerMcpClient;
        const first = yield* client.call(input).pipe(Effect.forkChild);
        yield* Deferred.await(started);
        const second = yield* client.call(input).pipe(Effect.forkChild);
        yield* Deferred.await(secondResolved);
        yield* Fiber.interrupt(first);
        if (cancel === "all") yield* Fiber.interrupt(second);
        expect(interrupted).toBe(0);
        yield* Deferred.succeed(release, undefined);
        if (cancel === "one") expect(yield* Fiber.join(second)).toEqual({ ok: true });
        expect(yield* client.call(input)).toEqual({ ok: true });
        expect(opens).toBe(1);
      }).pipe(Effect.provide(clientLayer(http, cancellableLinks)));
    }),
);

it.effect(
  "relinking during initialization rejects the old opener without evicting its successor",
  () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let generation = "old";
      let opens = 0;
      const calls: Array<string | undefined> = [];
      const outcomes: Array<string> = [];
      const changingLinks = Layer.mock(PeerLinks.PeerLinks)({
        resolve: () =>
          Effect.sync(
            () =>
              ({
                url: "https://peer.invalid",
                token: "same-synthetic-token",
                generation,
                link: {},
              }) as PeerLinks.ResolvedPeerLink,
          ),
        currentGeneration: () => Effect.sync(() => Option.some(generation)),
        recordOutcome: (_environmentId, captured) =>
          Effect.sync(() => {
            outcomes.push(captured);
          }),
      });
      const http = HttpClient.make((request) =>
        Effect.gen(function* () {
          if (request.body._tag !== "Uint8Array") return yield* Effect.die("Expected JSON request");
          const body = JSON.parse(new TextDecoder().decode(request.body.body));
          if (body.method === "initialize") {
            const session = `session-${++opens}`;
            if (opens === 1) {
              yield* Deferred.succeed(started, undefined);
              yield* Deferred.await(release);
            }
            return HttpClientResponse.fromWeb(request, initialization(body.id, session));
          }
          if (body.method === "notifications/initialized")
            return HttpClientResponse.fromWeb(request, new Response(null, { status: 202 }));
          calls.push(request.headers["mcp-session-id"]);
          return HttpClientResponse.fromWeb(request, successfulCall(body.id));
        }),
      );
      yield* Effect.gen(function* () {
        const client = yield* PeerMcpClient.PeerMcpClient;
        const old = yield* client.call(input).pipe(Effect.result, Effect.forkChild);
        yield* Deferred.await(started);
        generation = "new";
        expect(yield* client.call(input)).toEqual({ ok: true });
        yield* Deferred.succeed(release, undefined);
        const failure = yield* Fiber.join(old);
        expect(failure._tag).toBe("Failure");
        if (failure._tag === "Failure") expect(failure.failure.message).toContain("not dispatched");
        expect(yield* client.call(input)).toEqual({ ok: true });
        expect(calls).toEqual(["session-2", "session-2"]);
        expect(outcomes).toEqual(["new", "old", "new"]);
        expect(opens).toBe(2);
      }).pipe(Effect.provide(clientLayer(http, changingLinks)));
    }),
);

it.effect("closing the service interrupts initialization and all outside waiters", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    let interrupted = 0;
    const http = HttpClient.make(() =>
      Deferred.succeed(started, undefined).pipe(
        Effect.andThen(Effect.never),
        Effect.onInterrupt(() => Effect.sync(() => interrupted++)),
      ),
    );
    const lifetime = yield* Effect.acquireRelease(Scope.make(), (scope) =>
      Scope.close(scope, Exit.void),
    );
    const context = yield* Layer.build(clientLayer(http)).pipe(Scope.provide(lifetime));
    const client = Context.get(context, PeerMcpClient.PeerMcpClient);
    const first = yield* client.call(input).pipe(Effect.forkChild);
    yield* Deferred.await(started);
    const second = yield* client.call(input).pipe(Effect.forkChild);
    yield* Scope.close(lifetime, Exit.void);
    expect(Exit.hasInterrupts(yield* Fiber.await(first))).toBe(true);
    expect(Exit.hasInterrupts(yield* Fiber.await(second))).toBe(true);
    expect(interrupted).toBe(1);
  }).pipe(Effect.scoped),
);

it.effect(
  "an opener with no waiters still expires at the existing deadline and failures are not cached",
  () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const stopped = yield* Deferred.make<void>();
      let opens = 0;
      const http = HttpClient.make((request) =>
        Effect.gen(function* () {
          if (request.body._tag !== "Uint8Array") return yield* Effect.die("Expected JSON request");
          const body = JSON.parse(new TextDecoder().decode(request.body.body));
          if (body.method === "initialize") {
            if (++opens === 1) {
              yield* Deferred.succeed(started, undefined);
              return yield* Effect.never.pipe(
                Effect.onInterrupt(() => Deferred.succeed(stopped, undefined)),
              );
            }
            return HttpClientResponse.fromWeb(request, initialization(body.id, "recovered"));
          }
          return HttpClientResponse.fromWeb(
            request,
            body.method === "notifications/initialized"
              ? new Response(null, { status: 202 })
              : successfulCall(body.id),
          );
        }),
      );
      yield* Effect.gen(function* () {
        const client = yield* PeerMcpClient.PeerMcpClient;
        const abandoned = yield* client.call(input).pipe(Effect.forkChild);
        yield* Deferred.await(started);
        yield* Fiber.interrupt(abandoned);
        expect(yield* Deferred.isDone(stopped)).toBe(false);
        yield* TestClock.adjust("60 seconds");
        yield* Deferred.await(stopped);
        expect(yield* client.call(input)).toEqual({ ok: true });
        expect(opens).toBe(2);
      }).pipe(Effect.provide(clientLayer(http)));
    }),
);

it.effect("a link change after initialization is checked again immediately before dispatch", () =>
  Effect.gen(function* () {
    let checks = 0;
    let calls = 0;
    const changingLinks = Layer.mock(PeerLinks.PeerLinks)({
      resolve: () =>
        Effect.succeed({
          url: "https://peer.invalid",
          token: "synthetic-token",
          generation: "old",
          link: {},
        } as PeerLinks.ResolvedPeerLink),
      currentGeneration: () => Effect.sync(() => Option.some(++checks >= 3 ? "new" : "old")),
      recordOutcome: () => Effect.void,
    });
    const http = HttpClient.make((request) =>
      Effect.sync(() => {
        if (request.body._tag !== "Uint8Array") throw new Error("Expected JSON request");
        const body = JSON.parse(new TextDecoder().decode(request.body.body));
        if (body.method === "initialize")
          return HttpClientResponse.fromWeb(request, initialization(body.id, "old-session"));
        if (body.method === "notifications/initialized")
          return HttpClientResponse.fromWeb(request, new Response(null, { status: 202 }));
        calls++;
        return HttpClientResponse.fromWeb(request, successfulCall(body.id));
      }),
    );
    yield* Effect.gen(function* () {
      const client = yield* PeerMcpClient.PeerMcpClient;
      expect((yield* client.call(input).pipe(Effect.flip)).message).toContain("not dispatched");
      expect(calls).toBe(0);
      expect(checks).toBe(3);
    }).pipe(Effect.provide(clientLayer(http, changingLinks)));
  }),
);

it.effect("a resolution failure with no captured generation cannot write an outcome", () =>
  Effect.gen(function* () {
    let outcomes = 0;
    const missingLinks = Layer.mock(PeerLinks.PeerLinks)({
      resolve: () =>
        Effect.fail(new PeerLinkError({ reason: "unknown_link", message: "Unlinked" })),
      currentGeneration: () => Effect.succeed(Option.none()),
      recordOutcome: () =>
        Effect.sync(() => {
          outcomes++;
        }),
    });
    const http = HttpClient.make(() => Effect.die("No bearer may be dispatched"));
    yield* Effect.gen(function* () {
      const client = yield* PeerMcpClient.PeerMcpClient;
      expect((yield* client.call(input).pipe(Effect.flip)).message).toBe("Unlinked");
      expect(outcomes).toBe(0);
    }).pipe(Effect.provide(clientLayer(http, missingLinks)));
  }),
);

it.effect("a delayed old-session 404 cannot evict the new cached session", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    let opens = 0;
    let calls = 0;
    const seen: Array<string | undefined> = [];
    const http = HttpClient.make((request) =>
      Effect.gen(function* () {
        if (request.body._tag !== "Uint8Array") return yield* Effect.die("Expected JSON request");
        const body = JSON.parse(new TextDecoder().decode(request.body.body));
        if (body.method === "initialize")
          return HttpClientResponse.fromWeb(request, initialization(body.id, `session-${++opens}`));
        if (body.method === "notifications/initialized")
          return HttpClientResponse.fromWeb(request, new Response(null, { status: 202 }));
        const callNumber = ++calls;
        seen.push(request.headers["mcp-session-id"]);
        if (callNumber === 2) {
          yield* Deferred.succeed(started, undefined);
          yield* Deferred.await(release);
        }
        return HttpClientResponse.fromWeb(
          request,
          callNumber === 2 || callNumber === 3
            ? new Response(null, { status: 404 })
            : Response.json({
                jsonrpc: "2.0",
                id: body.id,
                result: { content: [{ type: "text", text: '{"ok":true}' }] },
              }),
        );
      }),
    );
    yield* Effect.gen(function* () {
      const client = yield* PeerMcpClient.PeerMcpClient;
      yield* client.call(input);
      const delayed = yield* client.call(input).pipe(Effect.result, Effect.forkChild);
      yield* Deferred.await(started);
      expect((yield* client.call(input).pipe(Effect.result))._tag).toBe("Failure");
      yield* client.call(input);
      yield* Deferred.succeed(release, undefined);
      expect((yield* Fiber.join(delayed))._tag).toBe("Failure");
      yield* client.call(input);
      expect(opens).toBe(2);
      expect(seen).toEqual(["session-1", "session-1", "session-1", "session-2", "session-2"]);
    }).pipe(
      Effect.provide(
        PeerMcpClient.layer.pipe(
          Layer.provide(links),
          Layer.provide(Layer.succeed(HttpClient.HttpClient, http)),
        ),
      ),
    );
  }),
);

it.effect.each(["wrong initialize ID", "wrong protocol version", "notification refused"] as const)(
  "does not dispatch a tool after %s",
  (scenario) => {
    let toolCalls = 0;
    const http = HttpClient.make((request) =>
      Effect.sync(() => {
        if (request.body._tag !== "Uint8Array") throw new Error("Expected JSON request");
        const body = JSON.parse(new TextDecoder().decode(request.body.body));
        if (body.method === "initialize")
          return HttpClientResponse.fromWeb(
            request,
            scenario === "wrong protocol version"
              ? Response.json(
                  {
                    jsonrpc: "2.0",
                    id: body.id,
                    result: {
                      protocolVersion: "invalid",
                      capabilities: {},
                      serverInfo: { name: "test", version: "1" },
                    },
                  },
                  { headers: { "mcp-session-id": "s" } },
                )
              : initialization(scenario === "wrong initialize ID" ? "wrong" : body.id, "s"),
          );
        if (body.method === "notifications/initialized")
          return HttpClientResponse.fromWeb(request, new Response(null, { status: 403 }));
        toolCalls++;
        return HttpClientResponse.fromWeb(
          request,
          Response.json({
            jsonrpc: "2.0",
            id: body.id,
            result: { content: [{ type: "text", text: '{"ok":true}' }] },
          }),
        );
      }),
    );
    return Effect.gen(function* () {
      const client = yield* PeerMcpClient.PeerMcpClient;
      expect((yield* client.call(input).pipe(Effect.result))._tag).toBe("Failure");
      expect(toolCalls).toBe(0);
    }).pipe(
      Effect.provide(
        PeerMcpClient.layer.pipe(
          Layer.provide(links),
          Layer.provide(Layer.succeed(HttpClient.HttpClient, http)),
        ),
      ),
    );
  },
);

it.effect("preserves typed refusals in JSON-RPC error.data", () => {
  const http = HttpClient.make((request) =>
    Effect.sync(() => {
      if (request.body._tag !== "Uint8Array") throw new Error("Expected JSON request");
      const body = JSON.parse(new TextDecoder().decode(request.body.body));
      return HttpClientResponse.fromWeb(
        request,
        body.method === "initialize"
          ? initialization(body.id, "s")
          : body.method === "notifications/initialized"
            ? new Response(null, { status: 202 })
            : Response.json({
                jsonrpc: "2.0",
                id: body.id,
                error: {
                  code: -32000,
                  message: "RPC wrapper",
                  data: { code: "capability_denied", message: "Denied by peer" },
                },
              }),
      );
    }),
  );
  return Effect.gen(function* () {
    const client = yield* PeerMcpClient.PeerMcpClient;
    const failure = yield* client.call(input).pipe(Effect.flip);
    expect(failure.code).toBe("capability_denied");
    expect(failure.message).toBe("Denied by peer");
  }).pipe(
    Effect.provide(
      PeerMcpClient.layer.pipe(
        Layer.provide(links),
        Layer.provide(Layer.succeed(HttpClient.HttpClient, http)),
      ),
    ),
  );
});

it.effect.each(["execute", "body"] as const)(
  "bounds a stalled %s, cancels local work, and never replays an uncertain mutation",
  (stage) =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const done = yield* Deferred.make<void>();
      let calls = 0;
      let cancelled = 0;
      const stalled = Deferred.succeed(started, undefined).pipe(
        Effect.andThen(Effect.never),
        Effect.onInterrupt(() =>
          Effect.sync(() => {
            cancelled++;
          }),
        ),
      );
      const http = HttpClient.make((request) =>
        Effect.gen(function* () {
          if (request.body._tag !== "Uint8Array") return yield* Effect.die("Expected JSON request");
          const body = JSON.parse(new TextDecoder().decode(request.body.body));
          if (body.method === "initialize")
            return HttpClientResponse.fromWeb(request, initialization(body.id, "s"));
          if (body.method === "notifications/initialized")
            return HttpClientResponse.fromWeb(request, new Response(null, { status: 202 }));
          calls++;
          const response = HttpClientResponse.fromWeb(
            request,
            Response.json({
              jsonrpc: "2.0",
              id: body.id,
              result: { content: [{ type: "text", text: '{"ok":true}' }] },
            }),
          );
          if (calls > 1) return response;
          if (stage === "execute") return yield* stalled;
          return Object.defineProperty(response, "text", { value: stalled });
        }),
      );
      yield* Effect.gen(function* () {
        const client = yield* PeerMcpClient.PeerMcpClient;
        const first = yield* client.call(input).pipe(
          Effect.result,
          Effect.tap(() => Deferred.succeed(done, undefined)),
          Effect.forkChild,
        );
        yield* Deferred.await(started);
        yield* TestClock.adjust("60 seconds");
        expect(yield* Deferred.isDone(done)).toBe(true);
        const result = yield* Fiber.join(first);
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure") {
          expect(result.failure.code).toBe("orchestration_error");
          expect(result.failure.message).toContain("not replayed");
          expect(result.failure.message).toContain("Reconcile");
        }
        expect(cancelled).toBe(1);
        expect(calls).toBe(1);
        expect(yield* client.call(input)).toEqual({ ok: true });
        expect(calls).toBe(2);
      }).pipe(
        Effect.provide(
          PeerMcpClient.layer.pipe(
            Layer.provide(links),
            Layer.provide(Layer.succeed(HttpClient.HttpClient, http)),
          ),
        ),
      );
    }),
);

it.effect.each(["interrupt", "deadline"] as const)(
  "%s releases saturated peer monitoring permits and allows a later call",
  (release) =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const recovered = yield* Deferred.make<void>();
      let calls = 0;
      const stalledSignals: Array<AbortSignal> = [];
      const http = HttpClient.make((request, _url, signal) =>
        Effect.gen(function* () {
          if (request.body._tag !== "Uint8Array") return yield* Effect.die("Expected JSON request");
          const body = JSON.parse(new TextDecoder().decode(request.body.body));
          if (body.method === "initialize")
            return HttpClientResponse.fromWeb(request, initialization(body.id, "s"));
          if (body.method === "notifications/initialized")
            return HttpClientResponse.fromWeb(request, new Response(null, { status: 202 }));
          const response = HttpClientResponse.fromWeb(
            request,
            Response.json({
              jsonrpc: "2.0",
              id: body.id,
              result: { content: [{ type: "text", text: '{"ok":true}' }] },
            }),
          );
          calls++;
          if (calls > 4) return response;
          stalledSignals.push(signal);
          return Object.defineProperty(response, "text", {
            value: Effect.gen(function* () {
              if (calls === 4) yield* Deferred.succeed(started, undefined);
              return yield* Effect.never;
            }),
          });
        }),
      );
      yield* Effect.gen(function* () {
        const client = yield* PeerMcpClient.PeerMcpClient;
        const monitor = yield* PeerMonitor.make;
        const blocked = yield* Effect.forEach([0, 1, 2, 3], () =>
          monitor(input.environmentId, client.call(input)).pipe(Effect.result, Effect.forkChild),
        );
        yield* Deferred.await(started);
        const later = yield* monitor(input.environmentId, client.call(input)).pipe(
          Effect.tap(() => Deferred.succeed(recovered, undefined)),
          Effect.forkChild,
        );
        expect(calls).toBe(4);
        if (release === "interrupt") yield* Fiber.interrupt(blocked[0]!);
        else yield* TestClock.adjust("2 minutes");
        yield* Deferred.await(recovered);
        expect(yield* Fiber.join(later)).toEqual({ ok: true });
        expect(calls).toBe(5);
        expect(stalledSignals[0]?.aborted).toBe(true);
        yield* Effect.forEach(blocked, Fiber.interrupt);
        expect(stalledSignals.every((signal) => signal.aborted)).toBe(true);
      }).pipe(
        Effect.provide(
          PeerMcpClient.layer.pipe(
            Layer.provide(links),
            Layer.provide(Layer.succeed(HttpClient.HttpClient, http)),
          ),
        ),
      );
    }),
);

it.effect("allows a supported 50-second remote wait plus body-read margin", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    const http = HttpClient.make((request) =>
      Effect.gen(function* () {
        if (request.body._tag !== "Uint8Array") return yield* Effect.die("Expected JSON request");
        const body = JSON.parse(new TextDecoder().decode(request.body.body));
        if (body.method === "initialize")
          return HttpClientResponse.fromWeb(request, initialization(body.id, "s"));
        if (body.method === "notifications/initialized")
          return HttpClientResponse.fromWeb(request, new Response(null, { status: 202 }));
        yield* Deferred.succeed(started, undefined);
        yield* Effect.sleep("50 seconds");
        const response = HttpClientResponse.fromWeb(
          request,
          Response.json({
            jsonrpc: "2.0",
            id: body.id,
            result: { content: [{ type: "text", text: '{"ok":true}' }] },
          }),
        );
        const text = response.text;
        return Object.defineProperty(response, "text", {
          value: Effect.sleep("5 seconds").pipe(Effect.andThen(text)),
        });
      }),
    );
    yield* Effect.gen(function* () {
      const client = yield* PeerMcpClient.PeerMcpClient;
      const waiting = yield* client
        .call({ ...input, tool: "wait_for_thread", arguments: { timeoutMs: 50_000 } })
        .pipe(Effect.forkChild);
      yield* Deferred.await(started);
      yield* TestClock.adjust("55 seconds");
      expect(yield* Fiber.join(waiting)).toEqual({ ok: true });
    }).pipe(
      Effect.provide(
        PeerMcpClient.layer.pipe(
          Layer.provide(links),
          Layer.provide(Layer.succeed(HttpClient.HttpClient, http)),
        ),
      ),
    );
  }),
);
