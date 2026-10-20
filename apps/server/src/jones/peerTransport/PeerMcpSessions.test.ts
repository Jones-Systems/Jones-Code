import { EnvironmentId } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";

import * as PeerLinks from "../../peer/PeerLinks.ts";
import * as PeerMcpClient from "../../peer/PeerMcpClient.ts";

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
      link: {},
    } as PeerLinks.ResolvedPeerLink),
  recordOutcome: () => Effect.void,
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

for (const scenario of [
  "wrong initialize ID",
  "wrong protocol version",
  "notification refused",
] as const) {
  it.effect(`does not dispatch a tool after ${scenario}`, () => {
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
  });
}

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
