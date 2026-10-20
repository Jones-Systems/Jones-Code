import { EnvironmentId } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";

import * as PeerLinks from "../../peer/PeerLinks.ts";
import * as PeerMcpClient from "../../peer/PeerMcpClient.ts";

const input = {
  environmentId: EnvironmentId.make("peer-test"),
  tool: "mutate",
  arguments: { commandId: "stable-command" },
  limits: { runtimeMode: "auto", interactionMode: "default" } as const,
  success: Schema.Struct({ count: Schema.Number }),
};
const initialized = (id: unknown, sessionId: string) =>
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
    { headers: { "mcp-session-id": sessionId } },
  );
const answer = (id: unknown, result: unknown) => Response.json({ jsonrpc: "2.0", id, result });
const content = (value: unknown) => [{ type: "text", text: JSON.stringify(value) }];
const links = Layer.mock(PeerLinks.PeerLinks)({
  resolve: () =>
    Effect.succeed({
      url: "https://peer.invalid",
      token: "synthetic-token",
      link: {},
    } as PeerLinks.ResolvedPeerLink),
  recordOutcome: () => Effect.void,
});
const fixture = (
  respond: (
    body: { id: unknown; method: string },
    request: HttpClientRequest.HttpClientRequest,
  ) => Response,
) => {
  let opens = 0;
  const http = HttpClient.make((request) =>
    Effect.sync(() => {
      if (request.body._tag !== "Uint8Array") throw new Error("Expected JSON request");
      const body = JSON.parse(new TextDecoder().decode(request.body.body));
      const response =
        body.method === "initialize"
          ? initialized(body.id, `session-${++opens}`)
          : body.method === "notifications/initialized"
            ? new Response(null, { status: 202 })
            : respond(body, request);
      return HttpClientResponse.fromWeb(request, response);
    }),
  );
  return PeerMcpClient.layer.pipe(
    Layer.provide(links),
    Layer.provide(Layer.succeed(HttpClient.HttpClient, http)),
  );
};

it.effect(
  "does not replay a committed mutation after 404; only an explicit retry opens a new session",
  () => {
    let mutations = 0;
    const sessions: Array<string | undefined> = [];
    const layer = fixture((body, request) => {
      mutations++;
      sessions.push(request.headers["mcp-session-id"]);
      return mutations === 1
        ? new Response(null, { status: 404 })
        : answer(body.id, { content: content({ count: mutations }) });
    });
    return Effect.gen(function* () {
      const client = yield* PeerMcpClient.PeerMcpClient;
      const first = yield* client.call(input).pipe(Effect.result);
      expect(first._tag).toBe("Failure");
      expect(mutations).toBe(1);
      expect(yield* client.call(input)).toEqual({ count: 2 });
      expect(sessions).toEqual(["session-1", "session-2"]);
    }).pipe(Effect.provide(layer));
  },
);

it.effect("prefers structuredContent over the text fallback", () =>
  Effect.gen(function* () {
    const client = yield* PeerMcpClient.PeerMcpClient;
    expect(yield* client.call(input)).toEqual({ count: 3 });
  }).pipe(
    Effect.provide(
      fixture((body) =>
        answer(body.id, { content: content({ count: 1 }), structuredContent: { count: 3 } }),
      ),
    ),
  ),
);

it.effect("reads SSE events separately and ignores notifications and other response IDs", () =>
  Effect.gen(function* () {
    const client = yield* PeerMcpClient.PeerMcpClient;
    expect(yield* client.call(input)).toEqual({ count: 4 });
  }).pipe(
    Effect.provide(
      fixture(
        (body) =>
          new Response(
            [
              'event: message\r\ndata: {"jsonrpc":"2.0","method":"notifications/progress","params":{}}\r\n\r\n',
              'data: {"jsonrpc":"2.0","id":"other","result":{"content":[]}}\r\n\r\n',
              `event: message\r\ndata: {"jsonrpc":"2.0","id":${JSON.stringify(body.id)},\r\ndata: "result":{"content":[],"structuredContent":{"count":4}}}\r\n\r\n`,
            ].join(""),
            { headers: { "content-type": "text/event-stream" } },
          ),
      ),
    ),
  ),
);

for (const [name, envelope] of [
  [
    "wrong response ID",
    { jsonrpc: "2.0", id: "wrong", result: { content: content({ count: 1 }) } },
  ],
  ["wrong protocol", { jsonrpc: "1.0", id: 1, result: { content: content({ count: 1 }) } }],
  [
    "both result and error",
    {
      jsonrpc: "2.0",
      id: 1,
      result: { content: content({ count: 1 }) },
      error: { code: -1, message: "failure" },
    },
  ],
  ["missing result and error", { jsonrpc: "2.0", id: 1 }],
] as const) {
  it.effect(`rejects ${name}`, () =>
    Effect.gen(function* () {
      const client = yield* PeerMcpClient.PeerMcpClient;
      const result = yield* client.call(input).pipe(Effect.result);
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") expect(result.failure.code).toBe("orchestration_error");
    }).pipe(Effect.provide(fixture(() => Response.json(envelope)))),
  );
}

it.effect("preserves the peer's typed failure from structuredContent", () =>
  Effect.gen(function* () {
    const client = yield* PeerMcpClient.PeerMcpClient;
    const failure = yield* client.call(input).pipe(Effect.flip);
    expect(failure.code).toBe("capability_denied");
    expect(failure.message).toBe("Peer refused");
  }).pipe(
    Effect.provide(
      fixture((body) =>
        answer(body.id, {
          content: [{ type: "text", text: "fallback" }],
          structuredContent: { code: "capability_denied", message: "Peer refused" },
          isError: true,
        }),
      ),
    ),
  ),
);

it.effect("disables fetch redirects before sending the bearer", () => {
  let redirectsFollowed = 0;
  const requestOptions: Array<RequestInit | undefined> = [];
  const fetch: typeof globalThis.fetch = async (_url, options) => {
    requestOptions.push(options);
    if (options?.redirect !== "manual") redirectsFollowed++;
    return new Response(null, {
      status: 307,
      headers: { location: "https://unverified.invalid/mcp" },
    });
  };
  return Effect.gen(function* () {
    const client = yield* PeerMcpClient.PeerMcpClient;
    const result = yield* client.call(input).pipe(Effect.result);
    expect(result._tag).toBe("Failure");
    expect(redirectsFollowed).toBe(0);
    expect(requestOptions).toHaveLength(1);
  }).pipe(
    Effect.provide(
      PeerMcpClient.layer.pipe(Layer.provide(links), Layer.provide(FetchHttpClient.layer)),
    ),
    Effect.provideService(FetchHttpClient.Fetch, fetch),
  );
});
