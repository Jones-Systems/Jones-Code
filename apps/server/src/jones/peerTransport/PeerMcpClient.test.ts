import { EnvironmentId } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";

import * as PeerLinks from "../../peer/PeerLinks.ts";
import * as PeerMcpClient from "../../peer/PeerMcpClient.ts";
import { LINKED_MESSAGE_ORIGIN_HEADER, type LinkedMessageOrigin } from "./LinkedMessageOrigin.ts";

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
      generation: "generation-1",
      link: {},
    } as PeerLinks.ResolvedPeerLink),
  recordOutcome: () => Effect.void,
  currentGeneration: () => Effect.succeed(Option.some("generation-1")),
});
const fixture = (
  respond: (
    body: { id: unknown; method: string },
    request: HttpClientRequest.HttpClientRequest,
  ) => Response,
  linkLayer = links,
  onRequest?: (method: string) => void,
) => {
  let opens = 0;
  const http = HttpClient.make((request) =>
    Effect.sync(() => {
      if (request.body._tag !== "Uint8Array") throw new Error("Expected JSON request");
      const body = JSON.parse(new TextDecoder().decode(request.body.body));
      onRequest?.(body.method);
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
    Layer.provide(linkLayer),
    Layer.provide(Layer.succeed(HttpClient.HttpClient, http)),
  );
};

it.effect.each([
  { name: "omitted", expectedGeneration: undefined, refused: false },
  { name: "matching", expectedGeneration: "generation-1", refused: false },
  { name: "mismatched", expectedGeneration: "generation-0", refused: true },
])("$name captured generation gates all MCP HTTP effects", ({ expectedGeneration, refused }) => {
  const methods: Array<string> = [];
  return Effect.gen(function* () {
    const client = yield* PeerMcpClient.PeerMcpClient;
    const result = yield* client
      .call({ ...input, ...(expectedGeneration === undefined ? {} : { expectedGeneration }) })
      .pipe(Effect.result);
    if (refused) {
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") {
        expect(result.failure.code).toBe("invalid_request");
        expect(result.failure.message).toContain("not dispatched");
      }
      expect(methods).toEqual([]);
    } else {
      expect(result).toMatchObject({ _tag: "Success", success: { count: 1 } });
      expect(methods).toEqual(["initialize", "notifications/initialized", "tools/call"]);
    }
  }).pipe(
    Effect.provide(
      fixture(
        (body) => answer(body.id, { content: content({ count: 1 }) }),
        links,
        (method) => methods.push(method),
      ),
    ),
  );
});

it.effect(
  "refuses an admitted generation replaced during resolution even when the new link is current",
  () => {
    let generation = "generation-1";
    const expectedGeneration = generation;
    let resolutions = 0;
    const methods: Array<string> = [];
    const replacingLinks = Layer.mock(PeerLinks.PeerLinks)({
      resolve: () =>
        Effect.sync(() => {
          resolutions++;
          generation = "generation-2";
          return {
            url: "https://replacement.invalid",
            token: "synthetic-replacement-token",
            generation,
            link: {},
          } as PeerLinks.ResolvedPeerLink;
        }),
      currentGeneration: () => Effect.sync(() => Option.some(generation)),
      recordOutcome: () => Effect.void,
    });
    return Effect.gen(function* () {
      const client = yield* PeerMcpClient.PeerMcpClient;
      const failure = yield* client.call({ ...input, expectedGeneration }).pipe(Effect.flip);
      expect(failure.code).toBe("invalid_request");
      expect(failure.message).toContain("not dispatched");
      expect(resolutions).toBe(1);
      expect(generation).toBe("generation-2");
      expect(methods).toEqual([]);
    }).pipe(
      Effect.provide(
        fixture(
          (body) => answer(body.id, { content: content({ count: 1 }) }),
          replacingLinks,
          (method) => methods.push(method),
        ),
      ),
    );
  },
);

it.effect("a matching capture still refuses relinking immediately before tool dispatch", () => {
  let checks = 0;
  const methods: Array<string> = [];
  const relinkingLinks = Layer.mock(PeerLinks.PeerLinks)({
    resolve: () =>
      Effect.succeed({
        url: "https://peer.invalid",
        token: "synthetic-token",
        generation: "generation-1",
        link: {},
      } as PeerLinks.ResolvedPeerLink),
    currentGeneration: () =>
      Effect.sync(() => Option.some(++checks >= 3 ? "generation-2" : "generation-1")),
    recordOutcome: () => Effect.void,
  });
  return Effect.gen(function* () {
    const client = yield* PeerMcpClient.PeerMcpClient;
    const failure = yield* client
      .call({ ...input, expectedGeneration: "generation-1" })
      .pipe(Effect.flip);
    expect(failure.code).toBe("invalid_request");
    expect(failure.message).toContain("not dispatched");
    expect(checks).toBe(3);
    expect(methods).toEqual(["initialize", "notifications/initialized"]);
  }).pipe(
    Effect.provide(
      fixture(
        (body) => answer(body.id, { content: content({ count: 1 }) }),
        relinkingLinks,
        (method) => methods.push(method),
      ),
    ),
  );
});

it.effect(
  "carries a caller's origin per request and preserves it on explicit retry without leaking it",
  () => {
    const origin: LinkedMessageOrigin = {
      schemaVersion: "t3.linked-message-origin/v1",
      environmentId: "source",
      thread: { threadId: "sender", title: "Sender" },
    };
    const origins: Array<string | undefined> = [];
    const layer = fixture((body, request) => {
      origins.push(request.headers[LINKED_MESSAGE_ORIGIN_HEADER]);
      return origins.length === 1
        ? new Response(null, { status: 404 })
        : answer(body.id, { content: content({ count: 1 }) });
    });
    return Effect.gen(function* () {
      const client = yield* PeerMcpClient.PeerMcpClient;
      expect((yield* client.call({ ...input, origin }).pipe(Effect.result))._tag).toBe("Failure");
      expect(origins).toHaveLength(1);
      yield* client.call({ ...input, origin });
      yield* client.call(input);
      expect(JSON.parse(Buffer.from(origins[0]!, "base64url").toString("utf8"))).toEqual(origin);
      expect(origins[1]).toBe(origins[0]);
      expect(origins[2]).toBeUndefined();
    }).pipe(Effect.provide(layer));
  },
);

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

it.effect.each([
  {
    name: "wrong response ID",
    envelope: { jsonrpc: "2.0", id: "wrong", result: { content: content({ count: 1 }) } },
  },
  {
    name: "wrong protocol",
    envelope: { jsonrpc: "1.0", id: 1, result: { content: content({ count: 1 }) } },
  },
  {
    name: "both result and error",
    envelope: {
      jsonrpc: "2.0",
      id: 1,
      result: { content: content({ count: 1 }) },
      error: { code: -1, message: "failure" },
    },
  },
  { name: "missing result and error", envelope: { jsonrpc: "2.0", id: 1 } },
] as const)("rejects $name", ({ envelope }) =>
  Effect.gen(function* () {
    const client = yield* PeerMcpClient.PeerMcpClient;
    const result = yield* client.call(input).pipe(Effect.result);
    expect(result._tag).toBe("Failure");
    if (result._tag === "Failure") expect(result.failure.code).toBe("orchestration_error");
  }).pipe(Effect.provide(fixture(() => Response.json(envelope)))),
);

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

it.effect.each(["JSON-RPC", "structuredContent", "text"] as const)(
  "preserves public remote admission metadata from %s without owner-only details",
  (encoding) => {
    const remoteAdmission = {
      id: "opaque-admission-id",
      bindingDigest: "a".repeat(64),
      status: "held",
    } as const;
    const refusal = {
      code: "remote_admission_pending",
      message: "Waiting for owner input.",
      remoteAdmission: { ...remoteAdmission, reason: "owner-only nested reason" },
      reason: "owner-only reason",
    };
    return Effect.gen(function* () {
      const client = yield* PeerMcpClient.PeerMcpClient;
      const failure = yield* client.call(input).pipe(Effect.flip);
      expect(failure.code).toBe("remote_admission_pending");
      expect(failure.message).toBe("Waiting for owner input.");
      expect(failure.remoteAdmission).toEqual(remoteAdmission);
      expect(failure).not.toHaveProperty("reason");
      expect(failure.remoteAdmission).not.toHaveProperty("reason");
    }).pipe(
      Effect.provide(
        fixture((body) =>
          encoding === "JSON-RPC"
            ? Response.json({
                jsonrpc: "2.0",
                id: body.id,
                error: { code: -32000, message: "RPC wrapper", data: refusal },
              })
            : answer(
                body.id,
                encoding === "structuredContent"
                  ? {
                      content: [{ type: "text", text: "fallback" }],
                      structuredContent: refusal,
                      isError: true,
                    }
                  : { content: content(refusal), isError: true },
              ),
        ),
      ),
    );
  },
);
