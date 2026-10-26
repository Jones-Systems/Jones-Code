import { EnvironmentId, type LinkedRequestContext } from "@t3tools/contracts";
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
import {
  LINKED_REQUEST_CONTEXT_HEADER,
  PEER_REQUEST_HEADER_BUDGET,
  REQUEST_CONTEXT_MAX_SENT_CHARS,
  UNCONTROLLED_HEADER_ALLOWANCE,
  requestHeaderBytes,
} from "./LinkedRequestContext.ts";

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
  options: {
    readonly token?: string;
    readonly sessionId?: string;
    readonly inspect?: (method: string, request: HttpClientRequest.HttpClientRequest) => void;
  } = {},
) => {
  let opens = 0;
  const http = HttpClient.make((request) =>
    Effect.sync(() => {
      if (request.body._tag !== "Uint8Array") throw new Error("Expected JSON request");
      const body = JSON.parse(new TextDecoder().decode(request.body.body));
      options.inspect?.(body.method, request);
      const response =
        body.method === "initialize"
          ? initialized(body.id, options.sessionId ?? `session-${++opens}`)
          : body.method === "notifications/initialized"
            ? new Response(null, { status: 202 })
            : respond(body, request);
      return HttpClientResponse.fromWeb(request, response);
    }),
  );
  return PeerMcpClient.layer.pipe(
    Layer.provide(
      options.token === undefined
        ? links
        : Layer.mock(PeerLinks.PeerLinks)({
            resolve: () =>
              Effect.succeed({
                url: "https://peer.invalid",
                token: options.token!,
                generation: "generation-1",
                link: {},
              } as PeerLinks.ResolvedPeerLink),
            recordOutcome: () => Effect.void,
            currentGeneration: () => Effect.succeed(Option.some("generation-1")),
          }),
    ),
    Layer.provide(Layer.succeed(HttpClient.HttpClient, http)),
  );
};

const requestContext: LinkedRequestContext = {
  schemaVersion: "t3.linked-request-context/v1",
  sentAt: "2026-10-26T12:34:56.789Z",
  thread: { threadId: "t".repeat(256), title: "界".repeat(120) },
  parent: { threadId: "親".repeat(256), title: "界".repeat(120), environmentId: "e".repeat(256) },
  project: {
    projectId: "項".repeat(256),
    title: "界".repeat(120),
    icon: { kind: "emoji", emoji: "🚀".repeat(16) },
  },
};

it.effect(
  "fits non-ASCII context within the whole header budget and preserves every existing header",
  () => {
    const calls: Array<Readonly<Record<string, string>>> = [];
    const origin: LinkedMessageOrigin = {
      schemaVersion: "t3.linked-message-origin/v1",
      environmentId: "e".repeat(256),
      thread: { threadId: "t".repeat(256), title: "界".repeat(512) },
    };
    const setupHeaders: Array<string | undefined> = [];
    return Effect.gen(function* () {
      const client = yield* PeerMcpClient.PeerMcpClient;
      yield* client.call({ ...input, origin });
      yield* client.call({ ...input, origin, context: requestContext });
      yield* client.call({ ...input, origin });
      const header = calls[1]![LINKED_REQUEST_CONTEXT_HEADER]!;
      expect(header).toBeDefined();
      expect(header.length).toBeLessThanOrEqual(REQUEST_CONTEXT_MAX_SENT_CHARS);
      const generatedNames = new Set(["content-type", "content-length", "b3", "traceparent"]);
      const controlled = Object.fromEntries(
        Object.entries(calls[1]!).filter(([name]) => !generatedNames.has(name)),
      );
      const generated = Object.fromEntries(
        Object.entries(calls[1]!).filter(([name]) => generatedNames.has(name)),
      );
      expect(requestHeaderBytes(controlled) + UNCONTROLLED_HEADER_ALLOWANCE).toBeLessThanOrEqual(
        PEER_REQUEST_HEADER_BUDGET,
      );
      expect(Object.keys(generated).sort()).toEqual([...generatedNames].sort());
      expect(requestHeaderBytes(generated)).toBeLessThan(UNCONTROLLED_HEADER_ALLOWANCE);
      expect(requestHeaderBytes(calls[1]!)).toBeLessThanOrEqual(PEER_REQUEST_HEADER_BUDGET);
      const decoded = JSON.parse(Buffer.from(header, "base64url").toString("utf8"));
      expect(decoded.schemaVersion).toBe(requestContext.schemaVersion);
      expect(decoded.sentAt).toBe(requestContext.sentAt);
      expect(decoded.project).toBeUndefined();
      expect(decoded.parent).toBeUndefined();
      const { [LINKED_REQUEST_CONTEXT_HEADER]: _context, ...existing } = calls[1]!;
      const withoutTrace = (headers: Readonly<Record<string, string>>) =>
        Object.fromEntries(
          Object.entries(headers).filter(([name]) => name !== "b3" && name !== "traceparent"),
        );
      expect(withoutTrace(existing)).toEqual(withoutTrace(calls[0]!));
      expect(withoutTrace(calls[2]!)).toEqual(withoutTrace(calls[0]!));
      for (const headers of calls) {
        expect(headers.b3).toMatch(/^[0-9a-f]{32}-[0-9a-f]{16}-[01](?:-[0-9a-f]{16})?$/i);
        expect(headers.traceparent).toMatch(/^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$/i);
      }
      expect(setupHeaders).toEqual([undefined, undefined]);
    }).pipe(
      Effect.provide(
        fixture(
          (body, request) => {
            calls.push(request.headers);
            return answer(body.id, { content: content({ count: 1 }) });
          },
          {
            token: "x".repeat(3500),
            inspect: (method, request) => {
              if (method !== "tools/call") {
                setupHeaders.push(request.headers[LINKED_REQUEST_CONTEXT_HEADER]);
              }
            },
          },
        ),
      ),
    );
  },
);

it.effect.each([6400, 6500, 9000])(
  "omits context when existing headers leave no usable room (token %s)",
  (tokenLength) => {
    let headers: Readonly<Record<string, string>> = {};
    return Effect.gen(function* () {
      const client = yield* PeerMcpClient.PeerMcpClient;
      yield* client.call({ ...input, context: requestContext });
      expect(headers[LINKED_REQUEST_CONTEXT_HEADER]).toBeUndefined();
      expect(headers.authorization).toBe(`Bearer ${"x".repeat(tokenLength)}`);
      expect(headers["mcp-session-id"]).toBe("s".repeat(500));
    }).pipe(
      Effect.provide(
        fixture(
          (body, request) => {
            headers = request.headers;
            return answer(body.id, { content: content({ count: 1 }) });
          },
          { token: "x".repeat(tokenLength), sessionId: "s".repeat(500) },
        ),
      ),
    );
  },
);

it.effect.each([404, 431, 500])("never replays without context after HTTP %s", (status) => {
  const calls: Array<string | undefined> = [];
  return Effect.gen(function* () {
    const client = yield* PeerMcpClient.PeerMcpClient;
    const result = yield* client.call({ ...input, context: requestContext }).pipe(Effect.result);
    expect(result._tag).toBe("Failure");
    expect(calls).toHaveLength(1);
    expect(calls[0]).toBeDefined();
  }).pipe(
    Effect.provide(
      fixture((_body, request) => {
        calls.push(request.headers[LINKED_REQUEST_CONTEXT_HEADER]);
        return new Response(null, { status });
      }),
    ),
  );
});

it.effect("adds context only to tools/call and never replays after a fetch failure", () => {
  const seen: Array<{ readonly method: string; readonly context: string | null }> = [];
  const fetch: typeof globalThis.fetch = async (_url, options) => {
    const payload = options?.body;
    if (typeof payload !== "string" && !(payload instanceof Uint8Array)) {
      throw new Error("Expected a synthetic JSON request body");
    }
    const body = JSON.parse(
      typeof payload === "string" ? payload : new TextDecoder().decode(payload),
    );
    seen.push({
      method: body.method,
      context: new Headers(options?.headers).get(LINKED_REQUEST_CONTEXT_HEADER),
    });
    if (body.method === "initialize") return initialized(body.id, "fetch-session");
    if (body.method === "notifications/initialized") return new Response(null, { status: 202 });
    throw new Error("Synthetic connection loss after dispatch");
  };
  return Effect.gen(function* () {
    const client = yield* PeerMcpClient.PeerMcpClient;
    const result = yield* client.call({ ...input, context: requestContext }).pipe(Effect.result);
    expect(result._tag).toBe("Failure");
    expect(seen.map((entry) => entry.method)).toEqual([
      "initialize",
      "notifications/initialized",
      "tools/call",
    ]);
    expect(seen[0]?.context).toBeNull();
    expect(seen[1]?.context).toBeNull();
    expect(seen[2]?.context).toBeTruthy();
    expect(seen[2]!.context!.length).toBeLessThanOrEqual(REQUEST_CONTEXT_MAX_SENT_CHARS);
  }).pipe(
    Effect.provide(
      PeerMcpClient.layer.pipe(Layer.provide(links), Layer.provide(FetchHttpClient.layer)),
    ),
    Effect.provideService(FetchHttpClient.Fetch, fetch),
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
