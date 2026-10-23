import {
  EnvironmentId,
  OrchestratorMcpFailure,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import { NodeHttpServer } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";
import { HttpBody, HttpClient, HttpRouter } from "effect/http";

import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import { McpInvocationContext } from "../../mcp/McpInvocationContext.ts";
import * as McpHttpServer from "../../mcp/McpHttpServer.ts";
import * as McpSessionRegistry from "../../mcp/McpSessionRegistry.ts";
import * as McpToolAccess from "../../mcp/McpToolAccess.ts";
import {
  LINKED_MESSAGE_ORIGIN_HEADER,
  encodeLinkedMessageOrigin,
  formatLinkedMessageOrigin,
  withLinkedMessageOrigin,
  type LinkedMessageOrigin,
} from "./LinkedMessageOrigin.ts";

const scope: McpInvocationScope = {
  environmentId: EnvironmentId.make("receiver"),
  requestNamespace: "client:peer-session",
  thread: undefined,
  client: { sessionId: "peer-session", label: "Laptop", access: "auto", linked: true },
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
};
const origin: LinkedMessageOrigin = {
  schemaVersion: "t3.linked-message-origin/v1",
  environmentId: "source",
  thread: { threadId: "receiver-local-id", title: "[Plan](bad) <script> & *go*\nnext" },
};
const header = encodeLinkedMessageOrigin(origin);
const rawHeader = (value: unknown) => Buffer.from(JSON.stringify(value)).toString("base64url");

describe("linked message origin", () => {
  it("binds peer assertions to the authenticated session without granting local thread authority", () => {
    const bound = withLinkedMessageOrigin(scope, header)!;
    expect(bound.thread).toBeUndefined();
    expect(bound.environmentId).toBe("receiver");
    expect(bound.requestNamespace).toBe(scope.requestNamespace);
    expect(bound.client?.access).toBe("auto");
    expect(bound.client?.remoteOrigin).toEqual({ ...origin, linkSessionId: "peer-session" });
    expect(bound.capabilities).toBe(scope.capabilities);
    expect(scope.client?.remoteOrigin).toBeUndefined();
  });

  it("preserves bounded Unicode and punctuation IDs and renders them as literal remote identities", () => {
    const unusualOrigin: LinkedMessageOrigin = {
      ...origin,
      environmentId: "机器 / source%3A1",
      thread: { threadId: "`[thread](https://example.invalid)<tag>``", title: "Source" },
    };
    const bound = withLinkedMessageOrigin(scope, encodeLinkedMessageOrigin(unusualOrigin));
    expect(bound?.client?.remoteOrigin).toEqual({
      ...unusualOrigin,
      linkSessionId: "peer-session",
    });
    expect(bound?.thread).toBeUndefined();
    expect(bound?.requestNamespace).toBe(scope.requestNamespace);
    expect(formatLinkedMessageOrigin(bound!.client!)).toBe(
      "Source (remote environment `机器 / source%3A1`, thread ``` `[thread](https://example.invalid)<tag>`` ```; via linked client Laptop)",
    );
  });

  it.each([" leading space", "trailing space ", "`", "````"])(
    "retains literal identity edges inside a safe Markdown code span (%s)",
    (threadId) => {
      const bound = withLinkedMessageOrigin(
        scope,
        encodeLinkedMessageOrigin({
          ...origin,
          thread: { threadId, title: "Source" },
        }),
      );
      expect(bound?.client?.remoteOrigin?.thread?.threadId).toBe(threadId);
      const fence = "`".repeat(threadId.includes("`") ? threadId.length + 1 : 1);
      expect(formatLinkedMessageOrigin(bound!.client!)).toContain(
        `thread ${fence} ${threadId} ${fence};`,
      );
    },
  );

  it("rejects forged metadata on provider and ordinary OAuth credentials", () => {
    expect(
      withLinkedMessageOrigin({ ...scope, client: { ...scope.client!, linked: false } }, header),
    ).toBeUndefined();
    expect(
      withLinkedMessageOrigin(
        {
          ...scope,
          client: undefined,
          thread: {
            threadId: ThreadId.make("local"),
            providerSessionId: "session",
            providerInstanceId: ProviderInstanceId.make("codex"),
          },
        },
        header,
      ),
    ).toBeUndefined();
  });

  it.each([
    "",
    "!invalid!",
    "a".repeat(4097),
    `${header}=`,
    `${header},${header}`,
    rawHeader({ ...origin, schemaVersion: "future" }),
    rawHeader({ ...origin, environmentId: "" }),
    rawHeader({ ...origin, environmentId: "x".repeat(257) }),
    rawHeader({ ...origin, environmentId: "source\ninjected" }),
    rawHeader({ ...origin, environmentId: "   " }),
    rawHeader({ ...origin, thread: { threadId: "x".repeat(257) } }),
    ...["\u0000", "\u007f", "\u0085", "\u202e", "\u2028", "\u2029", "\ud800"].map((control) =>
      rawHeader({ ...origin, thread: { threadId: `thread${control}id` } }),
    ),
    rawHeader({ ...origin, thread: { threadId: "local", title: "x".repeat(513) } }),
    rawHeader({ ...origin, access: "full-access" }),
    rawHeader({ ...origin, thread: { threadId: "local", providerSessionId: "forged" } }),
  ])("rejects malformed, oversized, unknown-version and excess-field metadata (%#)", (value) => {
    expect(withLinkedMessageOrigin(scope, value)).toBeUndefined();
  });

  it("keeps the whole-envelope bound for individually valid Unicode fields", () => {
    const oversized = encodeLinkedMessageOrigin({
      ...origin,
      environmentId: "源".repeat(256),
      thread: { threadId: "線".repeat(256), title: "題".repeat(512) },
    });
    expect(oversized.length).toBeGreaterThan(4096);
    expect(withLinkedMessageOrigin(scope, oversized)).toBeUndefined();
  });

  it("keeps old peers identifiable as linked callers when origin metadata is absent", () => {
    expect(withLinkedMessageOrigin(scope, undefined)).toBe(scope);
    expect(formatLinkedMessageOrigin(scope.client!)).toBe(
      "linked client Laptop (remote sender unavailable)",
    );
  });

  it("renders escaped remote attribution without a local thread link", () => {
    const bound = withLinkedMessageOrigin(scope, header)!;
    const label = formatLinkedMessageOrigin(bound.client!);
    expect(label).toBe(
      "\\[Plan\\]\\(bad\\) &lt;script&gt; &amp; \\*go\\* next (remote environment `source`, thread `receiver-local-id`; via linked client Laptop)",
    );
    expect(label).not.toContain("t3-thread://");
    expect(
      formatLinkedMessageOrigin({ ...bound.client!, sessionId: "another-session" }),
    ).not.toContain("receiver-local-id");
  });

  it("does not reuse another caller's per-request origin", () => {
    const first = withLinkedMessageOrigin(scope, header)!;
    const second = withLinkedMessageOrigin(
      scope,
      encodeLinkedMessageOrigin({ ...origin, thread: { threadId: "second" } }),
    )!;
    expect(first.client?.remoteOrigin?.thread?.threadId).toBe("receiver-local-id");
    expect(second.client?.remoteOrigin?.thread?.threadId).toBe("second");
    expect(withLinkedMessageOrigin(scope, undefined)?.client?.remoteOrigin).toBeUndefined();
  });
});

it.effect(
  "validates remote origin on the authenticated HTTP boundary for every credential type",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const seen: Array<McpInvocationScope> = [];
        const probe = Toolkit.make(
          Tool.make("origin_probe", {
            description: "Observe authenticated request context.",
            success: Schema.Struct({ ok: Schema.Boolean }),
            failure: OrchestratorMcpFailure,
            failureMode: "return",
            dependencies: [McpInvocationContext],
          }),
        );
        const providerScope = {
          ...scope,
          client: undefined,
          thread: {
            threadId: ThreadId.make("provider"),
            providerSessionId: "provider",
            providerInstanceId: ProviderInstanceId.make("codex"),
          },
        };
        const server = McpHttpServer.toolkitRegistration(
          probe,
          McpToolAccess.toLayer(probe, {
            origin_probe: McpToolAccess.reads(() =>
              McpInvocationContext.pipe(
                Effect.tap((value) => Effect.sync(() => seen.push(value))),
                Effect.as({ ok: true }),
              ),
            ),
          }),
        ).pipe(
          Layer.provideMerge(McpHttpServer.layerMcpTransport),
          Layer.provide(
            Layer.mock(McpSessionRegistry.McpSessionRegistry)({
              resolve: (token) => Effect.succeed(token === "provider" ? providerScope : undefined),
            }),
          ),
          Layer.provide(
            Layer.succeed(McpHttpServer.McpClientAuthenticator, {
              authenticate: (request) =>
                Effect.succeed(
                  request.headers.authorization === "Bearer linked.token"
                    ? scope
                    : request.headers.authorization === "Bearer ordinary.token"
                      ? { ...scope, client: { ...scope.client!, linked: false } }
                      : undefined,
                ),
            }),
          ),
        );
        yield* HttpRouter.serve(server, { disableListenLog: true, disableLogger: true }).pipe(
          Layer.build,
        );
        const http = yield* HttpClient.HttpClient;
        const call = (token: string, originHeader?: string) =>
          Effect.gen(function* () {
            const headers = {
              accept: "application/json, text/event-stream",
              authorization: `Bearer ${token}`,
            };
            const initialized = yield* http.post("/mcp", {
              headers,
              body: HttpBody.text(
                JSON.stringify({
                  jsonrpc: "2.0",
                  id: 1,
                  method: "initialize",
                  params: {
                    protocolVersion: "2025-06-18",
                    capabilities: {},
                    clientInfo: { name: "origin-test", version: "1" },
                  },
                }),
                "application/json",
              ),
            });
            yield* initialized.text;
            if (initialized.status !== 200) return initialized.status;
            const response = yield* http.post("/mcp", {
              headers: {
                ...headers,
                "mcp-session-id": initialized.headers["mcp-session-id"]!,
                "mcp-protocol-version": "2025-06-18",
                ...(originHeader === undefined
                  ? {}
                  : { [LINKED_MESSAGE_ORIGIN_HEADER]: originHeader }),
              },
              body: HttpBody.text(
                JSON.stringify({
                  jsonrpc: "2.0",
                  id: 2,
                  method: "tools/call",
                  params: { name: "origin_probe", arguments: {} },
                }),
                "application/json",
              ),
            });
            yield* response.text;
            return response.status;
          });
        expect(yield* call("provider", header)).toBe(400);
        expect(yield* call("ordinary.token", header)).toBe(400);
        expect(yield* call("linked.token", "bad!")).toBe(400);
        expect(seen).toHaveLength(0);
        expect(yield* call("linked.token", header)).toBe(200);
        expect(seen[0]?.client?.remoteOrigin).toEqual({ ...origin, linkSessionId: "peer-session" });
        expect(seen[0]?.thread).toBeUndefined();
        expect(yield* call("linked.token")).toBe(200);
        expect(seen[1]?.client?.linked).toBe(true);
        expect(seen[1]?.client?.remoteOrigin).toBeUndefined();
        expect(yield* call("unknown.token", header)).toBe(401);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);
