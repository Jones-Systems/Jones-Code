import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { HttpServer } from "effect/unstable/http";
import * as NetAddress from "effect/unstable/net/NetAddress";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as McpSessionRegistry from "./McpSessionRegistry.ts";
import type * as McpInvocationContext from "./McpInvocationContext.ts";

const environmentId = EnvironmentId.make("environment-1");
const makeFakeHttpServer = (hostname: string, port = 43123) =>
  HttpServer.HttpServer.of({
    address: NetAddress.inetAddressFromIpStringUnsafe(hostname, port),
    serve: (() => Effect.void) as HttpServer.HttpServer["Service"]["serve"],
  });
const fakeHttpServer = makeFakeHttpServer("127.0.0.1");
const fakeEnvironment = ServerEnvironment.ServerEnvironment.of({
  getEnvironmentId: Effect.succeed(environmentId),
  getDescriptor: Effect.die("unused"),
});

const makeRegistry = (now: () => number, httpServer = fakeHttpServer) =>
  McpSessionRegistry.__testing
    .make({
      now,
      livenessWindowMs: 100,
    })
    .pipe(
      Effect.provideService(HttpServer.HttpServer, httpServer),
      Effect.provideService(ServerEnvironment.ServerEnvironment, fakeEnvironment),
      Effect.provide(NodeServices.layer),
    );

it.effect("stores only a token hash, resolves the bearer token, and revokes by thread", () =>
  Effect.gen(function* () {
    let timestamp = 1_000;
    const registry = yield* makeRegistry(() => timestamp);
    const threadId = ThreadId.make("thread-1");
    const issued = yield* registry.issue({
      threadId,
      providerInstanceId: ProviderInstanceId.make("codex"),
      capabilities: new Set(["preview"]),
    });
    expect(issued.config.endpoint).toBe("http://127.0.0.1:43123/mcp");
    const token = issued.config.authorizationHeader.replace(/^Bearer\s+/, "");
    expect(token.length).toBeGreaterThan(20);

    const resolved = yield* registry.resolve(token);
    expect(resolved?.threadId).toBe(threadId);
    expect(resolved?.capabilities).toEqual(new Set(["preview"]));

    yield* registry.revokeThread(threadId);
    expect(yield* registry.resolve(token)).toBeUndefined();

    timestamp += 2_000;
  }),
);

it.effect("honors every explicit capability without amplifying the grant", () =>
  Effect.gen(function* () {
    const registry = yield* makeRegistry(() => 1_000);
    const withPreview = yield* registry.issue({
      threadId: ThreadId.make("thread-preview"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      capabilities: new Set(["preview"]),
    });
    const withoutPreview = yield* registry.issue({
      threadId: ThreadId.make("thread-no-preview"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      capabilities: new Set(),
    });
    const withDevice = yield* registry.issue({
      threadId: ThreadId.make("thread-device"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      capabilities: new Set(["device"]),
    });
    const withOrganization = yield* registry.issue({
      threadId: ThreadId.make("thread-organization"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      capabilities: new Set(["organization"]),
    });
    const withSnapshot = yield* registry.issue({
      threadId: ThreadId.make("thread-snapshot"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      capabilities: new Set(["decision-snapshot"]),
    });
    const capabilitiesOf = (issued: typeof withPreview) =>
      registry
        .resolve(issued.config.authorizationHeader.replace(/^Bearer\s+/, ""))
        .pipe(Effect.map((scope) => [...(scope?.capabilities ?? [])].sort()));

    expect(yield* capabilitiesOf(withPreview)).toEqual(["preview"]);
    expect(yield* capabilitiesOf(withoutPreview)).toEqual([]);
    expect(yield* capabilitiesOf(withDevice)).toEqual(["device"]);
    expect(yield* capabilitiesOf(withOrganization)).toEqual(["organization"]);
    expect(yield* capabilitiesOf(withSnapshot)).toEqual(["decision-snapshot"]);
    expect(withPreview.config.browserToolsAvailable).toBe(true);
    expect(withoutPreview.config.browserToolsAvailable).toBe(false);
    expect(withDevice.config.browserToolsAvailable).toBe(false);
  }),
);

it.effect("builds MCP endpoints from the bound server host", () =>
  Effect.gen(function* () {
    const cases = [
      ["100.64.0.40", "http://100.64.0.40:43123/mcp"],
      ["0.0.0.0", "http://127.0.0.1:43123/mcp"],
      ["::", "http://127.0.0.1:43123/mcp"],
      ["::1", "http://[::1]:43123/mcp"],
      ["127.0.0.1", "http://127.0.0.1:43123/mcp"],
    ] as const;

    for (const [hostname, expectedEndpoint] of cases) {
      const registry = yield* makeRegistry(() => 1_000, makeFakeHttpServer(hostname));
      const issued = yield* registry.issue({
        threadId: ThreadId.make(`thread-${hostname}`),
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: new Set(["preview"]),
      });
      expect(issued.config.endpoint).toBe(expectedEndpoint);
    }
  }),
);

it.effect("expires credentials once their session stops showing signs of life", () =>
  Effect.gen(function* () {
    let timestamp = 1_000;
    const registry = yield* makeRegistry(() => timestamp);
    const issued = yield* registry.issue({
      threadId: ThreadId.make("thread-2"),
      providerInstanceId: ProviderInstanceId.make("claude"),
      capabilities: new Set(["preview"]),
    });
    const token = issued.config.authorizationHeader.replace(/^Bearer\s+/, "");
    timestamp += 101;
    expect(yield* registry.resolve(token)).toBeUndefined();
  }),
);

it.effect("keeps a credential alive across turns that never touch an MCP tool", () =>
  Effect.gen(function* () {
    let timestamp = 1_000;
    const registry = yield* makeRegistry(() => timestamp);
    const threadId = ThreadId.make("thread-3");
    const issued = yield* registry.issue({
      threadId,
      providerInstanceId: ProviderInstanceId.make("claude"),
      capabilities: new Set(["preview"]),
    });
    const token = issued.config.authorizationHeader.replace(/^Bearer\s+/, "");

    // Well past the liveness window in total, but each turn reports in before
    // it lapses — this is the long-session case that used to lose the toolkit.
    for (let turn = 0; turn < 10; turn += 1) {
      timestamp += 99;
      yield* registry.touch(threadId);
    }

    expect((yield* registry.resolve(token))?.threadId).toBe(threadId);
  }),
);

it.effect("does not keep credentials of other threads alive", () =>
  Effect.gen(function* () {
    let timestamp = 1_000;
    const registry = yield* makeRegistry(() => timestamp);
    const issued = yield* registry.issue({
      threadId: ThreadId.make("thread-4"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      capabilities: new Set(["preview"]),
    });
    const token = issued.config.authorizationHeader.replace(/^Bearer\s+/, "");

    timestamp += 99;
    yield* registry.touch(ThreadId.make("thread-unrelated"));
    timestamp += 2;

    expect(yield* registry.resolve(token)).toBeUndefined();
  }),
);

it.effect(
  "rejects a browser flag that contradicts the credential grant before issuing a token",
  () =>
    Effect.gen(function* () {
      const registry = yield* makeRegistry(() => 1_000);
      for (const [browserToolsAvailable, capabilities] of [
        [false, new Set<McpInvocationContext.McpCapability>(["preview"])],
        [true, new Set<McpInvocationContext.McpCapability>()],
      ] as const) {
        const result = yield* registry
          .issue({
            threadId: ThreadId.make("thread-contradiction"),
            providerInstanceId: ProviderInstanceId.make("codex"),
            browserToolsAvailable,
            capabilities,
          })
          .pipe(Effect.exit);
        expect(result._tag).toBe("Failure");
      }
    }),
);

it.effect("grants orchestration and worktree only when explicitly requested", () =>
  Effect.gen(function* () {
    const registry = yield* makeRegistry(() => 1_000);
    const issued = yield* registry.issue({
      threadId: ThreadId.make("thread-ordinary"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      capabilities: new Set(["orchestration", "worktree", "pull-requests"]),
    });
    const resolved = yield* registry.resolve(
      issued.config.authorizationHeader.replace(/^Bearer\s+/, ""),
    );
    expect(resolved?.capabilities).toEqual(new Set(["orchestration", "worktree", "pull-requests"]));
    expect(issued.config.browserToolsAvailable).toBe(false);
  }),
);

it.effect("does not revoke an active credential for a contradictory replacement request", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const registry = yield* McpSessionRegistry.McpSessionRegistry;
      const request = {
        threadId: ThreadId.make("thread-active-grant"),
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: new Set<McpInvocationContext.McpCapability>(["orchestration"]),
        browserToolsAvailable: false,
      };
      const issued = yield* McpSessionRegistry.issueActiveMcpCredential(request);
      expect(issued).toBeDefined();
      const rejected = yield* McpSessionRegistry.issueActiveMcpCredential({
        ...request,
        browserToolsAvailable: true,
      }).pipe(Effect.exit);
      expect(rejected._tag).toBe("Failure");
      const token = issued!.config.authorizationHeader.replace(/^Bearer\s+/, "");
      expect((yield* registry.resolve(token))?.providerSessionId).toBe(
        issued!.config.providerSessionId,
      );
    }).pipe(
      Effect.provide(McpSessionRegistry.layer),
      Effect.provideService(HttpServer.HttpServer, fakeHttpServer),
      Effect.provideService(ServerEnvironment.ServerEnvironment, fakeEnvironment),
      Effect.provide(NodeServices.layer),
    ),
  ),
);
