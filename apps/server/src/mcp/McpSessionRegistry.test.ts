import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Layer from "effect/Layer";
import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  McpCapabilityUnavailableError,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { HttpServer } from "effect/http";
import * as NetAddress from "effect/net/NetAddress";
import * as TestClock from "effect/testing/TestClock";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as McpSessionRegistry from "./McpSessionRegistry.ts";
import * as McpInvocationContext from "./McpInvocationContext.ts";
import {
  CollectorFailure,
  DecisionSnapshotCollector,
} from "../jones/mcp/decisionSnapshot/collector.ts";
import {
  DecisionSnapshotNativeCounts,
  DecisionSnapshotToolkitHandlersLive,
  NativeCountReadError,
} from "../jones/mcp/decisionSnapshot/handlers.ts";
import { DecisionSnapshotToolkit } from "../jones/mcp/decisionSnapshot/tools.ts";

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

// Without `now`, the registry reads the Effect Clock (TestClock in these tests).
const makeRegistry = (now: (() => number) | undefined, httpServer = fakeHttpServer) =>
  McpSessionRegistry.__testing
    .make({
      ...(now ? { now } : {}),
      livenessWindowMs: 100,
    })
    .pipe(
      Effect.provideService(HttpServer.HttpServer, httpServer),
      Effect.provideService(ServerEnvironment.ServerEnvironment, fakeEnvironment),
      Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
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
    expect(resolved?.thread.threadId).toBe(threadId);
    expect(resolved?.capabilities).toEqual(
      new Set(["preview", "orchestration", "worktree", "pull-requests", "issues"]),
    );

    yield* registry.revokeThread(threadId);
    expect(yield* registry.resolve(token)).toBeUndefined();

    timestamp += 2_000;
  }),
);

it.effect("always grants pull-requests and gates browser and device access independently", () =>
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
    const capabilitiesOf = (issued: typeof withPreview) =>
      registry
        .resolve(issued.config.authorizationHeader.replace(/^Bearer\s+/, ""))
        .pipe(Effect.map((scope) => [...(scope?.capabilities ?? [])].sort()));

    expect(yield* capabilitiesOf(withPreview)).toEqual([
      "issues",
      "orchestration",
      "preview",
      "pull-requests",
      "worktree",
    ]);
    expect(yield* capabilitiesOf(withoutPreview)).toEqual([
      "issues",
      "orchestration",
      "pull-requests",
      "worktree",
    ]);
    expect(yield* capabilitiesOf(withDevice)).toEqual([
      "device",
      "issues",
      "orchestration",
      "pull-requests",
      "worktree",
    ]);
  }),
);

// regression: #3114 use bound host for MCP endpoint
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
    // Once a request found it expired, clocks moving back don't bring it back; a provider turn does.
    timestamp = 1_000;
    expect(yield* registry.resolve(token)).toBeUndefined();
    yield* registry.touch(ThreadId.make("thread-2"));
    expect((yield* registry.resolve(token))?.thread.threadId).toBe("thread-2");
  }),
);

it.effect("a backward wall clock step neither extends nor revives credentials", () =>
  Effect.gen(function* () {
    const registry = yield* makeRegistry(undefined);
    const issueToken = (threadId: string) =>
      registry
        .issue({
          threadId: ThreadId.make(threadId),
          providerInstanceId: ProviderInstanceId.make("codex"),
          capabilities: new Set(["preview"]),
        })
        .pipe(Effect.map((issued) => issued.config.authorizationHeader.replace(/^Bearer\s+/, "")));

    yield* TestClock.setTime(10_000);
    const extended = yield* issueToken("thread-extended");
    // setTime backwards moves wall time only; adjust moves both clocks.
    yield* TestClock.setTime(1_000);
    yield* TestClock.adjust(101);
    expect(yield* registry.resolve(extended)).toBeUndefined();

    const revived = yield* issueToken("thread-revived");
    yield* TestClock.adjust(101);
    expect(yield* registry.resolve(revived)).toBeUndefined();
    const active = yield* issueToken("thread-active");
    yield* TestClock.setTime(0);
    expect(yield* registry.resolve(revived)).toBeUndefined();
    expect((yield* registry.resolve(active))?.thread.threadId).toBe("thread-active");
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

it.effect("only a trusted touch restores an expired token with its original scope", () =>
  Effect.gen(function* () {
    let timestamp = 1_000;
    const registry = yield* makeRegistry(() => timestamp);
    const threadId = ThreadId.make("thread-idle");
    const issued = yield* registry.issue({
      threadId,
      providerInstanceId: ProviderInstanceId.make("claude"),
      capabilities: new Set(["preview"]),
    });
    const token = issued.config.authorizationHeader.replace(/^Bearer\s+/, "");
    const scope = yield* registry.resolve(token);
    expect(scope).toBeDefined();

    timestamp += 101;
    expect(yield* registry.resolve(token)).toBeUndefined();
    const otherThreadId = ThreadId.make("thread-active");
    const other = yield* registry.issue({
      threadId: otherThreadId,
      providerInstanceId: ProviderInstanceId.make("codex"),
      capabilities: new Set(["device"]),
    });
    yield* registry.touch(otherThreadId);
    expect(
      (yield* registry.resolve(other.config.authorizationHeader.replace(/^Bearer\s+/, "")))?.thread
        .threadId,
    ).toBe(otherThreadId);
    expect(yield* registry.resolve("unknown-token")).toBeUndefined();
    expect(yield* registry.resolve(token)).toBeUndefined();

    yield* registry.touch(threadId);
    expect(yield* registry.resolve(token)).toEqual(scope);
    timestamp += 101;
    expect(yield* registry.resolve(token)).toBeUndefined();
  }),
);

it.effect.each(["provider-session", "thread", "all"] as const)(
  "never restores an expired token after %s revocation",
  (revocation) =>
    Effect.gen(function* () {
      let timestamp = 1_000;
      const registry = yield* makeRegistry(() => timestamp);
      const threadId = ThreadId.make("thread-revoked");
      const issued = yield* registry.issue({
        threadId,
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: new Set(["preview"]),
      });
      const token = issued.config.authorizationHeader.replace(/^Bearer\s+/, "");

      timestamp += 101;
      yield* {
        "provider-session": registry.revokeProviderSession(issued.config.providerSessionId),
        thread: registry.revokeThread(threadId),
        all: registry.revokeAll,
      }[revocation];
      yield* registry.touch(threadId);
      expect(yield* registry.resolve(token)).toBeUndefined();
    }),
);

it.effect(
  "denies default snapshot credentials before reads and collects only after explicit issuance",
  () =>
    Effect.gen(function* () {
      const registry = yield* makeRegistry(() => 1_000);
      const request = {
        threadId: ThreadId.make("thread-focused-snapshot"),
        providerInstanceId: ProviderInstanceId.make("codex"),
      };
      const ordinary = yield* registry.issue(request);
      const explicit = yield* registry.issue({
        ...request,
        capabilities: new Set(["decision-snapshot"]),
      });
      const scopeOf = (credential: typeof ordinary) =>
        registry.resolve(credential.config.authorizationHeader.replace(/^Bearer\s+/, ""));
      const ordinaryScope = yield* scopeOf(ordinary);
      const explicitScope = yield* scopeOf(explicit);
      expect(ordinaryScope).toBeDefined();
      expect(explicitScope).toBeDefined();
      expect(ordinaryScope!.capabilities).toEqual(
        new Set(["orchestration", "worktree", "pull-requests", "preview"]),
      );
      expect(explicitScope!.capabilities).toEqual(
        new Set(["orchestration", "worktree", "pull-requests", "decision-snapshot"]),
      );
      let reads = 0;
      let launches = 0;
      const dependencies = Layer.mergeAll(
        Layer.succeed(DecisionSnapshotNativeCounts, {
          readOperatingCounts: () => {
            reads++;
            return Effect.fail(new NativeCountReadError({ source: "threads" }));
          },
        }),
        Layer.succeed(DecisionSnapshotCollector, {
          collect: () => {
            launches++;
            return Effect.fail(new CollectorFailure("runtime_unavailable"));
          },
        }),
      );
      const toolkit = yield* DecisionSnapshotToolkit.pipe(
        Effect.provide(DecisionSnapshotToolkitHandlersLive.pipe(Layer.provide(dependencies))),
      );
      const call = (scope: McpInvocationContext.McpInvocationScope) =>
        toolkit.handle("decision_snapshot", {}).pipe(
          Stream.unwrap,
          Stream.runCollect,
          Effect.map((results) => results.at(-1)!.result),
          Effect.provideService(McpInvocationContext.McpInvocationContext, scope),
          Effect.provide(dependencies),
        );
      const denied = yield* call(ordinaryScope!).pipe(Effect.flip);
      expect(denied).toBeInstanceOf(McpCapabilityUnavailableError);
      expect(denied).toMatchObject({ capability: "decision-snapshot", threadId: request.threadId });
      expect(reads).toBe(0);
      expect(launches).toBe(0);
      expect(yield* call(explicitScope!)).toMatchObject({
        schema: "codex.decision-snapshot/v1",
        coverage: "unavailable",
        authority_effect: "none",
        sources: {
          threads: { status: "unavailable" },
          workstreams: { status: "unavailable" },
          host: { status: "unavailable", reason: "runtime_unavailable" },
        },
      });
      expect(reads).toBe(1);
      expect(launches).toBe(1);
    }),
);

it.effect("recorded recovery credential lookup is non-secret and never renews liveness", () =>
  Effect.gen(function* () {
    let timestamp = 1000;
    const registry = yield* makeRegistry(() => timestamp);
    const issued = yield* registry.issue({
      threadId: ThreadId.make("recorded-recovery-agent"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      capabilities: new Set(["orchestration"]),
    });
    const id = issued.config.providerSessionId;
    const first = yield* registry.lookupByCredentialId(id);
    expect(first?.thread.providerSessionId).toBe(id);
    expect(JSON.stringify(first)).not.toContain(issued.config.authorizationHeader);
    expect(Object.keys(first!)).not.toContain("tokenHash");
    timestamp += 99;
    expect(yield* registry.lookupByCredentialId(id)).toEqual(first);
    timestamp += 2;
    expect(yield* registry.lookupByCredentialId(id)).toBeUndefined();
    expect(yield* registry.lookupByCredentialId("unknown")).toBeUndefined();
    yield* registry.touch(ThreadId.make("recorded-recovery-agent"));
    expect(yield* registry.lookupByCredentialId(id)).toEqual(first);
    yield* registry.revokeProviderSession(id);
    expect(yield* registry.lookupByCredentialId(id)).toBeUndefined();
  }),
);

it.effect("credential lookup follows both clocks and cannot revive an observed expiry", () =>
  Effect.gen(function* () {
    const registry = yield* makeRegistry(undefined);
    const threadId = ThreadId.make("lookup-dual-clock");
    yield* TestClock.setTime(10_000);
    const issued = yield* registry.issue({
      threadId,
      providerInstanceId: ProviderInstanceId.make("codex"),
    });
    const id = issued.config.providerSessionId;
    const original = yield* registry.lookupByCredentialId(id);
    yield* TestClock.setTime(1_000);
    yield* TestClock.adjust(101);
    expect(yield* registry.lookupByCredentialId(id)).toBeUndefined();
    yield* TestClock.setTime(0);
    expect(yield* registry.lookupByCredentialId(id)).toBeUndefined();
    yield* registry.touch(threadId);
    expect(yield* registry.lookupByCredentialId(id)).toEqual(original);
  }),
);

it.effect(
  "a token expired by authentication stays unavailable to non-renewing credential lookup",
  () =>
    Effect.gen(function* () {
      let timestamp = 1_000;
      const registry = yield* makeRegistry(() => timestamp);
      const issued = yield* registry.issue({
        threadId: ThreadId.make("lookup-after-expiry"),
        providerInstanceId: ProviderInstanceId.make("codex"),
      });
      const id = issued.config.providerSessionId;
      timestamp += 101;
      expect(
        yield* registry.resolve(issued.config.authorizationHeader.replace(/^Bearer\s+/, "")),
      ).toBeUndefined();
      timestamp = 1_000;
      expect(yield* registry.lookupByCredentialId(id)).toBeUndefined();
      yield* registry.revokeProviderSession(id);
      yield* registry.touch(issued.config.threadId);
      expect(yield* registry.lookupByCredentialId(id)).toBeUndefined();
    }),
);
