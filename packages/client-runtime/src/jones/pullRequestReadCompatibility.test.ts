import { RpcPermissionGuard } from "../rpc/client.ts";
import { vi } from "vite-plus/test";
import {
  EnvironmentId,
  AuthSourceControlWriteScope,
  ProjectId,
  PullRequestOperationError,
  WS_METHODS,
  type AuthSessionState,
} from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";
import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  type PreparedConnection,
  type SupervisorConnectionState,
} from "../connection/model.ts";
import * as EnvironmentRegistry from "../connection/registry.ts";
import { type ConnectionCatalogEntry } from "../connection/catalog.ts";
import * as EnvironmentSupervisor from "../connection/supervisor.ts";
import { EnvironmentRpcUnavailableError } from "../rpc/client.ts";
import type { WsRpcProtocolClient } from "../rpc/protocol.ts";
import type { RpcSession } from "../rpc/session.ts";
import { createPullRequestEnvironmentAtoms } from "../state/pullRequests.ts";
import * as PullRequestDiffLoader from "../state/pullRequestDiffHttp.ts";
import { createPullRequestRouter } from "../state/pullRequestRouting.ts";
import { GitHubRoutingPermissions } from "../connection/githubRoutingPermissions.ts";

const trustedRouting = {
  get: () => Effect.succeed("read-write" as const),
  changes: Stream.empty,
  set: () => Effect.void,
  forget: () => Effect.void,
};

const TARGET = new PrimaryConnectionTarget({
  environmentId: EnvironmentId.make("environment-1"),
  label: "Test environment",
  httpBaseUrl: "https://environment.example.test",
  wsBaseUrl: "wss://environment.example.test",
});

function session(client: WsRpcProtocolClient): RpcSession {
  return {
    client: {
      ...client,
      [WS_METHODS.pullRequestsInvalidate]:
        client[WS_METHODS.pullRequestsInvalidate] ?? (() => Effect.void),
    },
    initialConfig: client[WS_METHODS.serverGetConfig]?.({}).pipe(Effect.orDie) ?? Effect.never,
    subscribeServerConfig: (input) => client.subscribeServerConfig(input),
    ready: Effect.void,
    probe: Effect.void,
    closed: Effect.never,
  };
}

const makeTestRuntime = Effect.fn("makeTestRuntime")(function* (
  client: WsRpcProtocolClient,
  localClient?: WsRpcProtocolClient,
  localOrigin = false,
) {
  const originTarget = localOrigin
    ? new PrimaryConnectionTarget({
        ...TARGET,
        httpBaseUrl: "http://localhost:3774",
        wsBaseUrl: "ws://localhost:3774",
      })
    : TARGET;
  const connectionState: SupervisorConnectionState = {
    ...AVAILABLE_CONNECTION_STATE,
    desired: true,
    network: "online",
    phase: "connected",
    attempt: 1,
    generation: 1,
  };
  const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
    target: originTarget,
    state: yield* SubscriptionRef.make(connectionState),
    session: yield* SubscriptionRef.make(Option.some(session(client))),
    prepared: yield* SubscriptionRef.make(Option.none<PreparedConnection>()),
    connect: Effect.void,
    disconnect: Effect.void,
    retryNow: Effect.void,
  } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
  const localTarget = new PrimaryConnectionTarget({
    environmentId: EnvironmentId.make("local-environment"),
    label: "Local environment",
    httpBaseUrl: "http://localhost:3773",
    wsBaseUrl: "ws://localhost:3773",
  });
  const localSupervisor = {
    ...supervisor,
    target: localTarget,
    session: yield* SubscriptionRef.make(Option.some(session(localClient ?? client))),
  };
  const environmentRegistry = EnvironmentRegistry.EnvironmentRegistry.of({
    entries: yield* SubscriptionRef.make<ReadonlyMap<EnvironmentId, ConnectionCatalogEntry>>(
      new Map([
        [
          originTarget.environmentId,
          { target: originTarget, profile: Option.none(), enabled: true },
        ],
        ...(localClient === undefined
          ? []
          : [
              [
                localTarget.environmentId,
                { target: localTarget, profile: Option.none(), enabled: true },
              ] as const,
            ]),
      ]),
    ),
    run: (environmentId, effect) =>
      Effect.provideService(
        effect,
        EnvironmentSupervisor.EnvironmentSupervisor,
        environmentId === localTarget.environmentId ? localSupervisor : supervisor,
      ),
    runStream: (_environmentId, stream) =>
      Stream.provideService(stream, EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
    followStream: (_environmentId, stream) =>
      Stream.provideService(stream, EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
  } as EnvironmentRegistry.EnvironmentRegistry["Service"]);
  const runtime = Atom.runtime(
    Layer.merge(
      Layer.succeed(EnvironmentRegistry.EnvironmentRegistry, environmentRegistry),
      Layer.succeed(
        PullRequestDiffLoader.PullRequestDiffLoader,
        PullRequestDiffLoader.PullRequestDiffLoader.of({ load: () => Effect.die("unused") }),
      ),
    ),
  );
  const atoms = createPullRequestEnvironmentAtoms(runtime);
  const registry = yield* Effect.acquireRelease(Effect.sync(AtomRegistry.make), (registry) =>
    Effect.sync(() => registry.dispose()),
  );
  return { runtime, atoms, registry, environmentRegistry, supervisor, localSupervisor };
});

it.effect.each(
  [WS_METHODS.pullRequestsSummary, WS_METHODS.pullRequestsDetail].flatMap((tag) =>
    (
      [
        "local source pause",
        "alternate pause",
        "final source pause",
        "origin identity pause",
        "alternate identity pause",
        "transport fallback",
        "legacy error fallback",
      ] as const
    ).map((scenario) => ({ tag, scenario })),
  ),
)("$tag respects $scenario without extra reads", ({ tag, scenario }) =>
  Effect.scoped(
    Effect.gen(function* () {
      const calls: string[] = [];
      const pause = new PullRequestOperationError({
        operation: "detail",
        detail: "Provider reads are paused",
        reason: "rate-limited",
        retryAt: 123456,
      });
      const transport = new EnvironmentRpcUnavailableError({
        environmentId: EnvironmentId.make("test-environment"),
        message: "Connection unavailable",
      });
      const legacy = new PullRequestOperationError({
        operation: "detail",
        detail: "rate limit reached",
      });
      const fallback = scenario === "transport fallback" || scenario === "legacy error fallback";
      const clientFor = (alternate: boolean) => {
        const name = alternate ? "alternate" : "origin";
        return {
          [alternate ? WS_METHODS.pullRequestsRoutingIdentity : WS_METHODS.pullRequestsRouting]:
            () =>
              Effect.gen(function* () {
                calls.push(`${name}:identity`);
                if (scenario === `${name} identity pause`) return yield* pause;
                return {
                  host: "github.com",
                  provider: "github",
                  viewer: "synthetic-viewer",
                  accountId: "123",
                };
              }),
          [tag]: (input: { allowStale?: boolean }) =>
            Effect.gen(function* () {
              calls.push(`${name}:${input.allowStale === false ? "strict" : "ordinary"}`);
              if (fallback) {
                if (alternate) return null;
                return yield* scenario === "transport fallback" ? transport : legacy;
              }
              if (scenario === "final source pause" && alternate) return yield* transport;
              return yield* pause;
            }),
        } as unknown as WsRpcProtocolClient;
      };
      const { environmentRegistry, supervisor } = yield* makeTestRuntime(
        clientFor(false),
        clientFor(true),
        scenario === "local source pause" || fallback,
      );
      const result = yield* createPullRequestRouter()(tag, {
        projectId: ProjectId.make("project-1"),
        repository: "acme/web",
        number: 7,
      }).pipe(
        Effect.provideService(EnvironmentRegistry.EnvironmentRegistry, environmentRegistry),
        Effect.provideService(GitHubRoutingPermissions, trustedRouting),
        Effect.provideService(RpcPermissionGuard, { authorize: () => Effect.void }),
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.result,
      );
      const expected =
        scenario === "local source pause"
          ? ["origin:strict"]
          : scenario === "origin identity pause"
            ? ["origin:identity"]
            : scenario === "alternate identity pause"
              ? ["origin:identity", "alternate:identity"]
              : scenario === "final source pause"
                ? ["origin:identity", "alternate:identity", "alternate:strict", "origin:strict"]
                : fallback
                  ? ["origin:strict", "origin:identity", "alternate:identity", "alternate:strict"]
                  : ["origin:identity", "alternate:identity", "alternate:strict"];
      expect(calls).toEqual(expected);
      if (fallback) {
        expect(Result.isSuccess(result)).toBe(true);
        if (Result.isSuccess(result)) expect(result.success).toBeNull();
      } else {
        expect(Result.isFailure(result)).toBe(true);
        if (Result.isFailure(result)) {
          const error = result.failure;
          const isOperationError = Schema.is(PullRequestOperationError);
          expect(isOperationError(error)).toBe(true);
          if (isOperationError(error)) {
            expect(error.reason).toBe(pause.reason);
            expect(error.retryAt).toBe(pause.retryAt);
            expect(error.detail).toBe(pause.detail);
          }
        }
      }
    }),
  ),
);

vi.mock("../state/session.ts", () => ({
  createEnvironmentSessionAtoms: () => ({ sessionStateAtom: grantedSessions }),
}));
const grantedSessions = Atom.family((_id: EnvironmentId) =>
  Atom.make<AsyncResult.AsyncResult<AuthSessionState>>(
    AsyncResult.success({
      authenticated: true,
      auth: {
        policy: "remote-reachable" as const,
        bootstrapMethods: [],
        sessionMethods: [],
        sessionCookieName: "test",
      },
      scopes: [AuthSourceControlWriteScope],
    }),
  ),
);

it.effect("invalidates recorded readers after an outcome-unknown write without retrying it", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const calls: string[] = [];
      const failure = new PullRequestOperationError({
        operation: "runAction",
        detail: "Connection lost after dispatch",
      });
      const clientFor = (alternate: boolean) => {
        const name = alternate ? "alternate" : "origin";
        return {
          [alternate ? WS_METHODS.pullRequestsRoutingIdentity : WS_METHODS.pullRequestsRouting]:
            () => Effect.succeed({ host: "github.com", provider: "github", accountId: "123" }),
          [WS_METHODS.pullRequestsSummary]: () => Effect.succeed(null),
          [WS_METHODS.pullRequestsRunAction]: () =>
            Effect.sync(() => calls.push(`${name}:write`)).pipe(Effect.andThen(failure)),
          [WS_METHODS.pullRequestsInvalidate]: () =>
            Effect.sync(() => {
              calls.push(`${name}:invalidate`);
            }),
        } as unknown as WsRpcProtocolClient;
      };
      const { environmentRegistry, supervisor } = yield* makeTestRuntime(
        clientFor(false),
        clientFor(true),
      );
      const router = createPullRequestRouter();
      const reference = {
        projectId: ProjectId.make("project-unknown-write"),
        repository: "acme/web",
        number: 7,
      };
      yield* Effect.gen(function* () {
        yield* router(WS_METHODS.pullRequestsSummary, reference);
        expect(
          yield* router(WS_METHODS.pullRequestsRunAction, {
            ...reference,
            action: "close",
          }).pipe(Effect.flip),
        ).toBe(failure);
        expect(calls.filter((call) => call.endsWith(":write"))).toEqual(["alternate:write"]);
        expect(calls).toContain("origin:invalidate");
        expect(calls).toContain("alternate:invalidate");
      }).pipe(
        Effect.provideService(EnvironmentRegistry.EnvironmentRegistry, environmentRegistry),
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.provideService(GitHubRoutingPermissions, trustedRouting),
        Effect.provideService(RpcPermissionGuard, { authorize: () => Effect.void }),
      );
    }),
  ),
);
