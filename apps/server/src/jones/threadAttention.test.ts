import {
  AuthSessionId,
  AuthOrchestrationReadScope,
  AuthOrchestrationOperateScope,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentOrchestrationHttpApi,
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  ORCHESTRATION_PROTOCOL_HEADER,
  ORCHESTRATION_PROTOCOL_VERSION_TEXT,
  type AuthEnvironmentScope,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServer from "effect/http/HttpServer";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import { ThreadAttention } from "@t3tools/contracts/jones/threadAttention";
import { failEnvironmentAuthInvalid } from "../auth/http.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as OrchestrationHttpApi from "../orchestration-v2/http.ts";
import { EventSinkV2 } from "../orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as ProjectEnrichment from "../project/ProjectEnrichmentService.ts";
import * as ApplicationEvents from "../persistence/OrchestrationEventStore.ts";
import { projectThreadAttention } from "./threadAttention.ts";

const decodeAttention = Schema.decodeUnknownSync(ThreadAttention);
const now = DateTime.makeUnsafe("2026-10-18T12:00:00.000Z");
function shell(overrides: Partial<OrchestrationV2ThreadShell> = {}): OrchestrationV2ThreadShell {
  return {
    id: ThreadId.make("parent"),
    projectId: ProjectId.make("project"),
    title: "Thread",
    providerInstanceId: ProviderInstanceId.make("codex"),
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "test-model" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: {
      rootThreadId: ThreadId.make("parent"),
      parentThreadId: null,
      relationshipToParent: null,
    },
    forkedFrom: null,
    createdBy: "user",
    creationSource: "web",
    latestRunId: null,
    activeRunId: null,
    status: "idle",
    pendingRuntimeRequest: null,
    latestVisibleMessage: null,
    latestUserMessageAt: null,
    hasActionableProposedPlan: false,
    itemCount: 0,
    visibleItemCount: 0,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
    ...overrides,
  };
}
const selected = shell({
  latestRunId: RunId.make("run"),
  status: "running",
  pendingRuntimeRequest: {
    id: RuntimeRequestId.make("own-auth"),
    kind: "auth_refresh",
    createdAt: now,
  },
});
const child = shell({
  id: ThreadId.make("child"),
  status: "running",
  creationSource: "mcp",
  lineage: {
    rootThreadId: selected.id,
    parentThreadId: selected.id,
    relationshipToParent: "subagent",
  },
  pendingRuntimeRequest: {
    id: RuntimeRequestId.make("child-request"),
    kind: "permission",
    createdAt: now,
  },
});
function fixture(scopes: AuthEnvironmentScope[] = [AuthOrchestrationReadScope]) {
  let reads = 0;
  const auth = Layer.succeed(EnvironmentAuthenticatedAuth, (httpEffect) =>
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      if (request.headers.authorization !== "Bearer fixture")
        return yield* failEnvironmentAuthInvalid("missing_credential");
      return yield* httpEffect.pipe(
        Effect.provideService(EnvironmentAuthenticatedPrincipal, {
          sessionId: AuthSessionId.make("attention-fixture"),
          subject: "fixture",
          method: "bearer-access-token",
          scopes: new Set(scopes),
        }),
      );
    }),
  );
  const group = OrchestrationHttpApi.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ProjectionStore.ProjectionStoreV2)({}),
        Layer.succeed(ServerEnvironment.ServerEnvironmentIdentity, {
          getEnvironmentId: Effect.succeed(EnvironmentId.make("attention-test")),
        }),
        Layer.mock(EventSinkV2)({}),
        SqlitePersistence.layerMemory,
        Layer.mock(ThreadManagement.ThreadManagementService)({
          readShellSnapshot: () =>
            Effect.sync(() => {
              reads++;
              return Effect.succeed({
                schemaVersion: 1,
                snapshotSequence: 7,
                threads: [selected, child],
                archivedThreads: [],
              });
            }),
        }),
        Layer.mock(ProjectStore.ProjectStoreV2)({}),
        Layer.mock(ProjectEnrichment.ProjectEnrichmentService)({}),
        Layer.mock(ApplicationEvents.OrchestrationEventStore)({
          latestApplicationSequence: Effect.succeed(42),
        }),
      ),
    ),
  );
  const routes = HttpApiBuilder.layer(
    HttpApi.make("environment").add(EnvironmentOrchestrationHttpApi),
  ).pipe(Layer.provide(group), Layer.provide(auth), Layer.provide(HttpServer.layerServices));
  return { ...HttpRouter.toWebHandler(routes, { disableLogger: true }), reads: () => reads };
}
function request(threadId: string, authenticated = true) {
  return new Request(`http://test/api/orchestration/threads/${threadId}/attention`, {
    headers: {
      [ORCHESTRATION_PROTOCOL_HEADER]: ORCHESTRATION_PROTOCOL_VERSION_TEXT,
      ...(authenticated ? { authorization: "Bearer fixture" } : {}),
    },
  });
}

describe("exact thread attention read boundary", () => {
  it("requires authentication and read scope before loading the shell", async () => {
    for (const scopes of [[], [AuthOrchestrationOperateScope]] as AuthEnvironmentScope[][]) {
      const f = fixture(scopes);
      try {
        expect((await f.handler(request(selected.id, false), Context.empty())).status).toBe(401);
        expect((await f.handler(request(selected.id), Context.empty())).status).toBe(403);
        expect(f.reads()).toBe(0);
      } finally {
        await f.dispose();
      }
    }
  });
  it("returns only the selected attention with child signal and distinct own request identity", async () => {
    const f = fixture();
    try {
      const response = await f.handler(request(selected.id), Context.empty());
      expect(response.status).toBe(200);
      const result = decodeAttention(await response.json());
      expect(result).toEqual({
        schemaVersion: "t3.thread-attention/v1",
        threadId: selected.id,
        snapshotSequence: 42,
        status: "approval",
        pill: "Pending Approval",
        unseenCompletion: false,
        pendingChildKind: "approval",
        ownRequest: { id: "own-auth", kind: "auth_refresh", createdAt: DateTime.formatIso(now) },
        coverage: {
          source: "shell",
          childRequests: "active-shell",
          latestVisibleMessage: "unavailable",
          requestVersion: "unavailable",
          requestDetails: "unavailable",
          threadSequence: "unavailable",
          lastVisitedAt: "available",
        },
      });
      expect(JSON.stringify(result)).not.toContain("child-request");
      expect((await f.handler(request("missing"), Context.empty())).status).toBe(404);
    } finally {
      await f.dispose();
    }
  });
  it("does not leak titles, bodies or private request details from shell rows", () => {
    const thread = {
      ...selected,
      title: "private title",
      lastError: "private error",
      pendingRuntimeRequest: {
        ...selected.pendingRuntimeRequest!,
        kind: "user_input" as const,
        questions: ["secret value"],
      },
    };
    const result = projectThreadAttention({
      threadId: thread.id,
      threads: [thread],
      snapshotSequence: 9,
    });
    expect(result?.ownRequest?.kind).toBe("user_input");
    expect(result?.coverage.requestDetails).toBe("unavailable");
    expect(JSON.stringify(result)).not.toMatch(/private|secret|questions/);
    const { lastVisitedAt: _visit, ...older } = thread;
    expect(
      projectThreadAttention({ threadId: older.id, threads: [older], snapshotSequence: 9 })
        ?.coverage.lastVisitedAt,
    ).toBe("unavailable");
  });
});
