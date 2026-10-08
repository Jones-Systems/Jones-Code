import * as OrchestrationHttpApi from "./http.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { EventSinkV2 } from "./EventSink.ts";
import { describe, expect, it } from "vite-plus/test";
import * as Context from "effect/Context";

import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServer from "effect/http/HttpServer";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import {
  AuthSessionId,
  AuthOrchestrationReadScope,
  AuthOrchestrationOperateScope,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentOrchestrationHttpApi,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
  type AuthEnvironmentScope,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import { failEnvironmentAuthInvalid } from "../auth/http.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import type { ProviderAdapterV2SessionRuntime } from "./ProviderAdapter.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as ProjectEnrichment from "../project/ProjectEnrichmentService.ts";
import * as ApplicationEvents from "../persistence/OrchestrationEventStore.ts";

const threadId = ThreadId.make("goal-http-thread");
const instanceId = ProviderInstanceId.make("codex-owner");
const providerThreadId = ProviderThreadId.make("goal-http-provider-thread");
const providerSessionId = ProviderSessionId.make("goal-http-session");
const path = `http://test/api/orchestration/threads/${threadId}/provider-goal-state?expectedInstanceId=${instanceId}`;
function fixture(
  options: {
    scopes?: AuthEnvironmentScope[];
    mismatchedProjection?: boolean;
    projectionChanges?: boolean;
    missingService?: boolean;
  } = {},
) {
  let calls = 0;
  let shellReads = 0;
  const input: unknown[] = [];
  const auth = Layer.succeed(EnvironmentAuthenticatedAuth, (httpEffect) =>
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      if (request.headers.authorization !== "Bearer fixture")
        return yield* failEnvironmentAuthInvalid("missing_credential");
      return yield* httpEffect.pipe(
        Effect.provideService(EnvironmentAuthenticatedPrincipal, {
          sessionId: AuthSessionId.make("goal-fixture"),
          subject: "fixture",
          method: "bearer-access-token",
          scopes: new Set(options.scopes ?? [AuthOrchestrationReadScope]),
        }),
      );
    }),
  );
  const providerThread = {
    id: providerThreadId,
    providerInstanceId: instanceId,
    providerSessionId,
    nativeThreadRef: { driver: "codex", nativeId: "native-thread", strength: "strong" },
  } as OrchestrationV2ProviderThread;
  const projection = Layer.mock(ProjectionStore.ProjectionStoreV2)({
    getThreadShell: () => Effect.succeed({} as never),
    getThreadRecords: () =>
      Effect.sync(() => {
        shellReads++;
        return {
          thread: {
            id: threadId,
            providerInstanceId:
              options.mismatchedProjection || (options.projectionChanges && shellReads > 1)
                ? ProviderInstanceId.make("different")
                : instanceId,
            activeProviderThreadId: providerThreadId,
          },
          providerThreads: [providerThread],
        } as never;
      }),
  });
  const runtime = {
    instanceId,
    readGoalState: (request: OrchestrationV2ProviderThread) =>
      Effect.sync(() => {
        calls++;
        input.push(request.id);
        return {
          nativeThreadId: "native-thread",
          state: "inactive" as const,
          reasonCode: "goal_null" as const,
          objective: "private objective",
        };
      }),
  } as unknown as ProviderAdapterV2SessionRuntime;
  const provider = Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
    get: () => Effect.succeed(Option.some(runtime)),
  });
  let group = OrchestrationHttpApi.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        projection,
        Layer.mock(EventSinkV2)({}),
        SqlitePersistence.layerMemory,
        Layer.mock(ThreadManagement.ThreadManagementService)({}),
        Layer.mock(ProjectStore.ProjectStoreV2)({}),
        Layer.mock(ProjectEnrichment.ProjectEnrichmentService)({}),
        Layer.mock(ApplicationEvents.OrchestrationEventStore)({}),
      ),
    ),
  );
  if (!options.missingService) group = group.pipe(Layer.provide(provider));
  const routes = HttpApiBuilder.layer(
    HttpApi.make("environment").add(EnvironmentOrchestrationHttpApi),
  ).pipe(Layer.provide(group), Layer.provide(auth), Layer.provide(HttpServer.layerServices));
  return { ...HttpRouter.toWebHandler(routes, { disableLogger: true }), calls: () => calls, input };
}

describe("native goal HTTP read boundary", () => {
  it("requires authentication and orchestration read scope before inspecting a goal", async () => {
    for (const scopes of [[], [AuthOrchestrationOperateScope]] as AuthEnvironmentScope[][]) {
      const f = fixture({ scopes });
      try {
        expect((await f.handler(new Request(path), Context.empty())).status).toBe(401);
        expect(
          (
            await f.handler(
              new Request(path, { headers: { authorization: "Bearer fixture" } }),
              Context.empty(),
            )
          ).status,
        ).toBe(403);
        expect(f.calls()).toBe(0);
      } finally {
        await f.dispose();
      }
    }
  });

  it("returns only the redacted exact-target observation", async () => {
    const f = fixture();
    try {
      const response = await f.handler(
        new Request(path, { headers: { authorization: "Bearer fixture" } }),
        Context.empty(),
      );
      expect(response.status).toBe(200);
      expect(await response.json()).toEqual({
        schema: "t3.provider-goal-state/v1",
        threadId,
        providerInstanceId: instanceId,
        nativeThreadId: "native-thread",
        observedAtMs: expect.any(Number),
        state: "inactive",
        reasonCode: "goal_null",
      });
      expect(f.input).toEqual([providerThreadId]);
      const missing = await f.handler(
        new Request(path.split("?")[0]!, { headers: { authorization: "Bearer fixture" } }),
        Context.empty(),
      );
      expect(missing.status).toBe(400);
      expect(f.calls()).toBe(1);
    } finally {
      await f.dispose();
    }
  });

  it.each([
    [{ mismatchedProjection: true }, "instance_mismatch", 0],
    [{ projectionChanges: true }, "context_changed", 1],
    [{ missingService: true }, "unsupported", 0],
  ] as const)("holds %s with a closed unknown response", async (options, reason, calls) => {
    const f = fixture(options);
    try {
      const response = await f.handler(
        new Request(path, { headers: { authorization: "Bearer fixture" } }),
        Context.empty(),
      );
      expect(response.status).toBe(200);
      expect(await response.json()).toMatchObject({
        state: "unknown",
        reasonCode: reason,
        threadId,
        providerInstanceId: instanceId,
        nativeThreadId: null,
      });
      expect(f.calls()).toBe(calls);
    } finally {
      await f.dispose();
    }
  });
});
