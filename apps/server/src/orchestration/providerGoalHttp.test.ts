import { describe, expect, it } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as HttpApi from "effect/unstable/httpapi/HttpApi";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServer from "effect/unstable/http/HttpServer";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import {
  AuthSessionId,
  AuthOrchestrationReadScope,
  AuthOrchestrationOperateScope,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentOrchestrationHttpApi,
  ProviderInstanceId,
  ThreadId,
  type AuthEnvironmentScope,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import { failEnvironmentAuthInvalid } from "../auth/http.ts";
import { ProviderService } from "../provider/Services/ProviderService.ts";
import * as ServerConfig from "../config.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import * as ProjectCloneTracker from "../project/ProjectCloneTracker.ts";
import { orchestrationHttpApiLayer } from "./http.ts";
import { ProjectionSnapshotQuery } from "./Services/ProjectionSnapshotQuery.ts";
import { OrchestrationEngineService } from "./Services/OrchestrationEngine.ts";

const threadId = ThreadId.make("goal-http-thread");
const instanceId = ProviderInstanceId.make("codex-owner");
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
      if (request.headers.authorization !== "Bearer fixture") {
        return yield* failEnvironmentAuthInvalid("missing_credential");
      }
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
  const projection = Layer.mock(ProjectionSnapshotQuery)({
    getThreadShellById: () =>
      Effect.sync(() => {
        shellReads++;
        return Option.some({
          id: threadId,
          modelSelection: {
            instanceId:
              options.mismatchedProjection || (options.projectionChanges && shellReads > 1)
                ? ProviderInstanceId.make("different")
                : instanceId,
            model: "gpt-6.1-sol",
          },
          session: { threadId, providerInstanceId: instanceId, providerName: "codex" },
        } as OrchestrationThreadShell);
      }),
  });
  const provider = Layer.mock(ProviderService)({
    getProviderGoalState: (request) =>
      Effect.sync(() => {
        calls++;
        input.push(request);
        return {
          schema: "t3.provider-goal-state/v1" as const,
          threadId,
          providerInstanceId: instanceId,
          nativeThreadId: "native-thread",
          observedAtMs: 1000,
          state: "inactive" as const,
          reasonCode: "goal_null" as const,
          objective: "private objective",
        };
      }),
  });
  let group = orchestrationHttpApiLayer.pipe(
    Layer.provide(projection),
    Layer.provide(Layer.mock(OrchestrationEngineService)({})),
    Layer.provide(Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({})),
  );
  if (!options.missingService) group = group.pipe(Layer.provide(provider));
  const routes = HttpApiBuilder.layer(
    HttpApi.make("environment").add(EnvironmentOrchestrationHttpApi),
  ).pipe(
    Layer.provide(group),
    Layer.provide(auth),
    Layer.provide(HttpServer.layerServices),
    Layer.provideMerge(
      Layer.mergeAll(
        ServerConfig.layerTest(process.cwd(), { prefix: "t3-provider-goal-http-" }),
        WorkspacePaths.layer,
      ).pipe(Layer.provideMerge(NodeServices.layer)),
    ),
  );
  return { ...HttpRouter.toWebHandler(routes, { disableLogger: true }), calls: () => calls, input };
}

describe("native goal HTTP read boundary", () => {
  it("requires authentication and orchestration read scope before inspecting a goal", async () => {
    for (const scopes of [[], [AuthOrchestrationOperateScope]] as AuthEnvironmentScope[][]) {
      const f = fixture({ scopes });
      try {
        expect((await f.handler(new Request(path))).status).toBe(401);
        expect(
          (await f.handler(new Request(path, { headers: { authorization: "Bearer fixture" } })))
            .status,
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
      );
      expect(response.status).toBe(200);
      expect(await response.json()).toEqual({
        schema: "t3.provider-goal-state/v1",
        threadId,
        providerInstanceId: instanceId,
        nativeThreadId: "native-thread",
        observedAtMs: 1000,
        state: "inactive",
        reasonCode: "goal_null",
      });
      expect(f.input).toEqual([{ threadId, expectedInstanceId: instanceId }]);
      const missing = await f.handler(
        new Request(path.split("?")[0]!, { headers: { authorization: "Bearer fixture" } }),
      );
      expect(missing.status).toBe(400);
      expect(f.calls()).toBe(1);
    } finally {
      await f.dispose();
    }
  });

  for (const [options, reason, calls] of [
    [{ mismatchedProjection: true }, "instance_mismatch", 0],
    [{ projectionChanges: true }, "context_changed", 1],
    [{ missingService: true }, "unsupported", 0],
  ] as const) {
    it(`holds ${reason} with a closed unknown response`, async () => {
      const f = fixture(options);
      try {
        const response = await f.handler(
          new Request(path, { headers: { authorization: "Bearer fixture" } }),
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
  }
});
