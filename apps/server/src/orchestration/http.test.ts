import { describe, expect, it } from "vite-plus/test";
import {
  AuthSessionId,
  AuthOrchestrationOperateScope,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentOrchestrationHttpApi,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as HttpApi from "effect/unstable/httpapi/HttpApi";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServer from "effect/unstable/http/HttpServer";
import { orchestrationHttpApiLayer } from "./http.ts";
import { NativePreparationBinding, nativePreparationCommand } from "./NativeCreationPreparation.ts";
import {
  NativeCreationRepository,
  NativeCreationRepositoryError,
} from "../persistence/Services/NativeCreationRepository.ts";
import { OrchestrationEngineService } from "./Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "./Services/ProjectionSnapshotQuery.ts";
import * as ProjectCloneTracker from "../project/ProjectCloneTracker.ts";
import * as ServerConfig from "../config.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";

const binding = Schema.decodeUnknownSync(NativePreparationBinding)({
  backend_instance: "synthetic-backend",
  environment_id: "synthetic-env",
  project_id: "synthetic-project",
  project_cwd: "/synthetic/project",
  account_ref: "synthetic-account",
  runtime_mode: "full-access",
  interaction_mode: "default",
  base_branch: "main",
  start_from_origin: false,
  run_setup_script: false,
  provider_model_selection: { instanceId: "codex", model: "synthetic-model" },
});
const command = nativePreparationCommand(
  "synthetic-operation",
  binding,
  "Synthetic text",
  "Synthetic thread",
  "2026-10-02T12:00:00Z",
);
const fixture = (membership: "absent" | "present" | "unavailable", bootstrap = true) => {
  let dispatches = 0;
  let cloneChecks = 0;
  const auth = Layer.succeed(EnvironmentAuthenticatedAuth, (httpEffect) =>
    httpEffect.pipe(
      Effect.provideService(EnvironmentAuthenticatedPrincipal, {
        sessionId: AuthSessionId.make("synthetic-session"),
        subject: "synthetic",
        method: "bearer-access-token",
        scopes: new Set([AuthOrchestrationOperateScope]),
      }),
    ),
  );
  const repository = Layer.mock(NativeCreationRepository)({
    hasAutomationEnrollment: () =>
      membership === "unavailable"
        ? Effect.fail(
            new NativeCreationRepositoryError({
              code: "unresolved_claim",
              message: "Synthetic unavailable lookup",
            }),
          )
        : Effect.succeed(membership === "present"),
  });
  const group = orchestrationHttpApiLayer.pipe(
    Layer.provide(repository),
    Layer.provide(Layer.mock(ProjectionSnapshotQuery)({})),
    Layer.provide(
      Layer.mock(OrchestrationEngineService)({
        dispatch: () =>
          Effect.sync(() => {
            dispatches++;
            return { sequence: 1 };
          }),
      }),
    ),
    Layer.provide(
      Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({
        get: () =>
          Effect.sync(() => {
            cloneChecks++;
            return null;
          }),
      }),
    ),
  );
  const routes = HttpApiBuilder.layer(
    HttpApi.make("environment").add(EnvironmentOrchestrationHttpApi),
  ).pipe(
    Layer.provide(group),
    Layer.provide(auth),
    Layer.provide(HttpServer.layerServices),
    Layer.provideMerge(
      Layer.mergeAll(
        ServerConfig.layerTest(process.cwd(), { prefix: "t3-native-creation-http-" }),
        WorkspacePaths.layer,
      ).pipe(Layer.provideMerge(NodeServices.layer)),
    ),
  );
  const handler = HttpRouter.toWebHandler(routes, { disableLogger: true });
  const { bootstrap: _bootstrap, ...plain } = command;
  return {
    ...handler,
    request: new Request("http://test/api/orchestration/dispatch", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(bootstrap ? command : plain),
    }),
    dispatches: () => dispatches,
    cloneChecks: () => cloneChecks,
  };
};

describe("legacy HTTP bootstrap enrollment boundary", () => {
  for (const membership of ["present", "unavailable"] as const) {
    it(`denies ${membership} membership before clone checks, normalization or engine effects`, async () => {
      const f = fixture(membership);
      try {
        expect((await f.handler(f.request)).status).toBe(400);
        expect(f.dispatches()).toBe(0);
        expect(f.cloneChecks()).toBe(0);
      } finally {
        await f.dispose();
      }
    });
  }
  it("preserves existing authorization after a successful absent-marker lookup", async () => {
    const f = fixture("absent");
    try {
      expect((await f.handler(f.request)).status).toBe(200);
      expect(f.dispatches()).toBe(1);
    } finally {
      await f.dispose();
    }
  });
  it("does not apply enrollment to existing-target sends", async () => {
    const f = fixture("present", false);
    try {
      expect((await f.handler(f.request)).status).toBe(200);
      expect(f.dispatches()).toBe(1);
    } finally {
      await f.dispose();
    }
  });
});
