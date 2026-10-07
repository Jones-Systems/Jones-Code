import { describe, expect, it } from "vite-plus/test";
import { NodeServices } from "@effect/platform-node";
import * as Config from "../config.ts";
import * as Projects from "../project/ProjectService.ts";
import * as Clones from "../project/ProjectCloneTracker.ts";
import * as Threads from "./ThreadManagementService.ts";
import * as Launch from "./ThreadLaunchService.ts";
import * as Sink from "./EventSink.ts";
import * as Outbox from "./EffectOutbox.ts";
import * as Bridge from "./QueueCompatibility.ts";
import {
  AuthSessionId,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentQueueDispatchHttpApi,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpApi from "effect/unstable/httpapi/HttpApi";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServer from "effect/unstable/http/HttpServer";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import { failEnvironmentAuthInvalid } from "../auth/http.ts";
import { QueueCompatibility, QueueCompatibilityError } from "./QueueCompatibility.ts";
import { queueCompatibilityHttpApiLayer } from "./queueCompatibilityHttp.ts";

function fixture(
  scopes: AuthEnvironmentScope[],
  reason?: QueueCompatibilityError["reason"],
  realBridge = false,
) {
  const received: unknown[] = [];
  const auth = Layer.succeed(EnvironmentAuthenticatedAuth, (effect) =>
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      if (request.headers.authorization !== "Bearer fixture")
        return yield* failEnvironmentAuthInvalid("missing_credential");
      return yield* effect.pipe(
        Effect.provideService(EnvironmentAuthenticatedPrincipal, {
          sessionId: AuthSessionId.make("queue-http-fixture"),
          subject: "fixture",
          method: "bearer-access-token",
          scopes: new Set(scopes),
        }),
      );
    }),
  );
  const stubQueue = Layer.succeed(QueueCompatibility, {
    dispatch: (payload) =>
      Effect.suspend(() => {
        received.push(payload);
        return reason === undefined
          ? Effect.succeed({ sequence: 42 })
          : Effect.fail(new QueueCompatibilityError({ reason, cause: "private dispatch detail" }));
      }),
  });
  const unexpected = (operation: string) =>
    Effect.sync(() => {
      received.push(operation);
      throw new Error(`Unexpected bridge effect: ${operation}`);
    });
  const receivingQueue = Bridge.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(Threads.ThreadManagementService)({ dispatch: () => unexpected("dispatch") }),
        Layer.mock(Launch.ThreadLaunchService)({
          launch: () => unexpected("launch"),
          preflightLegacyBootstrap: () => unexpected("preflight"),
        }),
        Layer.mock(Projects.ProjectService)({ getById: () => unexpected("project-read") }),
        Layer.mock(Clones.ProjectCloneTracker)({ get: () => unexpected("clone-read") }),
        Layer.mock(Sink.EventSinkV2)({ latestSequence: () => unexpected("sequence-read") }),
        Layer.mock(Outbox.EffectOutboxV2)({ listByThreadId: () => unexpected("cleanup-read") }),
      ),
    ),
    Layer.provide(
      Config.layerTest(process.cwd(), { prefix: "queue-http-real-" }).pipe(
        Layer.provideMerge(NodeServices.layer),
      ),
    ),
  );
  const queue = realBridge ? receivingQueue : stubQueue;
  const routes = HttpApiBuilder.layer(
    HttpApi.make("environment").add(EnvironmentQueueDispatchHttpApi),
  ).pipe(
    Layer.provide(queueCompatibilityHttpApiLayer.pipe(Layer.provide(queue))),
    Layer.provide(auth),
    Layer.provide(HttpServer.layerServices),
  );
  return { ...HttpRouter.toWebHandler(routes, { disableLogger: true }), received };
}
const request = (
  authenticated = true,
  payload: unknown = { type: "thread.turn.start", commandId: "fixture" },
) =>
  new Request("http://test/api/orchestration/dispatch", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(authenticated ? { authorization: "Bearer fixture" } : {}),
    },
    body: JSON.stringify(payload),
  });
describe("queue compatibility dispatch HTTP", () => {
  it("requires authentication and operate scope before dispatch", async () => {
    const f = fixture(["orchestration:read"]);
    try {
      expect((await f.handler(request(false), Context.empty())).status).toBe(401);
      expect((await f.handler(request(), Context.empty())).status).toBe(403);
      expect(f.received).toEqual([]);
    } finally {
      await f.dispose();
    }
  });
  it("rejects guarded bootstrap through the authenticated HTTP route before receiving effects", async () => {
    const f = fixture(["orchestration:operate"], undefined, true);
    const payload = {
      type: "thread.turn.start",
      commandId: "http:C",
      threadId: "http:T",
      createdAt: "2026-10-05T00:00:00.000Z",
      message: { messageId: "http:M", role: "user", text: "Prompt", attachments: [] },
      runtimeMode: "full-access",
      interactionMode: "default",
      bootstrap: { runSetupScript: true },
      dispatchGuard: {
        observedSnapshotSequence: 0,
        expectedModelSelection: { instanceId: "codex", model: "gpt-6" },
        expectedSessionStatus: null,
        expectedActiveTurnId: null,
        expectedLatestTurnId: null,
        requireIdle: true,
      },
    };
    try {
      expect((await f.handler(request(false, payload), Context.empty())).status).toBe(401);
      const response = await f.handler(request(true, payload), Context.empty());
      expect(response.status).toBe(400);
      expect(await response.json()).toMatchObject({
        reason: "dispatch_guard_bootstrap_unsupported",
      });
      expect(f.received).toEqual([]);
      const forged = await f.handler(
        request(true, { ...payload, transport: "legacy_websocket" }),
        Context.empty(),
      );
      expect(forged.status).toBe(400);
      expect(await forged.json()).toMatchObject({ reason: "invalid_command" });
      expect(f.received).toEqual([]);
    } finally {
      await f.dispose();
    }
  });
  it("returns the accepted durable sequence", async () => {
    const f = fixture(["orchestration:operate"]);
    try {
      const result = await f.handler(request(), Context.empty());
      expect(result.status).toBe(200);
      expect(await result.json()).toEqual({ sequence: 42 });
      expect(f.received).toEqual([{ type: "thread.turn.start", commandId: "fixture" }]);
    } finally {
      await f.dispose();
    }
  });
  it.each([
    ["invalid_command", 400],
    ["dispatch_guard_bootstrap_unsupported", 400],
    ["dispatch_guard_rejected", 400],
    ["orchestration_dispatch_failed", 500],
  ] as const)("maps %s to a closed public error", async (reason, status) => {
    const f = fixture(["orchestration:operate"], reason);
    try {
      const result = await f.handler(request(), Context.empty());
      expect(result.status).toBe(status);
      expect(await result.text()).not.toContain("private dispatch detail");
    } finally {
      await f.dispose();
    }
  });
});
