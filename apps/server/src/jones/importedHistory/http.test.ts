import { assert, it } from "@effect/vitest";
import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  AuthSessionId,
  CommandId,
  ThreadId,
  RunId,
  MessageId,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentHttpApi,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServer from "effect/unstable/http/HttpServer";
import * as HttpApi from "effect/unstable/httpapi/HttpApi";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import { ThreadManagementService } from "../../orchestration-v2/ThreadManagementService.ts";
import { importedHistoryHttpApiLayer } from "./http.ts";

class TestApi extends HttpApi.make("environment").add(
  EnvironmentHttpApi.groups.jonesImportedHistory,
) {}
const threadId = ThreadId.make("thread:http-choice");
const commandId = CommandId.make("command:http-choice");
const payload = {
  type: "thread.imported-history.start",
  threadId,
  commandId,
  reviewedBasis: "a".repeat(64),
  delivery: {
    type: "queued_run",
    runId: RunId.make("run:http-choice"),
    messageId: MessageId.make("message:http-choice"),
  },
};
const fixture = (scopes: ReadonlyArray<AuthEnvironmentScope>, calls: unknown[]) => {
  const service = Layer.mock(ThreadManagementService)({
    startWithImportedHistory: (input) =>
      Effect.gen(function* () {
        const principal = yield* EnvironmentAuthenticatedPrincipal;
        calls.push({ input, actorSessionId: principal.sessionId });
        return {
          commandId: input.commandId,
          threadId: input.threadId,
          actorSessionId: principal.sessionId,
          commandDigest: "b".repeat(64),
          deliveryDigest: "c".repeat(64),
          reviewedBasis: input.reviewedBasis,
          status: "rejected" as const,
          runId: null,
          effectId: null,
          reason: "queued_preparation_unavailable",
        };
      }),
  });
  const auth = Layer.succeed(EnvironmentAuthenticatedAuth, (effect) =>
    effect.pipe(
      Effect.provideService(EnvironmentAuthenticatedPrincipal, {
        sessionId: AuthSessionId.make("session:http-choice"),
        subject: "fixture",
        method: "bearer-access-token",
        scopes: new Set(scopes),
      }),
    ),
  );
  const routes = HttpApiBuilder.layer(TestApi).pipe(
    Layer.provide(importedHistoryHttpApiLayer.pipe(Layer.provide(service))),
    Layer.provide(auth),
    Layer.provide(HttpServer.layerServices),
  );
  return HttpRouter.toWebHandler(routes, { disableLogger: true });
};
it.effect(
  "assembled HTTP route forwards exact choice with verified principal and typed outcome",
  () =>
    Effect.gen(function* () {
      const calls: unknown[] = [];
      const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(payload);
      yield* Effect.acquireUseRelease(
        Effect.sync(() => fixture([AuthOrchestrationOperateScope], calls)),
        (app) =>
          Effect.promise(async () => {
            const response = await app.handler(
              new Request("http://fixture/api/jones/imported-history/start", {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: encoded,
              }),
            );
            assert.strictEqual(response.status, 200);
            assert.deepEqual(calls, [{ input: payload, actorSessionId: "session:http-choice" }]);
            const decoded = await response.json();
            assert.deepEqual(decoded, {
              commandId,
              threadId,
              actorSessionId: "session:http-choice",
              commandDigest: "b".repeat(64),
              deliveryDigest: "c".repeat(64),
              reviewedBasis: "a".repeat(64),
              status: "rejected",
              runId: null,
              effectId: null,
              reason: "queued_preparation_unavailable",
            });
          }),
        (app) => Effect.promise(() => app.dispose()),
      );
    }),
);
it.effect("read-only caller cannot invoke imported admission through assembled HTTP routing", () =>
  Effect.gen(function* () {
    const calls: unknown[] = [];
    const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(payload);
    yield* Effect.acquireUseRelease(
      Effect.sync(() => fixture([AuthOrchestrationReadScope], calls)),
      (app) =>
        Effect.promise(async () => {
          const response = await app.handler(
            new Request("http://fixture/api/jones/imported-history/start", {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: encoded,
            }),
          );
          assert.strictEqual(response.status, 403);
          assert.deepEqual(calls, []);
        }),
      (app) => Effect.promise(() => app.dispose()),
    );
  }),
);
