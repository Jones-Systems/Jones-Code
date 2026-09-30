import * as NodeHttpPlatform from "@effect/platform-node/NodeHttpPlatform";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import { Etag } from "effect/unstable/http";
import * as HttpApi from "effect/unstable/httpapi/HttpApi";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import * as Schema from "effect/Schema";

import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentAuthInvalidError,
  EnvironmentConversationLibraryHttpApi,
  type EnvironmentSessionPrincipalShape,
} from "@t3tools/contracts";
import { LIBRARY_MAX_REQUEST_BYTES } from "@t3tools/contracts/conversationLibrary";

import * as ServerConfig from "../config.ts";
import { conversationLibraryHttpApiLayer } from "./http.ts";

const ConversationLibraryTestApi = HttpApi.make("environment").add(
  EnvironmentConversationLibraryHttpApi,
);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const testAuthenticationLayer = Layer.succeed(EnvironmentAuthenticatedAuth, (httpEffect) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const authorization = request.headers.authorization;
    if (authorization === undefined) {
      return yield* new EnvironmentAuthInvalidError({
        code: "auth_invalid",
        reason: "missing_credential",
        traceId: "conversation-library-test",
      });
    }

    const scopes =
      authorization === "Bearer test-operate"
        ? new Set([AuthOrchestrationReadScope, AuthOrchestrationOperateScope])
        : new Set([AuthOrchestrationReadScope]);
    const principal: EnvironmentSessionPrincipalShape = {
      sessionId:
        "conversation-library-test-session" as EnvironmentSessionPrincipalShape["sessionId"],
      subject: "conversation-library-test-client",
      method: "bearer-access-token",
      scopes,
    };
    return yield* httpEffect.pipe(
      Effect.provideService(EnvironmentAuthenticatedPrincipal, principal),
    );
  }),
);

const makeRouteLayer = () =>
  HttpApiBuilder.layer(ConversationLibraryTestApi).pipe(
    Layer.provide(conversationLibraryHttpApiLayer),
    Layer.provide(testAuthenticationLayer),
    Layer.provide(Etag.layerWeak),
    Layer.provide(NodeHttpPlatform.layer),
    Layer.provide(NodeServices.layer),
  );

const send = <E, R>(
  handler: Effect.Effect<HttpServerResponse.HttpServerResponse, E, R>,
  body: string,
  token?: "read" | "operate",
) => {
  const headers = new Headers({ "content-type": "application/json" });
  if (token !== undefined) headers.set("authorization", `Bearer test-${token}`);
  const request = HttpServerRequest.fromWeb(
    new Request("http://localhost/api/conversation-library", {
      method: "POST",
      headers,
      body,
    }),
  );
  return handler.pipe(
    Effect.provideService(HttpServerRequest.HttpServerRequest, request),
    Effect.map(HttpServerResponse.toWeb),
  );
};

it.layer(NodeServices.layer)("conversation library HTTP", (it) => {
  it.effect("authenticates before storage and keeps read-only opens in memory", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const handler = yield* HttpRouter.toHttpEffect(makeRouteLayer());
        const config = yield* ServerConfig.ServerConfig;
        const path = yield* Path.Path;
        const fs = yield* FileSystem.FileSystem;
        const libraryDir = path.join(config.stateDir, "conversation-library");

        const unauthorized = yield* send(handler, encodeJson({ kind: "hello" }));
        expect(unauthorized.status).toBe(401);
        expect(yield* fs.exists(libraryDir)).toBe(false);

        const read = yield* send(handler, encodeJson({ kind: "hello" }), "read");
        expect(read.status).toBe(200);
        expect(yield* Effect.promise(() => read.json())).toMatchObject({
          kind: "hello",
          canWrite: false,
        });
        expect(yield* fs.exists(libraryDir)).toBe(false);
      }).pipe(
        Effect.provide(
          ServerConfig.layerTest(process.cwd(), { prefix: "t3-conversation-library-http-" }),
        ),
      ),
    ),
  );

  it.effect("requires operate scope for writes and derives hello.canWrite from the principal", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const handler = yield* HttpRouter.toHttpEffect(makeRouteLayer());
        const config = yield* ServerConfig.ServerConfig;
        const path = yield* Path.Path;
        const fs = yield* FileSystem.FileSystem;
        const libraryDir = path.join(config.stateDir, "conversation-library");

        const denied = yield* send(
          handler,
          encodeJson({ kind: "createAccount", label: "Personal", workspace: "local" }),
          "read",
        );
        expect(denied.status).toBe(403);
        expect(yield* fs.exists(libraryDir)).toBe(false);

        const created = yield* send(
          handler,
          encodeJson({ kind: "createAccount", label: "Personal", workspace: "local" }),
          "operate",
        );
        expect(created.status).toBe(200);
        const createdBody = yield* Effect.promise(() => created.json());
        expect(createdBody).toMatchObject({
          kind: "account",
          account: { label: "Personal", workspace: "local" },
        });

        const hello = yield* send(handler, encodeJson({ kind: "hello" }), "operate");
        expect(yield* Effect.promise(() => hello.json())).toMatchObject({
          kind: "hello",
          canWrite: true,
        });

        const accounts = yield* send(handler, encodeJson({ kind: "accounts" }), "read");
        expect(yield* Effect.promise(() => accounts.json())).toMatchObject({
          kind: "accounts",
          accounts: [{ label: "Personal", workspace: "local" }],
        });
        expect(yield* fs.exists(path.join(libraryDir, "library.sqlite"))).toBe(true);
      }).pipe(
        Effect.provide(
          ServerConfig.layerTest(process.cwd(), { prefix: "t3-conversation-library-http-" }),
        ),
      ),
    ),
  );

  it.effect(
    "returns safe errors for malformed and over-limit streamed bodies before opening storage",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const handler = yield* HttpRouter.toHttpEffect(makeRouteLayer());
          const config = yield* ServerConfig.ServerConfig;
          const path = yield* Path.Path;
          const fs = yield* FileSystem.FileSystem;
          const libraryDir = path.join(config.stateDir, "conversation-library");

          const malformed = yield* send(handler, "{", "read");
          expect(malformed.status).toBe(400);
          expect(yield* Effect.promise(() => malformed.json())).toMatchObject({
            kind: "error",
            code: "invalid",
          });
          expect(yield* fs.exists(libraryDir)).toBe(false);

          const oversizedPayload = encodeJson({
            kind: "preview",
            conversations: [
              {
                title: "Oversized",
                mapping: {
                  node: {
                    message: {
                      author: { role: "user" },
                      content: { parts: ["x".repeat(LIBRARY_MAX_REQUEST_BYTES)] },
                    },
                  },
                },
              },
            ],
          });
          const oversized = yield* send(handler, oversizedPayload, "read");
          expect(oversizedPayload.length).toBeGreaterThan(LIBRARY_MAX_REQUEST_BYTES);
          expect(oversized.status).toBe(413);
          expect(yield* Effect.promise(() => oversized.json())).toMatchObject({
            kind: "error",
            code: "too-large",
          });
          expect(yield* fs.exists(libraryDir)).toBe(false);
        }).pipe(
          Effect.provide(
            ServerConfig.layerTest(process.cwd(), { prefix: "t3-conversation-library-http-" }),
          ),
        ),
      ),
  );
});
