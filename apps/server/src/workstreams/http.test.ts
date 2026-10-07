import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  AuthSessionId,
  EnvironmentHttpApi,
  EnvironmentScopeRequiredError,
  EnvironmentAuthInvalidError,
  EnvironmentHttpConflictError,
  EnvironmentInternalError,
  WorkstreamReceipt,
  T3WorkstreamListResult,
  type AuthEnvironmentScope,
  type WorkstreamCommand,
} from "@t3tools/contracts";
import * as Layer from "effect/Layer";
import * as HttpApi from "effect/unstable/httpapi/HttpApi";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpPlatform from "effect/unstable/http/HttpPlatform";
import * as Etag from "effect/unstable/http/Etag";
import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import { environmentAuthenticatedAuthLayer } from "../auth/http.ts";
import { make, WorkstreamGateway, WorkstreamGatewayError } from "./WorkstreamGateway.ts";
import { makeSyntheticWorkstreamTransport } from "./SyntheticWorkstreamTransport.ts";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { HttpIncomingMessage } from "effect/unstable/http";
import { describe, expect } from "vite-plus/test";

import {
  workstreamHttpApiLayer,
  workstreamResponseHeadersLayer,
  isWorkstreamHttpTarget,
  withWorkstreamBodyLimit,
  WORKSTREAM_RESPONSE_HEADERS,
} from "./http.ts";

describe("Workstream HTTP response containment", () => {
  it.effect(
    "caps the placement POST body before payload decoding without changing adjacent routes",
    () =>
      Effect.gen(function* () {
        const limit = (method: string, originalUrl: string) =>
          withWorkstreamBodyLimit(HttpIncomingMessage.MaxBodySize, { method, originalUrl });
        expect(yield* limit("POST", "/api/workstreams/thread-placements")).toBe(262_144n);
        expect(yield* limit("POST", "/api/workstreams/thread-placements?extra=x")).toBe(262_144n);
        expect(
          yield* limit("POST", "http://fixture/api/workstreams/thread-placements?extra=x"),
        ).toBe(262_144n);
        expect(yield* limit("POST", "https://fixture/api/workstreams/thread-placements")).toBe(
          262_144n,
        );
        expect(
          yield* limit("GET", "http://fixture/api/workstreams/thread-placements"),
        ).toBeUndefined();
        expect(
          yield* limit("POST", "http://fixture/api/workstreams/thread-placements-extra"),
        ).toBeUndefined();
        expect(yield* limit("POST", "http://fixture/api/workstreams/commands")).toBeUndefined();
        expect(yield* limit("POST", "/api/workstreams/commands")).toBeUndefined();
        expect(yield* limit("GET", "/api/workstreams")).toBeUndefined();
      }),
  );
  it("selects every Workstream API result and excludes adjacent APIs", () => {
    expect(isWorkstreamHttpTarget("/api/workstreams")).toBe(true);
    expect(isWorkstreamHttpTarget("/api/workstreams?limit=50")).toBe(true);
    expect(isWorkstreamHttpTarget("/api/workstreams/ws-1/history?cursor=next")).toBe(true);
    expect(isWorkstreamHttpTarget("http://fixture/api/workstreams?limit=50")).toBe(true);
    expect(isWorkstreamHttpTarget("https://fixture/api/workstreams/ws-1/history?cursor=next")).toBe(
      true,
    );
    expect(isWorkstreamHttpTarget("http://fixture/api/workstreams-extra")).toBe(false);
    expect(
      isWorkstreamHttpTarget("http://fixture/api/orchestration/snapshot?next=/api/workstreams"),
    ).toBe(false);
    expect(isWorkstreamHttpTarget("invalid-url")).toBe(false);
    expect(isWorkstreamHttpTarget("/api/orchestration/snapshot")).toBe(false);
    expect(WORKSTREAM_RESPONSE_HEADERS).toEqual({
      "cache-control": "private, no-store",
      "x-content-type-options": "nosniff",
    });
  });
});

class WorkstreamTestApi extends HttpApi.make("environment").add(
  EnvironmentHttpApi.groups.workstreams,
) {}

const command: WorkstreamCommand = {
  command_id: "command-http-fixture-0001",
  expected_server_generation: 7,
  expected_registry_version: 11,
  action: {
    operation: "update_workstream",
    workstream_id: "ws-core-v1",
    expected_version: 3,
    name: "HTTP fixture",
    lifecycle: "active",
    progress: { state: "progressing" },
    sort_order: 10,
  },
};

const withHttpFixture = Effect.fn(function* (
  scopes: readonly AuthEnvironmentScope[],
  run: (fixture: {
    handler: (request: Request) => Promise<Response>;
    calls: { reads: number; writes: number };
  }) => Promise<void>,
  failure?: "offline" | "cursor-stale",
) {
  const calls = { reads: 0, writes: 0 };
  const gateway = yield* make(makeSyntheticWorkstreamTransport(), {
    binding: {
      registryId: "https://workstream-registry.invalid",
      ownerId: "owner-fixture",
      principalId: "principal-fixture",
      authorizationRevision: 1,
    },
  });
  const authenticate: EnvironmentAuth.EnvironmentAuth["Service"]["authenticateHttpRequest"] = (
    request,
  ) => {
    const bearer = request.headers.authorization === "Bearer synthetic-token";
    const cookie = request.headers.cookie === "synthetic-session=fixture";
    if (!bearer && !cookie) {
      return Effect.fail(new EnvironmentAuth.ServerAuthMissingCredentialError({}));
    }
    return Effect.succeed({
      sessionId: AuthSessionId.make("session-http-fixture"),
      subject: "http-fixture",
      method: bearer ? ("bearer-access-token" as const) : ("browser-session-cookie" as const),
      scopes,
    });
  };
  const auth = new Proxy(
    {},
    {
      get(_target, key) {
        if (key !== "authenticateHttpRequest") {
          throw new Error(`Unexpected auth operation: ${String(key)}`);
        }
        return authenticate;
      },
    },
  ) as EnvironmentAuth.EnvironmentAuth["Service"];
  const routes = HttpApiBuilder.layer(WorkstreamTestApi).pipe(
    Layer.provide(workstreamHttpApiLayer),
    Layer.provide(environmentAuthenticatedAuthLayer),
    Layer.provide(Layer.succeed(EnvironmentAuth.EnvironmentAuth, auth)),
    Layer.provide(
      Layer.succeed(WorkstreamGateway, {
        ...gateway,
        readMetadata: (input) => {
          calls.reads += 1;
          return failure === undefined
            ? gateway.readMetadata(input)
            : Effect.fail(
                new WorkstreamGatewayError({ reason: failure, detail: "synthetic failure" }),
              );
        },
        submit: (input) => {
          calls.writes += 1;
          return gateway.submit(input);
        },
      }),
    ),
    Layer.provide(workstreamResponseHeadersLayer),
    Layer.provideMerge(
      HttpPlatform.layer.pipe(
        Layer.provideMerge(NodeServices.layer),
        Layer.provideMerge(Etag.layerWeak),
      ),
    ),
    Layer.provide(NodeServices.layer),
  );
  return yield* Effect.acquireUseRelease(
    Effect.sync(() => HttpRouter.toWebHandler(routes, { disableLogger: true })),
    (app) => Effect.promise(() => run({ handler: (request) => app.handler(request), calls })),
    (app) => Effect.promise(() => app.dispose()),
  );
});

const credentials: readonly {
  readonly kind: string;
  readonly headers: Record<string, string>;
}[] = [
  { kind: "bearer", headers: { authorization: "Bearer synthetic-token" } },
  { kind: "cookie", headers: { cookie: "synthetic-session=fixture" } },
];

it.effect.each(credentials)(
  "serves registry metadata through authenticated $kind read scope",
  ({ headers }) =>
    withHttpFixture([AuthOrchestrationReadScope], async ({ handler, calls }) => {
      const response = await handler(
        new Request("http://fixture/api/workstreams?limit=50", { headers }),
      );
      expect(response.status).toBe(200);
      const body = Schema.decodeUnknownSync(T3WorkstreamListResult)(await response.json());
      expect(body.source).toBe("live");
      expect(body.binding.registryId).toBe("https://workstream-registry.invalid");
      expect(response.headers.get("cache-control")).toBe("private, no-store");
      expect(response.headers.get("x-content-type-options")).toBe("nosniff");
      expect(calls).toEqual({ reads: 1, writes: 0 });
    }),
);

it.effect.each(credentials)(
  "rejects $kind command submission without operate scope before gateway writes",
  ({ headers }) =>
    withHttpFixture([AuthOrchestrationReadScope], async ({ handler, calls }) => {
      const response = await handler(
        new Request("http://fixture/api/workstreams/commands", {
          method: "POST",
          headers: { ...headers, "content-type": "application/json" },
          body: JSON.stringify({ command }),
        }),
      );
      expect(response.status).toBe(403);
      expect(response.headers.get("cache-control")).toBe("private, no-store");
      expect(response.headers.get("x-content-type-options")).toBe("nosniff");
      expect(
        Schema.decodeUnknownSync(EnvironmentScopeRequiredError)(await response.json())
          .requiredScope,
      ).toBe(AuthOrchestrationOperateScope);
      expect(calls).toEqual({ reads: 0, writes: 0 });
    }),
);

it.effect.each(credentials)(
  "submits a scoped $kind command and returns its committed receipt",
  ({ headers }) =>
    withHttpFixture([AuthOrchestrationOperateScope], async ({ handler, calls }) => {
      const response = await handler(
        new Request("http://fixture/api/workstreams/commands", {
          method: "POST",
          headers: { ...headers, "content-type": "application/json" },
          body: JSON.stringify({ command }),
        }),
      );
      expect(response.status).toBe(200);
      expect(Schema.decodeUnknownSync(WorkstreamReceipt)(await response.json()).state).toBe(
        "committed",
      );
      expect(response.headers.get("cache-control")).toBe("private, no-store");
      expect(response.headers.get("x-content-type-options")).toBe("nosniff");
      expect(calls).toEqual({ reads: 0, writes: 1 });
    }),
);

it.effect("rejects missing HTTP credentials before registry reads", () =>
  withHttpFixture([AuthOrchestrationReadScope], async ({ handler, calls }) => {
    const response = await handler(new Request("http://fixture/api/workstreams?limit=50"));
    expect(response.status).toBe(401);
    expect(response.headers.get("cache-control")).toBe("private, no-store");
    expect(response.headers.get("x-content-type-options")).toBe("nosniff");
    expect(
      Schema.decodeUnknownSync(EnvironmentAuthInvalidError)(await response.json()).reason,
    ).toBe("missing_credential");
    expect(calls).toEqual({ reads: 0, writes: 0 });
  }),
);

it.effect.each([
  ["offline", 500],
  ["cursor-stale", 409],
] as const)("keeps %s HTTP failures distinct and contains the response", ([reason, status]) =>
  withHttpFixture(
    [AuthOrchestrationReadScope],
    async ({ handler, calls }) => {
      const response = await handler(
        new Request("http://fixture/api/workstreams?limit=50", {
          headers: { authorization: "Bearer synthetic-token" },
        }),
      );
      expect(response.status).toBe(status);
      expect(response.headers.get("cache-control")).toBe("private, no-store");
      expect(response.headers.get("x-content-type-options")).toBe("nosniff");
      expect(calls).toEqual({ reads: 1, writes: 0 });
      const body = await response.json();
      if (reason === "cursor-stale")
        expect(Schema.decodeUnknownSync(EnvironmentHttpConflictError)(body).message).toBe(
          "workstream_cursor_stale",
        );
      else
        expect(Schema.decodeUnknownSync(EnvironmentInternalError)(body).reason).toBe(
          "internal_error",
        );
    },
    reason,
  ),
);
