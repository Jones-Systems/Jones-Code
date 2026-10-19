// @effect-diagnostics nodeBuiltinImport:off -- Tests exercise real POSIX filesystem custody and exact scratch cleanup.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { ServiceLauncherHostProcess } from "../cloud/serviceLauncherClient.ts";
import { ServerConfig } from "../config.ts";
import { workstreamGatewayLayerLive, workstreamRegistrationContextLayerLive } from "./http.ts";
import { vi } from "vite-plus/test";
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
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpPlatform from "effect/http/HttpPlatform";
import * as Etag from "effect/http/Etag";
import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import { layerAuthenticatedAuth } from "../auth/http.ts";
import {
  make,
  WorkstreamGateway,
  WorkstreamGatewayError,
  WorkstreamTransportError,
  type WorkstreamTransport,
} from "./WorkstreamGateway.ts";
import { WorkstreamsRegistrationContext } from "../jones/workstreams/registrationContext/service.ts";
import { makeRegistrationFixture } from "../jones/workstreams/registrationContext/testFixtures.ts";
import { makeSyntheticWorkstreamTransport } from "./SyntheticWorkstreamTransport.ts";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { HttpIncomingMessage } from "effect/http";
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
    submissions: { count: number };
  }) => Promise<void>,
  failure?: "offline" | "cursor-stale" | "not-configured",
  customizeTransport?: (transport: WorkstreamTransport) => WorkstreamTransport,
) {
  const calls = { reads: 0, writes: 0 };
  const submissions = { count: 0 };
  const synthetic = makeSyntheticWorkstreamTransport();
  const transport = customizeTransport?.(synthetic) ?? synthetic;
  const gateway = yield* make(
    {
      ...transport,
      submitCommand: (input) => {
        submissions.count += 1;
        return transport.submitCommand(input);
      },
    },
    {
      binding: {
        registryId: "https://workstream-registry.invalid",
        ownerId: "owner-fixture",
        principalId: "principal-fixture",
        authorizationRevision: 1,
      },
    },
  );
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
    Layer.provide(Layer.succeed(WorkstreamsRegistrationContext, makeRegistrationFixture().service)),
    Layer.provide(layerAuthenticatedAuth),
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
    (app) =>
      Effect.promise(() => run({ handler: (request) => app.handler(request), calls, submissions })),
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
    withHttpFixture([AuthOrchestrationReadScope], async ({ handler, calls, submissions }) => {
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
      expect(submissions.count).toBe(0);
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
  ["offline", 500, undefined],
  ["cursor-stale", 409, "workstream_cursor_stale"],
  ["not-configured", 409, "workstreams_not_configured"],
] as const)(
  "keeps %s HTTP failures distinct and contains the response",
  ([reason, status, conflict]) =>
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
        if (conflict !== undefined)
          expect(Schema.decodeUnknownSync(EnvironmentHttpConflictError)(body).message).toBe(
            conflict,
          );
        else
          expect(Schema.decodeUnknownSync(EnvironmentInternalError)(body).reason).toBe(
            "internal_error",
          );
      },
      reason,
    ),
);

const decodeInternalError = Schema.decodeUnknownSync(EnvironmentInternalError);

const submitRequest = (value: unknown = command) =>
  new Request("http://fixture/api/workstreams/commands", {
    method: "POST",
    headers: { authorization: "Bearer synthetic-token", "content-type": "application/json" },
    body: JSON.stringify({ command: value }),
  });

it.effect("rejects malformed commands before transport submission", () =>
  withHttpFixture([AuthOrchestrationOperateScope], async ({ handler, calls, submissions }) => {
    const result = await handler(submitRequest({ ...command, action: { operation: "invalid" } }));
    expect(result.status).toBe(400);
    expect(await result.text()).toBe("");
    expect(calls.writes).toBe(0);
    expect(submissions.count).toBe(0);
  }),
);

it.effect.each(["server", "registry"] as const)(
  "returns a definitive conflict for a changed %s version before transport submission",
  (version) =>
    withHttpFixture([AuthOrchestrationOperateScope], async ({ handler, submissions }) => {
      const result = await handler(
        submitRequest({
          ...command,
          ...(version === "server"
            ? { expected_server_generation: 6 }
            : { expected_registry_version: 10 }),
        }),
      );
      expect(submissions.count).toBe(0);
      expect(result.status).toBe(409);
      expect(await result.json()).toEqual({
        _tag: "EnvironmentHttpConflictError",
        message: "workstream_command_not_current",
      });
    }),
);

it.effect.each(["permission", "owner", "contract", "page-bound"] as const)(
  "returns a definitive conflict for pre-transport %s authorization rejection",
  (failure) =>
    withHttpFixture(
      [AuthOrchestrationOperateScope],
      async ({ handler, submissions }) => {
        const result = await handler(submitRequest());
        expect(submissions.count).toBe(0);
        expect(result.status).toBe(409);
        expect(await result.json()).toEqual({
          _tag: "EnvironmentHttpConflictError",
          message: "workstream_command_not_current",
        });
      },
      undefined,
      (transport) => ({
        ...transport,
        getCapabilities: (input) =>
          transport.getCapabilities(input).pipe(
            Effect.map((value) => ({
              ...value,
              ...(failure === "permission" ? { permissions: [] } : {}),
              ...(failure === "owner"
                ? { context: { ...value.context, owner_id: "other-owner" } }
                : {}),
              ...(failure === "contract" ? { manifest_sha256: "0".repeat(64) } : {}),
              // Malformed transport input deliberately bypasses the decoded capability type.
              ...(failure === "page-bound"
                ? { max_page_items: 0 as typeof value.max_page_items }
                : {}),
            })),
          ),
      }),
    ),
);

it.effect.each(["no-effect", "unknown-effect"] as const)(
  "keeps transport %s failures opaque after dispatch",
  (effect) =>
    withHttpFixture(
      [AuthOrchestrationOperateScope],
      async ({ handler, submissions }) => {
        const result = await handler(submitRequest());
        expect(submissions.count).toBe(1);
        expect(result.status).toBe(500);
        expect(decodeInternalError(await result.json()).reason).toBe("internal_error");
      },
      undefined,
      (transport) => ({
        ...transport,
        submitCommand: () =>
          Effect.fail(
            new WorkstreamTransportError({
              operation: "submit",
              effect,
              detail: "synthetic lost response",
            }),
          ),
      }),
    ),
);

it.effect("keeps invalid receipt validation opaque after transport submission", () =>
  withHttpFixture(
    [AuthOrchestrationOperateScope],
    async ({ handler, submissions }) => {
      const result = await handler(submitRequest());
      expect(submissions.count).toBe(1);
      expect(result.status).toBe(500);
    },
    undefined,
    (transport) => ({
      ...transport,
      submitCommand: (input) =>
        transport.submitCommand(input).pipe(
          Effect.map((receipt) => ({
            ...receipt,
            actor: { principal_id: "different-principal" },
          })),
        ),
    }),
  ),
);

it.effect("keeps capability transport failure opaque without claiming a local rejection", () =>
  withHttpFixture(
    [AuthOrchestrationOperateScope],
    async ({ handler, submissions }) => {
      const result = await handler(submitRequest());
      expect(submissions.count).toBe(0);
      expect(result.status).toBe(500);
    },
    undefined,
    (transport) => ({
      ...transport,
      getCapabilities: () =>
        Effect.fail(
          new WorkstreamTransportError({
            operation: "capabilities",
            effect: "no-effect",
            detail: "synthetic offline",
          }),
        ),
    }),
  ),
);

it.effect("keeps conflicting command bytes opaque because an earlier submission committed", () =>
  withHttpFixture(
    [AuthOrchestrationOperateScope],
    async ({ handler, submissions }) => {
      expect((await handler(submitRequest())).status).toBe(200);
      const result = await handler(
        submitRequest({ ...command, action: { ...command.action, name: "Changed" } }),
      );
      expect(result.status).toBe(500);
      expect(submissions.count).toBe(1);
    },
    undefined,
    (transport) => ({
      ...transport,
      getCapabilities: (input) =>
        transport.getCapabilities(input).pipe(
          Effect.map((value) => ({
            ...value,
            context: { ...value.context, registry_version: command.expected_registry_version },
          })),
        ),
    }),
  ),
);

it.effect("both live consumers share one file snapshot and keep it until the next startup", () =>
  Effect.gen(function* () {
    const base = yield* Effect.acquireRelease(
      Effect.sync(() =>
        NodeFS.mkdtempSync(
          NodePath.join(NodeFS.realpathSync(NodeOS.tmpdir()), "jones-activation-http-"),
        ),
      ),
      (path) =>
        Effect.sync(() => {
          vi.restoreAllMocks();
          vi.unstubAllGlobals();
          vi.unstubAllEnvs();
          NodeFS.rmSync(path, { recursive: true, force: true });
        }),
    );
    const environment = {
      T3_WORKSTREAM_CONTROL_PLANE_URL: "http://127.0.0.1:8787/",
      T3_WORKSTREAM_OWNER_ID: "owner-startup",
      T3_WORKSTREAM_PRINCIPAL_ID: "principal-startup",
      T3_WORKSTREAM_KEY_ID: "key-startup",
      T3_WORKSTREAM_SIGNING_SECRET: Buffer.alloc(32, 7).toString("base64"),
      T3_WORKSTREAM_AUTHORIZATION_REVISION: "3",
    };
    const directory = NodePath.join(base, "workstreams");
    NodeFS.mkdirSync(directory, { mode: 0o700 });
    const file = NodePath.join(directory, "activation.env");
    const bytes = Object.entries(environment)
      .map(([key, value]) => `${key}=${value}\n`)
      .join("");
    NodeFS.writeFileSync(file, bytes, { mode: 0o600 });
    NodeFS.writeFileSync(NodePath.join(base, "environment-id"), "environment-startup\n");
    for (const key of Object.keys(environment)) vi.stubEnv(key, undefined);
    const opened = vi.spyOn(NodeFS, "openSync");
    const requests: Headers[] = [];
    vi.stubGlobal("fetch", async (_input: unknown, init: RequestInit) => {
      requests.push(new Headers(init.headers));
      throw new Error("synthetic-offline");
    });
    const live = Layer.mergeAll(
      workstreamGatewayLayerLive,
      workstreamRegistrationContextLayerLive,
    ).pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.succeed(ServerConfig, {
            baseDir: base,
            authorityStateDir: NodePath.join(base, "authority"),
            stateDir: base,
            environmentIdPath: NodePath.join(base, "environment-id"),
          } as ServerConfig["Service"]),
          Layer.succeed(HostProcess.Environment, {}),
          Layer.succeed(ServiceLauncherHostProcess, {
            connected: false,
            send: () => false,
            on: () => {},
            off: () => {},
          }),
          NodeServices.layer,
        ),
      ),
    );
    yield* Effect.gen(function* () {
      const gateway = yield* WorkstreamGateway;
      const registration = yield* WorkstreamsRegistrationContext;
      expect(opened.mock.calls.filter(([path]) => path === file)).toHaveLength(1);
      NodeFS.writeFileSync(file, bytes.replaceAll("principal-startup", "principal-replaced"));
      vi.stubEnv("T3_WORKSTREAM_PRINCIPAL_ID", "principal-from-later-env");
      yield* gateway.readMetadata({}).pipe(Effect.result);
      yield* registration.read().pipe(Effect.result);
      expect(requests).toHaveLength(2);
      expect(requests.map((headers) => headers.get("x-control-principal-id"))).toEqual([
        "principal-startup",
        "principal-startup",
      ]);
      expect(process.env.T3_WORKSTREAM_SIGNING_SECRET).toBeUndefined();
    }).pipe(Effect.provide(live));
  }),
);

vi.mock("node:fs", async (importOriginal) => ({
  ...(await importOriginal<typeof import("node:fs")>()),
}));
