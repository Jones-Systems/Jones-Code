import { NodeHttpServer } from "@effect/platform-node";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import { Tool, Toolkit } from "effect/ai";
import { HttpBody, HttpClient, HttpRouter } from "effect/http";
import * as Auth from "../../../auth/EnvironmentAuth.ts";
import * as McpOAuth from "../../../auth/McpOAuth.ts";
import * as Secrets from "../../../auth/ServerSecretStore.ts";
import * as Config from "../../../config.ts";
import * as Authority from "../../../environment/NativeStoreAuthority.ts";
import * as Environment from "../../../environment/ServerEnvironment.ts";
import * as McpHttpServer from "../../../mcp/McpHttpServer.ts";
import * as Mcp from "../../../mcp/McpInvocationContext.ts";
import * as McpSessions from "../../../mcp/McpSessionRegistry.ts";
import * as McpAccess from "../../../mcp/McpToolAccess.ts";
import { runMigrations } from "../../../persistence/Migrations.ts";
import {
  LINKED_MESSAGE_ORIGIN_HEADER,
  encodeLinkedMessageOrigin,
} from "../../peerTransport/LinkedMessageOrigin.ts";
import pinMigration from "../../persistence/Migrations/128_JonesWorkstreamsPeerSessionPins.ts";
import * as Authentication from "../../workstreams/native/runtime/peerMcpAuthentication.ts";
import * as Pins from "../../workstreams/native/runtime/peerSessionPins.ts";
import * as Records from "../../workstreams/native/store/peerSessionPinStore.ts";
import * as Test from "../../workstreams/native/store/sql.testkit.ts";

// The MCP middleware under test is the real one, `McpHttpServer.layerMcpTransport`, in front of the real
// native peer qualification (`peerMcpAuthentication`, `peerSessionPins`) and real `EnvironmentAuth`. Native
// peer admission is keyed on the exact invocation object (a WeakMap), so these tests fail if a change to the
// middleware seats native registration on a different object than the one tool handlers later read.

const home = { ...Test.scope, home_environment_id: "home:test", server_generation: 1 };
const receiver = {
  environmentId: "home:test",
  authorityNamespace: "receiver-store",
  storeGeneration: 1,
};
const signing = Layer.succeed(
  Secrets.ServerSecretStore,
  Secrets.ServerSecretStore.of({
    get: () => Effect.succeedSome(Test.keyBytes),
    getOrCreateRandom: () => Effect.succeed(Test.keyBytes),
    set: () => Effect.die("unexpected set"),
    create: () => Effect.die("unexpected create"),
    remove: () => Effect.die("unexpected remove"),
  }),
);
const authority = Layer.succeed(Authority.NativeStoreAuthority, {
  readCurrent: Effect.succeed(receiver),
  trustProvider: { read: () => Effect.die("unused") },
} as unknown as Authority.NativeStoreAuthority["Service"]);
// One NativePeerSessions instance is shared by the registration hook and by the handlers that read it.
const services = Pins.layer.pipe(
  Layer.provideMerge(Authentication.layer.pipe(Layer.provideMerge(Pins.nativeLayer))),
  Layer.provideMerge(Records.layer),
  Layer.provideMerge(authority),
  Layer.provideMerge(Auth.layer.pipe(Layer.provide(signing))),
  Layer.provideMerge(Test.testLayer()),
  Layer.provideMerge(Config.layerTest(process.cwd(), { prefix: "nw-e2w-peer-middleware-" })),
  Layer.provideMerge(
    Layer.succeed(Environment.ServerEnvironmentIdentity, {
      getEnvironmentId: Effect.succeed(EnvironmentId.make("home:test")),
    }),
  ),
  Layer.provideMerge(NodeServices.layer),
);
type Services = Layer.Success<typeof services>;

const Observed = Schema.Struct({
  session: Schema.String,
  pin: Schema.String,
  permissions: Schema.Array(Schema.String),
  interaction_mode: Schema.NullOr(Schema.String),
});
const decodeObserved = Schema.decodeUnknownEffect(Observed);
const probe = Toolkit.make(
  Tool.make("native_peer_probe", {
    description: "Test only: what native peer qualification sees for this invocation.",
    parameters: Tool.EmptyParams,
    success: Observed,
    dependencies: [Mcp.McpInvocationContext],
  }),
);
const probes = McpAccess.toLayer(
  probe,
  Effect.gen(function* () {
    const peers = yield* Pins.NativePeerSessions;
    const pins = yield* Pins.PeerSessionPins;
    return {
      native_peer_probe: McpAccess.reads(() =>
        Effect.gen(function* () {
          const invocation = yield* Mcp.McpInvocationContext;
          const session = yield* peers.current(invocation).pipe(Effect.result);
          const pin = yield* pins
            .forInvocation(invocation, { role: "member_home", setupOnlyPrepared: true })
            .pipe(Effect.result);
          return {
            session: session._tag === "Success" ? "ok" : session.failure.code,
            pin: pin._tag === "Success" ? pin.success.pin.pin_id : pin.failure.code,
            permissions: pin._tag === "Success" ? pin.success.ceiling.permissions : [],
            interaction_mode:
              session._tag === "Success" ? session.success.session.ceiling.interaction_mode : null,
          };
        }),
      ),
    };
  }),
);

/** The real middleware, serving the probe, with `context` supplying every service it consults. */
const serve = (context: Context.Context<Services>) =>
  HttpRouter.serve(
    McpHttpServer.toolkitRegistration(probe, probes).pipe(
      Layer.provideMerge(McpHttpServer.layerMcpTransport),
      Layer.provide(McpOAuth.layerMcpClientAuthenticator),
      Layer.provide(
        Layer.mock(McpSessions.McpSessionRegistry)({ resolve: () => Effect.undefined }),
      ),
      Layer.provide(
        Layer.mock(Environment.ServerEnvironment)({
          getEnvironmentId: Effect.succeed(EnvironmentId.make("home:test")),
        }),
      ),
      Layer.provide(Layer.succeedContext(context)),
    ),
    { disableListenLog: true, disableLogger: true },
  ).pipe(Layer.build);

const prepare = Effect.gen(function* () {
  yield* TestClock.setTime(Date.parse(Test.now));
  yield* runMigrations({ toMigrationInclusive: 54 });
  yield* Test.seed;
  yield* pinMigration;
  const auth = yield* Auth.EnvironmentAuth;
  const native = yield* Pins.NativePeerSessions;
  const records = yield* Records.PeerSessionPinStore;
  const issue = (peerLink: boolean) =>
    auth.issueMcpClientSession({
      label: "synthetic peer",
      access: "full-access",
      peerLink,
      client: { deviceType: "bot" },
    });
  // Pin a linked session the way setup does: from the facts native authentication derives for it.
  const pin = Effect.fn("nativePeerMiddleware.pin")(function* (
    issued: Effect.Success<ReturnType<typeof issue>>,
  ) {
    const scope: Mcp.McpInvocationScope = {
      environmentId: EnvironmentId.make("home:test"),
      issuedAt: Date.parse(Test.now),
      requestNamespace: `client:${issued.sessionId}`,
      capabilities: new Set(["orchestration"]),
      thread: undefined,
      client: { sessionId: issued.sessionId, label: "peer", access: "full-access", linked: true },
    };
    yield* native.authenticate(
      {
        cookies: {},
        headers: { authorization: `Bearer ${issued.token}` },
      } as unknown as Parameters<Auth.EnvironmentAuth["Service"]["authenticateMcpClient"]>[0],
      scope,
    );
    const facts = yield* native.current(scope);
    const body = JSON.stringify({
      schema: "jones-workstreams-peer-pin-setup/1",
      assertion: {
        schema: "jones-workstreams-peer-session-pin/1",
        pin_id: "pin:middleware",
        revision: 1,
        receiver_authority: {
          environment_id: receiver.environmentId,
          authority_namespace: receiver.authorityNamespace,
          store_generation: receiver.storeGeneration,
        },
        session: facts.session,
        peer_environment_id: "source:test",
        roles: ["member_home"],
        expected_home: home,
        source_authority: {
          environment_id: "source:test",
          authority_namespace: "source-store",
          store_generation: 1,
        },
        source_enrollment_id: "enrollment:test",
        source_binding_version: null,
        singleton_binding_version: null,
        state: "prepared",
        expires_at: "2026-01-01T00:20:00.000Z",
        setup_request_id: "setup:test",
        owner_qualification_digest: "b".repeat(64),
        bind_receipt: null,
      },
      expected: { registry_version: 1, grant_id: "grant:test", grant_version: 1 },
    });
    yield* records.insert({ pin: Records.decodeSetupRequest(body).pin, request_body: body });
  });
  return { issue, pin };
});

/** One `tools/call` over HTTP through `/mcp`; every request is authenticated by the real middleware. */
const callProbe = (token: string, extraHeaders: Readonly<Record<string, string>> = {}) =>
  Effect.gen(function* () {
    const http = yield* HttpClient.HttpClient;
    const base = {
      accept: "application/json, text/event-stream",
      authorization: `Bearer ${token}`,
    };
    const rpc = (id: number, method: string, params: unknown, headers: Record<string, string>) =>
      http.post("/mcp", {
        headers: { ...base, ...headers },
        body: HttpBody.text(
          JSON.stringify({ jsonrpc: "2.0", id, method, params }),
          "application/json",
        ),
      });
    const initialized = yield* rpc(
      1,
      "initialize",
      {
        protocolVersion: "2025-06-18",
        capabilities: {},
        clientInfo: { name: "native-peer-middleware", version: "1" },
      },
      {},
    );
    yield* initialized.text;
    expect(initialized.status).toBe(200);
    const response = yield* rpc(
      2,
      "tools/call",
      { name: "native_peer_probe", arguments: {} },
      {
        "mcp-session-id": initialized.headers["mcp-session-id"]!,
        "mcp-protocol-version": "2025-06-18",
        ...extraHeaders,
      },
    );
    const text = yield* response.text;
    expect(response.status).toBe(200);
    const message = text.startsWith("data:")
      ? JSON.parse(
          text
            .split("\n")
            .filter((line) => line.startsWith("data:"))
            .map((line) => line.slice(5))
            .find((line) => JSON.parse(line).id === 2)!,
        )
      : JSON.parse(text);
    expect(message.error, text).toBeUndefined();
    expect(message.result?.isError, text).not.toBe(true);
    return yield* decodeObserved(
      message.result.structuredContent ?? JSON.parse(message.result.content[0].text),
    );
  });

const origin = encodeLinkedMessageOrigin({
  schemaVersion: "t3.linked-message-origin/v1",
  environmentId: "source:test",
  thread: { threadId: "source-thread", title: "Source thread" },
});

it.effect(
  "a pinned native peer session authenticates through the real MCP middleware, including after it rebuilds the scope",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* prepare;
        const linked = yield* f.issue(true);
        yield* f.pin(linked);
        yield* serve(yield* Effect.context<Services>());

        // The plain request: the invocation the handler reads is the one registration saw.
        expect(yield* callProbe(linked.token)).toEqual({
          session: "ok",
          pin: "pin:middleware",
          permissions: ["workstreams:read", "workstreams:write"],
          interaction_mode: "default",
        });
        // The middleware rebuilds the scope for a mode limit and for a linked message origin. Native
        // registration must follow the final object, and the limit must narrow the pin's ceiling.
        expect(
          yield* callProbe(linked.token, {
            [Mcp.MODE_LIMIT_HEADER]: "approval-required/plan",
            [LINKED_MESSAGE_ORIGIN_HEADER]: origin,
          }),
        ).toEqual({
          session: "ok",
          pin: "pin:middleware",
          permissions: ["workstreams:read"],
          interaction_mode: "plan",
        });

        // Controls: admission needs both the linked credential and its pin.
        const unpinned = yield* f.issue(true);
        expect(yield* callProbe(unpinned.token)).toMatchObject({
          session: "ok",
          pin: "not_current",
          permissions: [],
        });
        const ordinary = yield* f.issue(false);
        expect(yield* callProbe(ordinary.token)).toMatchObject({
          session: "unauthenticated",
          pin: "unauthenticated",
        });
      }),
    ).pipe(Effect.provide(Layer.mergeAll(services, NodeHttpServer.layerTest))),
);

it.effect(
  "native registration seated on a copy of the final scope leaves every native call unauthenticated",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* prepare;
        const linked = yield* f.issue(true);
        yield* f.pin(linked);
        const real = yield* Authentication.PeerMcpAuthentication;
        // The mis-seat to catch: a hook that registers a different object than the one provided to handlers.
        const misSeated = Context.add(
          yield* Effect.context<Services>(),
          Authentication.PeerMcpAuthentication,
          Authentication.PeerMcpAuthentication.of({
            register: (request, scope) => real.register(request, { ...scope }),
          }),
        );
        yield* serve(misSeated);
        expect(yield* callProbe(linked.token)).toEqual({
          session: "unauthenticated",
          pin: "unauthenticated",
          permissions: [],
          interaction_mode: null,
        });
      }),
    ).pipe(Effect.provide(Layer.mergeAll(services, NodeHttpServer.layerTest))),
);
