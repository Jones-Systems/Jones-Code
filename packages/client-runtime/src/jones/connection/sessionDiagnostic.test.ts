import {
  AuthStandardClientScopes,
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  ServerConfig,
  WsSubscribeServerConfigRpc,
  WS_METHODS,
} from "@t3tools/contracts";
import { RelayAuthInvalidError, RelayEnvironmentConnectScope } from "@t3tools/contracts/relay";
import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as Socket from "effect/socket/Socket";
import { HttpClient } from "effect/http";

import * as Authorization from "../../authorization/service.ts";
import * as TokenStore from "../../authorization/tokenStore.ts";
import * as Connectivity from "../../connection/connectivity.ts";
import { connectionDiagnosisText, presentConnectionDiagnosis } from "../../connection/diagnosis.ts";
import * as Driver from "../../connection/driver.ts";
import {
  AVAILABLE_CONNECTION_STATE,
  RelayConnectionTarget,
  type SupervisorConnectionState,
} from "../../connection/model.ts";
import * as Resolver from "../../connection/resolver.ts";
import * as Supervisor from "../../connection/supervisor.ts";
import * as Wakeups from "../../connection/wakeups.ts";
import * as Capabilities from "../../platform/capabilities.ts";
import * as Relay from "../../relay/managedRelay.ts";
import * as RpcHttp from "../../rpc/http.ts";
import * as RpcSession from "../../rpc/session.ts";
import { fetchEnvironmentSessionState } from "../../state/session.ts";
import { connectionSessionFacts } from "./sessionDiagnostic.ts";

const target = new RelayConnectionTarget({
  environmentId: EnvironmentId.make("synthetic-connect-host"),
  label: "Synthetic Mac",
});
const endpoint = {
  httpBaseUrl: "https://synthetic-host.example.test",
  wsBaseUrl: "wss://synthetic-host.example.test",
  providerKind: "cloudflare_tunnel" as const,
};
const descriptor = {
  environmentId: target.environmentId,
  label: target.label,
  platform: { os: "darwin", arch: "arm64" },
  serverVersion: "0.0.0-test",
  capabilities: { repositoryIdentity: true, connectionProbe: true },
} as const;
const authPolicy = {
  policy: "remote-reachable",
  bootstrapMethods: ["one-time-token"],
  sessionMethods: ["dpop-access-token"],
  sessionCookieName: "t3_session",
} as const;
const config = Schema.encodeSync(ServerConfig)({
  environment: descriptor,
  auth: authPolicy,
  cwd: "/synthetic/workspace",
  keybindingsConfigPath: "/synthetic/keybindings.json",
  keybindings: [],
  issues: [],
  providers: [],
  availableEditors: [],
  observability: {
    logsDirectoryPath: "/synthetic/logs",
    localTracingEnabled: false,
    otlpTracesEnabled: false,
    otlpMetricsEnabled: false,
    otlpLogsEnabled: false,
  },
  settings: DEFAULT_SERVER_SETTINGS,
});

const Request = Schema.TaggedStruct("Request", {
  id: Schema.Union([Schema.String, Schema.Number]),
  tag: Schema.String,
});
const isRequest = Schema.is(Request);
type Request = typeof Request.Type;
const encodeConfigStreamExit = Schema.encodeSync(
  Schema.toCodecJson(
    Schema.Exit(Schema.Void, WsSubscribeServerConfigRpc.successSchema.error, Schema.Defect()),
  ),
);
type SocketEvent = {
  readonly type: string;
  readonly data?: string;
  readonly code?: number;
  readonly reason?: string;
};

// Only the external socket is stubbed. The RPC protocol and initial-config
// readiness gate are the production RpcSessionFactory, not a ready fixture.
class SyntheticSocket {
  readonly CONNECTING = 0;
  readonly OPEN = 1;
  readonly CLOSING = 2;
  readonly CLOSED = 3;
  readyState = 0;
  private readonly listeners = new Map<string, Set<(event: SocketEvent) => void>>();
  readonly url: string;
  private readonly listening: Deferred.Deferred<SyntheticSocket>;
  private readonly requests: Queue.Queue<Request>;

  constructor(
    url: string,
    listening: Deferred.Deferred<SyntheticSocket>,
    requests: Queue.Queue<Request>,
  ) {
    this.url = url;
    this.listening = listening;
    this.requests = requests;
  }

  addEventListener(type: string, listener: (event: SocketEvent) => void) {
    const listeners = this.listeners.get(type) ?? new Set();
    listeners.add(listener);
    this.listeners.set(type, listeners);
    if (type === "open") Deferred.doneUnsafe(this.listening, Effect.succeed(this));
  }

  removeEventListener(type: string, listener: (event: SocketEvent) => void) {
    this.listeners.get(type)?.delete(listener);
  }

  send(data: string) {
    const message: unknown = JSON.parse(data);
    if (isRequest(message)) Queue.offerUnsafe(this.requests, message);
  }

  open() {
    this.readyState = this.OPEN;
    this.emit({ type: "open" });
  }

  reply(message: unknown) {
    this.emit({ type: "message", data: JSON.stringify(message) });
  }

  close(code = 1000, reason = "") {
    if (this.readyState === this.CLOSED) return;
    this.readyState = this.CLOSED;
    this.emit({ type: "close", code, reason });
  }

  private emit(event: SocketEvent) {
    for (const listener of this.listeners.get(event.type) ?? []) listener(event);
  }
}

type Scenario = "success" | "bootstrap_refused" | "token_refused" | "descriptor_mismatch";

const harness = Effect.fn("ConnectDiagnosticTest.harness")(function* (scenario: Scenario) {
  const requests: Array<{ readonly path: string; readonly authorization: string | null }> = [];
  const bootstrapCalls = yield* Ref.make(0);
  const listening = yield* Deferred.make<SyntheticSocket>();
  const rpcRequests = yield* Queue.unbounded<Request>();
  const tokens = yield* Ref.make(Option.none<TokenStore.RemoteDpopAccessToken>());
  const fetchFn: typeof fetch = async (input, init) => {
    const path = new URL(String(input)).pathname;
    const headers = new Headers(init?.headers);
    requests.push({ path, authorization: headers.get("authorization") });
    if (path === "/.well-known/t3/environment") {
      return Response.json({
        ...descriptor,
        environmentId: scenario === "descriptor_mismatch" ? "another-host" : target.environmentId,
      });
    }
    if (path === "/oauth/token") {
      expect(init?.method).toBe("POST");
      expect(headers.get("dpop")).toBe("synthetic-proof");
      const body =
        typeof init?.body === "string"
          ? init.body
          : init?.body instanceof Uint8Array
            ? new TextDecoder().decode(init.body)
            : "";
      expect(new URLSearchParams(body).get("subject_token")).toBe("synthetic-bootstrap");
      if (scenario === "token_refused") {
        return Response.json(
          {
            _tag: "EnvironmentAuthInvalidError",
            code: "auth_invalid",
            reason: "invalid_credential",
            traceId: "synthetic-token-refusal",
          },
          { status: 401 },
        );
      }
      return Response.json({
        access_token: "synthetic-access",
        issued_token_type: "urn:ietf:params:oauth:token-type:access_token",
        token_type: "DPoP",
        expires_in: 3600,
        scope: AuthStandardClientScopes.join(" "),
      });
    }
    if (path === "/api/auth/websocket-ticket/rpc") {
      expect(headers.get("authorization")).toBe("DPoP synthetic-access");
      expect(headers.get("dpop")).toBe("synthetic-proof");
      return Response.json({ ticket: "synthetic-ticket", expiresAt: "2099-01-01T00:00:00Z" });
    }
    if (
      path === "/api/auth/session" &&
      headers.get("authorization") === "DPoP synthetic-access" &&
      headers.has("dpop")
    ) {
      return Response.json({
        authenticated: true,
        auth: authPolicy,
        scopes: AuthStandardClientScopes,
        sessionMethod: "dpop-access-token",
      });
    }
    throw new Error(`Unexpected synthetic request: ${path}`);
  };
  const httpLayer = RpcHttp.layerRemoteHttpClient(fetchFn);
  const http = yield* HttpClient.HttpClient.pipe(Effect.provide(httpLayer));
  const signer = Relay.ManagedRelayDpopSigner.of({
    thumbprint: Effect.succeed("synthetic-thumbprint"),
    createProof: () => Effect.succeed("synthetic-proof"),
  });
  const relay = Layer.mock(Relay.ManagedRelayClient)({
    relayUrl: "https://synthetic-relay.example.test",
    connectEnvironment: (request) => {
      expect(request).toMatchObject({
        environmentId: target.environmentId,
        clerkToken: "synthetic-clerk",
        scopes: [RelayEnvironmentConnectScope],
      });
      return Ref.update(bootstrapCalls, (count) => count + 1).pipe(
        Effect.andThen(
          scenario === "bootstrap_refused"
            ? Effect.fail(
                new Relay.ManagedRelayRequestFailedError({
                  action: "connect relay environment",
                  cause: null,
                  relayError: new RelayAuthInvalidError({
                    code: "auth_invalid",
                    reason: "not_authorized",
                    traceId: "synthetic-bootstrap-refusal",
                  }),
                }),
              )
            : Effect.succeed({
                environmentId: target.environmentId,
                endpoint,
                credential: "synthetic-bootstrap",
                expiresAt: "2099-01-01T00:00:00Z",
              }),
        ),
      );
    },
  });
  // Keep the authorization service's renewal scope alive for the whole test.
  const authorizationContext = yield* Layer.build(
    Authorization.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          httpLayer,
          relay,
          Layer.succeed(Relay.ManagedRelayDpopSigner, signer),
          Layer.succeed(Capabilities.CloudSession, {
            identity: Effect.succeedSome({ accountId: "synthetic-account" }),
            clerkToken: Effect.succeed("synthetic-clerk"),
          }),
          Layer.succeed(Capabilities.RelayDeviceIdentity, { deviceId: Effect.succeedNone }),
          Layer.succeed(Capabilities.ClientPresentation, {
            metadata: { label: "Synthetic client", deviceType: "desktop", os: "macOS" },
          }),
          Layer.succeed(TokenStore.RemoteDpopAccessTokenStore, {
            get: () => Ref.get(tokens),
            put: (token) => Ref.set(tokens, Option.some(token)),
            remove: () => Ref.set(tokens, Option.none()),
          }),
        ),
      ),
    ),
  );
  const authorization = yield* Authorization.RemoteEnvironmentAuthorization.pipe(
    Effect.provide(authorizationContext),
  );
  const sessions = yield* RpcSession.make().pipe(
    Effect.provideService(
      Socket.WebSocketConstructor,
      (url) => new SyntheticSocket(url, listening, rpcRequests) as unknown as WebSocket,
    ),
  );
  const driver = yield* Driver.ConnectionDriver.pipe(
    Effect.provide(
      Driver.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(HttpClient.HttpClient, http),
            Layer.succeed(RpcSession.RpcSessionFactory, sessions),
            Layer.succeed(Authorization.RemoteEnvironmentAuthorization, authorization),
            Layer.mock(Resolver.ConnectionResolver)({
              prepare: (entry) =>
                authorization
                  .authorizeDpop({ expectedEnvironmentId: entry.target.environmentId })
                  .pipe(Effect.map((prepared) => ({ ...prepared, target: entry.target }))),
            }),
          ),
        ),
      ),
    ),
  );
  const supervisor = yield* Supervisor.make({ target, profile: Option.none(), enabled: true }).pipe(
    Effect.provideService(Driver.ConnectionDriver, driver),
    Effect.provideService(Connectivity.Connectivity, {
      status: Effect.succeed("online" as const),
      changes: Stream.empty,
    }),
    Effect.provideService(Wakeups.ConnectionWakeups, { changes: Stream.empty }),
  );
  return {
    supervisor,
    listening,
    rpcRequests,
    requests,
    bootstrapCalls,
    authorization,
    signer,
    http,
  };
});

const awaitState = (
  supervisor: Supervisor.EnvironmentSupervisor["Service"],
  predicate: (state: SupervisorConnectionState) => boolean,
) =>
  SubscriptionRef.changes(supervisor.state).pipe(
    Stream.filter(predicate),
    Stream.runHead,
    Effect.map(Option.getOrThrow),
  );

describe("Connect session diagnostics", () => {
  it.effect(
    "anonymous host 403 coexists with shared authorization, authenticated read, and synchronized RPC",
    () =>
      Effect.gen(function* () {
        const test = yield* harness("success");
        // The host's source path and this device's source path may receive
        // different answers at the same public URL.
        const hostHttp = yield* HttpClient.HttpClient.pipe(
          Effect.provide(
            RpcHttp.layerRemoteHttpClient(async (input) => {
              expect(String(input)).toBe(`${endpoint.httpBaseUrl}/.well-known/t3/environment`);
              return new Response("", { status: 403 });
            }),
          ),
        );
        const anonymous = yield* hostHttp
          .get(`${endpoint.httpBaseUrl}/.well-known/t3/environment`)
          .pipe(Effect.scoped);
        expect(anonymous.status).toBe(403);
        yield* test.supervisor.connect;
        const socket = yield* Deferred.await(test.listening);
        expect(new URL(socket.url).searchParams.get("wsTicket")).toBe("synthetic-ticket");
        socket.open();
        const subscription = yield* Queue.take(test.rpcRequests);
        expect(subscription.tag).toBe(WS_METHODS.subscribeServerConfig);
        const synchronizing = yield* awaitState(
          test.supervisor,
          (state) => state.stage === "synchronizing",
        );
        expect(connectionSessionFacts(synchronizing)).toContainEqual([
          "RPC readiness",
          "not ready (waiting for initial configuration)",
        ]);
        socket.reply({
          _tag: "Chunk",
          requestId: subscription.id,
          values: [{ version: 1, type: "snapshot", config }],
        });
        const connected = yield* awaitState(
          test.supervisor,
          (state) => state.phase === "connected",
        );
        const prepared = Option.getOrThrow(yield* SubscriptionRef.get(test.supervisor.prepared));
        const session = yield* fetchEnvironmentSessionState({
          prepared,
          signer: Option.some(test.signer),
          remoteAuthorization: Option.some(test.authorization),
        }).pipe(Effect.provideService(HttpClient.HttpClient, test.http));
        expect(session.authenticated).toBe(true);
        expect(yield* Ref.get(test.bootstrapCalls)).toBe(1);
        expect(test.requests.map((request) => request.path)).toContain("/oauth/token");
        expect(test.requests).toContainEqual({
          path: "/api/auth/session",
          authorization: "DPoP synthetic-access",
        });
        expect(connectionSessionFacts(connected)).toContainEqual([
          "RPC readiness",
          "ready (current connection synchronized its initial configuration)",
        ]);
        // The diagnostic does not invent retained HTTP/bootstrap receipts from a
        // successful socket, even though this test separately proved those calls.
        expect(connectionSessionFacts(connected)).toContainEqual([
          "Authenticated HTTP read",
          "not checked (not recorded by connection state)",
        ]);
        // End the server-owned stream before disposing its transport. Closing
        // the socket first races supervisor disposal against the protocol's
        // failure broadcast, leaving cancellation waiting on a closed writer.
        socket.reply({
          _tag: "Exit",
          requestId: subscription.id,
          exit: encodeConfigStreamExit(Exit.void),
        });
        const disconnected = yield* awaitState(
          test.supervisor,
          (state) => state.phase === "backoff",
        );
        const diagnosis = presentConnectionDiagnosis({
          hostLabel: target.label,
          state: disconnected,
          via: "relay",
        });
        // Established-session loss clears lastFailure; a later attempt must
        // fail before Details has a new failure to diagnose.
        expect(diagnosis).toBeNull();
        expect(socket.readyState).toBe(socket.CLOSED);
        expect(connectionSessionFacts(disconnected)).toContainEqual([
          "RPC readiness",
          "not checked for a current connected session",
        ]);
      }),
  );

  it.effect.each(["bootstrap_refused", "token_refused", "descriptor_mismatch"] as const)(
    "keeps %s separate and does not claim later stages",
    (scenario) =>
      Effect.gen(function* () {
        const test = yield* harness(scenario);
        yield* test.supervisor.connect;
        const state = yield* awaitState(test.supervisor, (state) => state.phase === "blocked");
        const diagnosis = presentConnectionDiagnosis({
          hostLabel: target.label,
          state,
          via: "relay",
        });
        expect(diagnosis?.hops.find((hop) => hop.result === "failed")?.hop).toBe(
          scenario === "bootstrap_refused" ? "relay" : "server",
        );
        expect(state.lastFailure?.reason).toBe(
          scenario === "descriptor_mismatch" ? "configuration" : "authentication",
        );
        expect(
          test.requests.some((request) => request.path === "/api/auth/websocket-ticket/rpc"),
        ).toBe(false);
        expect(test.requests.some((request) => request.path === "/api/auth/session")).toBe(false);
        if (scenario === "descriptor_mismatch") {
          expect(test.requests.some((request) => request.path === "/oauth/token")).toBe(false);
        }
        expect(connectionDiagnosisText(target.label, diagnosis!)).toContain(
          "RPC readiness: not checked",
        );
      }),
  );

  it("never promotes an opening socket or an old failure into ready evidence", () => {
    for (const state of [
      { ...AVAILABLE_CONNECTION_STATE, desired: true, phase: "connecting", stage: "opening" },
      { ...AVAILABLE_CONNECTION_STATE, desired: true, phase: "backoff", generation: 8 },
    ] satisfies SupervisorConnectionState[]) {
      expect(
        connectionSessionFacts(state).find(([label]) => label === "RPC readiness")?.[1],
      ).toContain("not checked");
    }
  });
});
