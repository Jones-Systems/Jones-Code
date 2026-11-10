// @effect-diagnostics nodeBuiltinImport:off -- Synthetic remote transport; retained fixture files remain outside source.
import * as NodeCrypto from "node:crypto";
import * as Index from "../crypto/identityIndex.ts";
import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Schema from "effect/Schema";
import * as Wire from "@t3tools/contracts";
import * as Parent from "@t3tools/contracts/jones/workstreamsForwardQualification";
import * as Carrier from "@t3tools/contracts/jones/workstreamsRetainedSourceEffect";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpResponse from "effect/http/HttpClientResponse";
import * as Peer from "../../../../peer/PeerMcpClient.ts";
import * as Links from "../../../../peer/PeerLinks.ts";
import * as Environment from "../../../../environment/ServerEnvironment.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import migration from "../../../persistence/Migrations/119_JonesPeerEnvironmentLinks.ts";
import * as Fixture from "../runtime/peerEffectRecovery.testkit.ts";
import * as Source from "./peerEffectSource.ts";
import * as Recovery from "./peerEffectRecovery.ts";
import * as Forward from "@t3tools/contracts/jones/workstreamsForwarding";
import * as Qualification from "./sourceForwardQualification.ts";
import * as Integration from "../integration.ts";
import * as Home from "./homeForwardPort.ts";
import * as Initial from "./peerEffectAdmission.ts";
import * as Runner from "../registry/effectRunner.ts";
import * as Registry from "../registry/service.ts";
import * as Session from "../registry/session.ts";
import * as Recorded from "../runtime/recordedCaller.ts";
import * as Resolver from "../providers/referenceResolver.ts";
import * as Settlement from "../providers/localSettlement.ts";
import * as GitHub from "../providers/githubPrObserver.ts";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";

vi.mock("../../../../../package.json", () => ({
  default: {
    version: "0.0.0-preview.20261002.100",
    jonesSource: {
      repository: "Jones-Systems/Jones-Code",
      sha: "a".repeat(40),
      tree: "b".repeat(40),
    },
  },
}));
type ParentRequest = Parent.WorkstreamsSourceParentReadmissionRequestV3;
type CarrierRequest = Carrier.WorkstreamsRetainedSourceEffectRequestV1;
interface Call {
  name: string;
  arguments: Record<string, unknown>;
  limits: string | undefined;
}
interface Control {
  calls: Call[];
  methods: string[];
  parent: (request: ParentRequest) => unknown;
  result: (request: CarrierRequest) => unknown;
  beforeReply?: (call: Call) => Effect.Effect<void>;
  relinkAtResolve: boolean;
  status: number;
  githubCalls: GitHub.GitHubPrHttpRequest[];
  githubStatus: number;
  beforeGitHub?: () => Effect.Effect<void>;
}
const encodeAttestationRequest = Schema.encodeSync(
  Schema.fromJsonString(Wire.WorkstreamsNativeAttestationRequest),
);
const providerUnknown = (request: CarrierRequest) => ({
  contract: Carrier.WORKSTREAMS_SOURCE_RETAINED_EFFECT_CONTRACT_V1,
  state: "result",
  request,
  result: {
    protocol: "workstreams-t3-provider/2.0.0",
    state: "unknown",
    request: request.request,
    reason: "attempt_not_found",
  },
});
const harness = (githubSource: string | null = "github:test") => {
  const control: Control = {
    calls: [],
    methods: [],
    parent: () => undefined,
    result: providerUnknown,
    relinkAtResolve: false,
    status: 200,
    githubCalls: [],
    githubStatus: 200,
  };
  const http = Layer.effect(
    HttpClient.HttpClient,
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      return HttpClient.make((request) =>
        Effect.gen(function* () {
          expect(Option.isNone(yield* Effect.serviceOption(sql.transactionService))).toBe(true);
          if (request.body._tag !== "Uint8Array")
            return yield* Effect.die("Expected MCP request bytes");
          const rpc = JSON.parse(new TextDecoder().decode(request.body.body));
          control.methods.push(rpc.method);
          let reply: Response;
          if (rpc.method === "initialize")
            reply = Response.json(
              {
                jsonrpc: "2.0",
                id: rpc.id,
                result: {
                  protocolVersion: "2025-06-18",
                  capabilities: {},
                  serverInfo: { name: "synthetic-source", version: "1" },
                },
              },
              { headers: { "mcp-session-id": "synthetic-peer-source-session" } },
            );
          else if (rpc.method === "notifications/initialized")
            reply = new Response(null, { status: 202 });
          else {
            expect(rpc.method).toBe("tools/call");
            expect(request.headers.authorization).toBe("Bearer synthetic-retained-source-token");
            const call = { ...rpc.params, limits: request.headers[Mcp.MODE_LIMIT_HEADER] };
            control.calls.push(call);
            if (control.beforeReply) yield* control.beforeReply(call);
            reply =
              control.status === 200
                ? Response.json({
                    jsonrpc: "2.0",
                    id: rpc.id,
                    result: {
                      content: [],
                      structuredContent:
                        rpc.params.name === "jones_workstreams_source_readmission"
                          ? control.parent(rpc.params.arguments)
                          : control.result(rpc.params.arguments),
                    },
                  })
                : new Response(null, { status: control.status });
          }
          return HttpResponse.fromWeb(request, reply);
        }),
      );
    }),
  );
  const links = Layer.effect(
    Links.PeerLinks,
    Effect.gen(function* () {
      const actual = yield* Links.PeerLinks,
        sql = yield* SqlClient.SqlClient;
      return Links.PeerLinks.of({
        ...actual,
        resolve: (environmentId) =>
          Effect.gen(function* () {
            expect(environmentId).toBe("source:test");
            expect(Option.isNone(yield* Effect.serviceOption(sql.transactionService))).toBe(true);
            if (control.relinkAtResolve)
              yield* sql
                .unsafe(
                  "UPDATE jones_peer_environment_links SET secret_name='replacement-home-to-source'",
                )
                .pipe(Effect.orDie);
            const generation = yield* actual.currentGeneration(environmentId);
            if (Option.isNone(generation)) return yield* Effect.die("Missing synthetic link");
            return {
              url: "https://retained-source.invalid",
              token: "synthetic-retained-source-token",
              generation: generation.value,
              link: {} as Links.ResolvedPeerLink["link"],
            };
          }),
      });
    }),
  ).pipe(Layer.provide(Links.layer), Layer.provide(Layer.mock(Environment.ServerEnvironment)({})));
  const github =
    githubSource === null
      ? GitHub.unconfiguredLayer
      : GitHub.layer.pipe(
          Layer.provide(
            Layer.merge(
              Layer.succeed(GitHub.GitHubPrAccess, {
                source_instance_id: githubSource,
                authorization_header: "Bearer synthetic-github-credential",
                user_agent: "native-peer-pr-test",
              }),
              Layer.effect(
                GitHub.GitHubPrTransport,
                Effect.gen(function* () {
                  const sql = yield* SqlClient.SqlClient;
                  return GitHub.GitHubPrTransport.of({
                    get: (request) =>
                      Effect.gen(function* () {
                        expect(
                          Option.isNone(yield* Effect.serviceOption(sql.transactionService)),
                        ).toBe(true);
                        expect(request.url).toBe(
                          "https://api.github.com/repos/jones/example/pulls/7",
                        );
                        expect(request.redirect).toBe("error");
                        expect(request.headers.authorization).toBe(
                          "Bearer synthetic-github-credential",
                        );
                        control.githubCalls.push(request);
                        if (control.beforeGitHub) yield* control.beforeGitHub();
                        return {
                          status: control.githubStatus,
                          headers: { etag: '"synthetic-peer-pr"' },
                          body: new TextEncoder().encode(
                            JSON.stringify({
                              number: 7,
                              state: "open",
                              draft: false,
                              merged: false,
                              merged_at: null,
                              updated_at: "2026-01-01T00:00:00.000Z",
                              base: { repo: { full_name: "jones/example" } },
                            }),
                          ),
                        };
                      }),
                  });
                }),
              ),
            ),
          ),
        );
  const layer = Layer.mergeAll(Source.layer, Qualification.layer).pipe(
    Layer.provideMerge(github),
    Layer.provideMerge(Peer.layer.pipe(Layer.provideMerge(links), Layer.provideMerge(http))),
  );
  return { control, layer };
};

type RecoveryFixture = Parameters<Parameters<typeof Fixture.withFixture>[0]>[0];
const encode = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeForward = Schema.decodeUnknownEffect(Forward.WorkstreamsForwardRequest);
const hash = (value: string) => NodeCrypto.createHash("sha256").update(value).digest("hex");
const withHome = <A, E, R>(
  f: RecoveryFixture,
  h: ReturnType<typeof harness>,
  use: (
    tools: Home.HomeTools["Service"],
    runner: Runner.EffectRunner["Service"],
  ) => Effect.Effect<A, E, R>,
  intercept?: (
    ticket: Initial.InitialPeerTicket,
    run: () => Effect.Effect<Wire.WorkstreamReceipt | null, Session.RegistryError>,
    recovery: Recovery.PeerEffectRecovery["Service"],
  ) => Effect.Effect<Wire.WorkstreamReceipt | null, Session.RegistryError>,
) =>
  Effect.gen(function* () {
    yield* migration;
    yield* f.sql.unsafe(
      "INSERT INTO jones_peer_environment_links (environment_id,secret_name,label,urls_json,access,linked_at,expires_at,fenced) VALUES ('source:test','home-to-source:original','synthetic-source','[\"https://retained-source.invalid\"]','full-access','2026-01-01T00:00:00.000Z','2026-01-02T00:00:00.000Z',1)",
    );
    const request = yield* decodeForward(f.forward);
    h.control.parent = (lookup) => ({
      contract: Parent.WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V3,
      request: lookup,
      state: "authorized",
      forward_request_sha256: hash(encode(request)),
      original_ceiling: f.envelope.recovery.original_ceiling,
      current_ceiling: f.envelope.recovery.original_ceiling,
      observed_at: "2026-01-01T00:00:00.000Z",
    });
    const reference: typeof f.intent.reference = {
      ...structuredClone(f.intent.reference),
      registration:
        f.command.action.operation === "request_native_t3_settlement"
          ? {
              state: "attested",
              attestation_version: 1,
              attested_at: "2026-01-01T00:00:00.000Z",
              expires_at: "2026-01-01T00:05:00.000Z",
              evidence: {
                provider: "t3",
                source_instance_id: "source:test",
                native_id: "thread:original",
                authority_namespace: "source-store",
                store_generation: 3,
                evidence_sha256: "e".repeat(64),
              },
            }
          : f.intent.reference.registration,
    };
    if (f.intent.kind !== "refresh_linked_pr")
      yield* f.registry.put(
        Fixture.scope,
        "references",
        {
          metadata: {
            native_reference_id: reference.native_reference_id,
            provider: "t3",
            resource_kind: "thread",
            id_kind: "internal",
            identity_index: yield* (yield* Index.WorkstreamsIdentityIndex).digest(
              Fixture.scope,
              reference.identity,
            ),
            legacy_identity_sha256: null,
            attestation_version: reference.registration.attestation_version,
            registration_state: reference.registration.state,
            created_by_principal: "principal:test",
            routing_source_instance_id: reference.registration.evidence?.source_instance_id ?? null,
            routing_native_id: reference.registration.evidence?.native_id ?? null,
            routing_authority_namespace:
              reference.registration.evidence?.authority_namespace ?? null,
            routing_store_generation: reference.registration.evidence?.store_generation ?? null,
            routing_attested_at: reference.registration.attested_at,
            routing_expires_at: reference.registration.expires_at,
            routing_evidence_sha256: reference.registration.evidence?.evidence_sha256 ?? null,
          },
          value: reference,
        },
        null,
      );
    // HOME-only ports refuse. PEER uses actual authentication, owner, SQL, crypto and transport.
    const local = Layer.succeed(
      Session.Session,
      Session.Session.of({
        authorize: () => Effect.fail(new Session.RegistryError({ code: "home_route_required" })),
      }),
    );
    const homePorts = Layer.mergeAll(
      Layer.mock(Recorded.RecordedCallers)({}),
      Layer.mock(Resolver.ReferenceResolver)({}),
      Layer.mock(Settlement.SourceSettlement)({}),
      Registry.layer.pipe(Layer.provide(local)),
    );
    const peer = Recovery.layer.pipe(Layer.provideMerge(h.layer));
    const runner = Runner.layer.pipe(Layer.provideMerge(peer), Layer.provideMerge(homePorts));
    const initial = Layer.effect(
      Initial.InitialPeerExecution,
      Effect.gen(function* () {
        const actual = yield* Initial.InitialPeerExecution;
        const recovery = yield* Recovery.PeerEffectRecovery;
        return Initial.InitialPeerExecution.of({
          ready: actual.ready,
          run: (ticket) =>
            intercept ? intercept(ticket, () => actual.run(ticket), recovery) : actual.run(ticket),
        });
      }),
    ).pipe(Layer.provideMerge(runner));
    const graph = Integration.nativePeerHomeForwardingLayer.pipe(
      Layer.provideMerge(initial),
      Layer.provideMerge(h.layer),
      Layer.provideMerge(local),
    );
    return yield* Effect.gen(function* () {
      const tools = yield* Effect.serviceOption(Home.HomeTools);
      if (Option.isNone(tools))
        return yield* Effect.die("Genuine application resources did not mount HomeTools");
      return yield* use(tools.value, yield* Runner.EffectRunner);
    }).pipe(Effect.provide(graph));
  });
const rows = (f: RecoveryFixture) =>
  Effect.gen(function* () {
    const parent = yield* f.sql.unsafe<{ state: string; result_envelope: string | null }>(
      "SELECT state,result_envelope FROM jones_workstreams_forward_receipts",
    );
    const receipt = yield* f.sql.unsafe<{ state: string }>(
      "SELECT state FROM jones_workstreams_receipts",
    );
    const effects = yield* f.sql.unsafe<{ state: string }>(
      "SELECT state FROM jones_workstreams_external_reservations",
    );
    return { parent, receipt, effects };
  });
const carriers = (h: ReturnType<typeof harness>) =>
  h.control.calls
    .filter((c) => c.name === "jones_workstreams_source_retained_effect")
    .map((c) => (c.arguments as unknown as CarrierRequest).kind);
const attested = (request: CarrierRequest) => {
  if (request.kind !== "attest") throw new Error("Expected attest");
  const at = "2026-01-01T00:00:00.000Z",
    expires = "2026-01-01T00:05:00.000Z";
  return {
    contract: Carrier.WORKSTREAMS_SOURCE_RETAINED_EFFECT_CONTRACT_V1,
    request,
    state: "result",
    result: {
      protocol: "workstreams-t3-provider/2.0.0",
      state: "attested",
      request: request.request,
      attested_at: at,
      expires_at: expires,
      attestation: {
        provider: "t3",
        source_instance_id: "source:test",
        native_id: "thread:original",
        authority_namespace: "source-store",
        store_generation: 3,
        evidence_sha256: hash(
          encodeAttestationRequest(request.request) + "\n" + at + "\n" + expires,
        ),
      },
    },
  };
};
const denied = () => (request: CarrierRequest) => {
  if (request.kind === "attest") throw new Error("Expected settlement");
  const r = request.request;
  const binding = {
    owner_id: r.owner_id,
    principal_id: r.principal_id,
    command_id: r.command_id,
    native_reference_id: r.native_reference_id,
    native_action: r.native_action,
    request_sha256: r.request_sha256,
    reservation_sha256: r.reservation_sha256,
  };
  return {
    contract: Carrier.WORKSTREAMS_SOURCE_RETAINED_EFFECT_CONTRACT_V1,
    request,
    state: "result",
    result: {
      protocol: "workstreams-t3-provider/2.0.0",
      state: "terminal",
      request: r,
      result: {
        native_outcome: "denied",
        native_evidence: {
          ...binding,
          result_sha256: hash(canonical({ ...binding, native_outcome: "denied" })),
        },
      },
      native_receipt: null,
      settlement_event: null,
    },
  };
};

it.effect(
  "genuine initial verification atomically accepts PEER and completes command plus parent; replay sends no provider call",
  () => {
    const h = harness();
    return Fixture.withFixture(
      (f) =>
        withHome(f, h, (tools) =>
          Effect.gen(function* () {
            h.control.result = attested;
            h.control.beforeReply = (call) =>
              Effect.gen(function* () {
                if (call.name !== "jones_workstreams_source_retained_effect") return;
                expect(yield* rows(f)).toMatchObject({
                  parent: [{ state: "accepted", result_envelope: null }],
                  receipt: [{ state: "pending" }],
                  effects: [{ state: "active" }],
                });
              }).pipe(Effect.orDie);
            const result = yield* tools.forward(f.forward, f.invocation);
            expect(result).toMatchObject({ kind: "command", result: { state: "committed" } });
            expect(yield* rows(f)).toMatchObject({
              parent: [{ state: "completed" }],
              receipt: [{ state: "committed" }],
              effects: [{ state: "released" }],
            });
            const replay = yield* tools.forward(f.forward, f.invocation);
            expect(replay).toEqual(result);
            expect(carriers(h)).toEqual(["attest"]);
            expect(yield* f.effects.active(Fixture.scope)).toEqual([]);
          }),
        ),
      "verify_reference",
      null,
      { accepted: false },
    );
  },
);
it.effect(
  "only fresh initial settlement sends settle; unknown replay and ordinary recovery use settle_lookup",
  () => {
    const h = harness();
    return Fixture.withFixture(
      (f) =>
        withHome(f, h, (tools, runner) =>
          Effect.gen(function* () {
            const result = yield* tools.forward(f.forward, f.invocation);
            expect(result).toMatchObject({ kind: "command", result: { state: "pending" } });
            expect(carriers(h)).toEqual(["settle"]);
            expect(yield* rows(f)).toMatchObject({
              parent: [{ state: "accepted", result_envelope: null }],
              receipt: [{ state: "pending" }],
            });
            expect((yield* tools.forward(f.forward, f.invocation).pipe(Effect.result))._tag).toBe(
              "Failure",
            );
            yield* runner.recoverOne(f.command.command_id);
            yield* runner.recoverOne(f.command.command_id);
            expect(carriers(h)).toEqual(["settle", "settle_lookup", "settle_lookup"]);
            expect(yield* rows(f)).toMatchObject({
              parent: [{ state: "accepted", result_envelope: null }],
            });
          }),
        ),
      "request_native_t3_settlement",
      null,
      { accepted: false },
    );
  },
);
it.effect(
  "fresh terminal settlement completes using retained exact bytes and original receiving pin",
  () => {
    const h = harness();
    return Fixture.withFixture(
      (f) =>
        withHome(f, h, (tools) =>
          Effect.gen(function* () {
            h.control.result = denied();
            const result = yield* tools.forward(f.forward, f.invocation);
            expect(result).toMatchObject({
              kind: "command",
              result: { state: "committed", effects: { native_settlement: { outcome: "denied" } } },
            });
            expect(carriers(h)).toEqual(["settle"]);
            expect(yield* rows(f)).toMatchObject({
              parent: [{ state: "completed" }],
              effects: [{ state: "released" }],
            });
          }),
        ),
      "request_native_t3_settlement",
      null,
      { accepted: false },
    );
  },
);
it.effect(
  "a copied ticket refuses; genuine ticket dispatches once and cannot be spent twice",
  () => {
    const h = harness();
    return Fixture.withFixture(
      (f) =>
        withHome(
          f,
          h,
          (tools) =>
            Effect.gen(function* () {
              h.control.result = denied();
              yield* tools.forward(f.forward, f.invocation);
              expect(carriers(h)).toEqual(["settle"]);
            }),
          (ticket, run, recovery) =>
            Effect.gen(function* () {
              expect((yield* recovery.runInitial({ ...ticket }).pipe(Effect.result))._tag).toBe(
                "Failure",
              );
              const result = yield* run();
              expect((yield* recovery.runInitial(ticket).pipe(Effect.result))._tag).toBe("Failure");
              return result;
            }),
        ),
      "request_native_t3_settlement",
      null,
      { accepted: false },
    );
  },
);
it.effect(
  "failure after acceptance but before dispatch leaves durable pending evidence; recovery never manufactures a fresh ticket",
  () => {
    const h = harness();
    return Fixture.withFixture(
      (f) =>
        withHome(
          f,
          h,
          (tools, runner) =>
            Effect.gen(function* () {
              expect((yield* tools.forward(f.forward, f.invocation).pipe(Effect.result))._tag).toBe(
                "Failure",
              );
              expect(carriers(h)).toEqual([]);
              expect(yield* rows(f)).toMatchObject({
                parent: [{ state: "accepted" }],
                receipt: [{ state: "pending" }],
              });
              yield* runner.recoverOne(f.command.command_id);
              expect(carriers(h)).toEqual(["settle_lookup"]);
            }),
          () => Effect.fail(new Session.RegistryError({ code: "unavailable" })),
        ),
      "request_native_t3_settlement",
      null,
      { accepted: false },
    );
  },
);
it.effect(
  "a final acceptance writer failure rolls parent receipt and reservation back together with no dispatch",
  () => {
    const h = harness();
    return Fixture.withFixture(
      (f) =>
        withHome(f, h, (tools) =>
          Effect.gen(function* () {
            yield* f.sql.unsafe(
              "CREATE TRIGGER synthetic_abort_reservation AFTER INSERT ON jones_workstreams_external_reservations BEGIN SELECT RAISE(ABORT,'synthetic acceptance failure'); END",
            );
            expect((yield* tools.forward(f.forward, f.invocation).pipe(Effect.result))._tag).toBe(
              "Failure",
            );
            expect(yield* rows(f)).toEqual({ parent: [], receipt: [], effects: [] });
            expect(carriers(h)).toEqual([]);
          }),
        ),
      "verify_reference",
      null,
      { accepted: false },
    );
  },
);
it.effect(
  "replacing the original registration with identical tuple data after commit refuses initial dispatch",
  () => {
    const h = harness();
    return Fixture.withFixture(
      (f) =>
        withHome(
          f,
          h,
          (tools) =>
            Effect.gen(function* () {
              expect((yield* tools.forward(f.forward, f.invocation).pipe(Effect.result))._tag).toBe(
                "Failure",
              );
              expect(yield* rows(f)).toMatchObject({
                parent: [{ state: "accepted" }],
                receipt: [{ state: "pending" }],
              });
              expect(carriers(h)).toEqual([]);
            }),
          (_ticket, run) =>
            Effect.gen(function* () {
              yield* f.authenticateOriginal().pipe(Effect.orDie);
              return yield* run();
            }),
        ),
      "request_native_t3_settlement",
      null,
      { accepted: false },
    );
  },
);

it.effect(
  "a ticket cannot escape its completed invocation even when the accepted record still exists",
  () => {
    const h = harness();
    let saved: Initial.InitialPeerTicket | null = null;
    return Fixture.withFixture(
      (f) =>
        withHome(
          f,
          h,
          (tools) =>
            Effect.gen(function* () {
              expect(yield* tools.forward(f.forward, f.invocation)).toMatchObject({
                result: { state: "pending" },
              });
              if (!saved) return yield* Effect.die("Fresh acceptance did not produce a ticket");
              const recovery = yield* Recovery.PeerEffectRecovery;
              expect((yield* recovery.runInitial(saved).pipe(Effect.result))._tag).toBe("Failure");
              expect(carriers(h)).toEqual([]);
              expect(yield* rows(f)).toMatchObject({
                parent: [{ state: "accepted" }],
                receipt: [{ state: "pending" }],
              });
            }),
          (ticket) => {
            saved = ticket;
            return Effect.succeed(null);
          },
        ),
      "request_native_t3_settlement",
      null,
      { accepted: false },
    );
  },
);
it.effect(
  "fresh source denial after acceptance refuses initial dispatch while retaining the original pending record",
  () => {
    const h = harness();
    return Fixture.withFixture(
      (f) =>
        withHome(f, h, (tools) =>
          Effect.gen(function* () {
            const parent = h.control.parent;
            let calls = 0;
            h.control.parent = (request) =>
              ++calls === 1
                ? parent(request)
                : {
                    contract: Parent.WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V3,
                    request,
                    state: "denied",
                    reason: "not_found",
                  };
            expect((yield* tools.forward(f.forward, f.invocation).pipe(Effect.result))._tag).toBe(
              "Failure",
            );
            expect(carriers(h)).toEqual([]);
            expect(yield* rows(f)).toMatchObject({
              parent: [{ state: "accepted" }],
              receipt: [{ state: "pending" }],
            });
          }),
        ),
      "request_native_t3_settlement",
      null,
      { accepted: false },
    );
  },
);
it.effect(
  "a lost initial reply remains ambiguous and ordinary recovery never replays settle",
  () => {
    const h = harness();
    return Fixture.withFixture(
      (f) =>
        withHome(f, h, (tools, runner) =>
          Effect.gen(function* () {
            h.control.beforeReply = (call) =>
              Effect.sync(() => {
                if (call.name === "jones_workstreams_source_retained_effect")
                  h.control.status = 503;
              });
            expect((yield* tools.forward(f.forward, f.invocation).pipe(Effect.result))._tag).toBe(
              "Failure",
            );
            expect(carriers(h)).toEqual(["settle"]);
            h.control.status = 200;
            delete h.control.beforeReply;
            yield* runner.recoverOne(f.command.command_id);
            expect(carriers(h)).toEqual(["settle", "settle_lookup"]);
            expect(yield* rows(f)).toMatchObject({
              parent: [{ state: "accepted" }],
              receipt: [{ state: "pending" }],
            });
          }),
        ),
      "request_native_t3_settlement",
      null,
      { accepted: false },
    );
  },
);
it.effect("initial dispatch and background lookup share the existing runner semaphore", () => {
  const h = harness();
  return Fixture.withFixture(
    (f) =>
      withHome(f, h, (tools, runner) =>
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>(),
            release = yield* Deferred.make<void>();
          h.control.beforeReply = (call) =>
            Effect.gen(function* () {
              if (
                call.name !== "jones_workstreams_source_retained_effect" ||
                (call.arguments as unknown as CarrierRequest).kind !== "settle"
              )
                return;
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
            });
          const initial = yield* tools.forward(f.forward, f.invocation).pipe(Effect.forkChild);
          yield* Deferred.await(entered);
          const lookup = yield* runner.recoverOne(f.command.command_id).pipe(Effect.forkChild);
          yield* Effect.yieldNow;
          expect(lookup.pollUnsafe()).toBeUndefined();
          expect(carriers(h)).toEqual(["settle"]);
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(initial);
          yield* Fiber.join(lookup);
          expect(carriers(h)).toEqual(["settle", "settle_lookup"]);
        }),
      ),
    "request_native_t3_settlement",
    null,
    { accepted: false },
  );
});
it.effect(
  "a late SQL grant change after reservation rolls all acceptance rows back before any provider call",
  () => {
    const h = harness();
    return Fixture.withFixture(
      (f) =>
        withHome(f, h, (tools) =>
          Effect.gen(function* () {
            yield* f.sql.unsafe(
              "CREATE TRIGGER synthetic_acceptance_grant AFTER INSERT ON jones_workstreams_external_reservations BEGIN INSERT INTO jones_workstreams_grants (registry_instance_id,owner_id,principal_id,grant_version,grant_id,permissions,state) VALUES ('11111111-1111-4111-8111-111111111111','owner:test','principal:test',2,'grant:late-acceptance','[\"workstreams:read\"]','active'); END",
            );
            expect((yield* tools.forward(f.forward, f.invocation).pipe(Effect.result))._tag).toBe(
              "Failure",
            );
            expect(yield* rows(f)).toEqual({ parent: [], receipt: [], effects: [] });
            expect(
              (yield* f.registry.listPlain(Fixture.scope, "grants")).map((x) => x.grant_version),
            ).toEqual([1]);
            expect(carriers(h)).toEqual([]);
          }),
        ),
      "verify_reference",
      null,
      { accepted: false },
    );
  },
);

it.effect(
  "authentic initial result is retained but an equal replacement registration cannot complete the old invocation",
  () => {
    const h = harness();
    return Fixture.withFixture(
      (f) =>
        withHome(f, h, (tools) =>
          Effect.gen(function* () {
            h.control.result = denied();
            h.control.beforeReply = (call) =>
              call.name === "jones_workstreams_source_retained_effect"
                ? f.authenticateOriginal().pipe(Effect.asVoid, Effect.orDie)
                : Effect.void;
            expect((yield* tools.forward(f.forward, f.invocation).pipe(Effect.result))._tag).toBe(
              "Failure",
            );
            expect(carriers(h)).toEqual(["settle"]);
            expect(yield* f.effects.result(Fixture.scope, f.command.command_id)).not.toBeNull();
            expect(yield* rows(f)).toMatchObject({
              parent: [{ state: "accepted", result_envelope: null }],
              receipt: [{ state: "pending" }],
              effects: [{ state: "active" }],
            });
          }),
        ),
      "request_native_t3_settlement",
      null,
      { accepted: false },
    );
  },
);

it.effect(
  "application forwarding stays unmounted without actual transport and runner resources",
  () =>
    Fixture.withFixture(
      () =>
        Effect.gen(function* () {
          expect((yield* Effect.serviceOption(Home.HomeTools))._tag).toBe("None");
        }).pipe(
          Effect.provide(
            Integration.nativePeerHomeForwardingLayer.pipe(
              Layer.provide(
                Layer.succeed(
                  Session.Session,
                  Session.Session.of({
                    authorize: () => Effect.die("Absent mount must not authorize"),
                  }),
                ),
              ),
            ),
          ),
        ),
      "verify_reference",
      null,
      { accepted: false },
    ),
);

it.effect(
  "genuine PEER PR refresh uses the configured GitHub source and atomically completes without an extra relay",
  () => {
    const h = harness();
    return Fixture.withFixture(
      (f) =>
        withHome(f, h, (tools) =>
          Effect.gen(function* () {
            h.control.beforeGitHub = () =>
              Effect.gen(function* () {
                expect(yield* rows(f)).toMatchObject({
                  parent: [{ state: "accepted", result_envelope: null }],
                  receipt: [{ state: "pending" }],
                  effects: [{ state: "active" }],
                });
              }).pipe(Effect.orDie);
            const result = yield* tools.forward(f.forward, f.invocation);
            expect(result).toMatchObject({
              kind: "command",
              result: {
                command_id: f.command.command_id,
                request_sha256: hash(f.forward.request.body),
                state: "committed",
                effects: {
                  observation: {
                    native_reference_id: "ref:test",
                    observation_version: 1,
                    outcome: "observed",
                    last_success: { state: "open", draft: false },
                    command_id: f.command.command_id,
                  },
                },
              },
              home: {
                incoming_peer_pin_id: f.pin.pin_id,
                incoming_peer_pin_revision: f.pin.revision,
              },
            });
            expect(yield* rows(f)).toMatchObject({
              parent: [{ state: "completed" }],
              receipt: [{ state: "committed" }],
              effects: [{ state: "released" }],
            });
            expect(yield* tools.forward(f.forward, f.invocation)).toEqual(result);
            expect(h.control.githubCalls).toHaveLength(1);
            expect(carriers(h)).toEqual([]);
            expect(
              h.control.calls.every((call) => call.name === "jones_workstreams_source_readmission"),
            ).toBe(true);
            expect(f.identity.source_authority.environment_id).toBe("source:test");
            expect(f.intent.source_binding.source_instance_id).toBe("github:test");
            expect(yield* f.sql`SELECT * FROM jones_workstreams_pr_observations`).toHaveLength(1);
          }),
        ),
      "refresh_linked_pr",
      null,
      { accepted: false },
    );
  },
);

it.effect(
  "initial PEER PR success is retained but an identical replacement registration cannot complete its old invocation",
  () => {
    const h = harness();
    return Fixture.withFixture(
      (f) =>
        withHome(f, h, (tools) =>
          Effect.gen(function* () {
            h.control.beforeGitHub = () =>
              f.authenticateOriginal().pipe(Effect.asVoid, Effect.orDie);
            expect((yield* tools.forward(f.forward, f.invocation).pipe(Effect.result))._tag).toBe(
              "Failure",
            );
            expect(h.control.githubCalls).toHaveLength(1);
            expect(yield* f.effects.result(Fixture.scope, f.command.command_id)).toMatchObject({
              observation: { outcome: "observed", command_id: f.command.command_id },
            });
            expect(yield* rows(f)).toMatchObject({
              parent: [{ state: "accepted", result_envelope: null }],
              receipt: [{ state: "pending" }],
              effects: [{ state: "active" }],
            });
            expect(yield* f.sql`SELECT * FROM jones_workstreams_pr_observations`).toHaveLength(0);
          }),
        ),
      "refresh_linked_pr",
      null,
      { accepted: false },
    );
  },
);

it.effect(
  "initial PEER PR refresh with no configured GitHub access preserves accepted intent without provider fallback",
  () => {
    const h = harness(null);
    return Fixture.withFixture(
      (f) =>
        withHome(f, h, (tools) =>
          Effect.gen(function* () {
            expect((yield* tools.forward(f.forward, f.invocation).pipe(Effect.result))._tag).toBe(
              "Failure",
            );
            expect(h.control.githubCalls).toHaveLength(0);
            expect(carriers(h)).toEqual([]);
            expect(yield* f.effects.result(Fixture.scope, f.command.command_id)).toBeNull();
            expect(yield* rows(f)).toMatchObject({
              parent: [{ state: "accepted", result_envelope: null }],
              receipt: [{ state: "pending" }],
              effects: [{ state: "active" }],
            });
          }),
        ),
      "refresh_linked_pr",
      null,
      { accepted: false },
    );
  },
);
