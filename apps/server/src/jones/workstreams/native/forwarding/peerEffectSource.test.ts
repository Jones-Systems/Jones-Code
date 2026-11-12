// @effect-diagnostics nodeBuiltinImport:off -- Synthetic remote transport; retained fixture files remain outside source.
import * as NodeCrypto from "node:crypto";
import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Schema from "effect/Schema";
import * as Wire from "@t3tools/contracts";
import * as Parent from "@t3tools/contracts/jones/workstreamsForwardQualification";
import * as Carrier from "@t3tools/contracts/jones/workstreamsRetainedSourceEffect";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpResponse from "effect/http/HttpClientResponse";
import * as DateTime from "effect/DateTime";
import * as Peer from "../../../../peer/PeerMcpClient.ts";
import * as Links from "../../../../peer/PeerLinks.ts";
import * as ServerSettings from "../../../../serverSettings.ts";
import * as Environment from "../../../../environment/ServerEnvironment.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import migration from "../../../persistence/Migrations/119_JonesPeerEnvironmentLinks.ts";
import * as Fixture from "../runtime/peerEffectRecovery.testkit.ts";
import * as Origins from "../runtime/peerEffectOrigin.ts";
import * as Receiving from "../runtime/retainedPeerSession.ts";
import * as Source from "./peerEffectSource.ts";
import { nativeSettlementResult } from "../domain/externalIntents.ts";

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
const harness = () => {
  const control: Control = {
    calls: [],
    methods: [],
    parent: () => undefined,
    result: providerUnknown,
    relinkAtResolve: false,
    status: 200,
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
  ).pipe(
    Layer.provide(Links.layer),
    Layer.provide(ServerSettings.layerTest({ linkedEnvironmentsEnabled: true })),
    Layer.provide(Layer.mock(Environment.ServerEnvironment)({})),
  );
  const layer = Source.layer.pipe(
    Layer.provideMerge(Peer.layer.pipe(Layer.provideMerge(links), Layer.provideMerge(http))),
  );
  return { control, layer };
};
type RecoveryFixture = Parameters<Parameters<typeof Fixture.withFixture>[0]>[0];
const within = <A, E, R>(
  f: RecoveryFixture,
  h: ReturnType<typeof harness>,
  use: (
    source: Source.PeerEffectSource["Service"],
    origin: Origins.ValidatedPeerReservation,
    receiving: Receiving.CurrentRetainedPeer,
  ) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    yield* migration;
    yield* f.sql.unsafe(
      "INSERT INTO jones_peer_environment_links (environment_id,secret_name,label,urls_json,access,linked_at,expires_at,fenced) VALUES ('source:test','home-to-source:original','synthetic-source','[\"https://retained-source.invalid\"]','full-access','2026-01-01T00:00:00.000Z','2026-01-02T00:00:00.000Z',1)",
    );
    h.control.parent = (request) => ({
      contract: Parent.WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V3,
      request,
      state: "authorized",
      forward_request_sha256: f.envelope.forward_request_sha256,
      original_ceiling: f.envelope.recovery.original_ceiling,
      current_ceiling: f.envelope.recovery.original_ceiling,
      observed_at: "2026-01-01T00:00:00.000Z",
    });
    const origins = yield* Origins.PeerEffectOrigins,
      sessions = yield* Receiving.OriginalReceivingSession;
    return yield* origins.withOrigin(f.command.command_id, (origin) =>
      sessions.withCurrent(origin, (receiving) =>
        Effect.gen(function* () {
          return yield* use(yield* Source.PeerEffectSource, origin, receiving);
        }).pipe(Effect.provide(h.layer)),
      ),
    );
  });
it.effect(
  "uses original home-to-source generation and exact retained bytes for lookup, never dispatch",
  () => {
    const h = harness();
    return Fixture.withFixture((f) =>
      within(f, h, (source, origin, receiving) =>
        source.withReadmission(origin, receiving, (p) =>
          Effect.gen(function* () {
            expect(origin.parent.identity.outbound_link_generation).not.toBe(
              origin.envelope.source_link_generation,
            );
            expect(receiving.pin.pin_id).not.toBe(origin.parent.identity.member_pin_id);
            const reply = yield* Source.lookupPeerSourceEffect(p);
            expect(reply).toMatchObject({
              kind: "observation",
              response: {
                state: "result",
                request: { kind: "settle_lookup" },
                result: { state: "unknown", reason: "attempt_not_found" },
              },
            });
            expect(h.control.calls.map((c) => c.name)).toEqual([
              "jones_workstreams_source_readmission",
              "jones_workstreams_source_retained_effect",
            ]);
            const request = h.control.calls[1]!.arguments as unknown as CarrierRequest;
            expect(request).toMatchObject({
              receiving_home_pin_id: f.pin.pin_id,
              receiving_home_pin_revision: 2,
              request: {
                command_id: f.command.command_id,
                request_sha256: f.identity.request_sha256,
                reservation_sha256: NodeCrypto.createHash("sha256")
                  .update(canonical(JSON.parse(f.envelope.intent_body)))
                  .digest("hex"),
              },
            });
            expect((yield* Source.lookupPeerSourceEffect(p).pipe(Effect.flip)).code).toBe(
              "source_observation_consumed",
            );
            yield* f.sql.withTransaction(Source.checkPeerSource(p));
            const rows = yield* f.sql<{
              state: string;
            }>`SELECT state FROM jones_workstreams_external_reservations`;
            expect(rows[0]!.state).toBe("active");
          }),
        ),
      ),
    );
  },
);
it.effect("rejects remote preparation inside an actual writer before transport", () => {
  const h = harness();
  return Fixture.withFixture((f) =>
    within(f, h, (source, origin, receiving) =>
      Effect.gen(function* () {
        const e = yield* f.sql
          .withTransaction(source.withReadmission(origin, receiving, () => Effect.void))
          .pipe(Effect.flip);
        expect(e).toMatchObject({ code: "preparation_in_transaction" });
        expect(h.control.methods).toEqual([]);
      }),
    ),
  );
});
it.effect("refuses an unlinked or replaced captured generation before MCP initialization", () => {
  const h = harness();
  return Fixture.withFixture((f) =>
    within(f, h, (source, origin, receiving) =>
      Effect.gen(function* () {
        yield* f.sql.unsafe(
          "UPDATE jones_peer_environment_links SET secret_name='replacement-home-to-source'",
        );
        expect(
          (yield* source.withReadmission(origin, receiving, () => Effect.void).pipe(Effect.result))
            ._tag,
        ).toBe("Failure");
        expect(h.control.methods).toEqual([]);
      }),
    ),
  );
});
it.effect(
  "transport generation fencing rejects relink during resolution without initializing",
  () => {
    const h = harness();
    h.control.relinkAtResolve = true;
    return Fixture.withFixture((f) =>
      within(f, h, (source, origin, receiving) =>
        Effect.gen(function* () {
          expect(
            (yield* source
              .withReadmission(origin, receiving, () => Effect.void)
              .pipe(Effect.result))._tag,
          ).toBe("Failure");
          expect(h.control.methods).toEqual([]);
        }),
      ),
    );
  },
);
it.effect.each(["digest", "echo", "old-version", "denied"] as const)(
  "refuses parent qualification substitution: %s",
  (variant) => {
    const h = harness();
    return Fixture.withFixture((f) =>
      within(f, h, (source, origin, receiving) =>
        Effect.gen(function* () {
          const initial = h.control.parent;
          h.control.parent = (request) => {
            const reply = initial(request) as Record<string, unknown>;
            if (variant === "digest") return { ...reply, forward_request_sha256: "d".repeat(64) };
            if (variant === "echo")
              return { ...reply, request: { ...request, receiving_home_pin_id: "pin:substitute" } };
            if (variant === "old-version")
              return { ...reply, contract: "workstreams-source-readmission/2.0.0" };
            return { contract: request.contract, request, state: "denied", reason: "not_found" };
          };
          expect(
            (yield* source
              .withReadmission(origin, receiving, () => Effect.void)
              .pipe(Effect.result))._tag,
          ).toBe("Failure");
          expect(h.control.calls.map((c) => c.name)).toEqual([
            "jones_workstreams_source_readmission",
          ]);
        }),
      ),
    );
  },
);
it.effect("rechecks latest home grant after the fresh source observation", () => {
  const h = harness();
  return Fixture.withFixture((f) =>
    within(f, h, (source, origin, receiving) =>
      Effect.gen(function* () {
        h.control.beforeReply = () =>
          f.registry
            .appendPlain(Fixture.scope, "grants", {
              grant_id: "grant:narrow-source",
              principal_id: "principal:test",
              grant_version: 2,
              permissions: JSON.stringify(["workstreams:read"]),
              state: "active",
            })
            .pipe(Effect.asVoid, Effect.orDie);
        expect(
          (yield* source.withReadmission(origin, receiving, () => Effect.void).pipe(Effect.result))
            ._tag,
        ).toBe("Failure");
      }),
    ),
  );
});
it.effect.each(["carrier", "inner", "generation", "session"] as const)(
  "rejects changed provider result/currentness: %s",
  (variant) => {
    const h = harness();
    return Fixture.withFixture((f) =>
      within(f, h, (source, origin, receiving) =>
        source.withReadmission(origin, receiving, (p) =>
          Effect.gen(function* () {
            if (variant === "carrier")
              h.control.result = (request) =>
                providerUnknown({ ...request, receiving_home_pin_id: "pin:wrong" });
            if (variant === "inner")
              h.control.result = (request) => ({
                ...providerUnknown(request),
                result: {
                  ...providerUnknown(request).result,
                  request: { ...request.request, command_id: "effect:wrong-request-0001" },
                },
              });
            if (variant === "generation")
              h.control.beforeReply = () =>
                f.sql
                  .unsafe("UPDATE jones_peer_environment_links SET secret_name='replacement'")
                  .pipe(Effect.asVoid, Effect.orDie);
            if (variant === "session")
              h.control.beforeReply = () =>
                Effect.gen(function* () {
                  yield* f.sessions.revoke({
                    sessionId: f.sessionId,
                    revokedAt: yield* DateTime.now,
                  });
                }).pipe(Effect.orDie);
            expect((yield* Source.lookupPeerSourceEffect(p).pipe(Effect.result))._tag).toBe(
              "Failure",
            );
            expect(h.control.calls).toHaveLength(2);
          }),
        ),
      ),
    );
  },
);
it.effect("HTTP session loss is not retried and cannot become settlement dispatch", () => {
  const h = harness();
  return Fixture.withFixture((f) =>
    within(f, h, (source, origin, receiving) =>
      source.withReadmission(origin, receiving, (p) =>
        Effect.gen(function* () {
          h.control.status = 404;
          expect((yield* Source.lookupPeerSourceEffect(p).pipe(Effect.result))._tag).toBe(
            "Failure",
          );
          expect(h.control.calls).toHaveLength(2);
          expect((h.control.calls[1]!.arguments as unknown as CarrierRequest).kind).toBe(
            "settle_lookup",
          );
        }),
      ),
    ),
  );
});
it.effect("carries narrowed original limits into provider lookup without widening", () => {
  const h = harness();
  return Fixture.withFixture((f) =>
    within(f, h, (source, origin, receiving) =>
      Effect.gen(function* () {
        const initial = h.control.parent;
        h.control.parent = (request) => ({
          ...(initial(request) as object),
          current_ceiling: {
            ...f.envelope.recovery.original_ceiling,
            access: "approval-required",
            runtime_mode: "approval-required",
          },
        });
        yield* source.withReadmission(origin, receiving, (p) =>
          Effect.gen(function* () {
            expect(p.ceiling.runtime_mode).toBe("approval-required");
            yield* Source.lookupPeerSourceEffect(p);
            expect(h.control.calls[1]!.limits).toBe("approval-required/default");
          }),
        );
      }),
    ),
  );
});
it.effect(
  "private source observations reject copied and escaped proofs and preserve callback failure",
  () => {
    const h = harness();
    return Fixture.withFixture((f) =>
      within(f, h, (source, origin, receiving) =>
        Effect.gen(function* () {
          let escaped: Source.PreparedPeerSource | undefined;
          yield* source.withReadmission(origin, receiving, (p) =>
            Effect.gen(function* () {
              escaped = p;
              yield* Source.checkPeerSourceOrigin(p, origin, receiving);
              expect(
                (yield* Source.checkPeerSourceOrigin(p, { ...origin }, receiving).pipe(
                  Effect.result,
                ))._tag,
              ).toBe("Failure");
              expect(
                (yield* Source.checkPeerSourceOrigin(p, origin, { ...receiving }).pipe(
                  Effect.result,
                ))._tag,
              ).toBe("Failure");
              expect((yield* Source.checkPeerSource({ ...p }).pipe(Effect.result))._tag).toBe(
                "Failure",
              );
            }),
          );
          if (escaped)
            expect((yield* Source.checkPeerSource(escaped).pipe(Effect.result))._tag).toBe(
              "Failure",
            );
          const result = yield* source
            .withReadmission(origin, receiving, () => Effect.fail("callback sentinel"))
            .pipe(Effect.flip);
          expect(result).toBe("callback sentinel");
        }),
      ),
    );
  },
);
it.effect("an unrelated same-client writer completes while source transport is blocked", () => {
  const h = harness();
  return Fixture.withFixture((f) =>
    within(f, h, (source, origin, receiving) =>
      source.withReadmission(origin, receiving, (p) =>
        Effect.gen(function* () {
          const started = yield* Deferred.make<void>(),
            release = yield* Deferred.make<void>();
          h.control.beforeReply = () =>
            Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release)));
          const read = yield* Source.lookupPeerSourceEffect(p).pipe(Effect.forkChild);
          yield* Deferred.await(started);
          yield* f.sql.withTransaction(
            f.sql`INSERT INTO writer_probe VALUES ('while-peer-blocked')`,
          );
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(read);
          expect(yield* f.sql<{ value: string }>`SELECT value FROM writer_probe`).toEqual([
            { value: "while-peer-blocked" },
          ]);
        }),
      ),
    ),
  );
});

it.effect(
  "retains authentic terminal evidence under the actual owner after receiving revocation, while completion stays fenced",
  () => {
    const h = harness();
    return Fixture.withFixture((f) =>
      within(f, h, (source, origin, receiving) =>
        source.withReadmission(origin, receiving, (p) =>
          Effect.gen(function* () {
            h.control.result = (request) => ({
              contract: Carrier.WORKSTREAMS_SOURCE_RETAINED_EFFECT_CONTRACT_V1,
              state: "result",
              request,
              result: {
                protocol: "workstreams-t3-provider/2.0.0",
                state: "terminal",
                request: request.request,
                result: nativeSettlementResult(origin.intent, "denied"),
                native_receipt: null,
                settlement_event: null,
              },
            });
            h.control.beforeReply = () =>
              Effect.gen(function* () {
                yield* f.sessions.revoke({
                  sessionId: f.sessionId,
                  revokedAt: yield* DateTime.now,
                });
              }).pipe(Effect.orDie);
            const reply = yield* Source.lookupPeerSourceEffect(p);
            expect(reply.kind).toBe("retained");
            if (reply.kind !== "retained")
              return yield* Effect.die("Expected authentic terminal result");
            yield* Source.checkAuthenticatedPeerResult(reply.evidence, origin);
            expect(
              (yield* Source.checkAuthenticatedPeerResult({ ...reply.evidence }, origin).pipe(
                Effect.result,
              ))._tag,
            ).toBe("Failure");
            expect((yield* Source.checkPeerSource(p).pipe(Effect.result))._tag).toBe("Failure");
            expect(
              (yield* f.sql<{
                state: string;
              }>`SELECT state FROM jones_workstreams_external_reservations`)[0]!.state,
            ).toBe("active");
          }),
        ),
      ),
    );
  },
);

it.effect.each(["valid", "digest", "ttl"] as const)(
  "validates genuinely bound source attestation evidence: %s",
  (variant) => {
    const h = harness();
    return Fixture.withFixture(
      (f) =>
        within(f, h, (source, origin, receiving) =>
          source.withReadmission(origin, receiving, (p) =>
            Effect.gen(function* () {
              h.control.result = (request) => {
                if (request.kind !== "attest")
                  throw new Error("Expected fixed attestation request");
                const at = "2026-01-01T00:00:00.000Z";
                const expires =
                  variant === "ttl" ? "2026-01-01T00:06:00.000Z" : "2026-01-01T00:05:00.000Z";
                const exact = encodeAttestationRequest(request.request);
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
                      evidence_sha256:
                        variant === "digest"
                          ? "d".repeat(64)
                          : NodeCrypto.createHash("sha256")
                              .update(exact + "\n" + at + "\n" + expires)
                              .digest("hex"),
                    },
                  },
                };
              };
              const result = yield* Source.lookupPeerSourceEffect(p).pipe(Effect.result);
              expect(result._tag).toBe(variant === "valid" ? "Success" : "Failure");
              if (variant === "valid" && result._tag === "Success") {
                expect(result.success.kind).toBe("retained");
                if (result.success.kind === "retained")
                  yield* Source.checkAuthenticatedPeerResult(result.success.evidence, origin);
              }
              expect((h.control.calls[1]!.arguments as unknown as CarrierRequest).kind).toBe(
                "attest",
              );
            }),
          ),
        ),
      "verify_reference",
    );
  },
);

it.effect(
  "preserves retained intent cache bytes in provider request and terminal-result association",
  () => {
    const h = harness();
    return Fixture.withFixture(
      (f) =>
        within(f, h, (source, origin, receiving) =>
          source.withReadmission(origin, receiving, (p) =>
            Effect.gen(function* () {
              h.control.result = (request) => ({
                contract: Carrier.WORKSTREAMS_SOURCE_RETAINED_EFFECT_CONTRACT_V1,
                state: "result",
                request,
                result: {
                  protocol: "workstreams-t3-provider/2.0.0",
                  state: "terminal",
                  request: request.request,
                  result: nativeSettlementResult(f.intent, "denied"),
                  native_receipt: null,
                  settlement_event: null,
                },
              });
              const reply = yield* Source.lookupPeerSourceEffect(p);
              expect(reply.kind).toBe("retained");
              expect(origin.intent.previous_observation).toEqual(f.intent.previous_observation);
            }),
          ),
        ),
      "request_native_t3_settlement",
      {
        native_reference_id: "ref:test",
        observation_version: 1,
        attempted_at: "2026-01-01T00:00:00.000Z",
        outcome: "observed",
        retry_after_seconds: null,
        last_success: {
          state: "open",
          draft: false,
          observed_at: "2026-01-01T00:00:00.000Z",
          provider_updated_at: null,
        },
        command_id: "observation:original-0001",
        observation_cache: {
          schema_version: 1,
          owner_id: "owner:test",
          native_reference_id: "ref:test",
          source_instance_id: "source:test",
          provider: "t3",
          locator: {
            host: "github.com",
            repository_owner: "synthetic",
            repository_name: "fixture",
            number: 1,
          },
          server_generation: 1,
          authority_namespace: "source-store",
          store_generation: 3,
          observation_version: 1,
          etag: "synthetic-cache-hint",
          representation: {
            state: "open",
            draft: false,
            observed_at: "2026-01-01T00:00:00.000Z",
            provider_updated_at: null,
          },
        },
      },
    );
  },
);
