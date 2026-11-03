// @effect-diagnostics nodeBuiltinImport:off -- Synthetic remote transport; retained fixture files remain outside source.
import * as NodeCrypto from "node:crypto";
import * as NodeUtil from "node:util";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
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
import * as DateTime from "effect/DateTime";
import * as Peer from "../../../../peer/PeerMcpClient.ts";
import * as Links from "../../../../peer/PeerLinks.ts";
import * as Environment from "../../../../environment/ServerEnvironment.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import migration from "../../../persistence/Migrations/119_JonesPeerEnvironmentLinks.ts";
import * as Fixture from "../runtime/peerEffectRecovery.testkit.ts";
import * as Origins from "../runtime/peerEffectOrigin.ts";
import * as Source from "./peerEffectSource.ts";
import * as Recovery from "./peerEffectRecovery.ts";
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
  ).pipe(Layer.provide(Links.layer), Layer.provide(Layer.mock(Environment.ServerEnvironment)({})));
  const layer = Source.layer.pipe(
    Layer.provideMerge(Peer.layer.pipe(Layer.provideMerge(links), Layer.provideMerge(http))),
  );
  return { control, layer };
};
type RecoveryFixture = Parameters<Parameters<typeof Fixture.withFixture>[0]>[0];
const withinRecovery = <A, E, R>(
  f: RecoveryFixture,
  h: ReturnType<typeof harness>,
  use: (recovery: Recovery.PeerEffectRecovery["Service"]) => Effect.Effect<A, E, R>,
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
    return yield* Effect.gen(function* () {
      return yield* use(yield* Recovery.PeerEffectRecovery);
    }).pipe(Effect.provide(Recovery.layer.pipe(Layer.provideMerge(h.layer))));
  });
const terminal =
  (f: RecoveryFixture, outcome: "committed" | "denied" = "denied") =>
  (request: CarrierRequest) => ({
    contract: Carrier.WORKSTREAMS_SOURCE_RETAINED_EFFECT_CONTRACT_V1,
    state: "result",
    request,
    result: {
      protocol: "workstreams-t3-provider/2.0.0",
      state: "terminal",
      request: request.request,
      result: nativeSettlementResult(f.intent, outcome),
      native_receipt:
        outcome === "committed"
          ? {
              commandId: "provider:retained-original",
              aggregateKind: "thread",
              aggregateId: "thread:original",
              acceptedAt: "2026-01-01T00:00:00Z",
              resultSequence: 1,
              status: "accepted",
            }
          : null,
      settlement_event:
        outcome === "committed"
          ? {
              eventId: "event:retained-original",
              commandId: "provider:retained-original",
              aggregateKind: "thread",
              aggregateId: "thread:original",
              sequence: 1,
              type: "thread.settled",
              occurredAt: "2026-01-01T00:00:00Z",
            }
          : null,
    },
  });
const states = (f: RecoveryFixture) =>
  Effect.gen(function* () {
    const receipts = yield* f.sql<{ state: string }>`SELECT state FROM jones_workstreams_receipts`;
    const reservations = yield* f.sql<{
      state: string;
    }>`SELECT state FROM jones_workstreams_external_reservations`;
    const parents = yield* f.sql<{
      state: string;
      result_envelope: string | null;
    }>`SELECT state,result_envelope FROM jones_workstreams_forward_receipts`;
    return {
      receipt: receipts[0]!.state,
      reservation: reservations[0]!.state,
      parent: parents[0]!.state,
      result: parents[0]!.result_envelope,
    };
  });
it.effect.each(["committed", "denied"] as const)(
  "retains and atomically completes exact original settlement outcome: %s",
  (outcome) => {
    const h = harness();
    return Fixture.withFixture((f) =>
      withinRecovery(f, h, (recovery) =>
        Effect.gen(function* () {
          h.control.result = terminal(f, outcome);
          const receipt = yield* recovery.recoverOne(f.command.command_id);
          expect(receipt).toMatchObject({
            command_id: f.command.command_id,
            state: "committed",
            effects: { native_settlement: { outcome } },
          });
          expect(yield* states(f)).toMatchObject({
            receipt: "committed",
            reservation: "released",
            parent: "completed",
          });
          expect(h.control.calls.map((c) => c.name)).toEqual([
            "jones_workstreams_source_readmission",
            "jones_workstreams_source_retained_effect",
            "jones_workstreams_source_readmission",
          ]);
          expect(
            h.control.calls
              .filter((c) => c.name === "jones_workstreams_source_retained_effect")
              .map((c) => (c.arguments as unknown as CarrierRequest).kind),
          ).toEqual(["settle_lookup"]);
          const origins = yield* Origins.PeerEffectOrigins;
          yield* origins.withOrigin(f.command.command_id, (origin) =>
            Effect.sync(() => {
              expect(origin.parent_result?.result).toEqual(receipt);
              expect(origin.parent_result?.identity).toEqual(f.identity);
              expect(origin.parent_result?.home.incoming_peer_pin_id).toBe(f.pin.pin_id);
            }),
          );
          expect(yield* f.effects.result(Fixture.scope, f.command.command_id)).toEqual(
            nativeSettlementResult(f.intent, outcome),
          );
          const context = yield* f.registry.context(Fixture.scope);
          expect(context?.registry_version).toBe(
            f.intent.admitted_registry_version + (outcome === "committed" ? 1 : 0),
          );
          const history = yield* f.sql`SELECT * FROM jones_workstreams_history`;
          expect(history).toHaveLength(1);
          // Reconstruct the recovery service; durable authoritative rows, not an in-memory ticket, own replay.
          const replay = yield* Effect.gen(function* () {
            return yield* (yield* Recovery.PeerEffectRecovery).recoverOne(f.command.command_id);
          }).pipe(Effect.provide(Recovery.layer.pipe(Layer.provideMerge(h.layer), Layer.fresh)));
          expect(replay).toEqual(receipt);
          expect(h.control.calls).toHaveLength(4);
          expect(h.control.calls[3]!.name).toBe("jones_workstreams_source_readmission");
          expect(yield* f.sql`SELECT * FROM jones_workstreams_history`).toHaveLength(1);
        }),
      ),
    );
  },
);
it.effect.each(["unknown", "denied", "unavailable"] as const)(
  "never dispatches or completes from source lookup %s",
  (state) => {
    const h = harness();
    return Fixture.withFixture((f) =>
      withinRecovery(f, h, (recovery) =>
        Effect.gen(function* () {
          h.control.result = (request) =>
            state === "unknown"
              ? providerUnknown(request)
              : {
                  contract: Carrier.WORKSTREAMS_SOURCE_RETAINED_EFFECT_CONTRACT_V1,
                  request,
                  state,
                  reason: state === "denied" ? "not_granted" : "provider_unconfigured",
                };
          expect(yield* recovery.recoverOne(f.command.command_id)).toBeNull();
          expect(yield* states(f)).toEqual({
            receipt: "pending",
            reservation: "active",
            parent: "accepted",
            result: null,
          });
          expect(yield* f.effects.result(Fixture.scope, f.command.command_id)).toBeNull();
          expect(h.control.calls).toHaveLength(2);
          expect((h.control.calls[1]!.arguments as unknown as CarrierRequest).kind).toBe(
            "settle_lookup",
          );
        }),
      ),
    );
  },
);
it.effect(
  "retains authentic terminal bytes after original-session revocation without releasing the parent",
  () => {
    const h = harness();
    return Fixture.withFixture((f) =>
      withinRecovery(f, h, (recovery) =>
        Effect.gen(function* () {
          h.control.result = terminal(f);
          h.control.beforeReply = (call) =>
            call.name === "jones_workstreams_source_retained_effect"
              ? f.sessions
                  .revoke({
                    sessionId: f.sessionId,
                    revokedAt: DateTime.makeUnsafe("2026-01-01T00:00:01Z"),
                  })
                  .pipe(Effect.asVoid, Effect.orDie)
              : Effect.void;
          expect((yield* recovery.recoverOne(f.command.command_id).pipe(Effect.result))._tag).toBe(
            "Failure",
          );
          expect(yield* f.effects.result(Fixture.scope, f.command.command_id)).toEqual(
            nativeSettlementResult(f.intent, "denied"),
          );
          expect(yield* states(f)).toEqual({
            receipt: "pending",
            reservation: "active",
            parent: "accepted",
            result: null,
          });
          expect(h.control.calls).toHaveLength(2);
        }),
      ),
    );
  },
);
it.effect(
  "new source denial after provider success preserves its result for a later readmission",
  () => {
    const h = harness();
    return Fixture.withFixture((f) =>
      withinRecovery(f, h, (recovery) =>
        Effect.gen(function* () {
          h.control.result = terminal(f);
          const authorized = h.control.parent;
          let parents = 0;
          h.control.parent = (request) =>
            ++parents === 1
              ? authorized(request)
              : {
                  contract: Parent.WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V3,
                  request,
                  state: "denied",
                  reason: "not_granted",
                };
          expect((yield* recovery.recoverOne(f.command.command_id).pipe(Effect.result))._tag).toBe(
            "Failure",
          );
          expect(yield* f.effects.result(Fixture.scope, f.command.command_id)).toEqual(
            nativeSettlementResult(f.intent, "denied"),
          );
          expect(yield* states(f)).toMatchObject({
            receipt: "pending",
            reservation: "active",
            parent: "accepted",
          });
          h.control.parent = authorized;
          expect(yield* recovery.recoverOne(f.command.command_id)).toMatchObject({
            state: "committed",
          });
          expect(
            h.control.calls.filter((c) => c.name === "jones_workstreams_source_retained_effect"),
          ).toHaveLength(1);
        }),
      ),
    );
  },
);
it.effect(
  "rolls command completion back when the parent write fails after observing its terminal receipt",
  () => {
    const h = harness();
    return Fixture.withFixture((f) =>
      withinRecovery(f, h, (recovery) =>
        Effect.gen(function* () {
          h.control.result = terminal(f, "committed");
          yield* f.sql.unsafe(
            "CREATE TRIGGER synthetic_parent_abort BEFORE UPDATE ON jones_workstreams_forward_receipts WHEN NEW.state='completed' AND (SELECT state FROM jones_workstreams_receipts WHERE command_id='effect:peer-origin-original-0001')='committed' BEGIN SELECT RAISE(ABORT,'synthetic_parent_after_terminal'); END",
          );
          const failed = yield* recovery.recoverOne(f.command.command_id).pipe(Effect.result);
          expect(failed._tag).toBe("Failure");
          if (failed._tag === "Failure")
            expect(NodeUtil.inspect(failed.failure, { depth: 10 })).toContain(
              "synthetic_parent_after_terminal",
            );
          expect(yield* states(f)).toEqual({
            receipt: "pending",
            reservation: "active",
            parent: "accepted",
            result: null,
          });
          expect((yield* f.registry.context(Fixture.scope))?.registry_version).toBe(
            f.intent.admitted_registry_version,
          );
          expect(yield* f.sql`SELECT * FROM jones_workstreams_history`).toHaveLength(0);
          expect(yield* f.effects.result(Fixture.scope, f.command.command_id)).toEqual(
            nativeSettlementResult(f.intent, "committed"),
          );
          yield* f.sql.unsafe("DROP TRIGGER synthetic_parent_abort");
          expect(yield* recovery.recoverOne(f.command.command_id)).toMatchObject({
            state: "committed",
          });
          expect(
            h.control.calls.filter((c) => c.name === "jones_workstreams_source_retained_effect"),
          ).toHaveLength(1);
        }),
      ),
    );
  },
);
it.effect("rejects ambient preparation before any original-session, source or key access", () => {
  const h = harness();
  return Fixture.withFixture((f) =>
    withinRecovery(f, h, (recovery) =>
      Effect.gen(function* () {
        let calls = 0;
        f.setSecretHook(() =>
          Effect.sync(() => {
            calls++;
          }),
        );
        expect(
          (yield* f.sql
            .withTransaction(recovery.recoverOne(f.command.command_id))
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
        expect(calls).toBe(0);
        expect(h.control.calls).toHaveLength(0);
      }),
    ),
  );
});
it.effect(
  "blocks external key acquisition outside the writer and rechecks home key rotation without refill",
  () => {
    const h = harness();
    return Fixture.withFixture((f) =>
      withinRecovery(f, h, (recovery) =>
        Effect.gen(function* () {
          h.control.result = terminal(f);
          const entered = yield* Deferred.make<void>(),
            release = yield* Deferred.make<void>();
          let stopped = false;
          let indexReads = 0;
          f.setSecretHook((name) =>
            Effect.gen(function* () {
              expect(Option.isNone(yield* Effect.serviceOption(f.sql.transactionService))).toBe(
                true,
              );
              // Index acquisition occurs in the freshly prepared writer graph, after origin/source authentication.
              if (name.startsWith("workstreams-index-key-") && ++indexReads === 2 && !stopped) {
                stopped = true;
                yield* Deferred.succeed(entered, undefined);
                yield* Deferred.await(release);
              }
            }),
          );
          const work = yield* recovery
            .recoverOne(f.command.command_id)
            .pipe(Effect.result, Effect.forkChild);
          yield* Deferred.await(entered);
          yield* f.sql.withTransaction(
            Effect.gen(function* () {
              yield* f.sql`INSERT INTO writer_probe VALUES ('key-wait-outside-writer')`;
              yield* f.sql`UPDATE jones_workstreams_keys SET state='decrypt_only' WHERE key_id='test-home'`;
              yield* f.sql`INSERT INTO jones_workstreams_keys(key_id,registry_instance_id,scope,state,created_at) VALUES ('replacement',${Fixture.scope.registry_instance_id},'home','active','2026-01-01T00:00:00Z')`;
              yield* f.sql`UPDATE jones_workstreams_registries SET active_key_id='replacement',registry_version=registry_version+1 WHERE registry_instance_id=${Fixture.scope.registry_instance_id}`;
            }),
          );
          yield* Deferred.succeed(release, undefined);
          expect((yield* Fiber.join(work))._tag).toBe("Failure");
          expect(yield* f.sql`SELECT value FROM writer_probe`).toEqual([
            { value: "key-wait-outside-writer" },
          ]);
          expect(yield* states(f)).toMatchObject({
            receipt: "pending",
            reservation: "active",
            parent: "accepted",
          });
        }),
      ),
    );
  },
);
it.effect(
  "does not perform a replacement lookup when a retained successful result is already encrypted",
  () => {
    const h = harness();
    return Fixture.withFixture((f) =>
      withinRecovery(f, h, (recovery) =>
        Effect.gen(function* () {
          yield* f.effects.saveResult(
            Fixture.scope,
            f.command.command_id,
            nativeSettlementResult(f.intent, "denied"),
          );
          h.control.result = () => {
            throw new Error("No provider redispatch or repeated lookup");
          };
          expect(yield* recovery.recoverOne(f.command.command_id)).toMatchObject({
            state: "committed",
          });
          expect(h.control.calls.map((c) => c.name)).toEqual([
            "jones_workstreams_source_readmission",
            "jones_workstreams_source_readmission",
          ]);
        }),
      ),
    );
  },
);
it.effect("never promotes a stored result from another accepted command", () => {
  const h = harness();
  return Fixture.withFixture((f) =>
    withinRecovery(f, h, (recovery) =>
      Effect.gen(function* () {
        yield* f.effects.saveResult(Fixture.scope, f.command.command_id, {
          native_outcome: "denied",
          native_evidence: { command_id: "effect:substituted" },
        });
        expect((yield* recovery.recoverOne(f.command.command_id).pipe(Effect.result))._tag).toBe(
          "Failure",
        );
        expect(yield* states(f)).toMatchObject({
          receipt: "pending",
          reservation: "active",
          parent: "accepted",
        });
        expect(
          h.control.calls.every((c) => c.name === "jones_workstreams_source_readmission"),
        ).toBe(true);
      }),
    ),
  );
});

it.effect(
  "completes attestation through the same atomic parent ledger without a home-local caller",
  () => {
    const h = harness();
    return Fixture.withFixture(
      (f) =>
        withinRecovery(f, h, (recovery) =>
          Effect.gen(function* () {
            const index = yield* Index.WorkstreamsIdentityIndex;
            const reference = f.intent.reference;
            yield* f.registry.put(
              Fixture.scope,
              "references",
              {
                metadata: {
                  native_reference_id: reference.native_reference_id,
                  provider: reference.identity.provider,
                  resource_kind: reference.identity.resource_kind,
                  id_kind: reference.identity.id_kind,
                  identity_index: yield* index.digest(Fixture.scope, reference.identity),
                  legacy_identity_sha256: null,
                  attestation_version: 0,
                  registration_state: "verification-pending",
                  created_by_principal: reference.created_by.principal_id,
                  routing_source_instance_id: null,
                  routing_native_id: null,
                  routing_authority_namespace: null,
                  routing_store_generation: null,
                  routing_attested_at: null,
                  routing_expires_at: null,
                  routing_evidence_sha256: null,
                },
                value: reference,
              },
              null,
            );
            h.control.result = (request) => {
              if (request.kind !== "attest") throw new Error("Expected attestation-only lookup");
              const at = "2026-01-01T00:00:00.000Z",
                expires = "2026-01-01T00:05:00.000Z";
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
                    evidence_sha256: NodeCrypto.createHash("sha256")
                      .update(exact + "\n" + at + "\n" + expires)
                      .digest("hex"),
                  },
                },
              };
            };
            const receipt = yield* recovery.recoverOne(f.command.command_id);
            expect(receipt).toMatchObject({
              state: "committed",
              effects: { registration: { state: "attested", attestation_version: 1 } },
            });
            expect(yield* states(f)).toMatchObject({
              receipt: "committed",
              reservation: "released",
              parent: "completed",
            });
            expect(
              h.control.calls
                .filter((c) => c.name === "jones_workstreams_source_retained_effect")
                .map((c) => (c.arguments as unknown as CarrierRequest).kind),
            ).toEqual(["attest"]);
            expect((yield* f.registry.list(Fixture.scope, "references"))[0]?.value).toMatchObject({
              registration: { state: "attested", attestation_version: 1 },
            });
          }),
        ),
      "verify_reference",
    );
  },
);
it.effect("late SQL grant narrowing rolls the terminal command and parent back together", () => {
  const h = harness();
  return Fixture.withFixture((f) =>
    withinRecovery(f, h, (recovery) =>
      Effect.gen(function* () {
        h.control.result = terminal(f, "committed");
        yield* f.sql.unsafe(
          "CREATE TRIGGER synthetic_late_grant AFTER UPDATE ON jones_workstreams_receipts WHEN NEW.state='committed' BEGIN INSERT INTO jones_workstreams_grants (registry_instance_id,owner_id,principal_id,grant_version,grant_id,permissions,state) VALUES ('11111111-1111-4111-8111-111111111111','owner:test','principal:test',2,'grant:late','[\"workstreams:read\"]','active'); END",
        );
        expect((yield* recovery.recoverOne(f.command.command_id).pipe(Effect.result))._tag).toBe(
          "Failure",
        );
        expect(yield* states(f)).toMatchObject({
          receipt: "pending",
          reservation: "active",
          parent: "accepted",
        });
        expect((yield* f.registry.context(Fixture.scope))?.registry_version).toBe(
          f.intent.admitted_registry_version,
        );
        expect(
          (yield* f.registry.listPlain(Fixture.scope, "grants")).map((r) => r.grant_version),
        ).toEqual([1]);
        expect(yield* f.effects.result(Fixture.scope, f.command.command_id)).toEqual(
          nativeSettlementResult(f.intent, "committed"),
        );
      }),
    ),
  );
});
it.effect(
  "retains encrypted results in SQLite and WAL and reopens the original parent for readback",
  () => {
    const h = harness();
    return Fixture.withFixture((f) =>
      withinRecovery(f, h, (recovery) =>
        Effect.gen(function* () {
          h.control.result = terminal(f);
          yield* recovery.recoverOne(f.command.command_id);
          const database = NodePath.join(f.baseDir, "userdata", "statev2.sqlite");
          for (const path of [database, database + "-wal"]) {
            if (NodeFS.existsSync(path)) {
              const bytes = NodeFS.readFileSync(path);
              expect(bytes.includes(Buffer.from('"native_outcome":"denied"'))).toBe(false);
              expect(bytes.includes(Buffer.from('"native_evidence":'))).toBe(false);
            }
          }
          // A second read-only handle observes the committed encrypted rows; it cannot mint owner authority.
          const reader = new NodeSqlite.DatabaseSync(database, { readOnly: true });
          try {
            const row = reader
              .prepare("SELECT state,result_envelope FROM jones_workstreams_forward_receipts")
              .get()!;
            expect(row.state).toBe("completed");
            expect(typeof row.result_envelope).toBe("string");
            expect(String(row.result_envelope)).not.toContain("native_evidence");
          } finally {
            reader.close();
          }
          const origins = yield* Origins.PeerEffectOrigins;
          yield* origins.withOrigin(f.command.command_id, (origin) =>
            Effect.sync(() => {
              expect(origin.parent_result?.identity).toEqual(f.identity);
              expect(origin.parent_result?.result.state).toBe("committed");
            }),
          );
        }),
      ),
    );
  },
);
