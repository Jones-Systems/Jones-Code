// @effect-diagnostics nodeBuiltinImport:off -- Synthetic config fixtures remain under external TMPDIR for ROOT cleanup.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { expect, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";
import * as Forward from "@t3tools/contracts/jones/workstreamsForwarding";
import * as Wire from "@t3tools/contracts/jones/workstreamsForwardQualification";
import type {
  WorkstreamsPeerSessionPinV1,
  WorkstreamsTransportCeiling,
} from "@t3tools/contracts/jones/workstreamsPeerAuthority";
import * as EffectNodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as Auth from "../../../../auth/EnvironmentAuth.ts";
import * as Secrets from "../../../../auth/ServerSecretStore.ts";
import * as Config from "../../../../config.ts";
import * as Environment from "../../../../environment/ServerEnvironment.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Links from "../../../../peer/PeerLinks.ts";
import * as Peer from "../../../../peer/PeerMcpClient.ts";
import { runMigrations } from "../../../../persistence/Migrations.ts";
import peerLinksMigration from "../../../persistence/Migrations/119_JonesPeerEnvironmentLinks.ts";
import { NativeHome } from "../mode/authorityMode.ts";
import * as Pins from "../runtime/peerSessionPins.ts";
import * as Supplied from "./suppliedEvidence.ts";
import * as Source from "./sourceForwardQualification.ts";

const now = "2026-01-01T00:00:00.000Z";
const home = {
  registry_instance_id: "11111111-1111-4111-8111-111111111111",
  owner_id: "owner:test",
  home_environment_id: "home:test",
  server_generation: 1,
  canonical_origin: "https://native.workstreams.invalid",
  served_by_environment_id: "home:test",
};
const sha = (s: string) => NodeCrypto.createHash("sha256").update(s).digest("hex");
const body = JSON.stringify({
  command_id: "appearance:source-0001",
  workstream_id: "workstream:test",
  expected_server_generation: 1,
  expected_version: 2,
  border_color: "#112233",
});
const target = {
  registry_instance_id: home.registry_instance_id,
  owner_id: home.owner_id,
  home_environment_id: home.home_environment_id,
  server_generation: home.server_generation,
};
const input = Schema.decodeUnknownSync(Forward.WorkstreamsForwardRequest)({
  contract: Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
  kind: "appearance",
  identity: {
    forward_id: "forward:source-00001",
    source_operation_id: "appearance:source-0001",
    parent_operation_id: null,
    command_ids: ["appearance:source-0001"],
    request_sha256: sha(body),
    recorded_caller_sha256: "b".repeat(64),
    expected_home: target,
    canonical_origin: home.canonical_origin,
    source_authority: {
      environment_id: "member:test",
      authority_namespace: "source-store",
      store_generation: 3,
    },
    source_enrollment_id: "enrollment:test",
    source_enrollment_session_id: "source-session:test",
    principal_id: "principal:test",
    singleton_binding_version: 4,
    source_binding_version: 5,
    member_pin_id: "member-pin:test",
    member_pin_bound_revision: 6,
    outbound_link_generation: "member-to-home-generation",
  },
  request: {
    expected_home: target,
    relay: {
      source_environment_id: "member:test",
      origin: { kind: "cli" },
      linked_origin_environment_id: null,
      hop: 1,
    },
    request: JSON.parse(body),
    request_body: body,
    request_sha256: sha(body),
  },
});
const digest = sha(
  Schema.encodeSync(Schema.fromJsonString(Forward.WorkstreamsForwardRequest))(input),
);
const readback: Forward.WorkstreamsForwardReadbackRequest = {
  contract: input.contract,
  identity: input.identity,
};
const full: WorkstreamsTransportCeiling = {
  access: "full-access",
  runtime_mode: "full-access",
  interaction_mode: "default",
  scopes: ["orchestration:read", "orchestration:operate"],
  permissions: ["workstreams:read", "workstreams:write"],
};
const authorized = (request: Wire.WorkstreamsSourceParentReadmissionRequestV3) => ({
  contract: Wire.WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V3,
  request,
  state: "authorized" as const,
  forward_request_sha256: digest,
  original_ceiling: full,
  current_ceiling: full,
  observed_at: now,
});
interface Control {
  reply: (request: Wire.WorkstreamsSourceParentReadmissionRequestV3) => unknown;
  calls: Array<{
    name: string;
    arguments: Wire.WorkstreamsSourceParentReadmissionRequestV3;
    limits: string | undefined;
  }>;
  methods: string[];
  resolutions: number;
  afterWriter: number;
  relinkAtResolve: boolean;
  beforeReply?: () => Effect.Effect<void>;
  status: number;
}
const harness = () => {
  const control: Control = {
    reply: authorized,
    calls: [],
    methods: [],
    resolutions: 0,
    afterWriter: 0,
    relinkAtResolve: false,
    status: 200,
  };
  const signing = Layer.succeed(
    Secrets.ServerSecretStore,
    Secrets.ServerSecretStore.of({
      get: () => Effect.succeed(Option.some(new Uint8Array(32).fill(17))),
      getOrCreateRandom: () => Effect.succeed(new Uint8Array(32).fill(17)),
      set: () => Effect.die("unexpected secret mutation"),
      create: () => Effect.die("unexpected secret creation"),
      remove: () => Effect.die("unexpected secret removal"),
    }),
  );
  const http = Layer.effect(
    HttpClient.HttpClient,
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      return HttpClient.make((request) =>
        Effect.gen(function* () {
          if (Option.isSome(yield* Effect.serviceOption(sql.transactionService))) {
            control.afterWriter++;
            return yield* Effect.die("External transport entered application writer");
          }
          if (request.body._tag !== "Uint8Array") return yield* Effect.die("Expected JSON bytes");
          const rpc = JSON.parse(new TextDecoder().decode(request.body.body));
          control.methods.push(rpc.method);
          let response: Response;
          if (rpc.method === "initialize")
            response = Response.json(
              {
                jsonrpc: "2.0",
                id: rpc.id,
                result: {
                  protocolVersion: "2025-06-18",
                  capabilities: {},
                  serverInfo: { name: "synthetic-source", version: "1" },
                },
              },
              { headers: { "mcp-session-id": "synthetic-mcp-session" } },
            );
          else if (rpc.method === "notifications/initialized")
            response = new Response(null, { status: 202 });
          else {
            expect(rpc.method).toBe("tools/call");
            expect(request.headers.authorization).toBe("Bearer synthetic-source-token");
            control.calls.push({ ...rpc.params, limits: request.headers[Mcp.MODE_LIMIT_HEADER] });
            if (control.beforeReply) yield* control.beforeReply();
            response =
              control.status === 200
                ? Response.json({
                    jsonrpc: "2.0",
                    id: rpc.id,
                    result: { content: [], structuredContent: control.reply(rpc.params.arguments) },
                  })
                : new Response(null, { status: control.status });
          }
          return HttpClientResponse.fromWeb(request, response);
        }),
      );
    }),
  );
  // Real bounded PeerLinks.currentGeneration/outcome SQL use the same application client.
  // Destination resolution and remote responses are synthetic; no sockets or installed keys.
  const links = Layer.effect(
    Links.PeerLinks,
    Effect.gen(function* () {
      const actual = yield* Links.PeerLinks,
        sql = yield* SqlClient.SqlClient;
      return Links.PeerLinks.of({
        ...actual,
        resolve: (environmentId) =>
          Effect.gen(function* () {
            control.resolutions++;
            expect(environmentId).toBe("member:test");
            if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
              return yield* Effect.die("Resolution inside writer");
            if (control.relinkAtResolve)
              yield* sql
                .unsafe(
                  "UPDATE jones_peer_environment_links SET secret_name='replacement-generation'",
                )
                .pipe(Effect.orDie);
            const generation = yield* actual.currentGeneration(environmentId);
            if (Option.isNone(generation)) return yield* Effect.die("Missing fixture generation");
            return {
              url: "https://source.invalid",
              token: "synthetic-source-token",
              generation: generation.value,
              link: {} as Links.ResolvedPeerLink["link"],
            };
          }),
      });
    }),
  ).pipe(Layer.provide(Links.layer), Layer.provide(Layer.mock(Environment.ServerEnvironment)({})));
  const transport = Peer.layer.pipe(Layer.provideMerge(links), Layer.provideMerge(http));
  const configDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "nw-source-qualification-"));
  const layer = Source.layer.pipe(
    Layer.provideMerge(transport),
    Layer.provideMerge(Pins.nativeLayer),
    Layer.provideMerge(Auth.layer.pipe(Layer.provide(signing))),
    Layer.provideMerge(Layer.succeed(NativeHome, home)),
    Layer.provideMerge(signing),
    Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
    Layer.provideMerge(EffectNodeCrypto.layer),
    Layer.provideMerge(Config.layerTest(process.cwd(), configDir)),
    Layer.provideMerge(
      Layer.succeed(Environment.ServerEnvironmentIdentity, {
        getEnvironmentId: Effect.succeed(EnvironmentId.make("home:test")),
      }),
    ),
    Layer.provideMerge(NodeServices.layer),
  );
  return { layer, control };
};
const fixture = Effect.gen(function* () {
  yield* TestClock.setTime(Date.parse(now));
  yield* runMigrations({ toMigrationInclusive: 54 });
  const sql = yield* SqlClient.SqlClient;
  yield* peerLinksMigration;
  yield* sql.unsafe(
    "INSERT INTO jones_peer_environment_links (environment_id,secret_name,label,urls_json,access,linked_at,expires_at,fenced) VALUES ('member:test','home-to-source-generation','synthetic-source','[\"https://source.invalid\"]','full-access','2026-01-01T00:00:00.000Z','2026-01-02T00:00:00.000Z',1)",
  );
  const native = yield* Pins.NativePeerSessions;
  const issued = yield* (yield* Auth.EnvironmentAuth).issueMcpClientSession({
    label: "synthetic-receiver",
    access: "full-access",
    peerLink: true,
    client: { deviceType: "bot" },
  });
  const invocation: Mcp.McpInvocationScope = {
    environmentId: EnvironmentId.make("home:test"),
    issuedAt: Date.parse(now),
    capabilities: new Set(["orchestration"]),
    requestNamespace: "source-qualification-test",
    thread: undefined,
    client: {
      sessionId: issued.sessionId,
      label: "untrusted-label",
      access: "full-access",
      linked: true,
    },
  };
  const authRequest = {
    cookies: {},
    headers: { authorization: "Bearer " + issued.token },
  } as unknown as Parameters<Auth.EnvironmentAuth["Service"]["authenticateMcpClient"]>[0];
  yield* native.authenticate(authRequest, invocation);
  const facts = yield* native.current(invocation);
  const pin: WorkstreamsPeerSessionPinV1 = {
    schema: "jones-workstreams-peer-session-pin/1",
    pin_id: "home-pin:test",
    revision: 7,
    receiver_authority: {
      environment_id: "home:test",
      authority_namespace: "home-store",
      store_generation: 2,
    },
    session: facts.session,
    peer_environment_id: "member:test",
    roles: ["member_home"],
    expected_home: target,
    source_authority: input.identity.source_authority,
    source_enrollment_id: input.identity.source_enrollment_id,
    source_binding_version: 5,
    singleton_binding_version: 4,
    state: "bound",
    expires_at: "2026-01-01T00:20:00.000Z",
    setup_request_id: "setup:test",
    setup_request_sha256: "c".repeat(64),
    owner_qualification_digest: "d".repeat(64),
    bind_receipt: {
      principal_id: "principal:test",
      binding_version: 5,
      registry_version: 8,
      request_sha256: "e".repeat(64),
    },
  };
  const service = yield* Supplied.SourceForwardQualification;
  const within = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Source.withQualificationScope(invocation, effect).pipe(
      Effect.provideService(Mcp.McpInvocationContext, invocation),
    );
  return { sql, native, invocation, authRequest, pin, service, within };
});

it.effect(
  "uses fixed V3, complete echo, separate directional generations and exact envelope digest",
  () => {
    const h = harness();
    return Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.within(
        Effect.gen(function* () {
          const p = yield* f.service.prepare(input, f.pin);
          expect(p).toHaveProperty("forward_request_sha256", digest);
          yield* f.sql.withTransaction(p.checkCurrent());
          expect(h.control.calls).toEqual([
            {
              name: "jones_workstreams_source_readmission",
              limits: "full-access/default",
              arguments: {
                contract: Wire.WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V3,
                accepted_forward: input.identity,
                receiving_home_pin_id: "home-pin:test",
                receiving_home_pin_revision: 7,
              },
            },
          ]);
          expect(h.control.methods).toEqual([
            "initialize",
            "notifications/initialized",
            "tools/call",
          ]);
          expect(h.control.afterWriter).toBe(0);
        }),
      );
    }).pipe(Effect.provide(h.layer));
  },
);
const identityChanges: Array<
  [string, (i: Forward.WorkstreamsForwardIdentity) => Forward.WorkstreamsForwardIdentity]
> = [
  ["forward id", (i) => ({ ...i, forward_id: "forward:changed-0001" })],
  ["source operation", (i) => ({ ...i, source_operation_id: "operation:changed-0001" })],
  ["parent", (i) => ({ ...i, parent_operation_id: "parent:changed-00001" })],
  ["children", (i) => ({ ...i, command_ids: ["command:changed-0001"] })],
  ["raw digest", (i) => ({ ...i, request_sha256: "c".repeat(64) })],
  ["caller digest", (i) => ({ ...i, recorded_caller_sha256: "c".repeat(64) })],
  ["home", (i) => ({ ...i, expected_home: { ...i.expected_home, owner_id: "owner:other" } })],
  ["canonical origin", (i) => ({ ...i, canonical_origin: "https://other.invalid" })],
  [
    "source authority",
    (i) => ({ ...i, source_authority: { ...i.source_authority, store_generation: 9 } }),
  ],
  ["enrollment", (i) => ({ ...i, source_enrollment_id: "enrollment:other" })],
  ["enrollment session", (i) => ({ ...i, source_enrollment_session_id: "session:other" })],
  ["principal", (i) => ({ ...i, principal_id: "principal:other" })],
  ["singleton", (i) => ({ ...i, singleton_binding_version: 9 })],
  ["source binding", (i) => ({ ...i, source_binding_version: 9 })],
  ["member pin", (i) => ({ ...i, member_pin_id: "pin:other" })],
  ["member pin revision", (i) => ({ ...i, member_pin_bound_revision: 9 })],
  [
    "wrong directional generation",
    (i) => ({ ...i, outbound_link_generation: "home-to-source-generation" }),
  ],
];
it.effect.each(identityChanges)("rejects changed full identity echo: %s", ([, change]) => {
  const h = harness();
  h.control.reply = (r) => authorized({ ...r, accepted_forward: change(r.accepted_forward) });
  return Effect.gen(function* () {
    const f = yield* fixture;
    expect((yield* f.within(f.service.prepare(input, f.pin)).pipe(Effect.result))._tag).toBe(
      "Failure",
    );
    expect(h.control.calls).toHaveLength(1);
  }).pipe(Effect.provide(h.layer));
});
it.effect.each([
  [
    "receiving pin",
    (r: Wire.WorkstreamsSourceParentReadmissionRequestV3) =>
      authorized({ ...r, receiving_home_pin_id: "member-pin:test" }),
  ],
  [
    "receiving revision",
    (r: Wire.WorkstreamsSourceParentReadmissionRequestV3) =>
      authorized({ ...r, receiving_home_pin_revision: 8 }),
  ],
  [
    "protocol",
    (r: Wire.WorkstreamsSourceParentReadmissionRequestV3) => ({
      ...authorized(r),
      contract: "workstreams-source-readmission/2.0.0",
    }),
  ],
  [
    "extra property",
    (r: Wire.WorkstreamsSourceParentReadmissionRequestV3) => ({
      ...authorized(r),
      destination: "other",
    }),
  ],
  [
    "envelope digest",
    (r: Wire.WorkstreamsSourceParentReadmissionRequestV3) => ({
      ...authorized(r),
      forward_request_sha256: input.identity.request_sha256,
    }),
  ],
  [
    "malformed digest",
    (r: Wire.WorkstreamsSourceParentReadmissionRequestV3) => ({
      ...authorized(r),
      forward_request_sha256: "x",
    }),
  ],
  [
    "widened source ceiling",
    (r: Wire.WorkstreamsSourceParentReadmissionRequestV3) => ({
      ...authorized(r),
      original_ceiling: { ...full, access: "auto", runtime_mode: "auto" },
    }),
  ],
  [
    "read-only source",
    (r: Wire.WorkstreamsSourceParentReadmissionRequestV3) => ({
      ...authorized(r),
      current_ceiling: {
        ...full,
        access: "read-only",
        runtime_mode: "approval-required",
        permissions: ["workstreams:read"],
      },
    }),
  ],
  [
    "plan source",
    (r: Wire.WorkstreamsSourceParentReadmissionRequestV3) => ({
      ...authorized(r),
      current_ceiling: { ...full, interaction_mode: "plan" },
    }),
  ],
  [
    "missing operate",
    (r: Wire.WorkstreamsSourceParentReadmissionRequestV3) => ({
      ...authorized(r),
      current_ceiling: { ...full, scopes: ["orchestration:read"] },
    }),
  ],
] as const)("fails closed for changed %s without retry/fallback", ([, reply]) => {
  const h = harness();
  h.control.reply = reply;
  return Effect.gen(function* () {
    const f = yield* fixture;
    expect((yield* f.within(f.service.prepare(input, f.pin)).pipe(Effect.result))._tag).toBe(
      "Failure",
    );
    expect(h.control.resolutions).toBe(1);
    expect(h.control.calls).toHaveLength(1);
  }).pipe(Effect.provide(h.layer));
});
it.effect("carries admitted receiving modes and refuses read-only receiving permission", () => {
  const h = harness();
  return Effect.gen(function* () {
    const f = yield* fixture;
    const pin = {
      ...f.pin,
      session: {
        ...f.pin.session,
        ceiling: {
          ...f.pin.session.ceiling,
          access: "auto" as const,
          runtime_mode: "auto" as const,
        },
      },
    };
    yield* f.within(
      Effect.gen(function* () {
        yield* (yield* f.service.prepare(input, pin)).checkCurrent();
        expect(h.control.calls[0]?.limits).toBe("auto/default");
      }),
    );
    const readonly = {
      ...pin,
      session: {
        ...pin.session,
        ceiling: {
          ...pin.session.ceiling,
          access: "read-only" as const,
          runtime_mode: "approval-required" as const,
          permissions: ["workstreams:read"] as const,
        },
      },
    };
    expect((yield* f.within(f.service.prepare(input, readonly)).pipe(Effect.result))._tag).toBe(
      "Failure",
    );
    expect(h.control.calls).toHaveLength(1);
  }).pipe(Effect.provide(h.layer));
});
it.effect("does not dispatch when resolution replaces the captured link", () => {
  const h = harness();
  h.control.relinkAtResolve = true;
  return Effect.gen(function* () {
    const f = yield* fixture;
    expect((yield* f.within(f.service.prepare(input, f.pin)).pipe(Effect.result))._tag).toBe(
      "Failure",
    );
    expect(h.control.methods).toEqual([]);
    expect(h.control.resolutions).toBe(1);
  }).pipe(Effect.provide(h.layer));
});
it.effect("checks generation after remote response and locally inside writer", () => {
  const h = harness();
  return Effect.gen(function* () {
    const f = yield* fixture;
    h.control.beforeReply = () =>
      f.sql
        .unsafe("UPDATE jones_peer_environment_links SET secret_name='replacement'")
        .pipe(Effect.asVoid, Effect.orDie);
    expect((yield* f.within(f.service.prepare(input, f.pin)).pipe(Effect.result))._tag).toBe(
      "Failure",
    );
    delete h.control.beforeReply;
    yield* f.within(
      Effect.gen(function* () {
        const p = yield* f.service.prepare(input, f.pin);
        yield* f.sql.withTransaction(
          Effect.gen(function* () {
            yield* f.sql.unsafe(
              "UPDATE jones_peer_environment_links SET secret_name='replacement-again'",
            );
            expect((yield* p.checkCurrent().pipe(Effect.result))._tag).toBe("Failure");
          }),
        );
      }),
    );
    expect(h.control.calls).toHaveLength(2);
  }).pipe(Effect.provide(h.layer));
});
it.effect("rejects clones, expired callback lifetime and reuse inside a fresh scope", () => {
  const h = harness();
  return Effect.gen(function* () {
    const f = yield* fixture;
    const p = yield* f.within(
      Effect.gen(function* () {
        const proof = yield* f.service.prepare(input, f.pin);
        expect((yield* { ...proof }.checkCurrent().pipe(Effect.result))._tag).toBe("Failure");
        return proof;
      }),
    );
    expect((yield* p.checkCurrent().pipe(Effect.result))._tag).toBe("Failure");
    expect((yield* f.within(p.checkCurrent()).pipe(Effect.result))._tag).toBe("Failure");
    expect(h.control.calls).toHaveLength(1);
  }).pipe(Effect.provide(h.layer));
});
it.effect("original registration closure rejects same-tuple authenticator replacement", () => {
  const h = harness();
  return Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.within(
      Effect.gen(function* () {
        const p = yield* f.service.prepare(input, f.pin);
        yield* f.native.authenticate(f.authRequest, f.invocation);
        expect((yield* p.checkCurrent().pipe(Effect.result))._tag).toBe("Failure");
      }),
    );
  }).pipe(Effect.provide(h.layer));
});
it.effect("requires the actual authenticator-registered invocation and callback", () => {
  const h = harness();
  return Effect.gen(function* () {
    const f = yield* fixture,
      cloned = { ...f.invocation };
    const result = yield* Source.withQualificationScope(
      cloned,
      f.service.prepare(input, f.pin),
    ).pipe(Effect.provideService(Mcp.McpInvocationContext, cloned), Effect.result);
    expect(result._tag).toBe("Failure");
    expect((yield* f.service.prepare(input, f.pin).pipe(Effect.result))._tag).toBe("Failure");
    expect(h.control.methods).toEqual([]);
  }).pipe(Effect.provide(h.layer));
});
it.effect.each([
  ["peer", (p: WorkstreamsPeerSessionPinV1) => ({ ...p, peer_environment_id: "peer:other" })],
  [
    "home",
    (p: WorkstreamsPeerSessionPinV1) => ({
      ...p,
      expected_home: { ...p.expected_home, server_generation: 9 },
    }),
  ],
  ["role", (p: WorkstreamsPeerSessionPinV1) => ({ ...p, roles: ["home_source"] })],
  [
    "session",
    (p: WorkstreamsPeerSessionPinV1) => ({
      ...p,
      session: { ...p.session, session_id: "session:other" },
    }),
  ],
  [
    "source",
    (p: WorkstreamsPeerSessionPinV1) => ({
      ...p,
      source_authority: { ...p.source_authority, authority_namespace: "other" },
    }),
  ],
  [
    "enrollment",
    (p: WorkstreamsPeerSessionPinV1) => ({ ...p, source_enrollment_id: "enrollment:other" }),
  ],
  [
    "principal",
    (p: WorkstreamsPeerSessionPinV1) => ({
      ...p,
      bind_receipt: { ...p.bind_receipt!, principal_id: "principal:other" },
    }),
  ],
] as const)("refuses mismatched receiving %s before resolution", ([, change]) => {
  const h = harness();
  return Effect.gen(function* () {
    const f = yield* fixture;
    expect(
      (yield* f
        .within(f.service.prepare(input, change(f.pin) as WorkstreamsPeerSessionPinV1))
        .pipe(Effect.result))._tag,
    ).toBe("Failure");
    expect(h.control.resolutions).toBe(0);
  }).pipe(Effect.provide(h.layer));
});
it.effect(
  "maps authenticated not_found to preaccept unknown only; refusals, unavailable and HTTP404 stay failures",
  () => {
    const h = harness();
    return Effect.gen(function* () {
      const f = yield* fixture;
      h.control.reply = (request) => ({
        contract: Wire.WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V3,
        request,
        state: "denied",
        reason: "not_found",
      });
      expect(yield* f.within(f.service.prepareReadback(readback, f.pin))).toEqual({
        state: "unknown",
        reason: "not_found",
      });
      expect((yield* f.within(f.service.prepare(input, f.pin)).pipe(Effect.result))._tag).toBe(
        "Failure",
      );
      h.control.reply = (request) => ({
        contract: Wire.WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V3,
        request,
        state: "denied",
        reason: "caller_changed",
      });
      expect(
        (yield* f.within(f.service.prepareReadback(readback, f.pin)).pipe(Effect.result))._tag,
      ).toBe("Failure");
      h.control.reply = (request) => ({
        contract: Wire.WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V3,
        request,
        state: "unavailable",
        reason: "unsupported_protocol",
      });
      expect(
        (yield* f.within(f.service.prepareReadback(readback, f.pin)).pipe(Effect.flip)).code,
      ).toBe("unsupported_protocol");
      h.control.status = 404;
      expect(
        (yield* f.within(f.service.prepareReadback(readback, f.pin)).pipe(Effect.result))._tag,
      ).toBe("Failure");
      expect(h.control.calls).toHaveLength(5);
    }).pipe(Effect.provide(h.layer));
  },
);
it.effect(
  "readback supplies the source envelope digest for mandatory retained-ledger comparison",
  () => {
    const h = harness();
    return Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.within(
        Effect.gen(function* () {
          const p = yield* f.service.prepareReadback(readback, f.pin);
          expect(p.state).toBe("authorized");
          if (p.state === "authorized") {
            expect(p).toHaveProperty("forward_request_sha256", digest);
            yield* p.checkCurrent();
          }
        }),
      );
    }).pipe(Effect.provide(h.layer));
  },
);
it.effect(
  "held remote wait allows unrelated same-client writer commit; no network after writer entry",
  () => {
    const h = harness();
    return Effect.gen(function* () {
      const f = yield* fixture,
        entered = yield* Deferred.make<void>(),
        release = yield* Deferred.make<void>();
      h.control.beforeReply = () =>
        Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)));
      const pending = yield* f
        .within(
          Effect.gen(function* () {
            const p = yield* f.service.prepare(input, f.pin);
            yield* f.sql.withTransaction(p.checkCurrent());
          }),
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      yield* f.sql.withTransaction(
        f.sql.unsafe("UPDATE jones_peer_environment_links SET last_error='unrelated-commit'"),
      );
      const rows = yield* f.sql.unsafe<{ last_error: string }>(
        "SELECT last_error FROM jones_peer_environment_links",
      );
      expect(rows[0]?.last_error).toBe("unrelated-commit");
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(pending);
      const before = h.control.calls.length;
      expect(
        (yield* f.sql
          .withTransaction(f.within(f.service.prepare(input, f.pin)))
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      expect(h.control.calls).toHaveLength(before);
      expect(h.control.afterWriter).toBe(0);
    }).pipe(Effect.provide(h.layer));
  },
);
it.effect(
  "documents invisible remote revocation race and freshly observes each independent stage",
  () => {
    const h = harness();
    return Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.within(
        Effect.gen(function* () {
          const p = yield* f.service.prepare(input, f.pin);
          // No local event communicates this remote change; the current observation cannot see it.
          h.control.reply = (request) => ({
            contract: Wire.WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V3,
            request,
            state: "denied",
            reason: "caller_changed",
          });
          yield* f.sql.withTransaction(p.checkCurrent());
        }),
      );
      expect((yield* f.within(f.service.prepare(input, f.pin)).pipe(Effect.result))._tag).toBe(
        "Failure",
      );
      expect(h.control.calls).toHaveLength(2);
    }).pipe(Effect.provide(h.layer));
  },
);

it.effect("native receiving-session revocation is visible inside the same-client writer", () => {
  const h = harness();
  return Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.within(
      Effect.gen(function* () {
        const p = yield* f.service.prepare(input, f.pin);
        yield* f.sql.withTransaction(
          Effect.gen(function* () {
            yield* f.sql.unsafe("UPDATE auth_sessions SET revoked_at='2026-01-01T00:00:00.000Z'");
            expect((yield* p.checkCurrent().pipe(Effect.result))._tag).toBe("Failure");
          }),
        );
      }),
    );
    expect(h.control.calls).toHaveLength(1);
    expect(h.control.afterWriter).toBe(0);
  }).pipe(Effect.provide(h.layer));
});
it.effect("pin expiry during remote observation fails before returning authorization", () => {
  const h = harness();
  return Effect.gen(function* () {
    const f = yield* fixture;
    h.control.beforeReply = () => TestClock.setTime(Date.parse("2026-01-01T00:00:45.000Z"));
    const soon = { ...f.pin, expires_at: "2026-01-01T00:00:30.000Z" };
    expect((yield* f.within(f.service.prepare(input, soon)).pipe(Effect.flip)).code).toBe(
      "invalid_response",
    );
    expect(h.control.calls).toHaveLength(1);
  }).pipe(Effect.provide(h.layer));
});
it.effect("a mutated denied echo cannot negotiate preaccept unknown", () => {
  const h = harness();
  h.control.reply = (request) => ({
    contract: Wire.WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V3,
    request: { ...request, receiving_home_pin_revision: request.receiving_home_pin_revision + 1 },
    state: "denied",
    reason: "not_found",
  });
  return Effect.gen(function* () {
    const f = yield* fixture;
    expect(
      (yield* f.within(f.service.prepareReadback(readback, f.pin)).pipe(Effect.result))._tag,
    ).toBe("Failure");
  }).pipe(Effect.provide(h.layer));
});
