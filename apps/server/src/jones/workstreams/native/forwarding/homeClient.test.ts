// @effect-diagnostics nodeBuiltinImport:off -- Synthetic caller and request bytes have deterministic digests.
import * as NodeCrypto from "node:crypto";
import { expect, it } from "@effect/vitest";
import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Wire from "@t3tools/contracts/jones/workstreamsForwarding";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import * as HttpClient from "effect/http/HttpClient";
import * as Peer from "../../../../peer/PeerMcpClient.ts";
import * as Links from "../../../../peer/PeerLinks.ts";
import { canonicalJson } from "../../operations/encoding.ts";
import { RegistryError } from "../registry/session.ts";
import type {
  MemberRecordedWriteCallerV2,
  AdmittedMemberWrite,
} from "../runtime/memberAdmission.ts";
import * as Client from "./homeClient.ts";

const sha = (s: string) => NodeCrypto.createHash("sha256").update(s).digest("hex");
const record: MemberRecordedWriteCallerV2 = {
  schema: "jones-workstreams-member-recorded-write-caller/2",
  local: {
    schema: "jones-workstreams-member-local-write-caller/2",
    environment_id: "member:test",
    auth: {
      kind: "bearer",
      session_id: "session:test",
      subject: "cli-issued-session",
      method: "bearer-access-token",
    },
    origin: { kind: "cli" },
    ceiling: {
      permissions: ["workstreams:read", "workstreams:write"],
      scopes: ["orchestration:read", "orchestration:operate"],
      capabilities: [],
      limits: { runtimeMode: "approval-required", interactionMode: "default" },
    },
    session: { issued_at: 1, expires_at: 100 },
    request_namespace: null,
    invocation_issued_at: null,
    link_origin: null,
    link_session: null,
    captured_at: 1,
    target: { kind: "metadata" },
  },
  association: {
    enrollment_id: "enrollment:test",
    enrollment_session_id: "enrollment-session:test",
    intent_sha256: "a".repeat(64),
    home: {
      registry_instance_id: "11111111-1111-4111-8111-111111111111",
      owner_id: "owner:test",
      home_environment_id: "home:test",
      server_generation: 1,
    },
    canonical_origin: "https://native.workstreams.invalid",
    principal_id: "principal:test",
    singleton_binding_version: 1,
    pin_bound_revision: 2,
    source_binding_version: 3,
    source: {
      environment_id: "member:test",
      authority_namespace: "store:test",
      store_generation: 1,
    },
    link_generation: "opaque:link",
    member_pin_id: "member-pin:test",
    home_pin_id: "home-pin:test",
    home_pin_bound_revision: 4,
  },
  original_outbound_ceiling: {
    access: "auto",
    runtime_mode: "auto",
    interaction_mode: "default",
    scopes: ["orchestration:read", "orchestration:operate"],
    permissions: ["workstreams:read", "workstreams:write"],
  },
};
const body = canonicalJson({
  command_id: "appearance:client-001",
  workstream_id: "workstream:test",
  expected_server_generation: 1,
  expected_version: 2,
  border_color: "#112233",
});
const a = record.association;
const request: Wire.WorkstreamsForwardRequest = {
  contract: Wire.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
  kind: "appearance",
  identity: {
    forward_id: "forward:client-00001",
    source_operation_id: "appearance:client-001",
    parent_operation_id: null,
    command_ids: ["appearance:client-001"],
    request_sha256: sha(body),
    recorded_caller_sha256: sha(canonicalJson(record)),
    expected_home: a.home,
    canonical_origin: a.canonical_origin,
    source_authority: a.source,
    source_enrollment_id: a.enrollment_id,
    source_enrollment_session_id: a.enrollment_session_id,
    principal_id: a.principal_id,
    singleton_binding_version: a.singleton_binding_version,
    source_binding_version: a.source_binding_version,
    member_pin_id: a.member_pin_id,
    member_pin_bound_revision: a.pin_bound_revision,
    outbound_link_generation: a.link_generation,
  },
  request: {
    expected_home: a.home,
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
};
const result: Wire.WorkstreamsForwardResult = {
  contract: request.contract,
  kind: "appearance",
  identity: request.identity,
  home: {
    registry: { ...a.home, canonical_origin: a.canonical_origin, registry_version: 22 },
    principal_id: a.principal_id,
    authorization_revision: 9,
    source_binding_version: a.source_binding_version,
    incoming_peer_pin_id: a.home_pin_id,
    incoming_peer_pin_revision: a.home_pin_bound_revision,
  },
  result: { workstream_id: "workstream:test", border_color: "#112233", version: 3 },
};
const admitted = (
  checkCurrent: AdmittedMemberWrite["checkCurrent"] = () =>
    Effect.succeed(record.local.ceiling.limits!),
): AdmittedMemberWrite => ({
  record,
  ceiling: record.local.ceiling,
  limits: record.local.ceiling.limits!,
  checkCurrent,
});
const refused = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.exit(effect).pipe(Effect.map((r) => r._tag));
const fixture = (reply: unknown = result) => {
  const calls: Parameters<Peer.PeerMcpClient["Service"]["call"]>[0][] = [];
  const peer = Layer.succeed(Peer.PeerMcpClient, {
    call: (input) => {
      calls.push(input);
      // oxlint-disable-next-line t3code/no-inline-schema-compile -- Synthetic generic peer must decode the schema supplied on each production call.
      return Schema.decodeUnknownEffect(input.success)(reply).pipe(
        Effect.mapError(
          () =>
            new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "synthetic decode",
            }),
        ),
      );
    },
  });
  return { calls, layer: Client.layer.pipe(Layer.provide(peer)) };
};

it.effect(
  "only fixed invocation tools receive captured generation, original narrowed limits and no escape arguments",
  () => {
    const f = fixture();
    return Effect.gen(function* () {
      expect(yield* (yield* Client.HomeForwardClient).forward(request, admitted())).toEqual(result);
      expect(f.calls).toHaveLength(1);
      expect(f.calls[0]).toMatchObject({
        environmentId: "home:test",
        expectedGeneration: "opaque:link",
        tool: "jones_workstreams_home_forward",
        arguments: request,
        limits: record.local.ceiling.limits,
      });
      expect(f.calls[0]?.origin).toBeUndefined();
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect("unknown/not_found is a negotiation response without an absence claim", () => {
  const reply = {
    contract: request.contract,
    identity: request.identity,
    state: "unknown",
    reason: "not_found",
  };
  const f = fixture(reply);
  return Effect.gen(function* () {
    const client = yield* Client.HomeForwardClient;
    yield* client.preflight(request, admitted());
    expect(yield* client.readback(request, admitted())).toEqual(reply);
    expect(f.calls.every((c) => c.tool === "jones_workstreams_home_forward_readback")).toBe(true);
  }).pipe(Effect.provide(f.layer));
});

it.effect.each([
  { ...result, contract: "unsupported/2" },
  { ...result, callback: "forbidden" },
  { ...result, identity: { ...result.identity, outbound_link_generation: "relinked" } },
  { ...result, home: { ...result.home, incoming_peer_pin_revision: 5 } },
  { ...result, home: { ...result.home, incoming_peer_pin_id: a.member_pin_id } },
  { ...result, home: { ...result.home, principal_id: "principal:other" } },
  { ...result, result: { ...result.result, version: 99 } },
  { ...result, result: { ...result.result, workstream_id: "workstream:other" } },
])("strictly refuses unsupported/excess/identity/provenance/result mismatch %#", (reply) => {
  const f = fixture(reply);
  return refused(
    Effect.gen(function* () {
      return yield* (yield* Client.HomeForwardClient).forward(request, admitted());
    }).pipe(Effect.provide(f.layer)),
  ).pipe(Effect.map((state) => expect(state).toBe("Failure")));
});

it.effect("currentness is checked after network before exposing a committed result", () => {
  const f = fixture();
  let n = 0;
  const caller = admitted(() =>
    ++n === 1
      ? Effect.succeed(record.local.ceiling.limits!)
      : Effect.fail(new RegistryError({ code: "forbidden" })),
  );
  return Effect.gen(function* () {
    expect(yield* refused((yield* Client.HomeForwardClient).forward(request, caller))).toBe(
      "Failure",
    );
    expect(f.calls).toHaveLength(1);
  }).pipe(Effect.provide(f.layer));
});

it.effect(
  "captured expectedGeneration reaches production PeerMcpTransport and rejects a relink before HTTP",
  () => {
    let http = 0;
    const links = Layer.succeed(Links.PeerLinks, {
      link: () => Effect.die("unused"),
      list: Effect.succeed([]),
      get: () => Effect.succeed(Option.none()),
      unlink: () => Effect.succeed(false),
      resolve: () => Effect.die("unused"),
      recordOutcome: () => Effect.void,
      currentGeneration: () => Effect.succeed(Option.some("opaque:replacement")),
    });
    const network = Layer.succeed(
      HttpClient.HttpClient,
      HttpClient.make(() => {
        http++;
        return Effect.die("HTTP must not run for a stale generation");
      }),
    );
    const layer = Client.layer.pipe(
      Layer.provide(Peer.layer.pipe(Layer.provide(Layer.mergeAll(links, network)))),
    );
    return Effect.gen(function* () {
      expect(yield* refused((yield* Client.HomeForwardClient).forward(request, admitted()))).toBe(
        "Failure",
      );
      expect(http).toBe(0);
    }).pipe(Effect.provide(layer));
  },
);
