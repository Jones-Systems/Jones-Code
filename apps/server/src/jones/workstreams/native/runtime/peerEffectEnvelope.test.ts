// @effect-diagnostics nodeBuiltinImport:off -- Synthetic recovery fixtures bind exact byte hashes.
import * as NodeCrypto from "node:crypto";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Envelope from "./peerEffectEnvelope.ts";
import * as Recorded from "./recordedCaller.ts";
import * as EffectCrypto from "@effect/platform-node/NodeCrypto";
const unwrap = (input: unknown) =>
  Recorded.unwrapEffectIntent(input).pipe(Effect.provide(EffectCrypto.layer));

const sha = (value: string) => NodeCrypto.createHash("sha256").update(value).digest("hex");
const fixture = () => {
  const command = {
    command_id: "command:peer-effect-original-0001",
    expected_server_generation: 1,
    expected_registry_version: 7,
    action: {
      operation: "request_native_t3_settlement",
      native_reference_id: "reference:original",
      expected_attestation_version: 1,
      native_action: "settle",
    },
  };
  const raw = " \n" + JSON.stringify(command, null, 2) + "\n";
  const home = {
    registry_instance_id: "11111111-1111-4111-8111-111111111111",
    owner_id: "owner:test",
    home_environment_id: "home:test",
    server_generation: 1,
  };
  const identity = {
    forward_id: "jwf-peer-forward-original-0001",
    source_operation_id: command.command_id,
    parent_operation_id: null,
    command_ids: [command.command_id],
    request_sha256: sha(raw),
    recorded_caller_sha256: "b".repeat(64),
    expected_home: home,
    canonical_origin: "https://home.invalid",
    source_authority: {
      environment_id: "source:test",
      authority_namespace: "source-store",
      store_generation: 3,
    },
    source_enrollment_id: "enrollment:original",
    source_enrollment_session_id: "session:source-original",
    principal_id: "principal:test",
    singleton_binding_version: 2,
    source_binding_version: 4,
    member_pin_id: "pin:member",
    member_pin_bound_revision: 2,
    outbound_link_generation: "member-to-home:\u03b2",
  };
  const grant = {
    owner_id: home.owner_id,
    principal_id: identity.principal_id,
    grant_id: "grant:original",
    grant_version: 2,
    permissions: ["workstreams:read", "workstreams:write"],
    state: "active",
  };
  const intent = {
    kind: "native_t3_settlement",
    command,
    owner_id: home.owner_id,
    actor: { principal_id: identity.principal_id },
    request_sha256: sha(raw),
    authorization: {
      owner_id: home.owner_id,
      actor: { principal_id: identity.principal_id },
      server_generation: 1,
      grant,
    },
    reference: { native_reference_id: "reference:original", owner_id: home.owner_id },
    retained_extra: "unchanged synthetic intent sentinel",
  };
  return {
    format: "jones-native-effect-recovery/2",
    kind: "PEER",
    intent_body: "\n" + JSON.stringify(intent, null, 2),
    request_body: raw,
    request_sha256: sha(raw),
    recovery: {
      schema: "jones-workstreams-peer-effect-recovery/2",
      kind: "source-recorded-forward",
      incoming_peer_pin_id: "pin:receiving-home",
      incoming_peer_pin_revision: 2,
      source_readmission: {
        contract: "workstreams-source-readmission/2.0.0",
        accepted_forward: identity,
        command_id: command.command_id,
        command_request_sha256: sha(raw),
      },
      original_ceiling: {
        access: "full-access",
        runtime_mode: "auto",
        interaction_mode: "default",
        scopes: ["orchestration:read", "orchestration:operate"],
        permissions: ["workstreams:read", "workstreams:write"],
      },
    },
    forward_request_sha256: "c".repeat(64),
    source_link_generation: "home-to-source:\u03b3",
  };
};

it.effect(
  "retains exact raw bytes and separate directional generations without inventing HOME caller authority",
  () =>
    Effect.gen(function* () {
      const input = fixture();
      const value = yield* Envelope.decodeStoredPeerEffect(input);
      expect(value).toEqual(input);
      const decoded = yield* unwrap(value);
      expect(decoded.peer_recovery).toEqual(value);
      expect(decoded.recovery).toBeNull();
      expect(decoded.intent).toEqual(JSON.parse(input.intent_body));
      expect("recorded_caller" in value).toBe(false);
      expect(value.source_link_generation).not.toBe(
        value.recovery.source_readmission.accepted_forward.outbound_link_generation,
      );
      expect(value.forward_request_sha256).not.toBe(value.request_sha256);
      expect(
        yield* Envelope.wrapPeerEffectIntent(
          value.intent_body,
          value.request_body,
          value.request_sha256,
          value.recovery,
          value.forward_request_sha256,
          value.source_link_generation,
        ),
      ).toEqual(value);
    }),
);

it.effect.each([
  [
    "changed raw whitespace",
    (v: ReturnType<typeof fixture>) => ({ ...v, request_body: v.request_body + " " }),
  ],
  [
    "changed inner digest",
    (v: ReturnType<typeof fixture>) => ({ ...v, request_sha256: "f".repeat(64) }),
  ],
  ["wrong kind", (v: ReturnType<typeof fixture>) => ({ ...v, kind: "HOME" })],
  [
    "wrong format",
    (v: ReturnType<typeof fixture>) => ({ ...v, format: "jones-native-effect-recovery/1" }),
  ],
  ["fabricated home caller", (v: ReturnType<typeof fixture>) => ({ ...v, recorded_caller: {} })],
  [
    "numeric link generation",
    (v: ReturnType<typeof fixture>) => ({ ...v, source_link_generation: 3 }),
  ],
  [
    "empty link generation",
    (v: ReturnType<typeof fixture>) => ({ ...v, source_link_generation: "" }),
  ],
  [
    "oversize link generation",
    (v: ReturnType<typeof fixture>) => ({ ...v, source_link_generation: "x".repeat(513) }),
  ],
  [
    "legacy recovery alias",
    (v: ReturnType<typeof fixture>) => ({
      ...v,
      recovery: { ...v.recovery, schema: "jones-workstreams-peer-effect-recovery/1" },
    }),
  ],
  [
    "another raw command",
    (v: ReturnType<typeof fixture>) => ({
      ...v,
      intent_body: v.intent_body.replace(
        "command:peer-effect-original-0001",
        "command:peer-effect-replaced-0001",
      ),
    }),
  ],
  [
    "another principal",
    (v: ReturnType<typeof fixture>) => ({
      ...v,
      intent_body: v.intent_body.replaceAll("principal:test", "principal:other"),
    }),
  ],
  [
    "another owner",
    (v: ReturnType<typeof fixture>) => ({
      ...v,
      intent_body: v.intent_body.replaceAll("owner:test", "owner:other"),
    }),
  ],
  [
    "another reference",
    (v: ReturnType<typeof fixture>) => ({
      ...v,
      intent_body: v.intent_body.replace("reference:original", "reference:replacement"),
    }),
  ],
] as const)("refuses %s before treating stored data as a peer origin", ([_label, mutate]) =>
  Effect.gen(function* () {
    expect(
      (yield* Envelope.decodeStoredPeerEffect(mutate(fixture())).pipe(Effect.result))._tag,
    ).toBe("Failure");
    expect((yield* unwrap(mutate(fixture())).pipe(Effect.result))._tag).toBe("Failure");
  }),
);

it.effect(
  "keeps historical unwrapped intents decodable and never turns them into recovery authority",
  () =>
    Effect.gen(function* () {
      const intent = { historical: "original bytes and no original caller" };
      expect(yield* unwrap(intent)).toEqual({ intent, recovery: null });
    }),
);
