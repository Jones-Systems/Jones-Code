// @effect-diagnostics nodeBuiltinImport:off -- Exact synthetic retained request bytes bind the native readback.
import * as NodeCrypto from "node:crypto";
import { expect, it } from "@effect/vitest";
import { OrchestratorMcpFailure, type WorkstreamReceipt } from "@t3tools/contracts";
import * as Wire from "@t3tools/contracts/jones/workstreamsForwarding";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Peer from "../../../../peer/PeerMcpClient.ts";
import { canonicalJson } from "../../operations/encoding.ts";
import { RegistryError } from "../registry/session.ts";
import type {
  MemberRecordedWriteCallerV2,
  AdmittedMemberWrite,
} from "../runtime/memberAdmission.ts";
import * as Admission from "../runtime/memberAdmission.ts";
import * as Store from "../store/operationStore.ts";
import * as Home from "./homeClient.ts";
import * as Receipts from "./memberCommandReceipts.ts";
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

const body =
  JSON.stringify(
    {
      command_id: "command:receipt-00001",
      expected_server_generation: 1,
      expected_registry_version: 9,
      action: {
        operation: "create_workstream",
        name: "Retained original",
        lifecycle: "active",
        progress: { state: "unknown" },
        sort_order: 0,
      },
    },
    null,
    2,
  ) + "\n";
const a = record.association;
const id = "command:receipt-00001";
const request: Wire.WorkstreamsForwardRequest = {
  contract: Wire.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
  kind: "command",
  identity: {
    forward_id: "jwf-receipt-test",
    source_operation_id: id,
    parent_operation_id: null,
    command_ids: [id],
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
    body,
    body_sha256: sha(body),
    relay: {
      source_environment_id: a.source.environment_id,
      origin: record.local.origin,
      linked_origin_environment_id: null,
      hop: 1,
    },
  },
};
const receipt: WorkstreamReceipt = {
  command_id: id,
  owner_id: a.home.owner_id,
  actor: { principal_id: a.principal_id },
  operation: "create_workstream",
  request_sha256: sha(body),
  server_generation: 1,
  accepted_at: "2026-01-01T00:00:00.000Z",
  state: "pending",
  retry_after_seconds: 1,
};
const result: Wire.WorkstreamsForwardResult = {
  contract: request.contract,
  kind: "command",
  identity: request.identity,
  home: {
    registry: { ...a.home, canonical_origin: a.canonical_origin, registry_version: 9 },
    principal_id: a.principal_id,
    authorization_revision: 1,
    source_binding_version: a.source_binding_version,
    incoming_peer_pin_id: a.home_pin_id,
    incoming_peer_pin_revision: a.home_pin_bound_revision,
  },
  result: receipt,
};
const row: Store.MemberForwardOperation = {
  operation_id: id,
  kind: "command",
  thread_environment_id: null,
  thread_id: null,
  origin: record.local.origin,
  destination: {
    registry_instance_id: a.home.registry_instance_id,
    owner_id: a.home.owner_id,
    home_environment_id: a.home.home_environment_id,
    home_generation: a.home.server_generation,
    principal_id: a.principal_id,
    binding_version: a.singleton_binding_version,
  },
  child_command_ids: [id],
  request_body: body,
  request_sha256: sha(body),
  accepted_at: "2026-01-01T00:00:00.000Z",
  updated_at: "2026-01-01T00:00:00.000Z",
  next_poll_at: null,
  state: "reconciling",
  result_body: null,
  recorded_caller: null,
  member_forward: {
    schema: "jones-workstreams-member-forward-accepted/1",
    kind: "MEMBER-FORWARD",
    forward_request_body: canonicalJson(request),
    forward_request_sha256: sha(canonicalJson(request)),
    recorded_caller: record,
  },
};
const fixture = (
  options: {
    row?: Store.MemberForwardOperation | null;
    denied?: boolean;
    reply?: unknown;
    failAfter?: boolean;
    changedLimits?: boolean;
  } = {},
) => {
  const calls: Array<Parameters<Peer.PeerMcpClient["Service"]["call"]>[0]> = [];
  let admissions = 0,
    checks = 0;
  const admitted: AdmittedMemberWrite = {
    record,
    ceiling: record.local.ceiling,
    limits: record.local.ceiling.limits!,
    checkCurrent: () =>
      Effect.suspend(() => {
        checks++;
        if (options.failAfter && checks >= 3)
          return Effect.fail(new RegistryError({ code: "forbidden" }));
        return Effect.succeed(
          options.changedLimits && checks >= 2
            ? { ...record.local.ceiling.limits!, interactionMode: "plan" as const }
            : record.local.ceiling.limits!,
        );
      }),
  };
  const ports = Layer.mergeAll(
    Layer.mock(Store.OperationStore)({
      readMemberForward: (input) =>
        Effect.succeed(input !== id ? null : options.row === undefined ? row : options.row),
    }),
    Layer.mock(Admission.MemberAdmission)({
      readmitWrite: (input) =>
        Effect.suspend(() => {
          admissions++;
          expect(input).toEqual(record);
          return options.denied
            ? Effect.fail(new RegistryError({ code: "forbidden" }))
            : Effect.succeed(admitted);
        }),
    }),
    Layer.succeed(Peer.PeerMcpClient, {
      call: (input) => {
        calls.push(input);
        expect(input.tool).toBe("jones_workstreams_home_forward_readback");
        // oxlint-disable-next-line t3code/no-inline-schema-compile -- The external peer applies the production client's supplied decoder.
        return Schema.decodeEffect(input.success)(
          options.reply ?? {
            contract: request.contract,
            identity: request.identity,
            state: "present",
            result,
          },
        ).pipe(
          Effect.mapError(
            () =>
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "Synthetic peer refusal",
              }),
          ),
        );
      },
    }),
  );
  return {
    layer: Receipts.layer.pipe(Layer.provideMerge(Home.layer), Layer.provide(ports)),
    calls,
    admissions: () => admissions,
  };
};
it.effect(
  "looks up exact original bytes/destination with normal admission and never forwards",
  () => {
    const f = fixture();
    return Effect.gen(function* () {
      const service = yield* Receipts.MemberCommandReceipts;
      expect(yield* service.lookup({ command_id: id })).toEqual(receipt);
      expect(yield* service.lookup({ command_id: id, retained_body: body })).toEqual(receipt);
      expect(f.calls).toHaveLength(2);
      for (const call of f.calls)
        expect(call).toMatchObject({
          environmentId: a.home.home_environment_id,
          expectedGeneration: a.link_generation,
          limits: record.local.ceiling.limits,
          arguments: { contract: request.contract, identity: request.identity },
        });
      expect(f.admissions()).toBe(2);
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect.each(["missing", "appearance"] as const)(
  "keeps %s local intent unknown without peer calls",
  (fault) => {
    const f = fixture({ row: fault === "missing" ? null : { ...row, kind: "appearance" } });
    return Effect.gen(function* () {
      expect(
        yield* (yield* Receipts.MemberCommandReceipts).lookup({ command_id: id }).pipe(Effect.flip),
      ).toMatchObject({ code: "unknown_effect" });
      expect(f.calls).toHaveLength(0);
      expect(f.admissions()).toBe(0);
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect(
  "changed bytes conflict only after original-caller admission, without a remote call",
  () => {
    const f = fixture();
    return Effect.gen(function* () {
      expect(
        yield* (yield* Receipts.MemberCommandReceipts)
          .lookup({ command_id: id, retained_body: body + " " })
          .pipe(Effect.flip),
      ).toMatchObject({ code: "operation_id_conflict" });
      expect(f.calls).toHaveLength(0);
      expect(f.admissions()).toBe(1);
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect(
  "a different credential cannot inspect retained bytes or use background admission",
  () => {
    const f = fixture({ denied: true });
    return Effect.gen(function* () {
      expect(
        yield* (yield* Receipts.MemberCommandReceipts)
          .lookup({ command_id: id, retained_body: "changed" })
          .pipe(Effect.flip),
      ).toMatchObject({ code: "forbidden" });
      expect(f.calls).toHaveLength(0);
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect.each(["not_found", "in_flight", "authority_unavailable", "generation_mismatch"] as const)(
  "%s home lookup never becomes an absence proof or another send",
  (reason) => {
    const f = fixture({
      reply: { contract: request.contract, identity: request.identity, state: "unknown", reason },
    });
    return Effect.gen(function* () {
      expect(
        (yield* (yield* Receipts.MemberCommandReceipts)
          .lookup({ command_id: id })
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      expect(f.calls).toHaveLength(1);
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect.each(["identity", "receipt", "extra"] as const)(
  "refuses %s response substitution",
  (fault) => {
    const f = fixture({
      reply:
        fault === "identity"
          ? {
              contract: request.contract,
              identity: { ...request.identity, outbound_link_generation: "replacement" },
              state: "present",
              result,
            }
          : {
              contract: request.contract,
              identity: request.identity,
              state: "present",
              result:
                fault === "extra"
                  ? { ...result, private_extra: true }
                  : { ...result, result: { ...receipt, request_sha256: "f".repeat(64) } },
            },
    });
    return Effect.gen(function* () {
      expect(
        (yield* (yield* Receipts.MemberCommandReceipts)
          .lookup({ command_id: id })
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      expect(f.calls).toHaveLength(1);
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect.each(["revoked", "limits"] as const)(
  "refuses %s original caller currentness drift",
  (fault) => {
    const f = fixture({ failAfter: fault === "revoked", changedLimits: fault === "limits" });
    return Effect.gen(function* () {
      expect(
        (yield* (yield* Receipts.MemberCommandReceipts)
          .lookup({ command_id: id })
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      expect(f.calls).toHaveLength(fault === "limits" ? 0 : 1);
    }).pipe(Effect.provide(f.layer));
  },
);
