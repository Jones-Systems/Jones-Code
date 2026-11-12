import { referenceForRead } from "./collections.ts";
import { describe, expect, it } from "vite-plus/test";
import * as Wire from "@t3tools/contracts";
import { decideNativeCommand, completeNativeExternalIntent } from "./decider.ts";
import {
  createWorkstreamDomainState,
  lookupReceipt,
  type RegistryCommandInput,
  type WorkstreamAction,
  type RegistryExternalResult,
  type RegistryExternalReservation,
  type OwnerGrant,
} from "./state.ts";
import { nativeSettlementResult, observationCache, matchingPrCache } from "./externalIntents.ts";
import { canonicalJson } from "./validation.ts";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Synchronous digests preserve the frozen cursor and exact-reservation wire bytes without I/O.
import * as NodeCrypto from "node:crypto";

const at = "2026-09-12T12:00:00.000Z";
const grant: OwnerGrant = {
  owner_id: "owner-one",
  principal_id: "principal-one",
  grant_id: "grant-one",
  grant_version: 1,
  permissions: ["workstreams:read", "workstreams:write"],
  state: "active",
};
function harness() {
  let state = createWorkstreamDomainState("owner-one");
  let sequence = 0;
  let commandSequence = 0;
  const ids = {
    next(kind: string) {
      return kind + ":" + String(++sequence).padStart(8, "0");
    },
  };
  const authorization = {
    owner_id: "owner-one",
    actor: { principal_id: "principal-one" },
    grant,
    server_generation: 1,
  };
  const input = (action: WorkstreamAction): RegistryCommandInput => {
    const command = {
      command_id: "command:" + String(++commandSequence).padStart(16, "0"),
      expected_server_generation: state.server_generation,
      expected_registry_version: state.registry_version,
      action,
    };
    return {
      authorization,
      current_grant: grant,
      command,
      request_sha256: NodeCrypto.createHash("sha256").update(canonicalJson(command)).digest("hex"),
      accepted_at: at,
    };
  };
  const submitInput = (value: RegistryCommandInput) => {
    const transition = decideNativeCommand(state, value, ids, at);
    state = transition.state;
    return transition.result;
  };
  const submit = (action: WorkstreamAction) => submitInput(input(action));
  const committed = (action: WorkstreamAction) => {
    const result = submit(action);
    if (!("receipt" in result) || result.receipt.state !== "committed")
      throw new Error("expected committed: " + canonicalJson(result));
    return result.receipt;
  };
  const create = (name = "Synthetic coordination") =>
    committed({
      operation: "create_workstream",
      name,
      lifecycle: "active",
      progress: { state: "unknown" },
      sort_order: 0,
    }).effects.workstream_versions[0]!.workstream_id;
  const ws = (id: string) => state.workstreams.get(id)!;
  const register = (native = "thread-one", provider = "t3", account = "one") => {
    const source = provider === "github" ? "github-public" : "t3-source";
    state.source_bindings.set(source, {
      provider,
      source_instance_id: source,
      authority_namespace: "owner-one",
      store_generation: 1,
      state: "active",
    });
    return committed({
      operation: "register_reference",
      identity: {
        provider,
        source_instance_id: source,
        resource_kind: provider === "github" ? "pull_request" : "thread",
        id_kind: provider === "github" ? "external" : "internal",
        native_id: native,
        account_provenance:
          provider === "github"
            ? { kind: "account", account_id: account, account_namespace: "namespace-" + account }
            : { kind: "not_account_scoped" },
      },
      pr_locator:
        provider === "github"
          ? { host: "github.com", repository_owner: "jones", repository_name: "example", number: 7 }
          : null,
    }).effects.native_reference_id!;
  };
  const finish = (
    intent: RegistryExternalReservation,
    result: RegistryExternalResult,
    recovery = false,
  ) => {
    const transition = completeNativeExternalIntent(state, intent, ids, result, at, recovery);
    state = transition.state;
    return transition.receipt;
  };
  const reserve = (action: WorkstreamAction) => {
    const result = submit(action);
    if (result.state !== "external") throw new Error("expected external: " + canonicalJson(result));
    return result.intent;
  };
  const verify = (id: string) => {
    const reference = state.references.get(id)!;
    const intent = reserve({
      operation: "verify_reference",
      native_reference_id: id,
      expected_attestation_version: reference.registration.attestation_version,
    });
    return finish(intent, {
      registration: {
        state: "attested",
        attestation_version: reference.registration.attestation_version + 1,
        attested_at: at,
        expires_at: "2026-09-12T12:05:00.000Z",
        evidence: {
          provider: reference.identity.provider,
          source_instance_id: reference.identity.source_instance_id,
          authority_namespace: "owner-one",
          native_id: reference.identity.native_id,
          store_generation: 1,
          evidence_sha256: "a".repeat(64),
        },
      },
    });
  };
  const reference = (provider = "t3", account = "one") => {
    const id = register(
      provider === "github" ? "jones/example#7" : "thread-one",
      provider,
      account,
    );
    verify(id);
    return id;
  };
  const attach = (workstream: string, ref: string, secondary = false) =>
    committed({
      operation: secondary ? "link_secondary" : "attach_primary",
      workstream_id: workstream,
      expected_version: ws(workstream).version,
      native_reference_id: ref,
    }).effects.membership_ids[0]!;
  const edge = (
    from: string,
    to: string,
    relation: "continues_as" | "superseded_by" = "continues_as",
  ) =>
    submit({
      operation: "add_edge",
      from_workstream_id: from,
      expected_from_version: ws(from).version,
      to_workstream_id: to,
      expected_to_version: ws(to).version,
      relation,
    });
  const update = (id: string, lifecycle: Wire.WorkstreamLifecycle) =>
    committed({
      operation: "update_workstream",
      workstream_id: id,
      expected_version: ws(id).version,
      name: ws(id).name,
      lifecycle,
      progress: ws(id).progress,
      sort_order: ws(id).sort_order,
    });
  const close = (wsId: string, membership: string) =>
    committed({
      operation: "remove_membership",
      workstream_id: wsId,
      expected_version: ws(wsId).version,
      membership_id: membership,
    });
  const refresh = (wsId: string, membership: string) =>
    reserve({
      operation: "refresh_linked_pr",
      workstream_id: wsId,
      expected_version: ws(wsId).version,
      membership_id: membership,
      expected_observation_version:
        state.observations.get(state.memberships.get(membership)!.native_reference_id)?.at(-1)
          ?.observation_version ?? 0,
    });
  const observe = (
    intent: RegistryExternalReservation,
    outcome: "observed" | "not_modified" | "inaccessible" | "rate_limited" | "invalid_response",
    last_success: (typeof Wire.WorkstreamPrObservation.Type)["last_success"],
    delay: number | null = null,
  ) => {
    const action = intent.command.action;
    if (action.operation !== "refresh_linked_pr") throw new Error("not PR intent");
    return finish(intent, {
      observation: {
        native_reference_id: intent.reference.native_reference_id,
        observation_version: action.expected_observation_version + 1,
        attempted_at: at,
        outcome,
        retry_after_seconds: delay,
        last_success,
        command_id: intent.command.command_id,
      },
    });
  };
  return {
    get state() {
      return state;
    },
    set state(value) {
      state = value;
    },
    authorization,
    ids,
    input,
    submitInput,
    submit,
    committed,
    create,
    ws,
    register,
    reference,
    verify,
    finish,
    reserve,
    attach,
    edge,
    update,
    close,
    refresh,
    observe,
  };
}
function rejected(result: ReturnType<ReturnType<typeof harness>["submit"]>, code: string) {
  expect(result.state).toBe("rejected");
  if (!("receipt" in result) || result.receipt.state !== "rejected")
    throw new Error("expected rejection");
  expect(result.receipt.error.code).toBe(code);
}

describe("native domain transactions", () => {
  it("decides against an immutable snapshot and requires current owner authority before replay", () => {
    const h = harness();
    const snapshot = h.state;
    const input = h.input({
      operation: "create_workstream",
      name: "A",
      lifecycle: "active",
      progress: { state: "unknown" },
      sort_order: 0,
    });
    const r = h.submitInput(input);
    expect(snapshot.registry_version).toBe(0);
    expect(snapshot.workstreams.size).toBe(0);
    expect(h.state.registry_version).toBe(1);
    const bytes = canonicalJson("receipt" in r && r.receipt);
    expect(canonicalJson(h.submitInput(input))).toContain(bytes);
    expect(h.state.audit).toHaveLength(1);
    expect(() =>
      h.submitInput({ ...input, current_grant: { ...grant, state: "revoked" } }),
    ).toThrow("forbidden");
    expect(() =>
      h.submitInput({ ...input, current_grant: { ...grant, grant_version: 2 } }),
    ).toThrow("forbidden");
  });
  it("records stale registry and aggregate rejections at unchanged version, and replays them", () => {
    const h = harness();
    const id = h.create();
    const before = h.state.registry_version;
    const input = h.input({
      operation: "update_workstream",
      workstream_id: id,
      expected_version: 0,
      name: "B",
      lifecycle: "active",
      progress: { state: "unknown" },
      sort_order: 0,
    });
    // A positive but stale aggregate version reaches business rejection after a real update.
    h.update(id, "paused");
    const nextInput = {
      ...input,
      command: {
        ...input.command,
        action: { ...input.command.action, expected_version: 1 } as WorkstreamAction,
      },
    };
    const result = h.submitInput({
      ...nextInput,
      command: { ...nextInput.command, expected_registry_version: h.state.registry_version },
    });
    rejected(result, "version_conflict");
    expect(h.state.registry_version).toBe(before + 1);
    expect(h.state.audit.at(-1)?.changed).toBe(false);
  });
  it("reserves an external effect and pending receipt together, with one active effect per reference", () => {
    const h = harness();
    const id = h.register();
    const before = h.state;
    const intent = h.reserve({
      operation: "verify_reference",
      native_reference_id: id,
      expected_attestation_version: 0,
    });
    expect(before.external_intents.size).toBe(0);
    expect(h.state.external_intents.get(intent.command.command_id)).toEqual(intent);
    expect(h.state.receipts.get(intent.command.command_id)?.state).toBe("pending");
    rejected(
      h.submit({
        operation: "verify_reference",
        native_reference_id: id,
        expected_attestation_version: 0,
      }),
      "backpressure",
    );
  });
  it("fails external completion atomically and preserves pending state on unknown results", () => {
    const h = harness();
    const ref = h.register();
    const intent = h.reserve({
      operation: "verify_reference",
      native_reference_id: ref,
      expected_attestation_version: 0,
    });
    const snapshot = structuredClone(h.state);
    expect(() => h.finish(intent, {})).toThrow("unknown_effect");
    expect(h.state).toEqual(snapshot);
    expect(() => h.finish({ ...intent, request_sha256: "b".repeat(64) }, {})).toThrow(
      "unknown_effect",
    );
  });
  it("recovers only the exact admitted native attempt across changed generations without executing it again", () => {
    const h = harness();
    const ref = h.reference();
    const intent = h.reserve({
      operation: "request_native_t3_settlement",
      native_reference_id: ref,
      expected_attestation_version: 1,
      native_action: "settle",
    });
    const result = nativeSettlementResult(intent, "committed");
    h.state.server_generation = 2;
    h.state.source_bindings.clear();
    h.state.references.clear();
    const receipt = h.finish(intent, result, true);
    expect(receipt.server_generation).toBe(2);
    expect(receipt.effects.native_settlement?.outcome).toBe("committed");
    const size = h.state.audit.length;
    expect(h.finish(intent, result, true)).toEqual(receipt);
    expect(h.state.audit).toHaveLength(size);
  });
  it("rejects bare, unresolved and reservation-substituted native outcomes", () => {
    const h = harness();
    const ref = h.reference();
    const intent = h.reserve({
      operation: "request_native_t3_settlement",
      native_reference_id: ref,
      expected_attestation_version: 1,
      native_action: "settle",
    });
    expect(() => h.finish(intent, { native_outcome: "committed" }, true)).toThrow("unknown_effect");
    expect(() => h.finish(intent, nativeSettlementResult(intent, "unresolved"), true)).toThrow(
      "unknown_effect",
    );
    const other = { ...intent, request_sha256: "b".repeat(64) };
    expect(() => h.finish(intent, nativeSettlementResult(other, "committed"), true)).toThrow(
      "unknown_effect",
    );
  });
  it("preserves declaration revisions, edge closures and both aggregate bumps", () => {
    const h = harness();
    const a = h.create("A"),
      b = h.create("B");
    const declaration = h.committed({
      operation: "set_declaration",
      workstream_id: a,
      expected_version: h.ws(a).version,
      declaration_id: null,
      expected_revision: 0,
      text: "one",
    });
    const id = declaration.effects.declaration_id!;
    h.committed({
      operation: "withdraw_declaration",
      workstream_id: a,
      expected_version: h.ws(a).version,
      declaration_id: id,
      expected_revision: 1,
    });
    expect(h.state.declarations.get(id)?.map((x) => x.state)).toEqual(["active", "withdrawn"]);
    const added = h.edge(a, b);
    if (!("receipt" in added) || added.receipt.state !== "committed")
      throw new Error("edge failed");
    const va = h.ws(a).version,
      vb = h.ws(b).version;
    h.committed({
      operation: "remove_edge",
      edge_id: added.receipt.effects.edge_id!,
      from_workstream_id: a,
      to_workstream_id: b,
      expected_from_version: va,
      expected_to_version: vb,
    });
    expect([h.ws(a).version, h.ws(b).version]).toEqual([va + 1, vb + 1]);
    expect(h.state.edges.get(added.receipt.effects.edge_id!)?.closed).not.toBeNull();
  });
  it("enforces owner scope, registry overflow and caller isolation without partial writes", () => {
    const h = harness();
    const id = h.create();
    h.state.workstreams.set(id, { ...h.ws(id), owner_id: "other-owner" });
    rejected(
      h.submit({
        operation: "update_workstream",
        workstream_id: id,
        expected_version: 1,
        name: "B",
        lifecycle: "active",
        progress: { state: "unknown" },
        sort_order: 0,
      }),
      "not_found",
    );
    h.state.registry_version = Number.MAX_SAFE_INTEGER;
    const before = h.state.workstreams.size;
    rejected(
      h.submit({
        operation: "create_workstream",
        name: "overflow",
        lifecycle: "active",
        progress: { state: "unknown" },
        sort_order: 0,
      }),
      "version_conflict",
    );
    expect(h.state.workstreams.size).toBe(before);
  });
  it("makes absence provable only in a complete same-snapshot receipt archive", () => {
    const h = harness();
    const context = { owner_id: "owner-one", registry_version: 0, server_generation: 1 };
    const base = {
      context,
      native: null,
      archive: null,
      open: null,
      completeness: "import_incomplete" as const,
    };
    expect(lookupReceipt("command:missing", base)).toEqual({
      kind: "unknown",
      reason: "import_incomplete",
    });
    expect(lookupReceipt("command:missing", { ...base, completeness: "complete" }).kind).toBe(
      "proven_absent",
    );
    const receipt = h.committed({
      operation: "create_workstream",
      name: "A",
      lifecycle: "active",
      progress: { state: "unknown" },
      sort_order: 0,
    });
    expect(lookupReceipt(receipt.command_id, { ...base, archive: receipt }).kind).toBe("present");
  });
});

describe("native archive and observation recovery", () => {
  it("replays legacy IDs before registry CAS and fences fresh work until the archive is complete", () => {
    const h = harness();
    const input = h.input({
      operation: "create_workstream",
      name: "legacy",
      lifecycle: "active",
      progress: { state: "unknown" },
      sort_order: 0,
    });
    const result = h.submitInput(input);
    if (!("receipt" in result)) throw new Error("missing receipt");
    h.state.receipts.clear();
    h.state.legacy_receipts.set(input.command.command_id, result.receipt);
    h.state.receipt_archive_state = "import_incomplete";
    const before = structuredClone(h.state);
    expect(h.submitInput(input)).toEqual(result);
    expect(h.state).toEqual(before);
    expect(() => h.submitInput({ ...input, request_sha256: "b".repeat(64) })).toThrow(
      "idempotency_conflict",
    );
    expect(() => h.create("fresh")).toThrow("unknown_effect");
    h.state.receipt_archive_state = "complete";
    expect(h.create("fresh")).toBeTruthy();
  });
  it("releases the exact active hint only after successful external completion", () => {
    const h = harness();
    const ref = h.reference();
    const intent = h.reserve({
      operation: "request_native_t3_settlement",
      native_reference_id: ref,
      expected_attestation_version: 1,
      native_action: "settle",
    });
    h.state.active_reservation = intent;
    const before = h.state.registry_version;
    h.finish(intent, nativeSettlementResult(intent, "denied"));
    expect(h.state.active_reservation).toBeNull();
    expect(h.state.registry_version).toBe(before);
    expect(
      h.reserve({
        operation: "request_native_t3_settlement",
        native_reference_id: ref,
        expected_attestation_version: 1,
        native_action: "unsettle",
      }).kind,
    ).toBe("native_t3_settlement");
  });
  it("requires the recorded external result to remain immutable", () => {
    const h = harness();
    const ref = h.reference();
    const intent = h.reserve({
      operation: "request_native_t3_settlement",
      native_reference_id: ref,
      expected_attestation_version: 1,
      native_action: "settle",
    });
    h.state.external_results.set(
      intent.command.command_id,
      nativeSettlementResult(intent, "denied"),
    );
    expect(() => h.finish(intent, nativeSettlementResult(intent, "committed"), true)).toThrow(
      "unknown_effect",
    );
    expect(h.finish(intent, nativeSettlementResult(intent, "denied"), true).changed).toBe(false);
  });
  it("keeps quarantined references private and displays expiry without mutating evidence", () => {
    const h = harness();
    const ref = h.register();
    const value = h.state.references.get(ref)!;
    expect(referenceForRead(value, "owner-one", "other-principal", undefined, at)).toBeNull();
    expect(referenceForRead(value, "other-owner", "principal-one", undefined, at)).toBeNull();
    h.verify(ref);
    const verified = h.state.references.get(ref)!;
    expect(
      referenceForRead(
        verified,
        "owner-one",
        "other-principal",
        h.state.source_bindings.get("t3-source"),
        "2026-09-12T12:06:00.000Z",
      )?.registration.state,
    ).toBe("expired");
    expect(h.state.references.get(ref)?.registration.state).toBe("attested");
  });
  it("round trips owner-bound ETag evidence and admits a 304 only against that prior representation", () => {
    const h = harness();
    const id = h.create(),
      ref = h.reference("github"),
      membership = h.attach(id, ref);
    const intent = h.refresh(id, membership);
    const observation = {
      native_reference_id: ref,
      observation_version: 1,
      attempted_at: at,
      outcome: "observed" as const,
      retry_after_seconds: null,
      last_success: {
        state: "open" as const,
        draft: true,
        observed_at: at,
        provider_updated_at: null,
      },
      command_id: intent.command.command_id,
    };
    const cache = observationCache(intent, observation, '"fixture-etag"');
    expect(cache?.etag).toBe('"fixture-etag"');
    h.finish(intent, { observation, observation_cache: cache });
    const next = h.refresh(id, membership);
    expect(
      matchingPrCache(next.previous_observation, next.reference, 1, next.source_binding),
    ).toEqual(cache);
    const done = h.observe(next, "not_modified", observation.last_success);
    expect(done.effects.observation?.outcome).toBe("not_modified");
    expect(h.state.observations.get(ref)?.at(-1)?.observation_cache?.etag).toBe('"fixture-etag"');
    expect(done.effects.observation).not.toHaveProperty("observation_cache");
    expect(
      matchingPrCache(
        h.state.observations.get(ref)!.at(-1)!,
        next.reference,
        2,
        next.source_binding,
      ),
    ).toBeNull();
  });
});
