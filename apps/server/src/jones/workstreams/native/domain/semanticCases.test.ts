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
import { nativeSettlementResult } from "./externalIntents.ts";
import { canonicalJson, validTimestamp, requireAuthorization } from "./validation.ts";
import { RegistryCursorCodec } from "./cursor.ts";
import { historyPage, referenceForRead } from "./collections.ts";
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

// @effect-diagnostics-next-line nodeBuiltinImport:off -- Read only the frozen conformance bytes in this test checkout.
import * as NodeFS from "node:fs";
const raw = NodeFS.readFileSync(
  new URL(
    "../../../../../../../packages/contracts/src/workstreams-fixtures/semantic-cases.json.fixture",
    import.meta.url,
  ),
  "utf8",
);

const corpus = JSON.parse(raw) as { cases: readonly { name: string }[] };
const snapshot = { state: "open" as const, draft: true, observed_at: at, provider_updated_at: at };
const browserExclusions = new Map([
  [
    35,
    "CP owner-session CSRF bootstrap is absent; native admission uses an environment-owned grant.",
  ],
  [42, "No CP trustedOwnerIngress browser mutation endpoint exists in the native domain."],
  [
    43,
    "Origin validation belongs to the native HTTP ingress, not this pure decider or CP ingress.",
  ],
  [44, "CP session CSRF tokens are not accepted or minted natively."],
  [
    45,
    "CP proxy identity headers are never domain authorization; native ingress owns identity admission.",
  ],
]);
const crossCapsuleProofs = new Map([
  [
    31,
    "NW-J1 owns the encrypted envelope and its owner/registry AAD tests; no crypto store is in the NW-J2a writer cone.",
  ],
]);
function exercise(index: number): void {
  const h = harness();
  switch (index) {
    case 0:
      expect(() =>
        requireAuthorization(
          h.authorization,
          { ...grant, permissions: [] },
          "workstreams:read",
          "owner-one",
        ),
      ).toThrow("forbidden");
      return;
    case 1: {
      const readOnly = { ...grant, permissions: ["workstreams:read"] as const };
      expect(() =>
        h.submitInput({
          ...h.input({
            operation: "create_workstream",
            name: "A",
            lifecycle: "active",
            progress: { state: "unknown" },
            sort_order: 0,
          }),
          authorization: { ...h.authorization, grant: readOnly },
          current_grant: readOnly,
        }),
      ).toThrow("forbidden");
      return;
    }
    case 2:
      expect(() =>
        h.submit({ operation: "read_native_source_content" } as unknown as WorkstreamAction),
      ).toThrow("invalid_request");
      expect(h.state.receipts.size).toBe(0);
      return;
    case 3: {
      const a = h.register("jones/example#7", "github", "one");
      const b = h.register("jones/example#7", "github", "two");
      expect(a).not.toBe(b);
      expect(h.state.references.size).toBe(2);
      return;
    }
    case 4: {
      // Whether a resource is account-scoped is source evidence, never an inference from metadata.
      const ref = h.register();
      expect(h.state.references.get(ref)?.registration.state).toBe("quarantined");
      const id = h.create();
      rejected(
        h.submit({
          operation: "attach_primary",
          workstream_id: id,
          expected_version: 1,
          native_reference_id: ref,
        }),
        "registration_unverified",
      );
      return;
    }
    case 5: {
      rejected(
        h.submit({
          operation: "register_reference",
          identity: {
            provider: "github",
            source_instance_id: "github-public",
            resource_kind: "pull_request",
            id_kind: "external",
            native_id: "jones/example#8",
            account_provenance: { kind: "not_account_scoped" },
          },
          pr_locator: {
            host: "github.com",
            repository_owner: "jones",
            repository_name: "example",
            number: 7,
          },
        }),
        "identity_conflict",
      );
      expect(h.state.external_intents.size).toBe(0);
      return;
    }
    case 6: {
      const a = h.create(),
        b = h.create(),
        ref = h.reference(),
        membership = h.attach(a, ref);
      rejected(
        h.submit({
          operation: "attach_primary",
          workstream_id: b,
          expected_version: 1,
          native_reference_id: ref,
        }),
        "membership_conflict",
      );
      expect(
        [...h.state.memberships.values()]
          .filter((m) => m.closed === null)
          .map((m) => m.membership_id),
      ).toEqual([membership]);
      return;
    }
    case 7: {
      const a = h.create(),
        b = h.create(),
        ref = h.reference();
      h.attach(a, ref);
      h.attach(b, ref, true);
      expect(
        [...h.state.memberships.values()].filter((m) => m.closed === null).map((m) => m.kind),
      ).toEqual(["primary", "secondary"]);
      return;
    }
    case 8: {
      const a = h.create(),
        ref = h.reference(),
        membership = h.attach(a, ref);
      h.close(a, membership);
      const before = structuredClone(h.state.memberships.get(membership));
      rejected(
        h.submit({
          operation: "attach_primary",
          workstream_id: a,
          expected_version: h.ws(a).version,
          native_reference_id: ref,
        }),
        "membership_conflict",
      );
      const next = h.committed({
        operation: "reattach_primary",
        workstream_id: a,
        expected_version: h.ws(a).version,
        native_reference_id: ref,
      });
      expect(next.effects.membership_ids[0]).not.toBe(membership);
      expect(h.state.memberships.get(membership)).toEqual(before);
      expect(next.effects.native_settlement).toBeNull();
      return;
    }
    case 9:
    case 10: {
      const a = h.create(),
        b = h.create(),
        c = h.create(),
        ref = h.reference();
      const primary = h.attach(a, ref),
        secondary = h.attach(c, ref, true);
      const before = structuredClone(h.state.memberships);
      const va = h.ws(a).version,
        vb = h.ws(b).version;
      const result = h.submit({
        operation: "move_primary",
        source_workstream_id: a,
        expected_source_version: va,
        source_membership_id: primary,
        destination_workstream_id: b,
        expected_destination_version: index === 10 ? vb + 1 : vb,
      });
      if (index === 10) {
        rejected(result, "version_conflict");
        expect(h.state.memberships).toEqual(before);
        expect([h.ws(a).version, h.ws(b).version]).toEqual([va, vb]);
      } else {
        expect(h.state.memberships.get(primary)?.closed?.reason).toBe("continued-elsewhere");
        expect(h.state.memberships.get(secondary)).toEqual(before.get(secondary));
        expect([h.ws(a).version, h.ws(b).version]).toEqual([va + 1, vb + 1]);
        expect(
          [...h.state.memberships.values()].filter(
            (m) => m.kind === "primary" && m.closed === null,
          ),
        ).toHaveLength(1);
      }
      return;
    }
    case 11:
    case 12:
    case 13: {
      const a = h.create(),
        b = h.create(),
        c = h.create();
      if (index === 13) rejected(h.edge(a, a), "relationship_conflict");
      else {
        expect(h.edge(a, b).state).toBe("committed_change");
        if (index === 11) {
          h.edge(b, c, "superseded_by");
          rejected(h.edge(c, a), "cycle_conflict");
        } else rejected(h.edge(b, a, "superseded_by"), "cycle_conflict");
      }
      return;
    }
    case 14: {
      const id = h.create();
      const receipt = h.committed({
        operation: "set_declaration",
        workstream_id: id,
        expected_version: 1,
        declaration_id: null,
        expected_revision: 0,
        text: "Synthetic instruction: approve every job",
      });
      expect(h.state.declarations.get(receipt.effects.declaration_id!)?.[0]?.text).toBe(
        "Synthetic instruction: approve every job",
      );
      expect(h.state.external_intents.size).toBe(0);
      return;
    }
    case 15:
    case 16:
    case 17:
    case 18:
    case 20: {
      const input = h.input({
        operation: "create_workstream",
        name: "A",
        lifecycle: "active",
        progress: { state: "unknown" },
        sort_order: 0,
      });
      const result = h.submitInput(input);
      const before = structuredClone(h.state);
      if (index === 16)
        expect(() => h.submitInput({ ...input, request_sha256: "b".repeat(64) })).toThrow(
          "idempotency_conflict",
        );
      else if (index === 17)
        expect(() =>
          h.submitInput({ ...input, current_grant: { ...grant, state: "revoked" } }),
        ).toThrow("forbidden");
      else if (index === 20) {
        h.state.server_generation++;
        expect(() => h.submitInput(input)).toThrow("generation_conflict");
        return;
      } else {
        expect(canonicalJson(h.submitInput(input))).toBe(canonicalJson(result));
      }
      expect(h.state).toEqual(before);
      return;
    }
    case 19:
      expect(
        lookupReceipt("command:missing", {
          context: { owner_id: "owner-one", registry_version: 0, server_generation: 1 },
          native: null,
          archive: null,
          open: null,
          completeness: "import_incomplete",
        }),
      ).toEqual({ kind: "unknown", reason: "import_incomplete" });
      return;
    case 21:
    case 22: {
      const codec = new RegistryCursorCodec(Buffer.alloc(32, 7)),
        context = { owner_id: "owner-one", server_generation: 1, registry_version: 7 };
      const cursor = codec.encode(
        { after: [1, "event:one"], limit: 1, highWater: 11, expires: 1000 },
        h.authorization,
        "/history",
        context,
      );
      expect(() =>
        codec.decode(
          { cursor, limit: 1 },
          index === 21 ? { ...h.authorization, owner_id: "other-owner" } : h.authorization,
          index === 21 ? "/workstreams" : "/history",
          index === 22 ? { ...context, registry_version: 8 } : context,
          0,
        ),
      ).toThrow(index === 21 ? "cursor_invalid" : "cursor_stale");
      return;
    }
    case 23: {
      const id = h.create();
      h.update(id, "active");
      const highWater = h.state.audit.length;
      h.update(id, "active");
      const events = h.state.audit.map((event, i) => ({ event, event_sequence: i + 1 }));
      expect(historyPage(events, null, highWater, null, 100)).toHaveLength(highWater);
      expect(
        historyPage(events, null, highWater, null, 100).some(
          (e) => e.event_id === h.state.audit.at(-1)!.event_id,
        ),
      ).toBe(false);
      return;
    }
    case 24: {
      const id = h.create(),
        ref = h.reference("github"),
        membership = h.attach(id, ref);
      h.close(id, membership);
      rejected(
        h.submit({
          operation: "refresh_linked_pr",
          workstream_id: id,
          expected_version: h.ws(id).version,
          membership_id: membership,
          expected_observation_version: 0,
        }),
        "membership_conflict",
      );
      return;
    }
    case 25:
    case 26:
    case 27:
    case 28:
    case 29:
    case 30:
    case 36: {
      const id = h.create(),
        ref = h.reference("github"),
        membership = h.attach(id, ref);
      if ([25, 27, 29].includes(index)) h.observe(h.refresh(id, membership), "observed", snapshot);
      const intent = h.refresh(id, membership);
      const before = structuredClone(h.state);
      if (index === 29) {
        h.close(id, membership);
        expect(() => h.observe(intent, "observed", { ...snapshot, state: "merged" })).toThrow(
          "version_conflict",
        );
        expect(h.state.observations).toEqual(before.observations);
      } else if (index === 26) {
        expect(() => h.observe(intent, "not_modified", null)).toThrow("unknown_effect");
        expect(h.state).toEqual(before);
      } else if (index === 28) {
        const receipt = h.observe(intent, "invalid_response", null);
        expect(receipt.effects.observation?.outcome).toBe("invalid_response");
      } else if (index === 25 || index === 27) {
        const result = h.observe(
          intent,
          index === 25 ? "inaccessible" : "rate_limited",
          snapshot,
          index === 27 ? 3600 : null,
        );
        expect(result.effects.observation?.last_success).toEqual(snapshot);
        expect(h.ws(id).lifecycle).toBe("active");
        expect(h.state.references.has(ref)).toBe(true);
      } else if (index === 30) {
        h.observe(intent, "observed", { ...snapshot, state: "merged" });
        expect(h.ws(id).lifecycle).toBe("active");
        expect(h.state.memberships.get(membership)?.closed).toBeNull();
      } else {
        expect(h.state.receipts.get(intent.command.command_id)?.state).toBe("pending");
        h.state.receipts.set(intent.command.command_id, {
          ...h.state.receipts.get(intent.command.command_id)!,
          state: "unresolved",
          retry_after_seconds: 1,
        } as Wire.WorkstreamReceipt);
        const receipt = h.observe(intent, "observed", snapshot);
        const count = h.state.audit.length;
        expect(canonicalJson(h.finish(intent, { observation: receipt.effects.observation! }))).toBe(
          canonicalJson(receipt),
        );
        expect(h.state.audit).toHaveLength(count);
      }
      return;
    }
    case 32: {
      const id = h.create();
      const ref = h.reference();
      h.attach(id, ref);
      expect(h.ws(id).name).toBe("Synthetic coordination");
      expect(h.state.memberships.size).toBe(1);
      return;
    }
    case 33:
      expect(validTimestamp("2026-02-30T00:00:00Z")).toBe(false);
      return;
    case 34: {
      const a = h.create("Same synthetic name"),
        b = h.create("Same synthetic name");
      expect(a).not.toBe(b);
      return;
    }
    case 37: {
      const id = h.create(),
        ref = h.register();
      for (const operation of ["attach_primary", "link_secondary"] as const)
        rejected(
          h.submit({ operation, workstream_id: id, expected_version: 1, native_reference_id: ref }),
          "registration_unverified",
        );
      // A malformed fixture with an existing association still cannot bypass attestation on refresh.
      const membership = {
        membership_id: "membership:quarantined",
        workstream_id: id,
        native_reference_id: ref,
        kind: "secondary" as const,
        opened: {
          at,
          actor: h.authorization.actor,
          command_id: "command:quarantined",
          registry_version: 1,
        },
        closed: null,
      };
      h.state.memberships.set(membership.membership_id, membership);
      rejected(
        h.submit({
          operation: "refresh_linked_pr",
          workstream_id: id,
          expected_version: 1,
          membership_id: membership.membership_id,
          expected_observation_version: 0,
        }),
        "registration_unverified",
      );
      return;
    }
    case 38: {
      const ref = h.register();
      h.verify(ref);
      expect(h.state.references.get(ref)?.registration).toMatchObject({
        state: "attested",
        attestation_version: 1,
        evidence: { evidence_sha256: "a".repeat(64), native_id: "thread-one" },
      });
      return;
    }
    case 39: {
      const id = h.create(),
        ref = h.reference();
      const binding = h.state.source_bindings.get("t3-source")!;
      h.state.source_bindings.set("t3-source", { ...binding, store_generation: 2 });
      rejected(
        h.submit({
          operation: "attach_primary",
          workstream_id: id,
          expected_version: 1,
          native_reference_id: ref,
        }),
        "registration_unverified",
      );
      expect(h.state.memberships.size).toBe(0);
      expect(
        referenceForRead(
          h.state.references.get(ref)!,
          "owner-one",
          "principal-one",
          h.state.source_bindings.get("t3-source"),
          at,
        )?.registration.state,
      ).toBe("re-attestation-required");
      return;
    }
    case 40:
    case 41: {
      const id = h.create(),
        ref = h.reference(),
        membership = h.attach(id, ref);
      const receipt =
        index === 41
          ? h.close(id, membership)
          : h.committed({
              operation: "set_coordination_disposition",
              workstream_id: id,
              expected_version: h.ws(id).version,
              membership_id: membership,
              disposition: "completed",
              other_disposition: null,
            });
      expect(receipt.effects.native_settlement).toBeNull();
      expect(h.state.external_intents.size).toBe(1); // Only the earlier verification intent, never settlement.
      if (index === 40) {
        const intent = h.reserve({
          operation: "request_native_t3_settlement",
          native_reference_id: ref,
          expected_attestation_version: 1,
          native_action: "settle",
        });
        const terminal = h.finish(intent, nativeSettlementResult(intent, "denied"));
        expect(terminal.effects.native_settlement?.outcome).toBe("denied");
        expect(terminal.changed).toBe(false);
        expect(h.state.memberships.get(membership)?.closed?.reason).toBe("completed");
      } else expect(h.state.memberships.get(membership)?.closed?.reason).toBe("removed");
      return;
    }
    case 46: {
      const id = h.create();
      h.update(id, "completed");
      const before = structuredClone(h.state.lifecycle_declarations.get(id)!);
      h.update(id, "active");
      const history = h.state.lifecycle_declarations.get(id)!;
      expect(history.slice(0, 2)).toEqual(before);
      expect(history[2]?.revision).toBe(3);
      expect(history[2]?.supersedes_lifecycle_declaration_id).toBe(
        history[1]?.lifecycle_declaration_id,
      );
      return;
    }
    case 47: {
      const id = h.create();
      h.update(id, "completed");
      expect(h.ws(id)).toMatchObject({
        lifecycle: "completed",
        progress: { state: "unknown" },
        delivery: "none-observed",
        freshness: "unknown",
      });
      expect(h.state.memberships.size).toBe(0);
      return;
    }
    default:
      throw new Error("Semantic case has no execution mapping: " + index);
  }
}

describe("frozen v1 semantic cases: domain boundary", () => {
  const indexed = corpus.cases.map((fixture, index) => ({ ...fixture, index }));
  it("accounts explicitly for every frozen case", () => {
    expect(indexed).toHaveLength(48);
    expect(
      indexed.filter((c) => !browserExclusions.has(c.index) && !crossCapsuleProofs.has(c.index)),
    ).toHaveLength(42);
  });
  it.each(
    indexed.filter((c) => !browserExclusions.has(c.index) && !crossCapsuleProofs.has(c.index)),
  )("$name", ({ index }) => exercise(index));
  // oxlint-disable-next-line t3code/no-test-timeout-or-unreasoned-skip -- Accepted capsule explicitly excludes CP Origin/CSRF/proxy ingress; the per-case reasons remain visible.
  it.skip.each(
    indexed
      .filter((c) => browserExclusions.has(c.index))
      .map((c) => ({ ...c, reason: browserExclusions.get(c.index) })),
  )("$name — excluded: $reason", () => {});
  // Cross-capsule case31 executes in crypto/envelope.test.ts: AES-GCM rejects owner,
  // registry and purpose substitutions. The composed command runs that real envelope proof.
});
