import type { MembershipEpisode, T3ThreadPlacement } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import { planSetPrimary, type SetPrimaryPlanInput } from "./planner.ts";

const reference: SetPrimaryPlanInput["reference"] = {
  native_reference_id: "reference",
  owner_id: "owner",
  identity: {
    provider: "t3",
    resource_kind: "thread",
    id_kind: "internal",
    source_instance_id: "env:a",
    native_id: "thread",
    account_provenance: { kind: "not_account_scoped" },
  },
  registration: {
    state: "attested",
    attestation_version: 2,
    attested_at: "2026-10-23T09:00:00Z",
    expires_at: "2026-10-24T09:00:00Z",
    evidence: {
      provider: "t3",
      source_instance_id: "env:a",
      native_id: "thread",
      authority_namespace: "authority",
      store_generation: 1,
      evidence_sha256: "a".repeat(64),
    },
  },
};
const primary: T3ThreadPlacement = {
  native_reference_id: "reference",
  membership_id: "membership",
  workstream_id: "alpha",
  kind: "primary",
  source_instance_id: "env:a",
  native_thread_id: "thread",
  attestation_version: 2,
  attested_at: "2026-10-23T09:00:00Z",
  expires_at: "2026-10-24T09:00:00Z",
  evidence_sha256: "a".repeat(64),
  authority_namespace: "authority",
  store_generation: 1,
  source_binding_version: 1,
};
const input: SetPrimaryPlanInput = {
  request: {
    contract: "workstream-operations/1.0.0",
    operation_id: "operation-00000001",
    kind: "set_primary",
    origin: { kind: "app" },
    thread: { environment_id: "env:a", thread_id: "thread" },
    target: {
      kind: "workstream",
      registry_id: "registry",
      owner_id: "owner",
      workstream_id: "beta",
    },
    expected: { kind: "any" },
  },
  registry: { registry_id: "registry", owner_id: "owner" },
  reference,
  placements: [primary],
  workstreams: [
    { workstream_id: "alpha", owner_id: "owner", name: "Alpha", version: 3 },
    { workstream_id: "beta", owner_id: "owner", name: "Beta", version: 7 },
  ],
  destinationMemberships: [],
};
const boundary = {
  at: "2026-10-23T08:00:00Z",
  actor: { principal_id: "owner" },
  command_id: "historical-command",
  registry_version: 1,
};
const closed: MembershipEpisode = {
  membership_id: "old",
  workstream_id: "beta",
  native_reference_id: "reference",
  kind: "primary",
  opened: boundary,
  closed: { ...boundary, reason: "removed", other_reason: null },
};

describe("planSetPrimary", () => {
  it("moves with exact source and destination versions without mutating observations", () => {
    const before = structuredClone(input);
    expect(planSetPrimary(input)).toEqual({
      kind: "action",
      action: {
        operation: "move_primary",
        source_workstream_id: "alpha",
        expected_source_version: 3,
        source_membership_id: "membership",
        destination_workstream_id: "beta",
        expected_destination_version: 7,
      },
    });
    expect(input).toEqual(before);
  });

  it("unassigns the primary without removing secondary memberships", () => {
    expect(
      planSetPrimary({
        ...input,
        placements: [
          primary,
          { ...primary, kind: "secondary", membership_id: "secondary", workstream_id: "beta" },
        ],
        request: { ...input.request, target: { ...input.registry, kind: "unassigned" } },
      }),
    ).toEqual({
      kind: "action",
      action: {
        operation: "remove_membership",
        workstream_id: "alpha",
        expected_version: 3,
        membership_id: "membership",
      },
    });
  });

  it("selects attach or reattach only from matching closed primary history", () => {
    for (const [episodes, operation] of [
      [[], "attach_primary"],
      [[closed], "reattach_primary"],
      [[{ ...closed, kind: "secondary" }], "attach_primary"],
      [[{ ...closed, native_reference_id: "other" }], "attach_primary"],
      [[{ ...closed, workstream_id: "alpha" }], "attach_primary"],
    ] as const) {
      expect(
        planSetPrimary({ ...input, placements: [], destinationMemberships: episodes }),
      ).toEqual({
        kind: "action",
        action: {
          operation,
          workstream_id: "beta",
          expected_version: 7,
          native_reference_id: "reference",
        },
      });
    }
  });

  it("covers every desired/current/expected combination with desired-state precedence", () => {
    for (const current of [null, "alpha", "beta"] as const) {
      for (const target of [null, "alpha", "beta"] as const) {
        for (const expected of [
          { kind: "any" },
          { kind: "unassigned" },
          { kind: "primary", workstream_id: "alpha" },
          { kind: "primary", workstream_id: "beta" },
          { kind: "primary", workstream_id: "unknown" },
        ] as const) {
          const result = planSetPrimary({
            ...input,
            placements: current === null ? [] : [{ ...primary, workstream_id: current }],
            request: {
              ...input.request,
              expected,
              target:
                target === null
                  ? { ...input.registry, kind: "unassigned" }
                  : { ...input.registry, kind: "workstream", workstream_id: target },
            },
          });
          const matches =
            expected.kind === "any" ||
            (expected.kind === "unassigned"
              ? current === null
              : current === expected.workstream_id);
          expect(result.kind).toBe(
            current === target ? "already_satisfied" : matches ? "action" : "conflict",
          );
        }
      }
    }
  });

  it("rejects missing workstreams even before an otherwise satisfied or conflicting decision", () => {
    for (const workstreams of [[], [input.workstreams[0]!], [input.workstreams[1]!]]) {
      expect(() => planSetPrimary({ ...input, workstreams })).toThrow("workstream is unavailable");
    }
    expect(() =>
      planSetPrimary({
        ...input,
        workstreams: [],
        request: {
          ...input.request,
          target: { ...input.registry, kind: "workstream", workstream_id: "alpha" },
        },
      }),
    ).toThrow("Current workstream");
  });

  it("requires a complete scan for attach and rejects contradictory open primary history", () => {
    const withoutScan = { ...input };
    delete withoutScan.destinationMemberships;
    expect(() => planSetPrimary({ ...withoutScan, placements: [] })).toThrow("scan is required");
    expect(() =>
      planSetPrimary({
        ...input,
        placements: [],
        destinationMemberships: [{ ...closed, closed: null }],
      }),
    ).toThrow("contradicts");
  });

  it("does not require an attach scan for moves or satisfied state", () => {
    const withoutScan = { ...input };
    delete withoutScan.destinationMemberships;
    expect(planSetPrimary(withoutScan).kind).toBe("action");
    expect(
      planSetPrimary({
        ...withoutScan,
        request: {
          ...input.request,
          target: { ...input.registry, kind: "workstream", workstream_id: "alpha" },
        },
      }),
    ).toEqual({ kind: "already_satisfied" });
  });

  it("rejects an unknown requested destination", () => {
    expect(() =>
      planSetPrimary({
        ...input,
        request: {
          ...input.request,
          target: { ...input.registry, kind: "workstream", workstream_id: "unknown" },
        },
      }),
    ).toThrow("Destination workstream is unavailable");
  });

  it("rejects registry, thread-reference and placement inconsistencies", () => {
    const invalid: SetPrimaryPlanInput[] = [
      { ...input, registry: { ...input.registry, registry_id: "other" } },
      { ...input, reference: { ...reference, owner_id: "other" } },
      {
        ...input,
        reference: { ...reference, identity: { ...reference.identity, native_id: "other" } },
      },
      {
        ...input,
        reference: {
          ...reference,
          identity: { ...reference.identity, source_instance_id: "other" },
        },
      },
      {
        ...input,
        reference: { ...reference, registration: { ...reference.registration, evidence: null } },
      },
      { ...input, placements: [primary, primary] },
      ...[
        { native_reference_id: "other" },
        { native_thread_id: "other" },
        { source_instance_id: "other" },
        { attestation_version: 3 },
        { evidence_sha256: "b".repeat(64) },
        { authority_namespace: "other" },
        { store_generation: 2 },
      ].map((change) => ({ ...input, placements: [{ ...primary, ...change }] })),
      { ...input, workstreams: [...input.workstreams, input.workstreams[0]!] },
      { ...input, workstreams: input.workstreams.map((group) => ({ ...group, version: 0 })) },
    ];
    for (const candidate of invalid)
      expect(() => planSetPrimary(candidate)).toThrow("Inconsistent");
  });
});
